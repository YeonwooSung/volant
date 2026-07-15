//! Phase 33: Kafka MessageSet compression on Produce and Fetch v0–3.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    decode_records, encode_message_set_compressed, encode_request, get_bytes, get_string,
    put_bytes, put_string,
};
use volant_broker::kafka::compress::CompressionCodec;
use volant_broker::{serve_kafka_listener, Broker};
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p33-{label}-{}-{}",
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn boot_kafka(broker: Arc<Broker>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        serve_kafka_listener(listener, broker).await.ok();
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

async fn rpc(addr: &str, request: BytesMut) -> BytesMut {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&request).await.unwrap();
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
        let n = stream.read_buf(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        if buf.len() >= 4 {
            let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if buf.len() >= 4 + size {
                let _ = buf.split_to(4);
                return buf.split_to(size);
            }
        }
    }
    panic!("connection closed without full kafka response");
}

fn produce_body(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

fn sample_records() -> Vec<Record> {
    vec![
        Record {
            offset: Offset::new(0),
            key: Some(Bytes::from_static(b"mk")),
            value: Bytes::from(b"message-set-compressed-value-".repeat(20)),
            timestamp_ms: 1_700_000_000_100,
            headers: vec![],
        },
        Record {
            offset: Offset::new(1),
            key: None,
            value: Bytes::from_static(b"second-ms"),
            timestamp_ms: 1_700_000_000_200,
            headers: vec![],
        },
    ]
}

async fn produce_compressed_messageset_and_verify(codec: CompressionCodec, topic: &str) {
    let dir = temp_dir(&format!("{codec:?}").to_lowercase());
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic(topic, 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let records = sample_records();
    let set = encode_message_set_compressed(&records, codec).unwrap();
    if codec != CompressionCodec::None {
        let attrs = set[17] as i8;
        let expected = if codec == CompressionCodec::Zstd {
            CompressionCodec::Lz4.as_u8()
        } else {
            codec.as_u8()
        };
        assert_eq!(attrs & 0x07, expected as i8);
    }

    let resp = rpc(
        &addr,
        encode_request(0, 0, 10, Some("p33"), &produce_body(topic, &set)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let err = src.get_i16();
    assert_eq!(err, 0, "produce error {err} codec={codec:?}");

    let native = broker
        .fetch(&TopicName::new(topic), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    assert_eq!(native.len(), 2, "codec={codec:?}");
    assert_eq!(native[0].key.as_ref().unwrap().as_ref(), b"mk");
    assert_eq!(native[0].value.as_ref(), records[0].value.as_ref());
    assert_eq!(native[1].value.as_ref(), b"second-ms");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_gzip_message_set() {
    produce_compressed_messageset_and_verify(CompressionCodec::Gzip, "ms-gzip").await;
}

#[tokio::test]
async fn produce_snappy_message_set() {
    produce_compressed_messageset_and_verify(CompressionCodec::Snappy, "ms-snappy").await;
}

#[tokio::test]
async fn produce_lz4_message_set() {
    produce_compressed_messageset_and_verify(CompressionCodec::Lz4, "ms-lz4").await;
}

#[tokio::test]
async fn produce_zstd_maps_to_lz4_message_set() {
    // Encode path maps zstd → lz4 for MessageSet.
    produce_compressed_messageset_and_verify(CompressionCodec::Zstd, "ms-zstd").await;
}

#[tokio::test]
async fn fetch_v0_returns_compressed_message_set_by_default() {
    let dir = temp_dir("fetch-v0");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("fv0", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Produce plain MessageSet via uncompressed encode path (RecordBatch also fine).
    let records = sample_records();
    let set = encode_message_set_compressed(&records, CompressionCodec::None).unwrap();
    let _ = rpc(
        &addr,
        encode_request(0, 0, 1, Some("p"), &produce_body("fv0", &set)),
    )
    .await;

    let mut fbody = BytesMut::new();
    fbody.put_i32(-1);
    fbody.put_i32(0);
    fbody.put_i32(1);
    fbody.put_i32(1);
    put_string(&mut fbody, "fv0");
    fbody.put_i32(1);
    fbody.put_i32(0);
    fbody.put_i64(0);
    fbody.put_i32(1_000_000);

    let fresp = rpc(&addr, encode_request(1, 0, 2, Some("f"), &fbody)).await;
    let mut src = fresp.freeze();
    assert_eq!(src.get_i32(), 2);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "fv0");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _hwm = src.get_i64();
    let record_set = get_bytes(&mut src).unwrap().unwrap_or_default();
    assert!(record_set.len() > 18);
    assert_eq!(record_set[16] as i8, 1);
    if std::env::var("VOLANT_KAFKA_FETCH_COMPRESSION").is_err() {
        // Default lz4 wrapper.
        assert_eq!(
            record_set[17] as i8 & 0x07,
            CompressionCodec::Lz4.as_u8() as i8
        );
    }
    let msgs = decode_records(&record_set).unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].value.as_ref(), records[0].value.as_ref());
    assert_eq!(msgs[1].value.as_ref(), b"second-ms");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
