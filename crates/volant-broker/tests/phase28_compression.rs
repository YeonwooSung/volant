//! Phase 28: Kafka RecordBatch compression (gzip/snappy/lz4/zstd) on Produce.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    decode_records, encode_record_batch_compressed, encode_request, get_bytes, get_string,
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
        "volant-p28-{label}-{}-{}",
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

async fn produce_compressed_and_verify(codec: CompressionCodec, topic: &str) {
    let dir = temp_dir(&format!("{codec:?}").to_lowercase());
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic(topic, 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let records = vec![
        Record {
            offset: Offset::new(0),
            key: Some(Bytes::from_static(b"ck")),
            value: Bytes::from(format!("value-under-{codec:?}-compression").into_bytes()),
            timestamp_ms: 1_700_000_000_300,
            headers: vec![("codec".into(), Bytes::from(format!("{codec:?}")))],
        },
        Record {
            offset: Offset::new(1),
            key: None,
            value: Bytes::from_static(b"second-compressed"),
            timestamp_ms: 1_700_000_000_400,
            headers: vec![],
        },
    ];
    let batch = encode_record_batch_compressed(&records, codec).unwrap();
    // attributes at offset after baseOffset(8)+batchLength(4)+leaderEpoch(4)+magic(1)+crc(4) = 21
    // attributes is first 2 bytes of crc payload; compression bits non-zero for codecs > 0
    if codec != CompressionCodec::None {
        let attrs = i16::from_be_bytes([batch[21], batch[22]]);
        assert_eq!(attrs & 0x07, i16::from(codec.as_u8()));
    }

    let mut body = BytesMut::new();
    body.put_i16(1); // acks
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(&batch));

    let req = encode_request(0, 0, 42, Some("p28"), &body);
    let resp = rpc(&addr, req).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 42);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let err = src.get_i16();
    assert_eq!(err, 0, "produce error {err} codec={codec:?}");
    assert_eq!(src.get_i64(), 0);

    // Native fetch sees plain records.
    let native = broker
        .fetch(&TopicName::new(topic), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    assert_eq!(native.len(), 2, "codec={codec:?}");
    assert_eq!(native[0].key.as_ref().unwrap().as_ref(), b"ck");
    assert_eq!(
        native[0].value.as_ref(),
        format!("value-under-{codec:?}-compression").as_bytes()
    );
    assert_eq!(native[0].headers[0].0, "codec");
    assert_eq!(native[1].value.as_ref(), b"second-compressed");

    // Kafka Fetch v4 returns uncompressed RecordBatch of the same data.
    let mut fbody = BytesMut::new();
    fbody.put_i32(-1);
    fbody.put_i32(0);
    fbody.put_i32(1);
    fbody.put_i32(1_048_576);
    fbody.put_u8(0);
    fbody.put_i32(1);
    put_string(&mut fbody, topic);
    fbody.put_i32(1);
    fbody.put_i32(0);
    fbody.put_i64(0);
    fbody.put_i32(1_000_000);

    let freq = encode_request(1, 4, 43, Some("f28"), &fbody);
    let fresp = rpc(&addr, freq).await;
    let mut fsrc = fresp.freeze();
    assert_eq!(fsrc.get_i32(), 43);
    assert_eq!(fsrc.get_i32(), 0); // throttle
    assert_eq!(fsrc.get_i32(), 1);
    assert_eq!(get_string(&mut fsrc).unwrap(), topic);
    assert_eq!(fsrc.get_i32(), 1);
    assert_eq!(fsrc.get_i32(), 0);
    assert_eq!(fsrc.get_i16(), 0);
    let _hwm = fsrc.get_i64();
    let _lso = fsrc.get_i64();
    assert_eq!(fsrc.get_i32(), 0);
    let record_set = get_bytes(&mut fsrc).unwrap().unwrap_or_default();
    assert_eq!(record_set[16] as i8, 2);
    // Fetch remains uncompressed.
    let attrs = i16::from_be_bytes([record_set[21], record_set[22]]);
    assert_eq!(attrs & 0x07, 0, "fetch should be uncompressed");
    let msgs = decode_records(&record_set).unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(
        msgs[0].value.as_ref(),
        format!("value-under-{codec:?}-compression").as_bytes()
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_gzip_record_batch() {
    produce_compressed_and_verify(CompressionCodec::Gzip, "gzip-t").await;
}

#[tokio::test]
async fn produce_snappy_record_batch() {
    produce_compressed_and_verify(CompressionCodec::Snappy, "snappy-t").await;
}

#[tokio::test]
async fn produce_lz4_record_batch() {
    produce_compressed_and_verify(CompressionCodec::Lz4, "lz4-t").await;
}

#[tokio::test]
async fn produce_zstd_record_batch() {
    produce_compressed_and_verify(CompressionCodec::Zstd, "zstd-t").await;
}
