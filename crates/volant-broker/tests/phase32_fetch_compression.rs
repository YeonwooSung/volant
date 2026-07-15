//! Phase 32: compressed Fetch v4 RecordBatch responses.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    decode_records, encode_record_batch, encode_request, get_bytes, get_string, put_bytes,
    put_string,
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
        "volant-p32-{label}-{}-{}",
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

fn fetch_v4_body(topic: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica
    body.put_i32(0); // max_wait
    body.put_i32(1); // min_bytes
    body.put_i32(1_048_576); // max_bytes
    body.put_u8(0); // isolation
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    body.put_i64(0);
    body.put_i32(1_000_000);
    body
}

fn fetch_v0_body(topic: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(0);
    body.put_i32(1);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    body.put_i64(0);
    body.put_i32(1_000_000);
    body
}

fn parse_fetch_v4_record_set(resp: BytesMut, corr: i32, topic: &str) -> Bytes {
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _hwm = src.get_i64();
    let _lso = src.get_i64();
    assert_eq!(src.get_i32(), 0); // aborted
    get_bytes(&mut src).unwrap().unwrap_or_default()
}

#[tokio::test]
async fn fetch_v4_default_lz4_compressed() {
    // Ensure default path (no env override in this process for the happy path).
    // OnceLock may already be initialized; we only require a non-none codec when
    // default applies, or successful decode either way.
    let dir = temp_dir("lz4");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("fc", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let records = vec![
        Record {
            offset: Offset::new(0),
            key: Some(Bytes::from_static(b"k1")),
            value: Bytes::from(vec![b'x'; 2048]), // large enough to show compression win
            timestamp_ms: 1_700_000_000_100,
            headers: vec![],
        },
        Record {
            offset: Offset::new(1),
            key: None,
            value: Bytes::from_static(b"second-fetch-value"),
            timestamp_ms: 1_700_000_000_200,
            headers: vec![],
        },
    ];
    let batch = encode_record_batch(&records);
    let prod = rpc(
        &addr,
        encode_request(0, 0, 1, Some("p"), &produce_body("fc", &batch)),
    )
    .await;
    let mut ps = prod.freeze();
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(get_string(&mut ps).unwrap(), "fc");
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(ps.get_i32(), 0);
    assert_eq!(ps.get_i16(), 0);

    let record_set = parse_fetch_v4_record_set(
        rpc(
            &addr,
            encode_request(1, 4, 2, Some("f"), &fetch_v4_body("fc")),
        )
        .await,
        2,
        "fc",
    );
    assert!(record_set.len() > 20);
    assert_eq!(record_set[16] as i8, 2); // magic
    let attrs = i16::from_be_bytes([record_set[21], record_set[22]]);
    let codec_bits = attrs & 0x07;
    // Default is lz4 (3) unless VOLANT_KAFKA_FETCH_COMPRESSION was set earlier.
    if std::env::var("VOLANT_KAFKA_FETCH_COMPRESSION").is_err() {
        assert_eq!(
            codec_bits,
            i16::from(CompressionCodec::Lz4.as_u8()),
            "default fetch compression should be lz4"
        );
    }
    // Compressed payload should be smaller than a plain re-encode of the same records.
    if codec_bits != 0 {
        let plain = encode_record_batch(
            &broker
                .fetch(&TopicName::new("fc"), PartitionId(0), Offset::new(0), 10)
                .unwrap(),
        );
        assert!(
            record_set.len() < plain.len(),
            "compressed fetch {} should be < plain {}",
            record_set.len(),
            plain.len()
        );
    }

    let msgs = decode_records(&record_set).unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].key.as_ref().unwrap().as_ref(), b"k1");
    assert_eq!(msgs[0].value.len(), 2048);
    assert_eq!(msgs[1].value.as_ref(), b"second-fetch-value");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v0_message_set_decodes() {
    let dir = temp_dir("ms");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("ms", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let records = vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(b"message-set-path"),
        timestamp_ms: 1_700_000_000_300,
        headers: vec![],
    }];
    let _ = rpc(
        &addr,
        encode_request(0, 0, 1, Some("p"), &produce_body("ms", &encode_record_batch(&records))),
    )
    .await;

    let resp = rpc(
        &addr,
        encode_request(1, 0, 2, Some("f"), &fetch_v0_body("ms")),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    // v0: no throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "ms");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _hwm = src.get_i64();
    let set = get_bytes(&mut src).unwrap().unwrap_or_default();
    // MessageSet path (Phase 33 may compress with wrapper attributes ≠ 0).
    assert!(set.len() > 18);
    assert_eq!(set[16] as i8, 1, "MessageSet magic");

    let msgs = decode_records(&set).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].value.as_ref(), b"message-set-path");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn empty_fetch_has_no_batch() {
    let dir = temp_dir("empty");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("empty", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let record_set = parse_fetch_v4_record_set(
        rpc(
            &addr,
            encode_request(1, 4, 9, Some("f"), &fetch_v4_body("empty")),
        )
        .await,
        9,
        "empty",
    );
    assert!(record_set.is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
