//! Phase 53: Flexible Produce v9 (KIP-482 compact records + response header v1).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, encode_request_flexible, get_compact_array_len,
    get_compact_nullable_string, get_compact_string, get_string, put_compact_array_len,
    put_compact_bytes, put_compact_nullable_string, put_compact_string, put_empty_tag_buffer,
    put_nullable_string, put_string, skip_tag_buffer,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_core::{Offset, Record};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p53-{label}-{}-{}",
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

fn sample_records(value: &'static [u8]) -> Vec<Record> {
    vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(value),
        timestamp_ms: 1_700_000_000_000,
        headers: vec![],
    }]
}

/// Produce v9 flexible body: compact txn_id, acks, timeout, compact topic/partition/records.
fn produce_v9_body(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, None); // transactional_id
    body.put_i16(1); // acks
    body.put_i32(5000);
    put_compact_array_len(&mut body, 1); // topics
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1); // partitions
    body.put_i32(0); // partition index
    put_compact_bytes(&mut body, Some(batch));
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // top-level tags
    body
}

/// Classic produce body (v3–8).
fn produce_classic_body(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, None);
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    // classic bytes
    body.put_i32(batch.len() as i32);
    body.extend_from_slice(batch);
    body
}

#[tokio::test]
async fn api_versions_produce_max_9() {
    let dir = temp_dir("api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32();
    let mut produce = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        if key == 0 {
            produce = Some((min, max));
        }
    }
    assert_eq!(produce, Some((0, 9)));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v9_flexible_roundtrip() {
    let dir = temp_dir("v9");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"hello-v9"));
    let body = produce_v9_body("events", &batch);
    let resp = rpc(
        &addr,
        encode_request_flexible(0, 9, 42, Some("flex-prod"), &body),
    )
    .await;
    let mut src = resp.freeze();
    // Response header v1
    assert_eq!(src.get_i32(), 42);
    skip_tag_buffer(&mut src).unwrap();

    let n_topics = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_topics, 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    let n_parts = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_parts, 1);
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // error
    let base = src.get_i64();
    assert!(base >= 0, "base offset {base}");
    assert_eq!(src.get_i64(), -1); // log_append_time
    let log_start = src.get_i64();
    assert!(log_start >= 0);
    let n_err = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_err, 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap(); // partition tags
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    assert_eq!(src.get_i32(), 0); // throttle
    skip_tag_buffer(&mut src).unwrap(); // top-level
    assert_eq!(src.remaining(), 0);

    // Visible via broker fetch path
    let fetched = broker
        .fetch(
            &volant_core::TopicName::new("events"),
            volant_core::PartitionId(0),
            Offset::new(base as u64),
            1024,
        )
        .unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].value.as_ref(), b"hello-v9");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v8_still_classic() {
    let dir = temp_dir("v8");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("classic", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"v8"));
    let body = produce_classic_body("classic", &batch);
    let resp = rpc(&addr, encode_request(0, 8, 7, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 7); // header v0 only
    assert_eq!(src.get_i32(), 1); // classic topic count
    assert_eq!(get_string(&mut src).unwrap(), "classic");
    assert_eq!(src.get_i32(), 1); // partitions
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let base = src.get_i64();
    assert!(base >= 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v10_unsupported() {
    let dir = temp_dir("v10");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // v10 not handled; version ≥9 still uses response header v1.
    let resp = rpc(
        &addr,
        encode_request_flexible(0, 10, 1, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35); // UNSUPPORTED_VERSION

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
