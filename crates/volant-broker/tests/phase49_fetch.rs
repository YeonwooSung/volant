//! Phase 49: Kafka Fetch classic v0–11
//! (log_start_offset, session header, preferred_read_replica, leader epoch).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, get_bytes, get_string, put_bytes, put_string,
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
        "volant-p49-{label}-{}-{}",
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

fn produce_body_v3(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    volant_broker::kafka::codec::put_nullable_string(&mut body, None);
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

/// Fetch body for classic versions 4–11.
///
/// `version` controls optional fields: session (v7+), leader_epoch (v9+),
/// follower log_start (v5+), forgotten (v7+), rack (v11+).
fn fetch_body(
    version: i16,
    topic: &str,
    fetch_offset: i64,
    isolation: u8,
    session_id: i32,
    current_leader_epoch: i32,
    rack: Option<&str>,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica
    body.put_i32(0); // max_wait
    body.put_i32(1); // min_bytes
    if version >= 3 {
        body.put_i32(1_048_576); // max_bytes
    }
    if version >= 4 {
        body.put_u8(isolation);
    }
    if version >= 7 {
        body.put_i32(session_id);
        body.put_i32(-1); // session_epoch
    }
    body.put_i32(1); // topics
    put_string(&mut body, topic);
    body.put_i32(1); // partitions
    body.put_i32(0); // partition index
    if version >= 9 {
        body.put_i32(current_leader_epoch);
    }
    body.put_i64(fetch_offset);
    if version >= 5 {
        body.put_i64(-1); // follower log_start_offset
    }
    body.put_i32(1_000_000); // partition_max_bytes
    if version >= 7 {
        body.put_i32(0); // forgotten_topics empty
    }
    if version >= 11 {
        put_string(&mut body, rack.unwrap_or(""));
    }
    body
}

#[tokio::test]
async fn api_versions_fetch_max_v13() {
    let dir = temp_dir("api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    src.advance(4 + 2);
    let n = src.get_i32();
    let mut produce = None;
    let mut fetch = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        if key == 0 {
            produce = Some((min, max));
        }
        if key == 1 {
            fetch = Some((min, max));
        }
    }
    assert_eq!(produce, Some((0, 13))); // Phase 71 TopicId
    assert_eq!(fetch, Some((0, 13))); // Phase 68 TopicId

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v5_log_start_offset() {
    let dir = temp_dir("v5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"v5-msg"));
    let _ = rpc(
        &addr,
        encode_request(0, 5, 2, Some("p"), &produce_body_v3("orders", &batch)),
    )
    .await;

    let resp = rpc(
        &addr,
        encode_request(
            1,
            5,
            3,
            Some("c"),
            &fetch_body(5, "orders", 0, 0, 0, -1, None),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 3);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let hwm = src.get_i64();
    let lso = src.get_i64();
    let log_start = src.get_i64();
    assert_eq!(lso, hwm);
    assert!(hwm >= 1);
    assert!(log_start >= 0, "log_start={log_start}");
    assert_eq!(src.get_i32(), 0); // aborted empty
    let records = get_bytes(&mut src).unwrap().unwrap();
    assert!(!records.is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v7_session_header() {
    let dir = temp_dir("v7");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"s"));
    let _ = rpc(
        &addr,
        encode_request(0, 5, 2, Some("p"), &produce_body_v3("t", &batch)),
    )
    .await;

    let resp = rpc(
        &addr,
        encode_request(
            1,
            7,
            4,
            Some("c"),
            &fetch_body(7, "t", 0, 0, 42, -1, None),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 4);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top-level error
    assert_eq!(src.get_i32(), 42); // session_id echo
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "t");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let hwm = src.get_i64();
    let lso = src.get_i64();
    let log_start = src.get_i64();
    assert_eq!(lso, hwm);
    assert!(log_start >= 0);
    assert_eq!(src.get_i32(), 0); // aborted
    let _ = get_bytes(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v11_preferred_read_replica() {
    let dir = temp_dir("v11");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("rack-t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"rack"));
    let _ = rpc(
        &addr,
        encode_request(0, 8, 2, Some("p"), &produce_body_v3("rack-t", &batch)),
    )
    .await;

    let resp = rpc(
        &addr,
        encode_request(
            1,
            11,
            5,
            Some("c"),
            &fetch_body(11, "rack-t", 0, 1, 0, -1, Some("us-east-1a")),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top error
    assert_eq!(src.get_i32(), 0); // session
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "rack-t");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let hwm = src.get_i64();
    let lso = src.get_i64();
    let log_start = src.get_i64();
    assert_eq!(lso, hwm);
    assert!(log_start >= 0);
    assert_eq!(src.get_i32(), 0); // aborted
    assert_eq!(src.get_i32(), -1); // preferred_read_replica
    let records = get_bytes(&mut src).unwrap().unwrap();
    assert!(!records.is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// Fetch v12 flexible support: phase54_flexible_fetch.
// Fetch v13+ (TopicId) remains unsupported there.
