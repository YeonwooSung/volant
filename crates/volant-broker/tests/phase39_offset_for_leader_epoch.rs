//! Phase 39: Kafka OffsetForLeaderEpoch on the shim.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, get_string, put_bytes, put_string,
};
use volant_broker::{
    serve_kafka_listener, AclEntry, AclOperation, AclPermission, Broker, ResourceType,
};
use volant_core::{Offset, Record};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p39-{label}-{}-{}",
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

/// Build OffsetForLeaderEpoch body for one partition.
fn ofle_body(
    version: i16,
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    leader_epoch: i32,
) -> BytesMut {
    let mut body = BytesMut::new();
    if version >= 3 {
        body.put_i32(-1); // replica_id (consumer)
    }
    body.put_i32(1); // topics
    put_string(&mut body, topic);
    body.put_i32(1); // partitions
    body.put_i32(partition);
    if version >= 2 {
        body.put_i32(current_leader_epoch);
    }
    body.put_i32(leader_epoch);
    body
}

async fn produce_one_async(addr: &str, topic: &str) {
    let records = vec![Record {
        offset: Offset::new(0),
        key: Some(Bytes::from_static(b"k")),
        value: Bytes::from_static(b"v"),
        timestamp_ms: 1_700_000_000_000,
        headers: vec![],
    }];
    let batch = encode_record_batch(&records);
    let mut body = BytesMut::new();
    body.put_i16(-1); // nullable transactional_id (Produce v3)
    body.put_i16(1); // acks
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(&batch));
    let resp = rpc(addr, encode_request(0, 3, 1, Some("p"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1); // corr
    // Produce: topics first; throttle is trailing (v1+)
    let topics = src.get_i32();
    assert_eq!(topics, 1);
    let _ = get_string(&mut src).unwrap();
    let parts = src.get_i32();
    assert_eq!(parts, 1);
    let _pid = src.get_i32();
    let err = src.get_i16();
    assert_eq!(err, 0, "produce failed");
}

#[tokio::test]
async fn api_versions_includes_offset_for_leader_epoch() {
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
    let mut found = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min_v = src.get_i16();
        let max_v = src.get_i16();
        if key == 23 {
            found = Some((min_v, max_v));
        }
    }
    assert_eq!(found, Some((0, 4))); // Phase 63 flexible v4
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn ofle_v2_returns_hwm_for_current_epoch() {
    let dir = temp_dir("hwm");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one_async(&addr, "orders").await;
    produce_one_async(&addr, "orders").await;

    // leader_epoch -1 = latest
    let body = ofle_body(2, "orders", 0, -1, -1);
    let resp = rpc(&addr, encode_request(23, 2, 10, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_i32(), 1); // partitions
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(src.get_i32(), 0); // partition
    let epoch = src.get_i32();
    assert!(epoch >= 0);
    let end = src.get_i64();
    assert_eq!(end, 2); // two produces

    // Explicit epoch 0 should also return HWM
    let body = ofle_body(2, "orders", 0, -1, 0);
    let resp = rpc(&addr, encode_request(23, 2, 11, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i32(), 0);
    let _ = src.get_i32();
    assert_eq!(src.get_i64(), 2);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn ofle_v2_unknown_leader_epoch() {
    let dir = temp_dir("unknown");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = ofle_body(2, "orders", 0, -1, 99);
    let resp = rpc(&addr, encode_request(23, 2, 12, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 75); // UNKNOWN_LEADER_EPOCH
    assert_eq!(src.get_i32(), 0);
    let _ = src.get_i32();
    assert_eq!(src.get_i64(), -1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn ofle_v2_client_ahead_current_epoch() {
    let dir = temp_dir("ahead");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // current_leader_epoch > partition epoch → UNKNOWN_LEADER_EPOCH
    let body = ofle_body(2, "orders", 0, 1, -1);
    let resp = rpc(&addr, encode_request(23, 2, 13, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 13);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 75); // UNKNOWN_LEADER_EPOCH
    assert_eq!(src.get_i32(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn ofle_unknown_topic() {
    let dir = temp_dir("notopic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = ofle_body(3, "missing", 0, -1, -1);
    let resp = rpc(&addr, encode_request(23, 3, 14, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 14);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "missing");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 3); // UNKNOWN_TOPIC_OR_PARTITION
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), -1);
    assert_eq!(src.get_i64(), -1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn ofle_acl_denied() {
    let dir = temp_dir("acl");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    // Enable ACLs: only alice may Describe; kafka-anonymous is denied.
    broker
        .acls()
        .create(vec![AclEntry {
            principal: "alice".into(),
            resource_type: ResourceType::Topic,
            resource: "orders".into(),
            operation: AclOperation::Describe,
            permission: AclPermission::Allow,
        }])
        .expect("acl");

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let body = ofle_body(2, "orders", 0, -1, -1);
    let resp = rpc(&addr, encode_request(23, 2, 15, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 15);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 29); // TOPIC_AUTHORIZATION_FAILED

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn ofle_v0_no_throttle_no_response_epoch() {
    let dir = temp_dir("v0");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one_async(&addr, "orders").await;

    let body = ofle_body(0, "orders", 0, -1, -1);
    let resp = rpc(&addr, encode_request(23, 0, 16, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 16);
    // no throttle in v0
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i32(), 0);
    // no leader_epoch field in v0 response
    assert_eq!(src.get_i64(), 1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
