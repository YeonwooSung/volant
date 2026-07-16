//! Phase 44: Kafka OffsetCommit classic v0–7 + FindCoordinator v2.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_consumer_subscription, encode_request, get_nullable_string, get_string, put_bytes,
    put_nullable_string, put_string,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p44-{label}-{}-{}",
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

fn join_v5(group: &str, member_id: &str, instance: Option<&str>, topics: &[&str]) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    body.put_i32(10_000);
    body.put_i32(10_000);
    put_string(&mut body, member_id);
    put_nullable_string(&mut body, instance);
    put_string(&mut body, "consumer");
    body.put_i32(1);
    put_string(&mut body, "range");
    let sub = encode_consumer_subscription(topics);
    put_bytes(&mut body, Some(&sub));
    body
}

/// OffsetCommit classic body for versions 2–7.
fn commit_body(
    version: i16,
    group: &str,
    generation: i32,
    member_id: &str,
    instance: Option<&str>,
    topic: &str,
    partition: i32,
    offset: i64,
    meta: &str,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    body.put_i32(generation);
    put_string(&mut body, member_id);
    if version >= 7 {
        put_nullable_string(&mut body, instance);
    }
    if (2..=4).contains(&version) {
        body.put_i64(-1); // retention
    }
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(offset);
    if version >= 6 {
        body.put_i32(-1); // committed_leader_epoch
    }
    put_string(&mut body, meta);
    body
}

#[tokio::test]
async fn api_versions_offset_commit_and_find_coordinator() {
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
    let mut found = std::collections::HashMap::new();
    for _ in 0..n {
        let key = src.get_i16();
        let min_v = src.get_i16();
        let max_v = src.get_i16();
        found.insert(key, (min_v, max_v));
    }
    assert_eq!(found.get(&8), Some(&(0, 7))); // OffsetCommit
    assert_eq!(found.get(&10), Some(&(0, 2))); // FindCoordinator
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn find_coordinator_v2_throttle() {
    let dir = temp_dir("fc");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    put_string(&mut body, "cg");
    body.put_i8(0); // group
    let resp = rpc(&addr, encode_request(10, 2, 2, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_nullable_string(&mut src).unwrap(), None);
    let node = src.get_i32();
    assert!(node >= 0);
    let host = get_string(&mut src).unwrap();
    assert!(!host.is_empty());
    let port = src.get_i32();
    assert!(port > 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_v5_no_retention_throttle() {
    let dir = temp_dir("v5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // generation 0 path — no live membership required
    let body = commit_body(5, "g5", 0, "", None, "orders", 0, 99, "m5");
    let resp = rpc(&addr, encode_request(8, 5, 3, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 3);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    // OffsetFetch v5 sees the commit
    let mut fbody = BytesMut::new();
    put_string(&mut fbody, "g5");
    fbody.put_i32(1);
    put_string(&mut fbody, "orders");
    fbody.put_i32(1);
    fbody.put_i32(0);
    let fresp = rpc(&addr, encode_request(9, 5, 4, Some("c"), &fbody)).await;
    let mut fs = fresp.freeze();
    assert_eq!(fs.get_i32(), 4);
    assert_eq!(fs.get_i32(), 0); // throttle
    assert_eq!(fs.get_i32(), 1);
    assert_eq!(get_string(&mut fs).unwrap(), "orders");
    assert_eq!(fs.get_i32(), 1);
    assert_eq!(fs.get_i32(), 0);
    assert_eq!(fs.get_i64(), 99);
    assert_eq!(fs.get_i32(), -1); // committed_leader_epoch
    assert_eq!(get_string(&mut fs).unwrap(), "m5");
    assert_eq!(fs.get_i16(), 0);
    assert_eq!(fs.get_i16(), 0); // top-level error

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_v7_static_instance() {
    let dir = temp_dir("v7");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(
        &addr,
        encode_request(
            11,
            5,
            10,
            Some("c"),
            &join_v5("cg", "", Some("worker-1"), &["events"]),
        ),
    )
    .await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    assert_eq!(js.get_i32(), 0);
    assert_eq!(js.get_i16(), 0);
    let generation = js.get_i32();
    let _ = get_string(&mut js).unwrap();
    let _ = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();
    assert_eq!(member_id, "static:worker-1");

    // Commit with empty member_id + group_instance_id
    let body = commit_body(
        7,
        "cg",
        generation,
        "",
        Some("worker-1"),
        "events",
        0,
        7,
        "static-meta",
    );
    let resp = rpc(&addr, encode_request(8, 7, 11, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "events");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    // Commit with member_id set + instance (member_id wins)
    let body2 = commit_body(
        7,
        "cg",
        generation,
        &member_id,
        Some("worker-1"),
        "events",
        0,
        8,
        "mid-meta",
    );
    let resp2 = rpc(&addr, encode_request(8, 7, 12, Some("c"), &body2)).await;
    let mut s2 = resp2.freeze();
    assert_eq!(s2.get_i32(), 12);
    assert_eq!(s2.get_i32(), 0);
    assert_eq!(s2.get_i32(), 1);
    let _ = get_string(&mut s2).unwrap();
    assert_eq!(s2.get_i32(), 1);
    assert_eq!(s2.get_i32(), 0);
    assert_eq!(s2.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_v6_leader_epoch_ignored() {
    let dir = temp_dir("v6");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = commit_body(6, "g6", 0, "", None, "t", 0, 55, "e");
    let resp = rpc(&addr, encode_request(8, 6, 5, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "t");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_v2_unchanged() {
    let dir = temp_dir("v2");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = commit_body(2, "g2", 0, "", None, "t", 0, 11, "old");
    let resp = rpc(&addr, encode_request(8, 2, 6, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 6);
    // v2: no throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "t");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
