//! Phase 45: Kafka topic admin classic versions (Create/DeleteTopics, CreatePartitions).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_request, get_nullable_string, get_string, put_string,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_core::TopicName;
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p45-{label}-{}-{}",
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

fn create_topics_body(
    version: i16,
    name: &str,
    partitions: i32,
    validate_only: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    put_string(&mut body, name);
    body.put_i32(partitions);
    body.put_i16(-1); // rf (optional on v4)
    body.put_i32(0); // assignments
    body.put_i32(0); // configs
    body.put_i32(5000); // timeout
    if version >= 1 {
        body.put_u8(if validate_only { 1 } else { 0 });
    }
    body
}

#[tokio::test]
async fn api_versions_topic_admin_classic_max() {
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
    assert_eq!(found.get(&19), Some(&(0, 4))); // CreateTopics
    assert_eq!(found.get(&20), Some(&(0, 3))); // DeleteTopics
    assert_eq!(found.get(&37), Some(&(0, 1))); // CreatePartitions
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_topics_v4_default_partitions_and_throttle() {
    let dir = temp_dir("ct4");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = create_topics_body(4, "auto-p", -1, false);
    let resp = rpc(&addr, encode_request(19, 4, 10, Some("a"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "auto-p");
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_nullable_string(&mut src).unwrap(), None);

    let meta = broker.metadata(Some(&[TopicName::new("auto-p")]));
    assert_eq!(meta.topics.len(), 1);
    assert_eq!(meta.topics[0].partitions.len(), 1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_topics_v1_validate_only() {
    let dir = temp_dir("vo");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = create_topics_body(1, "dry", 3, true);
    let resp = rpc(&addr, encode_request(19, 1, 11, Some("a"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    // v1: no throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "dry");
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_nullable_string(&mut src).unwrap(), None);

    // Topic must not exist
    assert!(broker
        .metadata(Some(&[TopicName::new("dry")]))
        .topics
        .is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_topics_v3_throttle() {
    let dir = temp_dir("dt3");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("gone", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    body.put_i32(1);
    put_string(&mut body, "gone");
    body.put_i32(5000);
    let resp = rpc(&addr, encode_request(20, 3, 12, Some("d"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    assert_eq!(src.get_i32(), 0); // throttle first
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "gone");
    assert_eq!(src.get_i16(), 0);

    assert!(broker
        .metadata(Some(&[TopicName::new("gone")]))
        .topics
        .is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_partitions_v1_validate_only_and_apply() {
    let dir = temp_dir("cp1");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // validate_only=true → success, no change
    let mut body = BytesMut::new();
    body.put_i32(1);
    put_string(&mut body, "orders");
    body.put_i32(4);
    body.put_i32(-1);
    body.put_i32(5000);
    body.put_u8(1); // validate_only
    let resp = rpc(&addr, encode_request(37, 1, 13, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 13);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_i16(), 0);
    let _ = get_nullable_string(&mut src).unwrap();

    let meta = broker.metadata(Some(&[TopicName::new("orders")]));
    assert_eq!(meta.topics[0].partitions.len(), 1);

    // apply
    let mut body2 = BytesMut::new();
    body2.put_i32(1);
    put_string(&mut body2, "orders");
    body2.put_i32(4);
    body2.put_i32(-1);
    body2.put_i32(5000);
    body2.put_u8(0);
    let resp2 = rpc(&addr, encode_request(37, 1, 14, Some("c"), &body2)).await;
    let mut s2 = resp2.freeze();
    assert_eq!(s2.get_i32(), 14);
    assert_eq!(s2.get_i32(), 0);
    assert_eq!(s2.get_i32(), 1);
    assert_eq!(get_string(&mut s2).unwrap(), "orders");
    assert_eq!(s2.get_i16(), 0);

    let meta2 = broker.metadata(Some(&[TopicName::new("orders")]));
    assert_eq!(meta2.topics[0].partitions.len(), 4);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_topics_v0_still_works() {
    let dir = temp_dir("v0");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = create_topics_body(0, "legacy", 2, false);
    let resp = rpc(&addr, encode_request(19, 0, 15, Some("a"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 15);
    // v0: no throttle, no error_message
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "legacy");
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
