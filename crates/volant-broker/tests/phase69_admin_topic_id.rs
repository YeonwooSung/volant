//! Phase 69: CreateTopics TopicId v7 + DeleteTopics TopicId v6 / ErrorMessage v5.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, get_uuid, put_compact_array_len, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_uuid, skip_tag_buffer, volant_topic_uuid,
    KAFKA_UUID_ZERO,
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
        "volant-p69-{label}-{}-{}",
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

fn create_topics_v7(name: &str, partitions: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    body.put_i32(partitions);
    body.put_i16(-1);
    put_compact_array_len(&mut body, 0);
    put_compact_array_len(&mut body, 0);
    put_empty_tag_buffer(&mut body);
    body.put_i32(5000);
    body.put_u8(0);
    put_empty_tag_buffer(&mut body);
    body
}

fn delete_topics_v5(name: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    body.put_i32(5000);
    put_empty_tag_buffer(&mut body);
    body
}

fn delete_topics_v6_by_id(uuid: &[u8; 16]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_nullable_string(&mut body, None);
    put_uuid(&mut body, uuid);
    put_empty_tag_buffer(&mut body);
    body.put_i32(5000);
    put_empty_tag_buffer(&mut body);
    body
}

fn delete_topics_v6_by_name(name: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_nullable_string(&mut body, Some(name));
    put_uuid(&mut body, &KAFKA_UUID_ZERO);
    put_empty_tag_buffer(&mut body);
    body.put_i32(5000);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_admin_topic_id_maxes() {
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
    assert_eq!(found.get(&19), Some(&(0, 7)));
    assert_eq!(found.get(&20), Some(&(0, 6)));
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_topics_v7_returns_topic_id() {
    let dir = temp_dir("create-v7");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(19, 7, 10, Some("a"), &create_topics_v7("tid-t", 2)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "tid-t");
    let uuid = get_uuid(&mut src).unwrap();
    assert_ne!(uuid, KAFKA_UUID_ZERO);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(src.get_i32(), 2);
    assert_eq!(src.get_i16(), 1);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    let meta = broker.metadata(Some(&[TopicName::new("tid-t")]));
    assert_eq!(meta.topics.len(), 1);
    assert_eq!(uuid, volant_topic_uuid(meta.topics[0].topic_id.0));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_topics_v5_error_message() {
    let dir = temp_dir("del-v5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(20, 5, 3, Some("a"), &delete_topics_v5("nope")),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 3);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "nope");
    assert_eq!(src.get_i16(), 3); // UNKNOWN_TOPIC_OR_PARTITION
    let msg = get_compact_nullable_string(&mut src).unwrap();
    assert!(msg.is_some());
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_topics_v6_by_topic_id() {
    let dir = temp_dir("del-id");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("doomed", 1).unwrap();
    let id = broker
        .metadata(Some(&[TopicName::new("doomed")]))
        .topics[0]
        .topic_id
        .0;
    let uuid = volant_topic_uuid(id);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(20, 6, 4, Some("a"), &delete_topics_v6_by_id(&uuid)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 4);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    // Name may be filled from resolution.
    let name = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(name.as_deref(), Some("doomed"));
    assert_eq!(get_uuid(&mut src).unwrap(), uuid);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    assert!(broker
        .metadata(Some(&[TopicName::new("doomed")]))
        .topics
        .is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_topics_v6_unknown_id() {
    let dir = temp_dir("del-unk");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut bad = [0u8; 16];
    bad[0] = 0xab;
    let resp = rpc(
        &addr,
        encode_request_flexible(20, 6, 5, Some("a"), &delete_topics_v6_by_id(&bad)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    skip_tag_buffer(&mut src).unwrap();
    let _ = src.get_i32();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(get_uuid(&mut src).unwrap(), bad);
    assert_eq!(src.get_i16(), 100); // UNKNOWN_TOPIC_ID

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_topics_v6_by_name_still_works() {
    let dir = temp_dir("del-name");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("named-del", 1).unwrap();
    let id = broker
        .metadata(Some(&[TopicName::new("named-del")]))
        .topics[0]
        .topic_id
        .0;
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            20,
            6,
            6,
            Some("a"),
            &delete_topics_v6_by_name("named-del"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 6);
    skip_tag_buffer(&mut src).unwrap();
    let _ = src.get_i32();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("named-del")
    );
    assert_eq!(get_uuid(&mut src).unwrap(), volant_topic_uuid(id));
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_topics_v5_still_no_topic_id_field() {
    let dir = temp_dir("v5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Same body layout as v7 (request unchanged).
    let resp = rpc(
        &addr,
        encode_request_flexible(19, 5, 1, Some("a"), &create_topics_v7("old-t", 1)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "old-t");
    // Immediately error code — no UUID between name and error on v5.
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
