//! Phase 60: Flexible CreateTopics v5 / DeleteTopics v4 / CreatePartitions v2.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, get_nullable_string, get_string, put_compact_array_len, put_compact_string,
    put_empty_tag_buffer, put_string, put_unsigned_varint, skip_tag_buffer,
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
        "volant-p60-{label}-{}-{}",
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

fn create_topics_v5(name: &str, partitions: i32, validate_only: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    body.put_i32(partitions);
    body.put_i16(-1); // rf
    put_compact_array_len(&mut body, 0); // assignments
    put_compact_array_len(&mut body, 0); // configs
    put_empty_tag_buffer(&mut body); // topic tags
    body.put_i32(5000); // timeout
    body.put_u8(if validate_only { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

fn delete_topics_v4(name: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    body.put_i32(5000);
    put_empty_tag_buffer(&mut body);
    body
}

fn create_partitions_v2(name: &str, count: i32, validate_only: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    body.put_i32(count);
    put_unsigned_varint(&mut body, 0); // null assignments
    put_empty_tag_buffer(&mut body);
    body.put_i32(5000);
    body.put_u8(if validate_only { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_topic_admin_flex_maxes() {
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
    assert_eq!(found.get(&19), Some(&(0, 5)));
    assert_eq!(found.get(&20), Some(&(0, 4)));
    assert_eq!(found.get(&37), Some(&(0, 2)));
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_delete_partitions_flexible_roundtrip() {
    let dir = temp_dir("roundtrip");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreateTopics v5
    let resp = rpc(
        &addr,
        encode_request_flexible(19, 5, 10, Some("a"), &create_topics_v5("flex-t", 2, false)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "flex-t");
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(src.get_i32(), 2); // num partitions
    assert_eq!(src.get_i16(), 1); // rf placeholder
    // null configs
    assert_eq!(get_compact_array_len(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    let meta = broker.metadata(Some(&[TopicName::new("flex-t")]));
    assert_eq!(meta.topics[0].partitions.len(), 2);

    // CreatePartitions v2: grow to 4
    let resp = rpc(
        &addr,
        encode_request_flexible(
            37,
            2,
            11,
            Some("a"),
            &create_partitions_v2("flex-t", 4, false),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "flex-t");
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    let meta = broker.metadata(Some(&[TopicName::new("flex-t")]));
    assert_eq!(meta.topics[0].partitions.len(), 4);

    // DeleteTopics v4
    let resp = rpc(
        &addr,
        encode_request_flexible(20, 4, 12, Some("a"), &delete_topics_v4("flex-t")),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "flex-t");
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    assert!(broker
        .metadata(Some(&[TopicName::new("flex-t")]))
        .topics
        .is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_topics_v5_validate_only_and_default_partitions() {
    let dir = temp_dir("vo");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(19, 5, 1, Some("a"), &create_topics_v5("dry", -1, true)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "dry");
    assert_eq!(src.get_i16(), 0);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1); // default partitions
    assert_eq!(src.get_i16(), 1);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    assert!(broker
        .metadata(Some(&[TopicName::new("dry")]))
        .topics
        .is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn classic_topic_admin_still_works() {
    let dir = temp_dir("classic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    body.put_i32(1);
    put_string(&mut body, "c-t");
    body.put_i32(1);
    body.put_i16(-1);
    body.put_i32(0);
    body.put_i32(0);
    body.put_i32(5000);
    body.put_u8(0);
    let resp = rpc(&addr, encode_request(19, 4, 10, Some("a"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // no header tags
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "c-t");
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_nullable_string(&mut src).unwrap(), None);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_versions_use_header_v1() {
    let dir = temp_dir("unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for (api, ver, corr) in [(19i16, 6i16, 1i32), (20, 5, 2), (37, 3, 3)] {
        let resp = rpc(
            &addr,
            encode_request_flexible(api, ver, corr, Some("c"), &[]),
        )
        .await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), corr);
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.get_i16(), 35, "api={api} ver={ver}");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
