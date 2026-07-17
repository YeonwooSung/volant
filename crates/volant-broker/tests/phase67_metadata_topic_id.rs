//! Phase 67: Metadata TopicId v10–12.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, get_uuid, put_compact_array_len, put_compact_nullable_string,
    put_empty_tag_buffer, put_uuid, skip_tag_buffer, volant_topic_uuid, KAFKA_UUID_ZERO,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p67-{label}-{}-{}",
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

/// Metadata v10: null topics (all) + allow_auto + cluster ops + topic ops + tags.
fn metadata_v10_all() -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u8(0); // null compact array
    body.put_u8(0); // allow_auto
    body.put_u8(0); // cluster ops
    body.put_u8(0); // topic ops
    put_empty_tag_buffer(&mut body);
    body
}

/// Metadata v11: null topics + allow_auto + topic ops only (no cluster ops) + tags.
fn metadata_v11_all() -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u8(0);
    body.put_u8(0); // allow_auto
    body.put_u8(1); // topic ops
    put_empty_tag_buffer(&mut body);
    body
}

/// Metadata v10 named topic (uuid zero + name).
fn metadata_v10_named(topic: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, &KAFKA_UUID_ZERO);
    put_compact_nullable_string(&mut body, Some(topic));
    put_empty_tag_buffer(&mut body);
    body.put_u8(0);
    body.put_u8(0);
    body.put_u8(0);
    put_empty_tag_buffer(&mut body);
    body
}

/// Metadata v12 by TopicId only.
fn metadata_v12_by_id(uuid: &[u8; 16]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, uuid);
    put_compact_nullable_string(&mut body, None);
    put_empty_tag_buffer(&mut body);
    body.put_u8(0); // allow_auto
    body.put_u8(0); // topic ops only (v11+)
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_brokers_header(src: &mut impl Buf) {
    let n = get_compact_array_len(src).unwrap().unwrap();
    for _ in 0..n {
        let _ = src.get_i32();
        let _ = get_compact_string(src).unwrap();
        let _ = src.get_i32();
        let _ = get_compact_nullable_string(src).unwrap();
        skip_tag_buffer(src).unwrap();
    }
    let _ = get_compact_nullable_string(src).unwrap(); // cluster_id
    let _ = src.get_i32(); // controller
}

#[tokio::test]
async fn api_versions_metadata_max_12() {
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
    let mut meta = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min_v = src.get_i16();
        let max_v = src.get_i16();
        if key == 3 {
            meta = Some((min_v, max_v));
        }
    }
    assert_eq!(meta, Some((0, 12)));
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v10_emits_topic_id() {
    let dir = temp_dir("v10");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let tid = broker.create_topic("orders", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(3, 10, 10, Some("c"), &metadata_v10_named("orders")),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    skip_brokers_header(&mut src);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    let uuid = get_uuid(&mut src).unwrap();
    assert_eq!(uuid, volant_topic_uuid(tid.0));
    assert_eq!(src.get_u8(), 0); // is_internal
    let n_parts = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_parts, 2);
    // cluster ops present on v10
    // skip partitions + topic ops + tags then cluster ops
    for _ in 0..n_parts {
        let _ = src.get_i16();
        let _ = src.get_i32();
        let _ = src.get_i32();
        let _ = src.get_i32();
        let nr = get_compact_array_len(&mut src).unwrap().unwrap();
        for _ in 0..nr {
            let _ = src.get_i32();
        }
        let ni = get_compact_array_len(&mut src).unwrap().unwrap();
        for _ in 0..ni {
            let _ = src.get_i32();
        }
        let no = get_compact_array_len(&mut src).unwrap().unwrap();
        for _ in 0..no {
            let _ = src.get_i32();
        }
        skip_tag_buffer(&mut src).unwrap();
    }
    let _topic_ops = src.get_i32();
    skip_tag_buffer(&mut src).unwrap();
    let _cluster_ops = src.get_i32(); // v10 still has cluster ops
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v11_no_cluster_ops() {
    let dir = temp_dir("v11");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(3, 11, 11, Some("c"), &metadata_v11_all()),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    skip_brokers_header(&mut src);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_string(&mut src).unwrap(), "t");
    let _uuid = get_uuid(&mut src).unwrap();
    assert_eq!(src.get_u8(), 0);
    let n_parts = get_compact_array_len(&mut src).unwrap().unwrap();
    for _ in 0..n_parts {
        let _ = src.get_i16();
        let _ = src.get_i32();
        let _ = src.get_i32();
        let _ = src.get_i32();
        let nr = get_compact_array_len(&mut src).unwrap().unwrap();
        for _ in 0..nr {
            let _ = src.get_i32();
        }
        let ni = get_compact_array_len(&mut src).unwrap().unwrap();
        for _ in 0..ni {
            let _ = src.get_i32();
        }
        let no = get_compact_array_len(&mut src).unwrap().unwrap();
        for _ in 0..no {
            let _ = src.get_i32();
        }
        skip_tag_buffer(&mut src).unwrap();
    }
    let topic_ops = src.get_i32();
    assert_ne!(topic_ops, i32::MIN); // included
    skip_tag_buffer(&mut src).unwrap();
    // no cluster ops on v11 — only top-level tags
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v12_lookup_by_topic_id() {
    let dir = temp_dir("v12");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let tid = broker.create_topic("payments", 1).unwrap();
    broker.create_topic("other", 1).unwrap();
    let uuid = volant_topic_uuid(tid.0);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(3, 12, 12, Some("c"), &metadata_v12_by_id(&uuid)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    skip_brokers_header(&mut src);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_string(&mut src).unwrap(), "payments");
    assert_eq!(get_uuid(&mut src).unwrap(), uuid);

    // unknown id
    let bad = volant_topic_uuid(999_999);
    let resp = rpc(
        &addr,
        encode_request_flexible(3, 12, 13, Some("c"), &metadata_v12_by_id(&bad)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 13);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    skip_brokers_header(&mut src);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 100); // UNKNOWN_TOPIC_ID
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(get_uuid(&mut src).unwrap(), bad);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v10_all_topics() {
    let dir = temp_dir("v10all");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("a", 1).unwrap();
    broker.create_topic("b", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(3, 10, 20, Some("c"), &metadata_v10_all()),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    skip_brokers_header(&mut src);
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n, 2);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
