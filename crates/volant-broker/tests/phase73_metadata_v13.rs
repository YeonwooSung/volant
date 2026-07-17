//! Phase 73: Metadata v13 top-level ErrorCode.

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
use volant_core::TopicName;
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p73-{label}-{}-{}",
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

/// Metadata v12/v13 request: null topics (all) + allow_auto + topic ops + tags.
fn metadata_v13_all() -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u8(0); // null compact array = all
    body.put_u8(0); // allow_auto
    body.put_u8(0); // topic ops
    put_empty_tag_buffer(&mut body);
    body
}

/// Metadata v12/v13 named topic (zero uuid + name).
fn metadata_v13_named(topic: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, &KAFKA_UUID_ZERO);
    put_compact_nullable_string(&mut body, Some(topic));
    put_empty_tag_buffer(&mut body);
    body.put_u8(0); // allow_auto
    body.put_u8(0); // topic ops
    put_empty_tag_buffer(&mut body);
    body
}

/// Metadata v12/v13 by TopicId only.
fn metadata_v13_by_id(uuid: &[u8; 16]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, uuid);
    put_compact_nullable_string(&mut body, None);
    put_empty_tag_buffer(&mut body);
    body.put_u8(0);
    body.put_u8(0);
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
async fn api_versions_metadata_max_13() {
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
    assert_eq!(meta, Some((0, 13)));
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v13_top_level_error_code_zero() {
    let dir = temp_dir("err0");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(3, 13, 13, Some("c"), &metadata_v13_named("orders")),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 13);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    skip_brokers_header(&mut src);

    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i16(), 0); // topic error
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    let uuid = get_uuid(&mut src).unwrap();
    assert_ne!(uuid, KAFKA_UUID_ZERO);
    assert_eq!(src.get_u8(), 0); // is_internal
    let n_parts = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_parts, 1);
    // skip partition
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i32(), 0); // partition index
    let _leader = src.get_i32();
    let _epoch = src.get_i32();
    let n_rep = get_compact_array_len(&mut src).unwrap().unwrap();
    for _ in 0..n_rep {
        let _ = src.get_i32();
    }
    let n_isr = get_compact_array_len(&mut src).unwrap().unwrap();
    for _ in 0..n_isr {
        let _ = src.get_i32();
    }
    let n_off = get_compact_array_len(&mut src).unwrap().unwrap();
    for _ in 0..n_off {
        let _ = src.get_i32();
    }
    skip_tag_buffer(&mut src).unwrap(); // partition tags
    let _ops = src.get_i32();
    skip_tag_buffer(&mut src).unwrap(); // topic tags

    // v13 top-level ErrorCode
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v13_by_topic_id_and_all() {
    let dir = temp_dir("by-id");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let id = broker.metadata(Some(&[TopicName::new("events")])).topics[0]
        .topic_id
        .0;
    let uuid = volant_topic_uuid(id);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // by id
    let resp = rpc(
        &addr,
        encode_request_flexible(3, 13, 20, Some("c"), &metadata_v13_by_id(&uuid)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    skip_brokers_header(&mut src);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    assert_eq!(get_uuid(&mut src).unwrap(), uuid);
    // skip rest of topic to ErrorCode
    let _ = src.get_u8();
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
    let _ = src.get_i32();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0); // top-level ErrorCode
    skip_tag_buffer(&mut src).unwrap();

    // all topics
    let resp = rpc(
        &addr,
        encode_request_flexible(3, 13, 21, Some("c"), &metadata_v13_all()),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 21);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    skip_brokers_header(&mut src);
    let n_topics = get_compact_array_len(&mut src).unwrap().unwrap();
    assert!(n_topics >= 1);
    for _ in 0..n_topics {
        let _ = src.get_i16();
        let _ = get_compact_string(&mut src).unwrap();
        let _ = get_uuid(&mut src).unwrap();
        let _ = src.get_u8();
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
        let _ = src.get_i32();
        skip_tag_buffer(&mut src).unwrap();
    }
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v12_still_omits_top_level_error() {
    let dir = temp_dir("v12");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(3, 12, 12, Some("c"), &metadata_v13_named("t")),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    skip_brokers_header(&mut src);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    let _ = src.get_i16();
    let _ = get_compact_string(&mut src).unwrap();
    let _ = get_uuid(&mut src).unwrap();
    let _ = src.get_u8();
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
    let _ = src.get_i32();
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    // v12: only top-level tags remain (no ErrorCode int16)
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v14_unsupported_header_v1() {
    let dir = temp_dir("v14");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(3, 14, 1, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
