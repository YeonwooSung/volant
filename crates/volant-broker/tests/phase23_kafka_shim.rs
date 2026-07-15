//! Phase 23: Kafka wire protocol shim MVP.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    decode_message_set, decode_request_header, encode_message_set, encode_request,
    encode_response_frame, get_bytes, get_nullable_string, get_string, put_bytes, put_string,
    try_decode_request,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_core::{Message, MessageBatch, Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p23-{label}-{}-{}",
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
        // try parse one response
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

#[tokio::test]
async fn api_versions() {
    let dir = temp_dir("api-versions");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let req = encode_request(18, 0, 42, Some("test"), &[]);
    let resp = rpc(&addr, req).await;
    let mut src = resp.freeze();
    let corr = src.get_i32();
    assert_eq!(corr, 42);
    let err = src.get_i16();
    assert_eq!(err, 0);
    let n = src.get_i32();
    assert!(n >= 4, "expected at least 4 APIs, got {n}");
    let mut keys = Vec::new();
    for _ in 0..n {
        let key = src.get_i16();
        let min_v = src.get_i16();
        let max_v = src.get_i16();
        keys.push(key);
        assert!(min_v <= max_v);
    }
    assert!(keys.contains(&0)); // Produce
    assert!(keys.contains(&1)); // Fetch
    assert!(keys.contains(&3)); // Metadata
    assert!(keys.contains(&18)); // ApiVersions

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_lists_topic() {
    let dir = temp_dir("metadata");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Metadata v1, all topics (null topics array; empty means none for v1+)
    let mut body = BytesMut::new();
    body.put_i32(-1);
    let req = encode_request(3, 1, 1, Some("meta"), &body);
    let resp = rpc(&addr, req).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1); // correlation
    let broker_count = src.get_i32();
    assert_eq!(broker_count, 1);
    let node_id = src.get_i32();
    let host = get_string(&mut src).unwrap();
    let port = src.get_i32();
    assert!(!host.is_empty());
    assert!(port > 0);
    assert_eq!(node_id, 0);
    let rack = get_nullable_string(&mut src).unwrap(); // v1 rack
    assert!(rack.is_none());
    let _controller = src.get_i32(); // v1
    let topic_count = src.get_i32();
    assert_eq!(topic_count, 1);
    let terr = src.get_i16();
    assert_eq!(terr, 0);
    let tname = get_string(&mut src).unwrap();
    assert_eq!(tname, "events");
    let is_internal = src.get_u8();
    assert_eq!(is_internal, 0);
    let pcount = src.get_i32();
    assert_eq!(pcount, 2);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_and_fetch_messageset() {
    let dir = temp_dir("produce-fetch");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Build MessageSet
    let records = vec![Record {
        offset: Offset::new(0),
        key: Some(Bytes::from_static(b"k")),
        value: Bytes::from_static(b"hello-kafka"),
        timestamp_ms: 1_700_000_000_000,
        headers: vec![],
    }];
    let set = encode_message_set(&records);

    // ProduceRequest v0
    let mut body = BytesMut::new();
    body.put_i16(1); // acks
    body.put_i32(5000); // timeout
    body.put_i32(1); // topic count
    put_string(&mut body, "t");
    body.put_i32(1); // partition count
    body.put_i32(0); // partition
    put_bytes(&mut body, Some(&set));

    let req = encode_request(0, 0, 9, Some("prod"), &body);
    let resp = rpc(&addr, req).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 9);
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(get_string(&mut src).unwrap(), "t");
    assert_eq!(src.get_i32(), 1); // partitions
    assert_eq!(src.get_i32(), 0); // partition id
    let err = src.get_i16();
    assert_eq!(err, 0, "produce error {err}");
    let base = src.get_i64();
    assert_eq!(base, 0);

    // FetchRequest v0
    let mut fbody = BytesMut::new();
    fbody.put_i32(-1); // replica_id
    fbody.put_i32(0); // max_wait
    fbody.put_i32(1); // min_bytes
    fbody.put_i32(1); // topic count
    put_string(&mut fbody, "t");
    fbody.put_i32(1);
    fbody.put_i32(0); // partition
    fbody.put_i64(0); // offset
    fbody.put_i32(1_000_000); // max_bytes

    let freq = encode_request(1, 0, 10, Some("fetch"), &fbody);
    let fresp = rpc(&addr, freq).await;
    let mut fsrc = fresp.freeze();
    assert_eq!(fsrc.get_i32(), 10);
    assert_eq!(fsrc.get_i32(), 1);
    assert_eq!(get_string(&mut fsrc).unwrap(), "t");
    assert_eq!(fsrc.get_i32(), 1);
    assert_eq!(fsrc.get_i32(), 0);
    let ferr = fsrc.get_i16();
    assert_eq!(ferr, 0, "fetch error {ferr}");
    let _hwm = fsrc.get_i64();
    let record_set = get_bytes(&mut fsrc).unwrap().unwrap_or_default();
    let msgs = decode_message_set(&record_set).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].value.as_ref(), b"hello-kafka");
    assert_eq!(msgs[0].key.as_ref().unwrap().as_ref(), b"k");

    // Also readable via native broker fetch.
    let native = broker
        .fetch(&TopicName::new("t"), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    assert_eq!(native.len(), 1);
    assert_eq!(native[0].value.as_ref(), b"hello-kafka");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn kafka_produce_visible_to_volant_path() {
    let dir = temp_dir("cross");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("x", 1).unwrap();
    // Native produce
    broker
        .produce(
            &TopicName::new("x"),
            PartitionId(0),
            MessageBatch {
                messages: vec![Message {
                    key: None,
                    value: Bytes::from_static(b"native"),
                    timestamp_ms: None,
                    headers: vec![],
                }],
            },
        )
        .unwrap();

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut fbody = BytesMut::new();
    fbody.put_i32(-1);
    fbody.put_i32(0);
    fbody.put_i32(1);
    fbody.put_i32(1);
    put_string(&mut fbody, "x");
    fbody.put_i32(1);
    fbody.put_i32(0);
    fbody.put_i64(0);
    fbody.put_i32(1_000_000);
    let freq = encode_request(1, 0, 1, Some("f"), &fbody);
    let fresp = rpc(&addr, freq).await;
    let mut fsrc = fresp.freeze();
    fsrc.advance(4); // corr
    fsrc.advance(4); // topic count
    let _ = get_string(&mut fsrc).unwrap();
    fsrc.advance(4 + 4); // part count + part id
    assert_eq!(fsrc.get_i16(), 0);
    let _ = fsrc.get_i64();
    let set = get_bytes(&mut fsrc).unwrap().unwrap();
    let msgs = decode_message_set(&set).unwrap();
    assert_eq!(msgs[0].value.as_ref(), b"native");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// Re-export codec helpers used only in tests via public path.
// (encode_response_frame / try_decode_request / decode_request_header kept available)
#[allow(dead_code)]
fn _codec_smoke() {
    let _ = encode_response_frame(&[]);
    let mut b = BytesMut::new();
    let _ = try_decode_request(&mut b);
    let mut empty: &[u8] = &[];
    let _ = decode_request_header(&mut empty);
}
