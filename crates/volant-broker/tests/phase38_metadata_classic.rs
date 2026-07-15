//! Phase 38: Kafka Metadata classic v0–8 on the shim.

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
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p38-{label}-{}-{}",
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

/// Metadata request body for classic versions.
/// `topics`: None → null array (all, v1+); Some([]) → empty; Some(names) → listed.
fn metadata_body(version: i16, topics: Option<&[&str]>, include_ops: bool) -> BytesMut {
    let mut body = BytesMut::new();
    match topics {
        None if version >= 1 => body.put_i32(-1),
        None => body.put_i32(0), // v0 empty = all
        Some(list) => {
            body.put_i32(list.len() as i32);
            for t in list {
                put_string(&mut body, t);
            }
        }
    }
    if version >= 4 {
        body.put_u8(1); // allow_auto_topic_creation (ignored)
    }
    if version >= 8 {
        body.put_u8(if include_ops { 1 } else { 0 }); // cluster ops
        body.put_u8(if include_ops { 1 } else { 0 }); // topic ops
    }
    body
}

/// Parse Metadata response body (after correlation_id). Returns (topic_count, remaining cursor checks).
struct MetaParsed {
    throttle: Option<i32>,
    broker_count: i32,
    rack: Option<Option<String>>,
    cluster_id: Option<Option<String>>,
    controller: Option<i32>,
    topic_count: i32,
    topic_name: Option<String>,
    leader_epoch: Option<i32>,
    offline_empty: Option<bool>,
    topic_auth_ops: Option<i32>,
    cluster_auth_ops: Option<i32>,
}

fn parse_metadata(src: &mut impl Buf, version: i16) -> MetaParsed {
    let throttle = if version >= 3 {
        Some(src.get_i32())
    } else {
        None
    };
    let broker_count = src.get_i32();
    let mut rack = None;
    for _ in 0..broker_count.max(0) {
        let _id = src.get_i32();
        let _host = get_string(src).unwrap();
        let _port = src.get_i32();
        if version >= 1 {
            rack = Some(get_nullable_string(src).unwrap());
        }
    }
    let cluster_id = if version >= 2 {
        Some(get_nullable_string(src).unwrap())
    } else {
        None
    };
    let controller = if version >= 1 {
        Some(src.get_i32())
    } else {
        None
    };
    let topic_count = src.get_i32();
    let mut topic_name = None;
    let mut leader_epoch = None;
    let mut offline_empty = None;
    let mut topic_auth_ops = None;
    if topic_count > 0 {
        let _err = src.get_i16();
        topic_name = Some(get_string(src).unwrap());
        if version >= 1 {
            let _internal = src.get_u8();
        }
        let pcount = src.get_i32();
        for _ in 0..pcount.max(0) {
            let _pe = src.get_i16();
            let _pid = src.get_i32();
            let _leader = src.get_i32();
            if version >= 7 {
                leader_epoch = Some(src.get_i32());
            }
            let rcount = src.get_i32();
            for _ in 0..rcount.max(0) {
                let _ = src.get_i32();
            }
            let icount = src.get_i32();
            for _ in 0..icount.max(0) {
                let _ = src.get_i32();
            }
            if version >= 5 {
                let ocount = src.get_i32();
                offline_empty = Some(ocount == 0);
                for _ in 0..ocount.max(0) {
                    let _ = src.get_i32();
                }
            }
        }
        if version >= 8 {
            topic_auth_ops = Some(src.get_i32());
        }
        // Skip remaining topics if any (we only created one in tests).
        for _ in 1..topic_count.max(0) {
            let _ = src.get_i16();
            let _ = get_string(src).unwrap();
            if version >= 1 {
                let _ = src.get_u8();
            }
            let pcount = src.get_i32();
            for _ in 0..pcount.max(0) {
                let _ = src.get_i16();
                let _ = src.get_i32();
                let _ = src.get_i32();
                if version >= 7 {
                    let _ = src.get_i32();
                }
                let rcount = src.get_i32();
                for _ in 0..rcount.max(0) {
                    let _ = src.get_i32();
                }
                let icount = src.get_i32();
                for _ in 0..icount.max(0) {
                    let _ = src.get_i32();
                }
                if version >= 5 {
                    let ocount = src.get_i32();
                    for _ in 0..ocount.max(0) {
                        let _ = src.get_i32();
                    }
                }
            }
            if version >= 8 {
                let _ = src.get_i32();
            }
        }
    }
    let cluster_auth_ops = if version >= 8 {
        Some(src.get_i32())
    } else {
        None
    };
    MetaParsed {
        throttle,
        broker_count,
        rack,
        cluster_id,
        controller,
        topic_count,
        topic_name,
        leader_epoch,
        offline_empty,
        topic_auth_ops,
        cluster_auth_ops,
    }
}

#[tokio::test]
async fn api_versions_metadata_max_8() {
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
        if key == 3 {
            found = Some((min_v, max_v));
        }
    }
    assert_eq!(found, Some((0, 8)));
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v2_cluster_id() {
    let dir = temp_dir("v2");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = metadata_body(2, None, false);
    let resp = rpc(&addr, encode_request(3, 2, 10, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    let p = parse_metadata(&mut src, 2);
    assert_eq!(p.broker_count, 1);
    assert_eq!(p.rack, Some(None));
    assert_eq!(p.cluster_id, Some(Some("volant".into())));
    assert!(p.controller.is_some());
    assert_eq!(p.topic_count, 1);
    assert_eq!(p.topic_name.as_deref(), Some("orders"));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v5_offline_and_throttle() {
    let dir = temp_dir("v5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 2).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = metadata_body(5, None, false);
    let resp = rpc(&addr, encode_request(3, 5, 11, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    let p = parse_metadata(&mut src, 5);
    assert_eq!(p.throttle, Some(0));
    assert_eq!(p.cluster_id, Some(Some("volant".into())));
    assert_eq!(p.topic_count, 1);
    assert_eq!(p.offline_empty, Some(true));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v7_leader_epoch() {
    let dir = temp_dir("v7");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = metadata_body(7, None, false);
    let resp = rpc(&addr, encode_request(3, 7, 12, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    let p = parse_metadata(&mut src, 7);
    assert_eq!(p.throttle, Some(0));
    assert_eq!(p.leader_epoch, Some(-1));
    assert_eq!(p.offline_empty, Some(true));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v8_authorized_ops() {
    let dir = temp_dir("v8");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // include flags false → INT32_MIN
    let body = metadata_body(8, None, false);
    let resp = rpc(&addr, encode_request(3, 8, 13, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 13);
    let p = parse_metadata(&mut src, 8);
    assert_eq!(p.topic_auth_ops, Some(i32::MIN));
    assert_eq!(p.cluster_auth_ops, Some(i32::MIN));

    // include flags true → non-zero bitfield (ACLs off ⇒ all common ops)
    let body = metadata_body(8, None, true);
    let resp = rpc(&addr, encode_request(3, 8, 14, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 14);
    let p = parse_metadata(&mut src, 8);
    let topic_ops = p.topic_auth_ops.unwrap();
    let cluster_ops = p.cluster_auth_ops.unwrap();
    assert_ne!(topic_ops, i32::MIN);
    assert_ne!(cluster_ops, i32::MIN);
    // Describe bit (code 8)
    assert_ne!(topic_ops & (1 << 8), 0);
    assert_ne!(cluster_ops & (1 << 8), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v1_empty_topics_means_none() {
    let dir = temp_dir("empty");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = metadata_body(1, Some(&[]), false);
    let resp = rpc(&addr, encode_request(3, 1, 15, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 15);
    let p = parse_metadata(&mut src, 1);
    assert_eq!(p.broker_count, 1);
    assert_eq!(p.topic_count, 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v4_named_topic() {
    let dir = temp_dir("named");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    broker.create_topic("payments", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = metadata_body(4, Some(&["payments"]), false);
    let resp = rpc(&addr, encode_request(3, 4, 16, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 16);
    let p = parse_metadata(&mut src, 4);
    assert_eq!(p.throttle, Some(0));
    assert_eq!(p.topic_count, 1);
    assert_eq!(p.topic_name.as_deref(), Some("payments"));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
