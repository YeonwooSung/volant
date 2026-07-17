//! Phase 50: Kafka ApiVersions classic v0–2 (trailing throttle).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::encode_request;
use volant_broker::{serve_kafka_listener, Broker};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p50-{label}-{}-{}",
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

fn parse_api_keys(src: &mut impl Buf) -> std::collections::HashMap<i16, (i16, i16)> {
    let n = src.get_i32();
    let mut found = std::collections::HashMap::new();
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        found.insert(key, (min, max));
    }
    found
}

#[tokio::test]
async fn api_versions_self_advertises_max_2() {
    let dir = temp_dir("self");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let found = parse_api_keys(&mut src);
    assert_eq!(found.get(&18), Some(&(0, 3))); // ApiVersions through flexible v3 (Phase 51)
    assert_eq!(found.get(&0), Some(&(0, 9))); // Produce (Phase 53)
    assert_eq!(found.get(&1), Some(&(0, 13))); // Fetch (Phase 68)
    // v0 has no trailing throttle
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v1_trailing_throttle() {
    let dir = temp_dir("v1");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 1, 2, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    assert_eq!(src.get_i16(), 0);
    let found = parse_api_keys(&mut src);
    assert!(found.len() >= 10);
    assert_eq!(found.get(&18), Some(&(0, 3)));
    assert_eq!(src.get_i32(), 0); // throttle trailing
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v2_wire_identical_to_v1() {
    let dir = temp_dir("v2");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 2, 3, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 3);
    assert_eq!(src.get_i16(), 0);
    let found = parse_api_keys(&mut src);
    assert_eq!(found.get(&18), Some(&(0, 3)));
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v4_unsupported() {
    let dir = temp_dir("v4");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // v4 not advertised; flexible body not required for unsupported-version path.
    let resp = rpc(&addr, encode_request(18, 4, 9, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 9);
    assert_eq!(src.get_i16(), 35); // UNSUPPORTED_VERSION

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
