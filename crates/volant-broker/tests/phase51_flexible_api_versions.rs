//! Phase 51: Flexible codec + ApiVersions v3 (KIP-482).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, put_compact_string,
    put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p51-{label}-{}-{}",
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

fn api_versions_v3_body(name: &str, version: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, name);
    put_compact_string(&mut body, version);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_advertises_max_3() {
    let dir = temp_dir("adv");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32();
    let mut max18 = None;
    for _ in 0..n {
        let key = src.get_i16();
        let _min = src.get_i16();
        let max = src.get_i16();
        if key == 18 {
            max18 = Some(max);
        }
    }
    assert_eq!(max18, Some(3));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v3_flexible_roundtrip() {
    let dir = temp_dir("v3");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = api_versions_v3_body("volant-test", "0.1.0");
    let resp = rpc(
        &addr,
        encode_request_flexible(18, 3, 42, Some("flex-client"), &body),
    )
    .await;
    let mut src = resp.freeze();
    // Response header v0: correlation only (no header tag buffer).
    assert_eq!(src.get_i32(), 42);
    assert_eq!(src.get_i16(), 0); // error
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    assert!(n >= 10, "expected many api keys, got {n}");
    let mut saw_self = false;
    let mut saw_produce = false;
    let mut saw_fetch = false;
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        skip_tag_buffer(&mut src).unwrap(); // per-entry tags
        if key == 18 {
            assert_eq!((min, max), (0, 3));
            saw_self = true;
        }
        if key == 0 {
            assert_eq!((min, max), (0, 9));
            saw_produce = true;
        }
        if key == 1 {
            assert_eq!((min, max), (0, 11));
            saw_fetch = true;
        }
    }
    assert!(saw_self && saw_produce && saw_fetch);
    assert_eq!(src.get_i32(), 0); // throttle
    skip_tag_buffer(&mut src).unwrap(); // top-level tags
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v0_still_classic() {
    let dir = temp_dir("v0");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 0, 7, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 7);
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32(); // classic array length
    assert!(n >= 10);
    for _ in 0..n {
        let _ = src.get_i16();
        let _ = src.get_i16();
        let _ = src.get_i16();
    }
    assert_eq!(src.remaining(), 0); // no throttle on v0

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
