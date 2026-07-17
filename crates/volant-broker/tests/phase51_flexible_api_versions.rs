//! Phase 51: Flexible codec + ApiVersions v3 (KIP-482).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, put_compact_string,
    put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

fn api_versions_v3_body(name: &str, version: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, name);
    put_compact_string(&mut body, version);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_advertises_max_3() {
    let dir = temp_dir("p51", "adv");
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
    let dir = temp_dir("p51", "v3");
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
            assert_eq!((min, max), (0, 13)); // Phase 71 Produce TopicId
            saw_produce = true;
        }
        if key == 1 {
            assert_eq!((min, max), (0, 13)); // Phase 68 Fetch TopicId
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
    let dir = temp_dir("p51", "v0");
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
