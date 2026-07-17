//! Phase 50: Kafka ApiVersions classic v0–2 (trailing throttle).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::Buf;
use volant_broker::kafka::codec::encode_request;
use volant_broker::Broker;
use volant_storage::StorageConfig;

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
    let dir = temp_dir("p50", "self");
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
    assert_eq!(found.get(&18), Some(&(0, 5))); // ApiVersions through flexible v5 (Phase 83)
    assert_eq!(found.get(&0), Some(&(0, 13))); // Produce (Phase 71 TopicId)
    assert_eq!(found.get(&1), Some(&(0, 18))); // Fetch (Phase 84 Kafka max)
    // v0 has no trailing throttle
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v1_trailing_throttle() {
    let dir = temp_dir("p50", "v1");
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
    assert_eq!(found.get(&18), Some(&(0, 5)));
    assert_eq!(src.get_i32(), 0); // throttle trailing
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v2_wire_identical_to_v1() {
    let dir = temp_dir("p50", "v2");
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
    assert_eq!(found.get(&18), Some(&(0, 5)));
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v6_unsupported() {
    let dir = temp_dir("p50", "v6");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // v6 beyond Kafka max 5; flexible body not required for unsupported-version path.
    // ApiVersions response header stays v0 (correlation only).
    let resp = rpc(&addr, encode_request(18, 6, 9, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 9);
    assert_eq!(src.get_i16(), 35); // UNSUPPORTED_VERSION

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
