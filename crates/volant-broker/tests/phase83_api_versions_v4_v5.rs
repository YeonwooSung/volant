//! Phase 83: ApiVersions v4–5 (Kafka max; empty feature tags; v5 ClusterId/NodeId ignored).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// ApiVersions flexible body for v3–4 (ClientSoftwareName/Version + tags).
fn api_versions_v3_v4_body(name: &str, version: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, name);
    put_compact_string(&mut body, version);
    put_empty_tag_buffer(&mut body);
    body
}

/// ApiVersions flexible body for v5 (+ ClusterId nullable + NodeId + tags).
fn api_versions_v5_body(
    name: &str,
    version: &str,
    cluster_id: Option<&str>,
    node_id: i32,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, name);
    put_compact_string(&mut body, version);
    put_compact_nullable_string(&mut body, cluster_id);
    body.put_i32(node_id);
    put_empty_tag_buffer(&mut body);
    body
}

fn parse_flex_api_keys(src: &mut impl Buf) -> std::collections::HashMap<i16, (i16, i16)> {
    let n = get_compact_array_len(src).unwrap().unwrap();
    let mut found = std::collections::HashMap::new();
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        skip_tag_buffer(src).unwrap();
        found.insert(key, (min, max));
    }
    found
}

#[tokio::test]
async fn api_versions_advertises_max_5() {
    let dir = temp_dir("p83", "adv");
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
    assert_eq!(max18, Some(5));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v4_flexible_roundtrip() {
    let dir = temp_dir("p83", "v4");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = api_versions_v3_v4_body("volant-test", "0.1.0");
    let resp = rpc(
        &addr,
        encode_request_flexible(18, 4, 42, Some("flex-v4"), &body),
    )
    .await;
    let mut src = resp.freeze();
    // Response header v0: correlation only (no header tag buffer) — ApiVersions special case.
    assert_eq!(src.get_i32(), 42);
    assert_eq!(src.get_i16(), 0); // error
    let found = parse_flex_api_keys(&mut src);
    assert!(found.len() >= 10, "expected many api keys, got {}", found.len());
    assert_eq!(found.get(&18), Some(&(0, 5)));
    assert_eq!(found.get(&0), Some(&(0, 13))); // Produce
    assert_eq!(found.get(&1), Some(&(0, 13))); // Fetch
    assert_eq!(src.get_i32(), 0); // throttle
    skip_tag_buffer(&mut src).unwrap(); // empty top-level tags (no features)
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v5_flexible_roundtrip() {
    let dir = temp_dir("p83", "v5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // ClusterId/NodeId present (including mismatched values) — always succeed.
    let body = api_versions_v5_body("volant-test", "0.2.0", Some("other-cluster"), 99);
    let resp = rpc(
        &addr,
        encode_request_flexible(18, 5, 55, Some("flex-v5"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 55); // header v0
    assert_eq!(src.get_i16(), 0); // never REBOOTSTRAP_REQUIRED
    let found = parse_flex_api_keys(&mut src);
    assert_eq!(found.get(&18), Some(&(0, 5)));
    assert_eq!(src.get_i32(), 0); // throttle
    skip_tag_buffer(&mut src).unwrap(); // empty features
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v5_null_cluster_id() {
    let dir = temp_dir("p83", "v5-null");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Defaults: ClusterId=null, NodeId=-1 (Kafka schema defaults).
    let body = api_versions_v5_body("cli", "1.0", None, -1);
    let resp = rpc(
        &addr,
        encode_request_flexible(18, 5, 66, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 66);
    assert_eq!(src.get_i16(), 0);
    let found = parse_flex_api_keys(&mut src);
    assert_eq!(found.get(&18), Some(&(0, 5)));
    assert_eq!(src.get_i32(), 0);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v3_still_works() {
    let dir = temp_dir("p83", "v3");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = api_versions_v3_v4_body("volant-test", "0.1.0");
    let resp = rpc(
        &addr,
        encode_request_flexible(18, 3, 77, Some("flex-v3"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 77);
    assert_eq!(src.get_i16(), 0);
    let found = parse_flex_api_keys(&mut src);
    assert_eq!(found.get(&18), Some(&(0, 5)));
    assert_eq!(src.get_i32(), 0);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v0_still_classic() {
    let dir = temp_dir("p83", "v0");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 0, 7, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 7);
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32();
    assert!(n >= 10);
    let mut max18 = None;
    for _ in 0..n {
        let key = src.get_i16();
        let _min = src.get_i16();
        let max = src.get_i16();
        if key == 18 {
            max18 = Some(max);
        }
    }
    assert_eq!(max18, Some(5));
    assert_eq!(src.remaining(), 0); // no throttle on v0

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_versions_v6_unsupported_header_v0() {
    let dir = temp_dir("p83", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Beyond Kafka max 5. Flexible request header is fine; response header stays v0.
    let resp = rpc(
        &addr,
        encode_request_flexible(18, 6, 99, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    // ApiVersions response header is always v0 (no tag buffer after corr).
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion
    // Body is just the error code for the generic unsupported path.

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
