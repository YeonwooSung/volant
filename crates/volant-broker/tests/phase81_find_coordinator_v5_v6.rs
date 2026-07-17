//! Phase 81: FindCoordinator v5–6 (wire-identical to v4 batch; no share groups).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// FindCoordinator v4+ body: key_type + compact keys + tags (v5/v6 identical).
fn find_coord_batch_body(key_type: i8, keys: &[&str]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i8(key_type);
    put_compact_array_len(&mut body, keys.len());
    for k in keys {
        put_compact_string(&mut body, k);
    }
    put_empty_tag_buffer(&mut body);
    body
}

fn parse_batch_success(src: &mut impl Buf, expected_keys: &[&str]) {
    assert_eq!(src.get_i32(), 0); // throttle always 0
    let n = get_compact_array_len(src).unwrap().unwrap();
    assert_eq!(n, expected_keys.len());
    for expected in expected_keys {
        let key = get_compact_string(src).unwrap();
        assert_eq!(key, *expected);
        let node = src.get_i32();
        let host = get_compact_string(src).unwrap();
        let port = src.get_i32();
        let err = src.get_i16();
        assert_eq!(err, 0);
        assert_ne!(err, 123); // never TRANSACTION_ABORTABLE
        assert_eq!(get_compact_nullable_string(src).unwrap(), None);
        skip_tag_buffer(src).unwrap();
        assert!(node >= 0 && !host.is_empty() && port > 0);
    }
    skip_tag_buffer(src).unwrap();
    assert_eq!(src.remaining(), 0);
}

#[tokio::test]
async fn api_versions_find_coordinator_max_6() {
    let dir = temp_dir("p81", "api");
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
    assert_eq!(found.get(&10), Some(&(0, 6))); // FindCoordinator
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn find_coordinator_v5_batch_group_and_txn() {
    let dir = temp_dir("p81", "v5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // v5 group keys
    let resp = rpc(
        &addr,
        encode_request_flexible(
            10,
            5,
            11,
            Some("fc"),
            &find_coord_batch_body(0, &["g1", "g2"]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap(); // response header v1
    parse_batch_success(&mut src, &["g1", "g2"]);

    // v5 transaction key
    let resp = rpc(
        &addr,
        encode_request_flexible(
            10,
            5,
            12,
            Some("fc"),
            &find_coord_batch_body(1, &["txn-app"]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    parse_batch_success(&mut src, &["txn-app"]);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn find_coordinator_v6_batch_and_share_rejected() {
    let dir = temp_dir("p81", "v6");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // v6 group batch success
    let resp = rpc(
        &addr,
        encode_request_flexible(
            10,
            6,
            21,
            Some("fc"),
            &find_coord_batch_body(0, &["share-not", "classic-g"]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 21);
    skip_tag_buffer(&mut src).unwrap();
    parse_batch_success(&mut src, &["share-not", "classic-g"]);

    // v6 share key_type (2) → InvalidRequest (no KIP-932)
    let resp = rpc(
        &addr,
        encode_request_flexible(
            10,
            6,
            22,
            Some("fc"),
            &find_coord_batch_body(2, &["g:tid:0"]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 22);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    // empty coordinators when key parse fails before keys are known
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n, 0);
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn find_coordinator_v4_still_works() {
    let dir = temp_dir("p81", "v4");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            10,
            4,
            31,
            Some("fc"),
            &find_coord_batch_body(0, &["legacy-g"]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 31);
    skip_tag_buffer(&mut src).unwrap();
    parse_batch_success(&mut src, &["legacy-g"]);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn find_coordinator_v7_unsupported() {
    let dir = temp_dir("p81", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(10, 7, 99, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 for flex versions
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
