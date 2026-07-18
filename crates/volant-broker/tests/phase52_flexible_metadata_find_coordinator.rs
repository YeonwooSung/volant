//! Phase 52: Flexible Metadata v9 + FindCoordinator v3–4 (KIP-482).

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

/// Metadata v9 request: null topics (all) + allow_auto + include ops flags + tags.
fn metadata_v9_all_body(include_ops: bool) -> BytesMut {
    let mut body = BytesMut::new();
    // null compact array = all topics
    put_unsigned_varint_zero(&mut body); // uvarint 0 = null
    body.put_u8(0); // allow_auto_topic_creation
    body.put_u8(if include_ops { 1 } else { 0 }); // cluster ops
    body.put_u8(if include_ops { 1 } else { 0 }); // topic ops
    put_empty_tag_buffer(&mut body);
    body
}

/// Metadata v9 request for a named topic.
fn metadata_v9_named_body(topic: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_empty_tag_buffer(&mut body); // topic tags
    body.put_u8(0); // allow_auto
    body.put_u8(0); // cluster ops
    body.put_u8(0); // topic ops
    put_empty_tag_buffer(&mut body);
    body
}

fn put_unsigned_varint_zero(dst: &mut BytesMut) {
    dst.put_u8(0);
}

/// FindCoordinator v3 body: compact key + key_type + tags.
fn find_coord_v3_body(key: &str, key_type: i8) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, key);
    body.put_i8(key_type);
    put_empty_tag_buffer(&mut body);
    body
}

/// FindCoordinator v4 body: key_type + compact keys + tags.
fn find_coord_v4_body(key_type: i8, keys: &[&str]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i8(key_type);
    put_compact_array_len(&mut body, keys.len());
    for k in keys {
        put_compact_string(&mut body, k);
    }
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_advertises_metadata_9_find_coord_6() {
    let dir = temp_dir("p52", "adv");
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
    let mut meta = None;
    let mut fc = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        if key == 3 {
            meta = Some((min, max));
        }
        if key == 10 {
            fc = Some((min, max));
        }
    }
    assert_eq!(meta, Some((0, 13))); // Phase 73 top-level ErrorCode
    assert_eq!(fc, Some((0, 6))); // Phase 81 FindCoordinator v5–6

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v9_flexible_roundtrip() {
    let dir = temp_dir("p52", "meta-v9");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = metadata_v9_all_body(false);
    let resp = rpc(
        &addr,
        encode_request_flexible(3, 9, 99, Some("flex-meta"), &body),
    )
    .await;
    let mut src = resp.freeze();
    // Response header v1: correlation + tag buffer
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();

    assert_eq!(src.get_i32(), 0); // throttle
    let n_brokers = get_compact_array_len(&mut src).unwrap().unwrap();
    assert!(n_brokers >= 1);
    for _ in 0..n_brokers {
        let _node = src.get_i32();
        let host = get_compact_string(&mut src).unwrap();
        let port = src.get_i32();
        let _rack = get_compact_nullable_string(&mut src).unwrap();
        skip_tag_buffer(&mut src).unwrap();
        assert!(!host.is_empty());
        assert!(port > 0);
    }
    let cluster_id = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(cluster_id.as_deref(), Some("volant"));
    let _controller = src.get_i32();

    let n_topics = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_topics, 1);
    assert_eq!(src.get_i16(), 0); // error
    let name = get_compact_string(&mut src).unwrap();
    assert_eq!(name, "orders");
    assert_eq!(src.get_u8(), 0); // is_internal
    let n_parts = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_parts, 2);
    for i in 0..n_parts {
        assert_eq!(src.get_i16(), 0);
        assert_eq!(src.get_i32(), i as i32);
        let _leader = src.get_i32();
        // Phase 87: live leader epoch (0 at create).
        assert_eq!(src.get_i32(), 0); // leader_epoch
        let n_rep = get_compact_array_len(&mut src).unwrap().unwrap();
        for _ in 0..n_rep {
            let _ = src.get_i32();
        }
        let n_isr = get_compact_array_len(&mut src).unwrap().unwrap();
        for _ in 0..n_isr {
            let _ = src.get_i32();
        }
        let n_off = get_compact_array_len(&mut src).unwrap().unwrap();
        assert_eq!(n_off, 0);
        skip_tag_buffer(&mut src).unwrap();
    }
    let _topic_ops = src.get_i32();
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    let _cluster_ops = src.get_i32();
    skip_tag_buffer(&mut src).unwrap(); // top-level
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v9_named_topic() {
    let dir = temp_dir("p52", "meta-named");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("payments", 1).unwrap();
    broker.create_topic("other", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = metadata_v9_named_body("payments");
    let resp = rpc(
        &addr,
        encode_request_flexible(3, 9, 7, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 7);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    let n_brokers = get_compact_array_len(&mut src).unwrap().unwrap();
    for _ in 0..n_brokers {
        let _ = src.get_i32();
        let _ = get_compact_string(&mut src).unwrap();
        let _ = src.get_i32();
        let _ = get_compact_nullable_string(&mut src).unwrap();
        skip_tag_buffer(&mut src).unwrap();
    }
    let _ = get_compact_nullable_string(&mut src).unwrap();
    let _ = src.get_i32();
    let n_topics = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_topics, 1);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_string(&mut src).unwrap(), "payments");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v8_still_classic() {
    let dir = temp_dir("p52", "meta-v8");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // classic null topics = all
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_u8(0); // allow_auto
    body.put_u8(0);
    body.put_u8(0);
    let resp = rpc(&addr, encode_request(3, 8, 5, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5); // corr only (header v0)
    assert_eq!(src.get_i32(), 0); // throttle
    let n_brokers = src.get_i32();
    assert!(n_brokers >= 1);
    // classic string host
    let _node = src.get_i32();
    let host_len = src.get_i16();
    assert!(host_len > 0);
    src.advance(host_len as usize);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn find_coordinator_v3_flexible_roundtrip() {
    let dir = temp_dir("p52", "fc-v3");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = find_coord_v3_body("my-group", 0);
    let resp = rpc(
        &addr,
        encode_request_flexible(10, 3, 11, Some("fc"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap(); // response header v1
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    let node = src.get_i32();
    let host = get_compact_string(&mut src).unwrap();
    let port = src.get_i32();
    skip_tag_buffer(&mut src).unwrap();
    assert!(node >= 0);
    assert!(!host.is_empty());
    assert!(port > 0);
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn find_coordinator_v4_batch() {
    let dir = temp_dir("p52", "fc-v4");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = find_coord_v4_body(0, &["g1", "g2"]);
    let resp = rpc(
        &addr,
        encode_request_flexible(10, 4, 12, Some("fc"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n, 2);
    let mut keys = Vec::new();
    for _ in 0..n {
        let key = get_compact_string(&mut src).unwrap();
        let node = src.get_i32();
        let host = get_compact_string(&mut src).unwrap();
        let port = src.get_i32();
        assert_eq!(src.get_i16(), 0);
        assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
        skip_tag_buffer(&mut src).unwrap();
        assert!(node >= 0 && !host.is_empty() && port > 0);
        keys.push(key);
    }
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(keys, vec!["g1".to_string(), "g2".to_string()]);
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn find_coordinator_v2_still_classic() {
    let dir = temp_dir("p52", "fc-v2");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    // classic string
    let k = b"classic-group";
    body.put_i16(k.len() as i16);
    body.extend_from_slice(k);
    body.put_i8(0);
    let resp = rpc(&addr, encode_request(10, 2, 3, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 3); // header v0 only
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0);
    // classic nullable error_message
    let msg_len = src.get_i16();
    assert_eq!(msg_len, -1);
    let _node = src.get_i32();
    let host_len = src.get_i16();
    assert!(host_len > 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_v14_unsupported() {
    let dir = temp_dir("p52", "meta-v14");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // v14 not handled; request version ≥9 still gets response header v1.
    let resp = rpc(
        &addr,
        encode_request_flexible(3, 14, 1, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap(); // response header v1
    assert_eq!(src.get_i16(), 35); // UNSUPPORTED_VERSION

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn find_coordinator_v7_unsupported() {
    let dir = temp_dir("p52", "fc-v7");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // v7 not handled; version ≥3 still uses response header v1.
    let resp = rpc(
        &addr,
        encode_request_flexible(10, 7, 1, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35); // UNSUPPORTED_VERSION

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
