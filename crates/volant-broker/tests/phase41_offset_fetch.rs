//! Phase 41: Kafka OffsetFetch classic v0–5 on the shim.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{encode_request, get_string, put_string};
use volant_broker::{
    AclEntry, AclOperation, AclPermission, Broker, ResourceType,
};
use volant_storage::StorageConfig;

/// OffsetCommit v0: group, [topic [partition, offset, metadata]] (no generation).
fn commit_v0(group: &str, topic: &str, partition: i32, offset: i64, meta: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(offset);
    put_string(&mut body, meta);
    body
}

/// OffsetFetch body.
/// topics: None → null (all, v2+); Some([]) → empty; Some(list) → listed.
fn fetch_body(version: i16, group: &str, topics: Option<&[(&str, &[i32])]>) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    match topics {
        None if version >= 2 => body.put_i32(-1),
        None => body.put_i32(0), // v0–1 empty = all
        Some(list) => {
            body.put_i32(list.len() as i32);
            for (topic, parts) in list {
                put_string(&mut body, topic);
                body.put_i32(parts.len() as i32);
                for p in *parts {
                    body.put_i32(*p);
                }
            }
        }
    }
    body
}

#[tokio::test]
async fn api_versions_offset_fetch_max_5() {
    let dir = temp_dir("p41", "api");
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
        if key == 9 {
            found = Some((min_v, max_v));
        }
    }
    assert_eq!(found, Some((0, 10))); // Phase 72 TopicId
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v5_throttle_epoch_and_top_error() {
    let dir = temp_dir("p41", "v5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 2).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let cresp = rpc(
        &addr,
        encode_request(8, 0, 1, Some("c"), &commit_v0("g1", "orders", 0, 42, "m")),
    )
    .await;
    let mut cs = cresp.freeze();
    assert_eq!(cs.get_i32(), 1);
    assert_eq!(cs.get_i32(), 1);
    let _ = get_string(&mut cs).unwrap();
    assert_eq!(cs.get_i32(), 1);
    assert_eq!(cs.get_i32(), 0);
    assert_eq!(cs.get_i16(), 0);

    let body = fetch_body(5, "g1", Some(&[("orders", &[0i32, 1])]));
    let resp = rpc(&addr, encode_request(9, 5, 10, Some("f"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_i32(), 2);
    // p0
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), 42);
    assert_eq!(src.get_i32(), -1); // committed_leader_epoch
    assert_eq!(get_string(&mut src).unwrap(), "m");
    assert_eq!(src.get_i16(), 0);
    // p1 unknown
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i64(), -1);
    assert_eq!(src.get_i32(), -1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i16(), 0); // top-level error

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v2_null_topics_all() {
    let dir = temp_dir("p41", "null");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    rpc(
        &addr,
        encode_request(8, 0, 1, Some("c"), &commit_v0("g1", "orders", 0, 7, "")),
    )
    .await;

    // null topics → all commits
    let body = fetch_body(2, "g1", None);
    let resp = rpc(&addr, encode_request(9, 2, 11, Some("f"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    // no throttle in v2
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), 7);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i16(), 0); // top-level

    // empty topics → none
    let body = fetch_body(2, "g1", Some(&[]));
    let resp = rpc(&addr, encode_request(9, 2, 12, Some("f"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    assert_eq!(src.get_i32(), 0); // empty topics
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v3_throttle() {
    let dir = temp_dir("p41", "v3");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    rpc(
        &addr,
        encode_request(8, 0, 1, Some("c"), &commit_v0("g1", "orders", 0, 5, "x")),
    )
    .await;

    let body = fetch_body(3, "g1", Some(&[("orders", &[0i32])]));
    let resp = rpc(&addr, encode_request(9, 3, 13, Some("f"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 13);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), 5);
    assert_eq!(get_string(&mut src).unwrap(), "x");
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_acl_denied_v5() {
    let dir = temp_dir("p41", "acl");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker
        .acls()
        .create(vec![AclEntry {
            principal: "alice".into(),
            resource_type: ResourceType::Group,
            resource: "g1".into(),
            operation: AclOperation::Read,
            permission: AclPermission::Allow,
        }])
        .expect("acl");

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let body = fetch_body(5, "g1", Some(&[("orders", &[0i32])]));
    let resp = rpc(&addr, encode_request(9, 5, 14, Some("f"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 14);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 0); // empty topics
    assert_eq!(src.get_i16(), 30); // GROUP_AUTHORIZATION_FAILED

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
