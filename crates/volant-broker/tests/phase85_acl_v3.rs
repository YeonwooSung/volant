//! Phase 85: Describe/Create/DeleteAcls v3 (User resource type; Kafka max).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_nullable_string, put_compact_string,
    put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::{
    AclEntry, AclOperation, AclPermission, Broker, ResourceType, CLUSTER_RESOURCE,
};
use volant_storage::StorageConfig;

fn seed_cluster_admin(broker: &Broker) {
    broker
        .acls()
        .create(vec![
            AclEntry {
                principal: "*".into(),
                resource_type: ResourceType::Cluster,
                resource: CLUSTER_RESOURCE.into(),
                operation: AclOperation::Alter,
                permission: AclPermission::Allow,
            },
            AclEntry {
                principal: "*".into(),
                resource_type: ResourceType::Cluster,
                resource: CLUSTER_RESOURCE.into(),
                operation: AclOperation::Describe,
                permission: AclPermission::Allow,
            },
        ])
        .unwrap();
}

/// CreateAcls flexible body for one binding (v2 and v3 share framing).
fn create_acls_flex(resource_type: i8, resource: &str, principal: &str, op: i8, perm: i8) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    body.put_i8(resource_type);
    put_compact_string(&mut body, resource);
    body.put_i8(3); // LITERAL
    put_compact_string(&mut body, principal);
    put_compact_string(&mut body, "*");
    body.put_i8(op);
    body.put_i8(perm);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn describe_acls_flex(resource_type: i8, resource: Option<&str>) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i8(resource_type);
    put_compact_nullable_string(&mut body, resource);
    body.put_i8(3); // LITERAL
    put_compact_nullable_string(&mut body, None);
    put_compact_nullable_string(&mut body, None);
    body.put_i8(1); // ANY op
    body.put_i8(1); // ANY perm
    put_empty_tag_buffer(&mut body);
    body
}

fn delete_acls_flex(resource_type: i8, resource: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    body.put_i8(resource_type);
    put_compact_nullable_string(&mut body, Some(resource));
    body.put_i8(3);
    put_compact_nullable_string(&mut body, None);
    put_compact_nullable_string(&mut body, None);
    body.put_i8(1);
    body.put_i8(1);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_acl_admin_max_3() {
    let dir = temp_dir("p85", "api");
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
    assert_eq!(found.get(&29), Some(&(0, 3))); // DescribeAcls
    assert_eq!(found.get(&30), Some(&(0, 3))); // CreateAcls
    assert_eq!(found.get(&31), Some(&(0, 3))); // DeleteAcls
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn acls_v3_user_resource_create_describe_delete_roundtrip() {
    let dir = temp_dir("p85", "user");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    seed_cluster_admin(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreateAcls v3 — User resource Describe Allow for User:admin on "alice"
    // Kafka ResourceType User = 7; Describe op = 8; Allow = 3.
    let resp = rpc(
        &addr,
        encode_request_flexible(
            30,
            3,
            20,
            Some("a"),
            &create_acls_flex(7, "alice", "User:admin", 8, 3),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap(); // header v1
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    // Stored as ResourceType::User in the durable ACL store.
    let listed = broker.acls().list(None, Some(ResourceType::User), Some("alice"));
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].resource_type, ResourceType::User);
    assert_eq!(listed[0].principal, "admin");
    assert_eq!(listed[0].operation, AclOperation::Describe);

    // DescribeAcls v3 filter User + name alice
    let resp = rpc(
        &addr,
        encode_request_flexible(29, 3, 21, Some("a"), &describe_acls_flex(7, Some("alice"))),
    )
    .await;
    let mut ds = resp.freeze();
    assert_eq!(ds.get_i32(), 21);
    skip_tag_buffer(&mut ds).unwrap();
    assert_eq!(ds.get_i32(), 0);
    assert_eq!(ds.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut ds).unwrap(), None);
    assert_eq!(get_compact_array_len(&mut ds).unwrap(), Some(1));
    assert_eq!(ds.get_i8(), 7); // User
    assert_eq!(get_compact_string(&mut ds).unwrap(), "alice");
    assert_eq!(ds.get_i8(), 3); // LITERAL
    assert_eq!(get_compact_array_len(&mut ds).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut ds).unwrap(), "User:admin");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "*");
    assert_eq!(ds.get_i8(), 8); // Describe
    assert_eq!(ds.get_i8(), 3); // Allow
    skip_tag_buffer(&mut ds).unwrap();
    skip_tag_buffer(&mut ds).unwrap();
    skip_tag_buffer(&mut ds).unwrap();

    // DeleteAcls v3
    let resp = rpc(
        &addr,
        encode_request_flexible(31, 3, 22, Some("a"), &delete_acls_flex(7, "alice")),
    )
    .await;
    let mut dr = resp.freeze();
    assert_eq!(dr.get_i32(), 22);
    skip_tag_buffer(&mut dr).unwrap();
    assert_eq!(dr.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut dr).unwrap(), Some(1));
    assert_eq!(dr.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut dr).unwrap(), None);
    assert_eq!(get_compact_array_len(&mut dr).unwrap(), Some(1));
    assert_eq!(dr.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut dr).unwrap(), None);
    assert_eq!(dr.get_i8(), 7); // User
    assert_eq!(get_compact_string(&mut dr).unwrap(), "alice");
    assert_eq!(dr.get_i8(), 3);
    assert_eq!(get_compact_string(&mut dr).unwrap(), "User:admin");
    assert_eq!(get_compact_string(&mut dr).unwrap(), "*");
    assert_eq!(dr.get_i8(), 8);
    assert_eq!(dr.get_i8(), 3);
    skip_tag_buffer(&mut dr).unwrap();
    skip_tag_buffer(&mut dr).unwrap();
    skip_tag_buffer(&mut dr).unwrap();

    assert!(broker
        .acls()
        .list(None, Some(ResourceType::User), Some("alice"))
        .is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn acls_v2_rejects_user_resource_type() {
    let dir = temp_dir("p85", "v2user");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    seed_cluster_admin(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreateAcls v2 with User resource type must fail InvalidRequest (42).
    let resp = rpc(
        &addr,
        encode_request_flexible(
            30,
            2,
            30,
            Some("a"),
            &create_acls_flex(7, "alice", "User:admin", 8, 3),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 30);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 42); // InvalidRequest
    let msg = get_compact_nullable_string(&mut src).unwrap();
    assert!(
        msg.as_deref()
            .map(|m| m.contains("User") || m.contains("v3"))
            .unwrap_or(false),
        "unexpected message: {msg:?}"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn acls_v3_topic_still_works() {
    let dir = temp_dir("p85", "topic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    seed_cluster_admin(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreateAcls v3 Topic Read Allow — same framing as v2 for non-User types.
    let resp = rpc(
        &addr,
        encode_request_flexible(
            30,
            3,
            40,
            Some("a"),
            &create_acls_flex(2, "orders", "User:alice", 3, 3),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 40);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0);

    let resp = rpc(
        &addr,
        encode_request_flexible(29, 3, 41, Some("a"), &describe_acls_flex(2, Some("orders"))),
    )
    .await;
    let mut ds = resp.freeze();
    assert_eq!(ds.get_i32(), 41);
    skip_tag_buffer(&mut ds).unwrap();
    assert_eq!(ds.get_i32(), 0);
    assert_eq!(ds.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut ds).unwrap(), None);
    assert_eq!(get_compact_array_len(&mut ds).unwrap(), Some(1));
    assert_eq!(ds.get_i8(), 2); // Topic
    assert_eq!(get_compact_string(&mut ds).unwrap(), "orders");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn acls_v4_unsupported_version_header_v1() {
    let dir = temp_dir("p85", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for (api, corr) in [(29i16, 50i32), (30, 51), (31, 52)] {
        let resp = rpc(
            &addr,
            encode_request_flexible(api, 4, corr, Some("c"), &[]),
        )
        .await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), corr);
        skip_tag_buffer(&mut src).unwrap(); // header v1 for unsupported flex
        assert_eq!(src.get_i16(), 35, "api={api}"); // UnsupportedVersion
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
