//! Phase 35: Kafka DeleteRecords + Describe/Create/DeleteAcls on the shim.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, get_nullable_string, get_string, put_nullable_string, put_string,
};
use volant_broker::{
    AclEntry, AclOperation, AclPermission, Broker, ResourceType,
    CLUSTER_RESOURCE,
};
use volant_core::{PartitionId, TopicName};
use volant_storage::StorageConfig;

/// Grant Cluster Alter+Describe to any principal so Kafka ACL admin APIs work
/// after enforcement is enabled.
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

#[tokio::test]
async fn api_versions_includes_delete_records_and_acl_keys() {
    let dir = temp_dir("p35", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    src.advance(4 + 2); // corr + error
    let n = src.get_i32();
    let mut keys = Vec::new();
    for _ in 0..n {
        keys.push(src.get_i16());
        let _ = src.get_i16();
        let _ = src.get_i16();
    }
    for k in [21i16, 29, 30, 31] {
        assert!(keys.contains(&k), "missing api key {k}");
    }
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_records_returns_low_watermark() {
    let dir = temp_dir("p35", "delrec");
    // Small segments so delete_records can drop whole segments.
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        segment_size: 512,
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let topic = TopicName::new("events");
    let pid = PartitionId(0);
    for i in 0..80u64 {
        let payload = format!("msg-{i:04}-{}", "x".repeat(40));
        broker
            .produce_one(
                &topic,
                pid,
                volant_core::Message::from_value(payload),
            )
            .unwrap();
    }
    broker.flush(&topic, pid).unwrap();

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    body.put_i32(1); // topics
    put_string(&mut body, "events");
    body.put_i32(1); // partitions
    body.put_i32(0); // partition
    body.put_i64(20); // before offset
    body.put_i32(5000); // timeout
    let resp = rpc(&addr, encode_request(21, 0, 10, Some("admin"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10); // corr
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(get_string(&mut src).unwrap(), "events");
    assert_eq!(src.get_i32(), 1); // partitions
    assert_eq!(src.get_i32(), 0); // partition
    let low = src.get_i64();
    let err = src.get_i16();
    assert_eq!(err, 0, "delete records error");
    assert!(low >= 0);

    // Unknown topic
    let mut body2 = BytesMut::new();
    body2.put_i32(1);
    put_string(&mut body2, "missing");
    body2.put_i32(1);
    body2.put_i32(0);
    body2.put_i64(1);
    body2.put_i32(1000);
    let resp2 = rpc(&addr, encode_request(21, 1, 11, Some("admin"), &body2)).await;
    let mut s2 = resp2.freeze();
    s2.advance(4 + 4); // corr + throttle
    assert_eq!(s2.get_i32(), 1);
    assert_eq!(get_string(&mut s2).unwrap(), "missing");
    assert_eq!(s2.get_i32(), 1);
    assert_eq!(s2.get_i32(), 0);
    let _low2 = s2.get_i64();
    assert_eq!(s2.get_i16(), 3); // UNKNOWN_TOPIC_OR_PARTITION

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_describe_delete_acls_round_trip() {
    let dir = temp_dir("p35", "acls");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // First CreateAcls enables enforcement; seed Cluster admin so the rest of
    // the round-trip can run as kafka-anonymous.
    seed_cluster_admin(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreateAcls v1: Topic Read Allow for User:alice on "orders"
    let mut body = BytesMut::new();
    body.put_i32(1); // creations
    body.put_i8(2); // Topic
    put_string(&mut body, "orders");
    body.put_i8(3); // LITERAL
    put_string(&mut body, "User:alice");
    put_string(&mut body, "*");
    body.put_i8(3); // Read
    body.put_i8(3); // Allow
    let resp = rpc(&addr, encode_request(30, 1, 20, Some("admin"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0, "create acls error");
    let _ = get_nullable_string(&mut src).unwrap();

    // Stored as bare principal in Volant.
    let listed = broker
        .acls()
        .list(Some("alice"), Some(ResourceType::Topic), Some("orders"));
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].operation, AclOperation::Read);
    assert_eq!(listed[0].permission, AclPermission::Allow);
    assert!(broker.acls().is_enabled());

    // DescribeAcls v1: Topic + name "orders" (seed also has Cluster * ACLs)
    let mut dbody = BytesMut::new();
    dbody.put_i8(2); // Topic
    put_nullable_string(&mut dbody, Some("orders"));
    dbody.put_i8(3); // LITERAL
    put_nullable_string(&mut dbody, Some("User:alice"));
    put_nullable_string(&mut dbody, None);
    dbody.put_i8(1); // Any op
    dbody.put_i8(1); // Any perm
    let dresp = rpc(&addr, encode_request(29, 1, 21, Some("admin"), &dbody)).await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 21);
    assert_eq!(ds.get_i32(), 0); // throttle
    assert_eq!(ds.get_i16(), 0); // error
    let _ = get_nullable_string(&mut ds).unwrap();
    assert_eq!(ds.get_i32(), 1); // resources
    assert_eq!(ds.get_i8(), 2); // Topic
    assert_eq!(get_string(&mut ds).unwrap(), "orders");
    assert_eq!(ds.get_i8(), 3); // LITERAL
    assert_eq!(ds.get_i32(), 1); // acls
    assert_eq!(get_string(&mut ds).unwrap(), "User:alice");
    assert_eq!(get_string(&mut ds).unwrap(), "*");
    assert_eq!(ds.get_i8(), 3); // Read
    assert_eq!(ds.get_i8(), 3); // Allow

    // DeleteAcls v1: filter topic orders + principal alice
    let mut del = BytesMut::new();
    del.put_i32(1); // filters
    del.put_i8(2); // Topic
    put_nullable_string(&mut del, Some("orders"));
    del.put_i8(3); // LITERAL
    put_nullable_string(&mut del, Some("User:alice"));
    put_nullable_string(&mut del, None);
    del.put_i8(1); // Any op
    del.put_i8(1); // Any perm
    let delr = rpc(&addr, encode_request(31, 1, 22, Some("admin"), &del)).await;
    let mut dr = delr.freeze();
    assert_eq!(dr.get_i32(), 22);
    assert_eq!(dr.get_i32(), 0);
    assert_eq!(dr.get_i32(), 1); // filter results
    assert_eq!(dr.get_i16(), 0);
    let _ = get_nullable_string(&mut dr).unwrap();
    assert_eq!(dr.get_i32(), 1); // matching
    assert_eq!(dr.get_i16(), 0); // matching error
    let _ = get_nullable_string(&mut dr).unwrap();
    assert_eq!(dr.get_i8(), 2);
    assert_eq!(get_string(&mut dr).unwrap(), "orders");

    assert!(broker
        .acls()
        .list(Some("alice"), Some(ResourceType::Topic), Some("orders"))
        .is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_acls_v0_and_cluster_resource() {
    let dir = temp_dir("p35", "cluster-acl");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    seed_cluster_admin(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreateAcls v0: Cluster Describe Allow (no pattern type field)
    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(4); // Cluster
    put_string(&mut body, "kafka-cluster");
    put_string(&mut body, "User:bob");
    put_string(&mut body, "*");
    body.put_i8(8); // Describe
    body.put_i8(3); // Allow
    let resp = rpc(&addr, encode_request(30, 0, 30, Some("admin"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);

    let listed = broker
        .acls()
        .list(Some("bob"), Some(ResourceType::Cluster), Some("volant"));
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].resource, "volant");

    // Describe should emit kafka-cluster (v0 has no pattern type)
    let mut dbody = BytesMut::new();
    dbody.put_i8(4); // Cluster
    put_nullable_string(&mut dbody, None);
    put_nullable_string(&mut dbody, None);
    put_nullable_string(&mut dbody, None);
    dbody.put_i8(1);
    dbody.put_i8(1);
    let dresp = rpc(&addr, encode_request(29, 0, 31, Some("admin"), &dbody)).await;
    let mut ds = dresp.freeze();
    ds.advance(4 + 4); // corr + throttle
    assert_eq!(ds.get_i16(), 0);
    let _ = get_nullable_string(&mut ds).unwrap();
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(ds.get_i8(), 4);
    assert_eq!(get_string(&mut ds).unwrap(), "kafka-cluster");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn acl_admin_denied_without_cluster_alter() {
    let dir = temp_dir("p35", "acl-deny");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Enable ACLs with only a super-user "admin"; anonymous is denied.
    broker
        .configure_acls(true, None, vec!["admin".into()], "token".into())
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(2);
    put_string(&mut body, "t");
    body.put_i8(3);
    put_string(&mut body, "User:x");
    put_string(&mut body, "*");
    body.put_i8(3);
    body.put_i8(3);
    let resp = rpc(&addr, encode_request(30, 1, 40, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 31); // CLUSTER_AUTHORIZATION_FAILED

    // Describe also denied
    let mut dbody = BytesMut::new();
    dbody.put_i8(1);
    put_nullable_string(&mut dbody, None);
    dbody.put_i8(1);
    put_nullable_string(&mut dbody, None);
    put_nullable_string(&mut dbody, None);
    dbody.put_i8(1);
    dbody.put_i8(1);
    let dresp = rpc(&addr, encode_request(29, 1, 41, Some("c"), &dbody)).await;
    let mut ds = dresp.freeze();
    ds.advance(4 + 4);
    assert_eq!(ds.get_i16(), 31);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_records_acl_denied() {
    let dir = temp_dir("p35", "del-deny");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    broker
        .configure_acls(true, None, vec!["root".into()], "token".into())
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    body.put_i32(1);
    put_string(&mut body, "t");
    body.put_i32(1);
    body.put_i32(0);
    body.put_i64(1);
    body.put_i32(1000);
    let resp = rpc(&addr, encode_request(21, 0, 50, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "t");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let _ = src.get_i64();
    assert_eq!(src.get_i16(), 29); // TOPIC_AUTHORIZATION_FAILED

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
