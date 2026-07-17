//! Phase 64: Flexible DeleteRecords v2 + Describe/Create/DeleteAcls v2.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_nullable_string, put_compact_string,
    put_empty_tag_buffer, put_string, skip_tag_buffer,
};
use volant_broker::{
    serve_kafka_listener, AclEntry, AclOperation, AclPermission, Broker, ResourceType,
    CLUSTER_RESOURCE,
};
use volant_core::{PartitionId, TopicName};
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

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p64-{label}-{}-{}",
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

fn delete_records_v2(topic: &str, partition: i32, offset: i64) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i64(offset);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body.put_i32(5000); // timeout
    put_empty_tag_buffer(&mut body);
    body
}

fn create_acls_v2(topic: &str, principal: &str, op: i8, perm: i8) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    body.put_i8(2); // Topic
    put_compact_string(&mut body, topic);
    body.put_i8(3); // LITERAL
    put_compact_string(&mut body, principal);
    put_compact_string(&mut body, "*");
    body.put_i8(op);
    body.put_i8(perm);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn describe_acls_v2(topic: Option<&str>) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i8(2); // Topic
    put_compact_nullable_string(&mut body, topic);
    body.put_i8(3); // LITERAL
    put_compact_nullable_string(&mut body, None); // principal any
    put_compact_nullable_string(&mut body, None); // host any
    body.put_i8(1); // ANY op
    body.put_i8(1); // ANY perm
    put_empty_tag_buffer(&mut body);
    body
}

fn delete_acls_v2(topic: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    body.put_i8(2);
    put_compact_nullable_string(&mut body, Some(topic));
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
async fn api_versions_delete_records_acl_flex_maxes() {
    let dir = temp_dir("api");
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
    assert_eq!(found.get(&21), Some(&(0, 2))); // DeleteRecords
    assert_eq!(found.get(&29), Some(&(0, 2))); // DescribeAcls
    assert_eq!(found.get(&30), Some(&(0, 2))); // CreateAcls
    assert_eq!(found.get(&31), Some(&(0, 2))); // DeleteAcls
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_records_v2_flexible() {
    let dir = temp_dir("delrec");
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
            .produce_one(&topic, pid, volant_core::Message::from_value(payload))
            .unwrap();
    }
    broker.flush(&topic, pid).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(21, 2, 10, Some("a"), &delete_records_v2("events", 0, 20)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    let low = src.get_i64();
    assert_eq!(src.get_i16(), 0);
    assert!(low >= 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn acls_flexible_create_describe_delete_roundtrip() {
    let dir = temp_dir("acls");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    seed_cluster_admin(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreateAcls v2 — Topic Read Allow for User:alice on orders
    let resp = rpc(
        &addr,
        encode_request_flexible(
            30,
            2,
            20,
            Some("a"),
            &create_acls_v2("orders", "User:alice", 3, 3),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    // DescribeAcls v2
    let resp = rpc(
        &addr,
        encode_request_flexible(29, 2, 21, Some("a"), &describe_acls_v2(Some("orders"))),
    )
    .await;
    let mut ds = resp.freeze();
    assert_eq!(ds.get_i32(), 21);
    skip_tag_buffer(&mut ds).unwrap();
    assert_eq!(ds.get_i32(), 0);
    assert_eq!(ds.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut ds).unwrap(), None);
    assert_eq!(get_compact_array_len(&mut ds).unwrap(), Some(1));
    assert_eq!(ds.get_i8(), 2); // Topic
    assert_eq!(get_compact_string(&mut ds).unwrap(), "orders");
    assert_eq!(ds.get_i8(), 3); // LITERAL
    assert_eq!(get_compact_array_len(&mut ds).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut ds).unwrap(), "User:alice");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "*");
    assert_eq!(ds.get_i8(), 3); // Read
    assert_eq!(ds.get_i8(), 3); // Allow
    skip_tag_buffer(&mut ds).unwrap();
    skip_tag_buffer(&mut ds).unwrap();
    skip_tag_buffer(&mut ds).unwrap();

    // DeleteAcls v2
    let resp = rpc(
        &addr,
        encode_request_flexible(31, 2, 22, Some("a"), &delete_acls_v2("orders")),
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
    assert_eq!(dr.get_i8(), 2);
    assert_eq!(get_compact_string(&mut dr).unwrap(), "orders");
    assert_eq!(dr.get_i8(), 3);
    assert_eq!(get_compact_string(&mut dr).unwrap(), "User:alice");
    assert_eq!(get_compact_string(&mut dr).unwrap(), "*");
    assert_eq!(dr.get_i8(), 3);
    assert_eq!(dr.get_i8(), 3);
    skip_tag_buffer(&mut dr).unwrap();
    skip_tag_buffer(&mut dr).unwrap();
    skip_tag_buffer(&mut dr).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn classic_create_acls_still_works() {
    let dir = temp_dir("classic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    seed_cluster_admin(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(2);
    put_string(&mut body, "t");
    body.put_i8(3);
    put_string(&mut body, "User:bob");
    put_string(&mut body, "*");
    body.put_i8(3);
    body.put_i8(3);
    let resp = rpc(&addr, encode_request(30, 1, 1, Some("a"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_versions_use_header_v1() {
    let dir = temp_dir("unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for (api, ver, corr) in [(21i16, 3i16, 40i32), (29, 3, 41), (30, 3, 42), (31, 3, 43)] {
        let resp = rpc(
            &addr,
            encode_request_flexible(api, ver, corr, Some("c"), &[]),
        )
        .await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), corr);
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.get_i16(), 35, "api={api}");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
