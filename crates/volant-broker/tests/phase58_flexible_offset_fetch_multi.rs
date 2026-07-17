//! Phase 58: OffsetFetch multi-group flexible v8.

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
    AclEntry, AclOperation, AclPermission, Broker, ResourceType,
};
use volant_storage::StorageConfig;

fn commit_v8(group: &str, topic: &str, partition: i32, offset: i64, meta: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    body.put_i32(0);
    put_compact_string(&mut body, "");
    put_compact_nullable_string(&mut body, None);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i64(offset);
    body.put_i32(-1);
    put_compact_nullable_string(&mut body, Some(meta));
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

/// One group's topics request: None = all, Some(empty) = none, Some(list) = listed.
type GroupTopics<'a> = Option<&'a [(&'a str, &'a [i32])]>;

/// OffsetFetch v8 multi-group body.
fn fetch_v8_multi(groups: &[(&str, GroupTopics<'_>)], require_stable: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, groups.len());
    for (gid, topics) in groups {
        put_compact_string(&mut body, gid);
        match topics {
            None => body.put_u8(0), // null = all
            Some(list) => {
                put_compact_array_len(&mut body, list.len());
                for (topic, parts) in *list {
                    put_compact_string(&mut body, topic);
                    put_compact_array_len(&mut body, parts.len());
                    for p in *parts {
                        body.put_i32(*p);
                    }
                    put_empty_tag_buffer(&mut body);
                }
            }
        }
        put_empty_tag_buffer(&mut body); // group tags
    }
    body.put_u8(if require_stable { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_offset_fetch_max_10() {
    let dir = temp_dir("p58", "api");
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
async fn offset_fetch_v8_two_groups() {
    let dir = temp_dir("p58", "two");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 2).unwrap();
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let _ = rpc(
        &addr,
        encode_request_flexible(8, 8, 1, Some("c"), &commit_v8("g1", "orders", 0, 10, "a")),
    )
    .await;
    let _ = rpc(
        &addr,
        encode_request_flexible(8, 8, 2, Some("c"), &commit_v8("g1", "orders", 1, 20, "b")),
    )
    .await;
    let _ = rpc(
        &addr,
        encode_request_flexible(8, 8, 3, Some("c"), &commit_v8("g2", "events", 0, 5, "c")),
    )
    .await;

    let body = fetch_v8_multi(
        &[
            ("g1", Some(&[("orders", &[0i32, 1])])),
            ("g2", None), // all for g2
        ],
        false,
    );
    let resp = rpc(
        &addr,
        encode_request_flexible(9, 8, 10, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap(); // header v1
    assert_eq!(src.get_i32(), 0); // throttle

    let n_groups = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_groups, 2);

    // g1
    assert_eq!(get_compact_string(&mut src).unwrap(), "g1");
    let n_topics = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_topics, 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    let n_parts = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_parts, 2);
    // p0
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), 10);
    assert_eq!(src.get_i32(), -1);
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("a")
    );
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    // p1
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i64(), 20);
    assert_eq!(src.get_i32(), -1);
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("b")
    );
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    assert_eq!(src.get_i16(), 0); // group error
    skip_tag_buffer(&mut src).unwrap(); // group tags

    // g2
    assert_eq!(get_compact_string(&mut src).unwrap(), "g2");
    let n_topics = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_topics, 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    let n_parts = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_parts, 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), 5);
    assert_eq!(src.get_i32(), -1);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();

    skip_tag_buffer(&mut src).unwrap(); // top-level
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v8_empty_topics_none() {
    let dir = temp_dir("p58", "none");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let _ = rpc(
        &addr,
        encode_request_flexible(8, 8, 1, Some("c"), &commit_v8("g", "t", 0, 1, "")),
    )
    .await;

    // Empty topics array = fetch none
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, "g");
    put_compact_array_len(&mut body, 0); // empty topics (not null)
    put_empty_tag_buffer(&mut body);
    body.put_u8(0);
    put_empty_tag_buffer(&mut body);

    let resp = rpc(
        &addr,
        encode_request_flexible(9, 8, 5, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "g");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v8_acl_per_group() {
    let dir = temp_dir("p58", "acl");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    // Enable ACLs: allow only g-allow for the Kafka anonymous principal.
    broker
        .acls()
        .create(vec![AclEntry {
            principal: "kafka-anonymous".into(),
            resource_type: ResourceType::Group,
            resource: "g-allow".into(),
            operation: AclOperation::Read,
            permission: AclPermission::Allow,
        }])
        .expect("acl");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let _ = rpc(
        &addr,
        encode_request_flexible(
            8,
            8,
            1,
            Some("c"),
            &commit_v8("g-allow", "t", 0, 9, "ok"),
        ),
    )
    .await;

    let body = fetch_v8_multi(
        &[
            ("g-allow", Some(&[("t", &[0i32])])),
            ("g-deny", Some(&[("t", &[0i32])])),
        ],
        false,
    );
    let resp = rpc(
        &addr,
        encode_request_flexible(9, 8, 20, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 2);

    // g-allow ok
    assert_eq!(get_compact_string(&mut src).unwrap(), "g-allow");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "t");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), 9);
    let _ = src.get_i32();
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();

    // g-deny auth failed
    assert_eq!(get_compact_string(&mut src).unwrap(), "g-deny");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);
    assert_eq!(src.get_i16(), 30); // GROUP_AUTHORIZATION_FAILED
    skip_tag_buffer(&mut src).unwrap();

    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v7_still_single_group() {
    let dir = temp_dir("p58", "v7");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let _ = rpc(
        &addr,
        encode_request_flexible(8, 8, 1, Some("c"), &commit_v8("g", "t", 0, 3, "")),
    )
    .await;

    let mut body = BytesMut::new();
    put_compact_string(&mut body, "g");
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, "t");
    put_compact_array_len(&mut body, 1);
    body.put_i32(0);
    put_empty_tag_buffer(&mut body);
    body.put_u8(0); // require_stable
    put_empty_tag_buffer(&mut body);

    let resp = rpc(
        &addr,
        encode_request_flexible(9, 7, 7, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 7);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    // single-group shape: topics at top level, not Groups
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "t");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), 3);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v11_unsupported_header_v1() {
    let dir = temp_dir("p58", "v11");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(9, 11, 1, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
