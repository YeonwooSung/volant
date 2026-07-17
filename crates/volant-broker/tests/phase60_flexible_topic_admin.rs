//! Phase 60: Flexible CreateTopics v5 / DeleteTopics v4 / CreatePartitions v2.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, get_nullable_string, get_string, put_compact_array_len, put_compact_string,
    put_empty_tag_buffer, put_string, put_unsigned_varint, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::TopicName;
use volant_storage::StorageConfig;

fn create_topics_v5(name: &str, partitions: i32, validate_only: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    body.put_i32(partitions);
    body.put_i16(-1); // rf
    put_compact_array_len(&mut body, 0); // assignments
    put_compact_array_len(&mut body, 0); // configs
    put_empty_tag_buffer(&mut body); // topic tags
    body.put_i32(5000); // timeout
    body.put_u8(if validate_only { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

fn delete_topics_v4(name: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    body.put_i32(5000);
    put_empty_tag_buffer(&mut body);
    body
}

fn create_partitions_v2(name: &str, count: i32, validate_only: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    body.put_i32(count);
    put_unsigned_varint(&mut body, 0); // null assignments
    put_empty_tag_buffer(&mut body);
    body.put_i32(5000);
    body.put_u8(if validate_only { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_topic_admin_flex_maxes() {
    let dir = temp_dir("p60", "api");
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
    assert_eq!(found.get(&19), Some(&(0, 7))); // Phase 69 TopicId
    assert_eq!(found.get(&20), Some(&(0, 6))); // Phase 69 TopicId
    assert_eq!(found.get(&37), Some(&(0, 2)));
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_delete_partitions_flexible_roundtrip() {
    let dir = temp_dir("p60", "roundtrip");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreateTopics v5
    let resp = rpc(
        &addr,
        encode_request_flexible(19, 5, 10, Some("a"), &create_topics_v5("flex-t", 2, false)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "flex-t");
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(src.get_i32(), 2); // num partitions
    assert_eq!(src.get_i16(), 1); // rf placeholder
    // null configs
    assert_eq!(get_compact_array_len(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    let meta = broker.metadata(Some(&[TopicName::new("flex-t")]));
    assert_eq!(meta.topics[0].partitions.len(), 2);

    // CreatePartitions v2: grow to 4
    let resp = rpc(
        &addr,
        encode_request_flexible(
            37,
            2,
            11,
            Some("a"),
            &create_partitions_v2("flex-t", 4, false),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "flex-t");
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    let meta = broker.metadata(Some(&[TopicName::new("flex-t")]));
    assert_eq!(meta.topics[0].partitions.len(), 4);

    // DeleteTopics v4
    let resp = rpc(
        &addr,
        encode_request_flexible(20, 4, 12, Some("a"), &delete_topics_v4("flex-t")),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "flex-t");
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    assert!(broker
        .metadata(Some(&[TopicName::new("flex-t")]))
        .topics
        .is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_topics_v5_validate_only_and_default_partitions() {
    let dir = temp_dir("p60", "vo");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(19, 5, 1, Some("a"), &create_topics_v5("dry", -1, true)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "dry");
    assert_eq!(src.get_i16(), 0);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1); // default partitions
    assert_eq!(src.get_i16(), 1);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    assert!(broker
        .metadata(Some(&[TopicName::new("dry")]))
        .topics
        .is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn classic_topic_admin_still_works() {
    let dir = temp_dir("p60", "classic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    body.put_i32(1);
    put_string(&mut body, "c-t");
    body.put_i32(1);
    body.put_i16(-1);
    body.put_i32(0);
    body.put_i32(0);
    body.put_i32(5000);
    body.put_u8(0);
    let resp = rpc(&addr, encode_request(19, 4, 10, Some("a"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // no header tags
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "c-t");
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_nullable_string(&mut src).unwrap(), None);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_versions_use_header_v1() {
    let dir = temp_dir("p60", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreateTopics v8 / DeleteTopics v7 / CreatePartitions v3 unsupported.
    for (api, ver, corr) in [(19i16, 8i16, 1i32), (20, 7, 2), (37, 3, 3)] {
        let resp = rpc(
            &addr,
            encode_request_flexible(api, ver, corr, Some("c"), &[]),
        )
        .await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), corr);
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.get_i16(), 35, "api={api} ver={ver}");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
