//! Phase 80: CreatePartitions v3 (wire-identical to v2; KIP-599 quota never emitted).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    put_unsigned_varint, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::TopicName;
use volant_storage::StorageConfig;

fn create_topics_v5(name: &str, partitions: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    body.put_i32(partitions);
    body.put_i16(-1); // rf
    put_compact_array_len(&mut body, 0); // assignments
    put_compact_array_len(&mut body, 0); // configs
    put_empty_tag_buffer(&mut body);
    body.put_i32(5000);
    body.put_u8(0); // validate_only
    put_empty_tag_buffer(&mut body);
    body
}

/// CreatePartitions flexible body (v2 and v3 share identical wire).
fn create_partitions_flex(name: &str, count: i32, validate_only: bool) -> BytesMut {
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
async fn api_versions_create_partitions_max_3() {
    let dir = temp_dir("p80", "api");
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
    assert_eq!(found.get(&37), Some(&(0, 3))); // CreatePartitions
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_partitions_v3_grow_and_error_message() {
    let dir = temp_dir("p80", "v3");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Create topic with 2 partitions (CreateTopics v5)
    let resp = rpc(
        &addr,
        encode_request_flexible(19, 5, 10, Some("a"), &create_topics_v5("p80-t", 2)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "p80-t");
    assert_eq!(src.get_i16(), 0);

    // CreatePartitions v3: grow to 4
    let resp = rpc(
        &addr,
        encode_request_flexible(
            37,
            3,
            11,
            Some("a"),
            &create_partitions_flex("p80-t", 4, false),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap(); // response header v1
    assert_eq!(src.get_i32(), 0); // throttle always 0 (no quotas)
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "p80-t");
    assert_eq!(src.get_i16(), 0); // no error; never THROTTLING_QUOTA_EXCEEDED
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    let meta = broker.metadata(Some(&[TopicName::new("p80-t")]));
    assert_eq!(meta.topics[0].partitions.len(), 4);

    // CreatePartitions v3: shrink rejected with ErrorMessage
    let resp = rpc(
        &addr,
        encode_request_flexible(
            37,
            3,
            12,
            Some("a"),
            &create_partitions_flex("p80-t", 1, false),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "p80-t");
    assert_ne!(src.get_i16(), 0); // InvalidPartitions
    let msg = get_compact_nullable_string(&mut src).unwrap();
    assert!(msg.is_some());
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    // validate_only v3: dry-run grow succeeds without applying
    let resp = rpc(
        &addr,
        encode_request_flexible(
            37,
            3,
            13,
            Some("a"),
            &create_partitions_flex("p80-t", 8, true),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 13);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "p80-t");
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    let meta = broker.metadata(Some(&[TopicName::new("p80-t")]));
    assert_eq!(meta.topics[0].partitions.len(), 4); // unchanged

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_partitions_v2_still_works() {
    let dir = temp_dir("p80", "v2");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(19, 5, 1, Some("a"), &create_topics_v5("p80-v2", 1)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    src.advance(4); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    let _ = get_compact_string(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);

    let resp = rpc(
        &addr,
        encode_request_flexible(
            37,
            2,
            2,
            Some("a"),
            &create_partitions_flex("p80-v2", 3, false),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "p80-v2");
    assert_eq!(src.get_i16(), 0);

    let meta = broker.metadata(Some(&[TopicName::new("p80-v2")]));
    assert_eq!(meta.topics[0].partitions.len(), 3);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_partitions_v4_unsupported() {
    let dir = temp_dir("p80", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(37, 4, 99, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 for flex versions
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
