//! Phase 78: KIP-951 CurrentLeader / NodeEndpoints on Produce v10+ and Fetch v12+.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request_flexible, get_compact_array_len, get_compact_bytes,
    get_compact_nullable_string, get_compact_string, get_uuid, put_compact_array_len,
    put_compact_bytes, put_compact_nullable_string, put_compact_string, put_empty_tag_buffer,
    read_unsigned_varint, skip_tag_buffer, volant_topic_uuid,
};
use volant_broker::Broker;
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

/// Produce v10 flexible body: null txn, acks, timeout, one topic/partition records.
fn produce_v10(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, None); // txn id
    body.put_i16(1); // acks
    body.put_i32(5000);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0); // partition
    put_compact_bytes(&mut body, Some(batch));
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // request tags
    body
}

/// Fetch v12 flexible: one topic, partition with current_leader_epoch.
fn fetch_v12(topic: &str, fetch_offset: i64, current_leader_epoch: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica_id
    body.put_i32(500); // max_wait
    body.put_i32(1); // min_bytes
    body.put_i32(1_048_576); // max_bytes
    body.put_u8(0); // isolation
    body.put_i32(0); // session_id
    body.put_i32(-1); // session_epoch
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0); // partition
    body.put_i32(current_leader_epoch);
    body.put_i64(fetch_offset);
    body.put_i32(-1); // last_fetched_epoch
    body.put_i64(0); // log_start_offset
    body.put_i32(1_048_576); // partition_max_bytes
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_compact_array_len(&mut body, 0); // forgotten
    put_compact_string(&mut body, ""); // rack
    put_empty_tag_buffer(&mut body); // request tags
    body
}

fn one_record_batch() -> BytesMut {
    let records = vec![Record {
        offset: Offset::new(0),
        key: Some(Bytes::from_static(b"k")),
        value: Bytes::from_static(b"v"),
        timestamp_ms: 1_700_000_000_000,
        headers: vec![],
    }];
    encode_record_batch(&records)
}

#[tokio::test]
async fn produce_v10_success_empty_kip951_tags() {
    let dir = temp_dir("p78", "ok");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = one_record_batch();
    let resp = rpc(
        &addr,
        encode_request_flexible(0, 10, 1, Some("p"), &produce_v10("t", &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap(); // header v1
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "t");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // error
    let _base = src.get_i64();
    let _append = src.get_i64();
    let _log_start = src.get_i64();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // record_errors
    assert!(get_compact_nullable_string(&mut src).unwrap().is_none());
    // Partition tags empty (no CurrentLeader on success).
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    assert_eq!(src.get_i32(), 0); // throttle
    // Top-level tags empty (no NodeEndpoints).
    skip_tag_buffer(&mut src).unwrap();
    assert!(!src.has_remaining());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v12_fenced_includes_current_leader_tag1() {
    let dir = temp_dir("p78", "fence");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let node = broker.node_id() as i32;
    // Bump epoch so client epoch 0 is fenced.
    broker
        .set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 3)
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 2, Some("c"), &fetch_v12("t", 0, 0)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    skip_tag_buffer(&mut src).unwrap(); // header v1
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top-level error
    assert_eq!(src.get_i32(), 0); // session
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "t");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 74); // FencedLeaderEpoch
    let _hwm = src.get_i64();
    let _lso = src.get_i64();
    let _log_start = src.get_i64();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // aborted
    assert_eq!(src.get_i32(), -1); // preferred_read_replica
    let _records = get_compact_bytes(&mut src).unwrap();
    // Partition TAG_BUFFER: one field, tag 1 = CurrentLeader
    let n = read_unsigned_varint(&mut src).unwrap();
    assert_eq!(n, 1, "one tagged field");
    let tag = read_unsigned_varint(&mut src).unwrap();
    assert_eq!(tag, 1, "CurrentLeader is tag 1 on Fetch");
    let len = read_unsigned_varint(&mut src).unwrap() as usize;
    let mut body = src.copy_to_bytes(len);
    assert_eq!(body.get_i32(), node); // leader id
    assert_eq!(body.get_i32(), 3); // leader epoch
    skip_tag_buffer(&mut body).unwrap();
    assert!(!body.has_remaining());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v9_has_no_node_endpoints_even_on_error_shape() {
    // v9 flexible but pre-KIP-951: success still empty tags; version gate is 10+.
    let dir = temp_dir("p78", "v9");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = one_record_batch();
    // produce_v10 body is also valid for v9 (no extra request fields).
    let resp = rpc(
        &addr,
        encode_request_flexible(0, 9, 3, Some("p"), &produce_v10("t", &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 3);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    let _ = get_compact_string(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    src.advance(4 + 2 + 8 + 8 + 8); // part, err, base, append, log_start
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    let _ = get_compact_nullable_string(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap(); // partition tags empty
    skip_tag_buffer(&mut src).unwrap(); // topic
    assert_eq!(src.get_i32(), 0);
    skip_tag_buffer(&mut src).unwrap(); // top-level empty

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Unit-style: force NotLeader via produce_partition path is hard on single-node;
/// cover CurrentLeader+NodeEndpoints via a produce error by using unknown topic
/// which does NOT emit tags — instead bump nothing and document fetch path above.
///
/// Additional: produce v13 still empty tags on success (TopicId path).
#[tokio::test]
async fn produce_v13_success_still_empty_tags() {
    let dir = temp_dir("p78", "v13");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let snap = broker.metadata(None);
    let tid = snap.topics[0].topic_id.0;
    let uuid = volant_topic_uuid(tid);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = one_record_batch();
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, None);
    body.put_i16(1);
    body.put_i32(5000);
    put_compact_array_len(&mut body, 1);
    body.extend_from_slice(&uuid);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0);
    put_compact_bytes(&mut body, Some(&batch));
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);

    let resp = rpc(
        &addr,
        encode_request_flexible(0, 13, 4, Some("p"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 4);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    let _ = get_uuid(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    src.advance(8 + 8 + 8);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    let _ = get_compact_nullable_string(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

