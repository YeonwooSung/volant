//! Phase 54: Flexible Fetch v12 (KIP-482 compact + response header v1).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, encode_request_flexible, get_bytes, get_compact_array_len,
    get_compact_bytes, get_compact_string, get_string, put_bytes, put_compact_array_len,
    put_compact_string, put_empty_tag_buffer, put_nullable_string, put_string, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::{Offset, Record};
use volant_storage::StorageConfig;

fn sample_records(value: &'static [u8]) -> Vec<Record> {
    vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(value),
        timestamp_ms: 1_700_000_000_000,
        headers: vec![],
    }]
}

fn produce_body_v3(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, None);
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

/// Fetch v12 flexible body for one topic/partition.
fn fetch_v12_body(topic: &str, fetch_offset: i64, session_id: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica_id
    body.put_i32(0); // max_wait
    body.put_i32(1); // min_bytes
    body.put_i32(1_048_576); // max_bytes
    body.put_u8(0); // isolation
    body.put_i32(session_id);
    body.put_i32(-1); // session_epoch
    put_compact_array_len(&mut body, 1); // topics
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1); // partitions
    body.put_i32(0); // partition
    body.put_i32(-1); // current_leader_epoch
    body.put_i64(fetch_offset);
    body.put_i32(-1); // last_fetched_epoch
    body.put_i64(-1); // log_start_offset
    body.put_i32(1_000_000); // partition_max_bytes
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_compact_array_len(&mut body, 0); // forgotten
    put_compact_string(&mut body, ""); // rack_id
    put_empty_tag_buffer(&mut body); // top-level (ClusterId tagged optional)
    body
}

/// Classic fetch body (v11).
fn fetch_v11_body(topic: &str, fetch_offset: i64) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(0);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(0);
    body.put_i32(0);
    body.put_i32(-1);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    body.put_i32(-1);
    body.put_i64(fetch_offset);
    body.put_i64(-1);
    body.put_i32(1_000_000);
    body.put_i32(0);
    put_string(&mut body, "");
    body
}

#[tokio::test]
async fn api_versions_fetch_max_13() {
    let dir = temp_dir("p54", "api");
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
    let mut fetch = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        if key == 1 {
            fetch = Some((min, max));
        }
    }
    assert_eq!(fetch, Some((0, 13))); // Phase 68 TopicId

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v12_flexible_roundtrip() {
    let dir = temp_dir("p54", "v12");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"flex-fetch"));
    let _ = rpc(
        &addr,
        encode_request(0, 5, 2, Some("p"), &produce_body_v3("orders", &batch)),
    )
    .await;

    let body = fetch_v12_body("orders", 0, 7);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 99, Some("flex-fetch"), &body),
    )
    .await;
    let mut src = resp.freeze();
    // Response header v1
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();

    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top error
    assert_eq!(src.get_i32(), 7); // session echo
    let n_topics = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_topics, 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    let n_parts = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_parts, 1);
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // error
    let hwm = src.get_i64();
    assert!(hwm >= 1, "hwm {hwm}");
    let lso = src.get_i64();
    assert_eq!(lso, hwm);
    let log_start = src.get_i64();
    assert!(log_start >= 0);
    let n_aborted = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_aborted, 0);
    assert_eq!(src.get_i32(), -1); // preferred_read_replica
    let records = get_compact_bytes(&mut src).unwrap().unwrap();
    assert!(!records.is_empty(), "expected record batch bytes");
    skip_tag_buffer(&mut src).unwrap(); // partition tags
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    skip_tag_buffer(&mut src).unwrap(); // top-level
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v11_still_classic() {
    let dir = temp_dir("p54", "v11");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"classic"));
    let _ = rpc(
        &addr,
        encode_request(0, 5, 2, Some("p"), &produce_body_v3("t", &batch)),
    )
    .await;

    let body = fetch_v11_body("t", 0);
    let resp = rpc(&addr, encode_request(1, 11, 5, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5); // header v0
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i32(), 0); // session
    assert_eq!(src.get_i32(), 1); // classic topic count
    assert_eq!(get_string(&mut src).unwrap(), "t");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _hwm = src.get_i64();
    let _lso = src.get_i64();
    let _ls = src.get_i64();
    assert_eq!(src.get_i32(), 0); // aborted classic
    assert_eq!(src.get_i32(), -1); // preferred
    let rec = get_bytes(&mut src).unwrap().unwrap();
    assert!(!rec.is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v14_unsupported() {
    let dir = temp_dir("p54", "v14");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // v14 not handled; version ≥12 still uses response header v1.
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 14, 1, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35); // UNSUPPORTED_VERSION

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
