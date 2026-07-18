//! Phase 88: Fetch DivergingEpoch + real fetch sessions (MVP).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, encode_request_flexible, get_compact_array_len,
    get_compact_bytes, get_compact_string, get_string, put_bytes, put_compact_array_len,
    put_compact_string, put_empty_tag_buffer, put_nullable_string, put_string, read_unsigned_varint,
    skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn sample_records(value: &'static [u8]) -> Vec<Record> {
    vec![Record {
        offset: Offset::new(0),
        key: Some(Bytes::from_static(b"k")),
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

async fn produce_one(addr: &str, topic: &str, value: &'static [u8]) {
    let batch = encode_record_batch(&sample_records(value));
    let resp = rpc(
        addr,
        encode_request(0, 3, 1, Some("p"), &produce_body_v3(topic, &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 1); // topics
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1); // partitions
    let _pid = src.get_i32();
    let err = src.get_i16();
    assert_eq!(err, 0, "produce failed");
}

/// Fetch v12 flexible body (single topic/partition).
fn fetch_v12(
    topic: &str,
    fetch_offset: i64,
    session_id: i32,
    session_epoch: i32,
    current_leader_epoch: i32,
    last_fetched_epoch: i32,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica
    body.put_i32(0); // max_wait
    body.put_i32(1); // min_bytes
    body.put_i32(1_048_576); // max_bytes
    body.put_u8(0); // isolation
    body.put_i32(session_id);
    body.put_i32(session_epoch);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0); // partition
    body.put_i32(current_leader_epoch);
    body.put_i64(fetch_offset);
    body.put_i32(last_fetched_epoch);
    body.put_i64(-1); // log_start
    body.put_i32(1_000_000); // max_bytes
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_compact_array_len(&mut body, 0); // forgotten
    put_compact_string(&mut body, ""); // rack
    put_empty_tag_buffer(&mut body);
    body
}

/// Fetch v12 with empty topics array (incremental re-fetch of session).
fn fetch_v12_empty_topics(session_id: i32, session_epoch: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(0);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(0);
    body.put_i32(session_id);
    body.put_i32(session_epoch);
    put_compact_array_len(&mut body, 0); // topics empty
    put_compact_array_len(&mut body, 0); // forgotten empty
    put_compact_string(&mut body, "");
    put_empty_tag_buffer(&mut body);
    body
}

/// Empty topics + forgotten partition.
fn fetch_v12_forget(session_id: i32, session_epoch: i32, topic: &str, partition: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(0);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(0);
    body.put_i32(session_id);
    body.put_i32(session_epoch);
    put_compact_array_len(&mut body, 0); // topics empty
    put_compact_array_len(&mut body, 1); // forgotten
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    put_empty_tag_buffer(&mut body);
    put_compact_string(&mut body, "");
    put_empty_tag_buffer(&mut body);
    body
}

fn assert_flex_header(src: &mut Bytes, corr: i32) -> (i16, i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap(); // response header v1
    assert_eq!(src.get_i32(), 0); // throttle
    let err = src.get_i16();
    let session = src.get_i32();
    (err, session)
}

/// Read one partition's tags; return DivergingEpoch if present.
fn read_diverging_from_partition_tags(src: &mut Bytes) -> Option<(i32, i64)> {
    let n = read_unsigned_varint(src).unwrap();
    let mut diverging = None;
    for _ in 0..n {
        let tag = read_unsigned_varint(src).unwrap();
        let len = read_unsigned_varint(src).unwrap() as usize;
        assert!(src.remaining() >= len);
        let mut val = src.copy_to_bytes(len);
        if tag == 0 && val.remaining() >= 12 {
            let epoch = val.get_i32();
            let end = val.get_i64();
            diverging = Some((epoch, end));
        }
    }
    diverging
}

#[tokio::test]
async fn diverging_epoch_on_truncated_fetch() {
    let dir = temp_dir("p88", "diverging");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Epoch 0: two produces → end of epoch 0 will be 2 after bump.
    produce_one(&addr, "orders", b"a").await;
    produce_one(&addr, "orders", b"b").await;
    broker
        .set_partition_leader_epoch(&TopicName::new("orders"), PartitionId(0), 1)
        .unwrap();
    produce_one(&addr, "orders", b"c").await;
    produce_one(&addr, "orders", b"d").await;

    // Client claims last_fetched_epoch=0 but fetch_offset=3 (> end 2).
    // FINAL epoch so no session is created.
    let body = fetch_v12("orders", 3, 0, -1, -1, 0);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 88, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, session) = assert_flex_header(&mut src, 88);
    assert_eq!(top_err, 0);
    assert_eq!(session, 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 1); // OFFSET_OUT_OF_RANGE
    let _hwm = src.get_i64();
    let _lso = src.get_i64();
    let _log_start = src.get_i64();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0); // aborted
    assert_eq!(src.get_i32(), -1); // preferred replica
    let records = get_compact_bytes(&mut src).unwrap().unwrap();
    assert!(records.is_empty());
    let diverging = read_diverging_from_partition_tags(&mut src);
    assert_eq!(
        diverging,
        Some((0, 2)),
        "DivergingEpoch must report prior epoch end"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_session_create_and_incremental_empty_topics() {
    let dir = temp_dir("p88", "session");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one(&addr, "orders", b"s1").await;

    // Create session: id=0, epoch=INITIAL(0)
    let body = fetch_v12("orders", 0, 0, 0, -1, -1);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 10, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, session_id) = assert_flex_header(&mut src, 10);
    assert_eq!(top_err, 0);
    assert!(session_id > 0, "broker must assign a session id");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _ = src.get_i64(); // hwm
    let _ = src.get_i64(); // lso
    let _ = src.get_i64(); // log_start
    let _ = get_compact_array_len(&mut src).unwrap();
    let _ = src.get_i32(); // preferred
    let records = get_compact_bytes(&mut src).unwrap().unwrap();
    assert!(!records.is_empty());

    // Incremental empty topics with correct epoch=1
    let body = fetch_v12_empty_topics(session_id, 1);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 11, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid2) = assert_flex_header(&mut src, 11);
    assert_eq!(top_err, 0);
    assert_eq!(sid2, session_id);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_session_forgotten_topics() {
    let dir = temp_dir("p88", "forgotten");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one(&addr, "orders", b"x").await;

    // Create session
    let body = fetch_v12("orders", 0, 0, 0, -1, -1);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 20, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, session_id) = assert_flex_header(&mut src, 20);
    assert_eq!(top_err, 0);
    assert!(session_id > 0);

    // Incremental: empty topics + forgotten partition 0 → empty response set
    let body = fetch_v12_forget(session_id, 1, "orders", 0);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 21, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid2) = assert_flex_header(&mut src, 21);
    assert_eq!(top_err, 0);
    assert_eq!(sid2, session_id);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);

    // Next empty incremental still empty
    let body = fetch_v12_empty_topics(session_id, 2);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 22, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, _) = assert_flex_header(&mut src, 22);
    assert_eq!(top_err, 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_session_invalid_id_and_epoch() {
    let dir = temp_dir("p88", "invalid");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one(&addr, "orders", b"z").await;

    // Unknown session id
    let body = fetch_v12_empty_topics(99999, 1);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 30, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid) = assert_flex_header(&mut src, 30);
    assert_eq!(top_err, 70); // FETCH_SESSION_ID_NOT_FOUND
    assert_eq!(sid, 99999);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);

    // Create a real session
    let body = fetch_v12("orders", 0, 0, 0, -1, -1);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 31, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, session_id) = assert_flex_header(&mut src, 31);
    assert_eq!(top_err, 0);
    assert!(session_id > 0);

    // Wrong epoch (expect 1, send 5)
    let body = fetch_v12_empty_topics(session_id, 5);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 32, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid) = assert_flex_header(&mut src, 32);
    assert_eq!(top_err, 71); // INVALID_FETCH_SESSION_EPOCH
    assert_eq!(sid, session_id);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
