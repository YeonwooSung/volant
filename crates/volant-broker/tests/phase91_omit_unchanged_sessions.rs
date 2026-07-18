//! Phase 91: omit-unchanged incremental fetch session responses (MVP).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, encode_request_flexible, get_compact_array_len,
    get_compact_bytes, get_compact_string, get_string, put_bytes, put_compact_array_len,
    put_compact_string, put_empty_tag_buffer, put_nullable_string, put_string, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::{Offset, Record};
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
    body.put_i32(-1); // current_leader_epoch
    body.put_i64(fetch_offset);
    body.put_i32(-1); // last_fetched_epoch
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

fn assert_flex_header(src: &mut Bytes, corr: i32) -> (i16, i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap(); // response header v1
    assert_eq!(src.get_i32(), 0); // throttle
    let err = src.get_i16();
    let session = src.get_i32();
    (err, session)
}

/// Create session at log end (empty records), then empty-topics incremental omits.
#[tokio::test]
async fn phase91_omit_when_unchanged() {
    let dir = temp_dir("p91", "omit");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one(&addr, "orders", b"a").await; // offset 0; HWM = 1

    // Create session with fetch_offset at HWM → empty records, seeds last_hwm/lso.
    let body = fetch_v12("orders", 1, 0, 0);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 10, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, session_id) = assert_flex_header(&mut src, 10);
    assert_eq!(top_err, 0);
    assert!(session_id > 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // error
    let hwm = src.get_i64();
    let lso = src.get_i64();
    assert_eq!(hwm, 1);
    assert_eq!(lso, 1);
    let _log_start = src.get_i64();
    let _ = get_compact_array_len(&mut src).unwrap(); // aborted
    let _ = src.get_i32(); // preferred
    let records = get_compact_bytes(&mut src).unwrap().unwrap();
    assert!(records.is_empty(), "create at HWM should return empty records");

    // Incremental empty topics, no new produce → omit entire topic.
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
    assert_eq!(
        get_compact_array_len(&mut src).unwrap().unwrap(),
        0,
        "unchanged partition must be omitted from incremental response"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// After omit, new produce advances HWM → incremental includes partition with data.
#[tokio::test]
async fn phase91_include_when_new_produce() {
    let dir = temp_dir("p91", "include");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one(&addr, "orders", b"a").await;

    // Create at HWM=1
    let body = fetch_v12("orders", 1, 0, 0);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 20, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, session_id) = assert_flex_header(&mut src, 20);
    assert_eq!(top_err, 0);
    assert!(session_id > 0);

    // Seed omit: empty incremental
    let body = fetch_v12_empty_topics(session_id, 1);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 21, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, _) = assert_flex_header(&mut src, 21);
    assert_eq!(top_err, 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);

    // New produce → HWM=2; session still has fetch_offset=1
    produce_one(&addr, "orders", b"b").await;

    let body = fetch_v12_empty_topics(session_id, 2);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 22, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid) = assert_flex_header(&mut src, 22);
    assert_eq!(top_err, 0);
    assert_eq!(sid, session_id);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let hwm = src.get_i64();
    assert_eq!(hwm, 2);
    let _lso = src.get_i64();
    let _log_start = src.get_i64();
    let _ = get_compact_array_len(&mut src).unwrap();
    let _ = src.get_i32();
    let records = get_compact_bytes(&mut src).unwrap().unwrap();
    assert!(
        !records.is_empty(),
        "new produce must include partition with records"
    );

    // Next empty incremental at same HWM with same empty-at-offset? fetch_offset still 1
    // so records still non-empty → include again (not omit). Update offset via merge
    // to HWM then omit.
    let body = fetch_v12("orders", 2, session_id, 3);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 23, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, _) = assert_flex_header(&mut src, 23);
    assert_eq!(top_err, 0);
    // Partial topics path always includes; empty records at HWM.
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i64(), 2); // hwm
    let _ = src.get_i64();
    let _ = src.get_i64();
    let _ = get_compact_array_len(&mut src).unwrap();
    let _ = src.get_i32();
    let records = get_compact_bytes(&mut src).unwrap().unwrap();
    assert!(records.is_empty());

    // Empty-topics again → omit
    let body = fetch_v12_empty_topics(session_id, 4);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 24, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, _) = assert_flex_header(&mut src, 24);
    assert_eq!(top_err, 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Phase 88 session errors still work with omit path present.
#[tokio::test]
async fn phase91_session_errors_unchanged() {
    let dir = temp_dir("p91", "errors");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = fetch_v12_empty_topics(99999, 1);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 30, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid) = assert_flex_header(&mut src, 30);
    assert_eq!(top_err, 70);
    assert_eq!(sid, 99999);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
