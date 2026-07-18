//! Phase 95: fetch session idle TTL + max concurrent sessions (MVP).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;
use std::time::Duration;

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
    assert_eq!(src.get_i32(), 1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    let _pid = src.get_i32();
    let err = src.get_i16();
    assert_eq!(err, 0, "produce failed");
}

fn fetch_v12(
    topic: &str,
    fetch_offset: i64,
    session_id: i32,
    session_epoch: i32,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(0);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(0);
    body.put_i32(session_id);
    body.put_i32(session_epoch);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0);
    body.put_i32(-1);
    body.put_i64(fetch_offset);
    body.put_i32(-1);
    body.put_i64(-1);
    body.put_i32(1_000_000);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_compact_array_len(&mut body, 0);
    put_compact_string(&mut body, "");
    put_empty_tag_buffer(&mut body);
    body
}

fn fetch_v12_empty_topics(session_id: i32, session_epoch: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(0);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(0);
    body.put_i32(session_id);
    body.put_i32(session_epoch);
    put_compact_array_len(&mut body, 0);
    put_compact_array_len(&mut body, 0);
    put_compact_string(&mut body, "");
    put_empty_tag_buffer(&mut body);
    body
}

fn assert_flex_header(src: &mut Bytes, corr: i32) -> (i16, i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
    assert_eq!(src.get_i32(), 0);
    let err = src.get_i16();
    let session = src.get_i32();
    (err, session)
}

/// Idle TTL expiry → next incremental returns FETCH_SESSION_ID_NOT_FOUND (70).
#[tokio::test]
async fn phase95_idle_ttl_evicts_session() {
    let dir = temp_dir("p95", "idle");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_fetch_session_idle_ms(80);
    broker.set_fetch_session_max(0); // unlimited
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one(&addr, "orders", b"a").await;

    let body = fetch_v12("orders", 0, 0, 0);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 10, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, session_id) = assert_flex_header(&mut src, 10);
    assert_eq!(top_err, 0);
    assert!(session_id > 0);

    tokio::time::sleep(Duration::from_millis(150)).await;

    let body = fetch_v12_empty_topics(session_id, 1);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 11, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid) = assert_flex_header(&mut src, 11);
    assert_eq!(top_err, 70, "idle-expired session must return 70");
    assert_eq!(sid, session_id);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);
    assert!(
        broker.fetch_sessions().evicted_total() >= 1,
        "idle eviction must increment counter"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// At max sessions, new create succeeds and LRU victim gets 70 on incremental.
#[tokio::test]
async fn phase95_max_sessions_evicts_lru() {
    let dir = temp_dir("p95", "max");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_fetch_session_idle_ms(0); // disable idle
    broker.set_fetch_session_max(2);
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one(&addr, "orders", b"a").await;

    // Session A
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 20, Some("c"), &fetch_v12("orders", 0, 0, 0)),
    )
    .await;
    let mut src = resp.freeze();
    let (err, sid_a) = assert_flex_header(&mut src, 20);
    assert_eq!(err, 0);
    assert!(sid_a > 0);

    // Session B
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 21, Some("c"), &fetch_v12("orders", 0, 0, 0)),
    )
    .await;
    let mut src = resp.freeze();
    let (err, sid_b) = assert_flex_header(&mut src, 21);
    assert_eq!(err, 0);
    assert!(sid_b > 0);
    assert_ne!(sid_a, sid_b);

    // Touch B so A is LRU
    let resp = rpc(
        &addr,
        encode_request_flexible(
            1,
            12,
            22,
            Some("c"),
            &fetch_v12_empty_topics(sid_b, 1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    let (err, _) = assert_flex_header(&mut src, 22);
    assert_eq!(err, 0);

    // Session C forces LRU eviction of A
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 23, Some("c"), &fetch_v12("orders", 0, 0, 0)),
    )
    .await;
    let mut src = resp.freeze();
    let (err, sid_c) = assert_flex_header(&mut src, 23);
    assert_eq!(err, 0);
    assert!(sid_c > 0);
    assert_eq!(broker.fetch_sessions().active_count(), 2);

    // A is gone
    let resp = rpc(
        &addr,
        encode_request_flexible(
            1,
            12,
            24,
            Some("c"),
            &fetch_v12_empty_topics(sid_a, 1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    let (err, sid) = assert_flex_header(&mut src, 24);
    assert_eq!(err, 70, "LRU-evicted session must return 70");
    assert_eq!(sid, sid_a);

    // B still works (epoch already advanced to 2 after touch)
    let resp = rpc(
        &addr,
        encode_request_flexible(
            1,
            12,
            25,
            Some("c"),
            &fetch_v12_empty_topics(sid_b, 2),
        ),
    )
    .await;
    let mut src = resp.freeze();
    let (err, sid) = assert_flex_header(&mut src, 25);
    assert_eq!(err, 0);
    assert_eq!(sid, sid_b);

    // C works with epoch 1
    let resp = rpc(
        &addr,
        encode_request_flexible(
            1,
            12,
            26,
            Some("c"),
            &fetch_v12_empty_topics(sid_c, 1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    let (err, sid) = assert_flex_header(&mut src, 26);
    assert_eq!(err, 0);
    assert_eq!(sid, sid_c);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Phase 91 omit-unchanged still works under Phase 95 limits (defaults).
#[tokio::test]
async fn phase95_omit_unchanged_regression() {
    let dir = temp_dir("p95", "omit");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Generous limits so this is purely an omit regression.
    broker.set_fetch_session_idle_ms(60_000);
    broker.set_fetch_session_max(1000);
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one(&addr, "orders", b"a").await; // HWM = 1

    // Create at HWM → empty records, seed cache.
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 30, Some("c"), &fetch_v12("orders", 1, 0, 0)),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, session_id) = assert_flex_header(&mut src, 30);
    assert_eq!(top_err, 0);
    assert!(session_id > 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i64(), 1); // hwm
    let _ = src.get_i64();
    let _ = src.get_i64();
    let _ = get_compact_array_len(&mut src).unwrap();
    let _ = src.get_i32();
    let records = get_compact_bytes(&mut src).unwrap().unwrap();
    assert!(records.is_empty());

    // Empty-topics incremental → omit
    let resp = rpc(
        &addr,
        encode_request_flexible(
            1,
            12,
            31,
            Some("c"),
            &fetch_v12_empty_topics(session_id, 1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid) = assert_flex_header(&mut src, 31);
    assert_eq!(top_err, 0);
    assert_eq!(sid, session_id);
    assert_eq!(
        get_compact_array_len(&mut src).unwrap().unwrap(),
        0,
        "omit-unchanged must still drop empty unchanged partitions"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
