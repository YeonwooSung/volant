//! Phase 115: durable fetch sessions survive broker restart (same data_dir).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::path::Path;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, encode_request_flexible, get_compact_array_len,
    get_compact_string, get_string, put_bytes, put_compact_array_len, put_compact_string,
    put_empty_tag_buffer, put_nullable_string, put_string, skip_tag_buffer,
};
use volant_broker::kafka::fetch_session::{FETCH_SESSIONS_DIR, FETCH_SESSIONS_FILE};
use volant_broker::Broker;
use volant_core::{Offset, Record};
use volant_storage::StorageConfig;

fn storage(dir: &Path) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        ..StorageConfig::default()
    }
}

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

/// Fetch v12 flexible body (single topic/partition).
fn fetch_v12(topic: &str, fetch_offset: i64, session_id: i32, session_epoch: i32) -> BytesMut {
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

fn fetch_v12_final(session_id: i32) -> BytesMut {
    // FINAL_EPOCH = -1 closes session.
    fetch_v12("orders", 0, session_id, -1)
}

fn assert_flex_header(src: &mut Bytes, corr: i32) -> (i16, i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    let err = src.get_i16();
    let session = src.get_i32();
    (err, session)
}

/// Session + omit cache survive drop + reopen of same data_dir.
#[tokio::test]
async fn session_and_omit_survive_restart() {
    let dir = temp_dir("p115", "omit-restart");
    let session_id = {
        let broker = Arc::new(Broker::new(storage(&dir)));
        // Keep sessions for the test window.
        broker.set_fetch_session_idle_ms(0);
        broker.create_topic("orders", 1).unwrap();
        let (addr, _server) = boot_kafka(Arc::clone(&broker)).await;
        produce_one(&addr, "orders", b"a").await; // offset 0; HWM = 1

        // Create session at log end → seeds last_hwm/lso.
        let body = fetch_v12("orders", 1, 0, 0);
        let resp = rpc(
            &addr,
            encode_request_flexible(1, 12, 10, Some("c"), &body),
        )
        .await;
        let mut src = resp.freeze();
        let (top_err, sid) = assert_flex_header(&mut src, 10);
        assert_eq!(top_err, 0);
        assert!(sid > 0);
        assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
        assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
        assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
        assert_eq!(src.get_i32(), 0);
        assert_eq!(src.get_i16(), 0);
        let hwm = src.get_i64();
        let lso = src.get_i64();
        assert_eq!(hwm, 1);
        assert_eq!(lso, 1);

        assert!(
            dir.join(FETCH_SESSIONS_DIR).join(FETCH_SESSIONS_FILE).is_file(),
            "durable snapshot should exist after create"
        );
        assert_eq!(broker.fetch_sessions().active_count(), 1);
        sid
        // broker dropped here
    };

    // Reopen same data_dir.
    let broker2 = Arc::new(Broker::new(storage(&dir)));
    broker2.set_fetch_session_idle_ms(0);
    assert!(
        broker2.fetch_sessions().restored() >= 1,
        "expected restored sessions, got {}",
        broker2.fetch_sessions().restored()
    );
    assert_eq!(broker2.fetch_sessions().active_count(), 1);

    let (addr2, _server2) = boot_kafka(Arc::clone(&broker2)).await;

    // Empty-topics incremental with expected epoch 1 must succeed and omit.
    let body = fetch_v12_empty_topics(session_id, 1);
    let resp = rpc(
        &addr2,
        encode_request_flexible(1, 12, 11, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid) = assert_flex_header(&mut src, 11);
    assert_eq!(top_err, 0, "session should be found after restart");
    assert_eq!(sid, session_id);
    // Omit-unchanged: no topics in response when HWM/LSO unchanged.
    let n_topics = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(
        n_topics, 0,
        "omit-unchanged should drop empty unchanged partitions after restore"
    );
}

/// FINAL close is not restored after restart.
#[tokio::test]
async fn final_close_not_restored() {
    let dir = temp_dir("p115", "final-close");
    let session_id = {
        let broker = Arc::new(Broker::new(storage(&dir)));
        broker.set_fetch_session_idle_ms(0);
        broker.create_topic("orders", 1).unwrap();
        let (addr, _server) = boot_kafka(Arc::clone(&broker)).await;
        produce_one(&addr, "orders", b"x").await;

        let body = fetch_v12("orders", 0, 0, 0);
        let resp = rpc(
            &addr,
            encode_request_flexible(1, 12, 20, Some("c"), &body),
        )
        .await;
        let mut src = resp.freeze();
        let (err, sid) = assert_flex_header(&mut src, 20);
        assert_eq!(err, 0);
        assert!(sid > 0);

        // FINAL close
        let body = fetch_v12_final(sid);
        let resp = rpc(
            &addr,
            encode_request_flexible(1, 12, 21, Some("c"), &body),
        )
        .await;
        let mut src = resp.freeze();
        let (err, out_sid) = assert_flex_header(&mut src, 21);
        assert_eq!(err, 0);
        assert_eq!(out_sid, 0);
        assert_eq!(broker.fetch_sessions().active_count(), 0);
        sid
    };

    let broker2 = Arc::new(Broker::new(storage(&dir)));
    broker2.set_fetch_session_idle_ms(0);
    assert_eq!(broker2.fetch_sessions().restored(), 0);
    assert_eq!(broker2.fetch_sessions().active_count(), 0);

    let (addr2, _server2) = boot_kafka(Arc::clone(&broker2)).await;
    let body = fetch_v12_empty_topics(session_id, 1);
    let resp = rpc(
        &addr2,
        encode_request_flexible(1, 12, 22, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, _) = assert_flex_header(&mut src, 22);
    assert_eq!(top_err, 70, "closed session must not resurrect");
}

/// Idle-expired snapshot entries are not restored as live sessions.
#[tokio::test]
async fn idle_expired_not_restored() {
    let dir = temp_dir("p115", "idle-load");
    {
        let broker = Arc::new(Broker::new(storage(&dir)));
        broker.set_fetch_session_idle_ms(0);
        broker.create_topic("orders", 1).unwrap();
        let (addr, _server) = boot_kafka(Arc::clone(&broker)).await;
        produce_one(&addr, "orders", b"z").await;
        let body = fetch_v12("orders", 0, 0, 0);
        let resp = rpc(
            &addr,
            encode_request_flexible(1, 12, 30, Some("c"), &body),
        )
        .await;
        let mut src = resp.freeze();
        let (err, sid) = assert_flex_header(&mut src, 30);
        assert_eq!(err, 0);
        assert!(sid > 0);
    }

    // Rewrite snapshot with ancient last_activity so default idle TTL drops it.
    let path = dir.join(FETCH_SESSIONS_DIR).join(FETCH_SESSIONS_FILE);
    let raw = std::fs::read_to_string(&path).expect("snapshot");
    let mut v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    if let Some(arr) = v.get_mut("sessions").and_then(|s| s.as_array_mut()) {
        for s in arr.iter_mut() {
            s.as_object_mut()
                .unwrap()
                .insert("last_activity_ms".into(), serde_json::json!(1));
        }
    }
    std::fs::write(&path, serde_json::to_string_pretty(&v).unwrap()).unwrap();

    // Default idle is 60s; activity=1 is expired.
    let broker2 = Arc::new(Broker::new(storage(&dir)));
    assert_eq!(broker2.fetch_sessions().restored(), 0);
    assert_eq!(broker2.fetch_sessions().active_count(), 0);
}

/// Direct manager restore metric visible after restart with live session.
#[tokio::test]
async fn restored_metric_after_restart() {
    let dir = temp_dir("p115", "metric");
    {
        let broker = Arc::new(Broker::new(storage(&dir)));
        broker.set_fetch_session_idle_ms(0);
        broker.create_topic("t", 1).unwrap();
        let (addr, _server) = boot_kafka(Arc::clone(&broker)).await;
        let body = fetch_v12("t", 0, 0, 0);
        let resp = rpc(
            &addr,
            encode_request_flexible(1, 12, 40, Some("c"), &body),
        )
        .await;
        let mut src = resp.freeze();
        let (err, sid) = assert_flex_header(&mut src, 40);
        assert_eq!(err, 0);
        assert!(sid > 0);
        // first process: restored may be 0 (nothing on disk at open)
        let _ = get_compact_array_len(&mut src);
    }
    let broker2 = Arc::new(Broker::new(storage(&dir)));
    broker2.set_fetch_session_idle_ms(0);
    assert_eq!(broker2.fetch_sessions().restored(), 1);
    assert_eq!(broker2.fetch_sessions().persist_errors_total(), 0);
}
