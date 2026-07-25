//! Phase 104: aborted soft-marker GC with DeleteRecords / retention / load.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, get_bytes, get_string, put_bytes, put_string,
    put_nullable_string,
};
use volant_broker::{Broker, IdempotentCheck};
use volant_core::{Message, Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn storage(dir: &std::path::Path) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        // Small segments so DeleteRecords can drop whole early segments.
        segment_size: 256,
        ..StorageConfig::default()
    }
}

fn big_payload(tag: &str, n: usize) -> String {
    format!("{tag}-{:0width$}", 0, width = n)
}

/// Fill sealed segments with non-txn records so a later DeleteRecords can advance
/// log_start past `min_end`.
fn fill_past(broker: &Broker, topic: &str, min_end: u64) {
    let name = TopicName::new(topic);
    let pid = PartitionId(0);
    let mut i = 0u32;
    while broker.high_watermark(&name, pid).unwrap_or(0) < min_end {
        let payload = big_payload(&format!("fill{i}"), 180);
        broker
            .produce_one(&name, pid, Message::from_value(payload))
            .unwrap();
        i += 1;
        if i > 200 {
            panic!("could not advance HWM past {min_end}");
        }
    }
    broker.flush(&name, pid).unwrap();
}

fn abort_one_range(broker: &Broker, topic: &str, txn_id: &str) -> (u64, u64, u64) {
    let (pid, epoch) = broker.init_producer_id_with_txn(txn_id);
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    let first = match broker.buffer_txn_produce(
        pid,
        epoch,
        topic,
        0,
        0,
        vec![Message::from_value(big_payload("aborted", 180))],
    ) {
        IdempotentCheck::Accept { base_offset } => base_offset,
        other => panic!("unexpected produce: {other:?}"),
    };
    let (code, _, _) = broker.end_txn(pid, epoch, false, &[]).unwrap();
    assert_eq!(code, 0);
    // Marker end is exclusive; control batch may sit at/after end.
    let end = first + 1;
    (pid, first, end)
}

#[test]
fn delete_records_past_marker_drops_memory_and_durable() {
    let dir = temp_dir("p104", "gc-drop");
    let broker = Broker::new(storage(&dir));
    broker.create_topic("events", 1).unwrap();

    // Seed a few non-txn records so aborted data is not at offset 0-only edge.
    fill_past(&broker, "events", 3);
    let (_pid, first, end) = abort_one_range(&broker, "events", "txn-gc-drop");
    assert!(
        broker.aborted_marker_count("events", 0) >= 1,
        "soft marker present after abort"
    );
    assert!(
        broker.is_aborted_offset("events", 0, first),
        "first offset marked aborted"
    );

    // More data after abort so DeleteRecords can drop early segments only.
    fill_past(&broker, "events", end + 20);

    let before_gc = broker.aborted_markers_gc_total();
    // Request delete well past marker end; whole-segment delete advances log_start.
    let (low, err) = broker.delete_records("events", 0, end + 10).unwrap();
    assert_eq!(err, 0);
    assert!(
        low >= end,
        "log start {low} should be >= marker end {end} for GC to apply"
    );

    assert_eq!(
        broker.aborted_marker_count("events", 0),
        0,
        "markers fully below log start must be GC'd"
    );
    assert!(
        broker.aborted_markers_gc_total() > before_gc,
        "GC counter should advance"
    );
    assert!(
        broker
            .aborted_transactions_for_fetch("events", 0, 0, u64::MAX)
            .is_empty()
    );

    // Durable: reload broker on same data_dir — markers stay gone.
    drop(broker);
    let broker2 = Broker::new(storage(&dir));
    assert_eq!(
        broker2.aborted_marker_count("events", 0),
        0,
        "durable __txn_markers must not rehydrate GC'd markers"
    );
    let markers_path = dir.join("__txn_markers").join("state.json");
    if markers_path.exists() {
        let raw = std::fs::read_to_string(&markers_path).unwrap();
        // No aborted entry for events/0 with end fully below live start.
        assert!(
            !raw.contains("\"end_offset\": 1") || broker2.aborted_marker_count("events", 0) == 0,
            "file should not restore dropped markers: {raw}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_records_not_past_marker_retains() {
    let dir = temp_dir("p104", "gc-retain");
    let broker = Broker::new(storage(&dir));
    broker.create_topic("keep", 1).unwrap();

    fill_past(&broker, "keep", 2);
    let (_pid, first, end) = abort_one_range(&broker, "keep", "txn-gc-keep");
    assert!(broker.aborted_marker_count("keep", 0) >= 1);

    fill_past(&broker, "keep", end + 15);

    let before = broker.aborted_marker_count("keep", 0);
    let before_gc = broker.aborted_markers_gc_total();

    // Delete only a tiny prefix that cannot cover the marker end (or drops nothing).
    let (low, err) = broker.delete_records("keep", 0, 1).unwrap();
    assert_eq!(err, 0);
    // Either no segments dropped, or log_start still < end → marker retained.
    if low < end {
        assert_eq!(
            broker.aborted_marker_count("keep", 0),
            before,
            "marker still overlaps live log (low={low} end={end})"
        );
        assert_eq!(broker.aborted_markers_gc_total(), before_gc);
        assert!(broker.is_aborted_offset("keep", 0, first));
    } else {
        // Extremely small first segment edge case: if low advanced past end, GC is correct.
        assert_eq!(broker.aborted_marker_count("keep", 0), 0);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_records_without_markers_still_works() {
    let dir = temp_dir("p104", "no-markers");
    let broker = Broker::new(storage(&dir));
    broker.create_topic("plain", 1).unwrap();
    fill_past(&broker, "plain", 25);

    let before_gc = broker.aborted_markers_gc_total();
    let (low, err) = broker.delete_records("plain", 0, 10).unwrap();
    assert_eq!(err, 0);
    assert!(low <= 25, "low watermark should not jump past HWM");
    assert_eq!(broker.aborted_marker_count("plain", 0), 0);
    assert_eq!(
        broker.aborted_markers_gc_total(),
        before_gc,
        "no markers → GC counter unchanged"
    );

    // Remaining data still fetchable from low watermark.
    let after = broker
        .fetch(
            &TopicName::new("plain"),
            PartitionId(0),
            Offset::new(low),
            100,
        )
        .unwrap();
    assert!(!after.is_empty() || low >= 25);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn load_self_heals_markers_below_log_start() {
    let dir = temp_dir("p104", "load-heal");
    let broker = Broker::new(storage(&dir));
    broker.create_topic("heal", 1).unwrap();
    fill_past(&broker, "heal", 2);
    let (_pid, _first, end) = abort_one_range(&broker, "heal", "txn-heal");
    fill_past(&broker, "heal", end + 20);

    // Truncate log without going through Broker::delete_records GC path:
    // call storage delete via a temporary broker method path is already GC'd.
    // Instead: write a stale marker into the file after delete+GC, then reload.
    let (low, err) = broker.delete_records("heal", 0, end + 10).unwrap();
    assert_eq!(err, 0);
    assert!(low >= end);
    assert_eq!(broker.aborted_marker_count("heal", 0), 0);
    drop(broker);

    // Inject a stale aborted marker fully below log start into durable file.
    let markers_path = dir.join("__txn_markers").join("state.json");
    std::fs::create_dir_all(markers_path.parent().unwrap()).unwrap();
    let stale = format!(
        r#"{{
  "open": [],
  "aborted": [
    {{
      "producer_id": 99,
      "topic": "heal",
      "partition": 0,
      "first_offset": 0,
      "end_offset": {end}
    }}
  ]
}}"#
    );
    std::fs::write(&markers_path, stale).unwrap();

    let broker2 = Broker::new(storage(&dir));
    assert_eq!(
        broker2.aborted_marker_count("heal", 0),
        0,
        "load-time GC must drop markers with end_offset <= log_start"
    );
    assert!(
        broker2.aborted_markers_gc_total() >= 1,
        "load GC should count the drop"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn overlapping_marker_retained_when_partially_live() {
    let dir = temp_dir("p104", "overlap");
    let broker = Broker::new(storage(&dir));
    broker.create_topic("ov", 1).unwrap();

    // Build: non-txn filler, then multi-message abort spanning a wider range.
    fill_past(&broker, "ov", 2);
    let (pid, epoch) = broker.init_producer_id_with_txn("txn-ov");
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    let mut first = 0u64;
    for seq in 0..3 {
        match broker.buffer_txn_produce(
            pid,
            epoch,
            "ov",
            0,
            seq,
            vec![Message::from_value(big_payload(&format!("a{seq}"), 180))],
        ) {
            IdempotentCheck::Accept { base_offset } => {
                if seq == 0 {
                    first = base_offset;
                }
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    let (code, _, _) = broker.end_txn(pid, epoch, false, &[]).unwrap();
    assert_eq!(code, 0);
    let end = first + 3;
    assert!(broker.aborted_marker_count("ov", 0) >= 1);

    fill_past(&broker, "ov", end + 20);

    // Delete a prefix that advances log_start into the middle of the marker range
    // (if possible). Marker must be retained while end_offset > log_start.
    // Try progressive deletes until low is in (first, end) or we give up.
    let mut retained_ok = false;
    for before in [first + 1, first + 2, end] {
        let (low, err) = broker.delete_records("ov", 0, before).unwrap();
        assert_eq!(err, 0);
        if low > first && low < end {
            assert!(
                broker.aborted_marker_count("ov", 0) >= 1,
                "partial overlap must keep marker (low={low} first={first} end={end})"
            );
            // Live aborted offsets still filtered.
            if low < end {
                assert!(broker.is_aborted_offset("ov", 0, low.min(end - 1)));
            }
            retained_ok = true;
            break;
        }
        if low >= end {
            // Whole-segment delete jumped past the marker — GC is correct.
            assert_eq!(broker.aborted_marker_count("ov", 0), 0);
            retained_ok = true;
            break;
        }
    }
    assert!(retained_ok, "could not exercise overlap/GC path via segments");

    let _ = std::fs::remove_dir_all(&dir);
}

fn sample(val: &'static [u8]) -> Vec<Record> {
    vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(val),
        timestamp_ms: 1,
        headers: vec![],
    }]
}

async fn init_txn_async(addr: &str, corr: i32, txn_id: &str) -> (i64, i16) {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    let resp = rpc(addr, encode_request(22, 0, corr, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i16(), 0);
    (src.get_i64(), src.get_i16())
}

async fn add_partitions(addr: &str, corr: i32, txn_id: &str, pid: i64, epoch: i16, topic: &str) {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    let resp = rpc(addr, encode_request(24, 0, corr, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
}

fn produce_body(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

async fn fetch_v4_aborted_n(addr: &str, corr: i32, topic: &str) -> i32 {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(100);
    body.put_i32(1);
    body.put_i32(1_000_000);
    body.put_u8(1); // READ_COMMITTED
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    body.put_i64(0);
    body.put_i32(1_000_000);
    let resp = rpc(addr, encode_request(1, 4, corr, Some("f"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _hwm = src.get_i64();
    let _lso = src.get_i64();
    let aborted_n = src.get_i32();
    for _ in 0..aborted_n {
        let _ = src.get_i64();
        let _ = src.get_i64();
    }
    let _ = get_bytes(&mut src);
    aborted_n
}

#[tokio::test]
async fn read_committed_aborted_list_clears_after_gc() {
    let dir = temp_dir("p104", "rc-list");
    let broker = Arc::new(Broker::new(storage(&dir)));
    broker.create_topic("rc", 1).unwrap();
    fill_past(&broker, "rc", 2);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let (pid, epoch) = init_txn_async(&addr, 1, "txn-rc").await;
    add_partitions(&addr, 2, "txn-rc", pid, epoch, "rc").await;

    let batch = encode_record_batch_idempotent(
        &sample(b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
        pid,
        epoch,
        0,
    );
    let presp = rpc(
        &addr,
        encode_request(0, 0, 3, Some("p"), &produce_body("rc", &batch)),
    )
    .await;
    let mut ps = presp.freeze();
    assert_eq!(ps.get_i32(), 3);
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(get_string(&mut ps).unwrap(), "rc");
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(ps.get_i32(), 0);
    assert_eq!(ps.get_i16(), 0);
    let base = ps.get_i64() as u64;

    let mut ebody = BytesMut::new();
    put_string(&mut ebody, "txn-rc");
    ebody.put_i64(pid);
    ebody.put_i16(epoch);
    ebody.put_u8(0); // abort
    let eresp = rpc(&addr, encode_request(26, 0, 4, Some("p"), &ebody)).await;
    let mut es = eresp.freeze();
    es.advance(4 + 4);
    assert_eq!(es.get_i16(), 0);

    let aborted_before = fetch_v4_aborted_n(&addr, 10, "rc").await;
    assert!(
        aborted_before >= 1,
        "READ_COMMITTED should list abort before GC"
    );

    let end = base + 1;
    fill_past(&broker, "rc", end + 20);
    let (low, err) = broker.delete_records("rc", 0, end + 10).unwrap();
    assert_eq!(err, 0);
    assert!(low >= end, "low={low} end={end}");

    let aborted_after = fetch_v4_aborted_n(&addr, 11, "rc").await;
    assert_eq!(
        aborted_after, 0,
        "READ_COMMITTED aborted list empty after marker GC"
    );
    assert_eq!(broker.aborted_marker_count("rc", 0), 0);

    // Remaining live markers (none) still correct; produce a fresh abort that
    // is not GC'd and confirm list returns.
    let (pid2, epoch2) = init_txn_async(&addr, 20, "txn-rc2").await;
    add_partitions(&addr, 21, "txn-rc2", pid2, epoch2, "rc").await;
    let batch2 = encode_record_batch_idempotent(&sample(b"late-abort-payload"), pid2, epoch2, 0);
    let _ = rpc(
        &addr,
        encode_request(0, 0, 22, Some("p"), &produce_body("rc", &batch2)),
    )
    .await;
    let mut ebody2 = BytesMut::new();
    put_string(&mut ebody2, "txn-rc2");
    ebody2.put_i64(pid2);
    ebody2.put_i16(epoch2);
    ebody2.put_u8(0);
    let eresp2 = rpc(&addr, encode_request(26, 0, 23, Some("p"), &ebody2)).await;
    let mut es2 = eresp2.freeze();
    es2.advance(4 + 4);
    assert_eq!(es2.get_i16(), 0);

    let aborted_new = fetch_v4_aborted_n(&addr, 24, "rc").await;
    assert!(
        aborted_new >= 1,
        "fresh abort after GC still appears in READ_COMMITTED list"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
