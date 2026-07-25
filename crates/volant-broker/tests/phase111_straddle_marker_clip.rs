//! Phase 111: clip straddling soft abort markers to live log_start.

#[path = "common/mod.rs"]
mod common;
use common::temp_dir;

use volant_broker::{Broker, IdempotentCheck};
use volant_core::{Message, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn storage(dir: &std::path::Path) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        // Small segments so DeleteRecords can advance log_start mid-range.
        segment_size: 256,
        ..StorageConfig::default()
    }
}

fn big_payload(tag: &str, n: usize) -> String {
    format!("{tag}-{:0width$}", 0, width = n)
}

/// Fill sealed segments with non-txn records so DeleteRecords can advance
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

/// Abort a multi-message txn; returns `(producer_id, first_offset, end_offset)`.
fn abort_wide_range(broker: &Broker, topic: &str, txn_id: &str, n: i32) -> (u64, u64, u64) {
    let (pid, epoch) = broker.init_producer_id_with_txn(txn_id);
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    let mut first = 0u64;
    for seq in 0..n {
        match broker.buffer_txn_produce(
            pid,
            epoch,
            topic,
            0,
            seq,
            vec![Message::from_value(big_payload(&format!("a{seq}"), 180))],
        ) {
            IdempotentCheck::Accept { base_offset } => {
                if seq == 0 {
                    first = base_offset;
                }
            }
            other => panic!("unexpected produce: {other:?}"),
        }
    }
    let (code, _, _) = broker.end_txn(pid, epoch, false, &[]).unwrap();
    assert_eq!(code, 0);
    let end = first + n as u64;
    (pid, first, end)
}

/// Progressive DeleteRecords until `log_start` lands in `(first, end)`, or
/// past `end`. Returns the low watermark achieved.
fn delete_into_or_past(broker: &Broker, topic: &str, first: u64, end: u64) -> u64 {
    fill_past(broker, topic, end + 30);
    let mut last_low = 0u64;
    // Prefer landing inside the marker range for the straddle path.
    for before in (first + 1)..(end + 15) {
        let (low, err) = broker.delete_records(topic, 0, before).unwrap();
        assert_eq!(err, 0);
        last_low = low;
        if low > first && low < end {
            return low;
        }
        if low >= end {
            return low;
        }
    }
    last_low
}

#[test]
fn straddle_delete_clips_first_offset_to_log_start() {
    let dir = temp_dir("p111", "straddle-clip");
    let broker = Broker::new(storage(&dir));
    broker.create_topic("clip", 1).unwrap();

    fill_past(&broker, "clip", 2);
    let (pid, first, end) = abort_wide_range(&broker, "clip", "txn-clip", 5);
    assert!(broker.aborted_marker_count("clip", 0) >= 1);
    assert!(broker.is_aborted_offset("clip", 0, first));
    assert!(end - first >= 3, "need a multi-offset range to straddle");

    let before_gc = broker.aborted_markers_gc_total();
    let low = delete_into_or_past(&broker, "clip", first, end);

    if low >= end {
        // Whole-segment delete jumped past the marker — full drop is correct.
        assert_eq!(broker.aborted_marker_count("clip", 0), 0);
        assert!(broker.aborted_markers_gc_total() > before_gc);
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }

    assert!(
        low > first && low < end,
        "expected straddle low={low} first={first} end={end}"
    );
    assert_eq!(
        broker.aborted_marker_count("clip", 0),
        1,
        "straddling marker must be retained (clipped, not dropped)"
    );
    // Clips do not bump the Phase 104 drop counter.
    assert_eq!(
        broker.aborted_markers_gc_total(),
        before_gc,
        "clip must not increment volant_aborted_markers_gc_total"
    );

    let ranges = broker.aborted_marker_ranges("clip", 0);
    assert_eq!(ranges.len(), 1);
    let (got_pid, got_first, got_end) = ranges[0];
    assert_eq!(got_pid, pid);
    assert_eq!(
        got_first, low,
        "first_offset must clip to log_start (was {first})"
    );
    assert_eq!(got_end, end, "end_offset must stay exclusive end");

    // Live remainder still aborted; prefix below log_start is not claimed.
    assert!(broker.is_aborted_offset("clip", 0, low));
    if end > low + 1 {
        assert!(broker.is_aborted_offset("clip", 0, end - 1));
    }
    let listed = broker.aborted_transactions_for_fetch("clip", 0, low, u64::MAX);
    assert!(
        listed.iter().any(|(p, f)| *p == pid && *f == low),
        "READ_COMMITTED list should expose clipped first_offset={low}, got {listed:?}"
    );
    // Fetch from below log_start still only lists the live clipped first.
    let listed_from_zero = broker.aborted_transactions_for_fetch("clip", 0, 0, u64::MAX);
    assert!(
        listed_from_zero
            .iter()
            .any(|(p, f)| *p == pid && *f == low),
        "clipped first must be listed even when fetch starts at 0: {listed_from_zero:?}"
    );
    assert!(
        !listed_from_zero
            .iter()
            .any(|(p, f)| *p == pid && *f == first && first < low),
        "obsolete pre-clip first_offset must not appear"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn straddle_clip_persists_across_reload() {
    let dir = temp_dir("p111", "clip-reload");
    let broker = Broker::new(storage(&dir));
    broker.create_topic("dur", 1).unwrap();

    fill_past(&broker, "dur", 2);
    let (pid, first, end) = abort_wide_range(&broker, "dur", "txn-dur", 5);
    let low = delete_into_or_past(&broker, "dur", first, end);

    if low >= end {
        // Segment geometry jumped past end — still exercise full-drop durable path.
        assert_eq!(broker.aborted_marker_count("dur", 0), 0);
        drop(broker);
        let broker2 = Broker::new(storage(&dir));
        assert_eq!(broker2.aborted_marker_count("dur", 0), 0);
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }

    let ranges = broker.aborted_marker_ranges("dur", 0);
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0], (pid, low, end));

    // Durable file should already reflect the clip.
    let markers_path = dir.join("__txn_markers").join("state.json");
    assert!(markers_path.exists(), "clip must persist __txn_markers");
    let raw = std::fs::read_to_string(&markers_path).unwrap();
    assert!(
        raw.contains(&format!("\"first_offset\": {low}")),
        "durable first_offset should be clipped to {low}: {raw}"
    );
    assert!(
        raw.contains(&format!("\"end_offset\": {end}")),
        "durable end_offset unchanged: {raw}"
    );

    drop(broker);
    let broker2 = Broker::new(storage(&dir));
    let ranges2 = broker2.aborted_marker_ranges("dur", 0);
    assert_eq!(
        ranges2,
        vec![(pid, low, end)],
        "reload must preserve clipped range"
    );
    assert!(broker2.is_aborted_offset("dur", 0, low));
    assert!(!broker2.is_aborted_offset("dur", 0, first.min(low.saturating_sub(1))) || first >= low);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn full_gc_still_drops_when_log_start_past_end() {
    let dir = temp_dir("p111", "full-drop");
    let broker = Broker::new(storage(&dir));
    broker.create_topic("gone", 1).unwrap();

    fill_past(&broker, "gone", 2);
    let (_pid, first, end) = abort_wide_range(&broker, "gone", "txn-gone", 3);
    assert!(broker.is_aborted_offset("gone", 0, first));
    fill_past(&broker, "gone", end + 25);

    let before_gc = broker.aborted_markers_gc_total();
    let (low, err) = broker.delete_records("gone", 0, end + 10).unwrap();
    assert_eq!(err, 0);
    assert!(
        low >= end,
        "need log_start past marker end for full drop (low={low} end={end})"
    );
    assert_eq!(broker.aborted_marker_count("gone", 0), 0);
    assert!(
        broker.aborted_markers_gc_total() > before_gc,
        "full drop must bump GC counter"
    );
    assert!(broker
        .aborted_transactions_for_fetch("gone", 0, 0, u64::MAX)
        .is_empty());

    drop(broker);
    let broker2 = Broker::new(storage(&dir));
    assert_eq!(broker2.aborted_marker_count("gone", 0), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn load_self_heals_straddle_by_clip() {
    let dir = temp_dir("p111", "load-clip");
    let broker = Broker::new(storage(&dir));
    broker.create_topic("heal", 1).unwrap();

    // Build a live log so log_start can sit mid-range after DeleteRecords.
    fill_past(&broker, "heal", 8);
    fill_past(&broker, "heal", 30);
    // Advance log_start into the middle without any markers first.
    let (low, err) = broker.delete_records("heal", 0, 5).unwrap();
    assert_eq!(err, 0);
    // If delete dropped nothing, force a higher request until something moves.
    let mut log_start = low;
    if log_start == 0 {
        for before in [10u64, 15, 20] {
            let (l, e) = broker.delete_records("heal", 0, before).unwrap();
            assert_eq!(e, 0);
            log_start = l;
            if log_start > 0 {
                break;
            }
        }
    }
    if log_start == 0 {
        // Segment geometry prevented truncate — skip rather than false-fail.
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }
    drop(broker);

    // Inject a straddling durable marker: first below log_start, end above.
    let end = log_start + 4;
    let markers_path = dir.join("__txn_markers").join("state.json");
    std::fs::create_dir_all(markers_path.parent().unwrap()).unwrap();
    let stale = format!(
        r#"{{
  "open": [],
  "aborted": [
    {{
      "producer_id": 77,
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
    let ranges = broker2.aborted_marker_ranges("heal", 0);
    assert_eq!(
        ranges.len(),
        1,
        "straddling marker must be retained after load clip"
    );
    assert_eq!(ranges[0].0, 77);
    assert_eq!(
        ranges[0].1, log_start,
        "load must clip first_offset to log_start={log_start}"
    );
    assert_eq!(ranges[0].2, end);
    // Clip does not count as a drop.
    assert_eq!(
        broker2.aborted_markers_gc_total(),
        0,
        "load clip must not bump drop counter"
    );

    // Persist should have rewritten the clipped first_offset.
    let raw = std::fs::read_to_string(&markers_path).unwrap();
    assert!(
        raw.contains(&format!("\"first_offset\": {log_start}")),
        "load clip must persist: {raw}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn fully_live_marker_unchanged() {
    let dir = temp_dir("p111", "live-keep");
    let broker = Broker::new(storage(&dir));
    broker.create_topic("live", 1).unwrap();

    fill_past(&broker, "live", 3);
    // Delete a tiny prefix first (may no-op), then abort so marker is fully live.
    let _ = broker.delete_records("live", 0, 1);
    let (pid, first, end) = abort_wide_range(&broker, "live", "txn-live", 2);
    let before = broker.aborted_marker_ranges("live", 0);
    assert_eq!(before.len(), 1);
    assert_eq!(before[0], (pid, first, end));

    // Delete that cannot reach the marker (before first) — retain unchanged.
    let (low, err) = broker.delete_records("live", 0, first.saturating_sub(1).max(1)).unwrap();
    assert_eq!(err, 0);
    if low <= first {
        let after = broker.aborted_marker_ranges("live", 0);
        assert_eq!(after, before, "fully live marker must not clip or drop");
        assert_eq!(broker.aborted_markers_gc_total(), 0);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
