//! Integration tests for Phase 1 durable partition log.

use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use volant_core::{Message, Offset};
use volant_storage::{PartitionLog, StorageConfig};

fn tmp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("volant-durable-{label}-{nanos}"));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(dir: &std::path::Path) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        segment_size: 256 * 1024 * 1024,
        use_mmap: true,
        flush_every_n: 0,
        index_interval_bytes: 4096,
        retention_ms: None,
        retention_bytes: None,
        ..StorageConfig::default()
    }
}

#[test]
fn append_read_roundtrip() {
    let dir = tmp_dir("roundtrip");
    let mut log = PartitionLog::open(config(&dir)).unwrap();

    let msg = Message {
        key: Some(Bytes::from("key1")),
        value: Bytes::from("value1"),
        timestamp_ms: Some(42),
        headers: vec![("h".into(), Bytes::from("v"))],
    };
    let rec = log.append(msg).unwrap();
    assert_eq!(rec.offset.raw(), 0);
    assert_eq!(rec.timestamp_ms, 42);
    assert_eq!(rec.key.as_ref().unwrap().as_ref(), b"key1");

    let r2 = log.append(Message::from_value("second")).unwrap();
    assert_eq!(r2.offset.raw(), 1);
    assert!(r2.timestamp_ms > 0); // default unix now

    let all = log.read(Offset::ZERO, 100).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].value.as_ref(), b"value1");
    assert_eq!(all[1].value.as_ref(), b"second");
    assert_eq!(all[0].headers[0].0, "h");

    // Clamp from < log_start (still 0)
    let got = log.read(Offset::ZERO, 1).unwrap();
    assert_eq!(got.len(), 1);

    // Empty if from >= hwm
    assert!(log.read(Offset::new(2), 10).unwrap().is_empty());
    assert_eq!(log.high_watermark().raw(), 2);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn segment_roll_with_tiny_segment_size() {
    let dir = tmp_dir("roll");
    let mut cfg = config(&dir);
    // Tiny size forces a roll after the first record.
    cfg.segment_size = 64;
    let mut log = PartitionLog::open(cfg).unwrap();

    for i in 0..5 {
        let rec = log
            .append(Message::from_value(format!("msg-{i}")))
            .unwrap();
        assert_eq!(rec.offset.raw(), i as u64);
    }

    let all = log.read(Offset::ZERO, 100).unwrap();
    assert_eq!(all.len(), 5);
    for (i, r) in all.iter().enumerate() {
        assert_eq!(r.value.as_ref(), format!("msg-{i}").as_bytes());
    }

    // Multiple segment files should exist.
    let logs: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".log"))
        .collect();
    assert!(
        logs.len() >= 2,
        "expected multiple segments after roll, got {logs:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reopen_recovery_preserves_data() {
    let dir = tmp_dir("reopen");
    {
        let mut log = PartitionLog::open(config(&dir)).unwrap();
        for i in 0..10 {
            log.append(Message::from_value(format!("m{i}"))).unwrap();
        }
        log.flush().unwrap();
        assert_eq!(log.high_watermark().raw(), 10);
    }

    let log = PartitionLog::open(config(&dir)).unwrap();
    assert_eq!(log.high_watermark().raw(), 10);
    assert_eq!(log.log_start_offset().raw(), 0);
    let all = log.read(Offset::ZERO, 100).unwrap();
    assert_eq!(all.len(), 10);
    assert_eq!(all[9].value.as_ref(), b"m9");
    assert_eq!(all[9].offset.raw(), 9);

    // Append after recovery continues monotonically.
    // Need mut — reopen again as mut
    drop(log);
    let mut log = PartitionLog::open(config(&dir)).unwrap();
    let r = log.append(Message::from_value("after")).unwrap();
    assert_eq!(r.offset.raw(), 10);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn torn_tail_recovery() {
    let dir = tmp_dir("torn");
    let log_file;
    {
        let mut log = PartitionLog::open(config(&dir)).unwrap();
        log.append(Message::from_value("keep-a")).unwrap();
        log.append(Message::from_value("keep-b")).unwrap();
        log.append(Message::from_value("torn-me")).unwrap();
        log.flush().unwrap();

        // Find the only .log file and truncate a few bytes.
        let entry = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().ends_with(".log"))
            .expect("log file");
        log_file = entry.path();
        let meta = fs::metadata(&log_file).unwrap();
        let size = meta.len();
        assert!(size > 10);
        // Truncate last 5 bytes → partial last record.
        let f = OpenOptions::new().write(true).open(&log_file).unwrap();
        f.set_len(size - 5).unwrap();
    }

    let log = PartitionLog::open(config(&dir)).unwrap();
    // Last record should be discarded; first two remain.
    assert_eq!(log.high_watermark().raw(), 2);
    let all = log.read(Offset::ZERO, 100).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].value.as_ref(), b"keep-a");
    assert_eq!(all[1].value.as_ref(), b"keep-b");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn delete_records_and_retention() {
    let dir = tmp_dir("retention");
    let mut cfg = config(&dir);
    cfg.segment_size = 80; // force multiple segments
    let mut log = PartitionLog::open(cfg.clone()).unwrap();

    for i in 0..8 {
        log.append(Message::from_value(format!("rec-{i}"))).unwrap();
    }
    log.flush().unwrap();
    let hwm = log.high_watermark().raw();
    assert_eq!(hwm, 8);

    // delete_records drops whole segments only.
    let new_start = log.delete_records(Offset::new(3)).unwrap();
    assert!(new_start.raw() <= 3);
    // Remaining records should still be readable from new start.
    let rest = log.read(Offset::ZERO, 100).unwrap();
    assert!(!rest.is_empty());
    assert!(rest[0].offset.raw() >= new_start.raw());
    assert_eq!(log.log_start_offset(), new_start);

    // Size retention: set very small limit and apply.
    let mut cfg2 = cfg.clone();
    cfg2.retention_bytes = Some(1); // force dropping oldest segments
    // Re-open with retention config by constructing new log is heavy;
    // instead call apply_retention after mutating via a new open.
    drop(log);

    let mut log = PartitionLog::open(cfg2).unwrap();
    let before = log.log_start_offset();
    let segs_before = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
        .count();
    log.apply_retention().unwrap();
    let segs_after = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
        .count();
    // With multiple segments and tiny retention_bytes, we should drop some (or keep one).
    assert!(segs_after <= segs_before);
    assert!(segs_after >= 1);
    assert!(log.log_start_offset().raw() >= before.raw());
    // High watermark unchanged by retention.
    assert_eq!(log.high_watermark().raw(), hwm);

    // Time retention: everything is "recent", so no extra drops required.
    let mut cfg3 = config(&dir);
    cfg3.retention_ms = Some(60_000 * 60 * 24); // 1 day
    let mut log = PartitionLog::open(cfg3).unwrap();
    log.apply_retention().unwrap();
    assert_eq!(log.high_watermark().raw(), hwm);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn read_bytes_respects_limits() {
    let dir = tmp_dir("bytes");
    let mut log = PartitionLog::open(config(&dir)).unwrap();
    for i in 0..5 {
        log.append(Message::from_value(vec![b'x'; 100]))
            .unwrap();
        let _ = i;
    }
    let few = log.read_bytes(Offset::ZERO, 100, 150).unwrap();
    // At least one message, but not all five given tight byte budget.
    assert!(!few.is_empty());
    assert!(few.len() < 5);

    let _ = fs::remove_dir_all(&dir);
}
