//! v0.20 produce group-commit (storage fsync coalescing).

use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use volant_core::{Message, Offset};
use volant_storage::{
    PartitionLog, SharedPartitionLog, StorageConfig, GROUP_COMMIT_MAX_RECORDS_ENV,
    GROUP_COMMIT_MS_ENV,
};

fn tmp_dir(label: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = N.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "volant-v20-{label}-{}-{}-{}",
        std::process::id(),
        n,
        nanos
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(dir: &std::path::Path, ms: u64, flush_every_n: u64, max_records: u64) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        segment_size: 256 * 1024 * 1024,
        use_mmap: true,
        flush_every_n,
        group_commit_max_ms: ms,
        group_commit_max_records: max_records,
        ..StorageConfig::default()
    }
}

#[test]
fn group_commit_off_flush_every_n_still_fsyncs() {
    let dir = tmp_dir("off");
    let mut log = PartitionLog::open(config(&dir, 0, 1, 0)).unwrap();

    let start = Instant::now();
    log.append(Message::from_value("a")).unwrap();
    log.append(Message::from_value("b")).unwrap();
    // No group-commit window: two appends with flush_every_n=1 → two fsyncs,
    // and no extra wait.
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "group_commit_max_ms=0 must not add a time wait"
    );
    assert_eq!(log.fsync_count(), 2);
    assert_eq!(log.group_commit_flushes(), 0);

    let got = log.read(Offset::ZERO, 10).unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].value.as_ref(), b"a");
    assert_eq!(got[1].value.as_ref(), b"b");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn concurrent_appenders_share_fsync() {
    let dir = tmp_dir("share");
    let log = SharedPartitionLog::open(config(&dir, 50, 0, 64)).unwrap();
    let barrier = Arc::new(Barrier::new(2));

    let a = {
        let log = log.clone();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            log.append(Message::from_value("one"))
        })
    };
    let b = {
        let log = log.clone();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            log.append(Message::from_value("two"))
        })
    };

    let r1 = a.join().unwrap().unwrap();
    let r2 = b.join().unwrap().unwrap();
    assert_ne!(r1.offset.raw(), r2.offset.raw());

    let fsyncs = log.fsync_count();
    assert!(
        fsyncs >= 1 && fsyncs <= 2,
        "expected 1 or 2 fsyncs for 2 appends, got {fsyncs}"
    );
    assert!(fsyncs <= 2);

    let got = log.read(Offset::ZERO, 10).unwrap();
    assert_eq!(got.len(), 2);
    let mut values: Vec<_> = got.iter().map(|r| r.value.as_ref().to_vec()).collect();
    values.sort();
    assert_eq!(values, [b"one".to_vec(), b"two".to_vec()]);

    drop(log);
    let reopened = PartitionLog::open(config(&dir, 50, 0, 64)).unwrap();
    let again = reopened.read(Offset::ZERO, 10).unwrap();
    assert_eq!(
        again.len(),
        2,
        "both records must be durable after group commit"
    );
    let mut values: Vec<_> = again.iter().map(|r| r.value.as_ref().to_vec()).collect();
    values.sort();
    assert_eq!(values, [b"one".to_vec(), b"two".to_vec()]);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn group_commit_durable_on_reopen() {
    let dir = tmp_dir("reopen");
    {
        let log = SharedPartitionLog::open(config(&dir, 20, 0, 2)).unwrap();
        log.append(Message::from_value("keep-a")).unwrap();
        log.append(Message::from_value("keep-b")).unwrap();
        assert!(log.fsync_count() >= 1);
    }

    let log = PartitionLog::open(config(&dir, 0, 0, 0)).unwrap();
    assert_eq!(log.high_watermark().raw(), 2);
    let all = log.read(Offset::ZERO, 10).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].value.as_ref(), b"keep-a");
    assert_eq!(all[1].value.as_ref(), b"keep-b");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn empty_no_waiters_does_not_spin() {
    let dir = tmp_dir("idle");
    let log = PartitionLog::open(config(&dir, 50, 0, 64)).unwrap();
    thread::sleep(Duration::from_millis(30));
    assert_eq!(log.fsync_count(), 0);
    assert_eq!(log.group_commit_flushes(), 0);
    assert!(!log.has_uncommitted());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn apply_group_commit_env_parses() {
    let prev_ms = std::env::var(GROUP_COMMIT_MS_ENV).ok();
    let prev_n = std::env::var(GROUP_COMMIT_MAX_RECORDS_ENV).ok();
    std::env::set_var(GROUP_COMMIT_MS_ENV, "25");
    std::env::set_var(GROUP_COMMIT_MAX_RECORDS_ENV, "8");
    let mut cfg = StorageConfig::default();
    cfg.apply_group_commit_env();
    assert_eq!(cfg.group_commit_max_ms, 25);
    assert_eq!(cfg.group_commit_max_records, 8);
    assert!(cfg.group_commit_enabled());
    match prev_ms {
        Some(v) => std::env::set_var(GROUP_COMMIT_MS_ENV, v),
        None => std::env::remove_var(GROUP_COMMIT_MS_ENV),
    }
    match prev_n {
        Some(v) => std::env::set_var(GROUP_COMMIT_MAX_RECORDS_ENV, v),
        None => std::env::remove_var(GROUP_COMMIT_MAX_RECORDS_ENV),
    }
}

#[test]
fn default_max_records_inherits_flush_every_n() {
    let mut cfg = StorageConfig::default();
    cfg.group_commit_max_ms = 10;
    cfg.flush_every_n = 7;
    assert_eq!(cfg.effective_group_commit_max_records(), 7);
    cfg.group_commit_max_records = 3;
    assert_eq!(cfg.effective_group_commit_max_records(), 3);
    cfg.flush_every_n = 0;
    cfg.group_commit_max_records = 0;
    assert_eq!(cfg.effective_group_commit_max_records(), 64);
}
