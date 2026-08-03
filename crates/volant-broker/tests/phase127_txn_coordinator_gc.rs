//! Phase 127: txn coordinator registry TTL GC.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use volant_broker::{
    Broker, DEFAULT_TXN_COORDINATOR_TTL_MS, TXN_COORDINATOR_DIR, TXN_COORDINATOR_FILE,
};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p127-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Guard(PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[test]
fn expire_stale_drops_old_mappings() {
    let base = unique_dir("expire");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: base.clone(),
        ..StorageConfig::default()
    }));
    let reg = broker.txn_coordinator_registry();
    reg.note("keep", 1, 2);
    reg.note("drop-me", 2, 3);
    reg.test_set_id_last_ms("drop-me", 100);
    reg.test_set_pid_last_ms(2, 100);
    let now = now_ms().max(1_000_000);
    reg.test_set_id_last_ms("keep", now);
    reg.test_set_pid_last_ms(1, now);

    let n = reg.expire_stale(60_000, now);
    assert!(n >= 2);
    assert!(reg.resolve_by_id("drop-me").is_none());
    assert_eq!(reg.resolve_by_id("keep"), Some(2));
    assert!(broker.txn_coordinator_registry_gc_total() >= 2);
}

#[test]
fn sweep_timeouts_runs_registry_gc_with_env_ttl() {
    let base = unique_dir("sweep");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: base.clone(),
        ..StorageConfig::default()
    }));
    // Process-local knob (avoid env races with parallel tests).
    broker.set_txn_coordinator_ttl_ms(50);
    let reg = broker.txn_coordinator_registry();
    reg.note("old", 9, 1);
    reg.test_set_id_last_ms("old", 1);
    reg.test_set_pid_last_ms(9, 1);

    // Wait past 50ms wall TTL, then sweep.
    std::thread::sleep(std::time::Duration::from_millis(80));
    let _ = broker.sweep_timeouts();
    assert!(
        reg.resolve_by_id("old").is_none(),
        "stale entry should be GC'd by sweep"
    );
}

#[test]
fn ttl_zero_disables_gc() {
    let base = unique_dir("disabled");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: base.clone(),
        ..StorageConfig::default()
    }));
    broker.set_txn_coordinator_ttl_ms(0);
    broker.note_txn_coordinator("linger", 5, 1);
    // Force ancient timestamps.
    broker
        .txn_coordinator_registry()
        .test_set_id_last_ms("linger", 1);
    broker
        .txn_coordinator_registry()
        .test_set_pid_last_ms(5, 1);
    assert_eq!(broker.expire_txn_coordinator_registry(), 0);
    assert_eq!(
        broker.txn_coordinator_registry().resolve_by_id("linger"),
        Some(1)
    );
}

#[test]
fn gc_survives_reload() {
    let base = unique_dir("reload");
    let _g = Guard(base.clone());
    {
        let b1 = Broker::new(StorageConfig {
            data_dir: base.clone(),
            ..StorageConfig::default()
        });
        b1.note_txn_coordinator("gone", 3, 2);
        b1.note_txn_coordinator("stay", 4, 2);
        let reg = b1.txn_coordinator_registry();
        let now = now_ms().max(500_000);
        reg.test_set_id_last_ms("gone", 10);
        reg.test_set_pid_last_ms(3, 10);
        reg.test_set_id_last_ms("stay", now);
        reg.test_set_pid_last_ms(4, now);
        assert!(reg.expire_stale(1_000, now) >= 2);
        // Force persist of timestamps for "stay".
        reg.note("stay", 4, 2);
    }
    assert!(base
        .join(TXN_COORDINATOR_DIR)
        .join(TXN_COORDINATOR_FILE)
        .is_file());
    let b2 = Broker::new(StorageConfig {
        data_dir: base.clone(),
        ..StorageConfig::default()
    });
    assert!(b2.txn_coordinator_registry().resolve_by_id("gone").is_none());
    assert_eq!(
        b2.txn_coordinator_registry().resolve_by_id("stay"),
        Some(2)
    );
}

#[test]
fn default_ttl_is_24h() {
    assert_eq!(DEFAULT_TXN_COORDINATOR_TTL_MS, 24 * 60 * 60 * 1000);
}
