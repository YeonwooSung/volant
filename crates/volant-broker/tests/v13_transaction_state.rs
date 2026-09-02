//! v0.13: opt-in `__transaction_state` coordinator log (KIP-890 JSON MVP).

#[path = "common/mod.rs"]
mod common;
use common::temp_dir;

use std::sync::{Mutex, OnceLock};

use bytes::Bytes;
use volant_broker::{
    Broker, TRANSACTION_STATE_HEADER, TRANSACTION_STATE_TOPIC, TXN_STATE_COMPLETE_ABORT,
    TXN_STATE_COMPLETE_COMMIT, TXN_STATE_PREPARE_COMMIT,
};
use volant_core::{Message, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct EnvRestore {
    key: &'static str,
    prev: Option<String>,
}

impl EnvRestore {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        // SAFETY: tests serialize env mutations via env_lock.
        std::env::set_var(key, value);
        Self { key, prev }
    }
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

fn broker_off(label: &str) -> (std::path::PathBuf, Broker) {
    let dir = temp_dir("v13", label);
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    (dir, broker)
}

fn broker_on(label: &str) -> (EnvRestore, std::path::PathBuf, Broker) {
    let env = EnvRestore::set("VOLANT_TRANSACTION_STATE_TOPIC", "1");
    let dir = temp_dir("v13", label);
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    (env, dir, broker)
}

fn reopen(dir: &std::path::PathBuf) -> Broker {
    Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    })
}

fn last_state(broker: &Broker, tid: &str) -> Option<String> {
    broker
        .read_transaction_state_latest()
        .into_iter()
        .find(|(k, _)| k == tid)
        .map(|(_, r)| r.state)
}

fn prepare_2pc(broker: &Broker, tid: &str, topic: &str) -> (u64, u16) {
    let r = broker.init_producer_id_with_opts(tid, true, false, 60_000);
    assert_eq!(r.error_code, 0);
    let pid = r.producer_id;
    let epoch = r.epoch;
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    match broker.buffer_txn_produce(
        pid,
        epoch,
        topic,
        0,
        0,
        vec![Message::from_value(Bytes::from_static(b"hello"))],
    ) {
        volant_broker::IdempotentCheck::Accept { .. } => {}
        other => panic!("produce rejected: {other:?}"),
    }
    let (err, _, _) = broker.end_txn(pid, epoch, true, &[]).unwrap();
    assert_eq!(err, 0);
    (pid, epoch)
}

#[test]
fn flag_off_does_not_create_topic() {
    let _g = env_lock().lock().unwrap();
    let (dir, broker) = broker_off("flag-off");
    assert!(!broker.transaction_state_topic_enabled());
    broker.create_topic("events", 1).unwrap();
    let (pid, epoch) = prepare_2pc(&broker, "txn-off", "events");
    assert_eq!(
        broker.describe_transaction("txn-off").unwrap().0,
        "PrepareCommit"
    );
    assert!(
        !broker
            .list_topics()
            .iter()
            .any(|t| t.as_str() == TRANSACTION_STATE_TOPIC),
        "__transaction_state must not auto-create when flag is off"
    );
    let (err, _, _) = broker.end_txn(pid, epoch, true, &[]).unwrap();
    assert_eq!(err, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flag_on_prepare_writes_prepare_commit() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("prep");
    broker.create_topic("events", 1).unwrap();
    let (_pid, _epoch) = prepare_2pc(&broker, "txn-prep", "events");
    assert_eq!(
        broker.describe_transaction("txn-prep").unwrap().0,
        "PrepareCommit"
    );
    assert!(broker
        .list_topics()
        .iter()
        .any(|t| t.as_str() == TRANSACTION_STATE_TOPIC));
    let recs = broker.read_transaction_state_log();
    assert!(
        recs.iter()
            .any(|(k, r)| k == "txn-prep" && r.state == TXN_STATE_PREPARE_COMMIT),
        "expected prepare_commit, got {recs:?}"
    );
    assert_eq!(
        last_state(&broker, "txn-prep").as_deref(),
        Some(TXN_STATE_PREPARE_COMMIT)
    );
    let rec = recs
        .iter()
        .rev()
        .find(|(k, _)| k == "txn-prep")
        .map(|(_, r)| r)
        .unwrap();
    assert_eq!(rec.v, 1);
    assert!(rec.state.eq(TXN_STATE_PREPARE_COMMIT));
    let fetched = broker
        .fetch(
            &TopicName::new(TRANSACTION_STATE_TOPIC),
            PartitionId(0),
            volant_core::Offset::ZERO,
            64,
        )
        .unwrap();
    assert!(fetched.iter().any(|r| {
        r.headers
            .iter()
            .any(|(k, v)| k == TRANSACTION_STATE_HEADER && v.as_ref() == b"1")
    }));
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn complete_end_txn_writes_complete_commit() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("complete");
    broker.create_topic("events", 1).unwrap();
    let (pid, epoch) = prepare_2pc(&broker, "txn-done", "events");
    let (err, _, _) = broker.end_txn(pid, epoch, true, &[]).unwrap();
    assert_eq!(err, 0);
    assert_eq!(broker.describe_transaction("txn-done").unwrap().0, "Empty");
    assert_eq!(
        last_state(&broker, "txn-done").as_deref(),
        Some(TXN_STATE_COMPLETE_COMMIT)
    );
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn prepared_survives_restart_with_flag_on() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("restart");
    broker.create_topic("events", 1).unwrap();
    let (pid, epoch) = prepare_2pc(&broker, "txn-dur", "events");
    assert_eq!(
        broker.describe_transaction("txn-dur").unwrap().0,
        "PrepareCommit"
    );
    drop(broker);

    let broker2 = reopen(&dir);
    assert!(broker2.transaction_state_topic_enabled());
    let desc = broker2.describe_transaction("txn-dur").unwrap();
    assert_eq!(desc.0, "PrepareCommit");
    assert_eq!(desc.3, pid);
    assert_eq!(desc.4, epoch);
    // KeepPreparedTxn returns OngoingTxn*.
    let keep = broker2.init_producer_id_with_opts("txn-dur", true, true, 60_000);
    assert_eq!(keep.error_code, 0);
    assert_eq!(keep.producer_id, pid);
    assert_eq!(keep.epoch, epoch);
    assert_eq!(keep.ongoing_txn_producer_id, pid as i64);
    assert_eq!(keep.ongoing_txn_producer_epoch, epoch as i16);
    let (err, _, _) = broker2.end_txn(pid, epoch, true, &[]).unwrap();
    assert_eq!(err, 0);
    assert_eq!(broker2.describe_transaction("txn-dur").unwrap().0, "Empty");
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn replay_rebuilds_prepared_when_file_missing() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("replay");
    broker.create_topic("events", 1).unwrap();
    let (pid, epoch) = prepare_2pc(&broker, "txn-replay", "events");
    drop(broker);
    let _ = std::fs::remove_dir_all(dir.join("__txn_prepared"));

    let broker2 = reopen(&dir);
    let desc = broker2.describe_transaction("txn-replay").unwrap();
    assert_eq!(desc.0, "PrepareCommit");
    let keep = broker2.init_producer_id_with_opts("txn-replay", true, true, 60_000);
    assert_eq!(keep.error_code, 0);
    assert_eq!(keep.ongoing_txn_producer_id, pid as i64);
    assert_eq!(keep.ongoing_txn_producer_epoch, epoch as i16);
    let (err, _, _) = broker2.end_txn(pid, epoch, true, &[]).unwrap();
    assert_eq!(err, 0);
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn fence_abort_writes_complete_abort() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("fence");
    broker.create_topic("events", 1).unwrap();
    let (pid, epoch) = prepare_2pc(&broker, "txn-fence", "events");
    // KeepPreparedTxn=false → force-abort prepared (single complete_abort record).
    let fenced = broker.init_producer_id_with_opts("txn-fence", true, false, 60_000);
    assert_eq!(fenced.error_code, 0);
    assert_eq!(fenced.producer_id, pid);
    assert_ne!(fenced.epoch, epoch);
    assert_eq!(fenced.ongoing_txn_producer_id, -1);
    assert_eq!(broker.describe_transaction("txn-fence").unwrap().0, "Empty");
    let log = broker.read_transaction_state_log();
    assert!(
        log.iter()
            .any(|(k, r)| k == "txn-fence" && r.state == TXN_STATE_COMPLETE_ABORT),
        "fence abort must write complete_abort, got {log:?}"
    );
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn topic_not_created_until_first_init() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("lazy");
    assert!(!broker
        .list_topics()
        .iter()
        .any(|t| t.as_str() == TRANSACTION_STATE_TOPIC));
    let r = broker.init_producer_id_with_opts("txn-lazy", true, false, 60_000);
    assert_eq!(r.error_code, 0);
    assert!(broker
        .list_topics()
        .iter()
        .any(|t| t.as_str() == TRANSACTION_STATE_TOPIC));
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}
