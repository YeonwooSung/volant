//! v0.232: populate `TransactionLogValue.partitions` for ongoing / prepare_*.

#[path = "common/mod.rs"]
mod common;
use common::temp_dir;

use std::sync::{Mutex, OnceLock};

use bytes::Bytes;
use volant_broker::{
    decode_transaction_log_key, decode_transaction_log_value, encode_transaction_log_value,
    parse_transaction_state_record, Broker, TransactionLogValue, TRANSACTION_STATE_FMT_KAFKA_V0,
    TRANSACTION_STATE_HEADER, TRANSACTION_STATE_TOPIC, TXN_LOG_STATUS_COMPLETE_ABORT,
    TXN_LOG_STATUS_COMPLETE_COMMIT, TXN_LOG_STATUS_ONGOING, TXN_LOG_STATUS_PREPARE_ABORT,
    TXN_LOG_STATUS_PREPARE_COMMIT,
};
use volant_core::{Message, Offset, PartitionId, TopicName};
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
    let dir = temp_dir("v232", label);
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    (dir, broker)
}

fn broker_on(label: &str) -> (EnvRestore, std::path::PathBuf, Broker) {
    let env = EnvRestore::set("VOLANT_TRANSACTION_STATE_TOPIC", "1");
    let dir = temp_dir("v232", label);
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    (env, dir, broker)
}

fn last_raw(broker: &Broker, tid: &str) -> Option<(Vec<u8>, Vec<u8>, Vec<(String, Bytes)>)> {
    let fetched = broker
        .fetch(
            &TopicName::new(TRANSACTION_STATE_TOPIC),
            PartitionId(0),
            Offset::ZERO,
            64,
        )
        .ok()?;
    fetched.into_iter().rev().find_map(|r| {
        let key = r.key.as_ref()?.as_ref();
        let parsed = parse_transaction_state_record(key, &r.value, &r.headers)?;
        if parsed.0 == tid {
            Some((key.to_vec(), r.value.to_vec(), r.headers))
        } else {
            None
        }
    })
}

fn last_value(broker: &Broker, tid: &str) -> TransactionLogValue {
    let (key, value, headers) = last_raw(broker, tid).expect("last record");
    assert!(headers.iter().any(|(k, v)| {
        k == TRANSACTION_STATE_HEADER && v.as_ref() == TRANSACTION_STATE_FMT_KAFKA_V0
    }));
    assert_eq!(decode_transaction_log_key(&key).as_deref(), Some(tid));
    decode_transaction_log_value(&value).expect("kafka value")
}

fn begin_and_write(
    broker: &Broker,
    tid: &str,
    enable_2pc: bool,
    topic: &str,
    extra_added: &[(String, u32)],
) -> (u64, u16) {
    let r = broker.init_producer_id_with_opts(tid, enable_2pc, false, 60_000);
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
    if !extra_added.is_empty() {
        assert_eq!(broker.record_txn_added_partitions(pid, extra_added), 0);
    }
    (pid, epoch)
}

#[test]
fn encode_decode_roundtrip_some_partitions() {
    let value = TransactionLogValue {
        version: 0,
        producer_id: 11,
        producer_epoch: 3,
        transaction_timeout_ms: 0,
        transaction_status: TXN_LOG_STATUS_ONGOING,
        partitions: Some(vec![
            ("events".into(), vec![0, 2]),
            ("other".into(), vec![1]),
        ]),
        transaction_last_update_timestamp_ms: 8,
        transaction_start_timestamp_ms: 4,
    };
    let bytes = encode_transaction_log_value(&value);
    let dec = decode_transaction_log_value(&bytes).expect("v0 value");
    assert_eq!(dec, value);
    assert_eq!(
        dec.partitions,
        Some(vec![
            ("events".into(), vec![0, 2]),
            ("other".into(), vec![1]),
        ])
    );
}

#[test]
fn flag_on_prepare_writes_open_partitions() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("prep-parts");
    broker.create_topic("events", 2).unwrap();
    broker.create_topic("other", 1).unwrap();
    let (pid, epoch) = begin_and_write(
        &broker,
        "txn-prep",
        true,
        "events",
        &[("other".into(), 0), ("events".into(), 1)],
    );
    let (err, _, _) = broker.end_txn(pid, epoch, true, &[]).unwrap();
    assert_eq!(err, 0);
    assert_eq!(
        broker.describe_transaction("txn-prep").unwrap().0,
        "PrepareCommit"
    );
    let decoded = last_value(&broker, "txn-prep");
    assert_eq!(decoded.transaction_status, TXN_LOG_STATUS_PREPARE_COMMIT);
    assert_eq!(
        decoded.partitions,
        Some(vec![
            ("events".into(), vec![0, 1]),
            ("other".into(), vec![0]),
        ])
    );
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flag_on_prepare_abort_writes_open_partitions() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("prep-abort-parts");
    broker.create_topic("events", 1).unwrap();
    let (pid, epoch) = begin_and_write(&broker, "txn-prep-abort", true, "events", &[]);
    let (err, _, _) = broker.end_txn(pid, epoch, false, &[]).unwrap();
    assert_eq!(err, 0);
    let decoded = last_value(&broker, "txn-prep-abort");
    assert_eq!(decoded.transaction_status, TXN_LOG_STATUS_PREPARE_ABORT);
    assert_eq!(
        decoded.partitions,
        Some(vec![("events".into(), vec![0])])
    );
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flag_on_complete_commit_partitions_null() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("complete-commit");
    broker.create_topic("events", 1).unwrap();
    let (pid, epoch) = begin_and_write(&broker, "txn-done", true, "events", &[]);
    let (err, _, _) = broker.end_txn(pid, epoch, true, &[]).unwrap();
    assert_eq!(err, 0);
    let (err, _, _) = broker.end_txn(pid, epoch, true, &[]).unwrap();
    assert_eq!(err, 0);
    assert_eq!(broker.describe_transaction("txn-done").unwrap().0, "Empty");
    let decoded = last_value(&broker, "txn-done");
    assert_eq!(decoded.transaction_status, TXN_LOG_STATUS_COMPLETE_COMMIT);
    assert!(decoded.partitions.is_none());
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flag_on_complete_abort_partitions_null() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("complete-abort");
    broker.create_topic("events", 1).unwrap();
    let (pid, epoch) = begin_and_write(&broker, "txn-abort", false, "events", &[]);
    let (err, _, _) = broker.end_txn(pid, epoch, false, &[]).unwrap();
    assert_eq!(err, 0);
    assert_eq!(broker.describe_transaction("txn-abort").unwrap().0, "Empty");
    let decoded = last_value(&broker, "txn-abort");
    assert_eq!(decoded.transaction_status, TXN_LOG_STATUS_COMPLETE_ABORT);
    assert!(decoded.partitions.is_none());
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flag_off_does_not_create_topic() {
    let _g = env_lock().lock().unwrap();
    let (dir, broker) = broker_off("flag-off");
    assert!(!broker.transaction_state_topic_enabled());
    broker.create_topic("events", 1).unwrap();
    let (pid, epoch) = begin_and_write(&broker, "txn-off", false, "events", &[]);
    let (err, _, _) = broker.end_txn(pid, epoch, false, &[]).unwrap();
    assert_eq!(err, 0);
    assert!(
        !broker
            .list_topics()
            .iter()
            .any(|t| t.as_str() == TRANSACTION_STATE_TOPIC),
        "__transaction_state must not auto-create when flag is off"
    );
    drop(broker);
    let _ = std::fs::remove_dir_all(&dir);
}
