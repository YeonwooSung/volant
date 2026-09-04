//! v0.229: opt-in `__transaction_state` writes Kafka TransactionLog v0.

#[path = "common/mod.rs"]
mod common;
use common::temp_dir;

use std::sync::{Mutex, OnceLock};

use bytes::Bytes;
use volant_broker::{
    decode_transaction_log_key, decode_transaction_log_value, encode_transaction_log_key,
    encode_transaction_log_value, parse_transaction_state_record, Broker, TransactionLogValue,
    TransactionStateRecord, TRANSACTION_STATE_FMT_JSON, TRANSACTION_STATE_FMT_KAFKA_V0,
    TRANSACTION_STATE_HEADER, TRANSACTION_STATE_TOPIC, TXN_LOG_STATUS_COMPLETE_ABORT,
    TXN_STATE_COMPLETE_ABORT, TXN_STATE_ONGOING,
};
use volant_core::{Message, MessageBatch, Offset, PartitionId, TopicName};
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
    let dir = temp_dir("v229", label);
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    (dir, broker)
}

fn broker_on(label: &str) -> (EnvRestore, std::path::PathBuf, Broker) {
    let env = EnvRestore::set("VOLANT_TRANSACTION_STATE_TOPIC", "1");
    let dir = temp_dir("v229", label);
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

fn abort_open(broker: &Broker, tid: &str, topic: &str) -> (u64, u16) {
    let r = broker.init_producer_id_with_opts(tid, false, false, 60_000);
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
    let (err, _, _) = broker.end_txn(pid, epoch, false, &[]).unwrap();
    assert_eq!(err, 0);
    (pid, epoch)
}

#[test]
fn encode_decode_roundtrip_key_value_v0() {
    let tid = "txn-hex";
    let key = encode_transaction_log_key(tid);
    assert_eq!(decode_transaction_log_key(&key).as_deref(), Some(tid));
    let value = TransactionLogValue {
        version: 0,
        producer_id: 11,
        producer_epoch: 3,
        transaction_timeout_ms: 0,
        transaction_status: TXN_LOG_STATUS_COMPLETE_ABORT,
        partitions: None,
        transaction_last_update_timestamp_ms: 8,
        transaction_start_timestamp_ms: 4,
    };
    let bytes = encode_transaction_log_value(&value);
    let dec = decode_transaction_log_value(&bytes).expect("v0 value");
    assert_eq!(dec, value);
    assert_eq!(dec.transaction_status, 5);
}

#[test]
fn flag_on_end_txn_abort_writes_kafka_complete_abort() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("abort-kafka");
    broker.create_topic("events", 1).unwrap();
    let (_pid, _epoch) = abort_open(&broker, "txn-abort", "events");
    assert_eq!(broker.describe_transaction("txn-abort").unwrap().0, "Empty");
    let (key, value, headers) = last_raw(&broker, "txn-abort").expect("last record");
    assert!(headers.iter().any(|(k, v)| {
        k == TRANSACTION_STATE_HEADER && v.as_ref() == TRANSACTION_STATE_FMT_KAFKA_V0
    }));
    assert_ne!(value.first(), Some(&b'{'));
    assert_eq!(
        decode_transaction_log_key(&key).as_deref(),
        Some("txn-abort")
    );
    let decoded = decode_transaction_log_value(&value).expect("kafka value");
    assert_eq!(decoded.version, 0);
    assert_eq!(decoded.transaction_status, TXN_LOG_STATUS_COMPLETE_ABORT);
    assert_eq!(decoded.transaction_status, 5);
    assert!(decoded.partitions.is_none());
    let latest = broker
        .read_transaction_state_latest()
        .into_iter()
        .find(|(k, _)| k == "txn-abort")
        .map(|(_, r)| r.state);
    assert_eq!(latest.as_deref(), Some(TXN_STATE_COMPLETE_ABORT));
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flag_on_kafka_write_replays_after_reopen() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("replay-kafka");
    broker.create_topic("events", 1).unwrap();
    abort_open(&broker, "txn-reopen", "events");
    drop(broker);

    let broker2 = reopen(&dir);
    assert!(broker2.transaction_state_topic_enabled());
    assert_eq!(
        broker2.describe_transaction("txn-reopen").unwrap().0,
        "Empty"
    );
    let last = broker2
        .read_transaction_state_latest()
        .into_iter()
        .find(|(k, _)| k == "txn-reopen")
        .map(|(_, r)| r.state);
    assert_eq!(last.as_deref(), Some(TXN_STATE_COMPLETE_ABORT));
    let (key, value, _) = last_raw(&broker2, "txn-reopen").expect("replay last");
    assert_eq!(
        decode_transaction_log_key(&key).as_deref(),
        Some("txn-reopen")
    );
    assert_eq!(
        decode_transaction_log_value(&value)
            .expect("kafka value")
            .transaction_status,
        TXN_LOG_STATUS_COMPLETE_ABORT
    );
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn dual_read_json_v1_ongoing_becomes_complete_abort() {
    let _g = env_lock().lock().unwrap();
    let (env, dir, broker) = broker_on("dual-json");
    broker.create_topic(TRANSACTION_STATE_TOPIC, 1).unwrap();
    let rec = TransactionStateRecord {
        v: 1,
        state: TXN_STATE_ONGOING.to_string(),
        producer_id: 42,
        epoch: 0,
        txn_start_ms: 1_700_000_000_000,
    };
    let json = serde_json::to_vec(&rec).unwrap();
    let mut msg = Message::with_key(Bytes::copy_from_slice(b"txn-json"), Bytes::from(json));
    msg.headers.push((
        TRANSACTION_STATE_HEADER.to_string(),
        Bytes::from_static(TRANSACTION_STATE_FMT_JSON),
    ));
    let mut batch = MessageBatch::default();
    batch.messages.push(msg);
    let name = TopicName::new(TRANSACTION_STATE_TOPIC);
    broker.produce(&name, PartitionId(0), batch).unwrap();
    let _ = broker.flush(&name, PartitionId(0));
    drop(broker);

    let broker2 = reopen(&dir);
    assert_eq!(broker2.describe_transaction("txn-json").unwrap().0, "Empty");
    let log = broker2.read_transaction_state_log();
    assert!(
        log.iter()
            .any(|(k, r)| k == "txn-json" && r.state == TXN_STATE_ONGOING),
        "dual-read must still see the seeded JSON ongoing: {log:?}"
    );
    let last = log
        .iter()
        .rev()
        .find(|(k, _)| k == "txn-json")
        .map(|(_, r)| r.state.as_str());
    assert_eq!(last, Some(TXN_STATE_COMPLETE_ABORT));
    let (key, value, headers) = last_raw(&broker2, "txn-json").expect("complete_abort");
    assert!(headers.iter().any(|(k, v)| {
        k == TRANSACTION_STATE_HEADER && v.as_ref() == TRANSACTION_STATE_FMT_KAFKA_V0
    }));
    assert_eq!(
        decode_transaction_log_key(&key).as_deref(),
        Some("txn-json")
    );
    assert_eq!(
        decode_transaction_log_value(&value)
            .expect("kafka complete_abort")
            .transaction_status,
        TXN_LOG_STATUS_COMPLETE_ABORT
    );
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flag_off_does_not_create_topic() {
    let _g = env_lock().lock().unwrap();
    let (dir, broker) = broker_off("flag-off");
    assert!(!broker.transaction_state_topic_enabled());
    broker.create_topic("events", 1).unwrap();
    abort_open(&broker, "txn-off", "events");
    assert!(
        !broker
            .list_topics()
            .iter()
            .any(|t| t.as_str() == TRANSACTION_STATE_TOPIC),
        "__transaction_state must not auto-create when flag is off"
    );
    drop(broker);
    let broker2 = reopen(&dir);
    assert!(!broker2.transaction_state_topic_enabled());
    assert!(!broker2
        .list_topics()
        .iter()
        .any(|t| t.as_str() == TRANSACTION_STATE_TOPIC));
    let _ = std::fs::remove_dir_all(&dir);
}
