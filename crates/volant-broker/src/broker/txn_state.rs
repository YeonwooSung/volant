//! Opt-in `__transaction_state` coordinator log (v0.13 / KIP-890 MVP).
//!
//! Volant JSON records, not Kafka KRaft / KIP-890 schemas. Default **off**.

use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use volant_core::{Message, MessageBatch, Offset, PartitionId, TopicName};

use super::*;

/// Internal transaction-state topic name (v0.13).
pub const TRANSACTION_STATE_TOPIC: &str = "__transaction_state";

/// Record header key (`1` = this JSON schema).
pub const TRANSACTION_STATE_HEADER: &str = "volant-txn-state";

/// Env knob: `1` / `true` / `yes` enables the topic (default **off**).
pub const ENV_TRANSACTION_STATE_TOPIC: &str = "VOLANT_TRANSACTION_STATE_TOPIC";

/// JSON schema version written into each record.
pub const TRANSACTION_STATE_RECORD_VERSION: u8 = 1;

/// Coordinator log states (Volant JSON, not Kafka `TxnState`).
pub const TXN_STATE_EMPTY: &str = "empty";
/// Open write-through txn.
pub const TXN_STATE_ONGOING: &str = "ongoing";
/// First EndTxn commit (Enable2Pc).
pub const TXN_STATE_PREPARE_COMMIT: &str = "prepare_commit";
/// First EndTxn abort (Enable2Pc).
pub const TXN_STATE_PREPARE_ABORT: &str = "prepare_abort";
/// Second EndTxn / one-shot commit.
pub const TXN_STATE_COMPLETE_COMMIT: &str = "complete_commit";
/// Second EndTxn / one-shot / fence / timeout abort.
pub const TXN_STATE_COMPLETE_ABORT: &str = "complete_abort";

/// One `__transaction_state` JSON value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionStateRecord {
    /// Schema version (`1`).
    pub v: u8,
    /// `empty|ongoing|prepare_commit|prepare_abort|complete_commit|complete_abort`.
    pub state: String,
    /// Producer id at the transition.
    pub producer_id: u64,
    /// Producer epoch at the transition.
    pub epoch: u16,
    /// Txn start / prepare clock (unix ms). `0` when unknown / empty.
    pub txn_start_ms: i64,
}

/// Parse `VOLANT_TRANSACTION_STATE_TOPIC` (default **off**).
pub fn transaction_state_topic_enabled_from_env() -> bool {
    match std::env::var(ENV_TRANSACTION_STATE_TOPIC) {
        Ok(s) => {
            let t = s.trim();
            t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes")
        }
        Err(_) => false,
    }
}

impl Broker {
    /// Whether this broker was constructed with the v0.13 topic enabled.
    pub fn transaction_state_topic_enabled(&self) -> bool {
        self.transaction_state_topic
    }

    /// Replication factor for `__transaction_state`: `min(3, N)`, or `1` single-node.
    pub(super) fn transaction_state_replication_factor(&self) -> u32 {
        match &self.cluster {
            None => 1,
            Some(c) => {
                let n = c.config.read().brokers.len().max(1) as u32;
                3u32.min(n).max(1)
            }
        }
    }

    /// Create `__transaction_state` (1 partition) if missing. Best-effort.
    pub(super) fn ensure_transaction_state_topic(&self) {
        if !self.transaction_state_topic {
            return;
        }
        let name = TopicName::new(TRANSACTION_STATE_TOPIC);
        if self.topics.read().contains_key(&name) {
            return;
        }
        if let Some(cluster) = &self.cluster {
            if cluster
                .assignment
                .read()
                .topics
                .contains_key(TRANSACTION_STATE_TOPIC)
            {
                return;
            }
            if !self.is_controller() {
                return;
            }
        }
        let rf = self.transaction_state_replication_factor();
        let _ = self.create_topic_with_replication(name, 1, rf);
    }

    /// Append one last-write-wins coordinator record. Best-effort; never fails APIs.
    pub(super) fn append_transaction_state(
        &self,
        transactional_id: &str,
        state: &str,
        producer_id: u64,
        epoch: u16,
        txn_start_ms: i64,
    ) {
        if !self.transaction_state_topic || transactional_id.is_empty() {
            return;
        }
        self.ensure_transaction_state_topic();
        let rec = TransactionStateRecord {
            v: TRANSACTION_STATE_RECORD_VERSION,
            state: state.to_owned(),
            producer_id,
            epoch,
            txn_start_ms,
        };
        let Ok(json) = serde_json::to_vec(&rec) else {
            return;
        };
        let mut msg = Message::with_key(
            Bytes::copy_from_slice(transactional_id.as_bytes()),
            Bytes::from(json),
        );
        msg.headers.push((
            TRANSACTION_STATE_HEADER.to_string(),
            Bytes::from_static(b"1"),
        ));
        let mut batch = MessageBatch::default();
        batch.messages.push(msg);
        let name = TopicName::new(TRANSACTION_STATE_TOPIC);
        if self.produce(&name, PartitionId(0), batch).is_ok() {
            let _ = self.flush(&name, PartitionId(0));
        }
    }

    /// Replay `__transaction_state-0` last-write-wins when the flag is on.
    ///
    /// Topic is SoT for state when present. Prepared **bodies** (ranges) come
    /// from `__txn_prepared` when the same id is still `prepare_*`. Missing
    /// file + `prepare_*` on the topic rebuilds a stub so KeepPrepared works.
    pub(super) fn replay_transaction_state_topic(&self) {
        if !self.transaction_state_topic {
            return;
        }
        let name = TopicName::new(TRANSACTION_STATE_TOPIC);
        if !self.topics.read().contains_key(&name) {
            return;
        }
        let latest = self.read_transaction_state_latest();
        if latest.is_empty() {
            return;
        }

        let mut mutated_prepared = false;
        for (tid, rec) in latest {
            match rec.state.as_str() {
                TXN_STATE_PREPARE_COMMIT | TXN_STATE_PREPARE_ABORT => {
                    let commit = rec.state == TXN_STATE_PREPARE_COMMIT;
                    let mut prepared = self.prepared_txns.lock();
                    if let Some(prep) = prepared.get_mut(&tid) {
                        prep.commit = commit;
                        prep.producer_id = rec.producer_id;
                        prep.producer_epoch = rec.epoch;
                        if rec.txn_start_ms > 0 {
                            prep.prepared_at_ms = rec.txn_start_ms;
                        }
                    } else {
                        let start = if rec.txn_start_ms > 0 {
                            rec.txn_start_ms
                        } else {
                            unix_now_ms()
                        };
                        prepared.insert(
                            tid.clone(),
                            PreparedTxn {
                                transactional_id: tid.clone(),
                                producer_id: rec.producer_id,
                                producer_epoch: rec.epoch,
                                commit,
                                prepared_at_ms: start,
                                open: OpenTxn {
                                    opened_at_ms: start,
                                    producer_epoch: rec.epoch,
                                    ..OpenTxn::default()
                                },
                            },
                        );
                    }
                    drop(prepared);
                    self.open_txns.lock().remove(&rec.producer_id);
                    self.ensure_txn_identity(&tid, rec.producer_id, rec.epoch);
                    mutated_prepared = true;
                }
                TXN_STATE_ONGOING => {
                    self.prepared_txns.lock().remove(&tid);
                    {
                        let mut open = self.open_txns.lock();
                        open.entry(rec.producer_id).or_insert_with(|| OpenTxn {
                            opened_at_ms: rec.txn_start_ms,
                            producer_epoch: rec.epoch,
                            ..OpenTxn::default()
                        });
                    }
                    self.ensure_txn_identity(&tid, rec.producer_id, rec.epoch);
                    mutated_prepared = true;
                }
                TXN_STATE_EMPTY | TXN_STATE_COMPLETE_COMMIT | TXN_STATE_COMPLETE_ABORT => {
                    if self.prepared_txns.lock().remove(&tid).is_some() {
                        mutated_prepared = true;
                    }
                    self.open_txns.lock().remove(&rec.producer_id);
                    self.ensure_txn_identity(&tid, rec.producer_id, rec.epoch);
                }
                _ => {}
            }
        }
        if mutated_prepared {
            self.persist_prepared_txns();
        }
    }

    fn ensure_txn_identity(&self, transactional_id: &str, producer_id: u64, epoch: u16) {
        self.transactional_ids
            .write()
            .insert(transactional_id.to_owned(), producer_id);
        let mut state = self.producer_state.write();
        if let Some(prod) = state.get_mut(&producer_id) {
            prod.transactional = true;
            prod.transactional_id = transactional_id.to_owned();
            if prod.epoch == 0 {
                prod.epoch = epoch;
            }
        } else {
            state.insert(
                producer_id,
                ProducerEpochState {
                    epoch,
                    transactional: true,
                    transactional_id: transactional_id.to_owned(),
                    enable_2pc: true,
                    transaction_timeout_ms: 0,
                    partitions: HashMap::new(),
                },
            );
        }
    }

    /// Last-write-wins records from `__transaction_state-0` (tests / replay).
    pub fn read_transaction_state_latest(&self) -> Vec<(String, TransactionStateRecord)> {
        let name = TopicName::new(TRANSACTION_STATE_TOPIC);
        let mut from = Offset::ZERO;
        let mut latest: HashMap<String, TransactionStateRecord> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        loop {
            let batch = match self.fetch_kafka(&name, PartitionId(0), from, 1024, false) {
                Ok(b) if !b.is_empty() => b,
                _ => break,
            };
            let next = batch.last().map(|r| r.offset.raw().saturating_add(1));
            for r in batch {
                let Some(key) = r.key.as_ref() else {
                    continue;
                };
                let tid = String::from_utf8_lossy(key).into_owned();
                let Ok(rec) = serde_json::from_slice::<TransactionStateRecord>(&r.value) else {
                    continue;
                };
                if !latest.contains_key(&tid) {
                    order.push(tid.clone());
                }
                latest.insert(tid, rec);
            }
            match next {
                Some(n) if n > from.raw() => from = Offset::new(n),
                _ => break,
            }
        }
        order
            .into_iter()
            .filter_map(|tid| latest.remove(&tid).map(|rec| (tid, rec)))
            .collect()
    }

    /// All `__transaction_state` records in log order (tests).
    pub fn read_transaction_state_log(&self) -> Vec<(String, TransactionStateRecord)> {
        let name = TopicName::new(TRANSACTION_STATE_TOPIC);
        let mut from = Offset::ZERO;
        let mut out = Vec::new();
        loop {
            let batch = match self.fetch(&name, PartitionId(0), from, 1024) {
                Ok(b) if !b.is_empty() => b,
                _ => break,
            };
            let next = batch.last().map(|r| r.offset.raw().saturating_add(1));
            for r in batch {
                let Some(key) = r.key.as_ref() else {
                    continue;
                };
                let tid = String::from_utf8_lossy(key).into_owned();
                let Ok(rec) = serde_json::from_slice::<TransactionStateRecord>(&r.value) else {
                    continue;
                };
                out.push((tid, rec));
            }
            match next {
                Some(n) if n > from.raw() => from = Offset::new(n),
                _ => break,
            }
        }
        out
    }
}
