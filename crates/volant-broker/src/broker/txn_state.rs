//! Opt-in `__transaction_state` coordinator log (v0.13 / v0.229).
//!
//! Writes Kafka `TransactionLogKey` / `TransactionLogValue` **v0** (classic,
//! not flexible). Dual-reads legacy Volant JSON v1 so existing logs replay.
//! Default **off**. Not full KIP-890/939.

use std::collections::HashMap;

use bytes::{Buf, Bytes};
use serde::{Deserialize, Serialize};
use volant_core::{Message, MessageBatch, Offset, PartitionId, TopicName};

use super::*;

/// Internal transaction-state topic name (v0.13).
pub const TRANSACTION_STATE_TOPIC: &str = "__transaction_state";

/// Record header key (`1` = JSON v1, `2` = Kafka TransactionLog v0).
pub const TRANSACTION_STATE_HEADER: &str = "volant-txn-state";

/// Header value for legacy Volant JSON v1 records (still read).
pub const TRANSACTION_STATE_FMT_JSON: &[u8] = b"1";

/// Header value for Kafka `TransactionLogKey` / `TransactionLogValue` v0 writes.
pub const TRANSACTION_STATE_FMT_KAFKA_V0: &[u8] = b"2";

/// Env knob: `1` / `true` / `yes` enables the topic (default **off**).
pub const ENV_TRANSACTION_STATE_TOPIC: &str = "VOLANT_TRANSACTION_STATE_TOPIC";

/// In-memory [`TransactionStateRecord`] schema version (`v` field).
pub const TRANSACTION_STATE_RECORD_VERSION: u8 = 1;

/// Coordinator log states (Volant strings; Kafka status bytes map to these).
pub const TXN_STATE_EMPTY: &str = "empty";
/// Open write-through txn.
pub const TXN_STATE_ONGOING: &str = "ongoing";
/// First EndTxn commit (Enable2Pc).
pub const TXN_STATE_PREPARE_COMMIT: &str = "prepare_commit";
/// First EndTxn abort (Enable2Pc).
pub const TXN_STATE_PREPARE_ABORT: &str = "prepare_abort";
/// Second EndTxn / one-shot commit.
pub const TXN_STATE_COMPLETE_COMMIT: &str = "complete_commit";
/// Second EndTxn / one-shot / fence / timeout / open crash≡abort.
pub const TXN_STATE_COMPLETE_ABORT: &str = "complete_abort";

/// Kafka `TransactionLogValue.transactionStatus` for [`TXN_STATE_EMPTY`].
pub const TXN_LOG_STATUS_EMPTY: i8 = 0;
/// Kafka status for [`TXN_STATE_ONGOING`].
pub const TXN_LOG_STATUS_ONGOING: i8 = 1;
/// Kafka status for [`TXN_STATE_PREPARE_COMMIT`].
pub const TXN_LOG_STATUS_PREPARE_COMMIT: i8 = 2;
/// Kafka status for [`TXN_STATE_PREPARE_ABORT`].
pub const TXN_LOG_STATUS_PREPARE_ABORT: i8 = 3;
/// Kafka status for [`TXN_STATE_COMPLETE_COMMIT`].
pub const TXN_LOG_STATUS_COMPLETE_COMMIT: i8 = 4;
/// Kafka status for [`TXN_STATE_COMPLETE_ABORT`].
pub const TXN_LOG_STATUS_COMPLETE_ABORT: i8 = 5;
/// Kafka `PrepareEpochFence` — decode as [`TXN_STATE_COMPLETE_ABORT`]; never write.
pub const TXN_LOG_STATUS_PREPARE_EPOCH_FENCE: i8 = 6;

/// One `__transaction_state` in-memory record (JSON v1 or decoded Kafka v0/v1).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionStateRecord {
    /// In-memory schema version (`1`). Not the on-disk Kafka value version.
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

/// Classic Kafka `TransactionLogValue` (v0 write; v0/v1 read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionLogValue {
    /// Message version (`0` on write; `0` or `1` on read).
    pub version: i16,
    /// Producer id in use by the transactional id.
    pub producer_id: i64,
    /// Epoch associated with the producer id.
    pub producer_epoch: i16,
    /// Transaction timeout in milliseconds (`0` if unknown).
    pub transaction_timeout_ms: i32,
    /// Kafka `transactionStatus` byte (see `TXN_LOG_STATUS_*`).
    pub transaction_status: i8,
    /// Topic → partitions in the txn.
    ///
    /// Written for `ongoing` / `prepare_*` from the live open or prepared set.
    /// **Null** for `empty` / `complete_*` (no live set) and when the set is empty.
    pub partitions: Option<Vec<(String, Vec<i32>)>>,
    /// Last update timestamp (unix ms). `0` on JSON replay.
    pub transaction_last_update_timestamp_ms: i64,
    /// Txn start timestamp (unix ms). Maps to [`TransactionStateRecord::txn_start_ms`].
    pub transaction_start_timestamp_ms: i64,
}

/// Map a Volant state string to a Kafka status byte. Does not emit `6`.
pub fn txn_state_to_log_status(state: &str) -> Option<i8> {
    match state {
        TXN_STATE_EMPTY => Some(TXN_LOG_STATUS_EMPTY),
        TXN_STATE_ONGOING => Some(TXN_LOG_STATUS_ONGOING),
        TXN_STATE_PREPARE_COMMIT => Some(TXN_LOG_STATUS_PREPARE_COMMIT),
        TXN_STATE_PREPARE_ABORT => Some(TXN_LOG_STATUS_PREPARE_ABORT),
        TXN_STATE_COMPLETE_COMMIT => Some(TXN_LOG_STATUS_COMPLETE_COMMIT),
        TXN_STATE_COMPLETE_ABORT => Some(TXN_LOG_STATUS_COMPLETE_ABORT),
        _ => None,
    }
}

/// Map a Kafka status byte to a Volant state string.
///
/// `6` (`PrepareEpochFence`) decodes as [`TXN_STATE_COMPLETE_ABORT`].
pub fn txn_log_status_to_state(status: i8) -> Option<&'static str> {
    match status {
        TXN_LOG_STATUS_EMPTY => Some(TXN_STATE_EMPTY),
        TXN_LOG_STATUS_ONGOING => Some(TXN_STATE_ONGOING),
        TXN_LOG_STATUS_PREPARE_COMMIT => Some(TXN_STATE_PREPARE_COMMIT),
        TXN_LOG_STATUS_PREPARE_ABORT => Some(TXN_STATE_PREPARE_ABORT),
        TXN_LOG_STATUS_COMPLETE_COMMIT => Some(TXN_STATE_COMPLETE_COMMIT),
        TXN_LOG_STATUS_COMPLETE_ABORT | TXN_LOG_STATUS_PREPARE_EPOCH_FENCE => {
            Some(TXN_STATE_COMPLETE_ABORT)
        }
        _ => None,
    }
}

/// Encode classic Kafka `TransactionLogKey` v0 (`int16` version + string).
pub fn encode_transaction_log_key(transactional_id: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + transactional_id.len());
    put_i16(&mut buf, 0);
    put_kafka_string(&mut buf, transactional_id);
    buf
}

/// Decode classic Kafka `TransactionLogKey` v0. `None` if not that schema.
pub fn decode_transaction_log_key(src: &[u8]) -> Option<String> {
    let mut src = src;
    if src.remaining() < 2 {
        return None;
    }
    let version = src.get_i16();
    if version != 0 {
        return None;
    }
    get_kafka_string(&mut src)
}

/// Encode classic Kafka `TransactionLogValue` **v0** (never writes v1 / TV2).
pub fn encode_transaction_log_value(value: &TransactionLogValue) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32);
    put_i16(&mut buf, 0);
    put_i64(&mut buf, value.producer_id);
    put_i16(&mut buf, value.producer_epoch);
    put_i32(&mut buf, value.transaction_timeout_ms);
    buf.push(value.transaction_status as u8);
    match &value.partitions {
        None => put_i32(&mut buf, -1),
        Some(topics) => {
            put_i32(&mut buf, topics.len() as i32);
            for (topic, parts) in topics {
                put_kafka_string(&mut buf, topic);
                put_i32(&mut buf, parts.len() as i32);
                for p in parts {
                    put_i32(&mut buf, *p);
                }
            }
        }
    }
    put_i64(&mut buf, value.transaction_last_update_timestamp_ms);
    put_i64(&mut buf, value.transaction_start_timestamp_ms);
    buf
}

/// Decode classic Kafka `TransactionLogValue` v0 or v1 (ignore extra v1 field).
pub fn decode_transaction_log_value(src: &[u8]) -> Option<TransactionLogValue> {
    let mut src = src;
    if src.remaining() < 2 {
        return None;
    }
    let version = src.get_i16();
    if version != 0 && version != 1 {
        return None;
    }
    if src.remaining() < 8 + 2 + 4 + 1 + 4 {
        return None;
    }
    let producer_id = src.get_i64();
    let producer_epoch = src.get_i16();
    let transaction_timeout_ms = src.get_i32();
    let transaction_status = src.get_i8();
    if src.remaining() < 4 {
        return None;
    }
    let part_count = src.get_i32();
    let partitions = if part_count < 0 {
        None
    } else {
        let n = part_count as usize;
        if n > src.remaining() {
            return None;
        }
        let mut topics = Vec::with_capacity(n);
        for _ in 0..n {
            let topic = get_kafka_string(&mut src)?;
            if src.remaining() < 4 {
                return None;
            }
            let pn = src.get_i32();
            if pn < 0 {
                return None;
            }
            let pn = pn as usize;
            if pn > src.remaining() / 4 {
                return None;
            }
            let mut parts = Vec::with_capacity(pn);
            for _ in 0..pn {
                if src.remaining() < 4 {
                    return None;
                }
                parts.push(src.get_i32());
            }
            topics.push((topic, parts));
        }
        Some(topics)
    };
    if src.remaining() < 16 {
        return None;
    }
    let transaction_last_update_timestamp_ms = src.get_i64();
    let transaction_start_timestamp_ms = src.get_i64();
    if version == 1 && src.remaining() >= 2 {
        let _client_transaction_version = src.get_i16();
    }
    Some(TransactionLogValue {
        version,
        producer_id,
        producer_epoch,
        transaction_timeout_ms,
        transaction_status,
        partitions,
        transaction_last_update_timestamp_ms,
        transaction_start_timestamp_ms,
    })
}

/// Decode one topic record: JSON v1 (header `1` or `{` body) or Kafka v0/v1.
pub fn parse_transaction_state_record(
    key: &[u8],
    value: &[u8],
    headers: &[(String, Bytes)],
) -> Option<(String, TransactionStateRecord)> {
    if is_json_txn_state(headers, value) {
        let tid = String::from_utf8_lossy(key).into_owned();
        let rec = serde_json::from_slice::<TransactionStateRecord>(value).ok()?;
        return Some((tid, rec));
    }
    let tid = decode_transaction_log_key(key)
        .unwrap_or_else(|| String::from_utf8_lossy(key).into_owned());
    let val = decode_transaction_log_value(value)?;
    let state = txn_log_status_to_state(val.transaction_status)?.to_owned();
    if val.producer_id < 0 || val.producer_epoch < 0 {
        return None;
    }
    Some((
        tid,
        TransactionStateRecord {
            v: TRANSACTION_STATE_RECORD_VERSION,
            state,
            producer_id: val.producer_id as u64,
            epoch: val.producer_epoch as u16,
            txn_start_ms: val.transaction_start_timestamp_ms,
        },
    ))
}

fn is_json_txn_state(headers: &[(String, Bytes)], value: &[u8]) -> bool {
    headers
        .iter()
        .any(|(k, v)| k == TRANSACTION_STATE_HEADER && v.as_ref() == TRANSACTION_STATE_FMT_JSON)
        || value.first() == Some(&b'{')
}

fn put_i16(buf: &mut Vec<u8>, v: i16) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn put_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn put_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn put_kafka_string(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    put_i16(buf, b.len() as i16);
    buf.extend_from_slice(b);
}

fn get_kafka_string(src: &mut &[u8]) -> Option<String> {
    if src.remaining() < 2 {
        return None;
    }
    let len = src.get_i16();
    if len < 0 {
        return None;
    }
    let len = len as usize;
    if src.remaining() < len {
        return None;
    }
    let (head, rest) = src.split_at(len);
    *src = rest;
    String::from_utf8(head.to_vec()).ok()
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
        let Some(status) = txn_state_to_log_status(state) else {
            return;
        };
        self.ensure_transaction_state_topic();
        let partitions = match state {
            TXN_STATE_ONGOING | TXN_STATE_PREPARE_COMMIT | TXN_STATE_PREPARE_ABORT => {
                self.txn_log_partitions(producer_id)
            }
            _ => None,
        };
        let value = TransactionLogValue {
            version: 0,
            producer_id: producer_id as i64,
            producer_epoch: epoch as i16,
            transaction_timeout_ms: 0,
            transaction_status: status,
            partitions,
            transaction_last_update_timestamp_ms: unix_now_ms(),
            transaction_start_timestamp_ms: txn_start_ms,
        };
        let key = encode_transaction_log_key(transactional_id);
        let val = encode_transaction_log_value(&value);
        let mut msg = Message::with_key(Bytes::from(key), Bytes::from(val));
        msg.headers.push((
            TRANSACTION_STATE_HEADER.to_string(),
            Bytes::from_static(TRANSACTION_STATE_FMT_KAFKA_V0),
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
                    // v0.226: restart ≡ crash ≡ abort for open (non-prepared)
                    // txns. Do not restore Ongoing — that contradicted markers
                    // and Describe/List. Append complete_abort so the topic
                    // last-write-wins matches (covers begin-only, no markers).
                    if self.prepared_txns.lock().remove(&tid).is_some() {
                        mutated_prepared = true;
                    }
                    self.open_txns.lock().remove(&rec.producer_id);
                    self.ensure_txn_identity(&tid, rec.producer_id, rec.epoch);
                    self.append_transaction_state(
                        &tid,
                        TXN_STATE_COMPLETE_ABORT,
                        rec.producer_id,
                        rec.epoch,
                        rec.txn_start_ms,
                    );
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
                let Some((tid, rec)) = parse_transaction_state_record(key, &r.value, &r.headers)
                else {
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
                let Some((tid, rec)) = parse_transaction_state_record(key, &r.value, &r.headers)
                else {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn transaction_log_key_v0_roundtrip() {
        let tid = "txn-abc";
        let bytes = encode_transaction_log_key(tid);
        assert_eq!(hex(&bytes), "0000000774786e2d616263");
        assert_eq!(decode_transaction_log_key(&bytes).as_deref(), Some(tid));
        // Raw utf8 key is not a Kafka TransactionLogKey.
        assert!(decode_transaction_log_key(tid.as_bytes()).is_none());
    }

    #[test]
    fn transaction_log_value_v0_roundtrip() {
        let v = TransactionLogValue {
            version: 0,
            producer_id: 1,
            producer_epoch: 2,
            transaction_timeout_ms: 0,
            transaction_status: TXN_LOG_STATUS_COMPLETE_ABORT,
            partitions: None,
            transaction_last_update_timestamp_ms: 100,
            transaction_start_timestamp_ms: 50,
        };
        let bytes = encode_transaction_log_value(&v);
        assert_eq!(
            hex(&bytes),
            "0000000000000000000100020000000005ffffffff00000000000000640000000000000032"
        );
        let dec = decode_transaction_log_value(&bytes).expect("v0 decode");
        assert_eq!(dec, v);
        assert!(dec.partitions.is_none());
    }

    #[test]
    fn transaction_log_value_v0_roundtrip_some_partitions() {
        let v = TransactionLogValue {
            version: 0,
            producer_id: 1,
            producer_epoch: 2,
            transaction_timeout_ms: 0,
            transaction_status: TXN_LOG_STATUS_ONGOING,
            partitions: Some(vec![
                ("events".into(), vec![0, 1]),
                ("other".into(), vec![0]),
            ]),
            transaction_last_update_timestamp_ms: 100,
            transaction_start_timestamp_ms: 50,
        };
        let bytes = encode_transaction_log_value(&v);
        let dec = decode_transaction_log_value(&bytes).expect("v0 decode");
        assert_eq!(dec, v);
        assert_eq!(
            dec.partitions,
            Some(vec![
                ("events".into(), vec![0, 1]),
                ("other".into(), vec![0]),
            ])
        );
    }

    #[test]
    fn transaction_log_value_v1_ignores_client_txn_version() {
        let v = TransactionLogValue {
            version: 0,
            producer_id: 9,
            producer_epoch: 1,
            transaction_timeout_ms: 0,
            transaction_status: TXN_LOG_STATUS_ONGOING,
            partitions: None,
            transaction_last_update_timestamp_ms: 1,
            transaction_start_timestamp_ms: 1,
        };
        let mut bytes = encode_transaction_log_value(&v);
        bytes[1] = 1; // version = 1
        bytes.extend_from_slice(&7i16.to_be_bytes());
        let dec = decode_transaction_log_value(&bytes).expect("v1 decode");
        assert_eq!(dec.version, 1);
        assert_eq!(dec.producer_id, 9);
        assert_eq!(dec.transaction_status, TXN_LOG_STATUS_ONGOING);
        assert_eq!(dec.transaction_start_timestamp_ms, 1);
    }

    #[test]
    fn prepare_epoch_fence_decodes_as_complete_abort() {
        assert_eq!(
            txn_log_status_to_state(TXN_LOG_STATUS_PREPARE_EPOCH_FENCE),
            Some(TXN_STATE_COMPLETE_ABORT)
        );
        assert_eq!(
            txn_state_to_log_status(TXN_STATE_COMPLETE_ABORT),
            Some(TXN_LOG_STATUS_COMPLETE_ABORT)
        );
        assert_ne!(
            txn_state_to_log_status(TXN_STATE_COMPLETE_ABORT),
            Some(TXN_LOG_STATUS_PREPARE_EPOCH_FENCE)
        );
    }

    #[test]
    fn parse_json_v1_and_kafka_v0() {
        let rec = TransactionStateRecord {
            v: 1,
            state: TXN_STATE_ONGOING.to_string(),
            producer_id: 7,
            epoch: 1,
            txn_start_ms: 42,
        };
        let json = serde_json::to_vec(&rec).unwrap();
        let headers = vec![(
            TRANSACTION_STATE_HEADER.to_string(),
            Bytes::from_static(TRANSACTION_STATE_FMT_JSON),
        )];
        let (tid, parsed) = parse_transaction_state_record(b"raw-tid", &json, &headers).unwrap();
        assert_eq!(tid, "raw-tid");
        assert_eq!(parsed, rec);

        let key = encode_transaction_log_key("kafka-tid");
        let val = encode_transaction_log_value(&TransactionLogValue {
            version: 0,
            producer_id: 7,
            producer_epoch: 1,
            transaction_timeout_ms: 0,
            transaction_status: TXN_LOG_STATUS_ONGOING,
            partitions: None,
            transaction_last_update_timestamp_ms: 99,
            transaction_start_timestamp_ms: 42,
        });
        let kh = vec![(
            TRANSACTION_STATE_HEADER.to_string(),
            Bytes::from_static(TRANSACTION_STATE_FMT_KAFKA_V0),
        )];
        let (tid, parsed) = parse_transaction_state_record(&key, &val, &kh).unwrap();
        assert_eq!(tid, "kafka-tid");
        assert_eq!(parsed.state, TXN_STATE_ONGOING);
        assert_eq!(parsed.producer_id, 7);
        assert_eq!(parsed.txn_start_ms, 42);
    }
}
