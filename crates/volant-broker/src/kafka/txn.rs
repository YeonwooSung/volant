//! Transaction API handlers (InitProducerId, AddPartitions/Offsets, EndTxn,
//! WriteTxnMarkers, TxnOffsetCommit).
//!
//! Split out of `handler.rs` so version bumps do not keep growing the god-file.
//! Wire version selects parse/encode shape; shared models own auth and open-txn policy.

use bytes::{Buf, BufMut, BytesMut};

use crate::acl::{AclOperation, ResourceType, CLUSTER_RESOURCE};
use crate::broker::Broker;

use super::codec::{
    get_compact_nullable_string, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    put_string, skip_tag_buffer,
};
use super::topic_id::{self, ResolvedTopic};
use super::wire;
use super::{map_idempotent_error, KafkaErrorCode};

// ─── Shared types ────────────────────────────────────────────────────────────

/// One partition under a topic for AddPartitionsToTxn.
#[derive(Debug, Clone)]
struct TopicPartitions {
    name: String,
    partitions: Vec<i32>,
}

/// Neutral IR for one transactional unit in AddPartitionsToTxn.
#[derive(Debug, Clone)]
struct AddPartitionsTxn {
    txn_id: String,
    producer_id: u64,
    producer_epoch: u16,
    topics: Vec<TopicPartitions>,
}

/// Deferred consumer offset buffered until EndTxn commit.
#[derive(Debug, Clone)]
struct BufferedTxnOffset {
    group_id: String,
    topic: String,
    partition: u32,
    offset: u64,
    metadata: String,
}

impl BufferedTxnOffset {
    fn into_broker_tuple(self) -> (String, String, u32, u64, String) {
        (
            self.group_id,
            self.topic,
            self.partition,
            self.offset,
            self.metadata,
        )
    }
}

/// Topic echo structure for TxnOffsetCommit responses.
#[derive(Debug)]
struct TxnOffsetTopic {
    resolved: ResolvedTopic,
    partitions: Vec<i32>,
}

// ─── Shared AddPartitions policy ─────────────────────────────────────────────

/// Cluster ACL + txn-id Write + ensure_txn_open + per-topic Write ACL → per-partition error.
fn partition_error_for_add(
    broker: &Broker,
    principal: &str,
    cluster_denied: bool,
    txn_id_denied: bool,
    open_err: i16,
    topic: &str,
) -> i16 {
    if txn_id_denied {
        KafkaErrorCode::TransactionalIdAuthorizationFailed.as_i16()
    } else if cluster_denied {
        KafkaErrorCode::ClusterAuthorizationFailed.as_i16()
    } else if open_err != 0 {
        open_err
    } else if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Topic,
            topic,
            AclOperation::Write,
        )
    {
        KafkaErrorCode::TopicAuthorizationFailed.as_i16()
    } else {
        KafkaErrorCode::None.as_i16()
    }
}

fn cluster_write_denied(broker: &Broker, principal: &str) -> bool {
    broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Write,
        )
}

/// Write on `TransactionalId` when ACLs are on and `txn_id` is non-empty.
/// Empty id is the idempotent-only path and skips this check.
fn transactional_id_write_denied(broker: &Broker, principal: &str, txn_id: &str) -> bool {
    !txn_id.is_empty()
        && broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::TransactionalId,
            txn_id,
            AclOperation::Write,
        )
}

fn open_txn_error(
    broker: &Broker,
    producer_id: u64,
    producer_epoch: u16,
    cluster_denied: bool,
) -> i16 {
    if cluster_denied {
        -1 // sentinel; partition_error_for_add maps cluster_denied first
    } else {
        let e = broker.ensure_txn_open(producer_id, producer_epoch);
        if e == 0 {
            0
        } else {
            map_idempotent_error(e)
        }
    }
}

// ─── InitProducerId ──────────────────────────────────────────────────────────

/// InitProducerId (API key 22) classic v0–1 / flexible v2–6 — Phase 29 / 62 / 75 / 77 / 90.
///
/// Returns optional Init registration fan-out (Phase 120) so peers learn the
/// txn coordinator without installing open state.
pub(crate) fn encode_init_producer_id(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) -> Option<crate::broker::Txn2pcFanout> {
    // Resume fields (v3+) still ignored for allocation. v6 Enable2Pc /
    // KeepPreparedTxn drive Phase 90 prepared-txn state; OngoingTxn* echoes
    // prepared pid/epoch when present.
    let flex = version >= 2;
    let v6 = version >= 6;

    let write_body = |out: &mut BytesMut,
                      err: i16,
                      pid: i64,
                      epoch: i16,
                      ongoing_pid: i64,
                      ongoing_epoch: i16| {
        out.put_i32(0); // throttle
        out.put_i16(err);
        out.put_i64(pid);
        out.put_i16(epoch);
        if v6 {
            out.put_i64(ongoing_pid);
            out.put_i16(ongoing_epoch);
        }
        if flex {
            put_empty_tag_buffer(out);
        }
    };

    if cluster_write_denied(broker, principal) {
        write_body(
            out,
            KafkaErrorCode::ClusterAuthorizationFailed.as_i16(),
            -1,
            -1,
            -1,
            -1,
        );
        return None;
    }

    let txn_id = match wire::read_nullable_string(src, flex) {
        Ok(v) => v.unwrap_or_default(),
        Err(_) => {
            write_body(out, KafkaErrorCode::InvalidRequest.as_i16(), -1, -1, -1, -1);
            return None;
        }
    };
    // transaction_timeout_ms — honored for open (non-prepared) txns (Phase 93).
    // Prepared-txn timeout remains broker-level (Phase 92: VOLANT_PREPARED_TXN_TIMEOUT_MS).
    // Phase 96: client timeout > broker max → INVALID_TRANSACTION_TIMEOUT (50).
    let mut transaction_timeout_ms: i32 = 0;
    if src.remaining() >= 4 {
        transaction_timeout_ms = src.get_i32();
    }
    // v3+: ProducerId + ProducerEpoch resume fields (explicitly skipped).
    if version >= 3 {
        if src.remaining() < 8 + 2 {
            write_body(out, KafkaErrorCode::InvalidRequest.as_i16(), -1, -1, -1, -1);
            return None;
        }
        let _resume_pid = src.get_i64();
        let _resume_epoch = src.get_i16();
    }
    // v6+: Enable2Pc + KeepPreparedTxn (Phase 90 prepared-txn MVP).
    let mut enable_2pc = false;
    let mut keep_prepared = false;
    if v6 {
        if src.remaining() < 2 {
            write_body(out, KafkaErrorCode::InvalidRequest.as_i16(), -1, -1, -1, -1);
            return None;
        }
        enable_2pc = src.get_u8() != 0;
        keep_prepared = src.get_u8() != 0;
    }
    if flex {
        let _ = skip_tag_buffer(src);
    }

    if transactional_id_write_denied(broker, principal, &txn_id) {
        write_body(
            out,
            KafkaErrorCode::TransactionalIdAuthorizationFailed.as_i16(),
            -1,
            -1,
            -1,
            -1,
        );
        return None;
    }

    let r = broker.init_producer_id_with_opts(
        &txn_id,
        enable_2pc,
        keep_prepared,
        transaction_timeout_ms,
    );
    if r.error_code != 0 {
        // Phase 96: INVALID_TRANSACTION_TIMEOUT (50) etc. — no pid allocated.
        write_body(out, r.error_code, -1, -1, -1, -1);
        return None;
    }
    write_body(
        out,
        KafkaErrorCode::None.as_i16(),
        r.producer_id as i64,
        r.epoch as i16,
        r.ongoing_txn_producer_id,
        r.ongoing_txn_producer_epoch,
    );
    // Phase 120: register Init owner on live peers (no open install).
    if !txn_id.is_empty() {
        let fanout =
            broker.txn_2pc_init_register_fanout(&txn_id, r.producer_id, r.epoch, enable_2pc);
        match fanout {
            crate::broker::Txn2pcFanout::None => None,
            other => Some(other),
        }
    } else {
        None
    }
}

// ─── AddPartitionsToTxn ──────────────────────────────────────────────────────

/// AddPartitionsToTxn (API 24) classic v0–2 / flexible v3 / batch v4–5.
///
/// Returns optional multi-broker open fan-out (Phase 114) when at least one
/// partition was added successfully.
pub(crate) fn encode_add_partitions_to_txn(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) -> Option<crate::broker::Txn2pcFanout> {
    if version >= 4 {
        encode_add_partitions_batch(broker, src, out, principal)
    } else {
        encode_add_partitions_flat(broker, src, out, version, principal)
    }
}

/// v0–3: flat V3AndBelow fields.
fn encode_add_partitions_flat(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) -> Option<crate::broker::Txn2pcFanout> {
    let flex = version >= 3;

    let empty_resp = |out: &mut BytesMut| {
        out.put_i32(0);
        if flex {
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
        } else {
            out.put_i32(0);
        }
    };

    let txn_id = match wire::read_string(src, flex) {
        Ok(t) => t,
        Err(_) => {
            empty_resp(out);
            return None;
        }
    };
    if src.remaining() < 8 + 2 {
        empty_resp(out);
        return None;
    }
    let producer_id = src.get_i64() as u64;
    let producer_epoch = src.get_i16() as u16;

    let topics = match parse_topic_partitions(src, flex) {
        Some(t) => t,
        None => {
            empty_resp(out);
            return None;
        }
    };
    if flex {
        let _ = skip_tag_buffer(src);
    }

    let txn = AddPartitionsTxn {
        txn_id, // not echoed on flat response; kept for TransactionalId ACL
        producer_id,
        producer_epoch,
        topics,
    };
    write_add_partitions_flat_response(broker, out, principal, flex, &txn)
}

/// Parse topics[{name, partitions[]}] classic or flexible. Fail-closed → None.
fn parse_topic_partitions(src: &mut impl Buf, flex: bool) -> Option<Vec<TopicPartitions>> {
    let topic_count = match wire::read_array_len(src, flex) {
        Ok(Some(n)) => n,
        Ok(None) => 0,
        Err(_) => return None,
    };
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = match wire::read_string(src, flex) {
            Ok(t) => t,
            Err(_) => return None,
        };
        let part_count = match wire::read_array_len(src, flex) {
            Ok(Some(n)) => n,
            Ok(None) => 0,
            Err(_) => return None,
        };
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            if src.remaining() < 4 {
                return None;
            }
            partitions.push(src.get_i32());
        }
        if flex {
            let _ = skip_tag_buffer(src);
        }
        topics.push(TopicPartitions { name, partitions });
    }
    Some(topics)
}

fn write_add_partitions_flat_response(
    broker: &Broker,
    out: &mut BytesMut,
    principal: &str,
    flex: bool,
    txn: &AddPartitionsTxn,
) -> Option<crate::broker::Txn2pcFanout> {
    let cluster_denied = cluster_write_denied(broker, principal);
    let txn_id_denied = transactional_id_write_denied(broker, principal, &txn.txn_id);
    let open_err = open_txn_error(broker, txn.producer_id, txn.producer_epoch, cluster_denied);

    // Phase 105: record successful membership for control batches (even with no produce).
    let mut ok_parts: Vec<(String, u32)> = Vec::new();

    out.put_i32(0); // throttle
    if flex {
        put_compact_array_len(out, txn.topics.len());
    } else {
        out.put_i32(txn.topics.len() as i32);
    }
    for t in &txn.topics {
        topic_id::write_name(out, flex, &t.name);
        if flex {
            put_compact_array_len(out, t.partitions.len());
        } else {
            out.put_i32(t.partitions.len() as i32);
        }
        for &partition in &t.partitions {
            out.put_i32(partition);
            let err = partition_error_for_add(
                broker,
                principal,
                cluster_denied,
                txn_id_denied,
                open_err,
                &t.name,
            );
            out.put_i16(err);
            if err == 0 && partition >= 0 {
                ok_parts.push((t.name.clone(), partition as u32));
            }
            if flex {
                put_empty_tag_buffer(out);
            }
        }
        if flex {
            put_empty_tag_buffer(out);
        }
    }
    if flex {
        put_empty_tag_buffer(out);
    }
    if !ok_parts.is_empty() {
        let _ = broker.record_txn_added_partitions(txn.producer_id, &ok_parts);
        return open_fanout_after_add(broker, txn.producer_id, 0);
    }
    None
}

/// v4–5 batch: Transactions[] with VerifyOnly (parsed, ignored — always add path).
fn encode_add_partitions_batch(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) -> Option<crate::broker::Txn2pcFanout> {
    let empty_resp = |out: &mut BytesMut| {
        out.put_i32(0);
        out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
        put_compact_array_len(out, 0);
        put_empty_tag_buffer(out);
    };

    let txn_count = match wire::read_array_len(src, true) {
        Ok(Some(n)) => n,
        Ok(None) => 0,
        Err(_) => {
            empty_resp(out);
            return None;
        }
    };

    let mut txns = Vec::with_capacity(txn_count);
    for _ in 0..txn_count {
        let txn_id = match wire::read_string(src, true) {
            Ok(t) => t,
            Err(_) => {
                empty_resp(out);
                return None;
            }
        };
        if src.remaining() < 8 + 2 + 1 {
            empty_resp(out);
            return None;
        }
        let producer_id = src.get_i64() as u64;
        let producer_epoch = src.get_i16() as u16;
        let _verify_only = src.get_u8() != 0; // ignored — always add path

        let topics = match parse_topic_partitions(src, true) {
            Some(t) => t,
            None => {
                empty_resp(out);
                return None;
            }
        };
        let _ = skip_tag_buffer(src); // transaction tags
        txns.push(AddPartitionsTxn {
            txn_id,
            producer_id,
            producer_epoch,
            topics,
        });
    }
    let _ = skip_tag_buffer(src); // request top-level tags

    let cluster_denied = cluster_write_denied(broker, principal);
    let mut fanout_pid: Option<u64> = None;

    out.put_i32(0); // throttle
    out.put_i16(KafkaErrorCode::None.as_i16()); // top-level error
    put_compact_array_len(out, txns.len());
    for txn in &txns {
        put_compact_string(out, &txn.txn_id);
        let txn_id_denied = transactional_id_write_denied(broker, principal, &txn.txn_id);
        let open_err = open_txn_error(broker, txn.producer_id, txn.producer_epoch, cluster_denied);
        // Phase 105: record successful membership for control batches.
        let mut ok_parts: Vec<(String, u32)> = Vec::new();
        put_compact_array_len(out, txn.topics.len());
        for t in &txn.topics {
            put_compact_string(out, &t.name);
            put_compact_array_len(out, t.partitions.len());
            for &partition in &t.partitions {
                out.put_i32(partition);
                let err = partition_error_for_add(
                    broker,
                    principal,
                    cluster_denied,
                    txn_id_denied,
                    open_err,
                    &t.name,
                );
                out.put_i16(err);
                if err == 0 && partition >= 0 {
                    ok_parts.push((t.name.clone(), partition as u32));
                }
                put_empty_tag_buffer(out);
            }
            put_empty_tag_buffer(out);
        }
        put_empty_tag_buffer(out);
        if !ok_parts.is_empty() {
            let _ = broker.record_txn_added_partitions(txn.producer_id, &ok_parts);
            fanout_pid = Some(txn.producer_id);
        }
    }
    put_empty_tag_buffer(out);
    fanout_pid.and_then(|pid| open_fanout_after_add(broker, pid, 0))
}

// ─── AddOffsetsToTxn ─────────────────────────────────────────────────────────

/// AddOffsetsToTxn (API 25) classic v0–2 / flexible v3–4.
///
/// Phase 82: v4 is wire-identical to v3.
/// Phase 94: may return TRANSACTION_ABORTABLE (123) when the producer is in
/// the timeout-abortable set (`ensure_txn_open` → map_idempotent_error).
pub(crate) fn encode_add_offsets_to_txn(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // v3–4 share flexible compact framing; v4 adds no new fields.
    let flex = version >= 3;

    let write_err = |out: &mut BytesMut, err: i16| {
        out.put_i32(0);
        out.put_i16(err);
        if flex {
            put_empty_tag_buffer(out);
        }
    };

    let txn_id = match wire::read_string(src, flex) {
        Ok(t) => t,
        Err(_) => {
            write_err(out, KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    if src.remaining() < 8 + 2 {
        write_err(out, KafkaErrorCode::InvalidRequest.as_i16());
        return;
    }
    let producer_id = src.get_i64() as u64;
    let producer_epoch = src.get_i16() as u16;
    let group_id = match wire::read_string(src, flex) {
        Ok(g) => g,
        Err(_) => {
            write_err(out, KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    if flex {
        let _ = skip_tag_buffer(src);
    }

    if transactional_id_write_denied(broker, principal, &txn_id) {
        write_err(
            out,
            KafkaErrorCode::TransactionalIdAuthorizationFailed.as_i16(),
        );
        return;
    }

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Group,
            &group_id,
            AclOperation::Read,
        )
    {
        write_err(out, KafkaErrorCode::GroupAuthorizationFailed.as_i16());
        return;
    }

    let err = broker.ensure_txn_open(producer_id, producer_epoch);
    write_err(
        out,
        if err == 0 {
            KafkaErrorCode::None.as_i16()
        } else {
            map_idempotent_error(err)
        },
    );
}

// ─── EndTxn ──────────────────────────────────────────────────────────────────

/// EndTxn (API 26) classic v0–2 / flexible v3–5 — Phase 47 / 62 / 75 / 114.
///
/// Returns optional multi-broker 2PC fan-out for the Kafka dispatch path to await
/// (Phase 114). Caller must run fan-out before considering prepare durable.
pub(crate) fn encode_end_txn(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) -> Option<crate::broker::Txn2pcFanout> {
    let flex = version >= 3;

    let write_resp = |out: &mut BytesMut, err: i16, pid: i64, epoch: i16| {
        out.put_i32(0);
        out.put_i16(err);
        // v5: echo ProducerId + ProducerEpoch after error_code.
        if version >= 5 {
            out.put_i64(pid);
            out.put_i16(epoch);
        }
        if flex {
            put_empty_tag_buffer(out);
        }
    };

    let txn_id = match wire::read_string(src, flex) {
        Ok(t) => t,
        Err(_) => {
            write_resp(out, KafkaErrorCode::InvalidRequest.as_i16(), -1, -1);
            return None;
        }
    };
    if src.remaining() < 8 + 2 + 1 {
        write_resp(out, KafkaErrorCode::InvalidRequest.as_i16(), -1, -1);
        return None;
    }
    let producer_id_i64 = src.get_i64();
    let producer_epoch_i16 = src.get_i16();
    let producer_id = producer_id_i64 as u64;
    let producer_epoch = producer_epoch_i16 as u16;
    let committed = src.get_u8() != 0;
    if flex {
        let _ = skip_tag_buffer(src);
    }

    if transactional_id_write_denied(broker, principal, &txn_id) {
        write_resp(
            out,
            KafkaErrorCode::TransactionalIdAuthorizationFailed.as_i16(),
            producer_id_i64,
            producer_epoch_i16,
        );
        return None;
    }

    if cluster_write_denied(broker, principal) {
        write_resp(
            out,
            KafkaErrorCode::ClusterAuthorizationFailed.as_i16(),
            producer_id_i64,
            producer_epoch_i16,
        );
        return None;
    }

    match broker.end_txn(producer_id, producer_epoch, committed, &[]) {
        Ok((err, _results, fanout)) => {
            write_resp(
                out,
                if err == 0 {
                    KafkaErrorCode::None.as_i16()
                } else {
                    map_idempotent_error(err)
                },
                producer_id_i64,
                producer_epoch_i16,
            );
            if err == 0 {
                Some(fanout)
            } else {
                None
            }
        }
        Err(_) => {
            write_resp(
                out,
                KafkaErrorCode::Unknown.as_i16(),
                producer_id_i64,
                producer_epoch_i16,
            );
            None
        }
    }
}

/// After a successful AddPartitions / ensure open, return open fan-out for
/// multi-broker 2PC (Phase 114).
pub(crate) fn open_fanout_after_add(
    broker: &Broker,
    producer_id: u64,
    open_err: i16,
) -> Option<crate::broker::Txn2pcFanout> {
    if open_err != 0 {
        return None;
    }
    let fanout = broker.txn_2pc_open_fanout(producer_id);
    match fanout {
        crate::broker::Txn2pcFanout::None => None,
        other => Some(other),
    }
}

// ─── TxnOffsetCommit ─────────────────────────────────────────────────────────

/// TxnOffsetCommit (API 28) classic v0–2 / flexible v3–6 — Phase 47 / 62 / 75 / 76.
pub(crate) fn encode_txn_offset_commit(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    let flex = version >= 3;
    let by_id = version >= 6;

    let empty_resp = |out: &mut BytesMut| {
        out.put_i32(0);
        if flex {
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
        } else {
            out.put_i32(0);
        }
    };

    let txn_id = match wire::read_string(src, flex) {
        Ok(t) => t,
        Err(_) => {
            empty_resp(out);
            return;
        }
    };
    let group_id = match wire::read_string(src, flex) {
        Ok(g) => g,
        Err(_) => {
            empty_resp(out);
            return;
        }
    };
    if src.remaining() < 8 + 2 {
        empty_resp(out);
        return;
    }
    let producer_id = src.get_i64() as u64;
    let producer_epoch = src.get_i16() as u16;

    // v3+: generation / member / instance — ignored (no group membership check).
    if version >= 3 {
        if src.remaining() < 4 {
            empty_resp(out);
            return;
        }
        let _generation = src.get_i32();
        // flex is always true when version >= 3 for this API.
        let _member = wire::read_string(src, true).ok();
        let _instance = get_compact_nullable_string(src).ok();
    }

    let mut collected: Vec<BufferedTxnOffset> = Vec::new();
    let mut structure: Vec<TxnOffsetTopic> = Vec::new();

    let topic_count = match wire::read_array_len(src, flex) {
        Ok(Some(n)) => n,
        Ok(None) => 0,
        Err(_) => {
            empty_resp(out);
            return;
        }
    };

    for _ in 0..topic_count {
        let resolved = match topic_id::read_and_resolve(broker, src, flex, by_id) {
            Ok(r) => r,
            Err(_) => {
                empty_resp(out);
                return;
            }
        };
        let part_count = match wire::read_array_len(src, flex) {
            Ok(Some(n)) => n,
            Ok(None) => 0,
            Err(_) => {
                empty_resp(out);
                return;
            }
        };
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            if src.remaining() < 4 + 8 {
                empty_resp(out);
                return;
            }
            let partition = src.get_i32();
            let offset = src.get_i64();
            // v2+ leader epoch (always present at flex v3+; also on classic v2).
            if version >= 2 {
                if src.remaining() < 4 {
                    empty_resp(out);
                    return;
                }
                let _leader_epoch = src.get_i32();
            }
            let metadata = if flex {
                get_compact_nullable_string(src)
                    .ok()
                    .flatten()
                    .unwrap_or_default()
            } else {
                super::codec::get_nullable_string(src)
                    .ok()
                    .flatten()
                    .unwrap_or_default()
            };
            if flex {
                let _ = skip_tag_buffer(src);
            }
            if offset >= 0 {
                if let Some(topic) = resolved.name.clone() {
                    collected.push(BufferedTxnOffset {
                        group_id: group_id.clone(),
                        topic,
                        partition: partition as u32,
                        offset: offset as u64,
                        metadata,
                    });
                }
            }
            partitions.push(partition);
        }
        if flex {
            let _ = skip_tag_buffer(src);
        }
        structure.push(TxnOffsetTopic {
            resolved,
            partitions,
        });
    }
    if flex {
        let _ = skip_tag_buffer(src);
    }

    let auth_err = if transactional_id_write_denied(broker, principal, &txn_id) {
        Some(KafkaErrorCode::TransactionalIdAuthorizationFailed.as_i16())
    } else if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Group,
            &group_id,
            AclOperation::Read,
        )
    {
        Some(KafkaErrorCode::GroupAuthorizationFailed.as_i16())
    } else {
        None
    };

    let part_err = if let Some(e) = auth_err {
        e
    } else if collected.is_empty() {
        KafkaErrorCode::None.as_i16()
    } else {
        let tuples: Vec<_> = collected
            .into_iter()
            .map(|o| o.into_broker_tuple())
            .collect();
        let err = broker.buffer_txn_offsets(producer_id, producer_epoch, &tuples);
        if err == 0 {
            KafkaErrorCode::None.as_i16()
        } else {
            map_idempotent_error(err)
        }
    };

    out.put_i32(0); // throttle
    if flex {
        put_compact_array_len(out, structure.len());
    } else {
        out.put_i32(structure.len() as i32);
    }
    for t in structure {
        topic_id::write_wire_id(out, flex, &t.resolved.wire);
        if flex {
            put_compact_array_len(out, t.partitions.len());
        } else {
            out.put_i32(t.partitions.len() as i32);
        }
        for p in t.partitions {
            out.put_i32(p);
            let pe = if t.resolved.is_unknown() {
                KafkaErrorCode::UnknownTopicId.as_i16()
            } else {
                part_err
            };
            out.put_i16(pe);
            if flex {
                put_empty_tag_buffer(out);
            }
        }
        if flex {
            put_empty_tag_buffer(out);
        }
    }
    if flex {
        put_empty_tag_buffer(out);
    }
}

// ─── WriteTxnMarkers ─────────────────────────────────────────────────────────

/// One marker in a WriteTxnMarkers request (CoordinatorEpoch already dropped).
struct WriteTxnMarkerIn {
    producer_id: i64,
    producer_epoch: i16,
    commit: bool,
    topics: Vec<TopicPartitions>,
}

/// WriteTxnMarkers (API key 27) classic v0 / flexible v1.
///
/// Replica-local COMMIT/ABORT control batches + matching soft `__txn_markers`.
/// Does **not** call [`Broker::end_txn`] (no coordinator finalize). Coordinator
/// epoch is parsed and ignored. ACL: Topic WRITE or Cluster ALTER.
pub(crate) fn encode_write_txn_markers(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    let flex = version >= 1;
    let markers = parse_write_txn_markers(src, flex);

    let cluster_ok = !broker.acls().is_enabled()
        || broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );

    out.put_i32(0); // throttle
    if flex {
        put_compact_array_len(out, markers.len());
    } else {
        out.put_i32(markers.len() as i32);
    }
    for m in &markers {
        let results = apply_write_txn_marker(broker, principal, cluster_ok, m);
        out.put_i64(m.producer_id);
        if flex {
            put_compact_array_len(out, results.len());
        } else {
            out.put_i32(results.len() as i32);
        }
        for (name, parts) in results {
            if flex {
                put_compact_string(out, &name);
                put_compact_array_len(out, parts.len());
            } else {
                put_string(out, &name);
                out.put_i32(parts.len() as i32);
            }
            for (partition, err) in parts {
                out.put_i32(partition);
                out.put_i16(err);
                if flex {
                    put_empty_tag_buffer(out);
                }
            }
            if flex {
                put_empty_tag_buffer(out);
            }
        }
        if flex {
            put_empty_tag_buffer(out);
        }
    }
    if flex {
        put_empty_tag_buffer(out);
    }
}

fn apply_write_txn_marker(
    broker: &Broker,
    principal: &str,
    cluster_ok: bool,
    marker: &WriteTxnMarkerIn,
) -> Vec<(String, Vec<(i32, i16)>)> {
    let mut allowed: Vec<(String, Vec<i32>)> = Vec::new();
    let mut denied: Vec<(String, Vec<(i32, i16)>)> = Vec::new();
    for t in &marker.topics {
        if write_txn_markers_topic_denied(broker, principal, cluster_ok, &t.name) {
            let code = KafkaErrorCode::TopicAuthorizationFailed.as_i16();
            denied.push((
                t.name.clone(),
                t.partitions.iter().map(|&p| (p, code)).collect(),
            ));
        } else {
            allowed.push((t.name.clone(), t.partitions.clone()));
        }
    }
    let mut results = if allowed.is_empty() {
        Vec::new()
    } else {
        broker.write_txn_markers(
            marker.producer_id as u64,
            marker.producer_epoch as u16,
            marker.commit,
            &allowed,
        )
    };
    results.extend(denied);
    // Echo request topic order.
    let mut by_name: std::collections::HashMap<String, Vec<(i32, i16)>> =
        results.into_iter().collect();
    marker
        .topics
        .iter()
        .map(|t| (t.name.clone(), by_name.remove(&t.name).unwrap_or_default()))
        .collect()
}

fn write_txn_markers_topic_denied(
    broker: &Broker,
    principal: &str,
    cluster_ok: bool,
    topic: &str,
) -> bool {
    if cluster_ok {
        return false;
    }
    !broker.acls().authorize(
        Some(principal),
        ResourceType::Topic,
        topic,
        AclOperation::Write,
    )
}

fn parse_write_txn_markers(src: &mut impl Buf, flex: bool) -> Vec<WriteTxnMarkerIn> {
    let n = match wire::read_array_len(src, flex) {
        Ok(Some(n)) => n,
        Ok(None) | Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    for _ in 0..n {
        if src.remaining() < 8 + 2 + 1 {
            break;
        }
        let producer_id = src.get_i64();
        let producer_epoch = src.get_i16();
        let commit = src.get_u8() != 0;
        let tn = match wire::read_array_len(src, flex) {
            Ok(Some(n)) => n,
            Ok(None) | Err(_) => break,
        };
        let mut topics = Vec::new();
        let mut topics_ok = true;
        for _ in 0..tn {
            let name = match wire::read_string(src, flex) {
                Ok(s) => s,
                Err(_) => {
                    topics_ok = false;
                    break;
                }
            };
            let pn = match wire::read_array_len(src, flex) {
                Ok(Some(n)) => n,
                Ok(None) | Err(_) => {
                    topics_ok = false;
                    break;
                }
            };
            let mut partitions = Vec::with_capacity(pn);
            for _ in 0..pn {
                if src.remaining() < 4 {
                    break;
                }
                partitions.push(src.get_i32());
            }
            if flex {
                let _ = skip_tag_buffer(src);
            }
            topics.push(TopicPartitions { name, partitions });
        }
        if !topics_ok {
            break;
        }
        if src.remaining() < 4 {
            break;
        }
        let _coordinator_epoch = src.get_i32();
        if flex {
            let _ = skip_tag_buffer(src);
        }
        out.push(WriteTxnMarkerIn {
            producer_id,
            producer_epoch,
            commit,
            topics,
        });
    }
    if flex {
        let _ = skip_tag_buffer(src);
    }
    out
}
