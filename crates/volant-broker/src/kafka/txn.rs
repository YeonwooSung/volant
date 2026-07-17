//! Transaction API handlers (InitProducerId, AddPartitions/Offsets, EndTxn, TxnOffsetCommit).
//!
//! Split out of `handler.rs` so version bumps do not keep growing the god-file.
//! Wire version selects parse/encode shape; shared models own auth and open-txn policy.

use bytes::{Buf, BufMut, BytesMut};

use crate::acl::{AclOperation, ResourceType, CLUSTER_RESOURCE};
use crate::broker::Broker;

use super::codec::{
    get_compact_nullable_string, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    skip_tag_buffer,
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

/// Cluster ACL + ensure_txn_open + per-topic Write ACL → per-partition error.
fn partition_error_for_add(
    broker: &Broker,
    principal: &str,
    cluster_denied: bool,
    open_err: i16,
    topic: &str,
) -> i16 {
    if cluster_denied {
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

fn open_txn_error(broker: &Broker, producer_id: u64, producer_epoch: u16, cluster_denied: bool) -> i16 {
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

/// InitProducerId (API key 22) classic v0–1 / flexible v2–6 — Phase 29 / 62 / 75 / 77.
pub(crate) fn encode_init_producer_id(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // Resume fields (v3+) and Enable2Pc/KeepPreparedTxn (v6) are parsed and
    // discarded — always allocate via init_producer_id_with_txn. v6 response
    // OngoingTxn* is always -1 (no prepared/2PC state).
    let flex = version >= 2;
    let v6 = version >= 6;

    let write_body = |out: &mut BytesMut, err: i16, pid: i64, epoch: i16| {
        out.put_i32(0); // throttle
        out.put_i16(err);
        out.put_i64(pid);
        out.put_i16(epoch);
        if v6 {
            // OngoingTxnProducerId / OngoingTxnProducerEpoch (KIP-890 / KIP-939).
            // Honest: Volant has no prepared/2PC transactions.
            out.put_i64(-1);
            out.put_i16(-1);
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
        );
        return;
    }

    let txn_id = match wire::read_nullable_string(src, flex) {
        Ok(v) => v.unwrap_or_default(),
        Err(_) => {
            write_body(out, KafkaErrorCode::InvalidRequest.as_i16(), -1, -1);
            return;
        }
    };
    // transaction_timeout_ms — ignored (no Kafka txn coordinator timeout).
    if src.remaining() >= 4 {
        let _timeout = src.get_i32();
    }
    // v3+: ProducerId + ProducerEpoch resume fields (explicitly skipped).
    if version >= 3 {
        if src.remaining() < 8 + 2 {
            write_body(out, KafkaErrorCode::InvalidRequest.as_i16(), -1, -1);
            return;
        }
        let _resume_pid = src.get_i64();
        let _resume_epoch = src.get_i16();
    }
    // v6+: Enable2Pc + KeepPreparedTxn (parsed, ignored — no real 2PC).
    if v6 {
        if src.remaining() < 2 {
            write_body(out, KafkaErrorCode::InvalidRequest.as_i16(), -1, -1);
            return;
        }
        let _enable_2pc = src.get_u8() != 0;
        let _keep_prepared = src.get_u8() != 0;
    }
    if flex {
        let _ = skip_tag_buffer(src);
    }

    let (pid, epoch) = broker.init_producer_id_with_txn(&txn_id);
    write_body(
        out,
        KafkaErrorCode::None.as_i16(),
        pid as i64,
        epoch as i16,
    );
}

// ─── AddPartitionsToTxn ──────────────────────────────────────────────────────

/// AddPartitionsToTxn (API 24) classic v0–2 / flexible v3 / batch v4–5.
pub(crate) fn encode_add_partitions_to_txn(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    if version >= 4 {
        encode_add_partitions_batch(broker, src, out, principal);
    } else {
        encode_add_partitions_flat(broker, src, out, version, principal);
    }
}

/// v0–3: flat V3AndBelow fields.
fn encode_add_partitions_flat(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
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

    let _txn_id = match wire::read_string(src, flex) {
        Ok(t) => t,
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

    let topics = match parse_topic_partitions(src, flex) {
        Some(t) => t,
        None => {
            empty_resp(out);
            return;
        }
    };
    if flex {
        let _ = skip_tag_buffer(src);
    }

    let txn = AddPartitionsTxn {
        txn_id: String::new(), // not echoed on flat response
        producer_id,
        producer_epoch,
        topics,
    };
    write_add_partitions_flat_response(broker, out, principal, flex, &txn);
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
) {
    let cluster_denied = cluster_write_denied(broker, principal);
    let open_err = open_txn_error(broker, txn.producer_id, txn.producer_epoch, cluster_denied);

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
            out.put_i16(partition_error_for_add(
                broker,
                principal,
                cluster_denied,
                open_err,
                &t.name,
            ));
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

/// v4–5 batch: Transactions[] with VerifyOnly (parsed, ignored — always add path).
fn encode_add_partitions_batch(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
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
            return;
        }
    };

    let mut txns = Vec::with_capacity(txn_count);
    for _ in 0..txn_count {
        let txn_id = match wire::read_string(src, true) {
            Ok(t) => t,
            Err(_) => {
                empty_resp(out);
                return;
            }
        };
        if src.remaining() < 8 + 2 + 1 {
            empty_resp(out);
            return;
        }
        let producer_id = src.get_i64() as u64;
        let producer_epoch = src.get_i16() as u16;
        let _verify_only = src.get_u8() != 0; // ignored — always add path

        let topics = match parse_topic_partitions(src, true) {
            Some(t) => t,
            None => {
                empty_resp(out);
                return;
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

    out.put_i32(0); // throttle
    out.put_i16(KafkaErrorCode::None.as_i16()); // top-level error
    put_compact_array_len(out, txns.len());
    for txn in &txns {
        put_compact_string(out, &txn.txn_id);
        let open_err = open_txn_error(broker, txn.producer_id, txn.producer_epoch, cluster_denied);
        put_compact_array_len(out, txn.topics.len());
        for t in &txn.topics {
            put_compact_string(out, &t.name);
            put_compact_array_len(out, t.partitions.len());
            for &partition in &t.partitions {
                out.put_i32(partition);
                out.put_i16(partition_error_for_add(
                    broker,
                    principal,
                    cluster_denied,
                    open_err,
                    &t.name,
                ));
                put_empty_tag_buffer(out);
            }
            put_empty_tag_buffer(out);
        }
        put_empty_tag_buffer(out);
    }
    put_empty_tag_buffer(out);
}

// ─── AddOffsetsToTxn ─────────────────────────────────────────────────────────

/// AddOffsetsToTxn (API 25) classic v0–2 / flexible v3–4.
///
/// Phase 82: v4 is wire-identical to v3 (KIP-890 may return
/// TRANSACTION_ABORTABLE — Volant never emits it; buffer-until-commit only).
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

    let _txn_id = match wire::read_string(src, flex) {
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

/// EndTxn (API 26) classic v0–2 / flexible v3–5 — Phase 47 / 62 / 75.
pub(crate) fn encode_end_txn(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
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

    let _txn_id = match wire::read_string(src, flex) {
        Ok(t) => t,
        Err(_) => {
            write_resp(out, KafkaErrorCode::InvalidRequest.as_i16(), -1, -1);
            return;
        }
    };
    if src.remaining() < 8 + 2 + 1 {
        write_resp(out, KafkaErrorCode::InvalidRequest.as_i16(), -1, -1);
        return;
    }
    let producer_id_i64 = src.get_i64();
    let producer_epoch_i16 = src.get_i16();
    let producer_id = producer_id_i64 as u64;
    let producer_epoch = producer_epoch_i16 as u16;
    let committed = src.get_u8() != 0;
    if flex {
        let _ = skip_tag_buffer(src);
    }

    if cluster_write_denied(broker, principal) {
        write_resp(
            out,
            KafkaErrorCode::ClusterAuthorizationFailed.as_i16(),
            producer_id_i64,
            producer_epoch_i16,
        );
        return;
    }

    match broker.end_txn(producer_id, producer_epoch, committed, &[]) {
        Ok((err, _results)) => {
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
        }
        Err(_) => {
            write_resp(
                out,
                KafkaErrorCode::Unknown.as_i16(),
                producer_id_i64,
                producer_epoch_i16,
            );
        }
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

    let _txn_id = match wire::read_string(src, flex) {
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

    let auth_err = if broker.acls().is_enabled()
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
        let tuples: Vec<_> = collected.into_iter().map(|o| o.into_broker_tuple()).collect();
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
