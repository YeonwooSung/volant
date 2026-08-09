//! Kafka wire handlers: Produce, Fetch, ListOffsets, OffsetForLeaderEpoch.

use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tracing::debug;
use volant_core::{Error, MessageBatch, Offset, PartitionId, TopicName};

use crate::acl::{AclOperation, ResourceType};
use crate::broker::{Broker, IdempotentCheck};

use super::codec::{
    decode_produce_batches, encode_control_record_batch, encode_message_set,
    encode_message_set_compressed, encode_record_batch, encode_record_batch_compressed,
    get_bytes, get_compact_array_len, get_compact_bytes, is_txn_control_record,
    parse_txn_control_record,
    get_compact_nullable_string, get_compact_string, get_nullable_string, get_string,
    put_bytes, put_compact_array_len, put_compact_bytes, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_nullable_string, put_string, put_unsigned_varint,
    read_unsigned_varint, skip_tag_buffer,
};
use super::compress::{fetch_compression_codec, CompressionCodec};
use super::topic_id;
use super::{map_idempotent_error, KafkaErrorCode};

/// Log start offset for a partition (Produce v5+), or `-1` if unknown.
pub(crate) fn produce_log_start_offset(broker: &Broker, topic: &str, partition: u32) -> i64 {
    match broker.list_offsets(topic, &[partition]) {
        Ok(rows) => rows
            .first()
            .map(|(_, earliest, _)| *earliest as i64)
            .unwrap_or(-1),
        Err(_) => -1,
    }
}

/// Whether this partition error should carry KIP-951 CurrentLeader.
fn kip951_leader_error(error: i16) -> bool {
    error == KafkaErrorCode::NotLeaderForPartition.as_i16()
        || error == KafkaErrorCode::FencedLeaderEpoch.as_i16()
}

/// Resolve (leader_id, leader_epoch) for KIP-951 from partition metadata.
fn resolve_current_leader(
    broker: &Broker,
    topic: &str,
    partition: u32,
    error: i16,
) -> Option<(i32, i32)> {
    if !kip951_leader_error(error) {
        return None;
    }
    let name = TopicName::new(topic.to_string());
    let snap = broker.metadata(Some(&[name]));
    let part = snap.topics.first().and_then(|t| {
        t.partitions
            .iter()
            .find(|p| p.partition_id.0 == partition)
    })?;
    Some((part.leader as i32, part.leader_epoch as i32))
}

/// Encode LeaderIdAndEpoch body (no outer tag framing).
fn encode_leader_id_and_epoch(leader_id: i32, leader_epoch: i32) -> BytesMut {
    let mut body = BytesMut::with_capacity(12);
    body.put_i32(leader_id);
    body.put_i32(leader_epoch);
    put_empty_tag_buffer(&mut body);
    body
}

/// Write a single tagged field TAG_BUFFER.
fn put_single_tag(out: &mut BytesMut, tag: u32, value: &[u8]) {
    put_unsigned_varint(out, 1);
    put_unsigned_varint(out, tag);
    put_unsigned_varint(out, value.len() as u32);
    out.extend_from_slice(value);
}

/// Write a multi-tag TAG_BUFFER (tags must be sorted ascending by id).
fn put_tags(out: &mut BytesMut, tags: &[(u32, BytesMut)]) {
    if tags.is_empty() {
        put_empty_tag_buffer(out);
        return;
    }
    put_unsigned_varint(out, tags.len() as u32);
    for (tag, value) in tags {
        put_unsigned_varint(out, *tag);
        put_unsigned_varint(out, value.len() as u32);
        out.extend_from_slice(value);
    }
}

/// Encode EpochEndOffset body for DivergingEpoch (tag 0): epoch + end_offset.
fn encode_diverging_epoch(epoch: i32, end_offset: i64) -> BytesMut {
    let mut body = BytesMut::with_capacity(12);
    body.put_i32(epoch);
    body.put_i64(end_offset);
    body
}

/// Produce/Fetch NodeEndpoints entry for one broker (rack from cluster.toml).
fn put_node_endpoints_tag(out: &mut BytesMut, endpoints: &[(i32, String, i32, Option<String>)]) {
    let mut value = BytesMut::new();
    put_compact_array_len(&mut value, endpoints.len());
    for (id, host, port, rack) in endpoints {
        value.put_i32(*id);
        put_compact_string(&mut value, host);
        value.put_i32(*port);
        put_compact_nullable_string(&mut value, rack.as_deref());
        put_empty_tag_buffer(&mut value);
    }
    put_single_tag(out, 0, &value);
}

/// Collect unique NodeEndpoints for leaders referenced by CurrentLeader ids.
fn node_endpoints_for_leaders(
    broker: &Broker,
    leader_ids: &[i32],
) -> Vec<(i32, String, i32, Option<String>)> {
    if leader_ids.is_empty() {
        return Vec::new();
    }
    let snap = broker.metadata(None);
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for &id in leader_ids {
        if !seen.insert(id) {
            continue;
        }
        let (host, port) = snap
            .brokers
            .iter()
            .find(|(i, _, _)| *i as i32 == id)
            .map(|(_, h, p)| (h.clone(), i32::from(*p)))
            .unwrap_or((snap.host.clone(), i32::from(snap.port)));
        let rack = broker.broker_rack(id as u32);
        out.push((id, host, port, rack));
    }
    out
}

/// Write one ProduceResponse partition entry (classic v0–8 / flexible v9+).
///
/// Field order: index, error, base_offset, log_append_time (v2+),
/// log_start_offset (v5+), record_errors[] + error_message (v8+), TAG_BUFFER (v9+).
///
/// v10+: optional CurrentLeader (tag 0) when `current_leader` is `Some`.
/// Returns `true` when CurrentLeader was written (caller may emit NodeEndpoints).
pub(crate) fn put_produce_partition_response(
    out: &mut BytesMut,
    version: i16,
    partition: i32,
    error: i16,
    base_offset: i64,
    log_start_offset: i64,
    current_leader: Option<(i32, i32)>,
) -> bool {
    let flexible = version >= 9;
    out.put_i32(partition);
    out.put_i16(error);
    out.put_i64(base_offset);
    if version >= 2 {
        out.put_i64(-1); // log_append_time_ms (CreateTime topics)
    }
    if version >= 5 {
        out.put_i64(log_start_offset);
    }
    if version >= 8 {
        if flexible {
            put_compact_array_len(out, 0); // record_errors empty
            put_compact_nullable_string(out, None); // error_message
        } else {
            out.put_i32(0); // record_errors (empty; no per-record drop detail)
            put_nullable_string(out, None); // error_message
        }
    }
    let mut wrote_leader = false;
    if flexible {
        if version >= 10 {
            if let Some((lid, lep)) = current_leader {
                let body = encode_leader_id_and_epoch(lid, lep);
                put_single_tag(out, 0, &body);
                wrote_leader = true;
            } else {
                put_empty_tag_buffer(out);
            }
        } else {
            put_empty_tag_buffer(out);
        }
    }
    wrote_leader
}

/// Empty Produce response (no topics) with correct classic/flexible framing.
pub(crate) fn put_produce_empty_response(out: &mut BytesMut, version: i16) {
    let flexible = version >= 9;
    if flexible {
        put_compact_array_len(out, 0);
    } else {
        out.put_i32(0);
    }
    if version >= 1 {
        out.put_i32(0); // throttle
    }
    if flexible {
        put_empty_tag_buffer(out);
    }
}

pub(crate) fn encode_produce(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
    // Produce classic v0–8 + flexible v9–13:
    //   request: transactional_id (v3+), acks, timeout, [topic [partition, records]]
    //   response: [topic [partition responses…]], throttle (v1+ at end), tags (v9+)
    //   v4: same wire as v3 (KAFKA_STORAGE_ERROR readiness)
    //   v5–6: log_start_offset in response
    //   v7: ZStd in batches (already supported; request wire unchanged)
    //   v8: record_errors[] + error_message per partition
    //   v9: compact strings/arrays/records + tag buffers + response header v1
    //   v10–12: KIP-951 CurrentLeader (partition tag 0) + NodeEndpoints (top tag 0)
    //   v13: TopicId UUID instead of topic name (request + response)
    let flexible = version >= 9;
    let use_topic_id = version >= 13;
    let mut kip951_leaders: Vec<i32> = Vec::new();

    if version >= 3 {
        let txn_result = if flexible {
            get_compact_nullable_string(src)
        } else {
            get_nullable_string(src)
        };
        let _txn_id = match txn_result {
            Ok(v) => v,
            Err(_) => {
                put_produce_empty_response(out, version);
                return;
            }
        };
    }

    if src.remaining() < 2 + 4 {
        put_produce_empty_response(out, version);
        return;
    }
    let acks = src.get_i16();
    let _timeout_ms = src.get_i32();
    let volant_acks: u8 = match acks {
        -1 => 255,
        0 => 0,
        _ => 1,
    };

    let topic_count = if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => n as i32,
            Ok(None) | Err(_) => {
                put_produce_empty_response(out, version);
                return;
            }
        }
    } else {
        if src.remaining() < 4 {
            put_produce_empty_response(out, version);
            return;
        }
        src.get_i32()
    };

    if flexible {
        put_compact_array_len(out, topic_count.max(0) as usize);
    } else {
        out.put_i32(topic_count.max(0));
    }

    for _ in 0..topic_count.max(0) {
        // v13+: TopicId UUID; ≤v12: topic name string.
        let resolved = match topic_id::read_and_resolve(broker, src, flexible, use_topic_id) {
            Ok(r) => r,
            Err(_) => break,
        };
        topic_id::write_wire_id(out, flexible, &resolved.wire);
        let topic_name = resolved.name_or_empty().to_string();
        let unknown_topic_id = resolved.is_unknown();

        let part_count = if flexible {
            match get_compact_array_len(src) {
                Ok(Some(n)) => n as i32,
                Ok(None) | Err(_) => {
                    if flexible {
                        put_compact_array_len(out, 0);
                        put_empty_tag_buffer(out);
                    } else {
                        out.put_i32(0);
                    }
                    break;
                }
            }
        } else {
            if src.remaining() < 4 {
                out.put_i32(0);
                break;
            }
            src.get_i32()
        };
        if flexible {
            put_compact_array_len(out, part_count.max(0) as usize);
        } else {
            out.put_i32(part_count.max(0));
        }

        for _ in 0..part_count.max(0) {
            if src.remaining() < 4 {
                break;
            }
            let partition = src.get_i32();
            let record_set = if flexible {
                match get_compact_bytes(src) {
                    Ok(b) => b.unwrap_or_default(),
                    Err(_) => {
                        put_produce_partition_response(
                            out,
                            version,
                            partition,
                            KafkaErrorCode::InvalidMessage.as_i16(),
                            -1,
                            -1, None);
                        let _ = skip_tag_buffer(src);
                        continue;
                    }
                }
            } else {
                match get_bytes(src) {
                    Ok(b) => b.unwrap_or_default(),
                    Err(_) => {
                        put_produce_partition_response(
                            out,
                            version,
                            partition,
                            KafkaErrorCode::InvalidMessage.as_i16(),
                            -1,
                            -1, None);
                        continue;
                    }
                }
            };
            if flexible {
                let _ = skip_tag_buffer(src); // partition tags
            }

            if unknown_topic_id {
                put_produce_partition_response(
                    out,
                    version,
                    partition,
                    KafkaErrorCode::UnknownTopicId.as_i16(),
                    -1,
                    -1, None);
                continue;
            }

            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(principal),
                    ResourceType::Topic,
                    &topic_name,
                    AclOperation::Write,
                )
            {
                put_produce_partition_response(
                    out,
                    version,
                    partition,
                    KafkaErrorCode::TopicAuthorizationFailed.as_i16(),
                    -1,
                    -1, None);
                continue;
            }

            let batches = match decode_produce_batches(&record_set) {
                Ok(b) => b,
                Err(e) => {
                    debug!(error = %e, "kafka produce records decode failed");
                    put_produce_partition_response(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::CorruptMessage.as_i16(),
                        -1,
                        -1, None);
                    continue;
                }
            };
            if batches.is_empty() || batches.iter().all(|b| b.messages.is_empty()) {
                let log_start =
                    produce_log_start_offset(broker, &topic_name, partition as u32);
                put_produce_partition_response(
                    out,
                    version,
                    partition,
                    KafkaErrorCode::None.as_i16(),
                    0,
                    log_start, None);
                continue;
            }

            let name = TopicName::new(topic_name.clone());
            let wait = if volant_acks == 255 {
                Some(Duration::from_secs(5))
            } else {
                None
            };

            match produce_partition_batches(
                broker,
                &name,
                partition as u32,
                &batches,
                volant_acks,
                wait,
            ) {
                Ok(base) => {
                    let log_start =
                        produce_log_start_offset(broker, &topic_name, partition as u32);
                    put_produce_partition_response(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::None.as_i16(),
                        base,
                        log_start, None);
                }
                Err(code) => {
                    let leader = if version >= 10 {
                        resolve_current_leader(broker, &topic_name, partition as u32, code)
                    } else {
                        None
                    };
                    if let Some((lid, _)) = leader {
                        kip951_leaders.push(lid);
                    }
                    put_produce_partition_response(
                        out, version, partition, code, -1, -1, leader,
                    );
                }
            }
        }
        if flexible {
            put_empty_tag_buffer(out); // topic tags
            let _ = skip_tag_buffer(src); // request topic tags
        }
    }
    if flexible {
        let _ = skip_tag_buffer(src); // request top-level tags
    }

    // Produce v1+ appends throttle_time_ms at the end.
    if version >= 1 {
        out.put_i32(0);
    }
    if flexible {
        if version >= 10 && !kip951_leaders.is_empty() {
            let endpoints = node_endpoints_for_leaders(broker, &kip951_leaders);
            put_node_endpoints_tag(out, &endpoints);
        } else {
            put_empty_tag_buffer(out); // response top-level tags
        }
    }
}

/// Produce one or more decoded batches for a single partition (Phase 29/31/86).
///
/// Returns the base offset of the first successful batch on success.
/// Transactional produces write-through to the log (Phase 86) and return real offsets.
pub(crate) fn produce_partition_batches(
    broker: &Broker,
    topic: &TopicName,
    partition: u32,
    batches: &[super::codec::DecodedRecordBatch],
    volant_acks: u8,
    wait: Option<Duration>,
) -> std::result::Result<i64, i16> {
    let mut first_base: Option<i64> = None;
    for batch in batches {
        if batch.messages.is_empty() {
            continue;
        }
        let count = batch.messages.len() as u32;
        let producer = batch.producer;

        if producer.is_idempotent() {
            // Volant uses u64 PID and treats 0 as non-idempotent; Kafka PIDs are ≥ 0.
            let pid = producer.producer_id as u64;
            let epoch = producer.producer_epoch as u16;
            let base_seq = producer.base_sequence;

            // Phase 86: transactional PID with open txn → write-through + LSO hold.
            if broker.is_transactional_producer(pid) {
                match broker.buffer_txn_produce(
                    pid,
                    epoch,
                    topic.as_str(),
                    partition,
                    base_seq,
                    batch.messages.clone(),
                ) {
                    IdempotentCheck::Accept { base_offset }
                    | IdempotentCheck::Duplicate { base_offset, .. } => {
                        if first_base.is_none() {
                            first_base = Some(base_offset as i64);
                        }
                    }
                    IdempotentCheck::Reject { error_code } => {
                        return Err(map_idempotent_error(error_code));
                    }
                }
                continue;
            }

            match broker.check_idempotent_produce(
                pid,
                epoch,
                topic.as_str(),
                partition,
                base_seq,
                count,
            ) {
                IdempotentCheck::Accept { .. } => {
                    let mb = MessageBatch {
                        messages: batch.messages.clone(),
                    };
                    let (records, err) = broker
                        .produce_with_acks(topic, PartitionId(partition), mb, volant_acks, wait)
                        .map_err(|e| match e {
                            Error::NotFound(_) => {
                                KafkaErrorCode::UnknownTopicOrPartition.as_i16()
                            }
                            _ => KafkaErrorCode::Unknown.as_i16(),
                        })?;
                    if err != 0 {
                        return Err(map_produce_ack_error(err));
                    }
                    let base = records
                        .first()
                        .map(|r| r.offset.raw())
                        .unwrap_or(0);
                    broker.record_idempotent_produce(
                        pid,
                        epoch,
                        topic.as_str(),
                        partition,
                        base_seq,
                        count,
                        base,
                    );
                    if first_base.is_none() {
                        first_base = Some(base as i64);
                    }
                }
                IdempotentCheck::Duplicate { base_offset, .. } => {
                    if first_base.is_none() {
                        first_base = Some(base_offset as i64);
                    }
                }
                IdempotentCheck::Reject { error_code } => {
                    return Err(map_idempotent_error(error_code));
                }
            }
        } else {
            let mb = MessageBatch {
                messages: batch.messages.clone(),
            };
            let (records, err) = broker
                .produce_with_acks(topic, PartitionId(partition), mb, volant_acks, wait)
                .map_err(|e| match e {
                    Error::NotFound(_) => KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
                    _ => KafkaErrorCode::Unknown.as_i16(),
                })?;
            if err != 0 {
                return Err(map_produce_ack_error(err));
            }
            let base = records
                .first()
                .map(|r| r.offset.raw() as i64)
                .unwrap_or(0);
            if first_base.is_none() {
                first_base = Some(base);
            }
        }
    }
    Ok(first_base.unwrap_or(0))
}

pub(crate) fn map_produce_ack_error(err: u16) -> i16 {
    if err == volant_protocol::ErrorCode::NotLeaderForPartition as u16 {
        KafkaErrorCode::NotLeaderForPartition.as_i16()
    } else {
        KafkaErrorCode::Unknown.as_i16()
    }
}

/// Peek Fetch v7+ `session_id` / `session_epoch` without consuming `src` (Phase 119).
///
/// Returns `None` when the header is truncated or version &lt; 7.
pub(crate) fn peek_fetch_session(version: i16, src: &[u8]) -> Option<(i32, i32)> {
    if version < 7 {
        return None;
    }
    let mut cur = src;
    // replica_id (≤v14), max_wait, min_bytes
    let need = if version <= 14 { 12 } else { 8 };
    if cur.len() < need {
        return None;
    }
    if version <= 14 {
        cur = &cur[4..]; // replica
    }
    cur = &cur[8..]; // max_wait + min_bytes
    if version >= 3 {
        if cur.len() < 4 {
            return None;
        }
        cur = &cur[4..]; // max_bytes
    }
    if version >= 4 {
        if cur.is_empty() {
            return None;
        }
        cur = &cur[1..]; // isolation
    }
    if cur.len() < 8 {
        return None;
    }
    let session_id = i32::from_be_bytes([cur[0], cur[1], cur[2], cur[3]]);
    let session_epoch = i32::from_be_bytes([cur[4], cur[5], cur[6], cur[7]]);
    Some((session_id, session_epoch))
}

/// Write Fetch response header before topic array (classic v0–11 / flexible v12).
pub(crate) fn put_fetch_response_header(
    out: &mut BytesMut,
    version: i16,
    error: i16,
    session_id: i32,
) {
    // throttle (v1+), top-level error + session_id (v7+)
    if version >= 1 {
        out.put_i32(0);
    }
    if version >= 7 {
        out.put_i16(error);
        out.put_i32(session_id);
    }
}

/// Empty Fetch response (no topics) with correct classic/flexible framing.
pub(crate) fn put_fetch_empty_response(
    out: &mut BytesMut,
    version: i16,
    error: i16,
    session_id: i32,
) {
    let flexible = version >= 12;
    put_fetch_response_header(out, version, error, session_id);
    if flexible {
        put_compact_array_len(out, 0);
        put_empty_tag_buffer(out);
    } else {
        out.put_i32(0);
    }
}

/// Write one FetchResponse partition entry (classic v0–11 / flexible v12).
///
/// Order: index, error, hwm, lso (v4+), log_start (v5+), aborted[] (v4+),
/// preferred_read_replica (v11+), records, TAG_BUFFER (v12+).
///
/// v12+: optional **DivergingEpoch tag 0** and **CurrentLeader tag 1**.
///
/// Phase 86: `lso` may be `< hwm` while a write-through txn is open;
/// `aborted` is `(producer_id, first_offset)` for soft abort markers.
///
/// Convenience wrapper around [`put_fetch_partition_response_full`] (LSO≡HWM on
/// success; no aborted list / DivergingEpoch). Kept for call sites and tests.
#[allow(dead_code)]
pub(crate) fn put_fetch_partition_response(
    out: &mut BytesMut,
    version: i16,
    partition: i32,
    error: i16,
    hwm: i64,
    log_start: i64,
    records: &[u8],
    current_leader: Option<(i32, i32)>,
) {
    put_fetch_partition_response_full(
        out,
        version,
        partition,
        error,
        hwm,
        if error == 0 { hwm } else { -1 },
        log_start,
        &[],
        records,
        current_leader,
        None,
        -1,
    );
}

/// Full Fetch partition response with explicit LSO + aborted list (Phase 86),
/// optional DivergingEpoch (Phase 88), and PreferredReadReplica (Phase 126).
pub(crate) fn put_fetch_partition_response_full(
    out: &mut BytesMut,
    version: i16,
    partition: i32,
    error: i16,
    hwm: i64,
    lso: i64,
    log_start: i64,
    aborted: &[(i64, i64)],
    records: &[u8],
    current_leader: Option<(i32, i32)>,
    diverging_epoch: Option<(i32, i64)>,
    preferred_read_replica: i32,
) {
    let flexible = version >= 12;
    // For DivergingEpoch (OFFSET_OUT_OF_RANGE), still emit real hwm/lso/log_start
    // so clients can truncate and continue; other errors keep -1 for lso/log_start.
    let keep_offsets = error == 0 || diverging_epoch.is_some();
    out.put_i32(partition);
    out.put_i16(error);
    out.put_i64(hwm);
    if version >= 4 {
        out.put_i64(if keep_offsets { lso } else { -1 });
    }
    if version >= 5 {
        out.put_i64(if keep_offsets { log_start } else { -1 });
    }
    if version >= 4 {
        if flexible {
            put_compact_array_len(out, aborted.len());
            for &(pid, first) in aborted {
                out.put_i64(pid);
                out.put_i64(first);
                put_empty_tag_buffer(out);
            }
        } else {
            out.put_i32(aborted.len() as i32);
            for &(pid, first) in aborted {
                out.put_i64(pid);
                out.put_i64(first);
            }
        }
    }
    if version >= 11 {
        // -1 = no redirect; positive broker id = preferred follower (Phase 126).
        out.put_i32(preferred_read_replica);
    }
    if flexible {
        put_compact_bytes(out, Some(records));
        let mut tags: Vec<(u32, BytesMut)> = Vec::new();
        if let Some((epoch, end)) = diverging_epoch {
            // Tag 0 = DivergingEpoch (Phase 88).
            tags.push((0, encode_diverging_epoch(epoch, end)));
        }
        if let Some((lid, lep)) = current_leader {
            // Tag 1 = CurrentLeader (Phase 78).
            tags.push((1, encode_leader_id_and_epoch(lid, lep)));
        }
        put_tags(out, &tags);
    } else {
        put_bytes(out, Some(records));
    }
}

/// Parse Fetch request top-level TAG_BUFFER after `rack_id`.
///
/// On flexible versions the buffer may carry:
/// - tag **0** ClusterId (compact string) — content ignored
/// - tag **1** ReplicaState (v15+): `ReplicaId` int32, `ReplicaEpoch` int64,
///   nested TAG_BUFFER — when present with ≥4 body bytes, updates `replica_id`
///   so preferred-replica redirect stays gated for followers (`replica_id >= 0`).
///
/// Leaves `replica_id` unchanged when the buffer is empty, tag 1 is absent, or
/// the ReplicaState body is shorter than 4 bytes (consumer default remains -1).
fn parse_fetch_request_tags(
    src: &mut impl Buf,
    version: i16,
    replica_id: &mut i32,
) -> Result<(), Error> {
    let n = read_unsigned_varint(src)?;
    for _ in 0..n {
        let tag = read_unsigned_varint(src)?;
        let len = read_unsigned_varint(src)? as usize;
        if src.remaining() < len {
            return Err(Error::Protocol("truncated tagged field body".into()));
        }
        if version >= 15 && tag == 1 && len >= 4 {
            // ReplicaState.ReplicaId (int32 BE); rest is epoch + nested tags.
            *replica_id = src.get_i32();
            let rest = len - 4;
            if rest > 0 {
                src.advance(rest);
            }
        } else {
            src.advance(len);
        }
    }
    Ok(())
}

pub(crate) fn encode_fetch(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
    // Fetch classic v0–11 + flexible v12–18 (Kafka max):
    //   request: replica_id (≤v14 only), max_wait, min_bytes,
    //            max_bytes (v3+), isolation (v4+),
    //            session_id + session_epoch (v7+),
    //            topics[{ name (≤v12) | TopicId uuid (v13+), partitions[{
    //              partition, current_leader_epoch (v9+), fetch_offset,
    //              last_fetched_epoch (v12+), log_start_offset (v5+),
    //              partition_max_bytes, tags (ReplicaDirectoryId v17+,
    //              HighWatermark v18+ ignored)
    //            }]}],
    //            forgotten_topics (v7+; name ≤v12 / TopicId v13+),
    //            rack_id (v11+), tags (v12+: ClusterId; v15+: ReplicaState)
    //   response: throttle (v1+), error+session_id (v7+),
    //             topics[{ name ≤v12 | TopicId v13+, partitions[{…}]}],
    //             tags (v12+; NodeEndpoints tag 0 on v16+ when CurrentLeader set)
    // Phase 88: real fetch sessions + DivergingEpoch (tag 0) on truncation.
    // Phase 91: omit-unchanged on empty-topics incremental (last HWM/LSO cache).
    // Phase 95/115: idle TTL + max sessions (lazy LRU); durable snapshot on mutations.
    // v14: wire-identical to v13 (OffsetMovedToTieredStorage never emitted).
    // v15: top-level ReplicaId dropped; ReplicaState tag 1 parsed for gate.
    // v16: NodeEndpoints top-level tag (KIP-951) on leader errors.
    // v17–18: request-only tagged fields; response framing unchanged from v16.
    use super::fetch_session::{
        FetchSessionManager, SessionPartition, SessionTopic, FINAL_EPOCH, INITIAL_EPOCH,
    };
    use super::topic_id::TopicWireId;
    use std::collections::HashMap;

    let flexible = version >= 12;
    let use_topic_id = version >= 13;

    // ReplicaId is a top-level field only through v14 (KIP-903 / Kafka v15+).
    // Consumer fetches use replica_id < 0; followers use >= 0. Preferred
    // redirect (Phase 126) only applies to consumer fetches.
    let header_need = if version <= 14 { 4 + 4 + 4 } else { 4 + 4 };
    if src.remaining() < header_need {
        put_fetch_empty_response(out, version, 0, 0);
        return;
    }
    // Default -1 = consumer. v15+ drops top-level ReplicaId; ReplicaState
    // (top-level request tag 1) is parsed below after rack_id.
    let mut replica_id = -1i32;
    if version <= 14 {
        replica_id = src.get_i32();
    }
    let _max_wait = src.get_i32();
    let _min_bytes = src.get_i32();
    if version >= 3 {
        if src.remaining() < 4 {
            put_fetch_empty_response(out, version, 0, 0);
            return;
        }
        let _max_bytes = src.get_i32();
    }
    // Phase 36/86: isolation_level (v4). 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED.
    let mut isolation = 0u8;
    if version >= 4 {
        if src.remaining() < 1 {
            put_fetch_empty_response(out, version, 0, 0);
            return;
        }
        isolation = src.get_u8();
        if isolation > 1 {
            put_fetch_empty_response(out, version, 0, 0);
            return;
        }
    }
    let read_committed = isolation == 1;

    // v7+: fetch session fields (Phase 88: real process-local sessions).
    let mut req_session_id = 0i32;
    let mut req_session_epoch = FINAL_EPOCH;
    if version >= 7 {
        if src.remaining() < 4 + 4 {
            put_fetch_empty_response(out, version, 0, 0);
            return;
        }
        req_session_id = src.get_i32();
        req_session_epoch = src.get_i32();
    }

    // --- Parse topics ---
    let topic_count = if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => n as i32,
            Ok(None) | Err(_) => {
                put_fetch_empty_response(out, version, 0, 0);
                return;
            }
        }
    } else {
        if src.remaining() < 4 {
            put_fetch_empty_response(out, version, 0, 0);
            return;
        }
        src.get_i32()
    };

    let mut req_topics: HashMap<String, SessionTopic> = HashMap::new();
    // Preserve request order for response when topics non-empty.
    let mut req_topic_order: Vec<String> = Vec::new();

    for _ in 0..topic_count.max(0) {
        let resolved = match topic_id::read_and_resolve(broker, src, flexible, use_topic_id) {
            Ok(r) => r,
            Err(_) => break,
        };
        let key = FetchSessionManager::topic_key(&resolved.wire, resolved.name_or_empty());
        let part_count = if flexible {
            match get_compact_array_len(src) {
                Ok(Some(n)) => n as i32,
                Ok(None) | Err(_) => break,
            }
        } else {
            if src.remaining() < 4 {
                break;
            }
            src.get_i32()
        };

        let mut partitions = HashMap::new();
        for _ in 0..part_count.max(0) {
            let need = 4
                + if version >= 9 { 4 } else { 0 }
                + 8
                + if version >= 12 { 4 } else { 0 }
                + if version >= 5 { 8 } else { 0 }
                + 4;
            if src.remaining() < need {
                break;
            }
            let partition = src.get_i32();
            let current_leader_epoch = if version >= 9 {
                src.get_i32()
            } else {
                -1
            };
            let fetch_offset = src.get_i64();
            let last_fetched_epoch = if version >= 12 {
                src.get_i32()
            } else {
                -1
            };
            if version >= 5 {
                let _follower_log_start = src.get_i64();
            }
            let max_bytes = src.get_i32().max(0) as usize;
            if flexible {
                let _ = skip_tag_buffer(src);
            }
            partitions.insert(
                partition,
                SessionPartition::new(
                    fetch_offset,
                    current_leader_epoch,
                    last_fetched_epoch,
                    max_bytes,
                ),
            );
        }
        if flexible {
            let _ = skip_tag_buffer(src); // topic request tags
        }
        if !req_topics.contains_key(&key) {
            req_topic_order.push(key.clone());
        }
        let entry = req_topics.entry(key).or_insert_with(|| SessionTopic {
            wire: resolved.wire.clone(),
            name: resolved.name_or_empty().to_string(),
            partitions: HashMap::new(),
        });
        entry.wire = resolved.wire;
        if let Some(n) = resolved.name {
            entry.name = n;
        }
        entry.partitions.extend(partitions);
    }

    // --- Parse forgotten_topics_data (v7+) ---
    let mut forgotten: Vec<(String, Vec<i32>)> = Vec::new();
    if version >= 7 {
        if flexible {
            if let Ok(Some(n)) = get_compact_array_len(src) {
                for _ in 0..n {
                    let resolved = match topic_id::read_and_resolve(broker, src, true, use_topic_id)
                    {
                        Ok(r) => r,
                        Err(_) => break,
                    };
                    let key =
                        FetchSessionManager::topic_key(&resolved.wire, resolved.name_or_empty());
                    let mut parts = Vec::new();
                    if let Ok(Some(pn)) = get_compact_array_len(src) {
                        for _ in 0..pn {
                            if src.remaining() < 4 {
                                break;
                            }
                            parts.push(src.get_i32());
                        }
                    }
                    let _ = skip_tag_buffer(src);
                    forgotten.push((key, parts));
                }
            }
        } else if src.remaining() >= 4 {
            let n = src.get_i32();
            for _ in 0..n.max(0) {
                let name = match get_string(src) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let mut parts = Vec::new();
                if src.remaining() < 4 {
                    break;
                }
                let pn = src.get_i32();
                for _ in 0..pn.max(0) {
                    if src.remaining() < 4 {
                        break;
                    }
                    parts.push(src.get_i32());
                }
                forgotten.push((name, parts));
            }
        }
    }
    // v11+: client rack_id for PreferredReadReplica (Phase 126).
    let mut client_rack: Option<String> = None;
    if version >= 11 {
        client_rack = if flexible {
            get_compact_string(src).ok().filter(|s| !s.is_empty())
        } else {
            get_string(src).ok().filter(|s| !s.is_empty())
        };
    }
    if flexible {
        // Request top-level tags: ClusterId (v12+ tag 0), ReplicaState (v15+ tag 1).
        let _ = parse_fetch_request_tags(src, version, &mut replica_id);
    }

    // --- Session handling (v7+) ---
    let sessions = broker.fetch_sessions();
    let mut resp_session_id = 0i32;
    let mut top_error = KafkaErrorCode::None.as_i16();
    // Phase 91: omit-unchanged only on empty-topics incremental.
    let mut omit_unchanged = false;
    // Topics to actually fetch/respond (order + content).
    let (fetch_topics, fetch_order) = if version < 7 {
        (req_topics, req_topic_order)
    } else if req_session_epoch == FINAL_EPOCH {
        // Close any existing session; full fetch; no new session.
        sessions.close(req_session_id);
        resp_session_id = 0;
        (req_topics, req_topic_order)
    } else if req_session_id == 0 || req_session_epoch == INITIAL_EPOCH {
        // Full fetch + create session from request partitions.
        // If client sent a non-zero id with INITIAL, close the old first.
        if req_session_id != 0 {
            sessions.close(req_session_id);
        }
        let mut fetch_topics = req_topics.clone();
        let mut fetch_order = req_topic_order.clone();
        // Apply forgotten against the new session set before create.
        for (key, parts) in &forgotten {
            if parts.is_empty() {
                fetch_topics.remove(key);
                continue;
            }
            if let Some(t) = fetch_topics.get_mut(key) {
                for p in parts {
                    t.partitions.remove(p);
                }
                if t.partitions.is_empty() {
                    fetch_topics.remove(key);
                }
            }
        }
        fetch_order.retain(|k| fetch_topics.contains_key(k));
        resp_session_id = sessions.create(fetch_topics.clone());
        (fetch_topics, fetch_order)
    } else {
        // Incremental: validate session + epoch.
        match sessions.begin_incremental(req_session_id, req_session_epoch) {
            Ok(()) => {
                resp_session_id = req_session_id;
                sessions.merge_topics(req_session_id, &req_topics);
                sessions.forget(req_session_id, &forgotten);
                if req_topics.is_empty() {
                    // Empty topics → re-fetch entire session set; omit unchanged (Phase 91).
                    omit_unchanged = true;
                    let fetch_topics = sessions.snapshot_topics(req_session_id);
                    let mut fetch_order: Vec<String> = fetch_topics.keys().cloned().collect();
                    fetch_order.sort(); // stable deterministic order
                    (fetch_topics, fetch_order)
                } else {
                    // Partial/updated topics: always include those partitions.
                    (req_topics, req_topic_order)
                }
            }
            Err(code) => {
                top_error = code;
                resp_session_id = req_session_id;
                // Empty responses on session error.
                put_fetch_empty_response(out, version, top_error, resp_session_id);
                return;
            }
        }
    };

    // --- Build + filter partition responses (Phase 91 may omit) ---
    // One built partition ready to encode (or skip).
    struct BuiltPart {
        partition: i32,
        error: i16,
        hwm: i64,
        lso: i64,
        log_start: i64,
        aborted: Vec<(i64, i64)>,
        records: BytesMut,
        current_leader: Option<(i32, i32)>,
        diverging: Option<(i32, i64)>,
        /// PreferredReadReplica broker id, or -1 (Phase 126).
        preferred_read_replica: i32,
        /// When true and omit_unchanged, drop from response.
        omit: bool,
        /// Update session last_hwm/lso after include.
        note_cache: bool,
    }

    let mut kip951_leaders: Vec<i32> = Vec::new();
    // (topic_key, wire, name, included partitions)
    let mut built_topics: Vec<(String, TopicWireId, String, Vec<BuiltPart>)> = Vec::new();

    for key in &fetch_order {
        let Some(topic) = fetch_topics.get(key) else {
            continue;
        };
        let topic_name = topic.name.clone();
        let unknown_topic_id = matches!(topic.wire, TopicWireId::Uuid(_)) && topic_name.is_empty();

        let mut part_ids: Vec<i32> = topic.partitions.keys().copied().collect();
        part_ids.sort_unstable();
        let mut built_parts: Vec<BuiltPart> = Vec::new();

        for partition in part_ids {
            let Some(part_req) = topic.partitions.get(&partition) else {
                continue;
            };
            let current_leader_epoch = part_req.current_leader_epoch;
            let fetch_offset = part_req.fetch_offset;
            let last_fetched_epoch = part_req.last_fetched_epoch;
            let max_bytes = part_req.max_bytes;

            let mut push_err = |error: i16, current_leader: Option<(i32, i32)>| {
                built_parts.push(BuiltPart {
                    partition,
                    error,
                    hwm: -1,
                    lso: -1,
                    log_start: -1,
                    aborted: Vec::new(),
                    records: BytesMut::new(),
                    current_leader,
                    diverging: None,
                    preferred_read_replica: -1,
                    omit: false,
                    note_cache: false,
                });
            };

            if unknown_topic_id {
                push_err(KafkaErrorCode::UnknownTopicId.as_i16(), None);
                continue;
            }

            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(principal),
                    ResourceType::Topic,
                    &topic_name,
                    AclOperation::Read,
                )
            {
                push_err(KafkaErrorCode::TopicAuthorizationFailed.as_i16(), None);
                continue;
            }

            let name = TopicName::new(topic_name.clone());
            let snap = broker.metadata(Some(&[name.clone()]));
            let part_meta = snap.topics.first().and_then(|t| {
                t.partitions
                    .iter()
                    .find(|p| p.partition_id.0 == partition as u32)
            });

            // v9+: fence on current_leader_epoch (-1 = no fence).
            if current_leader_epoch != -1 {
                let current_epoch = part_meta.map(|p| p.leader_epoch as i32).unwrap_or(-1);
                if current_leader_epoch > current_epoch && current_epoch >= 0 {
                    push_err(KafkaErrorCode::UnknownLeaderEpoch.as_i16(), None);
                    continue;
                }
                if current_epoch >= 0 && current_leader_epoch < current_epoch {
                    let leader = part_meta.map(|p| (p.leader as i32, p.leader_epoch as i32));
                    if let Some((lid, _)) = leader {
                        kip951_leaders.push(lid);
                    }
                    push_err(KafkaErrorCode::FencedLeaderEpoch.as_i16(), leader);
                    continue;
                }
            }

            // Phase 88: DivergingEpoch when last_fetched_epoch indicates truncation.
            if version >= 12 && last_fetched_epoch != -1 {
                if let Some((found_epoch, end_offset)) =
                    broker.offset_for_leader_epoch(&topic_name, partition as u32, last_fetched_epoch)
                {
                    if fetch_offset > end_offset {
                        let hwm = part_meta.map(|p| p.hwm as i64).unwrap_or(end_offset);
                        let lso =
                            broker.last_stable_offset(name.as_str(), partition as u32) as i64;
                        let log_start =
                            produce_log_start_offset(broker, &topic_name, partition as u32);
                        built_parts.push(BuiltPart {
                            partition,
                            error: KafkaErrorCode::OffsetOutOfRange.as_i16(),
                            hwm,
                            lso: if version >= 4 { lso } else { hwm },
                            log_start,
                            aborted: Vec::new(),
                            records: BytesMut::new(),
                            current_leader: None,
                            diverging: Some((found_epoch, end_offset)),
                            preferred_read_replica: -1,
                            omit: false,
                            note_cache: false,
                        });
                        continue;
                    }
                }
            }

            // Phase 126: PreferredReadReplica redirect (consumer Fetch only;
            // empty records). replica_id >= 0 ⇒ follower — never redirect.
            // READ_COMMITTED (isolation=1): suppress preferred — followers may
            // lack a complete soft-abort-marker view → filter/LSO divergence vs
            // leader. Keep reads on the leader (MVP residual vs full marker parity).
            if version >= 11 && replica_id < 0 && !read_committed {
                if let Some(pref) = broker.select_preferred_read_replica(
                    &name,
                    PartitionId(partition as u32),
                    client_rack.as_deref(),
                ) {
                    let hwm = part_meta.map(|p| p.hwm as i64).unwrap_or(0);
                    let lso =
                        broker.last_stable_offset(name.as_str(), partition as u32) as i64;
                    let log_start =
                        produce_log_start_offset(broker, &topic_name, partition as u32);
                    let resp_lso = if version >= 4 { lso } else { hwm };
                    broker.note_preferred_replica_redirect();
                    built_parts.push(BuiltPart {
                        partition,
                        error: KafkaErrorCode::None.as_i16(),
                        hwm,
                        lso: resp_lso,
                        log_start,
                        aborted: Vec::new(),
                        records: BytesMut::new(),
                        current_leader: None,
                        diverging: None,
                        preferred_read_replica: pref as i32,
                        // Never omit preferred redirects (client must see the id).
                        omit: false,
                        note_cache: false,
                    });
                    continue;
                }
            }

            let max_messages = (max_bytes / 64).clamp(1, 10_000);
            let from = Offset::new(fetch_offset.max(0) as u64);
            match broker.fetch_kafka(
                &name,
                PartitionId(partition as u32),
                from,
                max_messages,
                read_committed,
            ) {
                Ok(records) => {
                    let mut selected = Vec::new();
                    let mut used = 0usize;
                    for r in records {
                        let approx =
                            r.value.len() + r.key.as_ref().map(|k| k.len()).unwrap_or(0) + 32;
                        if !selected.is_empty() && used + approx > max_bytes {
                            break;
                        }
                        used += approx;
                        selected.push(r);
                    }
                    let hwm_fallback = selected
                        .last()
                        .map(|r| r.offset.raw() as i64 + 1)
                        .unwrap_or(fetch_offset);
                    let hwm = part_meta.map(|p| p.hwm as i64).unwrap_or(hwm_fallback);
                    let lso = broker.last_stable_offset(name.as_str(), partition as u32) as i64;
                    let log_start =
                        produce_log_start_offset(broker, &topic_name, partition as u32);
                    let aborted_pairs: Vec<(i64, i64)> = if read_committed && version >= 4 {
                        broker
                            .aborted_transactions_for_fetch(
                                name.as_str(),
                                partition as u32,
                                from.raw(),
                                lso.max(0) as u64,
                            )
                            .into_iter()
                            .map(|(pid, first)| (pid as i64, first as i64))
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let set = encode_fetch_record_set(&selected, version);
                    let resp_lso = if version >= 4 { lso } else { hwm };
                    let records_empty = set.is_empty();
                    let omit = omit_unchanged
                        && part_req.should_omit_unchanged(
                            hwm,
                            resp_lso,
                            records_empty,
                            KafkaErrorCode::None.as_i16(),
                        );
                    built_parts.push(BuiltPart {
                        partition,
                        error: KafkaErrorCode::None.as_i16(),
                        hwm,
                        lso: resp_lso,
                        log_start,
                        aborted: aborted_pairs,
                        records: set,
                        current_leader: None,
                        diverging: None,
                        preferred_read_replica: -1,
                        omit,
                        note_cache: true,
                    });
                }
                Err(Error::NotFound(_)) => {
                    push_err(KafkaErrorCode::UnknownTopicOrPartition.as_i16(), None);
                }
                Err(_) => {
                    push_err(KafkaErrorCode::Unknown.as_i16(), None);
                }
            }
        }

        built_topics.push((
            key.clone(),
            topic.wire.clone(),
            topic_name,
            built_parts,
        ));
    }

    // Filter omitted partitions; drop topics with no remaining partitions.
    let mut topics_out: Vec<(String, TopicWireId, String, Vec<BuiltPart>)> = Vec::new();
    for (key, wire, name, parts) in built_topics {
        let kept: Vec<BuiltPart> = parts.into_iter().filter(|p| !p.omit).collect();
        if kept.is_empty() {
            continue;
        }
        topics_out.push((key, wire, name, kept));
    }

    // --- Encode responses ---
    put_fetch_response_header(out, version, top_error, resp_session_id);
    if flexible {
        put_compact_array_len(out, topics_out.len());
    } else {
        out.put_i32(topics_out.len() as i32);
    }

    for (key, wire, _name, parts) in &topics_out {
        topic_id::write_wire_id(out, flexible, wire);
        if flexible {
            put_compact_array_len(out, parts.len());
        } else {
            out.put_i32(parts.len() as i32);
        }
        for p in parts {
            put_fetch_partition_response_full(
                out,
                version,
                p.partition,
                p.error,
                p.hwm,
                p.lso,
                p.log_start,
                &p.aborted,
                &p.records,
                p.current_leader,
                p.diverging,
                p.preferred_read_replica,
            );
            // Phase 91: seed/refresh omit cache for successful includes.
            if p.note_cache && p.error == 0 && resp_session_id != 0 {
                sessions.note_returned(resp_session_id, key, p.partition, p.hwm, p.lso);
            }
        }
        if flexible {
            put_empty_tag_buffer(out); // topic response tags
        }
    }

    if flexible {
        // Response top-level: NodeEndpoints (tag 0) on v16+ when any CurrentLeader.
        if version >= 16 && !kip951_leaders.is_empty() {
            let endpoints = node_endpoints_for_leaders(broker, &kip951_leaders);
            put_node_endpoints_tag(out, &endpoints);
        } else {
            put_empty_tag_buffer(out);
        }
    }
}

/// Encode partition records for a Fetch response.
///
/// Phase 32: v4 RecordBatch compression. Phase 33: v0–3 MessageSet wrapper compression.
/// Phase 89: txn control markers re-encode as Kafka control RecordBatches (v4+);
/// MessageSet path omits control markers (magic-2 only).
pub(crate) fn encode_fetch_record_set(records: &[volant_core::Record], version: i16) -> BytesMut {
    if records.is_empty() {
        return BytesMut::new();
    }
    let codec = fetch_compression_codec();
    if version < 4 {
        // MessageSet path (Phase 33): control frames are magic-2 only — drop them.
        let data: Vec<volant_core::Record> = records
            .iter()
            .filter(|r| !is_txn_control_record(r))
            .cloned()
            .collect();
        if data.is_empty() {
            return BytesMut::new();
        }
        if codec == CompressionCodec::None {
            return encode_message_set(&data);
        }
        return match encode_message_set_compressed(&data, codec) {
            Ok(set) => set,
            Err(e) => {
                debug!(error = %e, ?codec, "message set fetch compression failed; plain");
                encode_message_set(&data)
            }
        };
    }
    // RecordBatch path (Phase 32/89): interleave data batches + control batches.
    encode_fetch_record_batches(records, codec)
}

/// Encode Fetch v4+ records as contiguous RecordBatches, emitting a control
/// batch for each Phase 89 txn control marker.
fn encode_fetch_record_batches(
    records: &[volant_core::Record],
    codec: CompressionCodec,
) -> BytesMut {
    let mut out = BytesMut::new();
    let mut data: Vec<volant_core::Record> = Vec::new();
    let flush_data = |data: &mut Vec<volant_core::Record>, out: &mut BytesMut| {
        if data.is_empty() {
            return;
        }
        let batch = if codec == CompressionCodec::None {
            encode_record_batch(data)
        } else {
            match encode_record_batch_compressed(data, codec) {
                Ok(b) => b,
                Err(e) => {
                    debug!(error = %e, ?codec, "fetch compression failed; falling back to plain");
                    encode_record_batch(data)
                }
            }
        };
        out.extend_from_slice(&batch);
        data.clear();
    };
    for r in records {
        if let Some(ctrl) = parse_txn_control_record(r) {
            flush_data(&mut data, &mut out);
            let batch = encode_control_record_batch(
                r.offset.raw() as i64,
                ctrl.producer_id,
                ctrl.producer_epoch,
                ctrl.marker_type,
                ctrl.coordinator_epoch,
                r.timestamp_ms,
            );
            out.extend_from_slice(&batch);
        } else {
            data.push(r.clone());
        }
    }
    flush_data(&mut data, &mut out);
    out
}

/// OffsetForLeaderEpoch (API key 23) classic v0–3 / flexible v4 — Phase 39 / 63 / 87.
///
/// With durable epoch history (Phase 87), prior epochs return the end offset at
/// which that epoch closed (start of the next epoch). Current / `-1` still map
/// to HWM. Unknown requested epochs above the live epoch remain UNKNOWN.
pub(crate) fn encode_offset_for_leader_epoch(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // Request: replica_id (v3+), topics[{ name, partitions[{ partition,
    //   current_leader_epoch (v2+), leader_epoch }]}] [, tags v4+]
    // Response: throttle (v2+), topics[{ name, partitions[{ error, partition,
    //   leader_epoch (v1+), end_offset }]}] [, tags v4+]
    let flex = version >= 4;

    let empty = |out: &mut BytesMut| {
        if version >= 2 {
            out.put_i32(0); // throttle
        }
        if flex {
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
        } else {
            out.put_i32(0);
        }
    };

    if version >= 3 {
        if src.remaining() < 4 {
            empty(out);
            return;
        }
        let _replica_id = src.get_i32();
    }

    struct PartIn {
        partition: i32,
        current_leader_epoch: i32,
        leader_epoch: i32,
    }
    struct TopicIn {
        name: String,
        parts: Vec<PartIn>,
    }
    let mut topics: Vec<TopicIn> = Vec::new();

    if flex {
        let topic_count = match get_compact_array_len(src) {
            Ok(Some(n)) => n,
            Ok(None) => 0,
            Err(_) => {
                empty(out);
                return;
            }
        };
        for _ in 0..topic_count {
            let name = match get_compact_string(src) {
                Ok(t) => t,
                Err(_) => break,
            };
            let part_count = match get_compact_array_len(src) {
                Ok(Some(n)) => n,
                Ok(None) => 0,
                Err(_) => break,
            };
            let mut parts = Vec::with_capacity(part_count);
            for _ in 0..part_count {
                // partition + current_leader_epoch (v2+) + leader_epoch
                if src.remaining() < 4 + 4 + 4 {
                    break;
                }
                let partition = src.get_i32();
                let current_leader_epoch = src.get_i32();
                let leader_epoch = src.get_i32();
                let _ = skip_tag_buffer(src);
                parts.push(PartIn {
                    partition,
                    current_leader_epoch,
                    leader_epoch,
                });
            }
            let _ = skip_tag_buffer(src);
            topics.push(TopicIn { name, parts });
        }
        let _ = skip_tag_buffer(src);
    } else {
        if src.remaining() < 4 {
            empty(out);
            return;
        }
        let topic_count = src.get_i32();
        for _ in 0..topic_count.max(0) {
            let name = match get_string(src) {
                Ok(t) => t,
                Err(_) => break,
            };
            if src.remaining() < 4 {
                topics.push(TopicIn {
                    name,
                    parts: vec![],
                });
                break;
            }
            let part_count = src.get_i32();
            let mut parts = Vec::new();
            for _ in 0..part_count.max(0) {
                let need = if version >= 2 { 4 + 4 + 4 } else { 4 + 4 };
                if src.remaining() < need {
                    break;
                }
                let partition = src.get_i32();
                let current_leader_epoch = if version >= 2 {
                    src.get_i32()
                } else {
                    -1
                };
                let leader_epoch = src.get_i32();
                parts.push(PartIn {
                    partition,
                    current_leader_epoch,
                    leader_epoch,
                });
            }
            topics.push(TopicIn { name, parts });
        }
    }

    if version >= 2 {
        out.put_i32(0); // throttle
    }
    if flex {
        put_compact_array_len(out, topics.len());
    } else {
        out.put_i32(topics.len() as i32);
    }

    for t in topics {
        if flex {
            put_compact_string(out, &t.name);
            put_compact_array_len(out, t.parts.len());
        } else {
            put_string(out, &t.name);
            out.put_i32(t.parts.len() as i32);
        }

        for p in t.parts {
            let write_part = |out: &mut BytesMut, err: i16, epoch: i32, end: i64| {
                out.put_i16(err);
                out.put_i32(p.partition);
                if version >= 1 {
                    out.put_i32(epoch);
                }
                out.put_i64(end);
                if flex {
                    put_empty_tag_buffer(out);
                }
            };

            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(principal),
                    ResourceType::Topic,
                    &t.name,
                    AclOperation::Describe,
                )
            {
                write_part(
                    out,
                    KafkaErrorCode::TopicAuthorizationFailed.as_i16(),
                    -1,
                    -1,
                );
                continue;
            }

            let name = TopicName::new(t.name.clone());
            let snap = broker.metadata(Some(&[name]));
            let part_meta = snap.topics.first().and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|pm| pm.partition_id.0 == p.partition as u32)
            });

            let Some(pm) = part_meta else {
                write_part(
                    out,
                    KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
                    -1,
                    -1,
                );
                continue;
            };

            let current_epoch = pm.leader_epoch as i32;

            if p.current_leader_epoch != -1 {
                if p.current_leader_epoch > current_epoch {
                    write_part(
                        out,
                        KafkaErrorCode::UnknownLeaderEpoch.as_i16(),
                        current_epoch,
                        -1,
                    );
                    continue;
                }
                if p.current_leader_epoch < current_epoch {
                    write_part(
                        out,
                        KafkaErrorCode::FencedLeaderEpoch.as_i16(),
                        current_epoch,
                        -1,
                    );
                    continue;
                }
            }

            if p.leader_epoch != -1 && p.leader_epoch > current_epoch {
                write_part(
                    out,
                    KafkaErrorCode::UnknownLeaderEpoch.as_i16(),
                    current_epoch,
                    -1,
                );
                continue;
            }

            // Phase 87: durable history for prior epochs; current / -1 → HWM.
            match broker.offset_for_leader_epoch(t.name.as_str(), p.partition as u32, p.leader_epoch)
            {
                Some((found_epoch, end_offset)) => {
                    write_part(
                        out,
                        KafkaErrorCode::None.as_i16(),
                        found_epoch,
                        end_offset,
                    );
                }
                None => {
                    write_part(
                        out,
                        KafkaErrorCode::UnknownLeaderEpoch.as_i16(),
                        current_epoch,
                        -1,
                    );
                }
            }
        }
        if flex {
            put_empty_tag_buffer(out); // topic tags
        }
    }
    if flex {
        put_empty_tag_buffer(out); // response top-level tags
    }
}

pub(crate) fn encode_list_offsets(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // ListOffsets classic v0–5 / flexible v6–11:
    //   replica_id, isolation_level (v2+),
    //   topics[{ name, partitions[{ partition, current_leader_epoch (v4+),
    //     timestamp, max_num_offsets (v0) }]}]
    //   TimeoutMs (v10+, ignored), [, tags v6+]
    // Response: throttle (v2+), topics[{ name, partitions[{ partition, error,
    //   v0: [timestamp,offset] array | v1+: timestamp, offset, leader_epoch (v4+) }]}]
    //   [, tags v6+]
    // Special timestamps (Kafka ListOffsetsRequest):
    //   -1 latest, -2 earliest,
    //   -3 max timestamp (v7+, KIP-734), -4 earliest local (v8+ ≡ earliest),
    //   -5 latest tiered (v9+, no remote → -1/-1),
    //   -6 earliest pending upload (v11+, no remote → -1/-1).
    // Positive / other timestamps → InvalidTimestamp (no time index).
    let flex = version >= 6;

    let empty = |out: &mut BytesMut| {
        if version >= 2 {
            out.put_i32(0);
        }
        if flex {
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
        } else {
            out.put_i32(0);
        }
    };

    if src.remaining() < 4 {
        empty(out);
        return;
    }
    let _replica_id = src.get_i32();

    // v2+: isolation_level (0 / 1). Phase 86: READ_COMMITTED latest uses LSO.
    let mut list_read_committed = false;
    if version >= 2 {
        if src.remaining() < 1 {
            empty(out);
            return;
        }
        let isolation = src.get_u8();
        if isolation > 1 {
            empty(out);
            return;
        }
        list_read_committed = isolation == 1;
    }

    struct PartIn {
        partition: i32,
        current_leader_epoch: i32,
        timestamp: i64,
    }
    struct TopicIn {
        name: String,
        parts: Vec<PartIn>,
    }
    let mut topics: Vec<TopicIn> = Vec::new();

    if flex {
        let topic_count = match get_compact_array_len(src) {
            Ok(Some(n)) => n,
            Ok(None) => 0,
            Err(_) => {
                empty(out);
                return;
            }
        };
        for _ in 0..topic_count {
            let name = match get_compact_string(src) {
                Ok(t) => t,
                Err(_) => break,
            };
            let part_count = match get_compact_array_len(src) {
                Ok(Some(n)) => n,
                Ok(None) => 0,
                Err(_) => break,
            };
            let mut parts = Vec::with_capacity(part_count);
            for _ in 0..part_count {
                // partition + current_leader_epoch (v4+) + timestamp
                if src.remaining() < 4 + 4 + 8 {
                    break;
                }
                let partition = src.get_i32();
                let current_leader_epoch = src.get_i32();
                let timestamp = src.get_i64();
                let _ = skip_tag_buffer(src);
                parts.push(PartIn {
                    partition,
                    current_leader_epoch,
                    timestamp,
                });
            }
            let _ = skip_tag_buffer(src);
            topics.push(TopicIn { name, parts });
        }
        // v10+: TimeoutMs (remote/tiered await) — parsed, ignored.
        if version >= 10 && src.remaining() >= 4 {
            let _timeout_ms = src.get_i32();
        }
        let _ = skip_tag_buffer(src);
    } else {
        if src.remaining() < 4 {
            empty(out);
            return;
        }
        let topic_count = src.get_i32();
        for _ in 0..topic_count.max(0) {
            let name = match get_string(src) {
                Ok(t) => t,
                Err(_) => break,
            };
            if src.remaining() < 4 {
                topics.push(TopicIn {
                    name,
                    parts: vec![],
                });
                break;
            }
            let part_count = src.get_i32();
            let mut parts = Vec::new();
            for _ in 0..part_count.max(0) {
                let need = if version >= 4 { 4 + 4 + 8 } else { 4 + 8 };
                if src.remaining() < need {
                    break;
                }
                let partition = src.get_i32();
                let current_leader_epoch = if version >= 4 {
                    src.get_i32()
                } else {
                    -1
                };
                let timestamp = src.get_i64();
                if version == 0 {
                    if src.remaining() < 4 {
                        break;
                    }
                    let _max_num = src.get_i32();
                }
                parts.push(PartIn {
                    partition,
                    current_leader_epoch,
                    timestamp,
                });
            }
            topics.push(TopicIn { name, parts });
        }
    }

    /// Write a partition result with versioned fields.
    fn write_part(
        out: &mut BytesMut,
        version: i16,
        flex: bool,
        partition: i32,
        err: i16,
        timestamp: i64,
        offset: i64,
        leader_epoch: i32,
    ) {
        out.put_i32(partition);
        out.put_i16(err);
        if version == 0 {
            if err == KafkaErrorCode::None.as_i16() {
                out.put_i32(1);
                out.put_i64(timestamp);
                out.put_i64(offset);
            } else {
                out.put_i32(0); // empty old-style offsets array
            }
        } else {
            out.put_i64(timestamp);
            out.put_i64(offset);
            if version >= 4 {
                out.put_i32(leader_epoch);
            }
        }
        if flex {
            put_empty_tag_buffer(out);
        }
    }

    if version >= 2 {
        out.put_i32(0); // throttle
    }
    if flex {
        put_compact_array_len(out, topics.len());
    } else {
        out.put_i32(topics.len() as i32);
    }

    for t in topics {
        if flex {
            put_compact_string(out, &t.name);
            put_compact_array_len(out, t.parts.len());
        } else {
            put_string(out, &t.name);
            out.put_i32(t.parts.len() as i32);
        }

        for p in t.parts {
            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(principal),
                    ResourceType::Topic,
                    &t.name,
                    AclOperation::Describe,
                )
            {
                write_part(
                    out,
                    version,
                    flex,
                    p.partition,
                    KafkaErrorCode::TopicAuthorizationFailed.as_i16(),
                    p.timestamp,
                    -1,
                    -1,
                );
                continue;
            }

            let name = TopicName::new(t.name.clone());
            let part_meta = broker.metadata(Some(&[name])).topics.first().and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|pm| pm.partition_id.0 == p.partition as u32)
                    .cloned()
            });
            let current_epoch = part_meta
                .as_ref()
                .map(|pm| pm.leader_epoch as i32)
                .unwrap_or(-1);

            if p.current_leader_epoch != -1 {
                if p.current_leader_epoch > current_epoch && current_epoch >= 0 {
                    write_part(
                        out,
                        version,
                        flex,
                        p.partition,
                        KafkaErrorCode::UnknownLeaderEpoch.as_i16(),
                        p.timestamp,
                        -1,
                        current_epoch,
                    );
                    continue;
                }
                if p.current_leader_epoch < current_epoch {
                    write_part(
                        out,
                        version,
                        flex,
                        p.partition,
                        KafkaErrorCode::FencedLeaderEpoch.as_i16(),
                        p.timestamp,
                        -1,
                        current_epoch,
                    );
                    continue;
                }
            }

            // Kafka special timestamps (version-gated).
            const LATEST: i64 = -1;
            const EARLIEST: i64 = -2;
            const MAX_TIMESTAMP: i64 = -3;
            const EARLIEST_LOCAL: i64 = -4;
            const LATEST_TIERED: i64 = -5;
            const EARLIEST_PENDING_UPLOAD: i64 = -6;

            let ts = p.timestamp;
            let allowed = match ts {
                LATEST | EARLIEST => true,
                MAX_TIMESTAMP if version >= 7 => true,
                EARLIEST_LOCAL if version >= 8 => true,
                LATEST_TIERED if version >= 9 => true,
                EARLIEST_PENDING_UPLOAD if version >= 11 => true,
                _ => false,
            };
            if !allowed {
                write_part(
                    out,
                    version,
                    flex,
                    p.partition,
                    KafkaErrorCode::InvalidTimestamp.as_i16(),
                    ts,
                    -1,
                    current_epoch,
                );
                continue;
            }

            // No remote/tiered storage: tiered specials return empty (-1/-1).
            if ts == LATEST_TIERED || ts == EARLIEST_PENDING_UPLOAD {
                write_part(
                    out,
                    version,
                    flex,
                    p.partition,
                    KafkaErrorCode::None.as_i16(),
                    -1,
                    -1,
                    current_epoch.max(0),
                );
                continue;
            }

            // MAX_TIMESTAMP: scan for the record with the largest timestamp.
            if ts == MAX_TIMESTAMP {
                match broker.max_timestamp_offset(&t.name, p.partition as u32) {
                    Ok(Some((off, max_ts))) => {
                        write_part(
                            out,
                            version,
                            flex,
                            p.partition,
                            KafkaErrorCode::None.as_i16(),
                            max_ts, // actual max timestamp, not the -3 sentinel
                            off as i64,
                            current_epoch.max(0),
                        );
                    }
                    Ok(None) => {
                        // Empty partition.
                        write_part(
                            out,
                            version,
                            flex,
                            p.partition,
                            KafkaErrorCode::None.as_i16(),
                            -1,
                            -1,
                            current_epoch.max(0),
                        );
                    }
                    Err(Error::NotFound(_)) => {
                        write_part(
                            out,
                            version,
                            flex,
                            p.partition,
                            KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
                            ts,
                            -1,
                            -1,
                        );
                    }
                    Err(_) => {
                        write_part(
                            out,
                            version,
                            flex,
                            p.partition,
                            KafkaErrorCode::Unknown.as_i16(),
                            ts,
                            -1,
                            -1,
                        );
                    }
                }
                continue;
            }

            // EARLIEST / EARLIEST_LOCAL / LATEST via list_offsets.
            // Phase 86: READ_COMMITTED latest = LSO (may be < HWM during open txn).
            let want_earliest = ts == EARLIEST || ts == EARLIEST_LOCAL;
            match broker.list_offsets(&t.name, &[p.partition as u32]) {
                Ok(entries) => {
                    let (earliest, latest) = entries
                        .first()
                        .map(|(_, e, l)| (*e as i64, *l as i64))
                        .unwrap_or((0, 0));
                    let offset = if want_earliest {
                        earliest
                    } else if list_read_committed {
                        broker.last_stable_offset(t.name.as_str(), p.partition as u32) as i64
                    } else {
                        latest
                    };
                    write_part(
                        out,
                        version,
                        flex,
                        p.partition,
                        KafkaErrorCode::None.as_i16(),
                        ts,
                        offset,
                        current_epoch.max(0),
                    );
                }
                Err(Error::NotFound(_)) => {
                    write_part(
                        out,
                        version,
                        flex,
                        p.partition,
                        KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
                        ts,
                        -1,
                        -1,
                    );
                }
                Err(_) => {
                    write_part(
                        out,
                        version,
                        flex,
                        p.partition,
                        KafkaErrorCode::Unknown.as_i16(),
                        ts,
                        -1,
                        -1,
                    );
                }
            }
        }
        if flex {
            put_empty_tag_buffer(out); // topic tags
        }
    }
    if flex {
        put_empty_tag_buffer(out); // response top-level tags
    }
}
