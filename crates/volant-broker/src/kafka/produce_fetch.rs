//! Kafka wire handlers: Produce, Fetch, ListOffsets, OffsetForLeaderEpoch.

use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tracing::debug;
use volant_core::{Error, MessageBatch, Offset, PartitionId, TopicName};

use crate::acl::{AclOperation, ResourceType};
use crate::broker::{Broker, IdempotentCheck};

use super::codec::{
    decode_produce_batches, encode_message_set, encode_message_set_compressed, encode_record_batch,
    encode_record_batch_compressed, get_bytes, get_compact_array_len, get_compact_bytes,
    get_compact_nullable_string, get_compact_string, get_nullable_string, get_string, get_uuid,
    put_bytes, put_compact_array_len, put_compact_bytes, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_nullable_string, put_string, put_unsigned_varint,
    skip_tag_buffer,
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

/// Produce/Fetch NodeEndpoints entry for one broker.
fn put_node_endpoints_tag(out: &mut BytesMut, endpoints: &[(i32, String, i32)]) {
    let mut value = BytesMut::new();
    put_compact_array_len(&mut value, endpoints.len());
    for (id, host, port) in endpoints {
        value.put_i32(*id);
        put_compact_string(&mut value, host);
        value.put_i32(*port);
        put_compact_nullable_string(&mut value, None); // rack
        put_empty_tag_buffer(&mut value);
    }
    put_single_tag(out, 0, &value);
}

/// Collect unique NodeEndpoints for leaders referenced by CurrentLeader ids.
fn node_endpoints_for_leaders(broker: &Broker, leader_ids: &[i32]) -> Vec<(i32, String, i32)> {
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
        out.push((id, host, port));
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

/// Produce one or more decoded batches for a single partition (Phase 29/31).
///
/// Returns the base offset of the first successful batch on success.
/// Transactional produces buffer off-log until EndTxn (base offset 0).
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

            // Phase 31: transactional PID with open txn → buffer off-log.
            if broker.is_transactional_producer(pid) {
                match broker.buffer_txn_produce(
                    pid,
                    epoch,
                    topic.as_str(),
                    partition,
                    base_seq,
                    batch.messages.clone(),
                ) {
                    IdempotentCheck::Accept | IdempotentCheck::Duplicate { .. } => {
                        if first_base.is_none() {
                            first_base = Some(0);
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
                IdempotentCheck::Accept => {
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

/// Write Fetch response header before topic array (classic v0–11 / flexible v12).
pub(crate) fn put_fetch_response_header(out: &mut BytesMut, version: i16, session_id: i32) {
    // throttle (v1+), top-level error + session_id (v7+)
    if version >= 1 {
        out.put_i32(0);
    }
    if version >= 7 {
        out.put_i16(KafkaErrorCode::None.as_i16());
        out.put_i32(session_id);
    }
}

/// Empty Fetch response (no topics) with correct classic/flexible framing.
pub(crate) fn put_fetch_empty_response(out: &mut BytesMut, version: i16, session_id: i32) {
    let flexible = version >= 12;
    put_fetch_response_header(out, version, session_id);
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
/// v12+: optional CurrentLeader as **tag 1** (tag 0 is DivergingEpoch, unused).
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
    let flexible = version >= 12;
    out.put_i32(partition);
    out.put_i16(error);
    out.put_i64(hwm);
    if version >= 4 {
        // LSO == HWM under buffer-until-commit (Phase 36 honesty).
        out.put_i64(if error == 0 { hwm } else { -1 });
    }
    if version >= 5 {
        out.put_i64(if error == 0 { log_start } else { -1 });
    }
    if version >= 4 {
        if flexible {
            put_compact_array_len(out, 0); // aborted_transactions empty
        } else {
            out.put_i32(0); // aborted_transactions empty
        }
    }
    if version >= 11 {
        out.put_i32(-1); // preferred_read_replica (no rack-aware follower fetch)
    }
    if flexible {
        put_compact_bytes(out, Some(records));
        if let Some((lid, lep)) = current_leader {
            // Tag 1 = CurrentLeader (tag 0 would be DivergingEpoch).
            let body = encode_leader_id_and_epoch(lid, lep);
            put_single_tag(out, 1, &body);
        } else {
            put_empty_tag_buffer(out);
        }
    } else {
        put_bytes(out, Some(records));
    }
}

pub(crate) fn encode_fetch(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
    // Fetch classic v0–11 + flexible v12 + TopicId v13:
    //   request: replica_id, max_wait, min_bytes,
    //            max_bytes (v3+), isolation (v4+),
    //            session_id + session_epoch (v7+),
    //            topics[{ name (≤v12) | TopicId uuid (v13+), partitions[{
    //              partition, current_leader_epoch (v9+), fetch_offset,
    //              last_fetched_epoch (v12+), log_start_offset (v5+),
    //              partition_max_bytes
    //            }]}],
    //            forgotten_topics (v7+; name ≤v12 / TopicId v13+),
    //            rack_id (v11+), tags (v12+)
    //   response: throttle (v1+), error+session_id (v7+),
    //             topics[{ name ≤v12 | TopicId v13+, partitions[{…}]}], tags (v12+)
    // ClusterId (v12+) is a top-level tagged field — ignored via skip_tag_buffer.
    let flexible = version >= 12;
    let use_topic_id = version >= 13;

    if src.remaining() < 4 + 4 + 4 {
        put_fetch_empty_response(out, version, 0);
        return;
    }
    let _replica_id = src.get_i32();
    let _max_wait = src.get_i32();
    let _min_bytes = src.get_i32();
    if version >= 3 {
        if src.remaining() < 4 {
            put_fetch_empty_response(out, version, 0);
            return;
        }
        let _max_bytes = src.get_i32();
    }
    // Phase 36: isolation_level (v4). Both levels share the same path.
    let mut isolation = 0u8;
    if version >= 4 {
        if src.remaining() < 1 {
            put_fetch_empty_response(out, version, 0);
            return;
        }
        isolation = src.get_u8();
        if isolation > 1 {
            put_fetch_empty_response(out, version, 0);
            return;
        }
    }
    let _ = isolation;

    // v7+: incremental fetch session fields (no real session; always full response).
    let mut session_id = 0i32;
    if version >= 7 {
        if src.remaining() < 4 + 4 {
            put_fetch_empty_response(out, version, 0);
            return;
        }
        session_id = src.get_i32();
        let _session_epoch = src.get_i32();
        // Echo non-zero session_id; 0 means "not part of a session".
    }

    let topic_count = if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => n as i32,
            Ok(None) | Err(_) => {
                put_fetch_empty_response(out, version, session_id);
                return;
            }
        }
    } else {
        if src.remaining() < 4 {
            put_fetch_empty_response(out, version, session_id);
            return;
        }
        src.get_i32()
    };

    put_fetch_response_header(out, version, session_id);
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
            // partition + optional current_leader_epoch + fetch_offset
            // + last_fetched_epoch (v12+) + optional log_start_offset + partition_max_bytes
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
            if version >= 12 {
                let _last_fetched_epoch = src.get_i32(); // ignored (no diverging-epoch tags)
            }
            if version >= 5 {
                let _follower_log_start = src.get_i64(); // ignored (consumer path)
            }
            let max_bytes = src.get_i32().max(0) as usize;
            if flexible {
                let _ = skip_tag_buffer(src); // partition request tags
            }

            if unknown_topic_id {
                put_fetch_partition_response(
                    out,
                    version,
                    partition,
                    KafkaErrorCode::UnknownTopicId.as_i16(),
                    -1,
                    -1,
                    &[], None);
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
                put_fetch_partition_response(
                    out,
                    version,
                    partition,
                    KafkaErrorCode::TopicAuthorizationFailed.as_i16(),
                    -1,
                    -1,
                    &[], None);
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
                    put_fetch_partition_response(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::UnknownLeaderEpoch.as_i16(),
                        -1,
                        -1,
                        &[], None);
                    continue;
                }
                if current_epoch >= 0 && current_leader_epoch < current_epoch {
                    let leader = part_meta.map(|p| (p.leader as i32, p.leader_epoch as i32));
                    put_fetch_partition_response(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::FencedLeaderEpoch.as_i16(),
                        -1,
                        -1,
                        &[],
                        leader,
                    );
                    continue;
                }
            }

            let max_messages = (max_bytes / 64).clamp(1, 10_000);
            match broker.fetch(
                &name,
                PartitionId(partition as u32),
                Offset::new(fetch_offset.max(0) as u64),
                max_messages,
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
                    let log_start =
                        produce_log_start_offset(broker, &topic_name, partition as u32);
                    // Phase 32: v4+ RecordBatch (may be compressed); v0–3 MessageSet.
                    let set = encode_fetch_record_set(&selected, version);
                    put_fetch_partition_response(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::None.as_i16(),
                        hwm,
                        log_start,
                        &set,
                        None,
                    );
                }
                Err(Error::NotFound(_)) => {
                    put_fetch_partition_response(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
                        -1,
                        -1,
                        &[],
                        None,
                    );
                }
                Err(_) => {
                    put_fetch_partition_response(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::Unknown.as_i16(),
                        -1,
                        -1,
                        &[], None);
                }
            }
        }
        if flexible {
            put_empty_tag_buffer(out); // topic response tags
            let _ = skip_tag_buffer(src); // topic request tags
        }
    }

    // v7+: forgotten_topics_data (parse and ignore — no incremental sessions).
    if version >= 7 {
        if flexible {
            if let Ok(Some(n)) = get_compact_array_len(src) {
                for _ in 0..n {
                    if use_topic_id {
                        let _ = get_uuid(src);
                    } else {
                        let _ = get_compact_string(src);
                    }
                    if let Ok(Some(pn)) = get_compact_array_len(src) {
                        for _ in 0..pn {
                            if src.remaining() < 4 {
                                break;
                            }
                            let _ = src.get_i32();
                        }
                    }
                    let _ = skip_tag_buffer(src);
                }
            }
        } else if src.remaining() >= 4 {
            let forgotten = src.get_i32();
            for _ in 0..forgotten.max(0) {
                let _ = get_string(src);
                if src.remaining() < 4 {
                    break;
                }
                let n = src.get_i32();
                for _ in 0..n.max(0) {
                    if src.remaining() < 4 {
                        break;
                    }
                    let _ = src.get_i32();
                }
            }
        }
    }
    // v11+: rack_id (ignored — preferred_read_replica always -1).
    if version >= 11 {
        if flexible {
            let _ = get_compact_string(src);
        } else {
            let _ = get_string(src);
        }
    }
    if flexible {
        let _ = skip_tag_buffer(src); // request top-level tags (ClusterId, …)
        put_empty_tag_buffer(out); // response top-level tags
    }
}

/// Encode partition records for a Fetch response.
///
/// Phase 32: v4 RecordBatch compression. Phase 33: v0–3 MessageSet wrapper compression.
pub(crate) fn encode_fetch_record_set(records: &[volant_core::Record], version: i16) -> BytesMut {
    if records.is_empty() {
        return BytesMut::new();
    }
    let codec = fetch_compression_codec();
    if version < 4 {
        // MessageSet path (Phase 33).
        if codec == CompressionCodec::None {
            return encode_message_set(records);
        }
        return match encode_message_set_compressed(records, codec) {
            Ok(set) => set,
            Err(e) => {
                debug!(error = %e, ?codec, "message set fetch compression failed; plain");
                encode_message_set(records)
            }
        };
    }
    // RecordBatch path (Phase 32).
    if codec == CompressionCodec::None {
        return encode_record_batch(records);
    }
    match encode_record_batch_compressed(records, codec) {
        Ok(batch) => batch,
        Err(e) => {
            debug!(error = %e, ?codec, "fetch compression failed; falling back to plain");
            encode_record_batch(records)
        }
    }
}

/// OffsetForLeaderEpoch (API key 23) classic v0–3 / flexible v4 — Phase 39 / 63.
///
/// Without epoch history, any requested epoch ≤ the current partition epoch
/// (or -1 = latest) returns end_offset = HWM and the current leader epoch.
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
            let hwm = pm.hwm as i64;

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

            // No epoch history: any eligible epoch maps to current HWM.
            write_part(out, KafkaErrorCode::None.as_i16(), current_epoch, hwm);
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

    // v2+: isolation_level (0 / 1). Both map to the same offsets under
    // buffer-until-commit (LSO ≡ HWM); accept and ignore.
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
            let want_earliest = ts == EARLIEST || ts == EARLIEST_LOCAL;
            match broker.list_offsets(&t.name, &[p.partition as u32]) {
                Ok(entries) => {
                    let (earliest, latest) = entries
                        .first()
                        .map(|(_, e, l)| (*e as i64, *l as i64))
                        .unwrap_or((0, 0));
                    let offset = if want_earliest { earliest } else { latest };
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
