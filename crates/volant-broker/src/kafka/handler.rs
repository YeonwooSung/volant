//! Kafka connection accept loop and API handlers.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};
use volant_core::{Error, MessageBatch, Offset, PartitionId, Result, TopicName};

use crate::acl::{AclOperation, ResourceType, CLUSTER_RESOURCE};
use crate::broker::{Broker, IdempotentCheck};

use super::codec::{
    decode_consumer_subscription, decode_produce_batches, decode_request_header,
    encode_consumer_assignment, encode_message_set, encode_record_batch, encode_response_frame,
    get_bytes, get_nullable_string, get_string, put_bytes, put_nullable_string, put_response_header,
    put_string, try_decode_request,
};
use super::{
    map_group_error, map_idempotent_error, ApiKey, KafkaErrorCode, KAFKA_ANONYMOUS_PRINCIPAL,
    SUPPORTED_APIS,
};

/// Accept Kafka-protocol connections until the listener fails fatally.
pub async fn serve_kafka_listener(listener: TcpListener, broker: Arc<Broker>) -> Result<()> {
    if let Ok(local) = listener.local_addr() {
        info!(%local, "volant kafka shim listening");
    }
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                broker.metrics().record_connection();
                debug!(%peer, "kafka connection accepted");
                let b = Arc::clone(&broker);
                tokio::spawn(async move {
                    if let Err(e) = handle_kafka_connection(stream, b).await {
                        debug!(%peer, error = %e, "kafka connection closed");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "kafka accept failed");
                return Err(Error::Io(e));
            }
        }
    }
}

async fn handle_kafka_connection(mut stream: TcpStream, broker: Arc<Broker>) -> Result<()> {
    let mut buf = BytesMut::with_capacity(8 * 1024);
    loop {
        loop {
            match try_decode_request(&mut buf)? {
                Some(body) => {
                    let response = dispatch_kafka(&broker, body);
                    let frame = encode_response_frame(&response);
                    stream.write_all(&frame).await?;
                }
                None => break,
            }
        }
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
    }
}

fn dispatch_kafka(broker: &Broker, body: bytes::Bytes) -> BytesMut {
    let mut src = body;
    let hdr = match decode_request_header(&mut src) {
        Ok(h) => h,
        Err(e) => {
            debug!(error = %e, "kafka header decode failed");
            // Cannot respond without correlation_id.
            return BytesMut::new();
        }
    };
    let corr = hdr.correlation_id;
    let mut out = BytesMut::new();
    put_response_header(&mut out, corr);

    match ApiKey::from_i16(hdr.api_key) {
        Some(ApiKey::ApiVersions) if hdr.api_version == 0 => {
            encode_api_versions(&mut out);
        }
        Some(ApiKey::Metadata) if (0..=1).contains(&hdr.api_version) => {
            encode_metadata(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::Produce) if (0..=3).contains(&hdr.api_version) => {
            encode_produce(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::Fetch) if (0..=4).contains(&hdr.api_version) => {
            encode_fetch(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::ListOffsets) if (0..=1).contains(&hdr.api_version) => {
            encode_list_offsets(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::CreateTopics) if (0..=1).contains(&hdr.api_version) => {
            encode_create_topics(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::DeleteTopics) if (0..=1).contains(&hdr.api_version) => {
            encode_delete_topics(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::FindCoordinator) if hdr.api_version == 0 => {
            encode_find_coordinator(broker, &mut src, &mut out);
        }
        Some(ApiKey::JoinGroup) if (0..=1).contains(&hdr.api_version) => {
            encode_join_group(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::SyncGroup) if hdr.api_version == 0 => {
            encode_sync_group(broker, &mut src, &mut out);
        }
        Some(ApiKey::Heartbeat) if hdr.api_version == 0 => {
            encode_heartbeat(broker, &mut src, &mut out);
        }
        Some(ApiKey::LeaveGroup) if hdr.api_version == 0 => {
            encode_leave_group(broker, &mut src, &mut out);
        }
        Some(ApiKey::OffsetCommit) if (0..=2).contains(&hdr.api_version) => {
            encode_offset_commit(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::OffsetFetch) if (0..=1).contains(&hdr.api_version) => {
            encode_offset_fetch(broker, &mut src, &mut out);
        }
        Some(ApiKey::DescribeGroups) if hdr.api_version == 0 => {
            encode_describe_groups(broker, &mut src, &mut out);
        }
        Some(ApiKey::ListGroups) if hdr.api_version == 0 => {
            encode_list_groups(broker, &mut out);
        }
        Some(ApiKey::DeleteGroups) if hdr.api_version == 0 => {
            encode_delete_groups(broker, &mut src, &mut out);
        }
        Some(ApiKey::CreatePartitions) if hdr.api_version == 0 => {
            encode_create_partitions(broker, &mut src, &mut out);
        }
        Some(ApiKey::DescribeConfigs) if hdr.api_version == 0 => {
            encode_describe_configs(broker, &mut src, &mut out);
        }
        Some(ApiKey::AlterConfigs) if hdr.api_version == 0 => {
            encode_alter_configs(broker, &mut src, &mut out);
        }
        Some(ApiKey::InitProducerId) if (0..=1).contains(&hdr.api_version) => {
            encode_init_producer_id(broker, &mut src, &mut out);
        }
        Some(_) => {
            // Supported API but wrong version — use a generic error body when possible.
            // ApiVersions clients probe versions; return UnsupportedVersion-shaped empty.
            out.put_i16(KafkaErrorCode::UnsupportedVersion.as_i16());
        }
        None => {
            out.put_i16(KafkaErrorCode::UnsupportedVersion.as_i16());
        }
    }
    out
}

fn encode_api_versions(out: &mut BytesMut) {
    out.put_i16(KafkaErrorCode::None.as_i16());
    out.put_i32(SUPPORTED_APIS.len() as i32);
    for (key, min_v, max_v) in SUPPORTED_APIS {
        out.put_i16(*key as i16);
        out.put_i16(*min_v);
        out.put_i16(*max_v);
    }
}

fn encode_metadata(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // Authorization: Cluster Describe when listing all; Topic Describe per topic.
    if broker.acls().is_enabled() {
        // We'll check per-topic below; for empty list need cluster describe.
    }

    let topic_count = if src.remaining() >= 4 {
        src.get_i32()
    } else {
        0
    };
    let mut requested: Vec<String> = Vec::new();
    if topic_count > 0 {
        for _ in 0..topic_count {
            match get_string(src) {
                Ok(t) => requested.push(t),
                Err(_) => break,
            }
        }
    }

    if broker.acls().is_enabled() {
        if requested.is_empty() {
            if !broker.acls().authorize(
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Cluster,
                CLUSTER_RESOURCE,
                AclOperation::Describe,
            ) {
                // Return empty with error on a synthetic topic — Metadata v0 has no top-level error.
                // Emit empty brokers + empty topics.
                out.put_i32(0); // brokers
                if version >= 1 {
                    out.put_i32(-1); // controller_id
                }
                out.put_i32(0); // topics
                return;
            }
        }
    }

    let filter: Option<Vec<TopicName>> = if requested.is_empty() {
        None
    } else {
        Some(requested.iter().map(|t| TopicName::new(t.clone())).collect())
    };
    let snap = match &filter {
        None => broker.metadata(None),
        Some(ts) => broker.metadata(Some(ts.as_slice())),
    };

    // Brokers
    out.put_i32(snap.brokers.len() as i32);
    for (id, host, port) in &snap.brokers {
        out.put_i32(*id as i32);
        put_string(out, host);
        out.put_i32(i32::from(*port));
    }
    if version >= 1 {
        out.put_i32(snap.controller_id as i32);
    }

    // Topics
    let topics: Vec<_> = snap
        .topics
        .into_iter()
        .filter(|t| {
            if !broker.acls().is_enabled() {
                return true;
            }
            broker.acls().authorize(
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Topic,
                t.name.as_str(),
                AclOperation::Describe,
            )
        })
        .collect();
    out.put_i32(topics.len() as i32);
    for t in topics {
        out.put_i16(KafkaErrorCode::None.as_i16());
        put_string(out, t.name.as_str());
        if version >= 1 {
            out.put_u8(0); // is_internal = false
        }
        out.put_i32(t.partitions.len() as i32);
        for p in &t.partitions {
            out.put_i16(KafkaErrorCode::None.as_i16());
            out.put_i32(p.partition_id.0 as i32);
            out.put_i32(p.leader as i32);
            out.put_i32(p.replicas.len() as i32);
            for r in &p.replicas {
                out.put_i32(*r as i32);
            }
            out.put_i32(p.isr.len() as i32);
            for r in &p.isr {
                out.put_i32(*r as i32);
            }
        }
    }
}

fn encode_produce(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // Produce v3+ prepends transactional_id (nullable string).
    if version >= 3 {
        let _txn_id = match get_nullable_string(src) {
            Ok(v) => v,
            Err(_) => {
                out.put_i32(0);
                return;
            }
        };
    }

    if src.remaining() < 2 + 4 {
        out.put_i32(0); // topic responses empty
        return;
    }
    let acks = src.get_i16();
    let _timeout_ms = src.get_i32();
    let volant_acks: u8 = match acks {
        -1 => 255,
        0 => 0,
        _ => 1,
    };

    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let topic_count = src.get_i32();
    out.put_i32(topic_count.max(0));

    for _ in 0..topic_count.max(0) {
        let topic = match get_string(src) {
            Ok(t) => t,
            Err(_) => break,
        };
        put_string(out, &topic);

        if src.remaining() < 4 {
            out.put_i32(0);
            break;
        }
        let part_count = src.get_i32();
        out.put_i32(part_count.max(0));

        for _ in 0..part_count.max(0) {
            if src.remaining() < 4 {
                break;
            }
            let partition = src.get_i32();
            let record_set = match get_bytes(src) {
                Ok(b) => b.unwrap_or_default(),
                Err(_) => {
                    out.put_i32(partition);
                    out.put_i16(KafkaErrorCode::InvalidMessage.as_i16());
                    out.put_i64(-1);
                    if version >= 2 {
                        out.put_i64(-1); // log_append_time
                    }
                    continue;
                }
            };

            out.put_i32(partition);

            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(KAFKA_ANONYMOUS_PRINCIPAL),
                    ResourceType::Topic,
                    &topic,
                    AclOperation::Write,
                )
            {
                out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
                out.put_i64(-1);
                if version >= 2 {
                    out.put_i64(-1);
                }
                continue;
            }

            let batches = match decode_produce_batches(&record_set) {
                Ok(b) => b,
                Err(e) => {
                    debug!(error = %e, "kafka produce records decode failed");
                    out.put_i16(KafkaErrorCode::CorruptMessage.as_i16());
                    out.put_i64(-1);
                    if version >= 2 {
                        out.put_i64(-1);
                    }
                    continue;
                }
            };
            if batches.is_empty() || batches.iter().all(|b| b.messages.is_empty()) {
                out.put_i16(KafkaErrorCode::None.as_i16());
                out.put_i64(0);
                if version >= 2 {
                    out.put_i64(-1);
                }
                continue;
            }

            let name = TopicName::new(topic.clone());
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
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    out.put_i64(base);
                    if version >= 2 {
                        out.put_i64(-1); // log_append_time unused
                    }
                }
                Err(code) => {
                    out.put_i16(code);
                    out.put_i64(-1);
                    if version >= 2 {
                        out.put_i64(-1);
                    }
                }
            }
        }
    }

    // Produce v1+ appends throttle_time_ms at the end.
    if version >= 1 {
        out.put_i32(0);
    }
}

/// Produce one or more decoded batches for a single partition (Phase 29 idempotent).
///
/// Returns the base offset of the first successful batch on success.
fn produce_partition_batches(
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

fn map_produce_ack_error(err: u16) -> i16 {
    if err == volant_protocol::ErrorCode::NotLeaderForPartition as u16 {
        KafkaErrorCode::NotLeaderForPartition.as_i16()
    } else {
        KafkaErrorCode::Unknown.as_i16()
    }
}

/// InitProducerId (API key 22) — Phase 29.
fn encode_init_producer_id(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut) {
    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(KAFKA_ANONYMOUS_PRINCIPAL),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Write,
        )
    {
        out.put_i32(0); // throttle
        out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        out.put_i64(-1);
        out.put_i16(-1);
        return;
    }

    let txn_id = match get_nullable_string(src) {
        Ok(v) => v.unwrap_or_default(),
        Err(_) => {
            out.put_i32(0);
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            out.put_i64(-1);
            out.put_i16(-1);
            return;
        }
    };
    // transaction_timeout_ms — ignored (no Kafka txn coordinator timeout).
    if src.remaining() >= 4 {
        let _timeout = src.get_i32();
    }

    let (pid, epoch) = broker.init_producer_id_with_txn(&txn_id);
    out.put_i32(0); // throttle_time_ms
    out.put_i16(KafkaErrorCode::None.as_i16());
    out.put_i64(pid as i64);
    out.put_i16(epoch as i16);
}

fn encode_fetch(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // FetchRequest v0–2: replica_id, max_wait, min_bytes, [topic ...]
    // v3: + max_bytes
    // v4: + isolation_level
    if src.remaining() < 4 + 4 + 4 {
        if version >= 1 {
            out.put_i32(0); // throttle
        }
        out.put_i32(0);
        return;
    }
    let _replica_id = src.get_i32();
    let _max_wait = src.get_i32();
    let _min_bytes = src.get_i32();
    if version >= 3 {
        if src.remaining() < 4 {
            if version >= 1 {
                out.put_i32(0);
            }
            out.put_i32(0);
            return;
        }
        let _max_bytes = src.get_i32();
    }
    if version >= 4 {
        if src.remaining() < 1 {
            out.put_i32(0); // throttle
            out.put_i32(0);
            return;
        }
        let _isolation = src.get_u8();
    }

    // Fetch response v1+ starts with throttle_time_ms.
    if version >= 1 {
        out.put_i32(0);
    }

    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let topic_count = src.get_i32();
    out.put_i32(topic_count.max(0));

    for _ in 0..topic_count.max(0) {
        let topic = match get_string(src) {
            Ok(t) => t,
            Err(_) => break,
        };
        put_string(out, &topic);

        if src.remaining() < 4 {
            out.put_i32(0);
            break;
        }
        let part_count = src.get_i32();
        out.put_i32(part_count.max(0));

        for _ in 0..part_count.max(0) {
            if src.remaining() < 4 + 8 + 4 {
                break;
            }
            let partition = src.get_i32();
            let fetch_offset = src.get_i64();
            let max_bytes = src.get_i32().max(0) as usize;

            out.put_i32(partition);

            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(KAFKA_ANONYMOUS_PRINCIPAL),
                    ResourceType::Topic,
                    &topic,
                    AclOperation::Read,
                )
            {
                out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
                out.put_i64(-1); // high_watermark
                if version >= 4 {
                    out.put_i64(-1); // last_stable_offset
                    out.put_i32(0); // aborted_transactions
                }
                put_bytes(out, Some(&[]));
                continue;
            }

            let name = TopicName::new(topic.clone());
            // Estimate max messages from max_bytes (rough).
            let max_messages = (max_bytes / 64).clamp(1, 10_000);
            match broker.fetch(
                &name,
                PartitionId(partition as u32),
                Offset::new(fetch_offset.max(0) as u64),
                max_messages,
            ) {
                Ok(records) => {
                    // Trim to max_bytes approximately.
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
                    let hwm = selected
                        .last()
                        .map(|r| r.offset.raw() as i64 + 1)
                        .unwrap_or(fetch_offset);
                    // Prefer broker HWM if available via metadata.
                    let hwm = broker
                        .metadata(Some(&[name.clone()]))
                        .topics
                        .first()
                        .and_then(|t| {
                            t.partitions
                                .iter()
                                .find(|p| p.partition_id.0 == partition as u32)
                                .map(|p| p.hwm as i64)
                        })
                        .unwrap_or(hwm);

                    let set = if version >= 4 {
                        encode_record_batch(&selected)
                    } else {
                        encode_message_set(&selected)
                    };
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    out.put_i64(hwm);
                    if version >= 4 {
                        out.put_i64(hwm); // last_stable_offset ≈ hwm (no txns)
                        out.put_i32(0); // aborted_transactions empty
                    }
                    put_bytes(out, Some(&set));
                }
                Err(Error::NotFound(_)) => {
                    out.put_i16(KafkaErrorCode::UnknownTopicOrPartition.as_i16());
                    out.put_i64(-1);
                    if version >= 4 {
                        out.put_i64(-1);
                        out.put_i32(0);
                    }
                    put_bytes(out, Some(&[]));
                }
                Err(_) => {
                    out.put_i16(KafkaErrorCode::Unknown.as_i16());
                    out.put_i64(-1);
                    if version >= 4 {
                        out.put_i64(-1);
                        out.put_i32(0);
                    }
                    put_bytes(out, Some(&[]));
                }
            }
        }
    }
}

fn encode_list_offsets(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // ListOffsets v0: replica_id, [topic [partition, timestamp, max_num_offsets]]
    // ListOffsets v1: replica_id, [topic [partition, timestamp]]
    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let _replica_id = src.get_i32();
    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let topic_count = src.get_i32();
    out.put_i32(topic_count.max(0));

    for _ in 0..topic_count.max(0) {
        let topic = match get_string(src) {
            Ok(t) => t,
            Err(_) => break,
        };
        put_string(out, &topic);

        if src.remaining() < 4 {
            out.put_i32(0);
            break;
        }
        let part_count = src.get_i32();
        out.put_i32(part_count.max(0));

        for _ in 0..part_count.max(0) {
            if src.remaining() < 4 + 8 {
                break;
            }
            let partition = src.get_i32();
            let timestamp = src.get_i64();
            if version == 0 {
                if src.remaining() < 4 {
                    break;
                }
                let _max_num = src.get_i32();
            }

            out.put_i32(partition);

            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(KAFKA_ANONYMOUS_PRINCIPAL),
                    ResourceType::Topic,
                    &topic,
                    AclOperation::Describe,
                )
            {
                out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
                if version == 0 {
                    out.put_i32(0); // empty offset array
                } else {
                    out.put_i64(timestamp);
                    out.put_i64(-1);
                }
                continue;
            }

            // Kafka: -1 = latest, -2 = earliest.
            let want_earliest = timestamp == -2;
            let want_latest = timestamp == -1;
            if !want_earliest && !want_latest {
                out.put_i16(KafkaErrorCode::InvalidTimestamp.as_i16());
                if version == 0 {
                    out.put_i32(0);
                } else {
                    out.put_i64(timestamp);
                    out.put_i64(-1);
                }
                continue;
            }

            match broker.list_offsets(&topic, &[partition as u32]) {
                Ok(entries) => {
                    let (earliest, latest) = entries
                        .first()
                        .map(|(_, e, l)| (*e as i64, *l as i64))
                        .unwrap_or((0, 0));
                    let offset = if want_earliest { earliest } else { latest };
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    if version == 0 {
                        out.put_i32(1);
                        out.put_i64(timestamp);
                        out.put_i64(offset);
                    } else {
                        out.put_i64(timestamp);
                        out.put_i64(offset);
                    }
                }
                Err(Error::NotFound(_)) => {
                    out.put_i16(KafkaErrorCode::UnknownTopicOrPartition.as_i16());
                    if version == 0 {
                        out.put_i32(0);
                    } else {
                        out.put_i64(timestamp);
                        out.put_i64(-1);
                    }
                }
                Err(_) => {
                    out.put_i16(KafkaErrorCode::Unknown.as_i16());
                    if version == 0 {
                        out.put_i32(0);
                    } else {
                        out.put_i64(timestamp);
                        out.put_i64(-1);
                    }
                }
            }
        }
    }
}

fn encode_create_topics(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // CreateTopics v0: [topic_data]
    // CreateTopics v1: [topic_data] timeout_ms
    // topic_data: name, num_partitions, replication_factor,
    //   [assigned_partition → [broker]], [config_key → config_value]
    if src.remaining() < 4 {
        out.put_i32(0);
        if version >= 1 {
            out.put_i32(0);
        }
        return;
    }
    let topic_count = src.get_i32();
    // Collect first so we can still parse timeout if present.
    struct TopicReq {
        name: String,
        partitions: i32,
        configs: Vec<(String, String)>,
    }
    let mut reqs = Vec::new();
    for _ in 0..topic_count.max(0) {
        let name = match get_string(src) {
            Ok(n) => n,
            Err(_) => break,
        };
        if src.remaining() < 4 + 2 {
            break;
        }
        let partitions = src.get_i32();
        let _rf = src.get_i16();
        // replica assignments
        if src.remaining() < 4 {
            break;
        }
        let assign_count = src.get_i32();
        for _ in 0..assign_count.max(0) {
            if src.remaining() < 4 + 4 {
                break;
            }
            let _part = src.get_i32();
            let broker_count = src.get_i32();
            for _ in 0..broker_count.max(0) {
                if src.remaining() < 4 {
                    break;
                }
                let _ = src.get_i32();
            }
        }
        // configs
        let mut configs = Vec::new();
        if src.remaining() < 4 {
            reqs.push(TopicReq {
                name,
                partitions,
                configs,
            });
            break;
        }
        let cfg_count = src.get_i32();
        for _ in 0..cfg_count.max(0) {
            let k = match get_string(src) {
                Ok(s) => s,
                Err(_) => break,
            };
            let v = match get_string(src) {
                Ok(s) => s,
                Err(_) => break,
            };
            configs.push((k, v));
        }
        reqs.push(TopicReq {
            name,
            partitions,
            configs,
        });
    }
    if version >= 1 && src.remaining() >= 4 {
        let _timeout = src.get_i32();
    }

    out.put_i32(reqs.len() as i32);
    for t in reqs {
        put_string(out, &t.name);

        if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Cluster,
                CLUSTER_RESOURCE,
                AclOperation::Create,
            )
            && !broker.acls().authorize(
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Topic,
                &t.name,
                AclOperation::Create,
            )
        {
            out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
            continue;
        }

        if t.partitions <= 0 {
            out.put_i16(KafkaErrorCode::InvalidPartitions.as_i16());
            continue;
        }

        let result = if t.configs.is_empty() {
            broker.create_topic(t.name.as_str(), t.partitions as u32)
        } else {
            broker.create_topic_with_configs(t.name.as_str(), t.partitions as u32, &t.configs)
        };

        match result {
            Ok(_) => out.put_i16(KafkaErrorCode::None.as_i16()),
            Err(Error::InvalidArgument(msg)) if msg.contains("already exists") => {
                out.put_i16(KafkaErrorCode::TopicAlreadyExists.as_i16());
            }
            Err(Error::InvalidArgument(_)) => {
                out.put_i16(KafkaErrorCode::InvalidTopicException.as_i16());
            }
            Err(_) => out.put_i16(KafkaErrorCode::Unknown.as_i16()),
        }
    }
    if version >= 1 {
        out.put_i32(0); // throttle_time_ms
    }
}

fn encode_delete_topics(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // DeleteTopics v0/v1: [topic names] timeout_ms
    if src.remaining() < 4 {
        out.put_i32(0);
        if version >= 1 {
            out.put_i32(0);
        }
        return;
    }
    let topic_count = src.get_i32();
    let mut names = Vec::new();
    for _ in 0..topic_count.max(0) {
        match get_string(src) {
            Ok(n) => names.push(n),
            Err(_) => break,
        }
    }
    if src.remaining() >= 4 {
        let _timeout = src.get_i32();
    }

    out.put_i32(names.len() as i32);
    for name in names {
        put_string(out, &name);
        if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Topic,
                &name,
                AclOperation::Delete,
            )
        {
            out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
            continue;
        }
        match broker.delete_topic(&TopicName::new(name.clone())) {
            Ok(()) => out.put_i16(KafkaErrorCode::None.as_i16()),
            Err(Error::NotFound(_)) => {
                out.put_i16(KafkaErrorCode::UnknownTopicOrPartition.as_i16());
            }
            Err(_) => out.put_i16(KafkaErrorCode::Unknown.as_i16()),
        }
    }
    if version >= 1 {
        out.put_i32(0);
    }
}

fn encode_find_coordinator(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut) {
    // FindCoordinator v0: group_id string
    let _group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            out.put_i32(-1);
            put_string(out, "");
            out.put_i32(-1);
            return;
        }
    };
    let snap = broker.metadata(None);
    let (id, host, port) = snap
        .brokers
        .first()
        .cloned()
        .unwrap_or((snap.node_id, snap.host.clone(), snap.port));
    out.put_i16(KafkaErrorCode::None.as_i16());
    out.put_i32(id as i32);
    put_string(out, &host);
    out.put_i32(i32::from(port));
}

fn encode_join_group(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // JoinGroup v0: group_id, session_timeout, member_id, protocol_type, [protocols]
    // JoinGroup v1: + rebalance_timeout after session_timeout
    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    if src.remaining() < 4 {
        out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
        return;
    }
    let session_timeout = src.get_i32().max(0) as u32;
    if version >= 1 {
        if src.remaining() < 4 {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
        let _rebalance_timeout = src.get_i32();
    }
    let member_id = match get_string(src) {
        Ok(m) => m,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    let _protocol_type = match get_string(src) {
        Ok(p) => p,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(KAFKA_ANONYMOUS_PRINCIPAL),
            ResourceType::Group,
            &group_id,
            AclOperation::Read,
        )
    {
        out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
        out.put_i32(0);
        put_string(out, "");
        put_string(out, "");
        put_string(out, "");
        out.put_i32(0);
        return;
    }

    if src.remaining() < 4 {
        out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
        return;
    }
    let protocol_count = src.get_i32();
    let mut selected_protocol = String::from("range");
    let mut topics: Vec<String> = Vec::new();
    for i in 0..protocol_count.max(0) {
        let name = match get_string(src) {
            Ok(n) => n,
            Err(_) => break,
        };
        let meta = match get_bytes(src) {
            Ok(b) => b.unwrap_or_default(),
            Err(_) => break,
        };
        if i == 0 {
            selected_protocol = name;
            if let Ok(t) = decode_consumer_subscription(&meta) {
                topics = t;
            }
        }
    }

    let result = match broker.groups().join(
        &group_id,
        &member_id,
        session_timeout,
        topics,
        "",
        |t| broker.partition_count_opt(t),
    ) {
        Ok(r) => r,
        Err(_) => {
            out.put_i16(KafkaErrorCode::Unknown.as_i16());
            out.put_i32(0);
            put_string(out, &selected_protocol);
            put_string(out, "");
            put_string(out, "");
            out.put_i32(0);
            return;
        }
    };

    if result.error_code != 0 {
        out.put_i16(map_group_error(result.error_code));
        out.put_i32(result.generation as i32);
        put_string(out, &selected_protocol);
        put_string(out, "");
        put_string(out, &result.member_id);
        out.put_i32(0);
        return;
    }

    // Leader = lexicographically smallest live member id.
    let members_snap = broker
        .groups()
        .describe_group(&group_id)
        .map(|d| d.members)
        .unwrap_or_default();
    let leader = members_snap
        .iter()
        .map(|m| m.member_id.as_str())
        .min()
        .unwrap_or(result.member_id.as_str())
        .to_owned();

    out.put_i16(KafkaErrorCode::None.as_i16());
    out.put_i32(result.generation as i32);
    put_string(out, &selected_protocol);
    put_string(out, &leader);
    put_string(out, &result.member_id);
    if result.member_id == leader {
        out.put_i32(members_snap.len() as i32);
        for m in &members_snap {
            put_string(out, &m.member_id);
            put_bytes(out, Some(&[]));
        }
    } else {
        out.put_i32(0);
    }
}

fn encode_sync_group(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut) {
    // SyncGroup v0: group_id, generation, member_id, [member_id assignment_bytes]
    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            put_bytes(out, Some(&[]));
            return;
        }
    };
    if src.remaining() < 4 {
        out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
        put_bytes(out, Some(&[]));
        return;
    }
    let generation = src.get_i32() as u32;
    let member_id = match get_string(src) {
        Ok(m) => m,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            put_bytes(out, Some(&[]));
            return;
        }
    };
    // Consume leader assignments (ignored — coordinator already assigned).
    if src.remaining() >= 4 {
        let n = src.get_i32();
        for _ in 0..n.max(0) {
            let _ = get_string(src);
            let _ = get_bytes(src);
        }
    }

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(KAFKA_ANONYMOUS_PRINCIPAL),
            ResourceType::Group,
            &group_id,
            AclOperation::Read,
        )
    {
        out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
        put_bytes(out, Some(&[]));
        return;
    }

    // Generation check via heartbeat (updates last_heartbeat too).
    let hb = broker.groups().heartbeat(&group_id, &member_id, generation);
    if hb.error_code != 0 {
        out.put_i16(map_group_error(hb.error_code));
        put_bytes(out, Some(&[]));
        return;
    }

    let assignment = broker
        .groups()
        .assignment(&group_id, &member_id)
        .unwrap_or_default();
    let bytes = encode_consumer_assignment(&assignment);
    out.put_i16(KafkaErrorCode::None.as_i16());
    put_bytes(out, Some(&bytes));
}

fn encode_heartbeat(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut) {
    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    if src.remaining() < 4 {
        out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
        return;
    }
    let generation = src.get_i32() as u32;
    let member_id = match get_string(src) {
        Ok(m) => m,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(KAFKA_ANONYMOUS_PRINCIPAL),
            ResourceType::Group,
            &group_id,
            AclOperation::Read,
        )
    {
        out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
        return;
    }

    let result = broker.groups().heartbeat(&group_id, &member_id, generation);
    out.put_i16(map_group_error(result.error_code));
}

fn encode_leave_group(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut) {
    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    let member_id = match get_string(src) {
        Ok(m) => m,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(KAFKA_ANONYMOUS_PRINCIPAL),
            ResourceType::Group,
            &group_id,
            AclOperation::Read,
        )
    {
        out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
        return;
    }

    let result = broker
        .groups()
        .leave(&group_id, &member_id, |t| broker.partition_count_opt(t));
    out.put_i16(map_group_error(result.error_code));
}

fn encode_offset_commit(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // v0: group_id, [topic [partition, offset, metadata]]
    // v1: group_id, generation, member_id, [topic [partition, offset, timestamp, metadata]]
    // v2: group_id, generation, member_id, retention_time, [topic [partition, offset, metadata]]
    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            out.put_i32(0);
            return;
        }
    };

    let mut generation: u32 = 0;
    let mut member_id = String::new();
    if version >= 1 {
        if src.remaining() < 4 {
            out.put_i32(0);
            return;
        }
        generation = src.get_i32() as u32;
        member_id = match get_string(src) {
            Ok(m) => m,
            Err(_) => {
                out.put_i32(0);
                return;
            }
        };
    }
    if version >= 2 {
        if src.remaining() < 8 {
            out.put_i32(0);
            return;
        }
        let _retention = src.get_i64();
    }

    let auth_denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(KAFKA_ANONYMOUS_PRINCIPAL),
            ResourceType::Group,
            &group_id,
            AclOperation::Read,
        );

    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let topic_count = src.get_i32();

    struct TopicReq {
        topic: String,
        partitions: Vec<i32>,
    }
    let mut parsed: Vec<TopicReq> = Vec::new();
    let mut entries: Vec<(String, u32, u64, String)> = Vec::new();

    for _ in 0..topic_count.max(0) {
        let topic = match get_string(src) {
            Ok(t) => t,
            Err(_) => break,
        };
        if src.remaining() < 4 {
            break;
        }
        let part_count = src.get_i32();
        let mut partitions = Vec::new();
        for _ in 0..part_count.max(0) {
            if src.remaining() < 4 + 8 {
                break;
            }
            let partition = src.get_i32();
            let offset = src.get_i64().max(0) as u64;
            if version == 1 {
                if src.remaining() < 8 {
                    break;
                }
                let _ts = src.get_i64();
            }
            let metadata = match get_string(src) {
                Ok(m) => m,
                Err(_) => String::new(),
            };
            entries.push((topic.clone(), partition as u32, offset, metadata));
            partitions.push(partition);
        }
        parsed.push(TopicReq { topic, partitions });
    }

    let kerr = if auth_denied {
        KafkaErrorCode::GroupAuthorizationFailed.as_i16()
    } else {
        match broker
            .groups()
            .commit_offsets(&group_id, &member_id, generation, &entries)
        {
            Ok(r) => map_group_error(r.error_code),
            Err(_) => KafkaErrorCode::Unknown.as_i16(),
        }
    };

    out.put_i32(parsed.len() as i32);
    for t in parsed {
        put_string(out, &t.topic);
        out.put_i32(t.partitions.len() as i32);
        for p in t.partitions {
            out.put_i32(p);
            out.put_i16(kerr);
        }
    }
}

fn encode_offset_fetch(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut) {
    // OffsetFetch v0–1: group_id, [topic [partitions]]
    // Empty topics array means all committed offsets for the group (Kafka v0–1
    // actually uses null topics for all; we treat count 0 as all).
    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            out.put_i32(0);
            return;
        }
    };

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(KAFKA_ANONYMOUS_PRINCIPAL),
            ResourceType::Group,
            &group_id,
            AclOperation::Read,
        )
    {
        out.put_i32(0);
        return;
    }

    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let topic_count = src.get_i32();

    // Build query list.
    let mut query: Vec<(String, u32)> = Vec::new();
    let mut requested: Vec<(String, Vec<i32>)> = Vec::new();
    if topic_count > 0 {
        for _ in 0..topic_count {
            let topic = match get_string(src) {
                Ok(t) => t,
                Err(_) => break,
            };
            if src.remaining() < 4 {
                break;
            }
            let pc = src.get_i32();
            let mut parts = Vec::new();
            for _ in 0..pc.max(0) {
                if src.remaining() < 4 {
                    break;
                }
                let p = src.get_i32();
                parts.push(p);
                query.push((topic.clone(), p as u32));
            }
            requested.push((topic, parts));
        }
    }

    let fetched = match broker.groups().fetch_offsets(&group_id, &query) {
        Ok(r) => r.entries,
        Err(_) => Vec::new(),
    };

    if topic_count <= 0 {
        // All offsets: group by topic.
        use std::collections::BTreeMap;
        let mut by_topic: BTreeMap<String, Vec<(u32, i64, String)>> = BTreeMap::new();
        for e in fetched {
            let off = if e.offset == u64::MAX {
                -1i64
            } else {
                e.offset as i64
            };
            by_topic
                .entry(e.topic)
                .or_default()
                .push((e.partition, off, e.metadata));
        }
        out.put_i32(by_topic.len() as i32);
        for (topic, parts) in by_topic {
            put_string(out, &topic);
            out.put_i32(parts.len() as i32);
            for (p, off, meta) in parts {
                out.put_i32(p as i32);
                out.put_i64(off);
                put_string(out, &meta);
                out.put_i16(KafkaErrorCode::None.as_i16());
            }
        }
        return;
    }

    out.put_i32(requested.len() as i32);
    for (topic, parts) in requested {
        put_string(out, &topic);
        out.put_i32(parts.len() as i32);
        for p in parts {
            let entry = fetched
                .iter()
                .find(|e| e.topic == topic && e.partition == p as u32);
            let (off, meta) = match entry {
                Some(e) if e.offset == u64::MAX => (-1i64, e.metadata.clone()),
                Some(e) => (e.offset as i64, e.metadata.clone()),
                None => (-1i64, String::new()),
            };
            out.put_i32(p);
            out.put_i64(off);
            put_string(out, &meta);
            out.put_i16(KafkaErrorCode::None.as_i16());
        }
    }
}

fn encode_list_groups(broker: &Broker, out: &mut BytesMut) {
    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(KAFKA_ANONYMOUS_PRINCIPAL),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        )
    {
        out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        out.put_i32(0);
        return;
    }
    let groups = broker.groups().list_groups();
    out.put_i16(KafkaErrorCode::None.as_i16());
    out.put_i32(groups.len() as i32);
    for g in groups {
        put_string(out, &g.group_id);
        put_string(out, "consumer"); // protocol_type
    }
}

fn encode_describe_groups(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut) {
    // DescribeGroups v0: [group_id]
    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let n = src.get_i32();
    let mut ids = Vec::new();
    for _ in 0..n.max(0) {
        match get_string(src) {
            Ok(g) => ids.push(g),
            Err(_) => break,
        }
    }
    out.put_i32(ids.len() as i32);
    for group_id in ids {
        if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Group,
                &group_id,
                AclOperation::Describe,
            )
        {
            out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
            put_string(out, &group_id);
            put_string(out, ""); // state
            put_string(out, ""); // protocol_type
            put_string(out, ""); // protocol
            out.put_i32(0); // members
            continue;
        }

        match broker.groups().describe_group(&group_id) {
            Some(desc) => {
                out.put_i16(KafkaErrorCode::None.as_i16());
                put_string(out, &group_id);
                put_string(out, "Stable");
                put_string(out, "consumer");
                put_string(out, "range");
                out.put_i32(desc.members.len() as i32);
                for m in &desc.members {
                    put_string(out, &m.member_id);
                    put_string(out, "volant-kafka"); // client_id
                    put_string(out, "/"); // client_host
                    // member metadata: consumer subscription of topics
                    let topics: Vec<&str> = m.topics.iter().map(|s| s.as_str()).collect();
                    let meta = super::codec::encode_consumer_subscription(&topics);
                    put_bytes(out, Some(&meta));
                    let asg = encode_consumer_assignment(&m.assignment);
                    put_bytes(out, Some(&asg));
                }
            }
            None => {
                // Empty or unknown — check if offsets exist.
                let known = broker
                    .groups()
                    .list_group_ids()
                    .iter()
                    .any(|g| g == &group_id);
                if known {
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    put_string(out, &group_id);
                    put_string(out, "Empty");
                    put_string(out, "consumer");
                    put_string(out, "");
                    out.put_i32(0);
                } else {
                    out.put_i16(KafkaErrorCode::GroupIdNotFound.as_i16());
                    put_string(out, &group_id);
                    put_string(out, "Dead");
                    put_string(out, "");
                    put_string(out, "");
                    out.put_i32(0);
                }
            }
        }
    }
}

fn encode_delete_groups(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut) {
    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let n = src.get_i32();
    let mut ids = Vec::new();
    for _ in 0..n.max(0) {
        match get_string(src) {
            Ok(g) => ids.push(g),
            Err(_) => break,
        }
    }
    out.put_i32(ids.len() as i32);
    for group_id in ids {
        put_string(out, &group_id);
        if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Group,
                &group_id,
                AclOperation::Delete,
            )
        {
            out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
            continue;
        }
        match broker.groups().delete_group(&group_id) {
            Ok(0) => out.put_i16(KafkaErrorCode::None.as_i16()),
            Ok(68) => out.put_i16(KafkaErrorCode::NonEmptyGroup.as_i16()),
            Ok(69) => out.put_i16(KafkaErrorCode::GroupIdNotFound.as_i16()),
            Ok(_) => out.put_i16(KafkaErrorCode::Unknown.as_i16()),
            Err(_) => out.put_i16(KafkaErrorCode::Unknown.as_i16()),
        }
    }
}

fn encode_create_partitions(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut) {
    // CreatePartitions v0: [topic, count, [assignment]] timeout
    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let topic_count = src.get_i32();
    struct Req {
        topic: String,
        count: i32,
    }
    let mut reqs = Vec::new();
    for _ in 0..topic_count.max(0) {
        let topic = match get_string(src) {
            Ok(t) => t,
            Err(_) => break,
        };
        if src.remaining() < 4 {
            break;
        }
        let count = src.get_i32();
        // assignments: array of broker id arrays (nullable: -1 length)
        if src.remaining() < 4 {
            break;
        }
        let assign_len = src.get_i32();
        if assign_len >= 0 {
            for _ in 0..assign_len {
                if src.remaining() < 4 {
                    break;
                }
                let brokers = src.get_i32();
                for _ in 0..brokers.max(0) {
                    if src.remaining() < 4 {
                        break;
                    }
                    let _ = src.get_i32();
                }
            }
        }
        reqs.push(Req { topic, count });
    }
    if src.remaining() >= 4 {
        let _timeout = src.get_i32();
    }

    out.put_i32(reqs.len() as i32);
    for r in reqs {
        put_string(out, &r.topic);
        if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Topic,
                &r.topic,
                AclOperation::Alter,
            )
        {
            out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
            put_nullable_string(out, Some("topic authorization failed"));
            continue;
        }
        if r.count <= 0 {
            out.put_i16(KafkaErrorCode::InvalidPartitions.as_i16());
            put_nullable_string(out, Some("invalid partition count"));
            continue;
        }
        match broker.create_partitions(&r.topic, r.count as u32) {
            Ok(_) => {
                out.put_i16(KafkaErrorCode::None.as_i16());
                put_nullable_string(out, None);
            }
            Err(Error::NotFound(_)) => {
                out.put_i16(KafkaErrorCode::UnknownTopicOrPartition.as_i16());
                put_nullable_string(out, Some("topic not found"));
            }
            Err(Error::InvalidArgument(msg)) => {
                out.put_i16(KafkaErrorCode::InvalidPartitions.as_i16());
                put_nullable_string(out, Some(&msg));
            }
            Err(_) => {
                out.put_i16(KafkaErrorCode::Unknown.as_i16());
                put_nullable_string(out, None);
            }
        }
    }
}

fn encode_describe_configs(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut) {
    // DescribeConfigs v0: [resource_type:i8, resource_name, [config_names] | null]
    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let n = src.get_i32();
    struct Res {
        rtype: i8,
        name: String,
        keys: Option<Vec<String>>,
    }
    let mut resources = Vec::new();
    for _ in 0..n.max(0) {
        if src.remaining() < 1 {
            break;
        }
        let rtype = src.get_i8();
        let name = match get_string(src) {
            Ok(s) => s,
            Err(_) => break,
        };
        if src.remaining() < 4 {
            break;
        }
        let key_count = src.get_i32();
        let keys = if key_count < 0 {
            None
        } else {
            let mut ks = Vec::new();
            for _ in 0..key_count {
                match get_string(src) {
                    Ok(k) => ks.push(k),
                    Err(_) => break,
                }
            }
            Some(ks)
        };
        resources.push(Res { rtype, name, keys });
    }

    out.put_i32(resources.len() as i32);
    for r in resources {
        // resource_type 2 = TOPIC
        if r.rtype != 2 {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            out.put_i8(r.rtype);
            put_string(out, &r.name);
            put_nullable_string(out, Some("only TOPIC resources supported"));
            out.put_i32(0);
            continue;
        }
        if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Topic,
                &r.name,
                AclOperation::Describe,
            )
        {
            out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
            out.put_i8(r.rtype);
            put_string(out, &r.name);
            put_nullable_string(out, None);
            out.put_i32(0);
            continue;
        }
        match broker.describe_configs(&r.name) {
            Ok((_id, _pc, cfg)) => {
                let mut entries = cfg.to_entries();
                if let Some(filter) = &r.keys {
                    entries.retain(|(k, _)| filter.iter().any(|f| f == k));
                }
                out.put_i16(KafkaErrorCode::None.as_i16());
                out.put_i8(r.rtype);
                put_string(out, &r.name);
                put_nullable_string(out, None); // error message
                out.put_i32(entries.len() as i32);
                for (k, v) in entries {
                    put_string(out, &k);
                    // config_value nullable
                    if v.is_empty() {
                        put_nullable_string(out, None);
                    } else {
                        put_nullable_string(out, Some(&v));
                    }
                    out.put_u8(0); // read_only
                    out.put_u8(if v.is_empty() { 1 } else { 0 }); // is_default
                    out.put_u8(0); // is_sensitive
                }
            }
            Err(Error::NotFound(_)) => {
                out.put_i16(KafkaErrorCode::UnknownTopicOrPartition.as_i16());
                out.put_i8(r.rtype);
                put_string(out, &r.name);
                put_nullable_string(out, Some("topic not found"));
                out.put_i32(0);
            }
            Err(_) => {
                out.put_i16(KafkaErrorCode::Unknown.as_i16());
                out.put_i8(r.rtype);
                put_string(out, &r.name);
                put_nullable_string(out, None);
                out.put_i32(0);
            }
        }
    }
}

fn encode_alter_configs(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut) {
    // AlterConfigs v0: [resource_type, resource_name, [name, value]] validate_only
    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let n = src.get_i32();
    struct Res {
        rtype: i8,
        name: String,
        entries: Vec<(String, String)>,
    }
    let mut resources = Vec::new();
    for _ in 0..n.max(0) {
        if src.remaining() < 1 {
            break;
        }
        let rtype = src.get_i8();
        let name = match get_string(src) {
            Ok(s) => s,
            Err(_) => break,
        };
        if src.remaining() < 4 {
            break;
        }
        let ec = src.get_i32();
        let mut entries = Vec::new();
        for _ in 0..ec.max(0) {
            let k = match get_string(src) {
                Ok(s) => s,
                Err(_) => break,
            };
            // value is nullable string in some versions; treat as string
            let v = match get_nullable_string(src) {
                Ok(Some(s)) => s,
                Ok(None) => String::new(),
                Err(_) => match get_string(src) {
                    Ok(s) => s,
                    Err(_) => String::new(),
                },
            };
            entries.push((k, v));
        }
        resources.push(Res {
            rtype,
            name,
            entries,
        });
    }
    let validate_only = if src.remaining() >= 1 {
        src.get_u8() != 0
    } else {
        false
    };

    // Response: [error_code, error_message, resource_type, resource_name]
    out.put_i32(resources.len() as i32);
    for r in resources {
        if r.rtype != 2 {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            put_nullable_string(out, Some("only TOPIC resources supported"));
            out.put_i8(r.rtype);
            put_string(out, &r.name);
            continue;
        }
        if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Topic,
                &r.name,
                AclOperation::Alter,
            )
        {
            out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
            put_nullable_string(out, None);
            out.put_i8(r.rtype);
            put_string(out, &r.name);
            continue;
        }
        if validate_only {
            match volant_broker_topic_config_validate(&r.entries) {
                Ok(()) => {
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    put_nullable_string(out, None);
                }
                Err(msg) => {
                    out.put_i16(KafkaErrorCode::InvalidConfig.as_i16());
                    put_nullable_string(out, Some(&msg));
                }
            }
            out.put_i8(r.rtype);
            put_string(out, &r.name);
            continue;
        }
        match broker.alter_configs(&r.name, &r.entries) {
            Ok(_) => {
                out.put_i16(KafkaErrorCode::None.as_i16());
                put_nullable_string(out, None);
            }
            Err(Error::NotFound(_)) => {
                out.put_i16(KafkaErrorCode::UnknownTopicOrPartition.as_i16());
                put_nullable_string(out, Some("topic not found"));
            }
            Err(Error::InvalidArgument(msg)) => {
                out.put_i16(KafkaErrorCode::InvalidConfig.as_i16());
                put_nullable_string(out, Some(&msg));
            }
            Err(_) => {
                out.put_i16(KafkaErrorCode::Unknown.as_i16());
                put_nullable_string(out, None);
            }
        }
        out.put_i8(r.rtype);
        put_string(out, &r.name);
    }
}

fn volant_broker_topic_config_validate(entries: &[(String, String)]) -> std::result::Result<(), String> {
    crate::topic_config::TopicConfig::from_entries(entries).map(|_| ()).map_err(|e| e.to_string())
}
