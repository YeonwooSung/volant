//! Kafka connection accept loop and API handlers.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};
use volant_core::{Error, MessageBatch, Offset, PartitionId, Result, TopicName};

use crate::acl::{
    AclEntry, AclOperation, AclPermission, ResourceType, CLUSTER_RESOURCE,
};
use crate::broker::{Broker, IdempotentCheck};

use super::codec::{
    decode_consumer_subscription, decode_produce_batches, decode_request_header,
    encode_consumer_assignment, encode_message_set, encode_message_set_compressed,
    encode_record_batch, encode_record_batch_compressed, encode_response_frame, get_bytes,
    get_nullable_string, get_string, put_bytes, put_nullable_string, put_response_header,
    put_string, try_decode_request,
};
use super::compress::{fetch_compression_codec, CompressionCodec};
use super::sasl::{self, SaslMechanism, SaslState, MECHANISMS};
use super::{
    map_group_error, map_idempotent_error, ApiKey, KafkaErrorCode, KAFKA_ANONYMOUS_PRINCIPAL,
    SUPPORTED_APIS,
};

/// Per-connection Kafka auth state (Phase 30).
#[derive(Debug, Default)]
struct KafkaConnState {
    /// Authenticated principal (SCRAM username), if any.
    principal: Option<String>,
    /// SASL state machine.
    sasl: SaslState,
}

impl KafkaConnState {
    /// Principal used for ACL checks.
    fn acl_principal(&self) -> &str {
        self.principal
            .as_deref()
            .unwrap_or(KAFKA_ANONYMOUS_PRINCIPAL)
    }

    /// Whether the connection is authenticated for ACL / gate purposes.
    fn authenticated(&self) -> bool {
        self.principal.is_some()
    }
}

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
    let mut conn = KafkaConnState::default();
    loop {
        loop {
            match try_decode_request(&mut buf)? {
                Some(body) => {
                    let response = dispatch_kafka(&broker, body, &mut conn);
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

fn dispatch_kafka(broker: &Broker, body: bytes::Bytes, conn: &mut KafkaConnState) -> BytesMut {
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

    let api = ApiKey::from_i16(hdr.api_key);
    // When SCRAM users exist, require SASL before non-auth APIs (Phase 30).
    let auth_gate = broker.scram().has_users() && !conn.authenticated();
    if auth_gate {
        let allowed = matches!(
            api,
            Some(ApiKey::ApiVersions)
                | Some(ApiKey::SaslHandshake)
                | Some(ApiKey::SaslAuthenticate)
        );
        if !allowed {
            out.put_i16(KafkaErrorCode::SaslAuthenticationFailed.as_i16());
            return out;
        }
    }

    let principal = conn.acl_principal().to_owned();
    let principal = principal.as_str();

    match api {
        Some(ApiKey::ApiVersions) if hdr.api_version == 0 => {
            encode_api_versions(&mut out);
        }
        Some(ApiKey::SaslHandshake) if (0..=1).contains(&hdr.api_version) => {
            encode_sasl_handshake(&mut src, &mut out, conn);
        }
        Some(ApiKey::SaslAuthenticate) if (0..=1).contains(&hdr.api_version) => {
            encode_sasl_authenticate(broker, &mut src, &mut out, hdr.api_version, conn);
        }
        Some(ApiKey::Metadata) if (0..=1).contains(&hdr.api_version) => {
            encode_metadata(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::Produce) if (0..=3).contains(&hdr.api_version) => {
            encode_produce(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::Fetch) if (0..=4).contains(&hdr.api_version) => {
            encode_fetch(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::ListOffsets) if (0..=1).contains(&hdr.api_version) => {
            encode_list_offsets(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::CreateTopics) if (0..=1).contains(&hdr.api_version) => {
            encode_create_topics(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::DeleteTopics) if (0..=1).contains(&hdr.api_version) => {
            encode_delete_topics(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::DeleteRecords) if (0..=1).contains(&hdr.api_version) => {
            encode_delete_records(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::DescribeAcls) if (0..=1).contains(&hdr.api_version) => {
            encode_describe_acls(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::CreateAcls) if (0..=1).contains(&hdr.api_version) => {
            encode_create_acls(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::DeleteAcls) if (0..=1).contains(&hdr.api_version) => {
            encode_delete_acls(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::FindCoordinator) if (0..=1).contains(&hdr.api_version) => {
            encode_find_coordinator(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::AddPartitionsToTxn) if hdr.api_version == 0 => {
            encode_add_partitions_to_txn(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::AddOffsetsToTxn) if hdr.api_version == 0 => {
            encode_add_offsets_to_txn(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::EndTxn) if hdr.api_version == 0 => {
            encode_end_txn(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::TxnOffsetCommit) if hdr.api_version == 0 => {
            encode_txn_offset_commit(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::JoinGroup) if (0..=1).contains(&hdr.api_version) => {
            encode_join_group(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::SyncGroup) if hdr.api_version == 0 => {
            encode_sync_group(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::Heartbeat) if hdr.api_version == 0 => {
            encode_heartbeat(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::LeaveGroup) if hdr.api_version == 0 => {
            encode_leave_group(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::OffsetCommit) if (0..=2).contains(&hdr.api_version) => {
            encode_offset_commit(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::OffsetFetch) if (0..=1).contains(&hdr.api_version) => {
            encode_offset_fetch(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::DescribeGroups) if hdr.api_version == 0 => {
            encode_describe_groups(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::ListGroups) if hdr.api_version == 0 => {
            encode_list_groups(broker, &mut out, principal);
        }
        Some(ApiKey::DeleteGroups) if hdr.api_version == 0 => {
            encode_delete_groups(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::OffsetDelete) if hdr.api_version == 0 => {
            encode_offset_delete(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::CreatePartitions) if hdr.api_version == 0 => {
            encode_create_partitions(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::DescribeConfigs) if hdr.api_version == 0 => {
            encode_describe_configs(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::AlterConfigs) if hdr.api_version == 0 => {
            encode_alter_configs(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::InitProducerId) if (0..=1).contains(&hdr.api_version) => {
            encode_init_producer_id(broker, &mut src, &mut out, principal);
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

fn encode_sasl_handshake(src: &mut impl Buf, out: &mut BytesMut, conn: &mut KafkaConnState) {
    let mechanism = match get_string(src) {
        Ok(m) => m,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            put_mechanisms_list(out);
            return;
        }
    };
    match SaslMechanism::parse(&mechanism) {
        Some(m) => {
            conn.sasl = SaslState::Selected(m);
            out.put_i16(KafkaErrorCode::None.as_i16());
        }
        None => {
            out.put_i16(KafkaErrorCode::UnsupportedSaslMechanism.as_i16());
        }
    }
    put_mechanisms_list(out);
}

fn put_mechanisms_list(out: &mut BytesMut) {
    out.put_i32(MECHANISMS.len() as i32);
    for m in MECHANISMS {
        put_string(out, m);
    }
}

fn encode_sasl_authenticate(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    conn: &mut KafkaConnState,
) {
    let auth_bytes = match get_bytes(src) {
        Ok(b) => b.unwrap_or_default(),
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            put_nullable_string(out, Some("truncated auth bytes"));
            put_bytes(out, None);
            if version >= 1 {
                out.put_i64(0);
            }
            return;
        }
    };

    let step = match sasl::authenticate_step(broker, &mut conn.sasl, &auth_bytes) {
        Ok(s) => s,
        Err(e) => {
            out.put_i16(KafkaErrorCode::SaslAuthenticationFailed.as_i16());
            put_nullable_string(out, Some(&e.to_string()));
            put_bytes(out, None);
            if version >= 1 {
                out.put_i64(0);
            }
            return;
        }
    };

    if step.failed {
        out.put_i16(KafkaErrorCode::SaslAuthenticationFailed.as_i16());
        put_nullable_string(out, step.error_message.as_deref());
        put_bytes(out, Some(&step.auth_bytes));
    } else {
        if let Some(p) = step.principal {
            conn.principal = Some(p);
        }
        out.put_i16(KafkaErrorCode::None.as_i16());
        put_nullable_string(out, None);
        put_bytes(out, Some(&step.auth_bytes));
    }
    if version >= 1 {
        out.put_i64(0); // session_lifetime_ms
    }
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

fn encode_metadata(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
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
                Some(principal),
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
                Some(principal),
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

fn encode_produce(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
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
                    Some(principal),
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

/// Produce one or more decoded batches for a single partition (Phase 29/31).
///
/// Returns the base offset of the first successful batch on success.
/// Transactional produces buffer off-log until EndTxn (base offset 0).
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

fn map_produce_ack_error(err: u16) -> i16 {
    if err == volant_protocol::ErrorCode::NotLeaderForPartition as u16 {
        KafkaErrorCode::NotLeaderForPartition.as_i16()
    } else {
        KafkaErrorCode::Unknown.as_i16()
    }
}

/// InitProducerId (API key 22) — Phase 29.
fn encode_init_producer_id(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, principal: &str) {
    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
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

fn encode_fetch(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
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
    // Phase 36: isolation_level (v4). 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED.
    // Volant buffer-until-commit means both levels see only committed log data;
    // LSO always equals HWM and aborted_transactions is always empty.
    let mut isolation = 0u8;
    if version >= 4 {
        if src.remaining() < 1 {
            out.put_i32(0); // throttle
            out.put_i32(0);
            return;
        }
        isolation = src.get_u8();
        if isolation > 1 {
            // Invalid isolation — empty response with throttle.
            out.put_i32(0);
            out.put_i32(0);
            return;
        }
    }
    let _ = isolation; // both levels share the same encode path (honest).

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
                    Some(principal),
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

                    // Phase 32: Fetch v4 RecordBatches may be compressed.
                    // Phase 36: LSO = HWM for both isolation levels (no unstable log data).
                    let set = encode_fetch_record_set(&selected, version);
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    out.put_i64(hwm);
                    if version >= 4 {
                        out.put_i64(hwm); // last_stable_offset == hwm
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

/// Encode partition records for a Fetch response.
///
/// Phase 32: v4 RecordBatch compression. Phase 33: v0–3 MessageSet wrapper compression.
fn encode_fetch_record_set(records: &[volant_core::Record], version: i16) -> BytesMut {
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

fn encode_list_offsets(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
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
                    Some(principal),
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

fn encode_create_topics(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
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
                Some(principal),
                ResourceType::Cluster,
                CLUSTER_RESOURCE,
                AclOperation::Create,
            )
            && !broker.acls().authorize(
                Some(principal),
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

fn encode_delete_topics(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
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
                Some(principal),
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

fn encode_find_coordinator(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // FindCoordinator v0: key STRING
    // FindCoordinator v1: key STRING + key_type INT8 (0=group, 1=transaction)
    let _key = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            if version >= 1 {
                out.put_i32(0); // throttle
            }
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            if version >= 1 {
                put_nullable_string(out, Some("invalid key"));
            }
            out.put_i32(-1);
            put_string(out, "");
            out.put_i32(-1);
            return;
        }
    };
    if version >= 1 {
        if src.remaining() < 1 {
            out.put_i32(0);
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            put_nullable_string(out, Some("missing key_type"));
            out.put_i32(-1);
            put_string(out, "");
            out.put_i32(-1);
            return;
        }
        let key_type = src.get_i8();
        // 0 = group, 1 = transaction — both resolve to this broker.
        if key_type != 0 && key_type != 1 {
            out.put_i32(0);
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            put_nullable_string(out, Some("unsupported key_type"));
            out.put_i32(-1);
            put_string(out, "");
            out.put_i32(-1);
            return;
        }
    }
    let snap = broker.metadata(None);
    let (id, host, port) = snap
        .brokers
        .first()
        .cloned()
        .unwrap_or((snap.node_id, snap.host.clone(), snap.port));
    if version >= 1 {
        out.put_i32(0); // throttle_time_ms
    }
    out.put_i16(KafkaErrorCode::None.as_i16());
    if version >= 1 {
        put_nullable_string(out, None); // error_message
    }
    out.put_i32(id as i32);
    put_string(out, &host);
    out.put_i32(i32::from(port));
}

/// AddPartitionsToTxn (API 24) v0 — opens a txn if needed (Phase 31).
fn encode_add_partitions_to_txn(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    // Request: transactional_id, producer_id, producer_epoch, [topics → [partitions]]
    let _txn_id = match get_string(src) {
        Ok(t) => t,
        Err(_) => {
            out.put_i32(0);
            out.put_i32(0);
            return;
        }
    };
    if src.remaining() < 8 + 2 + 4 {
        out.put_i32(0);
        out.put_i32(0);
        return;
    }
    let producer_id = src.get_i64() as u64;
    let producer_epoch = src.get_i16() as u16;
    let topic_count = src.get_i32();

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Write,
        )
    {
        out.put_i32(0); // throttle
        out.put_i32(topic_count.max(0));
        // Echo structure with auth errors if we can still parse; otherwise empty.
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
            let n = src.get_i32();
            out.put_i32(n.max(0));
            for _ in 0..n.max(0) {
                if src.remaining() < 4 {
                    break;
                }
                let p = src.get_i32();
                out.put_i32(p);
                out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
            }
        }
        return;
    }

    let open_err = broker.ensure_txn_open(producer_id, producer_epoch);
    let part_err = if open_err == 0 {
        KafkaErrorCode::None.as_i16()
    } else {
        map_idempotent_error(open_err)
    };

    out.put_i32(0); // throttle_time_ms
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
            out.put_i32(partition);
            if open_err == 0
                && broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(principal),
                    ResourceType::Topic,
                    &topic,
                    AclOperation::Write,
                )
            {
                out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
            } else {
                out.put_i16(part_err);
            }
        }
    }
}

/// AddOffsetsToTxn (API 25) v0 — register group for transactional offsets (Phase 31).
fn encode_add_offsets_to_txn(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    // Request: transactional_id, producer_id, producer_epoch, group_id
    let _txn_id = match get_string(src) {
        Ok(t) => t,
        Err(_) => {
            out.put_i32(0);
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    if src.remaining() < 8 + 2 {
        out.put_i32(0);
        out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
        return;
    }
    let producer_id = src.get_i64() as u64;
    let producer_epoch = src.get_i16() as u16;
    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            out.put_i32(0);
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Group,
            &group_id,
            AclOperation::Read,
        )
    {
        out.put_i32(0);
        out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
        return;
    }

    // Ensure txn is open (idempotent if already open via AddPartitionsToTxn).
    let err = broker.ensure_txn_open(producer_id, producer_epoch);
    out.put_i32(0); // throttle
    out.put_i16(if err == 0 {
        KafkaErrorCode::None.as_i16()
    } else {
        map_idempotent_error(err)
    });
}

/// EndTxn (API 26) v0 — commit or abort (Phase 31).
fn encode_end_txn(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, principal: &str) {
    // Request: transactional_id, producer_id, producer_epoch, committed (bool)
    let _txn_id = match get_string(src) {
        Ok(t) => t,
        Err(_) => {
            out.put_i32(0);
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    if src.remaining() < 8 + 2 + 1 {
        out.put_i32(0);
        out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
        return;
    }
    let producer_id = src.get_i64() as u64;
    let producer_epoch = src.get_i16() as u16;
    let committed = src.get_u8() != 0;

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Write,
        )
    {
        out.put_i32(0);
        out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        return;
    }

    match broker.end_txn(producer_id, producer_epoch, committed, &[]) {
        Ok((err, _results)) => {
            out.put_i32(0); // throttle
            out.put_i16(if err == 0 {
                KafkaErrorCode::None.as_i16()
            } else {
                map_idempotent_error(err)
            });
        }
        Err(_) => {
            out.put_i32(0);
            out.put_i16(KafkaErrorCode::Unknown.as_i16());
        }
    }
}

/// TxnOffsetCommit (API 28) v0 — buffer offsets until EndTxn commit (Phase 31).
fn encode_txn_offset_commit(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    // Request: transactional_id, group_id, producer_id, producer_epoch,
    //          [topics → [partition, offset, metadata]]
    let _txn_id = match get_string(src) {
        Ok(t) => t,
        Err(_) => {
            out.put_i32(0);
            out.put_i32(0);
            return;
        }
    };
    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            out.put_i32(0);
            out.put_i32(0);
            return;
        }
    };
    if src.remaining() < 8 + 2 + 4 {
        out.put_i32(0);
        out.put_i32(0);
        return;
    }
    let producer_id = src.get_i64() as u64;
    let producer_epoch = src.get_i16() as u16;
    let topic_count = src.get_i32();

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Group,
            &group_id,
            AclOperation::Read,
        )
    {
        out.put_i32(0);
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
            let n = src.get_i32();
            out.put_i32(n.max(0));
            for _ in 0..n.max(0) {
                // skip partition, offset, metadata
                if src.remaining() < 4 + 8 {
                    break;
                }
                let p = src.get_i32();
                let _ = src.get_i64();
                let _ = get_nullable_string(src);
                out.put_i32(p);
                out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
            }
        }
        return;
    }

    // Collect offsets, then buffer once.
    let mut collected: Vec<(String, String, u32, u64, String)> = Vec::new();
    // Also keep structure for response echo.
    struct Part {
        partition: i32,
    }
    struct TopicParts {
        name: String,
        parts: Vec<Part>,
    }
    let mut structure: Vec<TopicParts> = Vec::new();

    for _ in 0..topic_count.max(0) {
        let topic = match get_string(src) {
            Ok(t) => t,
            Err(_) => break,
        };
        if src.remaining() < 4 {
            structure.push(TopicParts {
                name: topic,
                parts: vec![],
            });
            break;
        }
        let part_count = src.get_i32();
        let mut parts = Vec::new();
        for _ in 0..part_count.max(0) {
            if src.remaining() < 4 + 8 {
                break;
            }
            let partition = src.get_i32();
            let offset = src.get_i64();
            let metadata = get_nullable_string(src).ok().flatten().unwrap_or_default();
            if offset >= 0 {
                collected.push((
                    group_id.clone(),
                    topic.clone(),
                    partition as u32,
                    offset as u64,
                    metadata,
                ));
            }
            parts.push(Part { partition });
        }
        structure.push(TopicParts { name: topic, parts });
    }

    let err = broker.buffer_txn_offsets(producer_id, producer_epoch, &collected);

    let part_err = if err == 0 {
        KafkaErrorCode::None.as_i16()
    } else {
        map_idempotent_error(err)
    };

    out.put_i32(0); // throttle
    out.put_i32(structure.len() as i32);
    for t in structure {
        put_string(out, &t.name);
        out.put_i32(t.parts.len() as i32);
        for p in t.parts {
            out.put_i32(p.partition);
            out.put_i16(part_err);
        }
    }
}

fn encode_join_group(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
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
            Some(principal),
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

fn encode_sync_group(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, principal: &str) {
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
            Some(principal),
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

fn encode_heartbeat(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, principal: &str) {
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
            Some(principal),
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

fn encode_leave_group(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, principal: &str) {
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
            Some(principal),
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

fn encode_offset_commit(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
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
            Some(principal),
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

fn encode_offset_fetch(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, principal: &str) {
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
            Some(principal),
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

fn encode_list_groups(broker: &Broker, out: &mut BytesMut, principal: &str) {
    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
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

fn encode_describe_groups(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, principal: &str) {
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
                Some(principal),
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

fn encode_offset_delete(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    // OffsetDelete v0: group_id, [topic [partition]]
    // Response: error_code, throttle_time_ms, [topic [partition, error_code]]
    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            out.put_i32(0);
            out.put_i32(0);
            return;
        }
    };

    struct TopicParts {
        name: String,
        partitions: Vec<i32>,
    }
    let mut topics: Vec<TopicParts> = Vec::new();
    if src.remaining() >= 4 {
        let topic_count = src.get_i32();
        for _ in 0..topic_count.max(0) {
            let name = match get_string(src) {
                Ok(n) => n,
                Err(_) => break,
            };
            if src.remaining() < 4 {
                break;
            }
            let pc = src.get_i32();
            let mut partitions = Vec::new();
            for _ in 0..pc.max(0) {
                if src.remaining() < 4 {
                    break;
                }
                partitions.push(src.get_i32());
            }
            topics.push(TopicParts { name, partitions });
        }
    }

    let auth_denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Group,
            &group_id,
            AclOperation::Delete,
        );

    if auth_denied {
        out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
        out.put_i32(0); // throttle
        out.put_i32(topics.len() as i32);
        for t in &topics {
            put_string(out, &t.name);
            out.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                out.put_i32(*p);
                out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
            }
        }
        return;
    }

    // Collect pairs for a single delete_offsets call (never empty-all unless
    // the client listed partitions — empty topics means no-op).
    let mut pairs: Vec<(String, u32)> = Vec::new();
    for t in &topics {
        for &p in &t.partitions {
            if p >= 0 {
                pairs.push((t.name.clone(), p as u32));
            }
        }
    }

    let delete_err = if pairs.is_empty() {
        None
    } else {
        match broker.groups().delete_offsets(&group_id, &pairs) {
            Ok(_) => None,
            Err(_) => Some(KafkaErrorCode::Unknown.as_i16()),
        }
    };

    out.put_i16(KafkaErrorCode::None.as_i16());
    out.put_i32(0); // throttle
    out.put_i32(topics.len() as i32);
    for t in &topics {
        put_string(out, &t.name);
        out.put_i32(t.partitions.len() as i32);
        for &p in &t.partitions {
            out.put_i32(p);
            if p < 0 {
                out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            } else if let Some(e) = delete_err {
                out.put_i16(e);
            } else {
                out.put_i16(KafkaErrorCode::None.as_i16());
            }
        }
    }
}

fn encode_delete_groups(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, principal: &str) {
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
                Some(principal),
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

fn encode_create_partitions(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, principal: &str) {
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
                Some(principal),
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

fn encode_describe_configs(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, principal: &str) {
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
                Some(principal),
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

fn encode_alter_configs(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, principal: &str) {
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
                Some(principal),
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

// ---------------------------------------------------------------------------
// Phase 35: DeleteRecords + ACL admin (Describe/Create/DeleteAcls)
// ---------------------------------------------------------------------------

/// Kafka ResourceType: Any.
const KAFKA_RT_ANY: i8 = 1;
/// Kafka ResourceType: Topic.
const KAFKA_RT_TOPIC: i8 = 2;
/// Kafka ResourceType: Group.
const KAFKA_RT_GROUP: i8 = 3;
/// Kafka ResourceType: Cluster.
const KAFKA_RT_CLUSTER: i8 = 4;

/// Kafka AclOperation: Any.
const KAFKA_OP_ANY: i8 = 1;
/// Kafka AclPermissionType: Any.
const KAFKA_PERM_ANY: i8 = 1;
/// Kafka AclPermissionType: Deny.
const KAFKA_PERM_DENY: i8 = 2;
/// Kafka AclPermissionType: Allow.
const KAFKA_PERM_ALLOW: i8 = 3;

/// Kafka ResourcePatternType: Any.
const KAFKA_PATTERN_ANY: i8 = 1;
/// Kafka ResourcePatternType: Literal.
const KAFKA_PATTERN_LITERAL: i8 = 3;

/// Kafka cluster resource name advertised on the wire.
const KAFKA_CLUSTER_NAME: &str = "kafka-cluster";

fn encode_delete_records(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    // DeleteRecords v0/v1: [topic [partition offset]] timeout_ms
    // Response: throttle [topic [partition low_watermark error]]
    out.put_i32(0); // throttle_time_ms
    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let topic_count = src.get_i32();
    struct PartReq {
        partition: i32,
        offset: i64,
    }
    struct TopicReq {
        name: String,
        parts: Vec<PartReq>,
    }
    let mut topics = Vec::new();
    for _ in 0..topic_count.max(0) {
        let name = match get_string(src) {
            Ok(n) => n,
            Err(_) => break,
        };
        if src.remaining() < 4 {
            break;
        }
        let pc = src.get_i32();
        let mut parts = Vec::new();
        for _ in 0..pc.max(0) {
            if src.remaining() < 12 {
                break;
            }
            parts.push(PartReq {
                partition: src.get_i32(),
                offset: src.get_i64(),
            });
        }
        topics.push(TopicReq { name, parts });
    }
    if src.remaining() >= 4 {
        let _timeout = src.get_i32();
    }

    out.put_i32(topics.len() as i32);
    for t in topics {
        put_string(out, &t.name);
        out.put_i32(t.parts.len() as i32);
        let topic_allowed = !broker.acls().is_enabled()
            || broker.acls().authorize(
                Some(principal),
                ResourceType::Topic,
                &t.name,
                AclOperation::Delete,
            );
        for p in t.parts {
            out.put_i32(p.partition);
            if !topic_allowed {
                out.put_i64(0);
                out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
                continue;
            }
            if p.partition < 0 || p.offset < 0 {
                out.put_i64(0);
                out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
                continue;
            }
            match broker.delete_records(&t.name, p.partition as u32, p.offset as u64) {
                Ok((low, err)) => {
                    out.put_i64(low as i64);
                    let kerr = if err == 0 {
                        KafkaErrorCode::None.as_i16()
                    } else if err == 13 {
                        // Volant ErrorCode::NotLeaderForPartition
                        KafkaErrorCode::NotLeaderForPartition.as_i16()
                    } else {
                        KafkaErrorCode::Unknown.as_i16()
                    };
                    out.put_i16(kerr);
                }
                Err(Error::NotFound(_)) => {
                    out.put_i64(0);
                    out.put_i16(KafkaErrorCode::UnknownTopicOrPartition.as_i16());
                }
                Err(_) => {
                    out.put_i64(0);
                    out.put_i16(KafkaErrorCode::Unknown.as_i16());
                }
            }
        }
    }
}

fn encode_describe_acls(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // DescribeAcls v0/v1 request fields; response: throttle, error, msg, resources
    out.put_i32(0); // throttle

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        )
    {
        out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        put_nullable_string(out, Some("Cluster Describe denied"));
        out.put_i32(0);
        return;
    }

    let filter = match parse_acl_filter(src, version) {
        Ok(f) => f,
        Err(msg) => {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            put_nullable_string(out, Some(&msg));
            out.put_i32(0);
            return;
        }
    };

    let matched = filter_acl_entries(broker, &filter);
    let groups = group_acls_by_resource(&matched);

    out.put_i16(KafkaErrorCode::None.as_i16());
    put_nullable_string(out, None);
    out.put_i32(groups.len() as i32);
    for (rt, name, acls) in groups {
        out.put_i8(rt);
        put_string(out, &name);
        if version >= 1 {
            out.put_i8(KAFKA_PATTERN_LITERAL);
        }
        out.put_i32(acls.len() as i32);
        for e in acls {
            put_string(out, &kafka_principal(&e.principal));
            put_string(out, "*");
            out.put_i8(volant_op_to_kafka(e.operation));
            out.put_i8(volant_perm_to_kafka(e.permission));
        }
    }
}

fn encode_create_acls(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // CreateAcls: [creations...] → throttle + [error, msg] per creation
    out.put_i32(0); // throttle

    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let n = src.get_i32();

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        )
    {
        out.put_i32(n.max(0));
        for _ in 0..n.max(0) {
            // Drain remaining request fields best-effort so we still respond.
            let _ = parse_acl_creation(src, version);
            out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
            put_nullable_string(out, Some("Cluster Alter denied"));
        }
        return;
    }

    struct CreationResult {
        error: i16,
        message: Option<String>,
    }
    let mut results = Vec::new();
    let mut to_create = Vec::new();

    for _ in 0..n.max(0) {
        match parse_acl_creation(src, version) {
            Ok(entry) => {
                to_create.push(entry);
                results.push(CreationResult {
                    error: KafkaErrorCode::None.as_i16(),
                    message: None,
                });
            }
            Err(msg) => {
                results.push(CreationResult {
                    error: KafkaErrorCode::InvalidRequest.as_i16(),
                    message: Some(msg),
                });
            }
        }
    }

    if !to_create.is_empty() {
        if let Err(_) = broker.acls().create(to_create) {
            // Mark previously-ok results as storage failure.
            for r in results.iter_mut() {
                if r.error == KafkaErrorCode::None.as_i16() {
                    r.error = KafkaErrorCode::Unknown.as_i16();
                    r.message = Some("failed to persist ACLs".into());
                }
            }
        }
    }

    out.put_i32(results.len() as i32);
    for r in results {
        out.put_i16(r.error);
        put_nullable_string(out, r.message.as_deref());
    }
}

fn encode_delete_acls(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // DeleteAcls: [filters...] → throttle + [error, msg, matching_acls...]
    out.put_i32(0); // throttle

    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let n = src.get_i32();

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        )
    {
        out.put_i32(n.max(0));
        for _ in 0..n.max(0) {
            let _ = parse_acl_filter(src, version);
            out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
            put_nullable_string(out, Some("Cluster Alter denied"));
            out.put_i32(0);
        }
        return;
    }

    out.put_i32(n.max(0));
    for _ in 0..n.max(0) {
        let filter = match parse_acl_filter(src, version) {
            Ok(f) => f,
            Err(msg) => {
                out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
                put_nullable_string(out, Some(&msg));
                out.put_i32(0);
                continue;
            }
        };
        let matched = filter_acl_entries(broker, &filter);
        match broker.acls().delete(&matched) {
            Ok(_) => {
                out.put_i16(KafkaErrorCode::None.as_i16());
                put_nullable_string(out, None);
                out.put_i32(matched.len() as i32);
                for e in &matched {
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    put_nullable_string(out, None);
                    out.put_i8(volant_rt_to_kafka(e.resource_type));
                    put_string(out, &kafka_resource_name(e.resource_type, &e.resource));
                    if version >= 1 {
                        out.put_i8(KAFKA_PATTERN_LITERAL);
                    }
                    put_string(out, &kafka_principal(&e.principal));
                    put_string(out, "*");
                    out.put_i8(volant_op_to_kafka(e.operation));
                    out.put_i8(volant_perm_to_kafka(e.permission));
                }
            }
            Err(_) => {
                out.put_i16(KafkaErrorCode::Unknown.as_i16());
                put_nullable_string(out, Some("failed to delete ACLs"));
                out.put_i32(0);
            }
        }
    }
}

/// Parsed Kafka ACL filter (Describe/Delete).
struct AclFilter {
    resource_type: Option<ResourceType>,
    resource_name: Option<String>,
    principal: Option<String>,
    operation: Option<AclOperation>,
    permission: Option<AclPermission>,
}

fn parse_acl_filter(src: &mut impl Buf, version: i16) -> std::result::Result<AclFilter, String> {
    if src.remaining() < 1 {
        return Err("truncated ACL filter".into());
    }
    let rt_raw = src.get_i8();
    let resource_type = kafka_rt_to_volant_filter(rt_raw)?;
    let resource_name = match get_nullable_string(src) {
        Ok(Some(s)) if !s.is_empty() => Some(normalize_resource_name(resource_type, &s)),
        Ok(_) => None,
        Err(_) => return Err("invalid resource name filter".into()),
    };
    if version >= 1 {
        if src.remaining() < 1 {
            return Err("missing pattern type filter".into());
        }
        let pattern = src.get_i8();
        if pattern != KAFKA_PATTERN_LITERAL && pattern != KAFKA_PATTERN_ANY {
            return Err(format!("unsupported pattern type {pattern}"));
        }
    }
    let principal = match get_nullable_string(src) {
        Ok(Some(s)) if !s.is_empty() => Some(strip_user_prefix(&s)),
        Ok(_) => None,
        Err(_) => return Err("invalid principal filter".into()),
    };
    // Host filter — ignored (Volant has no host dimension).
    let _host = match get_nullable_string(src) {
        Ok(h) => h,
        Err(_) => return Err("invalid host filter".into()),
    };
    if src.remaining() < 2 {
        return Err("truncated operation/permission filter".into());
    }
    let op_raw = src.get_i8();
    let perm_raw = src.get_i8();
    let operation = kafka_op_to_volant_filter(op_raw)?;
    let permission = kafka_perm_to_volant_filter(perm_raw)?;
    Ok(AclFilter {
        resource_type,
        resource_name,
        principal,
        operation,
        permission,
    })
}

fn parse_acl_creation(src: &mut impl Buf, version: i16) -> std::result::Result<AclEntry, String> {
    if src.remaining() < 1 {
        return Err("truncated ACL creation".into());
    }
    let rt_raw = src.get_i8();
    let resource_type = kafka_rt_to_volant(rt_raw)?;
    let resource_name = match get_string(src) {
        Ok(s) if !s.is_empty() => normalize_resource_name(Some(resource_type), &s),
        Ok(_) => return Err("empty resource name".into()),
        Err(_) => return Err("invalid resource name".into()),
    };
    if version >= 1 {
        if src.remaining() < 1 {
            return Err("missing pattern type".into());
        }
        let pattern = src.get_i8();
        if pattern != KAFKA_PATTERN_LITERAL {
            return Err(format!("unsupported pattern type {pattern} (only LITERAL)"));
        }
    }
    let principal = match get_string(src) {
        Ok(s) if !s.is_empty() => strip_user_prefix(&s),
        Ok(_) => return Err("empty principal".into()),
        Err(_) => return Err("invalid principal".into()),
    };
    let _host = match get_string(src) {
        Ok(h) => h,
        Err(_) => return Err("invalid host".into()),
    };
    if src.remaining() < 2 {
        return Err("truncated operation/permission".into());
    }
    let op_raw = src.get_i8();
    let perm_raw = src.get_i8();
    let operation = kafka_op_to_volant(op_raw)?;
    let permission = kafka_perm_to_volant(perm_raw)?;
    Ok(AclEntry {
        principal,
        resource_type,
        resource: resource_name,
        operation,
        permission,
    })
}

fn filter_acl_entries(broker: &Broker, filter: &AclFilter) -> Vec<AclEntry> {
    let all = broker.acls().list(
        filter.principal.as_deref(),
        filter.resource_type,
        filter.resource_name.as_deref(),
    );
    all.into_iter()
        .filter(|e| {
            // Kafka ANY means any stored value; a specific value requires exact match.
            if let Some(op) = filter.operation {
                if e.operation != op {
                    return false;
                }
            }
            if let Some(perm) = filter.permission {
                if e.permission != perm {
                    return false;
                }
            }
            true
        })
        .collect()
}

fn group_acls_by_resource(entries: &[AclEntry]) -> Vec<(i8, String, Vec<AclEntry>)> {
    let mut map: std::collections::BTreeMap<(i8, String), Vec<AclEntry>> =
        std::collections::BTreeMap::new();
    for e in entries {
        let rt = volant_rt_to_kafka(e.resource_type);
        let name = kafka_resource_name(e.resource_type, &e.resource);
        map.entry((rt, name)).or_default().push(e.clone());
    }
    map.into_iter()
        .map(|((rt, name), acls)| (rt, name, acls))
        .collect()
}

fn kafka_rt_to_volant(v: i8) -> std::result::Result<ResourceType, String> {
    match v {
        KAFKA_RT_TOPIC => Ok(ResourceType::Topic),
        KAFKA_RT_GROUP => Ok(ResourceType::Group),
        KAFKA_RT_CLUSTER => Ok(ResourceType::Cluster),
        other => Err(format!("unsupported resource type {other}")),
    }
}

fn kafka_rt_to_volant_filter(v: i8) -> std::result::Result<Option<ResourceType>, String> {
    if v == KAFKA_RT_ANY {
        return Ok(None);
    }
    kafka_rt_to_volant(v).map(Some)
}

fn volant_rt_to_kafka(rt: ResourceType) -> i8 {
    match rt {
        ResourceType::Topic => KAFKA_RT_TOPIC,
        ResourceType::Group => KAFKA_RT_GROUP,
        ResourceType::Cluster => KAFKA_RT_CLUSTER,
    }
}

fn kafka_op_to_volant(v: i8) -> std::result::Result<AclOperation, String> {
    match v {
        2 => Ok(AclOperation::All),
        3 => Ok(AclOperation::Read),
        4 => Ok(AclOperation::Write),
        5 => Ok(AclOperation::Create),
        6 => Ok(AclOperation::Delete),
        7 => Ok(AclOperation::Alter),
        8 => Ok(AclOperation::Describe),
        9 => Ok(AclOperation::ClusterAction),
        // Best-effort collapse of config / idempotent ops.
        10 => Ok(AclOperation::Describe),
        11 => Ok(AclOperation::Alter),
        12 => Ok(AclOperation::Write),
        other => Err(format!("unsupported operation {other}")),
    }
}

fn kafka_op_to_volant_filter(v: i8) -> std::result::Result<Option<AclOperation>, String> {
    if v == KAFKA_OP_ANY {
        return Ok(None);
    }
    kafka_op_to_volant(v).map(Some)
}

fn volant_op_to_kafka(op: AclOperation) -> i8 {
    match op {
        AclOperation::All => 2,
        AclOperation::Read => 3,
        AclOperation::Write => 4,
        AclOperation::Create => 5,
        AclOperation::Delete => 6,
        AclOperation::Alter => 7,
        AclOperation::Describe => 8,
        AclOperation::ClusterAction => 9,
    }
}

fn kafka_perm_to_volant(v: i8) -> std::result::Result<AclPermission, String> {
    match v {
        KAFKA_PERM_DENY => Ok(AclPermission::Deny),
        KAFKA_PERM_ALLOW => Ok(AclPermission::Allow),
        other => Err(format!("unsupported permission type {other}")),
    }
}

fn kafka_perm_to_volant_filter(v: i8) -> std::result::Result<Option<AclPermission>, String> {
    if v == KAFKA_PERM_ANY {
        return Ok(None);
    }
    kafka_perm_to_volant(v).map(Some)
}

fn volant_perm_to_kafka(p: AclPermission) -> i8 {
    match p {
        AclPermission::Deny => KAFKA_PERM_DENY,
        AclPermission::Allow => KAFKA_PERM_ALLOW,
    }
}

fn strip_user_prefix(principal: &str) -> String {
    if let Some(rest) = principal.strip_prefix("User:") {
        rest.to_string()
    } else {
        principal.to_string()
    }
}

fn kafka_principal(volant_principal: &str) -> String {
    if volant_principal.starts_with("User:") {
        volant_principal.to_string()
    } else {
        format!("User:{volant_principal}")
    }
}

fn normalize_resource_name(rt: Option<ResourceType>, name: &str) -> String {
    if matches!(rt, Some(ResourceType::Cluster))
        || (rt.is_none() && (name == KAFKA_CLUSTER_NAME || name == CLUSTER_RESOURCE))
    {
        if name == KAFKA_CLUSTER_NAME || name == CLUSTER_RESOURCE {
            return CLUSTER_RESOURCE.to_string();
        }
    }
    if matches!(rt, Some(ResourceType::Cluster)) {
        // Any cluster resource name collapses to canonical.
        return CLUSTER_RESOURCE.to_string();
    }
    name.to_string()
}

fn kafka_resource_name(rt: ResourceType, volant_name: &str) -> String {
    if rt == ResourceType::Cluster {
        KAFKA_CLUSTER_NAME.to_string()
    } else {
        volant_name.to_string()
    }
}
