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
use crate::group::static_member_id;

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
        Some(ApiKey::Metadata) if (0..=8).contains(&hdr.api_version) => {
            encode_metadata(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::Produce) if (0..=3).contains(&hdr.api_version) => {
            encode_produce(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::Fetch) if (0..=4).contains(&hdr.api_version) => {
            encode_fetch(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::ListOffsets) if (0..=5).contains(&hdr.api_version) => {
            encode_list_offsets(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::CreateTopics) if (0..=4).contains(&hdr.api_version) => {
            encode_create_topics(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::DeleteTopics) if (0..=3).contains(&hdr.api_version) => {
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
        Some(ApiKey::FindCoordinator) if (0..=2).contains(&hdr.api_version) => {
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
        Some(ApiKey::JoinGroup) if (0..=5).contains(&hdr.api_version) => {
            encode_join_group(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::SyncGroup) if (0..=3).contains(&hdr.api_version) => {
            encode_sync_group(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::Heartbeat) if (0..=3).contains(&hdr.api_version) => {
            encode_heartbeat(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::LeaveGroup) if (0..=3).contains(&hdr.api_version) => {
            encode_leave_group(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::OffsetCommit) if (0..=7).contains(&hdr.api_version) => {
            encode_offset_commit(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::OffsetFetch) if (0..=5).contains(&hdr.api_version) => {
            encode_offset_fetch(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::DescribeGroups) if (0..=4).contains(&hdr.api_version) => {
            encode_describe_groups(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::ListGroups) if (0..=2).contains(&hdr.api_version) => {
            encode_list_groups(broker, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::DeleteGroups) if (0..=1).contains(&hdr.api_version) => {
            encode_delete_groups(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::OffsetDelete) if hdr.api_version == 0 => {
            encode_offset_delete(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::CreatePartitions) if (0..=1).contains(&hdr.api_version) => {
            encode_create_partitions(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::DescribeConfigs) if (0..=3).contains(&hdr.api_version) => {
            encode_describe_configs(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::AlterConfigs) if (0..=1).contains(&hdr.api_version) => {
            encode_alter_configs(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::IncrementalAlterConfigs) if hdr.api_version == 0 => {
            encode_incremental_alter_configs(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::InitProducerId) if (0..=1).contains(&hdr.api_version) => {
            encode_init_producer_id(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::OffsetForLeaderEpoch) if (0..=3).contains(&hdr.api_version) => {
            encode_offset_for_leader_epoch(broker, &mut src, &mut out, hdr.api_version, principal);
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

/// Stable cluster id advertised on Metadata v2+ (classic).
const KAFKA_CLUSTER_ID: &str = "volant";

/// Kafka `Integer.MIN_VALUE` — authorized operations not included in the response.
const AUTH_OPS_OMITTED: i32 = i32::MIN;

fn encode_metadata(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
    // Request:
    //   topics: array (v0) / nullable array (v1+)
    //     v0: empty = all topics
    //     v1+: null (-1) = all topics; empty (0) = no topics
    //   allow_auto_topic_creation: bool (v4+, ignored)
    //   include_cluster_authorized_operations: bool (v8)
    //   include_topic_authorized_operations: bool (v8)
    let topic_len = if src.remaining() >= 4 {
        src.get_i32()
    } else {
        0
    };

    let list_all: bool;
    let mut requested: Vec<String> = Vec::new();
    if version == 0 {
        list_all = topic_len <= 0;
        if topic_len > 0 {
            for _ in 0..topic_len {
                match get_string(src) {
                    Ok(t) => requested.push(t),
                    Err(_) => break,
                }
            }
        }
    } else if topic_len < 0 {
        // null array → all topics
        list_all = true;
    } else {
        list_all = false;
        for _ in 0..topic_len {
            match get_string(src) {
                Ok(t) => requested.push(t),
                Err(_) => break,
            }
        }
    }

    // v4+: allow_auto_topic_creation (ignored — Volant does not auto-create on Metadata).
    if version >= 4 && src.remaining() >= 1 {
        let _allow_auto = src.get_u8();
    }

    let mut include_cluster_ops = false;
    let mut include_topic_ops = false;
    if version >= 8 {
        if src.remaining() >= 1 {
            include_cluster_ops = src.get_u8() != 0;
        }
        if src.remaining() >= 1 {
            include_topic_ops = src.get_u8() != 0;
        }
    }

    let need_cluster_describe = list_all;
    if broker.acls().is_enabled() && need_cluster_describe {
        if !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        ) {
            // Empty brokers + empty topics with versioned framing.
            write_metadata_empty(out, version, include_cluster_ops, principal, broker);
            return;
        }
    }

    let filter: Option<Vec<TopicName>> = if list_all {
        None
    } else if requested.is_empty() {
        // Explicit empty list → no topics in the response.
        write_metadata_brokers_header(broker, out, version);
        out.put_i32(0); // topics
        if version >= 8 {
            out.put_i32(cluster_authorized_ops(
                broker,
                principal,
                include_cluster_ops,
            ));
        }
        return;
    } else {
        Some(requested.iter().map(|t| TopicName::new(t.clone())).collect())
    };

    let snap = match &filter {
        None => broker.metadata(None),
        Some(ts) => broker.metadata(Some(ts.as_slice())),
    };

    // throttle_time_ms (v3+)
    if version >= 3 {
        out.put_i32(0);
    }

    // Brokers
    out.put_i32(snap.brokers.len() as i32);
    for (id, host, port) in &snap.brokers {
        out.put_i32(*id as i32);
        put_string(out, host);
        out.put_i32(i32::from(*port));
        if version >= 1 {
            put_nullable_string(out, None); // rack
        }
    }

    // cluster_id (v2+)
    if version >= 2 {
        put_nullable_string(out, Some(KAFKA_CLUSTER_ID));
    }

    // controller_id (v1+)
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
            if version >= 7 {
                out.put_i32(-1); // leader_epoch unknown
            }
            out.put_i32(p.replicas.len() as i32);
            for r in &p.replicas {
                out.put_i32(*r as i32);
            }
            out.put_i32(p.isr.len() as i32);
            for r in &p.isr {
                out.put_i32(*r as i32);
            }
            if version >= 5 {
                out.put_i32(0); // offline_replicas empty
            }
        }
        if version >= 8 {
            out.put_i32(topic_authorized_ops(
                broker,
                principal,
                t.name.as_str(),
                include_topic_ops,
            ));
        }
    }

    if version >= 8 {
        out.put_i32(cluster_authorized_ops(
            broker,
            principal,
            include_cluster_ops,
        ));
    }
}

/// Write brokers + cluster framing for Metadata when the topic list is empty.
fn write_metadata_brokers_header(broker: &Broker, out: &mut BytesMut, version: i16) {
    // Brokers/controller only; topic array is written by the caller.
    let snap = broker.metadata(None);
    if version >= 3 {
        out.put_i32(0); // throttle
    }
    out.put_i32(snap.brokers.len() as i32);
    for (id, host, port) in &snap.brokers {
        out.put_i32(*id as i32);
        put_string(out, host);
        out.put_i32(i32::from(*port));
        if version >= 1 {
            put_nullable_string(out, None);
        }
    }
    if version >= 2 {
        put_nullable_string(out, Some(KAFKA_CLUSTER_ID));
    }
    if version >= 1 {
        out.put_i32(snap.controller_id as i32);
    }
}

/// ACL-denied Metadata: empty brokers and topics with correct versioned fields.
fn write_metadata_empty(
    out: &mut BytesMut,
    version: i16,
    include_cluster_ops: bool,
    _principal: &str,
    _broker: &Broker,
) {
    if version >= 3 {
        out.put_i32(0);
    }
    out.put_i32(0); // brokers
    if version >= 2 {
        put_nullable_string(out, Some(KAFKA_CLUSTER_ID));
    }
    if version >= 1 {
        out.put_i32(-1); // controller_id
    }
    out.put_i32(0); // topics
    if version >= 8 {
        // Denied cluster describe → omit ops (or empty bitfield). Use omitted when not requested.
        out.put_i32(if include_cluster_ops { 0 } else { AUTH_OPS_OMITTED });
    }
}

fn topic_authorized_ops(
    broker: &Broker,
    principal: &str,
    topic: &str,
    include: bool,
) -> i32 {
    if !include {
        return AUTH_OPS_OMITTED;
    }
    let ops = [
        AclOperation::Read,
        AclOperation::Write,
        AclOperation::Create,
        AclOperation::Delete,
        AclOperation::Alter,
        AclOperation::Describe,
    ];
    let mut bits = 0i32;
    for op in ops {
        let allowed = if broker.acls().is_enabled() {
            broker
                .acls()
                .authorize(Some(principal), ResourceType::Topic, topic, op)
        } else {
            true
        };
        if allowed {
            bits |= 1i32 << (volant_op_to_kafka(op) as u32);
        }
    }
    bits
}

fn cluster_authorized_ops(broker: &Broker, principal: &str, include: bool) -> i32 {
    if !include {
        return AUTH_OPS_OMITTED;
    }
    let ops = [
        AclOperation::Create,
        AclOperation::Alter,
        AclOperation::Describe,
        AclOperation::ClusterAction,
    ];
    let mut bits = 0i32;
    for op in ops {
        let allowed = if broker.acls().is_enabled() {
            broker
                .acls()
                .authorize(Some(principal), ResourceType::Cluster, CLUSTER_RESOURCE, op)
        } else {
            true
        };
        if allowed {
            bits |= 1i32 << (volant_op_to_kafka(op) as u32);
        }
    }
    bits
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

/// OffsetForLeaderEpoch (API key 23) classic v0–3.
///
/// Without epoch history, any requested epoch ≤ the current partition epoch
/// (or -1 = latest) returns end_offset = HWM and the current leader epoch.
fn encode_offset_for_leader_epoch(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // Request: replica_id (v3+), topics[{ name, partitions[{ partition,
    //   current_leader_epoch (v2+), leader_epoch }] }]
    if version >= 3 {
        if src.remaining() < 4 {
            if version >= 2 {
                out.put_i32(0); // throttle
            }
            out.put_i32(0);
            return;
        }
        let _replica_id = src.get_i32();
    }

    if version >= 2 {
        out.put_i32(0); // throttle_time_ms
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
            // partition + optional current_leader_epoch + leader_epoch
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

            // Response partition entry: error, partition, leader_epoch (v1+), end_offset.
            let write_part = |out: &mut BytesMut, err: i16, epoch: i32, end: i64| {
                out.put_i16(err);
                out.put_i32(partition);
                if version >= 1 {
                    out.put_i32(epoch);
                }
                out.put_i64(end);
            };

            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(principal),
                    ResourceType::Topic,
                    &topic,
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

            let name = TopicName::new(topic.clone());
            let snap = broker.metadata(Some(&[name]));
            let part_meta = snap.topics.first().and_then(|t| {
                t.partitions
                    .iter()
                    .find(|p| p.partition_id.0 == partition as u32)
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

            // Fence on current_leader_epoch (v2+).
            if current_leader_epoch != -1 {
                if current_leader_epoch > current_epoch {
                    write_part(
                        out,
                        KafkaErrorCode::UnknownLeaderEpoch.as_i16(),
                        current_epoch,
                        -1,
                    );
                    continue;
                }
                if current_leader_epoch < current_epoch {
                    write_part(
                        out,
                        KafkaErrorCode::FencedLeaderEpoch.as_i16(),
                        current_epoch,
                        -1,
                    );
                    continue;
                }
            }

            // Lookup requested leader_epoch (-1 = latest / current).
            if leader_epoch != -1 && leader_epoch > current_epoch {
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
    }
}

fn encode_list_offsets(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
    // ListOffsets classic v0–5:
    //   replica_id, isolation_level (v2+),
    //   topics[{ name, partitions[{ partition, current_leader_epoch (v4+),
    //     timestamp, max_num_offsets (v0) }] }]
    // Response: throttle (v2+), topics[{ name, partitions[{ partition, error,
    //   v0: [timestamp,offset] array | v1+: timestamp, offset, leader_epoch (v4+) }] }]
    if src.remaining() < 4 {
        if version >= 2 {
            out.put_i32(0);
        }
        out.put_i32(0);
        return;
    }
    let _replica_id = src.get_i32();

    // v2+: isolation_level (0 / 1). Both map to the same offsets under
    // buffer-until-commit (LSO ≡ HWM); accept and ignore.
    if version >= 2 {
        if src.remaining() < 1 {
            out.put_i32(0); // throttle
            out.put_i32(0);
            return;
        }
        let isolation = src.get_u8();
        if isolation > 1 {
            out.put_i32(0);
            out.put_i32(0);
            return;
        }
    }

    if version >= 2 {
        out.put_i32(0); // throttle_time_ms
    }

    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let topic_count = src.get_i32();
    out.put_i32(topic_count.max(0));

    /// Write a partition result with versioned fields.
    fn write_part(
        out: &mut BytesMut,
        version: i16,
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
    }

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
            // partition + optional current_leader_epoch + timestamp [+ max_num v0]
            let need = if version >= 4 {
                4 + 4 + 8
            } else {
                4 + 8
            };
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

            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(principal),
                    ResourceType::Topic,
                    &topic,
                    AclOperation::Describe,
                )
            {
                write_part(
                    out,
                    version,
                    partition,
                    KafkaErrorCode::TopicAuthorizationFailed.as_i16(),
                    timestamp,
                    -1,
                    -1,
                );
                continue;
            }

            // Resolve current partition epoch for fencing / response (v4+).
            let name = TopicName::new(topic.clone());
            let part_meta = broker.metadata(Some(&[name])).topics.first().and_then(|t| {
                t.partitions
                    .iter()
                    .find(|p| p.partition_id.0 == partition as u32)
                    .cloned()
            });
            let current_epoch = part_meta
                .as_ref()
                .map(|p| p.leader_epoch as i32)
                .unwrap_or(-1);

            if current_leader_epoch != -1 {
                if current_leader_epoch > current_epoch && current_epoch >= 0 {
                    write_part(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::UnknownLeaderEpoch.as_i16(),
                        timestamp,
                        -1,
                        current_epoch,
                    );
                    continue;
                }
                if current_leader_epoch < current_epoch {
                    write_part(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::FencedLeaderEpoch.as_i16(),
                        timestamp,
                        -1,
                        current_epoch,
                    );
                    continue;
                }
            }

            // Kafka: -1 = latest, -2 = earliest.
            let want_earliest = timestamp == -2;
            let want_latest = timestamp == -1;
            if !want_earliest && !want_latest {
                write_part(
                    out,
                    version,
                    partition,
                    KafkaErrorCode::InvalidTimestamp.as_i16(),
                    timestamp,
                    -1,
                    current_epoch,
                );
                continue;
            }

            match broker.list_offsets(&topic, &[partition as u32]) {
                Ok(entries) => {
                    let (earliest, latest) = entries
                        .first()
                        .map(|(_, e, l)| (*e as i64, *l as i64))
                        .unwrap_or((0, 0));
                    let offset = if want_earliest { earliest } else { latest };
                    write_part(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::None.as_i16(),
                        timestamp,
                        offset,
                        current_epoch.max(0),
                    );
                }
                Err(Error::NotFound(_)) => {
                    write_part(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
                        timestamp,
                        -1,
                        -1,
                    );
                }
                Err(_) => {
                    write_part(
                        out,
                        version,
                        partition,
                        KafkaErrorCode::Unknown.as_i16(),
                        timestamp,
                        -1,
                        -1,
                    );
                }
            }
        }
    }
}

/// Default partition count when CreateTopics v4+ sends `num_partitions = -1`.
const DEFAULT_TOPIC_PARTITIONS: u32 = 1;

fn encode_create_topics(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // CreateTopics classic v0–4 (flexible 5+):
    //   request: [name, num_partitions, rf, assignments, configs], timeout, validate_only (v1+)
    //   response: throttle (v2+), [name, error, error_message (v1+)]
    // v4: num_partitions / rf may be -1 (default partitions; RF ignored).
    if src.remaining() < 4 {
        if version >= 2 {
            out.put_i32(0);
        }
        out.put_i32(0);
        return;
    }
    let topic_count = src.get_i32();
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
        // replica assignments (ignored)
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
            // Kafka values are nullable strings.
            let v = match get_nullable_string(src) {
                Ok(Some(s)) => s,
                Ok(None) => String::new(),
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
    // timeout_ms is present on all Kafka versions; tolerate short bodies.
    if src.remaining() >= 4 {
        let _timeout = src.get_i32();
    }
    let validate_only = if version >= 1 && src.remaining() >= 1 {
        src.get_u8() != 0
    } else {
        false
    };

    if version >= 2 {
        out.put_i32(0); // throttle
    }
    out.put_i32(reqs.len() as i32);
    for t in reqs {
        put_string(out, &t.name);

        let write_err = |out: &mut BytesMut, code: KafkaErrorCode, msg: Option<&str>| {
            out.put_i16(code.as_i16());
            if version >= 1 {
                put_nullable_string(out, msg);
            }
        };

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
            write_err(
                out,
                KafkaErrorCode::TopicAuthorizationFailed,
                Some("topic authorization failed"),
            );
            continue;
        }

        // Resolve partition count: v4+ allows -1 → default.
        let partitions = if t.partitions == -1 && version >= 4 {
            DEFAULT_TOPIC_PARTITIONS
        } else if t.partitions <= 0 {
            write_err(
                out,
                KafkaErrorCode::InvalidPartitions,
                Some("invalid partition count"),
            );
            continue;
        } else {
            t.partitions as u32
        };

        // Already exists?
        let exists = !broker
            .metadata(Some(&[TopicName::new(t.name.clone())]))
            .topics
            .is_empty();
        if exists {
            write_err(
                out,
                KafkaErrorCode::TopicAlreadyExists,
                Some("topic already exists"),
            );
            continue;
        }

        if validate_only {
            write_err(out, KafkaErrorCode::None, None);
            continue;
        }

        let result = if t.configs.is_empty() {
            broker.create_topic(t.name.as_str(), partitions)
        } else {
            broker.create_topic_with_configs(t.name.as_str(), partitions, &t.configs)
        };

        match result {
            Ok(_) => write_err(out, KafkaErrorCode::None, None),
            Err(Error::InvalidArgument(msg)) if msg.contains("already exists") => {
                write_err(
                    out,
                    KafkaErrorCode::TopicAlreadyExists,
                    Some("topic already exists"),
                );
            }
            Err(Error::InvalidArgument(msg)) => {
                write_err(out, KafkaErrorCode::InvalidTopicException, Some(&msg));
            }
            Err(_) => write_err(out, KafkaErrorCode::Unknown, None),
        }
    }
}

fn encode_delete_topics(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // DeleteTopics classic v0–3 (flexible 4+):
    //   request: [topic names] timeout_ms
    //   response: throttle (v1+), [name, error_code]
    if src.remaining() < 4 {
        if version >= 1 {
            out.put_i32(0);
        }
        out.put_i32(0);
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

    if version >= 1 {
        out.put_i32(0); // throttle (Kafka places this first)
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
}

fn encode_find_coordinator(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // FindCoordinator classic v0–2 (flexible 3+):
    //   v0: key
    //   v1–2: key + key_type (0=group, 1=transaction); response throttle + error_message
    // v2 is wire-identical to v1 (quota-timing semantics only on real Kafka).
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
    // JoinGroup classic v0–5:
    //   group_id, session_timeout, rebalance_timeout (v1+), member_id,
    //   group_instance_id (v5+), protocol_type, [protocols]
    // Response: throttle (v2+), error, generation, protocol, leader, member_id,
    //   members[{ member_id, group_instance_id (v5+), metadata }]
    let write_error_body = |out: &mut BytesMut, err: i16, generation: i32, protocol: &str, mid: &str| {
        if version >= 2 {
            out.put_i32(0); // throttle
        }
        out.put_i16(err);
        out.put_i32(generation);
        put_string(out, protocol);
        put_string(out, "");
        put_string(out, mid);
        out.put_i32(0);
    };

    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "");
            return;
        }
    };
    if src.remaining() < 4 {
        write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "");
        return;
    }
    let session_timeout = src.get_i32().max(0) as u32;
    if version >= 1 {
        if src.remaining() < 4 {
            write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "");
            return;
        }
        let _rebalance_timeout = src.get_i32();
    }
    let member_id = match get_string(src) {
        Ok(m) => m,
        Err(_) => {
            write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "");
            return;
        }
    };
    let group_instance_id = if version >= 5 {
        get_nullable_string(src).ok().flatten().unwrap_or_default()
    } else {
        String::new()
    };
    let _protocol_type = match get_string(src) {
        Ok(p) => p,
        Err(_) => {
            write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "");
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
        write_error_body(
            out,
            KafkaErrorCode::GroupAuthorizationFailed.as_i16(),
            0,
            "",
            "",
        );
        return;
    }

    if src.remaining() < 4 {
        write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "");
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
        &group_instance_id,
        |t| broker.partition_count_opt(t),
    ) {
        Ok(r) => r,
        Err(_) => {
            write_error_body(
                out,
                KafkaErrorCode::Unknown.as_i16(),
                0,
                &selected_protocol,
                "",
            );
            return;
        }
    };

    if result.error_code != 0 {
        write_error_body(
            out,
            map_group_error(result.error_code),
            result.generation as i32,
            &selected_protocol,
            &result.member_id,
        );
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

    // Echo instance id for this member when present (static members only).
    let self_instance = if group_instance_id.is_empty() {
        None
    } else {
        Some(group_instance_id.as_str())
    };

    if version >= 2 {
        out.put_i32(0); // throttle
    }
    out.put_i16(KafkaErrorCode::None.as_i16());
    out.put_i32(result.generation as i32);
    put_string(out, &selected_protocol);
    put_string(out, &leader);
    put_string(out, &result.member_id);
    if result.member_id == leader {
        out.put_i32(members_snap.len() as i32);
        for m in &members_snap {
            put_string(out, &m.member_id);
            if version >= 5 {
                // Best-effort: only the joining static member knows its instance id.
                if m.member_id == result.member_id {
                    put_nullable_string(out, self_instance);
                } else if let Some(inst) = m.member_id.strip_prefix("static:") {
                    put_nullable_string(out, Some(inst));
                } else {
                    put_nullable_string(out, None);
                }
            }
            put_bytes(out, Some(&[]));
        }
    } else {
        out.put_i32(0);
    }
}

fn encode_sync_group(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // SyncGroup classic v0–3:
    //   group_id, generation, member_id, group_instance_id (v3+), [assignments]
    // Response: throttle (v1+), error, assignment bytes
    let fail = |out: &mut BytesMut, err: i16| {
        if version >= 1 {
            out.put_i32(0);
        }
        out.put_i16(err);
        put_bytes(out, Some(&[]));
    };

    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            fail(out, KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    if src.remaining() < 4 {
        fail(out, KafkaErrorCode::InvalidRequest.as_i16());
        return;
    }
    let generation = src.get_i32() as u32;
    let mut member_id = match get_string(src) {
        Ok(m) => m,
        Err(_) => {
            fail(out, KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    if version >= 3 {
        let instance = get_nullable_string(src).ok().flatten().unwrap_or_default();
        if member_id.is_empty() && !instance.is_empty() {
            member_id = static_member_id(&instance);
        }
    }
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
        fail(out, KafkaErrorCode::GroupAuthorizationFailed.as_i16());
        return;
    }

    let hb = broker.groups().heartbeat(&group_id, &member_id, generation);
    if hb.error_code != 0 {
        fail(out, map_group_error(hb.error_code));
        return;
    }

    let assignment = broker
        .groups()
        .assignment(&group_id, &member_id)
        .unwrap_or_default();
    let bytes = encode_consumer_assignment(&assignment);
    if version >= 1 {
        out.put_i32(0); // throttle
    }
    out.put_i16(KafkaErrorCode::None.as_i16());
    put_bytes(out, Some(&bytes));
}

fn encode_heartbeat(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // Heartbeat classic v0–3: group_id, generation, member_id, group_instance_id (v3+)
    // Response: throttle (v1+), error
    let fail = |out: &mut BytesMut, err: i16| {
        if version >= 1 {
            out.put_i32(0);
        }
        out.put_i16(err);
    };

    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            fail(out, KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    if src.remaining() < 4 {
        fail(out, KafkaErrorCode::InvalidRequest.as_i16());
        return;
    }
    let generation = src.get_i32() as u32;
    let mut member_id = match get_string(src) {
        Ok(m) => m,
        Err(_) => {
            fail(out, KafkaErrorCode::InvalidRequest.as_i16());
            return;
        }
    };
    if version >= 3 {
        let instance = get_nullable_string(src).ok().flatten().unwrap_or_default();
        if member_id.is_empty() && !instance.is_empty() {
            member_id = static_member_id(&instance);
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
        fail(out, KafkaErrorCode::GroupAuthorizationFailed.as_i16());
        return;
    }

    let result = broker.groups().heartbeat(&group_id, &member_id, generation);
    if version >= 1 {
        out.put_i32(0); // throttle
    }
    out.put_i16(map_group_error(result.error_code));
}

fn encode_leave_group(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // LeaveGroup classic v0–3:
    //   v0–2: group_id, member_id
    //   v3: group_id, members[{ member_id, group_instance_id }]
    // Response: throttle (v1+), error, members[] (v3+)
    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            if version >= 1 {
                out.put_i32(0);
            }
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            if version >= 3 {
                out.put_i32(0);
            }
            return;
        }
    };

    // Collect members to leave: (member_id, optional instance_id for response).
    let mut to_leave: Vec<(String, Option<String>)> = Vec::new();
    if version >= 3 {
        if src.remaining() < 4 {
            if version >= 1 {
                out.put_i32(0);
            }
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            out.put_i32(0);
            return;
        }
        let n = src.get_i32();
        for _ in 0..n.max(0) {
            let mid = match get_string(src) {
                Ok(m) => m,
                Err(_) => break,
            };
            let instance = get_nullable_string(src).ok().flatten();
            let resolved = if !mid.is_empty() {
                mid
            } else if let Some(ref inst) = instance {
                if inst.is_empty() {
                    continue;
                }
                static_member_id(inst)
            } else {
                continue;
            };
            to_leave.push((resolved, instance));
        }
    } else {
        let member_id = match get_string(src) {
            Ok(m) => m,
            Err(_) => {
                if version >= 1 {
                    out.put_i32(0);
                }
                out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
                return;
            }
        };
        to_leave.push((member_id, None));
    }

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Group,
            &group_id,
            AclOperation::Read,
        )
    {
        if version >= 1 {
            out.put_i32(0);
        }
        out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
        if version >= 3 {
            out.put_i32(to_leave.len() as i32);
            for (mid, inst) in &to_leave {
                put_string(out, mid);
                put_nullable_string(out, inst.as_deref());
                out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
            }
        }
        return;
    }

    let mut member_results: Vec<(String, Option<String>, i16)> = Vec::new();
    let mut top_err = KafkaErrorCode::None.as_i16();
    for (mid, inst) in to_leave {
        let result = broker
            .groups()
            .leave(&group_id, &mid, |t| broker.partition_count_opt(t));
        let err = map_group_error(result.error_code);
        if err != 0 && top_err == 0 {
            top_err = err;
        }
        member_results.push((mid, inst, err));
    }

    if version >= 1 {
        out.put_i32(0); // throttle
    }
    out.put_i16(top_err);
    if version >= 3 {
        out.put_i32(member_results.len() as i32);
        for (mid, inst, err) in member_results {
            put_string(out, &mid);
            put_nullable_string(out, inst.as_deref());
            out.put_i16(err);
        }
    }
}

fn encode_offset_commit(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // OffsetCommit classic v0–7 (flexible 8+):
    //   v0: group_id, [topic [partition, offset, metadata]]
    //   v1: + generation, member_id; partition commit_timestamp
    //   v2–4: + retention_time_ms (no commit_timestamp)
    //   v5: no retention_time
    //   v6+: + committed_leader_epoch per partition (ignored; not stored)
    //   v7+: + group_instance_id (nullable; maps to static: when member_id empty)
    // Response: throttle_time_ms (v3+), [topic [partition, error]]
    let empty = |out: &mut BytesMut, version: i16| {
        if version >= 3 {
            out.put_i32(0); // throttle
        }
        out.put_i32(0);
    };

    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            empty(out, version);
            return;
        }
    };

    let mut generation: u32 = 0;
    let mut member_id = String::new();
    if version >= 1 {
        if src.remaining() < 4 {
            empty(out, version);
            return;
        }
        generation = src.get_i32() as u32;
        member_id = match get_string(src) {
            Ok(m) => m,
            Err(_) => {
                empty(out, version);
                return;
            }
        };
    }
    // v7+: group_instance_id (nullable). Prefer member_id when set; otherwise
    // derive static:{instance} like JoinGroup / Heartbeat.
    if version >= 7 {
        match get_nullable_string(src) {
            Ok(Some(inst)) if member_id.is_empty() && !inst.is_empty() => {
                member_id = static_member_id(&inst);
            }
            Ok(_) => {}
            Err(_) => {
                empty(out, version);
                return;
            }
        }
    }
    // Retention only on v2–4 (ignored — broker-controlled retention).
    if (2..=4).contains(&version) {
        if src.remaining() < 8 {
            empty(out, version);
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
        empty(out, version);
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
            // v6+: committed_leader_epoch (not stored; OffsetFetch returns -1).
            if version >= 6 {
                if src.remaining() < 4 {
                    break;
                }
                let _leader_epoch = src.get_i32();
            }
            // v1 only: commit_timestamp
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

    if version >= 3 {
        out.put_i32(0); // throttle
    }
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

fn encode_offset_fetch(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // OffsetFetch classic v0–5:
    //   group_id, topics nullable array (v2+: null=all, empty=none; v0–1: empty=all)
    // Response: throttle (v3+), topics[{ name, partitions[{ partition, offset,
    //   committed_leader_epoch (v5+), metadata, error }] }], top-level error (v2+)
    let write_partition = |out: &mut BytesMut, partition: i32, offset: i64, meta: &str| {
        out.put_i32(partition);
        out.put_i64(offset);
        if version >= 5 {
            out.put_i32(-1); // committed_leader_epoch unknown
        }
        put_string(out, meta);
        out.put_i16(KafkaErrorCode::None.as_i16());
    };

    let finish = |out: &mut BytesMut, top_error: i16| {
        if version >= 2 {
            out.put_i16(top_error);
        }
    };

    let group_id = match get_string(src) {
        Ok(g) => g,
        Err(_) => {
            if version >= 3 {
                out.put_i32(0);
            }
            out.put_i32(0);
            finish(out, KafkaErrorCode::InvalidRequest.as_i16());
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
        if version >= 3 {
            out.put_i32(0); // throttle
        }
        out.put_i32(0); // empty topics
        // v0–1: empty topics only; v2+: GroupAuthorizationFailed
        finish(
            out,
            if version >= 2 {
                KafkaErrorCode::GroupAuthorizationFailed.as_i16()
            } else {
                0
            },
        );
        return;
    }

    if src.remaining() < 4 {
        if version >= 3 {
            out.put_i32(0);
        }
        out.put_i32(0);
        finish(out, KafkaErrorCode::None.as_i16());
        return;
    }
    let topic_count = src.get_i32();

    // Topics array semantics:
    //   v0–1: count <= 0 → all (legacy empty-as-all)
    //   v2+:  count < 0 (null) → all; count == 0 → none; count > 0 → listed
    let list_all = if version >= 2 {
        topic_count < 0
    } else {
        topic_count <= 0
    };
    let list_none = version >= 2 && topic_count == 0;

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

    if version >= 3 {
        out.put_i32(0); // throttle_time_ms
    }

    if list_none {
        out.put_i32(0);
        finish(out, KafkaErrorCode::None.as_i16());
        return;
    }

    // Empty query when list_all → fetch_all inside group coordinator.
    let fetched = match broker.groups().fetch_offsets(
        &group_id,
        if list_all { &[] } else { &query },
    ) {
        Ok(r) => r.entries,
        Err(_) => Vec::new(),
    };

    if list_all {
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
                write_partition(out, p as i32, off, &meta);
            }
        }
        finish(out, KafkaErrorCode::None.as_i16());
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
            write_partition(out, p, off, &meta);
        }
    }
    finish(out, KafkaErrorCode::None.as_i16());
}

fn encode_list_groups(broker: &Broker, out: &mut BytesMut, version: i16, principal: &str) {
    // ListGroups classic v0–2:
    //   request: empty
    //   response: throttle_time_ms (v1+), error_code, [group_id, protocol_type]
    if version >= 1 {
        out.put_i32(0); // throttle
    }
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

fn encode_describe_groups(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // DescribeGroups classic v0–4:
    //   request: [group_id], include_authorized_operations (v3+)
    //   response: throttle (v1+), [error, group_id, state, protocol_type, protocol,
    //             members[{member_id, group_instance_id (v4+), client_id, client_host,
    //             metadata, assignment}], authorized_operations (v3+)]
    if src.remaining() < 4 {
        if version >= 1 {
            out.put_i32(0);
        }
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
    let include_ops = if version >= 3 && src.remaining() >= 1 {
        src.get_u8() != 0
    } else {
        false
    };

    if version >= 1 {
        out.put_i32(0); // throttle
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
            if version >= 3 {
                out.put_i32(group_authorized_ops(broker, principal, &group_id, include_ops));
            }
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
                    if version >= 4 {
                        // Derive instance id from Phase 12 static: prefix.
                        if let Some(inst) = m.member_id.strip_prefix("static:") {
                            put_nullable_string(out, Some(inst));
                        } else {
                            put_nullable_string(out, None);
                        }
                    }
                    put_string(out, "volant-kafka"); // client_id
                    put_string(out, "/"); // client_host
                    let topics: Vec<&str> = m.topics.iter().map(|s| s.as_str()).collect();
                    let meta = super::codec::encode_consumer_subscription(&topics);
                    put_bytes(out, Some(&meta));
                    let asg = encode_consumer_assignment(&m.assignment);
                    put_bytes(out, Some(&asg));
                }
                if version >= 3 {
                    out.put_i32(group_authorized_ops(broker, principal, &group_id, include_ops));
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
                if version >= 3 {
                    out.put_i32(group_authorized_ops(broker, principal, &group_id, include_ops));
                }
            }
        }
    }
}

/// Kafka authorized-operations bitfield for a consumer group (DescribeGroups v3+).
fn group_authorized_ops(broker: &Broker, principal: &str, group_id: &str, include: bool) -> i32 {
    if !include {
        return AUTH_OPS_OMITTED;
    }
    let ops = [
        AclOperation::Read,
        AclOperation::Delete,
        AclOperation::Describe,
    ];
    let mut bits = 0i32;
    for op in ops {
        let allowed = if broker.acls().is_enabled() {
            broker
                .acls()
                .authorize(Some(principal), ResourceType::Group, group_id, op)
        } else {
            true
        };
        if allowed {
            bits |= 1i32 << (volant_op_to_kafka(op) as u32);
        }
    }
    bits
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

fn encode_delete_groups(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    _version: i16,
    principal: &str,
) {
    // DeleteGroups classic v0–1:
    //   request: [group_id]
    //   response: throttle_time_ms (all versions), [group_id, error_code]
    // Kafka includes throttle from v0; Phase 43 corrects the earlier missing field.
    if src.remaining() < 4 {
        out.put_i32(0); // throttle
        out.put_i32(0); // results
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
    out.put_i32(0); // throttle
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

fn encode_create_partitions(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    _version: i16,
    principal: &str,
) {
    // CreatePartitions classic v0–1 (flexible 2+):
    //   request: [topic, count, assignments|null], timeout, validate_only
    //   response: throttle (all versions), [name, error, error_message]
    // Phase 45 adds missing throttle framing (Kafka has throttle on v0+).
    if src.remaining() < 4 {
        out.put_i32(0); // throttle
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
    let validate_only = if src.remaining() >= 1 {
        src.get_u8() != 0
    } else {
        false
    };

    out.put_i32(0); // throttle
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
        if validate_only {
            // Dry-run: topic must exist and count must be a valid increase.
            let meta = broker.metadata(Some(&[TopicName::new(r.topic.clone())]));
            if meta.topics.is_empty() {
                out.put_i16(KafkaErrorCode::UnknownTopicOrPartition.as_i16());
                put_nullable_string(out, Some("topic not found"));
            } else {
                let cur = meta.topics[0].partitions.len() as i32;
                if r.count < cur {
                    out.put_i16(KafkaErrorCode::InvalidPartitions.as_i16());
                    put_nullable_string(out, Some("partition count must not decrease"));
                } else {
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    put_nullable_string(out, None);
                }
            }
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

/// Kafka `DescribeConfigsResponse.ConfigSource` ids (classic).
const CFG_SRC_TOPIC: i8 = 1;
const CFG_SRC_DEFAULT: i8 = 5;
/// Kafka `DescribeConfigsResponse.ConfigType` ids.
const CFG_TYPE_STRING: i8 = 2;
const CFG_TYPE_LONG: i8 = 5;

fn config_type_for_key(key: &str) -> i8 {
    match key {
        "retention.ms" | "retention.bytes" | "segment.bytes" => CFG_TYPE_LONG,
        _ => CFG_TYPE_STRING,
    }
}

fn config_documentation(key: &str) -> Option<&'static str> {
    match key {
        "retention.ms" => Some("Log retention time in milliseconds"),
        "retention.bytes" => Some("Log retention size in bytes"),
        "segment.bytes" => Some("Segment roll size in bytes"),
        "cleanup.policy" => Some("delete | compact"),
        _ => None,
    }
}

fn encode_describe_configs(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // DescribeConfigs classic v0–3 (flexible 4+):
    //   request: resources[], include_synonyms (v1+), include_documentation (v3+)
    //   response: throttle (all versions),
    //     [error, error_message, resource_type, resource_name, configs[…]]
    //   config entry: name, value, read_only,
    //     is_default (v0) | config_source (v1+), is_sensitive,
    //     synonyms (v1+), config_type + documentation (v3+)
    // Phase 46: leading throttle + Kafka field order (error_message before type/name).
    if src.remaining() < 4 {
        out.put_i32(0); // throttle
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
    // include_synonyms (v1+): parsed for wire compatibility; we always emit an
    // empty synonyms list (no layered broker-default store).
    if version >= 1 && src.remaining() >= 1 {
        let _include_synonyms = src.get_u8() != 0;
    }
    let include_docs = if version >= 3 && src.remaining() >= 1 {
        src.get_u8() != 0
    } else {
        false
    };

    out.put_i32(0); // throttle
    out.put_i32(resources.len() as i32);
    for r in resources {
        // Kafka field order: error, error_message, resource_type, resource_name, configs
        let write_header =
            |out: &mut BytesMut, code: KafkaErrorCode, msg: Option<&str>, rtype: i8, name: &str| {
                out.put_i16(code.as_i16());
                put_nullable_string(out, msg);
                out.put_i8(rtype);
                put_string(out, name);
            };

        // resource_type 2 = TOPIC
        if r.rtype != 2 {
            write_header(
                out,
                KafkaErrorCode::InvalidRequest,
                Some("only TOPIC resources supported"),
                r.rtype,
                &r.name,
            );
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
            write_header(
                out,
                KafkaErrorCode::TopicAuthorizationFailed,
                None,
                r.rtype,
                &r.name,
            );
            out.put_i32(0);
            continue;
        }
        match broker.describe_configs(&r.name) {
            Ok((_id, _pc, cfg)) => {
                let mut entries = cfg.to_entries();
                if let Some(filter) = &r.keys {
                    entries.retain(|(k, _)| filter.iter().any(|f| f == k));
                }
                write_header(out, KafkaErrorCode::None, None, r.rtype, &r.name);
                out.put_i32(entries.len() as i32);
                for (k, v) in entries {
                    let is_default = v.is_empty();
                    put_string(out, &k);
                    if is_default {
                        put_nullable_string(out, None);
                    } else {
                        put_nullable_string(out, Some(&v));
                    }
                    out.put_u8(0); // read_only
                    if version == 0 {
                        out.put_u8(if is_default { 1 } else { 0 }); // is_default
                    } else {
                        // config_source
                        out.put_i8(if is_default {
                            CFG_SRC_DEFAULT
                        } else {
                            CFG_SRC_TOPIC
                        });
                    }
                    out.put_u8(0); // is_sensitive
                    if version >= 1 {
                        // synonyms: empty (no layered broker defaults)
                        out.put_i32(0);
                    }
                    if version >= 3 {
                        out.put_i8(config_type_for_key(&k));
                        if include_docs {
                            put_nullable_string(out, config_documentation(&k));
                        } else {
                            put_nullable_string(out, None);
                        }
                    }
                }
            }
            Err(Error::NotFound(_)) => {
                write_header(
                    out,
                    KafkaErrorCode::UnknownTopicOrPartition,
                    Some("topic not found"),
                    r.rtype,
                    &r.name,
                );
                out.put_i32(0);
            }
            Err(_) => {
                write_header(out, KafkaErrorCode::Unknown, None, r.rtype, &r.name);
                out.put_i32(0);
            }
        }
    }
}

fn encode_alter_configs(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    _version: i16,
    principal: &str,
) {
    // AlterConfigs classic v0–1 (flexible 2+):
    //   request: [resource_type, resource_name, [name, value]] validate_only
    //   response: throttle (all versions), [error, error_message, type, name]
    // Phase 46: leading throttle (Kafka has throttle on v0+).
    if src.remaining() < 4 {
        out.put_i32(0); // throttle
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
            let v = match get_nullable_string(src) {
                Ok(Some(s)) => s,
                Ok(None) => String::new(),
                Err(_) => String::new(),
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

    out.put_i32(0); // throttle
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

/// IncrementalAlterConfigs (API 44) v0 — Phase 37.
///
/// Kafka `ConfigOperation`: 0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT.
/// Volant topic configs only support SET and DELETE (clear via empty value).
fn encode_incremental_alter_configs(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    out.put_i32(0); // throttle_time_ms

    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let n = src.get_i32();

    /// Kafka ConfigOperation::Set.
    const OP_SET: i8 = 0;
    /// Kafka ConfigOperation::Delete.
    const OP_DELETE: i8 = 1;

    struct Res {
        rtype: i8,
        name: String,
        /// Flattened SET/DELETE entries for Volant (`""` value = clear).
        entries: Vec<(String, String)>,
        /// Parse-time error for this resource (if any).
        parse_err: Option<String>,
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
        let cfg_count = src.get_i32();
        let mut entries = Vec::new();
        let mut parse_err = None;
        for _ in 0..cfg_count.max(0) {
            // Always drain the entry fields so subsequent resources / validate_only
            // stay aligned even after a parse error on an earlier op.
            let key = match get_string(src) {
                Ok(s) => s,
                Err(_) => {
                    if parse_err.is_none() {
                        parse_err = Some("invalid config name".into());
                    }
                    break;
                }
            };
            if src.remaining() < 1 {
                if parse_err.is_none() {
                    parse_err = Some("truncated config operation".into());
                }
                break;
            }
            let op = src.get_i8();
            let value = match get_nullable_string(src) {
                Ok(v) => v.unwrap_or_default(),
                Err(_) => {
                    if parse_err.is_none() {
                        parse_err = Some("invalid config value".into());
                    }
                    break;
                }
            };
            if parse_err.is_some() {
                continue;
            }
            match op {
                OP_SET => entries.push((key, value)),
                OP_DELETE => entries.push((key, String::new())),
                2 | 3 => {
                    parse_err = Some(
                        "APPEND/SUBTRACT not supported (no list-typed topic configs)".into(),
                    );
                }
                other => {
                    parse_err = Some(format!("unknown config operation {other}"));
                }
            }
        }
        resources.push(Res {
            rtype,
            name,
            entries,
            parse_err,
        });
    }
    let validate_only = if src.remaining() >= 1 {
        src.get_u8() != 0
    } else {
        false
    };

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
        if let Some(msg) = r.parse_err {
            out.put_i16(KafkaErrorCode::InvalidConfig.as_i16());
            put_nullable_string(out, Some(&msg));
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
