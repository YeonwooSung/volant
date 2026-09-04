//! Kafka connection accept loop, dispatch, and SASL handlers.

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};
use volant_core::{Error, Result};

use crate::broker::Broker;

use super::codec::{
    decode_request_header, encode_response_frame, get_bytes, get_compact_bytes, get_string,
    put_bytes, put_compact_bytes, put_compact_nullable_string, put_empty_tag_buffer,
    put_nullable_string, put_response_header, put_response_header_v1, put_string, skip_tag_buffer,
    try_decode_request,
};
use super::sasl::{self, SaslMechanism, SaslState, MECHANISMS};
use super::txn;
use super::{acl_api, admin_api, group_api, meta_api, produce_fetch};
use super::{ApiKey, KafkaErrorCode, KAFKA_ANONYMOUS_PRINCIPAL};

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

/// Accept Kafka-protocol connections until the listener fails fatally or a
/// process shutdown signal arrives (Phase 109).
///
/// In-flight connection tasks are aborted with a bounded drain timeout.
pub async fn serve_kafka_listener(listener: TcpListener, broker: Arc<Broker>) -> Result<()> {
    serve_kafka_listener_until(listener, broker, crate::net::shutdown_signal()).await
}

/// Like [`serve_kafka_listener`], but stops when `shutdown` completes (Phase 109).
pub async fn serve_kafka_listener_until<F>(
    listener: TcpListener,
    broker: Arc<Broker>,
    shutdown: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()>,
{
    if let Ok(local) = listener.local_addr() {
        info!(%local, "volant kafka shim listening");
    }
    tokio::pin!(shutdown);
    let mut conns: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    loop {
        conns.retain(|h| !h.is_finished());
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received; draining kafka accept loop");
                break;
            }
            acc = listener.accept() => {
                match acc {
                    Ok((stream, peer)) => {
                        broker.metrics().record_connection();
                        debug!(%peer, "kafka connection accepted");
                        let b = Arc::clone(&broker);
                        conns.push(tokio::spawn(async move {
                            if let Err(e) = handle_kafka_connection(stream, b).await {
                                debug!(%peer, error = %e, "kafka connection closed");
                            }
                        }));
                    }
                    Err(e) => {
                        error!(error = %e, "kafka accept failed");
                        drain_kafka_conns(conns).await;
                        return Err(Error::Io(e));
                    }
                }
            }
        }
    }
    drain_kafka_conns(conns).await;
    Ok(())
}

/// Bounded connection abort (mirrors native/metrics drain in `net.rs`).
async fn drain_kafka_conns(handles: Vec<tokio::task::JoinHandle<()>>) {
    if handles.is_empty() {
        return;
    }
    for h in &handles {
        h.abort();
    }
    let join_all = async {
        for h in handles {
            let _ = h.await;
        }
    };
    if tokio::time::timeout(std::time::Duration::from_secs(2), join_all)
        .await
        .is_err()
    {
        tracing::warn!("kafka connection drain timed out");
    }
}

async fn handle_kafka_connection(mut stream: TcpStream, broker: Arc<Broker>) -> Result<()> {
    let mut buf = BytesMut::with_capacity(8 * 1024);
    let mut conn = KafkaConnState::default();
    loop {
        loop {
            match try_decode_request(&mut buf)? {
                Some(body) => {
                    let response = dispatch_kafka(&broker, body, &mut conn).await;
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

async fn dispatch_kafka(
    broker: &Arc<Broker>,
    body: bytes::Bytes,
    conn: &mut KafkaConnState,
) -> BytesMut {
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
    let api = ApiKey::from_i16(hdr.api_key);

    // Flexible APIs (except ApiVersions) use response header v1: corr + TAG_BUFFER.
    // ApiVersions always uses response header v0 even when the body is flexible.
    let flexible_response_header = matches!(
        (api, hdr.api_version),
        (Some(ApiKey::Metadata), v) if v >= 9
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::FindCoordinator), v) if v >= 3
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::Produce), v) if v >= 9
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::Fetch), v) if v >= 12
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::JoinGroup), v) if v >= 6
    ) || matches!(
        (api, hdr.api_version),
        (
            Some(ApiKey::SyncGroup)
                | Some(ApiKey::Heartbeat)
                | Some(ApiKey::LeaveGroup),
            v
        ) if v >= 4
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::OffsetCommit), v) if v >= 8
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::OffsetFetch), v) if v >= 6
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::DescribeGroups), v) if v >= 5
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::ListGroups), v) if v >= 3
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::DeleteGroups), v) if v >= 2
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::CreateTopics), v) if v >= 5
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::DeleteTopics), v) if v >= 4
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::CreatePartitions), v) if v >= 2
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::DescribeConfigs), v) if v >= 4
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::AlterConfigs), v) if v >= 2
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::IncrementalAlterConfigs), v) if v >= 1
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::InitProducerId), v) if v >= 2
    ) || matches!(
        (api, hdr.api_version),
        (
            Some(ApiKey::AddPartitionsToTxn)
                | Some(ApiKey::AddOffsetsToTxn)
                | Some(ApiKey::EndTxn)
                | Some(ApiKey::TxnOffsetCommit),
            v
        ) if v >= 3
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::ListOffsets), v) if v >= 6
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::OffsetForLeaderEpoch), v) if v >= 4
    ) || matches!(
        (api, hdr.api_version),
        (
            Some(ApiKey::DeleteRecords)
                | Some(ApiKey::DescribeAcls)
                | Some(ApiKey::CreateAcls)
                | Some(ApiKey::DeleteAcls),
            v
        ) if v >= 2
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::SaslAuthenticate), v) if v >= 2
    ) || matches!(
        (api, hdr.api_version),
        (
            Some(ApiKey::DescribeLogDirs)
                | Some(ApiKey::AlterReplicaLogDirs)
                | Some(ApiKey::WriteTxnMarkers),
            v
        ) if v >= 1
    ) || matches!(
        (api, hdr.api_version),
        (
            Some(ApiKey::DescribeCluster)
                | Some(ApiKey::DescribeProducers)
                | Some(ApiKey::DescribeTransactions)
                | Some(ApiKey::ListTransactions)
                | Some(ApiKey::AlterPartitionReassignments)
                | Some(ApiKey::ListPartitionReassignments)
                | Some(ApiKey::DescribeUserScramCredentials)
                | Some(ApiKey::AlterUserScramCredentials)
                | Some(ApiKey::DescribeClientQuotas)
                | Some(ApiKey::AlterClientQuotas)
                | Some(ApiKey::ListClientMetricsResources)
                | Some(ApiKey::DescribeTopicPartitions)
                | Some(ApiKey::BrokerRegistration)
                | Some(ApiKey::BrokerHeartbeat)
                | Some(ApiKey::UnregisterBroker)
                | Some(ApiKey::UpdateFeatures)
                | Some(ApiKey::Envelope)
                | Some(ApiKey::FetchSnapshot)
                | Some(ApiKey::DescribeQuorum)
                | Some(ApiKey::AllocateProducerIds)
                | Some(ApiKey::AssignReplicasToDirs)
                | Some(ApiKey::GetTelemetrySubscriptions)
                | Some(ApiKey::PushTelemetry)
                | Some(ApiKey::AlterPartition)
                | Some(ApiKey::CreateDelegationToken)
                | Some(ApiKey::RenewDelegationToken)
                | Some(ApiKey::ExpireDelegationToken)
                | Some(ApiKey::DescribeDelegationToken)
                | Some(ApiKey::ConsumerGroupHeartbeat)
                | Some(ApiKey::ConsumerGroupDescribe)
                | Some(ApiKey::ControllerRegistration),
            _
        )
    ) || matches!(
        (api, hdr.api_version),
        (Some(ApiKey::ElectLeaders), v) if v >= 1
    );

    let mut out = BytesMut::new();
    if flexible_response_header {
        put_response_header_v1(&mut out, corr);
    } else {
        put_response_header(&mut out, corr);
    }

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
        Some(ApiKey::ApiVersions) if (0..=5).contains(&hdr.api_version) => {
            // Flexible request header (v3+): classic ClientId + header TAG_BUFFER.
            // Response header stays v0 even for flexible body (Kafka special case).
            if hdr.api_version >= 3 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "api versions flexible header tag buffer");
                }
            }
            meta_api::encode_api_versions(&mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::SaslHandshake) if (0..=1).contains(&hdr.api_version) => {
            encode_sasl_handshake(&mut src, &mut out, conn);
        }
        Some(ApiKey::SaslAuthenticate) if (0..=2).contains(&hdr.api_version) => {
            if hdr.api_version >= 2 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "sasl authenticate flexible header tag buffer");
                }
            }
            encode_sasl_authenticate(broker, &mut src, &mut out, hdr.api_version, conn);
        }
        Some(ApiKey::DescribeCluster) if (0..=2).contains(&hdr.api_version) => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "describe cluster flexible header tag buffer");
            }
            meta_api::encode_describe_cluster(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
        }
        Some(ApiKey::DescribeProducers) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "describe producers flexible header tag buffer");
            }
            meta_api::encode_describe_producers(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::DescribeTransactions) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "describe transactions flexible header tag buffer");
            }
            meta_api::encode_describe_transactions(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::ListTransactions) if (0..=2).contains(&hdr.api_version) => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "list transactions flexible header tag buffer");
            }
            meta_api::encode_list_transactions(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
        }
        Some(ApiKey::Metadata) if (0..=13).contains(&hdr.api_version) => {
            if hdr.api_version >= 9 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "metadata flexible header tag buffer");
                }
            }
            meta_api::encode_metadata(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::DescribeTopicPartitions) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "describe topic partitions flexible header tag buffer");
            }
            meta_api::encode_describe_topic_partitions(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::Produce) if (0..=13).contains(&hdr.api_version) => {
            if hdr.api_version >= 9 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "produce flexible header tag buffer");
                }
            }
            produce_fetch::encode_produce(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::Fetch) if (0..=18).contains(&hdr.api_version) => {
            if hdr.api_version >= 12 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "fetch flexible header tag buffer");
                }
            }
            // Phase 119: transparent forward when session lives on a peer.
            // Phase 138: promote-from-mirror on owner miss (inside maybe_forward).
            if let Some(body) = crate::net::maybe_forward_kafka_fetch(
                broker.as_ref(),
                hdr.api_version,
                principal,
                src.as_ref(),
            )
            .await
            {
                out.extend_from_slice(&body);
            } else {
                produce_fetch::encode_fetch(broker, &mut src, &mut out, hdr.api_version, principal);
                // Phase 138: best-effort session mirror fan-out after local mutations.
                crate::net::schedule_session_mirror_fanout(broker);
            }
        }
        Some(ApiKey::ListOffsets) if (0..=11).contains(&hdr.api_version) => {
            if hdr.api_version >= 6 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "list offsets flexible header tag buffer");
                }
            }
            produce_fetch::encode_list_offsets(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
        }
        Some(ApiKey::CreateTopics) if (0..=7).contains(&hdr.api_version) => {
            if hdr.api_version >= 5 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "create topics flexible header tag buffer");
                }
            }
            admin_api::encode_create_topics(broker, &mut src, &mut out, hdr.api_version, principal)
                .await;
        }
        Some(ApiKey::DeleteTopics) if (0..=6).contains(&hdr.api_version) => {
            if hdr.api_version >= 4 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "delete topics flexible header tag buffer");
                }
            }
            admin_api::encode_delete_topics(broker, &mut src, &mut out, hdr.api_version, principal)
                .await;
        }
        Some(ApiKey::DeleteRecords) if (0..=2).contains(&hdr.api_version) => {
            if hdr.api_version >= 2 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "delete records flexible header tag buffer");
                }
            }
            // v2 request-level tag 0 = wait_majority u8; v0–1 stay flag 0.
            let flag = if hdr.api_version >= 2 {
                acl_api::peek_delete_records_wait_flag(src.clone())
            } else {
                0
            };
            let wait = broker.effective_delete_records_wait_majority(flag);
            let fanouts = acl_api::encode_delete_records(
                broker.as_ref(),
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
                wait,
            )
            .await;
            if !wait {
                for (topic, partition, before_offset) in fanouts {
                    let b = Arc::clone(broker);
                    tokio::spawn(async move {
                        let _ =
                            crate::net::fanout_delete_records(&b, &topic, partition, before_offset)
                                .await;
                    });
                }
            }
        }
        Some(ApiKey::DescribeAcls) if (0..=3).contains(&hdr.api_version) => {
            if hdr.api_version >= 2 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "describe acls flexible header tag buffer");
                }
            }
            acl_api::encode_describe_acls(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::CreateAcls) if (0..=3).contains(&hdr.api_version) => {
            if hdr.api_version >= 2 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "create acls flexible header tag buffer");
                }
            }
            if let Some(gen) =
                acl_api::encode_create_acls(broker, &mut src, &mut out, hdr.api_version, principal)
            {
                let b = Arc::clone(broker);
                tokio::spawn(async move {
                    crate::net::fanout_cluster_acl_snapshot(&b, gen).await;
                });
            }
        }
        Some(ApiKey::DeleteAcls) if (0..=3).contains(&hdr.api_version) => {
            if hdr.api_version >= 2 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "delete acls flexible header tag buffer");
                }
            }
            if let Some(gen) =
                acl_api::encode_delete_acls(broker, &mut src, &mut out, hdr.api_version, principal)
            {
                let b = Arc::clone(broker);
                tokio::spawn(async move {
                    crate::net::fanout_cluster_acl_snapshot(&b, gen).await;
                });
            }
        }
        Some(ApiKey::FindCoordinator) if (0..=6).contains(&hdr.api_version) => {
            if hdr.api_version >= 3 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "find coordinator flexible header tag buffer");
                }
            }
            meta_api::encode_find_coordinator(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::AddPartitionsToTxn) if (0..=5).contains(&hdr.api_version) => {
            if hdr.api_version >= 3 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "add partitions to txn flexible header tag buffer");
                }
            }
            if let Some(fanout) = txn::encode_add_partitions_to_txn(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            ) {
                // Phase 114: best-effort open fan-out so partition leaders can
                // accept write-through produce (await so tests see ready peers).
                let _ = crate::net::run_txn_2pc_fanout(broker.as_ref(), &fanout).await;
            }
        }
        Some(ApiKey::AddOffsetsToTxn) if (0..=4).contains(&hdr.api_version) => {
            if hdr.api_version >= 3 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "add offsets to txn flexible header tag buffer");
                }
            }
            // Phase 122: transparent forward when this node is not the txn coordinator.
            if let Some(body) = crate::net::maybe_forward_kafka_txn(
                broker.as_ref(),
                25, // AddOffsetsToTxn
                hdr.api_version,
                principal,
                src.as_ref(),
            )
            .await
            {
                out.extend_from_slice(&body);
            } else {
                txn::encode_add_offsets_to_txn(
                    broker,
                    &mut src,
                    &mut out,
                    hdr.api_version,
                    principal,
                );
            }
        }
        Some(ApiKey::EndTxn) if (0..=5).contains(&hdr.api_version) => {
            if hdr.api_version >= 3 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "end txn flexible header tag buffer");
                }
            }
            // Phase 120: transparent forward when this node is not the txn coordinator.
            if let Some(body) = crate::net::maybe_forward_kafka_txn(
                broker.as_ref(),
                26, // EndTxn
                hdr.api_version,
                principal,
                src.as_ref(),
            )
            .await
            {
                out.extend_from_slice(&body);
            } else {
                // Snapshot header framing so we can rewrite body if prepare fan-out fails.
                let body_start = out.len();
                if let Some(fanout) =
                    txn::encode_end_txn(broker, &mut src, &mut out, hdr.api_version, principal)
                {
                    use crate::broker::Txn2pcFanout;
                    match &fanout {
                        Txn2pcFanout::Prepare {
                            transactional_id, ..
                        } => {
                            if !crate::net::run_txn_2pc_fanout(broker.as_ref(), &fanout).await {
                                broker.rollback_local_prepare(transactional_id);
                                // Rewrite body as Unknown after response header.
                                out.truncate(body_start);
                                out.put_i32(0); // throttle
                                out.put_i16(KafkaErrorCode::Unknown.as_i16());
                                if hdr.api_version >= 5 {
                                    out.put_i64(-1);
                                    out.put_i16(-1);
                                }
                                if hdr.api_version >= 3 {
                                    put_empty_tag_buffer(&mut out);
                                }
                            }
                        }
                        Txn2pcFanout::None => {}
                        _ => {
                            let _ = crate::net::run_txn_2pc_fanout(broker.as_ref(), &fanout).await;
                        }
                    }
                }
            }
        }
        Some(ApiKey::WriteTxnMarkers) if (0..=1).contains(&hdr.api_version) => {
            if hdr.api_version >= 1 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "write txn markers flexible header tag buffer");
                }
            }
            txn::encode_write_txn_markers(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::TxnOffsetCommit) if (0..=6).contains(&hdr.api_version) => {
            if hdr.api_version >= 3 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "txn offset commit flexible header tag buffer");
                }
            }
            // Phase 122: transparent forward when this node is not the txn coordinator.
            if let Some(body) = crate::net::maybe_forward_kafka_txn(
                broker.as_ref(),
                28, // TxnOffsetCommit
                hdr.api_version,
                principal,
                src.as_ref(),
            )
            .await
            {
                out.extend_from_slice(&body);
            } else {
                txn::encode_txn_offset_commit(
                    broker,
                    &mut src,
                    &mut out,
                    hdr.api_version,
                    principal,
                );
            }
        }
        Some(ApiKey::JoinGroup) if (0..=9).contains(&hdr.api_version) => {
            if hdr.api_version >= 6 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "join group flexible header tag buffer");
                }
            }
            group_api::encode_join_group(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::SyncGroup) if (0..=5).contains(&hdr.api_version) => {
            if hdr.api_version >= 4 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "sync group flexible header tag buffer");
                }
            }
            group_api::encode_sync_group(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::Heartbeat) if (0..=4).contains(&hdr.api_version) => {
            if hdr.api_version >= 4 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "heartbeat flexible header tag buffer");
                }
            }
            group_api::encode_heartbeat(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::LeaveGroup) if (0..=5).contains(&hdr.api_version) => {
            if hdr.api_version >= 4 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "leave group flexible header tag buffer");
                }
            }
            group_api::encode_leave_group(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::OffsetCommit) if (0..=10).contains(&hdr.api_version) => {
            if hdr.api_version >= 8 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "offset commit flexible header tag buffer");
                }
            }
            group_api::encode_offset_commit(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::OffsetFetch) if (0..=10).contains(&hdr.api_version) => {
            if hdr.api_version >= 6 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "offset fetch flexible header tag buffer");
                }
            }
            group_api::encode_offset_fetch(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::DescribeGroups) if (0..=6).contains(&hdr.api_version) => {
            if hdr.api_version >= 5 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "describe groups flexible header tag buffer");
                }
            }
            group_api::encode_describe_groups(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
        }
        Some(ApiKey::ConsumerGroupHeartbeat) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "consumer group heartbeat flexible header tag buffer");
            }
            group_api::encode_consumer_group_heartbeat(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::ConsumerGroupDescribe) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "consumer group describe flexible header tag buffer");
            }
            group_api::encode_consumer_group_describe(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::ListGroups) if (0..=5).contains(&hdr.api_version) => {
            if hdr.api_version >= 3 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "list groups flexible header tag buffer");
                }
            }
            group_api::encode_list_groups(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::DeleteGroups) if (0..=3).contains(&hdr.api_version) => {
            if hdr.api_version >= 2 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "delete groups flexible header tag buffer");
                }
            }
            group_api::encode_delete_groups(broker, &mut src, &mut out, hdr.api_version, principal);
        }
        Some(ApiKey::OffsetDelete) if hdr.api_version == 0 => {
            group_api::encode_offset_delete(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::CreateDelegationToken) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "create delegation token flexible header tag buffer");
            }
            admin_api::encode_create_delegation_token(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::RenewDelegationToken) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "renew delegation token flexible header tag buffer");
            }
            admin_api::encode_renew_delegation_token(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::ExpireDelegationToken) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "expire delegation token flexible header tag buffer");
            }
            admin_api::encode_expire_delegation_token(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::CreatePartitions) if (0..=3).contains(&hdr.api_version) => {
            if hdr.api_version >= 2 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "create partitions flexible header tag buffer");
                }
            }
            admin_api::encode_create_partitions(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            )
            .await;
        }
        Some(ApiKey::ElectLeaders) if (0..=1).contains(&hdr.api_version) => {
            if hdr.api_version >= 1 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "elect leaders flexible header tag buffer");
                }
            }
            admin_api::encode_elect_leaders(broker, &mut src, &mut out, hdr.api_version, principal)
                .await;
        }
        Some(ApiKey::AlterPartitionReassignments) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "alter partition reassignments flexible header tag buffer");
            }
            admin_api::encode_alter_partition_reassignments(broker, &mut src, &mut out, principal)
                .await;
        }
        Some(ApiKey::ListPartitionReassignments) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "list partition reassignments flexible header tag buffer");
            }
            admin_api::encode_list_partition_reassignments(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::BrokerRegistration) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "broker registration flexible header tag buffer");
            }
            admin_api::encode_broker_registration(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::BrokerHeartbeat) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "broker heartbeat flexible header tag buffer");
            }
            admin_api::encode_broker_heartbeat(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::ControllerRegistration) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "controller registration flexible header tag buffer");
            }
            admin_api::encode_controller_registration(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::UnregisterBroker) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "unregister broker flexible header tag buffer");
            }
            admin_api::encode_unregister_broker(broker, &mut src, &mut out, principal).await;
        }
        Some(ApiKey::DescribeUserScramCredentials) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "describe user scram credentials flexible header tag buffer");
            }
            admin_api::encode_describe_user_scram_credentials(
                broker, &mut src, &mut out, principal,
            );
        }
        Some(ApiKey::AlterUserScramCredentials) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "alter user scram credentials flexible header tag buffer");
            }
            admin_api::encode_alter_user_scram_credentials(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::DescribeClientQuotas) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "describe client quotas flexible header tag buffer");
            }
            admin_api::encode_describe_client_quotas(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::AlterClientQuotas) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "alter client quotas flexible header tag buffer");
            }
            admin_api::encode_alter_client_quotas(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::ListClientMetricsResources) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "list client metrics resources flexible header tag buffer");
            }
            admin_api::encode_list_client_metrics_resources(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::UpdateFeatures) if (0..=1).contains(&hdr.api_version) => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "update features flexible header tag buffer");
            }
            admin_api::encode_update_features(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
        }
        Some(ApiKey::Envelope) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "envelope flexible header tag buffer");
            }
            admin_api::encode_envelope(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::FetchSnapshot) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "fetch snapshot flexible header tag buffer");
            }
            admin_api::encode_fetch_snapshot(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::DescribeQuorum) if (0..=1).contains(&hdr.api_version) => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "describe quorum flexible header tag buffer");
            }
            admin_api::encode_describe_quorum(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
        }
        Some(ApiKey::AllocateProducerIds) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "allocate producer ids flexible header tag buffer");
            }
            admin_api::encode_allocate_producer_ids(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::AlterPartition) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "alter partition flexible header tag buffer");
            }
            admin_api::encode_alter_partition(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::AssignReplicasToDirs) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "assign replicas to dirs flexible header tag buffer");
            }
            admin_api::encode_assign_replicas_to_dirs(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::GetTelemetrySubscriptions) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "get telemetry subscriptions flexible header tag buffer");
            }
            admin_api::encode_get_telemetry_subscriptions(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::PushTelemetry) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "push telemetry flexible header tag buffer");
            }
            admin_api::encode_push_telemetry(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::DescribeDelegationToken) if hdr.api_version == 0 => {
            if let Err(e) = skip_tag_buffer(&mut src) {
                debug!(error = %e, "describe delegation token flexible header tag buffer");
            }
            admin_api::encode_describe_delegation_token(broker, &mut src, &mut out, principal);
        }
        Some(ApiKey::AlterReplicaLogDirs) if (0..=1).contains(&hdr.api_version) => {
            if hdr.api_version >= 1 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "alter replica log dirs flexible header tag buffer");
                }
            }
            admin_api::encode_alter_replica_log_dirs(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
        }
        Some(ApiKey::DescribeLogDirs) if (0..=1).contains(&hdr.api_version) => {
            if hdr.api_version >= 1 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "describe log dirs flexible header tag buffer");
                }
            }
            admin_api::encode_describe_log_dirs(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
        }
        Some(ApiKey::DescribeConfigs) if (0..=4).contains(&hdr.api_version) => {
            if hdr.api_version >= 4 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "describe configs flexible header tag buffer");
                }
            }
            admin_api::encode_describe_configs(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
        }
        Some(ApiKey::AlterConfigs) if (0..=2).contains(&hdr.api_version) => {
            if hdr.api_version >= 2 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "alter configs flexible header tag buffer");
                }
            }
            let fanouts = admin_api::encode_alter_configs(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
            // Phase 113: best-effort BROKER config fan-out after controller Alter.
            for (generation, entries) in fanouts {
                let b = Arc::clone(broker);
                tokio::spawn(async move {
                    crate::net::fanout_cluster_broker_config(&b, generation, &entries).await;
                });
            }
        }
        Some(ApiKey::IncrementalAlterConfigs) if (0..=1).contains(&hdr.api_version) => {
            if hdr.api_version >= 1 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "incremental alter configs flexible header tag buffer");
                }
            }
            let fanouts = admin_api::encode_incremental_alter_configs(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
            for (generation, entries) in fanouts {
                let b = Arc::clone(broker);
                tokio::spawn(async move {
                    crate::net::fanout_cluster_broker_config(&b, generation, &entries).await;
                });
            }
        }
        Some(ApiKey::InitProducerId) if (0..=6).contains(&hdr.api_version) => {
            if hdr.api_version >= 2 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "init producer id flexible header tag buffer");
                }
            }
            // Phase 120: successful transactional Init → register coordinator on peers.
            if let Some(fanout) =
                txn::encode_init_producer_id(broker, &mut src, &mut out, hdr.api_version, principal)
            {
                let _ = crate::net::run_txn_2pc_fanout(broker.as_ref(), &fanout).await;
            }
        }
        Some(ApiKey::OffsetForLeaderEpoch) if (0..=4).contains(&hdr.api_version) => {
            if hdr.api_version >= 4 {
                if let Err(e) = skip_tag_buffer(&mut src) {
                    debug!(error = %e, "offset for leader epoch flexible header tag buffer");
                }
            }
            produce_fetch::encode_offset_for_leader_epoch(
                broker,
                &mut src,
                &mut out,
                hdr.api_version,
                principal,
            );
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
    let flexible = version >= 2;

    let auth_bytes = if flexible {
        match get_compact_bytes(src) {
            Ok(b) => {
                let _ = skip_tag_buffer(src);
                b.unwrap_or_default()
            }
            Err(_) => {
                out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
                put_compact_nullable_string(out, Some("truncated auth bytes"));
                put_compact_bytes(out, None);
                out.put_i64(0); // session_lifetime_ms (v1+)
                put_empty_tag_buffer(out);
                return;
            }
        }
    } else {
        match get_bytes(src) {
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
        }
    };

    let step = match sasl::authenticate_step(broker, &mut conn.sasl, &auth_bytes) {
        Ok(s) => s,
        Err(e) => {
            out.put_i16(KafkaErrorCode::SaslAuthenticationFailed.as_i16());
            if flexible {
                put_compact_nullable_string(out, Some(&e.to_string()));
                put_compact_bytes(out, None);
                out.put_i64(0);
                put_empty_tag_buffer(out);
            } else {
                put_nullable_string(out, Some(&e.to_string()));
                put_bytes(out, None);
                if version >= 1 {
                    out.put_i64(0);
                }
            }
            return;
        }
    };

    if step.failed {
        out.put_i16(KafkaErrorCode::SaslAuthenticationFailed.as_i16());
        if flexible {
            put_compact_nullable_string(out, step.error_message.as_deref());
            put_compact_bytes(out, Some(&step.auth_bytes));
        } else {
            put_nullable_string(out, step.error_message.as_deref());
            put_bytes(out, Some(&step.auth_bytes));
        }
    } else {
        if let Some(p) = step.principal {
            conn.principal = Some(p);
        }
        out.put_i16(KafkaErrorCode::None.as_i16());
        if flexible {
            put_compact_nullable_string(out, None);
            put_compact_bytes(out, Some(&step.auth_bytes));
        } else {
            put_nullable_string(out, None);
            put_bytes(out, Some(&step.auth_bytes));
        }
    }
    if version >= 1 {
        out.put_i64(0); // session_lifetime_ms
    }
    if flexible {
        put_empty_tag_buffer(out);
    }
}
