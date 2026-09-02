//! Request authorization, dispatch, and opcode handlers.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tracing::{info_span, warn, Instrument};
use volant_core::{Error, Message, MessageBatch, Offset, PartitionId, Result, TopicName};
use volant_protocol::{
    decode_request, Assignment, BrokerInfo, ErrorCode, FetchRecord, Frame, OffsetFetchEntry,
    PartitionInfo, Request, Response, TopicInfo,
};

use crate::broker::{Broker, MembershipOverlaySnapshot, Txn2pcFanout};
use crate::cluster::MetadataLogEntry;

use super::fanout::{
    complete_assignment_mutation, fanout_assignment_consensus, fanout_cluster_acl_snapshot,
    fanout_delete_records, fanout_delete_records_replicas_only, fanout_membership_put,
    fanout_metadata_raft_append, fanout_truncate_journal_note_provisional,
    metadata_entry_from_wire, put_end_txn_error_response, run_txn_2pc_fanout,
    schedule_catch_up_peer_admin_state, schedule_catch_up_peer_truncate_journal,
    schedule_isr_update_reports, schedule_session_mirror_fanout, snapshot_if_must_wait,
};
use super::inter_broker_rpc;

/// Dispatch one framed request with connection auth / SCRAM state (plaintext + TLS).
pub async fn dispatch_with_auth(
    broker: &Arc<Broker>,
    frame: Frame,
    authenticated: &mut bool,
    principal: &mut Option<String>,
    scram_challenge: &mut Option<crate::scram::ScramChallenge>,
) -> Response {
    let req = match decode_request(frame.header.opcode, &frame.payload) {
        Ok(r) => r,
        Err(e) => {
            broker.metrics().record_error(ErrorCode::Protocol as u16);
            return Response::Error {
                code: ErrorCode::Protocol as u16,
                message: e.to_string(),
            };
        }
    };

    // Shared-token Auth (Phase 7).
    if let Request::Auth { token } = &req {
        let response = match broker.auth_token() {
            None => {
                // Auth disabled: accept any token as a no-op success.
                *authenticated = true;
                *principal = Some(broker.auth_principal_name());
                Response::Auth { error_code: 0 }
            }
            Some(expected) if expected == *token => {
                *authenticated = true;
                *principal = Some(broker.auth_principal_name());
                Response::Auth { error_code: 0 }
            }
            Some(_) => {
                *authenticated = false;
                *principal = None;
                broker
                    .metrics()
                    .record_error(ErrorCode::AuthenticationFailed as u16);
                Response::Auth {
                    error_code: ErrorCode::AuthenticationFailed as u16,
                }
            }
        };
        return response;
    }

    // SCRAM-SHA-256 (Phase 22) — allowed before authentication.
    if matches!(
        &req,
        Request::ScramFirst { .. } | Request::ScramFinal { .. }
    ) {
        return handle_scram(broker, req, authenticated, principal, scram_challenge);
    }

    // Bootstrap CreateScramUser when the store is empty (no auth yet).
    if matches!(&req, Request::CreateScramUser { .. }) && !broker.scram().has_users() {
        return dispatch_request_as(broker, req, principal.as_deref()).await;
    }

    let auth_required = broker.auth_required();
    if auth_required && !*authenticated {
        broker
            .metrics()
            .record_error(ErrorCode::AuthenticationRequired as u16);
        return Response::Error {
            code: ErrorCode::AuthenticationRequired as u16,
            message: "authentication required; send Auth or ScramFirst/ScramFinal first".into(),
        };
    }

    dispatch_request_as(broker, req, principal.as_deref()).await
}

fn handle_scram(
    broker: &Broker,
    req: Request,
    authenticated: &mut bool,
    principal: &mut Option<String>,
    scram_challenge: &mut Option<crate::scram::ScramChallenge>,
) -> Response {
    match req {
        Request::ScramFirst {
            username,
            client_nonce,
        } => match broker.scram().begin(&username, &client_nonce) {
            Ok((chal, salt, iterations, combined_nonce)) => {
                *scram_challenge = Some(chal);
                Response::ScramFirst {
                    error_code: 0,
                    combined_nonce,
                    salt: bytes::Bytes::from(salt),
                    iterations,
                }
            }
            Err(_) => {
                *scram_challenge = None;
                Response::ScramFirst {
                    error_code: ErrorCode::InvalidArg as u16,
                    combined_nonce: String::new(),
                    salt: bytes::Bytes::new(),
                    iterations: 0,
                }
            }
        },
        Request::ScramFinal {
            username,
            combined_nonce,
            client_proof,
        } => {
            let Some(chal) = scram_challenge.take() else {
                broker
                    .metrics()
                    .record_error(ErrorCode::AuthenticationFailed as u16);
                return Response::ScramFinal {
                    error_code: ErrorCode::AuthenticationFailed as u16,
                    server_signature: bytes::Bytes::new(),
                };
            };
            match broker
                .scram()
                .finish(&chal, &username, &combined_nonce, &client_proof)
            {
                Ok(server_sig) => {
                    *authenticated = true;
                    *principal = Some(username);
                    Response::ScramFinal {
                        error_code: 0,
                        server_signature: bytes::Bytes::from(server_sig),
                    }
                }
                Err(_) => {
                    *authenticated = false;
                    *principal = None;
                    broker
                        .metrics()
                        .record_error(ErrorCode::AuthenticationFailed as u16);
                    Response::ScramFinal {
                        error_code: ErrorCode::AuthenticationFailed as u16,
                        server_signature: bytes::Bytes::new(),
                    }
                }
            }
        }
        _ => Response::Error {
            code: ErrorCode::Protocol as u16,
            message: "internal scram dispatch error".into(),
        },
    }
}

/// Handle a decoded request (shared by plaintext and TLS accept paths).
pub async fn dispatch_request(broker: &Arc<Broker>, req: Request) -> Response {
    dispatch_request_as(broker, req, None).await
}

/// Dispatch with an optional connection principal for ACL checks (Phase 20).
pub async fn dispatch_request_as(
    broker: &Arc<Broker>,
    req: Request,
    principal: Option<&str>,
) -> Response {
    if let Some(denied) = authorize_request(broker, &req, principal) {
        broker
            .metrics()
            .record_error(ErrorCode::AuthorizationFailed as u16);
        return denied;
    }
    match handle_request(broker, req).await {
        Ok(resp) => {
            record_response_metrics(broker, &resp);
            resp
        }
        Err(e) => {
            let resp = map_error(e);
            record_response_metrics(broker, &resp);
            resp
        }
    }
}

fn deny(msg: impl Into<String>) -> Response {
    Response::Error {
        code: ErrorCode::AuthorizationFailed as u16,
        message: msg.into(),
    }
}

/// Return an AuthorizationFailed response if the principal may not run `req`.
fn authorize_request(broker: &Broker, req: &Request, principal: Option<&str>) -> Option<Response> {
    use crate::acl::{AclOperation, ResourceType, CLUSTER_RESOURCE};

    // Inter-broker traffic and auth handshakes are not ACL-gated.
    // TruncateJournal* is *not* in this list: the journal survives leadership
    // and is Cluster-Alter gated when ACLs are enabled (inter-broker auth
    // principal still allowed).
    match req {
        Request::ReplicaFetch { .. }
        | Request::HeartbeatBroker { .. }
        | Request::ClusterState { .. }
        | Request::ReplicaDeleteRecords { .. }
        | Request::ClusterBrokerConfig { .. }
        | Request::ClusterAclSnapshot { .. }
        | Request::TxnParticipantOpen { .. }
        | Request::TxnParticipantPrepare { .. }
        | Request::TxnParticipantComplete { .. }
        | Request::KafkaFetchForward { .. }
        | Request::KafkaTxnForward { .. }
        | Request::FetchSessionMirrorPut { .. }
        | Request::FetchSessionMirrorDelete { .. }
        | Request::IsrUpdate { .. }
        | Request::AssignmentConsensusNote { .. }
        | Request::MetadataRaftAppend { .. }
        | Request::MembershipPut { .. }
        | Request::OpenraftAppend { .. }
        | Request::OpenraftVote { .. }
        | Request::OpenraftInstallSnapshot { .. }
        | Request::Auth { .. }
        | Request::ScramFirst { .. }
        | Request::ScramFinal { .. } => return None,
        _ => {}
    }

    if !broker.acls().is_enabled() {
        return None;
    }

    let acls = broker.acls();
    let check = |rt: ResourceType, resource: &str, op: AclOperation| -> bool {
        acls.authorize(principal, rt, resource, op)
    };

    let ok = match req {
        Request::Produce { topic, .. } => check(ResourceType::Topic, topic, AclOperation::Write),
        Request::Fetch { topic, .. } | Request::ListOffsets { topic, .. } => {
            check(ResourceType::Topic, topic, AclOperation::Read)
        }
        Request::CreateTopic { name, .. } => check(ResourceType::Topic, name, AclOperation::Create),
        Request::DeleteTopic { name } | Request::DeleteRecords { topic: name, .. } => {
            check(ResourceType::Topic, name, AclOperation::Delete)
        }
        Request::Metadata { topics } => {
            if topics.is_empty() {
                check(
                    ResourceType::Cluster,
                    CLUSTER_RESOURCE,
                    AclOperation::Describe,
                )
            } else {
                topics
                    .iter()
                    .all(|t| check(ResourceType::Topic, t, AclOperation::Describe))
            }
        }
        Request::DescribeConfigs { topic } => {
            check(ResourceType::Topic, topic, AclOperation::Describe)
        }
        Request::AlterConfigs { topic, .. } | Request::CreatePartitions { topic, .. } => {
            check(ResourceType::Topic, topic, AclOperation::Alter)
        }
        Request::OffsetCommit { group_id, .. }
        | Request::OffsetFetch { group_id, .. }
        | Request::JoinGroup { group_id, .. }
        | Request::Heartbeat { group_id, .. }
        | Request::LeaveGroup { group_id, .. } => {
            check(ResourceType::Group, group_id, AclOperation::Read)
        }
        Request::DescribeGroup { group_id } => {
            check(ResourceType::Group, group_id, AclOperation::Describe)
        }
        Request::DeleteOffsets { group_id, .. } => {
            check(ResourceType::Group, group_id, AclOperation::Delete)
        }
        Request::ListGroups => check(
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        ),
        Request::InitProducerId { .. } | Request::BeginTxn { .. } | Request::EndTxn { .. } => {
            check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Write)
        }
        Request::CreateAcls { .. }
        | Request::DeleteAcls { .. }
        | Request::AddBroker { .. }
        | Request::RemoveBroker { .. }
        | Request::ReassignPartitions { .. } => {
            check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Alter)
        }
        Request::ListAcls { .. } | Request::ListMembers => check(
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        ),
        Request::CreateScramUser { .. } | Request::DeleteScramUser { .. } => {
            check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Alter)
        }
        Request::ListScramUsers => check(
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        ),
        // Journal RPCs survive leadership: require Cluster Alter, or allow the
        // configured inter-broker auth principal (token Auth) so fan-out works.
        Request::TruncateJournalNote { .. } | Request::TruncateJournalPush { .. } => {
            let ib = broker.auth_principal_name();
            principal.map(|p| p == ib.as_str()).unwrap_or(false)
                || check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Alter)
        }
        Request::ReplicaFetch { .. }
        | Request::HeartbeatBroker { .. }
        | Request::ClusterState { .. }
        | Request::ReplicaDeleteRecords { .. }
        | Request::ClusterBrokerConfig { .. }
        | Request::ClusterAclSnapshot { .. }
        | Request::TxnParticipantOpen { .. }
        | Request::TxnParticipantPrepare { .. }
        | Request::TxnParticipantComplete { .. }
        | Request::KafkaFetchForward { .. }
        | Request::KafkaTxnForward { .. }
        | Request::FetchSessionMirrorPut { .. }
        | Request::FetchSessionMirrorDelete { .. }
        | Request::IsrUpdate { .. }
        | Request::AssignmentConsensusNote { .. }
        | Request::MetadataRaftAppend { .. }
        | Request::MembershipPut { .. }
        | Request::OpenraftAppend { .. }
        | Request::OpenraftVote { .. }
        | Request::OpenraftInstallSnapshot { .. }
        | Request::Auth { .. }
        | Request::ScramFirst { .. }
        | Request::ScramFinal { .. } => true,
    };

    if ok {
        None
    } else {
        Some(deny(format!(
            "principal '{}' not authorized",
            principal.unwrap_or("")
        )))
    }
}

fn record_response_metrics(broker: &Broker, resp: &Response) {
    let m = broker.metrics();
    match resp {
        Response::Produce {
            count, error_code, ..
        } => {
            let ok = *error_code == 0;
            // Approximate bytes not available here; count messages only.
            m.record_produce(ok, u64::from(*count), 0);
            if !ok {
                m.record_error(*error_code);
            }
        }
        Response::Fetch {
            records,
            error_code,
            ..
        } => {
            let ok = *error_code == 0;
            let messages = records.len() as u64;
            let bytes: u64 = records.iter().map(|r| r.value.len() as u64).sum();
            m.record_fetch(ok, messages, bytes);
            if !ok {
                m.record_error(*error_code);
            }
        }
        Response::Error { code, .. } => {
            m.record_error(*code);
        }
        Response::CreateTopic { error_code, .. }
        | Response::DeleteTopic { error_code, .. }
        | Response::OffsetCommit { error_code }
        | Response::OffsetFetch { error_code, .. }
        | Response::JoinGroup { error_code, .. }
        | Response::Heartbeat { error_code }
        | Response::LeaveGroup { error_code }
        | Response::ReplicaFetch { error_code, .. }
        | Response::HeartbeatBroker { error_code, .. }
        | Response::ClusterState { error_code, .. }
        | Response::Auth { error_code }
        | Response::InitProducerId { error_code, .. }
        | Response::DescribeGroup { error_code, .. }
        | Response::ListGroups { error_code, .. }
        | Response::DeleteOffsets { error_code, .. }
        | Response::DescribeConfigs { error_code, .. }
        | Response::AlterConfigs { error_code, .. }
        | Response::DeleteRecords { error_code, .. }
        | Response::CreatePartitions { error_code, .. }
        | Response::ListOffsets { error_code, .. }
        | Response::BeginTxn { error_code }
        | Response::EndTxn { error_code, .. }
        | Response::CreateAcls { error_code }
        | Response::DeleteAcls { error_code, .. }
        | Response::ListAcls { error_code, .. }
        | Response::ScramFirst { error_code, .. }
        | Response::ScramFinal { error_code, .. }
        | Response::CreateScramUser { error_code }
        | Response::DeleteScramUser { error_code }
        | Response::ListScramUsers { error_code, .. }
        | Response::ReplicaDeleteRecords { error_code, .. }
        | Response::ClusterBrokerConfig { error_code, .. }
        | Response::ClusterAclSnapshot { error_code, .. }
        | Response::TxnParticipantOpen { error_code }
        | Response::TxnParticipantPrepare { error_code }
        | Response::TxnParticipantComplete { error_code }
        | Response::KafkaFetchForward { error_code, .. }
        | Response::KafkaTxnForward { error_code, .. }
        | Response::TruncateJournalNote { error_code, .. }
        | Response::TruncateJournalPush { error_code }
        | Response::FetchSessionMirrorPut { error_code }
        | Response::FetchSessionMirrorDelete { error_code }
        | Response::IsrUpdate { error_code, .. }
        | Response::AssignmentConsensusNote { error_code, .. }
        | Response::MembershipPut { error_code, .. }
        | Response::AddBroker { error_code, .. }
        | Response::RemoveBroker { error_code, .. }
        | Response::ListMembers { error_code, .. }
        | Response::ReassignPartitions { error_code, .. } => {
            if *error_code != 0 {
                m.record_error(*error_code);
            }
        }
        Response::MetadataRaftAppend { success, .. } => {
            if *success == 0 {
                m.record_error(ErrorCode::Unknown as u16);
            }
        }
        Response::OpenraftAppend { .. }
        | Response::OpenraftVote { .. }
        | Response::OpenraftInstallSnapshot { .. } => {}
        Response::Metadata { .. } => {}
    }
}

async fn handle_request(broker: &Arc<Broker>, req: Request) -> Result<Response> {
    match req {
        Request::Auth { .. } => {
            // Handled in dispatch_with_auth; should not reach here.
            Ok(Response::Auth { error_code: 0 })
        }
        Request::CreateTopic {
            name,
            partitions,
            configs,
        } => {
            if broker.cluster_config().is_some() && !broker.is_controller() {
                return Ok(Response::Error {
                    code: ErrorCode::NotController as u16,
                    message: format!("not controller; controller_id={}", broker.controller_id()),
                });
            }
            let topic = TopicName::new(name.clone());
            let prev = snapshot_if_must_wait(broker);
            match broker.create_topic_with_configs(topic, partitions, &configs) {
                Ok(id) => {
                    // Phase 150: best-effort (or wait) assignment majority.
                    if !complete_assignment_mutation(broker, prev).await? {
                        return Ok(Response::Error {
                            code: ErrorCode::NotEnoughReplicas as u16,
                            message: format!(
                                "assignment consensus majority failed for create topic {name}"
                            ),
                        });
                    }
                    Ok(Response::CreateTopic {
                        topic_id: id.0,
                        name,
                        partitions,
                        error_code: 0,
                    })
                }
                Err(e) => {
                    // Surface NotController-style messages.
                    if e.to_string().contains("not controller") {
                        Ok(Response::Error {
                            code: ErrorCode::NotController as u16,
                            message: e.to_string(),
                        })
                    } else {
                        Err(e)
                    }
                }
            }
        }
        Request::DeleteTopic { name } => {
            let topic = TopicName::new(name.clone());
            let prev = snapshot_if_must_wait(broker);
            broker.delete_topic(&topic)?;
            if !complete_assignment_mutation(broker, prev).await? {
                return Ok(Response::Error {
                    code: ErrorCode::NotEnoughReplicas as u16,
                    message: format!(
                        "assignment consensus majority failed for delete topic {name}"
                    ),
                });
            }
            Ok(Response::DeleteTopic {
                name,
                error_code: 0,
            })
        }
        Request::Metadata { topics } => {
            let filter: Option<Vec<TopicName>> = if topics.is_empty() {
                None
            } else {
                Some(topics.into_iter().map(TopicName::new).collect())
            };
            let snap = broker.metadata(filter.as_deref());
            Ok(Response::Metadata {
                brokers: snap
                    .brokers
                    .into_iter()
                    .map(|(node_id, host, port)| BrokerInfo {
                        node_id,
                        host,
                        port,
                    })
                    .collect(),
                topics: snap
                    .topics
                    .into_iter()
                    .map(|t| TopicInfo {
                        name: t.name.0,
                        topic_id: t.topic_id.0,
                        error_code: 0,
                        partitions: t
                            .partitions
                            .into_iter()
                            .map(|p| PartitionInfo {
                                partition_id: p.partition_id.0,
                                leader: p.leader,
                                hwm: p.hwm,
                                replicas: p.replicas,
                                isr: p.isr,
                                leader_epoch: p.leader_epoch,
                            })
                            .collect(),
                    })
                    .collect(),
            })
        }
        Request::Produce {
            topic,
            partition,
            acks,
            messages,
            producer_id,
            producer_epoch,
            base_sequence,
        } => {
            let span = info_span!("produce", topic = %topic, partition, msg_count = messages.len());
            async {
                let topic_name = TopicName::new(topic.clone());
                if messages.is_empty() {
                    return Err(Error::InvalidArgument("empty produce batch".into()));
                }

                let approx_bytes: u64 = messages.iter().map(|m| m.value.len() as u64).sum();
                let msg_count = messages.len() as u32;

                let pid = if partition < 0 {
                    let key = messages[0].key.as_deref();
                    broker.select_partition(&topic_name, key)?
                } else {
                    PartitionId(partition as u32)
                };

                // Leadership check early for clearer response.
                if broker.cluster_config().is_some()
                    && broker.topics_has_partition(&topic_name, pid)
                    && !broker.is_partition_leader(&topic_name, pid)
                {
                    return Ok(Response::Produce {
                        topic,
                        partition: pid.0,
                        base_offset: 0,
                        count: 0,
                        error_code: ErrorCode::NotLeaderForPartition as u16,
                    });
                }

                // Phase 18/86: transactional produce write-through; LSO holds until EndTxn.
                if producer_id != 0
                    && base_sequence >= 0
                    && broker.is_transactional_producer(producer_id)
                {
                    let mut msgs = Vec::with_capacity(messages.len());
                    for m in messages {
                        let timestamp_ms = if m.timestamp_ms < 0 {
                            None
                        } else {
                            Some(m.timestamp_ms)
                        };
                        msgs.push(Message {
                            key: m.key,
                            value: m.value,
                            timestamp_ms,
                            headers: m.headers,
                        });
                    }
                    match broker.buffer_txn_produce(
                        producer_id,
                        producer_epoch,
                        &topic,
                        pid.0,
                        base_sequence,
                        msgs,
                    ) {
                        crate::broker::IdempotentCheck::Reject { error_code } => {
                            return Ok(Response::Produce {
                                topic,
                                partition: pid.0,
                                base_offset: 0,
                                count: 0,
                                error_code,
                            });
                        }
                        crate::broker::IdempotentCheck::Duplicate { base_offset, count } => {
                            return Ok(Response::Produce {
                                topic,
                                partition: pid.0,
                                base_offset,
                                count,
                                error_code: 0,
                            });
                        }
                        crate::broker::IdempotentCheck::Accept { base_offset } => {
                            return Ok(Response::Produce {
                                topic,
                                partition: pid.0,
                                base_offset,
                                count: msg_count,
                                error_code: 0,
                            });
                        }
                    }
                }

                // Idempotent de-dupe / sequence gate (Phase 10).
                match broker.check_idempotent_produce(
                    producer_id,
                    producer_epoch,
                    &topic,
                    pid.0,
                    base_sequence,
                    msg_count,
                ) {
                    crate::broker::IdempotentCheck::Reject { error_code } => {
                        return Ok(Response::Produce {
                            topic,
                            partition: pid.0,
                            base_offset: 0,
                            count: 0,
                            error_code,
                        });
                    }
                    crate::broker::IdempotentCheck::Duplicate { base_offset, count } => {
                        return Ok(Response::Produce {
                            topic,
                            partition: pid.0,
                            base_offset,
                            count,
                            error_code: 0,
                        });
                    }
                    crate::broker::IdempotentCheck::Accept { .. } => {}
                }

                let mut batch = MessageBatch::default();
                for m in messages {
                    let timestamp_ms = if m.timestamp_ms < 0 {
                        None
                    } else {
                        Some(m.timestamp_ms)
                    };
                    batch.messages.push(Message {
                        key: m.key,
                        value: m.value,
                        timestamp_ms,
                        headers: m.headers,
                    });
                }

                // Append; for acks=all enforce min_isr and wait for HWM asynchronously.
                let (records, error_code) =
                    broker.produce_with_acks(&topic_name, pid, batch, acks, None)?;

                if error_code == ErrorCode::NotLeaderForPartition as u16
                    || error_code == ErrorCode::NotEnoughReplicas as u16
                {
                    return Ok(Response::Produce {
                        topic,
                        partition: pid.0,
                        base_offset: 0,
                        count: 0,
                        error_code,
                    });
                }

                let base_offset = records.first().map(|r| r.offset.raw()).unwrap_or(0);
                let count = records.len() as u32;

                let mut final_error = error_code;
                if acks == 255 && broker.cluster_config().is_some() && count > 0 {
                    let target = base_offset + u64::from(count);
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                    loop {
                        let hwm = broker.committed_hwm(&topic_name, pid).unwrap_or(0);
                        if hwm >= target {
                            break;
                        }
                        if tokio::time::Instant::now() >= deadline {
                            final_error = ErrorCode::Timeout as u16;
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }

                if acks != 0 && !broker.group_commit_enabled() {
                    broker.flush(&topic_name, pid)?;
                }

                if final_error == 0 {
                    broker.metrics().add_produce_bytes(approx_bytes);
                    broker.record_idempotent_produce(
                        producer_id,
                        producer_epoch,
                        &topic,
                        pid.0,
                        base_sequence,
                        count,
                        base_offset,
                    );
                }

                Ok(Response::Produce {
                    topic,
                    partition: pid.0,
                    base_offset,
                    count,
                    error_code: final_error,
                })
            }
            .instrument(span)
            .await
        }

        Request::Fetch {
            topic,
            partition,
            from_offset,
            max_messages,
            max_bytes: _,
            max_wait_ms,
        } => {
            let span = info_span!("fetch", topic = %topic, partition, from_offset);
            async {
                let topic_name = TopicName::new(topic.clone());
                let pid = PartitionId(partition);
                let from = Offset::new(from_offset);
                let max = max_messages as usize;

                // In multi-node, prefer leader for client fetch (followers may have data
                // but HWM is authoritative on leader). Still allow fetch on any replica
                // capped at local committed_hwm.
                let mut records = broker.fetch(&topic_name, pid, from, max)?;
                if records.is_empty() && max_wait_ms > 0 {
                    let deadline =
                        tokio::time::Instant::now() + Duration::from_millis(u64::from(max_wait_ms));
                    while records.is_empty() && tokio::time::Instant::now() < deadline {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        records = broker.fetch(&topic_name, pid, from, max)?;
                    }
                }

                let hwm = broker.high_watermark(&topic_name, pid).unwrap_or(0);
                let wire_records = records
                    .into_iter()
                    .map(|r| FetchRecord {
                        offset: r.offset.raw(),
                        timestamp_ms: r.timestamp_ms,
                        key: r.key,
                        value: r.value,
                        headers: r.headers,
                    })
                    .collect();

                Ok(Response::Fetch {
                    topic,
                    partition,
                    high_watermark: hwm,
                    error_code: 0,
                    records: wire_records,
                })
            }
            .instrument(span)
            .await
        }
        Request::JoinGroup {
            group_id,
            member_id,
            session_timeout_ms,
            topics,
            group_instance_id,
        } => {
            let result = broker.groups().join(
                &group_id,
                &member_id,
                session_timeout_ms,
                topics,
                &group_instance_id,
                |t| broker.partition_count_opt(t),
            )?;
            Ok(Response::JoinGroup {
                error_code: result.error_code,
                generation: result.generation,
                member_id: result.member_id,
                assignment: result
                    .assignment
                    .into_iter()
                    .map(|(topic, partition)| Assignment { topic, partition })
                    .collect(),
                revoked: result
                    .revoked
                    .into_iter()
                    .map(|(topic, partition)| Assignment { topic, partition })
                    .collect(),
            })
        }
        Request::Heartbeat {
            group_id,
            member_id,
            generation,
        } => {
            let result = broker.groups().heartbeat(&group_id, &member_id, generation);
            Ok(Response::Heartbeat {
                error_code: result.error_code,
            })
        }
        Request::LeaveGroup {
            group_id,
            member_id,
        } => {
            let result = broker
                .groups()
                .leave(&group_id, &member_id, |t| broker.partition_count_opt(t));
            Ok(Response::LeaveGroup {
                error_code: result.error_code,
            })
        }
        Request::OffsetCommit {
            group_id,
            member_id,
            generation,
            entries,
        } => {
            let wire: Vec<(String, u32, u64, String)> = entries
                .into_iter()
                .map(|e| (e.topic, e.partition, e.offset, e.metadata))
                .collect();
            let result = broker
                .groups()
                .commit_offsets(&group_id, &member_id, generation, &wire)?;
            Ok(Response::OffsetCommit {
                error_code: result.error_code,
            })
        }
        Request::OffsetFetch { group_id, entries } => {
            let wire: Vec<(String, u32)> = entries
                .into_iter()
                .map(|e| (e.topic, e.partition))
                .collect();
            let result = broker.groups().fetch_offsets(&group_id, &wire)?;
            Ok(Response::OffsetFetch {
                error_code: result.error_code,
                entries: result
                    .entries
                    .into_iter()
                    .map(|e| OffsetFetchEntry {
                        topic: e.topic,
                        partition: e.partition,
                        offset: e.offset,
                        metadata: e.metadata,
                    })
                    .collect(),
            })
        }
        Request::ReplicaFetch {
            topic,
            partition,
            from_offset,
            max_bytes,
            replica_id,
        } => {
            let (error_code, high_watermark, leader_epoch, records) = broker.handle_replica_fetch(
                &topic,
                partition,
                from_offset,
                max_bytes,
                replica_id,
            )?;
            // Phase 142: best-effort leader→controller ISR report after local reconcile.
            if broker.has_pending_isr_reports() {
                schedule_isr_update_reports(broker);
            }
            Ok(Response::ReplicaFetch {
                error_code,
                topic,
                partition,
                high_watermark,
                leader_epoch,
                records,
            })
        }
        Request::HeartbeatBroker {
            broker_id,
            controller_id_known,
            generation,
            applied_config_generation,
            applied_acl_generation,
            applied_journal_generation,
        } => {
            let (error_code, controller_id, generation, alive_brokers) =
                broker.handle_heartbeat_broker(broker_id, controller_id_known, generation);
            // Phase 117 + 136: if we are controller and peer lags on ACL/config
            // gens, re-push SoT state (covers offline miss + rejoin).
            // Phase 136: schedule async (single-flight + min-interval) so the
            // HeartbeatBroker response is not blocked on config/ACL RPCs.
            if error_code == 0 && broker.is_controller() && broker_id != broker.node_id() {
                let (need_cfg, need_acl) =
                    broker.peer_admin_gens_lag(applied_config_generation, applied_acl_generation);
                if need_cfg || need_acl {
                    if let Some(addr) = broker.broker_addr(broker_id) {
                        schedule_catch_up_peer_admin_state(
                            Arc::clone(broker),
                            broker_id,
                            addr,
                            applied_config_generation,
                            applied_acl_generation,
                        );
                    }
                }
            }
            // Phase 131 + 132: any node with a newer truncate journal re-pushes
            // to a lagging peer (multi-controller; not controller-gated).
            // Phase 132: schedule async (single-flight + min-interval) so the
            // HeartbeatBroker response is not blocked on TruncateJournalPush.
            if error_code == 0
                && broker_id != broker.node_id()
                && broker.peer_journal_gen_lags(applied_journal_generation)
            {
                if let Some(addr) = broker.broker_addr(broker_id) {
                    schedule_catch_up_peer_truncate_journal(
                        Arc::clone(broker),
                        broker_id,
                        addr,
                        applied_journal_generation,
                    );
                }
            }
            Ok(Response::HeartbeatBroker {
                error_code,
                controller_id,
                generation,
                alive_brokers,
            })
        }
        Request::ClusterState {
            known_generation: _,
        } => {
            let (error_code, generation, controller_id, topics) = broker.cluster_state_snapshot();
            Ok(Response::ClusterState {
                error_code,
                generation,
                controller_id,
                topics,
            })
        }
        // Phase 113 PR1: decode + dispatch stubs (real fan-out in PR2–PR4).
        Request::ReplicaDeleteRecords {
            topic,
            partition,
            before_offset,
            leader_epoch,
        } => {
            let (error_code, low_watermark) = broker.handle_replica_delete_records(
                &topic,
                partition,
                before_offset,
                leader_epoch,
            );
            Ok(Response::ReplicaDeleteRecords {
                error_code,
                low_watermark,
            })
        }
        Request::ClusterBrokerConfig {
            generation,
            entries,
        } => {
            let (error_code, applied_generation) =
                broker.handle_cluster_broker_config(generation, &entries);
            Ok(Response::ClusterBrokerConfig {
                error_code,
                applied_generation,
            })
        }
        Request::ClusterAclSnapshot {
            generation,
            snapshot,
        } => {
            let (error_code, applied_generation) =
                broker.handle_cluster_acl_snapshot(generation, &snapshot);
            Ok(Response::ClusterAclSnapshot {
                error_code,
                applied_generation,
            })
        }
        Request::TruncateJournalNote {
            topic,
            partition,
            before_offset,
            leader_epoch,
        } => {
            let (error_code, generation) =
                broker.handle_truncate_journal_note(&topic, partition, before_offset, leader_epoch);
            Ok(Response::TruncateJournalNote {
                error_code,
                generation,
            })
        }
        Request::TruncateJournalPush {
            generation,
            snapshot,
        } => {
            let error_code = broker.handle_truncate_journal_push(generation, &snapshot);
            Ok(Response::TruncateJournalPush { error_code })
        }
        Request::InitProducerId { transactional_id } => {
            let (producer_id, epoch) = broker.init_producer_id_with_txn(&transactional_id);
            // Phase 120: register Init owner on peers (no open).
            if !transactional_id.is_empty() {
                let fanout = broker.txn_2pc_init_register_fanout(
                    &transactional_id,
                    producer_id,
                    epoch,
                    false,
                );
                let _ = run_txn_2pc_fanout(broker, &fanout).await;
            }
            Ok(Response::InitProducerId {
                producer_id,
                epoch,
                error_code: 0,
            })
        }
        Request::BeginTxn {
            producer_id,
            producer_epoch,
        } => {
            let error_code = broker.begin_txn(producer_id, producer_epoch);
            if error_code == 0 {
                let fanout = broker.txn_2pc_open_fanout(producer_id);
                let _ = run_txn_2pc_fanout(broker, &fanout).await;
            }
            Ok(Response::BeginTxn { error_code })
        }
        Request::EndTxn {
            producer_id,
            producer_epoch,
            committed,
            offsets,
        } => {
            let offset_tuples: Vec<_> = offsets
                .into_iter()
                .map(|o| (o.group_id, o.topic, o.partition, o.offset, o.metadata))
                .collect();
            let (mut error_code, results, fanout) =
                broker.end_txn(producer_id, producer_epoch, committed, &offset_tuples)?;
            if error_code == 0 {
                match &fanout {
                    Txn2pcFanout::Prepare {
                        transactional_id, ..
                    } => {
                        if !run_txn_2pc_fanout(broker, &fanout).await {
                            broker.rollback_local_prepare(transactional_id);
                            error_code = ErrorCode::Unknown as u16;
                        }
                    }
                    Txn2pcFanout::None => {}
                    _ => {
                        let _ = run_txn_2pc_fanout(broker, &fanout).await;
                    }
                }
            }
            Ok(Response::EndTxn {
                error_code,
                results: results
                    .into_iter()
                    .map(|r| volant_protocol::TxnProduceResult {
                        topic: r.topic,
                        partition: r.partition,
                        base_offset: r.base_offset,
                        count: r.count,
                    })
                    .collect(),
            })
        }
        Request::TxnParticipantOpen {
            transactional_id,
            producer_id,
            producer_epoch,
            enable_2pc,
            coordinator_node_id,
            install_open,
        } => {
            let error_code = broker.handle_txn_participant_open(
                &transactional_id,
                producer_id,
                producer_epoch,
                enable_2pc,
                coordinator_node_id,
                install_open,
            );
            Ok(Response::TxnParticipantOpen { error_code })
        }
        Request::TxnParticipantPrepare {
            transactional_id,
            producer_id,
            producer_epoch,
            commit,
        } => {
            let error_code = broker.handle_txn_participant_prepare(
                &transactional_id,
                producer_id,
                producer_epoch,
                commit,
            );
            Ok(Response::TxnParticipantPrepare { error_code })
        }
        Request::TxnParticipantComplete {
            transactional_id,
            producer_id,
            producer_epoch,
            commit,
        } => {
            let error_code = broker.handle_txn_participant_complete(
                &transactional_id,
                producer_id,
                producer_epoch,
                commit,
            );
            Ok(Response::TxnParticipantComplete { error_code })
        }
        Request::KafkaFetchForward {
            api_version,
            principal,
            body,
        } => {
            // Phase 119: owner-side local encode only (never re-forward).
            let mut src = body;
            let mut out = BytesMut::new();
            crate::kafka::produce_fetch::encode_fetch(
                broker,
                &mut src,
                &mut out,
                api_version,
                &principal,
            );
            // Phase 138: mirror session mutations from owner-side encode.
            schedule_session_mirror_fanout(broker);
            Ok(Response::KafkaFetchForward {
                error_code: 0,
                body: out.freeze(),
            })
        }
        Request::FetchSessionMirrorPut {
            session_id: _,
            snapshot,
        } => {
            // Phase 138: install foreign mirror snapshot (best-effort SoT copy).
            let error_code = match broker.fetch_sessions().apply_mirror_put(&snapshot) {
                Ok(()) => 0,
                Err(e) => {
                    tracing::debug!(error = %e, "fetch session mirror put apply failed");
                    ErrorCode::InvalidArg as u16
                }
            };
            Ok(Response::FetchSessionMirrorPut { error_code })
        }
        Request::FetchSessionMirrorDelete { session_id } => {
            broker.fetch_sessions().apply_mirror_delete(session_id);
            Ok(Response::FetchSessionMirrorDelete { error_code: 0 })
        }
        Request::IsrUpdate {
            topic,
            partition,
            leader_id,
            leader_epoch,
            isr,
            generation_hint,
        } => {
            let (error_code, generation) = broker.apply_leader_isr_update(
                &topic,
                partition,
                leader_id,
                leader_epoch,
                &isr,
                generation_hint,
            );
            // Phase 150/154: best-effort assignment majority after controller ISR bump.
            if error_code == 0 {
                if broker.metadata_raft_enabled() {
                    let _ = fanout_metadata_raft_append(broker).await;
                } else if broker.assignment_consensus_enabled() {
                    let _ = fanout_assignment_consensus(broker).await;
                }
            }
            Ok(Response::IsrUpdate {
                error_code,
                generation,
            })
        }
        Request::AssignmentConsensusNote {
            generation,
            controller_id,
            topics,
        } => {
            let (error_code, acked_gen) =
                broker.handle_assignment_consensus_note(generation, controller_id, &topics);
            Ok(Response::AssignmentConsensusNote {
                error_code,
                generation: acked_gen,
            })
        }
        Request::MetadataRaftAppend {
            leader_id,
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } => {
            let internal: Vec<MetadataLogEntry> =
                entries.iter().map(metadata_entry_from_wire).collect();
            let (resp_term, success, match_index) = broker.handle_metadata_raft_append(
                leader_id,
                term,
                prev_log_index,
                prev_log_term,
                &internal,
                leader_commit,
            );
            Ok(Response::MetadataRaftAppend {
                term: resp_term,
                success: if success { 1 } else { 0 },
                match_index,
            })
        }
        Request::MembershipPut {
            generation,
            brokers,
        } => {
            let endpoints: Vec<crate::cluster::BrokerEndpoint> = brokers
                .into_iter()
                .map(|b| crate::cluster::BrokerEndpoint {
                    id: b.id,
                    host: b.host,
                    port: b.port,
                    rack: b.rack,
                })
                .collect();
            match broker.apply_membership_put(generation, endpoints) {
                Ok(applied_generation) => {
                    // Leader-only; no-op when flag off or voter set already matches.
                    let _ = broker.change_openraft_membership().await;
                    Ok(Response::MembershipPut {
                        error_code: 0,
                        applied_generation,
                    })
                }
                Err(e) => Ok(Response::Error {
                    code: ErrorCode::InvalidArg as u16,
                    message: e.to_string(),
                }),
            }
        }
        Request::AddBroker {
            id,
            host,
            port,
            rack,
        } => {
            if broker.should_forward_membership() {
                return Ok(forward_membership_to_leader(
                    broker,
                    Request::AddBroker {
                        id,
                        host,
                        port,
                        rack,
                    },
                )
                .await);
            }
            let prev = broker.snapshot_membership_overlay();
            match broker.add_broker(id, host, port, rack) {
                Ok(generation) => {
                    let (error_code, generation) =
                        after_overlay_mutation(broker, &prev, generation).await;
                    Ok(Response::AddBroker {
                        error_code,
                        generation,
                    })
                }
                Err(e) => Ok(Response::Error {
                    code: ErrorCode::InvalidArg as u16,
                    message: e.to_string(),
                }),
            }
        }
        Request::RemoveBroker { id } => {
            if broker.should_forward_membership() {
                return Ok(
                    forward_membership_to_leader(broker, Request::RemoveBroker { id }).await,
                );
            }
            let prev = broker.snapshot_membership_overlay();
            match broker.remove_broker(id) {
                Ok(generation) => {
                    let (error_code, generation) =
                        after_overlay_mutation(broker, &prev, generation).await;
                    Ok(Response::RemoveBroker {
                        error_code,
                        generation,
                    })
                }
                Err(e) => Ok(Response::Error {
                    code: ErrorCode::InvalidArg as u16,
                    message: e.to_string(),
                }),
            }
        }
        Request::OpenraftAppend { payload } => {
            let out = broker.handle_openraft_append(&payload).await?;
            Ok(Response::OpenraftAppend { payload: out })
        }
        Request::OpenraftVote { payload } => {
            let out = broker.handle_openraft_vote(&payload).await?;
            Ok(Response::OpenraftVote { payload: out })
        }
        Request::OpenraftInstallSnapshot { payload } => {
            let out = broker.handle_openraft_install_snapshot(&payload).await?;
            Ok(Response::OpenraftInstallSnapshot { payload: out })
        }
        Request::ListMembers => {
            let snap = broker.list_membership();
            Ok(Response::ListMembers {
                error_code: 0,
                generation: snap.generation,
                brokers: snap
                    .brokers
                    .into_iter()
                    .map(|b| volant_protocol::MembershipBroker {
                        id: b.id,
                        host: b.host,
                        port: b.port,
                        rack: b.rack,
                    })
                    .collect(),
                live: snap.live,
            })
        }
        Request::ReassignPartitions {
            topic,
            partition,
            replicas,
        } => {
            if broker.cluster_config().is_some() && !broker.is_controller() {
                return Ok(Response::ReassignPartitions {
                    error_code: ErrorCode::NotController as u16,
                    generation: 0,
                });
            }
            let prev = snapshot_if_must_wait(broker);
            match broker.reassign_partitions(&topic, partition, &replicas) {
                Ok(generation) => {
                    if !complete_assignment_mutation(broker, prev).await? {
                        return Ok(Response::ReassignPartitions {
                            error_code: ErrorCode::NotEnoughReplicas as u16,
                            generation: 0,
                        });
                    }
                    Ok(Response::ReassignPartitions {
                        error_code: 0,
                        generation,
                    })
                }
                Err(Error::NotFound(_)) => Ok(Response::ReassignPartitions {
                    error_code: ErrorCode::NotFound as u16,
                    generation: 0,
                }),
                Err(Error::InvalidArgument(m)) if m.starts_with("not controller") => {
                    Ok(Response::ReassignPartitions {
                        error_code: ErrorCode::NotController as u16,
                        generation: 0,
                    })
                }
                Err(Error::InvalidArgument(_)) => Ok(Response::ReassignPartitions {
                    error_code: ErrorCode::InvalidArg as u16,
                    generation: 0,
                }),
                Err(e) => Err(e),
            }
        }
        Request::KafkaTxnForward {
            api_key,
            api_version,
            principal,
            body,
        } => {
            // Phase 120/122: coordinator-side local encode only (never re-forward).
            // api_key: 25 AddOffsetsToTxn, 26 EndTxn (+ 2PC fan-out), 28 TxnOffsetCommit.
            let mut src = body;
            let mut out = BytesMut::new();
            match api_key {
                25 => {
                    crate::kafka::txn::encode_add_offsets_to_txn(
                        broker,
                        &mut src,
                        &mut out,
                        api_version,
                        &principal,
                    );
                }
                26 => {
                    if let Some(fanout) = crate::kafka::txn::encode_end_txn(
                        broker,
                        &mut src,
                        &mut out,
                        api_version,
                        &principal,
                    ) {
                        use crate::broker::Txn2pcFanout;
                        match &fanout {
                            Txn2pcFanout::Prepare {
                                transactional_id, ..
                            } => {
                                if !run_txn_2pc_fanout(broker, &fanout).await {
                                    broker.rollback_local_prepare(transactional_id);
                                    out.clear();
                                    put_end_txn_error_response(&mut out, api_version, -1);
                                    // Unknown
                                }
                            }
                            Txn2pcFanout::None => {}
                            _ => {
                                let _ = run_txn_2pc_fanout(broker, &fanout).await;
                            }
                        }
                    }
                }
                28 => {
                    crate::kafka::txn::encode_txn_offset_commit(
                        broker,
                        &mut src,
                        &mut out,
                        api_version,
                        &principal,
                    );
                }
                _ => {
                    return Ok(Response::KafkaTxnForward {
                        error_code: ErrorCode::InvalidArg as u16,
                        body: Bytes::new(),
                    });
                }
            }
            Ok(Response::KafkaTxnForward {
                error_code: 0,
                body: out.freeze(),
            })
        }
        Request::DescribeGroup { group_id } => match broker.groups().describe_group(&group_id) {
            Some(desc) => {
                let members = desc
                    .members
                    .into_iter()
                    .map(|m| volant_protocol::GroupMemberInfo {
                        member_id: m.member_id,
                        topics: m.topics,
                        assignment: m
                            .assignment
                            .into_iter()
                            .map(|(topic, partition)| volant_protocol::Assignment {
                                topic,
                                partition,
                            })
                            .collect(),
                    })
                    .collect();
                Ok(Response::DescribeGroup {
                    error_code: 0,
                    group_id: desc.group_id,
                    generation: desc.generation,
                    members,
                })
            }
            None => Ok(Response::DescribeGroup {
                error_code: ErrorCode::NotFound as u16,
                group_id,
                generation: 0,
                members: vec![],
            }),
        },
        Request::ListGroups => {
            let groups = broker
                .groups()
                .list_groups()
                .into_iter()
                .map(|g| volant_protocol::GroupListing {
                    group_id: g.group_id,
                    state: if g.stable {
                        volant_protocol::GroupState::Stable
                    } else {
                        volant_protocol::GroupState::Empty
                    },
                    member_count: g.member_count,
                    generation: g.generation,
                })
                .collect();
            Ok(Response::ListGroups {
                error_code: 0,
                groups,
            })
        }
        Request::DeleteOffsets { group_id, entries } => {
            let pairs: Vec<(String, u32)> = entries
                .into_iter()
                .map(|e| (e.topic, e.partition))
                .collect();
            let deleted_count = broker.groups().delete_offsets(&group_id, &pairs)?;
            Ok(Response::DeleteOffsets {
                error_code: 0,
                deleted_count,
            })
        }
        Request::DescribeConfigs { topic } => match broker.describe_configs(&topic) {
            Ok((topic_id, partition_count, cfg)) => Ok(Response::DescribeConfigs {
                error_code: 0,
                topic,
                topic_id,
                partition_count,
                configs: cfg.to_entries(),
            }),
            Err(Error::NotFound(_)) => Ok(Response::DescribeConfigs {
                error_code: ErrorCode::NotFound as u16,
                topic,
                topic_id: 0,
                partition_count: 0,
                configs: vec![],
            }),
            Err(e) => Err(e),
        },
        Request::AlterConfigs { topic, configs } => match broker.alter_configs(&topic, &configs) {
            Ok(_) => Ok(Response::AlterConfigs {
                error_code: 0,
                topic,
            }),
            Err(Error::NotFound(_)) => Ok(Response::AlterConfigs {
                error_code: ErrorCode::NotFound as u16,
                topic,
            }),
            Err(e) => Err(e),
        },
        Request::DeleteRecords {
            topic,
            partition,
            before_offset,
            wait_majority,
        } => {
            // Phase 137: per-request wait_majority trailer (0=broker, 1/2 force).
            let wait = broker.effective_delete_records_wait_majority(wait_majority);
            if wait {
                // Phase 148: majority-first — do **not** local-truncate until
                // journal majority. Fail → NotEnoughReplicas + current log_start.
                match broker.delete_records_leader_log_start(&topic, partition) {
                    Ok((log_start, pre_err)) if pre_err != 0 => Ok(Response::DeleteRecords {
                        error_code: pre_err,
                        topic,
                        partition,
                        low_watermark: log_start,
                    }),
                    Ok((log_start, _)) => {
                        let note_offset =
                            broker.delete_records_note_offset(&topic, partition, before_offset);
                        let majority_ok = fanout_truncate_journal_note_provisional(
                            broker,
                            &topic,
                            partition,
                            note_offset,
                        )
                        .await;
                        if !majority_ok {
                            broker.note_delete_records_majority_wait_fail();
                            broker.note_delete_records_majority_first_fail();
                            return Ok(Response::DeleteRecords {
                                error_code: ErrorCode::NotEnoughReplicas as u16,
                                topic,
                                partition,
                                low_watermark: log_start,
                            });
                        }
                        match broker.delete_records(&topic, partition, before_offset) {
                            Ok((low_watermark, error_code)) => {
                                if error_code == 0 {
                                    // Journal already majority-noted; replica/outbox only.
                                    fanout_delete_records_replicas_only(
                                        broker,
                                        &topic,
                                        partition,
                                        low_watermark,
                                    )
                                    .await;
                                    broker.note_delete_records_majority_wait_success();
                                    broker.note_delete_records_majority_first_success();
                                }
                                Ok(Response::DeleteRecords {
                                    error_code,
                                    topic,
                                    partition,
                                    low_watermark,
                                })
                            }
                            Err(Error::NotFound(_)) => Ok(Response::DeleteRecords {
                                error_code: ErrorCode::NotFound as u16,
                                topic,
                                partition,
                                low_watermark: 0,
                            }),
                            Err(e) => Err(e),
                        }
                    }
                    Err(Error::NotFound(_)) => Ok(Response::DeleteRecords {
                        error_code: ErrorCode::NotFound as u16,
                        topic,
                        partition,
                        low_watermark: 0,
                    }),
                    Err(e) => Err(e),
                }
            } else {
                // Wait off (default): local-first then best-effort fan-out
                // (Phase 113/129/130/135). Client success independent of majority.
                match broker.delete_records(&topic, partition, before_offset) {
                    Ok((low_watermark, error_code)) => {
                        if error_code == 0 {
                            let _ = fanout_delete_records(broker, &topic, partition, low_watermark)
                                .await;
                        }
                        Ok(Response::DeleteRecords {
                            error_code,
                            topic,
                            partition,
                            low_watermark,
                        })
                    }
                    Err(Error::NotFound(_)) => Ok(Response::DeleteRecords {
                        error_code: ErrorCode::NotFound as u16,
                        topic,
                        partition,
                        low_watermark: 0,
                    }),
                    Err(e) => Err(e),
                }
            }
        }
        Request::CreatePartitions { topic, total_count } => {
            let prev = snapshot_if_must_wait(broker);
            match broker.create_partitions(&topic, total_count) {
                Ok(partitions) => {
                    if !complete_assignment_mutation(broker, prev).await? {
                        return Ok(Response::CreatePartitions {
                            error_code: ErrorCode::NotEnoughReplicas as u16,
                            topic,
                            partitions: 0,
                        });
                    }
                    Ok(Response::CreatePartitions {
                        error_code: 0,
                        topic,
                        partitions,
                    })
                }
                Err(Error::NotFound(_)) => Ok(Response::CreatePartitions {
                    error_code: ErrorCode::NotFound as u16,
                    topic,
                    partitions: 0,
                }),
                Err(Error::InvalidArgument(m)) if m.starts_with("not controller") => {
                    Ok(Response::CreatePartitions {
                        error_code: ErrorCode::NotController as u16,
                        topic,
                        partitions: 0,
                    })
                }
                Err(Error::InvalidArgument(_)) => Ok(Response::CreatePartitions {
                    error_code: ErrorCode::InvalidArg as u16,
                    topic,
                    partitions: 0,
                }),
                Err(e) => Err(e),
            }
        }
        Request::ListOffsets { topic, partitions } => {
            match broker.list_offsets(&topic, &partitions) {
                Ok(entries) => Ok(Response::ListOffsets {
                    error_code: 0,
                    topic,
                    entries: entries
                        .into_iter()
                        .map(
                            |(partition, earliest, latest)| volant_protocol::OffsetListing {
                                partition,
                                earliest,
                                latest,
                            },
                        )
                        .collect(),
                }),
                Err(Error::NotFound(_)) => Ok(Response::ListOffsets {
                    error_code: ErrorCode::NotFound as u16,
                    topic,
                    entries: vec![],
                }),
                Err(e) => Err(e),
            }
        }
        Request::CreateAcls { entries } => match wire_to_acl_entries(&entries) {
            Ok(parsed) => match broker.create_acls_admin(parsed) {
                Ok(gen) => {
                    if let Some(g) = gen {
                        fanout_cluster_acl_snapshot(broker, g).await;
                    }
                    Ok(Response::CreateAcls { error_code: 0 })
                }
                Err(Error::InvalidArgument(m)) if m.starts_with("not controller") => {
                    Ok(Response::CreateAcls {
                        error_code: ErrorCode::NotController as u16,
                    })
                }
                Err(_) => Ok(Response::CreateAcls {
                    error_code: ErrorCode::Storage as u16,
                }),
            },
            Err(_) => Ok(Response::CreateAcls {
                error_code: ErrorCode::InvalidArg as u16,
            }),
        },
        Request::DeleteAcls { entries } => match wire_to_acl_entries(&entries) {
            Ok(parsed) => match broker.delete_acls_admin(&parsed) {
                Ok((removed, gen)) => {
                    if let Some(g) = gen {
                        fanout_cluster_acl_snapshot(broker, g).await;
                    }
                    Ok(Response::DeleteAcls {
                        error_code: 0,
                        removed: removed as u32,
                    })
                }
                Err(Error::InvalidArgument(m)) if m.starts_with("not controller") => {
                    Ok(Response::DeleteAcls {
                        error_code: ErrorCode::NotController as u16,
                        removed: 0,
                    })
                }
                Err(_) => Ok(Response::DeleteAcls {
                    error_code: ErrorCode::Storage as u16,
                    removed: 0,
                }),
            },
            Err(_msg) => Ok(Response::DeleteAcls {
                error_code: ErrorCode::InvalidArg as u16,
                removed: 0,
            }),
        },
        Request::ListAcls {
            principal,
            resource_type,
            resource,
        } => {
            let rt = if resource_type == 255 {
                None
            } else {
                crate::acl::ResourceType::from_u8(resource_type)
            };
            if resource_type != 255 && rt.is_none() {
                return Ok(Response::ListAcls {
                    error_code: ErrorCode::InvalidArg as u16,
                    entries: vec![],
                });
            }
            let p = if principal.is_empty() {
                None
            } else {
                Some(principal.as_str())
            };
            let r = if resource.is_empty() {
                None
            } else {
                Some(resource.as_str())
            };
            let entries = broker
                .acls()
                .list(p, rt, r)
                .into_iter()
                .map(acl_entry_to_wire)
                .collect();
            Ok(Response::ListAcls {
                error_code: 0,
                entries,
            })
        }
        Request::ScramFirst { .. } | Request::ScramFinal { .. } => {
            // Handled in dispatch_with_auth before this path.
            Ok(Response::Error {
                code: ErrorCode::Protocol as u16,
                message: "scram must be handled on the connection auth path".into(),
            })
        }
        Request::CreateScramUser {
            username,
            password,
            iterations,
        } => match broker.scram().upsert_user(&username, &password, iterations) {
            Ok(()) => Ok(Response::CreateScramUser { error_code: 0 }),
            Err(Error::InvalidArgument(_)) => Ok(Response::CreateScramUser {
                error_code: ErrorCode::InvalidArg as u16,
            }),
            Err(_) => Ok(Response::CreateScramUser {
                error_code: ErrorCode::Storage as u16,
            }),
        },
        Request::DeleteScramUser { username } => match broker.scram().delete_user(&username) {
            Ok(true) => Ok(Response::DeleteScramUser { error_code: 0 }),
            Ok(false) => Ok(Response::DeleteScramUser {
                error_code: ErrorCode::NotFound as u16,
            }),
            Err(_) => Ok(Response::DeleteScramUser {
                error_code: ErrorCode::Storage as u16,
            }),
        },
        Request::ListScramUsers => Ok(Response::ListScramUsers {
            error_code: 0,
            usernames: broker.scram().list_usernames(),
        }),
    }
}

fn wire_to_acl_entries(
    entries: &[volant_protocol::AclBinding],
) -> std::result::Result<Vec<crate::acl::AclEntry>, String> {
    let mut out = Vec::with_capacity(entries.len());
    for e in entries {
        let resource_type = crate::acl::ResourceType::from_u8(e.resource_type)
            .ok_or_else(|| format!("invalid resource_type {}", e.resource_type))?;
        let operation = crate::acl::AclOperation::from_u8(e.operation)
            .ok_or_else(|| format!("invalid operation {}", e.operation))?;
        let permission = crate::acl::AclPermission::from_u8(e.permission)
            .ok_or_else(|| format!("invalid permission {}", e.permission))?;
        if e.principal.is_empty() {
            return Err("empty principal".into());
        }
        if e.resource.is_empty() {
            return Err("empty resource".into());
        }
        out.push(crate::acl::AclEntry {
            principal: e.principal.clone(),
            resource_type,
            resource: e.resource.clone(),
            operation,
            permission,
        });
    }
    Ok(out)
}

fn acl_entry_to_wire(e: crate::acl::AclEntry) -> volant_protocol::AclBinding {
    volant_protocol::AclBinding {
        principal: e.principal,
        resource_type: e.resource_type.as_u8(),
        resource: e.resource,
        operation: e.operation.as_u8(),
        permission: e.permission.as_u8(),
    }
}

/// Native **NotController (14)** when there is no openraft leader or the
/// forward RPC fails. Local overlay is unchanged. Distinct from leader
/// joint-fail **NotEnoughReplicas (15)**.
fn membership_forward_unavailable(broker: &Broker, req: &Request) -> Response {
    let error_code = ErrorCode::NotController as u16;
    let generation = broker.membership_generation();
    match req {
        Request::AddBroker { .. } => Response::AddBroker {
            error_code,
            generation,
        },
        Request::RemoveBroker { .. } => Response::RemoveBroker {
            error_code,
            generation,
        },
        _ => Response::Error {
            code: error_code,
            message: "not controller".into(),
        },
    }
}

/// v0.38: send the same AddBroker / RemoveBroker body to `controller_id()`.
///
/// Does not persist overlay on this (follower) node. A second inbound
/// membership mutate while a forward is in flight returns 14 so A↔B
/// leadership-split cannot recurse.
async fn forward_membership_to_leader(broker: &Broker, req: Request) -> Response {
    let leader = broker.controller_id();
    if leader == 0 || leader == broker.node_id() {
        return membership_forward_unavailable(broker, &req);
    }
    let Some(addr) = broker.broker_addr(leader) else {
        return membership_forward_unavailable(broker, &req);
    };
    if !broker.membership_forward_try_enter() {
        return membership_forward_unavailable(broker, &req);
    }
    struct ForwardGuard<'a>(&'a Broker);
    impl Drop for ForwardGuard<'_> {
        fn drop(&mut self) {
            self.0.membership_forward_exit();
        }
    }
    let _guard = ForwardGuard(broker);
    let result = inter_broker_rpc(broker, &addr, &req).await;
    match result {
        Ok(resp) => match &resp {
            Response::AddBroker { .. } | Response::RemoveBroker { .. } | Response::Error { .. } => {
                resp
            }
            other => {
                warn!(
                    ?other,
                    leader, "membership forward: unexpected leader response"
                );
                membership_forward_unavailable(broker, &req)
            }
        },
        Err(e) => {
            warn!(
                error = %e,
                leader,
                %addr,
                "membership forward to openraft leader failed"
            );
            membership_forward_unavailable(broker, &req)
        }
    }
}

/// After overlay add/remove: leader joint-sync (rollback on fail) or v0.26
/// best-effort. Followers with openraft + forward on never reach here
/// (v0.38). Flag-off and `VOLANT_OPENRAFT_FORWARD_MEMBERSHIP=0` keep
/// persist + MembershipPut.
async fn after_overlay_mutation(
    broker: &Broker,
    prev: &MembershipOverlaySnapshot,
    generation: u64,
) -> (u16, u64) {
    if broker.openraft_joint_rollback_armed() {
        if broker.change_openraft_membership().await {
            fanout_membership_put(broker).await;
            (0, generation)
        } else {
            if let Err(e) = broker.restore_membership_overlay(prev) {
                warn!(
                    error = %e,
                    "openraft joint rollback failed to restore overlay"
                );
            }
            (
                ErrorCode::NotEnoughReplicas as u16,
                broker.membership_generation(),
            )
        }
    } else {
        fanout_membership_put(broker).await;
        let _ = broker.change_openraft_membership().await;
        (0, generation)
    }
}

fn map_error(e: Error) -> Response {
    let (code, message) = match &e {
        Error::NotFound(m) => (ErrorCode::NotFound as u16, m.clone()),
        Error::InvalidArgument(m) => (ErrorCode::InvalidArg as u16, m.clone()),
        Error::Storage(m) => (ErrorCode::Storage as u16, m.clone()),
        Error::Protocol(m) => (ErrorCode::Protocol as u16, m.clone()),
        Error::Io(err) => (ErrorCode::Io as u16, err.to_string()),
        Error::NotImplemented(m) => (ErrorCode::Unsupported as u16, (*m).to_string()),
    };
    Response::Error { code, message }
}
