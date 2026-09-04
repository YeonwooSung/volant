//! Kafka wire handlers: Create/Delete topics, CreatePartitions,
//! AlterPartitionReassignments, ListPartitionReassignments,
//! ElectLeaders, Describe/AlterUserScramCredentials,
//! Describe/AlterClientQuotas, DescribeDelegationToken,
//! ListClientMetricsResources, AlterReplicaLogDirs, AssignReplicasToDirs,
//! DescribeLogDirs, DescribeTopicPartitions, BrokerRegistration,
//! BrokerHeartbeat, UnregisterBroker, Envelope, FetchSnapshot,
//! ControllerRegistration, UpdateRaftVoter,
//! UpdateFeatures, DescribeQuorum, AllocateProducerIds,
//! GetTelemetrySubscriptions, PushTelemetry, AlterPartition,
//! CreateDelegationToken, RenewDelegationToken, ExpireDelegationToken, configs.

use bytes::{Buf, BufMut, BytesMut};
use volant_core::{Error, PartitionId, TopicName};

use crate::acl::{AclOperation, ResourceType, CLUSTER_RESOURCE};
use crate::broker::{Broker, LocalLogDirFilter, LocalLogDirTopic};
use crate::net::{complete_assignment_mutation, fanout_membership_put, snapshot_if_must_wait};

use crate::scram::ScramHash;

use super::codec::{
    get_compact_array_len, get_compact_bytes, get_compact_nullable_string, get_compact_string,
    get_nullable_string, get_string, get_uuid, put_compact_array_len, put_compact_bytes,
    put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, put_nullable_string,
    put_string, put_unsigned_varint, put_uuid, skip_tag_buffer, KAFKA_UUID_ZERO,
};
use super::topic_id;
use super::KafkaErrorCode;

/// Kafka `ScramMechanism`: SCRAM-SHA-256.
const KAFKA_SCRAM_SHA_256: i8 = 1;
/// Kafka `ScramMechanism`: SCRAM-SHA-512.
const KAFKA_SCRAM_SHA_512: i8 = 2;

/// Default partition count when CreateTopics v4+ sends `num_partitions = -1`.
const DEFAULT_TOPIC_PARTITIONS: u32 = 1;

pub(crate) async fn encode_create_topics(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // CreateTopics classic v0–4 + flexible v5–7:
    //   request: topics[{name, partitions, rf, assignments, configs}], timeout, validate_only (v1+)
    //   response: throttle (v2+), topics[{name, TopicId (v7+), error, error_message (v1+),
    //             num_partitions/rf/configs (v5+)}]
    // v4+: num_partitions / rf may be -1 (default partitions; RF ignored).
    // v6: same wire as v5 (quota throttle error accepted/ignored).
    // v7: TopicId UUID after name (deterministic Volant mapping).
    let flexible = version >= 5;
    struct TopicReq {
        name: String,
        partitions: i32,
        configs: Vec<(String, String)>,
    }
    let mut reqs = Vec::new();
    let validate_only;

    if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => {
                for _ in 0..n {
                    let name = match get_compact_string(src) {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    if src.remaining() < 4 + 2 {
                        break;
                    }
                    let partitions = src.get_i32();
                    let _rf = src.get_i16();
                    if let Ok(Some(ac)) = get_compact_array_len(src) {
                        for _ in 0..ac {
                            if src.remaining() < 4 {
                                break;
                            }
                            let _part = src.get_i32();
                            if let Ok(Some(bc)) = get_compact_array_len(src) {
                                for _ in 0..bc {
                                    if src.remaining() < 4 {
                                        break;
                                    }
                                    let _ = src.get_i32();
                                }
                            }
                            let _ = skip_tag_buffer(src);
                        }
                    }
                    let mut configs = Vec::new();
                    if let Ok(Some(cc)) = get_compact_array_len(src) {
                        for _ in 0..cc {
                            let k = match get_compact_string(src) {
                                Ok(s) => s,
                                Err(_) => break,
                            };
                            let v = match get_compact_nullable_string(src) {
                                Ok(Some(s)) => s,
                                Ok(None) => String::new(),
                                Err(_) => break,
                            };
                            let _ = skip_tag_buffer(src);
                            configs.push((k, v));
                        }
                    }
                    let _ = skip_tag_buffer(src);
                    reqs.push(TopicReq {
                        name,
                        partitions,
                        configs,
                    });
                }
            }
            Ok(None) | Err(_) => {}
        }
        if src.remaining() >= 4 {
            let _timeout = src.get_i32();
        }
        validate_only = if version >= 1 && src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
        let _ = skip_tag_buffer(src);
    } else {
        if src.remaining() < 4 {
            if version >= 2 {
                out.put_i32(0);
            }
            out.put_i32(0);
            return;
        }
        let topic_count = src.get_i32();
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
        if src.remaining() >= 4 {
            let _timeout = src.get_i32();
        }
        validate_only = if version >= 1 && src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
    }

    if version >= 2 {
        out.put_i32(0); // throttle
    }
    if flexible {
        put_compact_array_len(out, reqs.len());
    } else {
        out.put_i32(reqs.len() as i32);
    }

    for t in reqs {
        if flexible {
            put_compact_string(out, &t.name);
        } else {
            put_string(out, &t.name);
        }

        let write_result = |out: &mut BytesMut,
                            code: KafkaErrorCode,
                            msg: Option<&str>,
                            parts: i32,
                            rf: i16,
                            numeric_id: Option<u32>| {
            // v7+: TopicId immediately after Name.
            if version >= 7 {
                let uuid = numeric_id
                    .map(topic_id::uuid_for_numeric_id)
                    .unwrap_or(KAFKA_UUID_ZERO);
                topic_id::write_uuid(out, &uuid);
            }
            out.put_i16(code.as_i16());
            if version >= 1 {
                if flexible {
                    put_compact_nullable_string(out, msg);
                } else {
                    put_nullable_string(out, msg);
                }
            }
            if flexible {
                // v5+: NumPartitions, ReplicationFactor, Configs (null), tags
                out.put_i32(parts);
                out.put_i16(rf);
                put_unsigned_varint_null_array(out); // null configs
                put_empty_tag_buffer(out);
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
            write_result(
                out,
                KafkaErrorCode::TopicAuthorizationFailed,
                Some("topic authorization failed"),
                -1,
                -1,
                None,
            );
            continue;
        }

        let partitions = if t.partitions == -1 && version >= 4 {
            DEFAULT_TOPIC_PARTITIONS
        } else if t.partitions <= 0 {
            write_result(
                out,
                KafkaErrorCode::InvalidPartitions,
                Some("invalid partition count"),
                -1,
                -1,
                None,
            );
            continue;
        } else {
            t.partitions as u32
        };

        let exists = !broker
            .metadata(Some(&[TopicName::new(t.name.clone())]))
            .topics
            .is_empty();
        if exists {
            write_result(
                out,
                KafkaErrorCode::TopicAlreadyExists,
                Some("topic already exists"),
                -1,
                -1,
                None,
            );
            continue;
        }

        if validate_only {
            write_result(
                out,
                KafkaErrorCode::None,
                None,
                partitions as i32,
                1,
                None, // no id until actually created
            );
            continue;
        }

        let prev = snapshot_if_must_wait(broker);
        let result = if t.configs.is_empty() {
            broker.create_topic(t.name.as_str(), partitions)
        } else {
            broker.create_topic_with_configs(t.name.as_str(), partitions, &t.configs)
        };

        match result {
            Ok(id) => match complete_assignment_mutation(broker, prev).await {
                Ok(true) => write_result(
                    out,
                    KafkaErrorCode::None,
                    None,
                    partitions as i32,
                    1,
                    Some(id.0),
                ),
                Ok(false) => write_result(
                    out,
                    KafkaErrorCode::NotEnoughReplicas,
                    Some("assignment consensus majority failed"),
                    -1,
                    -1,
                    None,
                ),
                Err(_) => write_result(out, KafkaErrorCode::Unknown, None, -1, -1, None),
            },
            Err(Error::InvalidArgument(msg)) if msg.contains("already exists") => {
                write_result(
                    out,
                    KafkaErrorCode::TopicAlreadyExists,
                    Some("topic already exists"),
                    -1,
                    -1,
                    None,
                );
            }
            Err(Error::InvalidArgument(msg)) => {
                write_result(
                    out,
                    KafkaErrorCode::InvalidTopicException,
                    Some(&msg),
                    -1,
                    -1,
                    None,
                );
            }
            Err(_) => write_result(out, KafkaErrorCode::Unknown, None, -1, -1, None),
        }
    }
    if flexible {
        put_empty_tag_buffer(out);
    }
}

/// Compact null array length (`uvarint(0)`).
pub(crate) fn put_unsigned_varint_null_array(dst: &mut BytesMut) {
    put_unsigned_varint(dst, 0);
}

pub(crate) async fn encode_delete_topics(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // DeleteTopics classic v0–3 + flexible v4–6:
    //   request ≤v5: topic_names[] + timeout_ms
    //   request v6: topics[{name nullable, topicId, tags}] + timeout + tags
    //   response: throttle (v1+), responses[{
    //     name (≤v5 string / v6 nullable), TopicId (v6+),
    //     error, ErrorMessage (v5+), tags (flex)
    //   }]
    let flexible = version >= 4;
    let by_topic_id = version >= 6;

    /// One delete target after request parse.
    struct DelReq {
        name: Option<String>,
        uuid: [u8; 16],
        /// Resolved name for delete (if any).
        resolved: Option<String>,
        /// Resolved numeric id (if known before delete).
        numeric_id: Option<u32>,
        /// Parse-time error (e.g. unknown TopicId).
        early_err: Option<KafkaErrorCode>,
    }

    let mut reqs: Vec<DelReq> = Vec::new();

    if by_topic_id {
        match get_compact_array_len(src) {
            Ok(Some(n)) => {
                for _ in 0..n {
                    let name = match get_compact_nullable_string(src) {
                        Ok(n) => n,
                        Err(_) => break,
                    };
                    let uuid = match get_uuid(src) {
                        Ok(u) => u,
                        Err(_) => break,
                    };
                    let _ = skip_tag_buffer(src);

                    let r = topic_id::resolve_delete_entry(broker, name, uuid);
                    reqs.push(DelReq {
                        name: r.request_name,
                        uuid: r.uuid,
                        resolved: r.resolved_name,
                        numeric_id: r.numeric_id,
                        early_err: r.unknown_topic_id.then_some(KafkaErrorCode::UnknownTopicId),
                    });
                }
            }
            Ok(None) | Err(_) => {}
        }
        if src.remaining() >= 4 {
            let _timeout = src.get_i32();
        }
        let _ = skip_tag_buffer(src);
    } else if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => {
                for _ in 0..n {
                    match get_compact_string(src) {
                        Ok(s) => {
                            let id = topic_id::numeric_id_for_name(broker, &s);
                            reqs.push(DelReq {
                                name: Some(s.clone()),
                                uuid: topic_id::uuid_for_name(broker, &s),
                                resolved: Some(s),
                                numeric_id: id,
                                early_err: None,
                            });
                        }
                        Err(_) => break,
                    }
                }
            }
            Ok(None) | Err(_) => {}
        }
        if src.remaining() >= 4 {
            let _timeout = src.get_i32();
        }
        let _ = skip_tag_buffer(src);
    } else {
        if src.remaining() < 4 {
            if version >= 1 {
                out.put_i32(0);
            }
            out.put_i32(0);
            return;
        }
        let topic_count = src.get_i32();
        for _ in 0..topic_count.max(0) {
            match get_string(src) {
                Ok(s) => reqs.push(DelReq {
                    name: Some(s.clone()),
                    uuid: KAFKA_UUID_ZERO,
                    resolved: Some(s),
                    numeric_id: None,
                    early_err: None,
                }),
                Err(_) => break,
            }
        }
        if src.remaining() >= 4 {
            let _timeout = src.get_i32();
        }
    }

    if version >= 1 {
        out.put_i32(0); // throttle
    }
    if flexible {
        put_compact_array_len(out, reqs.len());
    } else {
        out.put_i32(reqs.len() as i32);
    }

    for r in reqs {
        // Name field: classic/v4–5 non-null string; v6 nullable compact.
        if by_topic_id {
            put_compact_nullable_string(out, r.name.as_deref().or(r.resolved.as_deref()));
            // Prefer request uuid; fall back to resolved numeric mapping.
            topic_id::write_uuid(out, &topic_id::echo_uuid(r.uuid, r.numeric_id));
        } else if flexible {
            put_compact_string(out, r.name.as_deref().unwrap_or(""));
        } else {
            put_string(out, r.name.as_deref().unwrap_or(""));
        }

        let (err, err_msg): (KafkaErrorCode, Option<&str>) = if let Some(e) = r.early_err {
            (e, Some("unknown topic id"))
        } else if let Some(ref name) = r.resolved {
            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(principal),
                    ResourceType::Topic,
                    name,
                    AclOperation::Delete,
                )
            {
                (
                    KafkaErrorCode::TopicAuthorizationFailed,
                    Some("topic authorization failed"),
                )
            } else {
                let prev = snapshot_if_must_wait(broker);
                match broker.delete_topic(&TopicName::new(name.clone())) {
                    Ok(()) => match complete_assignment_mutation(broker, prev).await {
                        Ok(true) => (KafkaErrorCode::None, None),
                        Ok(false) => (
                            KafkaErrorCode::NotEnoughReplicas,
                            Some("assignment consensus majority failed"),
                        ),
                        Err(_) => (KafkaErrorCode::Unknown, None),
                    },
                    Err(Error::NotFound(_)) => (
                        KafkaErrorCode::UnknownTopicOrPartition,
                        Some("unknown topic or partition"),
                    ),
                    Err(_) => (KafkaErrorCode::Unknown, None),
                }
            }
        } else {
            (
                KafkaErrorCode::UnknownTopicOrPartition,
                Some("unknown topic or partition"),
            )
        };

        out.put_i16(err.as_i16());
        if version >= 5 {
            put_compact_nullable_string(out, err_msg);
        }
        if flexible {
            put_empty_tag_buffer(out);
        }
    }
    if flexible {
        put_empty_tag_buffer(out);
    }
}

pub(crate) async fn encode_create_partitions(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // CreatePartitions classic v0–1 + flexible v2–3:
    //   request: topics[{name, count, assignments|null}], timeout, validate_only
    //   response: throttle (all versions), results[{name, error, error_message}]
    // Phase 45 adds missing throttle framing (Kafka has throttle on v0+).
    // Flexible v2: compact framing + TAG_BUFFER.
    // Phase 80: v3 is wire-identical to v2 (KIP-599 may return
    // THROTTLING_QUOTA_EXCEEDED — Volant has no quotas, so never emits it).
    let flexible = version >= 2;
    struct Req {
        topic: String,
        count: i32,
    }
    let mut reqs = Vec::new();
    let validate_only;

    if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => {
                for _ in 0..n {
                    let topic = match get_compact_string(src) {
                        Ok(t) => t,
                        Err(_) => break,
                    };
                    if src.remaining() < 4 {
                        break;
                    }
                    let count = src.get_i32();
                    // assignments: compact nullable array
                    match get_compact_array_len(src) {
                        Ok(None) => {}
                        Ok(Some(ac)) => {
                            for _ in 0..ac {
                                if let Ok(Some(bc)) = get_compact_array_len(src) {
                                    for _ in 0..bc {
                                        if src.remaining() < 4 {
                                            break;
                                        }
                                        let _ = src.get_i32();
                                    }
                                }
                                let _ = skip_tag_buffer(src);
                            }
                        }
                        Err(_) => {}
                    }
                    let _ = skip_tag_buffer(src);
                    reqs.push(Req { topic, count });
                }
            }
            Ok(None) | Err(_) => {}
        }
        if src.remaining() >= 4 {
            let _timeout = src.get_i32();
        }
        validate_only = if src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
        let _ = skip_tag_buffer(src);
    } else {
        if src.remaining() < 4 {
            out.put_i32(0); // throttle
            out.put_i32(0);
            return;
        }
        let topic_count = src.get_i32();
        for _ in 0..topic_count.max(0) {
            let topic = match get_string(src) {
                Ok(t) => t,
                Err(_) => break,
            };
            if src.remaining() < 4 {
                break;
            }
            let count = src.get_i32();
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
        validate_only = if src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
    }

    out.put_i32(0); // throttle
    if flexible {
        put_compact_array_len(out, reqs.len());
    } else {
        out.put_i32(reqs.len() as i32);
    }
    for r in reqs {
        if flexible {
            put_compact_string(out, &r.topic);
        } else {
            put_string(out, &r.topic);
        }
        let (code, msg): (i16, Option<&str>) = if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(principal),
                ResourceType::Topic,
                &r.topic,
                AclOperation::Alter,
            ) {
            (
                KafkaErrorCode::TopicAuthorizationFailed.as_i16(),
                Some("topic authorization failed"),
            )
        } else if r.count <= 0 {
            (
                KafkaErrorCode::InvalidPartitions.as_i16(),
                Some("invalid partition count"),
            )
        } else if validate_only {
            let meta = broker.metadata(Some(&[TopicName::new(r.topic.clone())]));
            if meta.topics.is_empty() {
                (
                    KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
                    Some("topic not found"),
                )
            } else {
                let cur = meta.topics[0].partitions.len() as i32;
                if r.count < cur {
                    (
                        KafkaErrorCode::InvalidPartitions.as_i16(),
                        Some("partition count must not decrease"),
                    )
                } else {
                    (KafkaErrorCode::None.as_i16(), None)
                }
            }
        } else {
            let prev = snapshot_if_must_wait(broker);
            match broker.create_partitions(&r.topic, r.count as u32) {
                Ok(_) => match complete_assignment_mutation(broker, prev).await {
                    Ok(true) => (KafkaErrorCode::None.as_i16(), None),
                    Ok(false) => (
                        KafkaErrorCode::NotEnoughReplicas.as_i16(),
                        Some("assignment consensus majority failed"),
                    ),
                    Err(_) => (KafkaErrorCode::Unknown.as_i16(), None),
                },
                Err(Error::NotFound(_)) => (
                    KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
                    Some("topic not found"),
                ),
                Err(Error::InvalidArgument(ref msg)) => {
                    // Need owned string for message — write below with String
                    out.put_i16(KafkaErrorCode::InvalidPartitions.as_i16());
                    if flexible {
                        put_compact_nullable_string(out, Some(msg));
                        put_empty_tag_buffer(out);
                    } else {
                        put_nullable_string(out, Some(msg));
                    }
                    continue;
                }
                Err(_) => (KafkaErrorCode::Unknown.as_i16(), None),
            }
        };
        out.put_i16(code);
        if flexible {
            put_compact_nullable_string(out, msg);
            put_empty_tag_buffer(out);
        } else {
            put_nullable_string(out, msg);
        }
    }
    if flexible {
        put_empty_tag_buffer(out);
    }
}

/// AlterPartitionReassignments v0 (always flexible). Wraps native
/// `reassign_partitions` + `complete_assignment_mutation`. TimeoutMs is
/// ignored. `replicas = null` is cancel: no pending log → 83.
pub(crate) async fn encode_alter_partition_reassignments(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    struct PartReq {
        partition: i32,
        replicas: Option<Vec<i32>>,
    }
    struct TopicReq {
        name: String,
        partitions: Vec<PartReq>,
    }
    let mut topics = Vec::new();
    if src.remaining() >= 4 {
        let _timeout_ms = src.get_i32();
    }
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                let name = match get_compact_string(src) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let mut parts = Vec::new();
                match get_compact_array_len(src) {
                    Ok(Some(pc)) => {
                        for _ in 0..pc {
                            if src.remaining() < 4 {
                                break;
                            }
                            let partition = src.get_i32();
                            let replicas = match get_compact_array_len(src) {
                                Ok(None) => None,
                                Ok(Some(rc)) => {
                                    let mut ids = Vec::with_capacity(rc);
                                    for _ in 0..rc {
                                        if src.remaining() < 4 {
                                            break;
                                        }
                                        ids.push(src.get_i32());
                                    }
                                    Some(ids)
                                }
                                Err(_) => None,
                            };
                            let _ = skip_tag_buffer(src);
                            parts.push(PartReq {
                                partition,
                                replicas,
                            });
                        }
                    }
                    Ok(None) | Err(_) => {}
                }
                let _ = skip_tag_buffer(src);
                topics.push(TopicReq {
                    name,
                    partitions: parts,
                });
            }
        }
        Ok(None) | Err(_) => {}
    }
    let _ = skip_tag_buffer(src);

    let write_top = |out: &mut BytesMut, code: KafkaErrorCode, msg: Option<&str>, n: usize| {
        out.put_i32(0); // throttle
        out.put_i16(code.as_i16());
        put_compact_nullable_string(out, msg);
        put_compact_array_len(out, n);
    };

    if broker.cluster_config().is_some() && !broker.is_controller() {
        let msg = format!("not controller; controller_id={}", broker.controller_id());
        write_top(out, KafkaErrorCode::NotController, Some(&msg), 0);
        put_empty_tag_buffer(out);
        return;
    }

    write_top(out, KafkaErrorCode::None, None, topics.len());
    for t in topics {
        put_compact_string(out, &t.name);
        if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(principal),
                ResourceType::Topic,
                &t.name,
                AclOperation::Alter,
            )
        {
            put_compact_array_len(out, t.partitions.len());
            for p in t.partitions {
                out.put_i32(p.partition);
                out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
                put_compact_nullable_string(out, Some("topic authorization failed"));
                put_empty_tag_buffer(out);
            }
            put_empty_tag_buffer(out);
            continue;
        }

        put_compact_array_len(out, t.partitions.len());
        for p in t.partitions {
            out.put_i32(p.partition);
            let (code, msg) =
                apply_reassign_partition(broker, &t.name, p.partition, p.replicas.as_deref()).await;
            out.put_i16(code);
            put_compact_nullable_string(out, msg.as_deref());
            put_empty_tag_buffer(out);
        }
        put_empty_tag_buffer(out);
    }
    put_empty_tag_buffer(out);
}

async fn apply_reassign_partition(
    broker: &Broker,
    topic: &str,
    partition: i32,
    replicas: Option<&[i32]>,
) -> (i16, Option<String>) {
    if partition < 0 {
        return (
            KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
            Some("unknown topic or partition".into()),
        );
    }
    let pid = partition as u32;
    let Some(replicas) = replicas else {
        // Cancel: Volant apply is instant — no pending reassignment log.
        let meta = broker.metadata(Some(&[TopicName::new(topic)]));
        if meta.topics.is_empty() || pid as usize >= meta.topics[0].partitions.len() {
            return (
                KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
                Some("unknown topic or partition".into()),
            );
        }
        return (
            KafkaErrorCode::NoReassignmentInProgress.as_i16(),
            Some("no reassignment in progress".into()),
        );
    };
    let mut ids = Vec::with_capacity(replicas.len());
    for &r in replicas {
        if r < 0 {
            return (
                KafkaErrorCode::InvalidReplicaAssignment.as_i16(),
                Some("invalid replica assignment".into()),
            );
        }
        ids.push(r as u32);
    }
    let prev = snapshot_if_must_wait(broker);
    match broker.reassign_partitions(topic, pid, &ids) {
        Ok(_) => match complete_assignment_mutation(broker, prev).await {
            Ok(true) => (KafkaErrorCode::None.as_i16(), None),
            Ok(false) => (
                KafkaErrorCode::NotEnoughReplicas.as_i16(),
                Some("assignment consensus majority failed".into()),
            ),
            Err(_) => (KafkaErrorCode::Unknown.as_i16(), None),
        },
        Err(Error::NotFound(_)) => (
            KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
            Some("unknown topic or partition".into()),
        ),
        Err(Error::InvalidArgument(msg)) if msg.starts_with("not controller") => {
            (KafkaErrorCode::NotController.as_i16(), Some(msg))
        }
        Err(Error::InvalidArgument(msg)) if msg.contains("out of range") => {
            (KafkaErrorCode::UnknownTopicOrPartition.as_i16(), Some(msg))
        }
        Err(Error::InvalidArgument(msg)) if msg.contains("reassign requires cluster") => {
            (KafkaErrorCode::InvalidRequest.as_i16(), Some(msg))
        }
        Err(Error::InvalidArgument(msg)) => {
            (KafkaErrorCode::InvalidReplicaAssignment.as_i16(), Some(msg))
        }
        Err(_) => (KafkaErrorCode::Unknown.as_i16(), None),
    }
}

/// ListPartitionReassignments v0 (always flexible). Apply is instant, so
/// there is no in-progress reassignment log: `replicas` is the current
/// assignment and `addingReplicas` / `removingReplicas` are empty.
/// TimeoutMs is parsed and ignored.
pub(crate) fn encode_list_partition_reassignments(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    struct TopicReq {
        name: String,
        /// Empty = all partitions of this topic.
        partitions: Vec<i32>,
    }
    enum TopicFilter {
        All,
        Named(Vec<TopicReq>),
    }

    if src.remaining() >= 4 {
        let _timeout_ms = src.get_i32();
    }
    let filter = match get_compact_array_len(src) {
        Ok(None) => TopicFilter::All,
        Ok(Some(n)) => {
            let mut topics = Vec::with_capacity(n);
            for _ in 0..n {
                let name = match get_compact_string(src) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let mut partitions = Vec::new();
                match get_compact_array_len(src) {
                    Ok(Some(pc)) => {
                        for _ in 0..pc {
                            if src.remaining() < 4 {
                                break;
                            }
                            partitions.push(src.get_i32());
                        }
                    }
                    Ok(None) | Err(_) => {}
                }
                let _ = skip_tag_buffer(src);
                topics.push(TopicReq { name, partitions });
            }
            TopicFilter::Named(topics)
        }
        Err(_) => TopicFilter::Named(Vec::new()),
    };
    let _ = skip_tag_buffer(src);

    let write_top = |out: &mut BytesMut, code: KafkaErrorCode, msg: Option<&str>, n: usize| {
        out.put_i32(0); // throttle
        out.put_i16(code.as_i16());
        put_compact_nullable_string(out, msg);
        put_compact_array_len(out, n);
    };

    if broker.cluster_config().is_some() && !broker.is_controller() {
        let msg = format!("not controller; controller_id={}", broker.controller_id());
        write_top(out, KafkaErrorCode::NotController, Some(&msg), 0);
        put_empty_tag_buffer(out);
        return;
    }

    if matches!(filter, TopicFilter::All)
        && broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        )
    {
        write_top(
            out,
            KafkaErrorCode::TopicAuthorizationFailed,
            Some("cluster authorization failed"),
            0,
        );
        put_empty_tag_buffer(out);
        return;
    }

    match filter {
        TopicFilter::All => {
            let snap = broker.metadata(None);
            write_top(out, KafkaErrorCode::None, None, snap.topics.len());
            for t in snap.topics {
                put_compact_string(out, t.name.as_str());
                put_compact_array_len(out, t.partitions.len());
                for p in t.partitions {
                    write_list_reassign_partition(
                        out,
                        p.partition_id.0 as i32,
                        &p.replicas,
                        KafkaErrorCode::None,
                        None,
                    );
                }
                put_empty_tag_buffer(out);
            }
        }
        TopicFilter::Named(topics) => {
            write_top(out, KafkaErrorCode::None, None, topics.len());
            for t in topics {
                put_compact_string(out, &t.name);
                if broker.acls().is_enabled()
                    && !broker.acls().authorize(
                        Some(principal),
                        ResourceType::Topic,
                        &t.name,
                        AclOperation::Describe,
                    )
                {
                    let known = topic_assignment_partitions(broker, &t.name);
                    let indexes = list_partition_indexes(&t.partitions, known.as_deref());
                    put_compact_array_len(out, indexes.len());
                    for pid in indexes {
                        write_list_reassign_partition(
                            out,
                            pid,
                            &[],
                            KafkaErrorCode::TopicAuthorizationFailed,
                            Some("topic authorization failed"),
                        );
                    }
                    put_empty_tag_buffer(out);
                    continue;
                }
                write_named_topic_reassignments(broker, out, &t.name, &t.partitions);
                put_empty_tag_buffer(out);
            }
        }
    }
    put_empty_tag_buffer(out);
}

fn write_list_reassign_partition(
    out: &mut BytesMut,
    partition: i32,
    replicas: &[u32],
    code: KafkaErrorCode,
    msg: Option<&str>,
) {
    out.put_i32(partition);
    put_compact_array_len(out, replicas.len());
    for &r in replicas {
        out.put_i32(r as i32);
    }
    put_compact_array_len(out, 0); // addingReplicas
    put_compact_array_len(out, 0); // removingReplicas
    out.put_i16(code.as_i16());
    put_compact_nullable_string(out, msg);
    put_empty_tag_buffer(out);
}

fn topic_assignment_partitions(
    broker: &Broker,
    name: &str,
) -> Option<Vec<crate::broker::PartitionMetadata>> {
    let snap = broker.metadata(Some(&[TopicName::new(name)]));
    snap.topics.into_iter().next().map(|t| t.partitions)
}

/// BrokerRegistration v0 (always flexible). Volant membership is
/// overlay `membership.json` + native AddBroker — **not** KRaft
/// BrokerRegistration (no incarnation, no DirectoryId, no features).
///
/// Parses official Kafka `BrokerRegistrationRequest.json` v0 fields
/// (`brokerId`, `clusterId`, `incarnationId`, listeners, features,
/// rack) and discards them. Does **not** call `add_broker`. Returns
/// throttle **0**, error **42** `INVALID_REQUEST`, brokerEpoch **-1**.
/// Official `BrokerRegistrationResponse.json` has no `errorMessage`.
/// Controller is not required. ACL: Cluster **ALTER** (disabled ACLs
/// allow). Denied → **31**.
pub(crate) fn encode_broker_registration(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    parse_broker_registration_request(src);

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );
    let error = if denied {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::InvalidRequest
    };

    write_broker_registration(out, error);
}

fn parse_broker_registration_request(src: &mut impl Buf) {
    // Official v0 (flex, `BrokerRegistrationRequest.json`):
    // BrokerId i32, ClusterId compact string, IncarnationId uuid,
    // Listeners[] { name, host, port u16, securityProtocol i16, tagged },
    // Features[] { name, min i16, max i16, tagged },
    // Rack compact nullable string, tagged.
    // IsMigratingZkBroker / LogDirs / PreviousBrokerEpoch are v1+.
    if src.remaining() >= 4 {
        let _broker_id = src.get_i32();
    }
    let _ = get_compact_string(src);
    let _ = get_uuid(src);
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                if get_compact_string(src).is_err() {
                    break;
                }
                if get_compact_string(src).is_err() {
                    break;
                }
                if src.remaining() < 2 {
                    break;
                }
                let _port = src.get_u16();
                if src.remaining() < 2 {
                    break;
                }
                let _security = src.get_i16();
                let _ = skip_tag_buffer(src);
            }
        }
        Ok(None) | Err(_) => {}
    }
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                if get_compact_string(src).is_err() {
                    break;
                }
                if src.remaining() < 4 {
                    break;
                }
                let _min = src.get_i16();
                let _max = src.get_i16();
                let _ = skip_tag_buffer(src);
            }
        }
        Ok(None) | Err(_) => {}
    }
    let _ = get_compact_nullable_string(src);
    let _ = skip_tag_buffer(src);
}

fn write_broker_registration(out: &mut BytesMut, error: KafkaErrorCode) {
    // Official BrokerRegistrationResponse.json (v0+):
    // throttleTimeMs, errorCode, brokerEpoch (default -1), tagged.
    out.put_i32(0); // throttleTimeMs
    out.put_i16(error.as_i16());
    out.put_i64(-1); // brokerEpoch — none assigned
    put_empty_tag_buffer(out);
}

/// BrokerHeartbeat v0 (always flexible). Volant is **not** a KRaft
/// controller (no fencing, no metadata offset catch-up, no assigned
/// epoch). Does **not** wrap native Heartbeat (key 12) or
/// `GroupCoordinator::heartbeat`.
///
/// Parses official Kafka `BrokerHeartbeatRequest.json` v0 fields
/// (`brokerId`, `brokerEpoch`, `currentMetadataOffset`, `wantFence`,
/// `wantShutDown`) and discards them. Returns throttle **0**, error
/// **42** `INVALID_REQUEST`, `isCaughtUp` false, `isFenced` true,
/// `shouldShutDown` false. Official `BrokerHeartbeatResponse.json`
/// has no `errorMessage`. Controller is not required. ACL: Cluster
/// **ALTER** (disabled ACLs allow). Denied → **31**.
pub(crate) fn encode_broker_heartbeat(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    parse_broker_heartbeat_request(src);

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );
    let error = if denied {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::InvalidRequest
    };

    write_broker_heartbeat(out, error);
}

fn parse_broker_heartbeat_request(src: &mut impl Buf) {
    // Official v0 (flex, `BrokerHeartbeatRequest.json`):
    // BrokerId i32, BrokerEpoch i64, CurrentMetadataOffset i64,
    // WantFence bool, WantShutDown bool, tagged.
    // OfflineLogDirs is v1+ tagged; CordonedLogDirs is v2+ tagged.
    if src.remaining() >= 4 {
        let _broker_id = src.get_i32();
    }
    if src.remaining() >= 8 {
        let _broker_epoch = src.get_i64();
    }
    if src.remaining() >= 8 {
        let _current_metadata_offset = src.get_i64();
    }
    if src.remaining() >= 1 {
        let _want_fence = src.get_i8();
    }
    if src.remaining() >= 1 {
        let _want_shut_down = src.get_i8();
    }
    let _ = skip_tag_buffer(src);
}

fn write_broker_heartbeat(out: &mut BytesMut, error: KafkaErrorCode) {
    // Official BrokerHeartbeatResponse.json (v0+):
    // throttleTimeMs, errorCode, isCaughtUp, isFenced, shouldShutDown, tagged.
    // Official defaults: isCaughtUp=false, isFenced=true, shouldShutDown=false.
    // No errorMessage.
    out.put_i32(0); // throttleTimeMs
    out.put_i16(error.as_i16());
    out.put_i8(0); // isCaughtUp = false
    out.put_i8(1); // isFenced = true
    out.put_i8(0); // shouldShutDown = false
    put_empty_tag_buffer(out);
}

/// Envelope v0 (always flexible). Volant has no request forwarding
/// (not KIP-590).
///
/// Parses official Kafka `EnvelopeRequest.json` v0 fields
/// (`RequestData` compact bytes, `RequestPrincipal` compact nullable
/// bytes, `ClientHostAddress` compact bytes) and discards them. Does
/// **not** unwrap or execute the embedded request. Returns
/// `ResponseData` **null**, error **42** `INVALID_REQUEST`
/// (`forwarding not supported`). Official `EnvelopeResponse.json` has
/// no `throttleTimeMs`. Controller is not required. ACL: Cluster
/// **ALTER** (disabled ACLs allow). Denied → **31**.
pub(crate) fn encode_envelope(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    parse_envelope_request(src);

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );
    let error = if denied {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::InvalidRequest
    };

    write_envelope(out, error);
}

fn parse_envelope_request(src: &mut impl Buf) {
    // Official v0 (flex, `EnvelopeRequest.json`):
    // RequestData compact bytes, RequestPrincipal compact nullable bytes,
    // ClientHostAddress compact bytes, tagged.
    // RequestData is discarded — never interpreted as an inner Kafka request.
    let _ = get_compact_bytes(src);
    let _ = get_compact_bytes(src);
    let _ = get_compact_bytes(src);
    let _ = skip_tag_buffer(src);
}

fn write_envelope(out: &mut BytesMut, error: KafkaErrorCode) {
    // Official EnvelopeResponse.json (v0+): ResponseData compact
    // nullable bytes (always null — we do not forward), errorCode,
    // tagged. No throttleTimeMs.
    put_compact_bytes(out, None);
    out.put_i16(error.as_i16());
    put_empty_tag_buffer(out);
}

/// FetchSnapshot v0 (always flexible). Volant is **not** a KRaft
/// controller and does **not** serve KRaft metadata snapshots.
///
/// Parses official Kafka `FetchSnapshotRequest.json` v0 fields
/// (`replicaId`, `maxBytes`, `topics[]` with inline `SnapshotId`,
/// request-level tagged `clusterId`) and discards them. Does **not**
/// call native InstallSnapshot 112/113 or openraft snapshot APIs.
/// Returns throttle **0**, top-level **42** `INVALID_REQUEST`, empty
/// `topics[]`. Official `FetchSnapshotResponse.json` has no
/// `errorMessage`. Controller is not required. ACL: Cluster
/// **DESCRIBE** (disabled ACLs allow). Denied → **31**, empty topics.
pub(crate) fn encode_fetch_snapshot(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    parse_fetch_snapshot_request(src);

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        );
    let error = if denied {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::InvalidRequest
    };

    write_fetch_snapshot(out, error);
}

fn parse_fetch_snapshot_request(src: &mut impl Buf) {
    // Official v0 (flex, `FetchSnapshotRequest.json`):
    // ReplicaId i32, MaxBytes i32, Topics[] compact {
    //   Name compact string,
    //   Partitions[] {
    //     Partition i32, CurrentLeaderEpoch i32,
    //     SnapshotId { EndOffset i64, Epoch i32 } (inline),
    //     Position i64, tagged
    //   },
    //   tagged
    // },
    // tagged (ClusterId is request-level tag 0).
    // ReplicaDirectoryId is v1+ tag 0 — out of advertised range.
    // Missing fields stop that level; never panic.
    if src.remaining() >= 4 {
        let _replica_id = src.get_i32();
    }
    if src.remaining() >= 4 {
        let _max_bytes = src.get_i32();
    }
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                if get_compact_string(src).is_err() {
                    break;
                }
                match get_compact_array_len(src) {
                    Ok(Some(pn)) => {
                        for _ in 0..pn {
                            // Partition + CurrentLeaderEpoch + EndOffset + Epoch + Position
                            if src.remaining() < 4 + 4 + 8 + 4 + 8 {
                                break;
                            }
                            let _partition = src.get_i32();
                            let _leader_epoch = src.get_i32();
                            let _end_offset = src.get_i64();
                            let _snap_epoch = src.get_i32();
                            let _position = src.get_i64();
                            let _ = skip_tag_buffer(src);
                        }
                    }
                    Ok(None) | Err(_) => {}
                }
                let _ = skip_tag_buffer(src);
            }
        }
        Ok(None) | Err(_) => {}
    }
    let _ = skip_tag_buffer(src);
}

fn write_fetch_snapshot(out: &mut BytesMut, error: KafkaErrorCode) {
    // Official FetchSnapshotResponse.json v0:
    // throttleTimeMs, errorCode, topics[] compact, tagged.
    // CurrentLeader is a per-partition tagged field; unused because topics empty.
    out.put_i32(0); // throttleTimeMs
    out.put_i16(error.as_i16());
    put_compact_array_len(out, 0); // empty topics — do not echo request
    put_empty_tag_buffer(out);
}

/// ControllerRegistration v0 (always flexible). Volant is **not** a
/// KRaft controller quorum.
///
/// Parses official Kafka `ControllerRegistrationRequest.json` v0 fields
/// (`controllerId`, `incarnationId`, `zkMigrationReady`, listeners,
/// features) and discards them. Does **not** persist. Does **not**
/// call `add_broker`. Returns throttle **0**, error **42**
/// `INVALID_REQUEST`, errorMessage `"not KRaft controller registration"`.
/// Controller is not required. ACL: Cluster **ALTER** (disabled ACLs
/// allow). Denied → **31**, errorMessage null.
pub(crate) fn encode_controller_registration(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    parse_controller_registration_request(src);

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );
    let (error, msg) = if denied {
        (KafkaErrorCode::ClusterAuthorizationFailed, None)
    } else {
        (
            KafkaErrorCode::InvalidRequest,
            Some("not KRaft controller registration"),
        )
    };

    write_controller_registration(out, error, msg);
}

fn parse_controller_registration_request(src: &mut impl Buf) {
    // Official v0 (flex, `ControllerRegistrationRequest.json`):
    // ControllerId i32, IncarnationId uuid, ZkMigrationReady bool,
    // Listeners[] { name, host, port u16, securityProtocol i16, tagged },
    // Features[] { name, min i16, max i16, tagged }, tagged.
    // Close to BrokerRegistration v0 minus clusterId/rack, plus ZkMigrationReady.
    if src.remaining() >= 4 {
        let _controller_id = src.get_i32();
    }
    let _ = get_uuid(src);
    if src.remaining() >= 1 {
        let _zk_migration_ready = src.get_u8();
    }
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                if get_compact_string(src).is_err() {
                    break;
                }
                if get_compact_string(src).is_err() {
                    break;
                }
                if src.remaining() < 2 {
                    break;
                }
                let _port = src.get_u16();
                if src.remaining() < 2 {
                    break;
                }
                let _security = src.get_i16();
                let _ = skip_tag_buffer(src);
            }
        }
        Ok(None) | Err(_) => {}
    }
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                if get_compact_string(src).is_err() {
                    break;
                }
                if src.remaining() < 4 {
                    break;
                }
                let _min = src.get_i16();
                let _max = src.get_i16();
                let _ = skip_tag_buffer(src);
            }
        }
        Ok(None) | Err(_) => {}
    }
    let _ = skip_tag_buffer(src);
}

fn write_controller_registration(out: &mut BytesMut, error: KafkaErrorCode, msg: Option<&str>) {
    // Official ControllerRegistrationResponse.json (v0):
    // throttleTimeMs, errorCode, errorMessage compact nullable, tagged.
    out.put_i32(0); // throttleTimeMs
    out.put_i16(error.as_i16());
    put_compact_nullable_string(out, msg);
    put_empty_tag_buffer(out);
}

/// UpdateRaftVoter v0 (always flexible). Volant is **not** a KRaft
/// voter set (no DirectoryId, no listener store, no KRaft version
/// feature store).
///
/// Parses official Kafka `UpdateRaftVoterRequest.json` v0 fields
/// (`clusterId`, `currentLeaderEpoch`, `voterId`, `voterDirectoryId`,
/// listeners, `KRaftVersionFeature`) and discards them. Does **not**
/// persist. Returns throttle **0**, error **42** `INVALID_REQUEST`
/// (`not KRaft raft voter`). Official `UpdateRaftVoterResponse.json`
/// has no `errorMessage`. CurrentLeader (official tag 0) is omitted
/// (empty tag buffer). Controller is not required. ACL: Cluster
/// **ALTER** (disabled ACLs allow). Denied → **31**, empty tags.
pub(crate) fn encode_update_raft_voter(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    parse_update_raft_voter_request(src);

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );
    let error = if denied {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::InvalidRequest
    };

    write_update_raft_voter(out, error);
}

fn parse_update_raft_voter_request(src: &mut impl Buf) {
    // Official v0 (flex, `UpdateRaftVoterRequest.json`):
    // ClusterId compact nullable string, CurrentLeaderEpoch i32,
    // VoterId i32, VoterDirectoryId uuid,
    // Listeners[] { name, host, port u16, tagged },
    // KRaftVersionFeature { min i16, max i16, tagged } (inline, not
    // nullable), tagged.
    let _ = get_compact_nullable_string(src);
    if src.remaining() >= 4 {
        let _current_leader_epoch = src.get_i32();
    }
    if src.remaining() >= 4 {
        let _voter_id = src.get_i32();
    }
    let _ = get_uuid(src);
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                if get_compact_string(src).is_err() {
                    break;
                }
                if get_compact_string(src).is_err() {
                    break;
                }
                if src.remaining() < 2 {
                    break;
                }
                let _port = src.get_u16();
                let _ = skip_tag_buffer(src);
            }
        }
        Ok(None) | Err(_) => {}
    }
    // KRaftVersionFeature is an inline untagged struct (not nullable).
    if src.remaining() >= 4 {
        let _min = src.get_i16();
        let _max = src.get_i16();
        let _ = skip_tag_buffer(src);
    }
    let _ = skip_tag_buffer(src);
}

fn write_update_raft_voter(out: &mut BytesMut, error: KafkaErrorCode) {
    // Official UpdateRaftVoterResponse.json (v0):
    // throttleTimeMs, errorCode, tagged.
    // CurrentLeader is official tag 0 — omit (empty tag buffer).
    // No errorMessage.
    out.put_i32(0); // throttleTimeMs
    out.put_i16(error.as_i16());
    put_empty_tag_buffer(out);
}

/// UnregisterBroker v0 (always flexible). Wraps native `remove_broker`
/// (same invert as AddBroker / v0.217). Not Kafka KRaft incarnation.
/// TimeoutMs is parsed when present before tags and ignored.
pub(crate) async fn encode_unregister_broker(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    let write = |out: &mut BytesMut, code: KafkaErrorCode, msg: Option<&str>| {
        out.put_i32(0); // throttle
        out.put_i16(code.as_i16());
        put_compact_nullable_string(out, msg);
        put_empty_tag_buffer(out);
    };

    if src.remaining() < 4 {
        write(
            out,
            KafkaErrorCode::InvalidRequest,
            Some("truncated BrokerId"),
        );
        return;
    }
    let broker_id = src.get_i32();
    // Official v0 is BrokerId + tags. Some versions send TimeoutMs before tags.
    if src.remaining() >= 5 {
        let _timeout_ms = src.get_i32();
    }
    let _ = skip_tag_buffer(src);

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        )
    {
        write(
            out,
            KafkaErrorCode::ClusterAuthorizationFailed,
            Some("cluster authorization failed"),
        );
        return;
    }

    if broker.cluster_config().is_none() {
        write(
            out,
            KafkaErrorCode::InvalidRequest,
            Some("unregister requires cluster"),
        );
        return;
    }

    if !broker.is_controller() {
        let msg = format!("not controller; controller_id={}", broker.controller_id());
        write(out, KafkaErrorCode::NotController, Some(&msg));
        return;
    }

    if broker_id < 0 {
        write(
            out,
            KafkaErrorCode::InvalidRequest,
            Some("invalid broker id"),
        );
        return;
    }

    match broker.remove_broker(broker_id as u32) {
        Ok(_) => {
            fanout_membership_put(broker).await;
            write(out, KafkaErrorCode::None, None);
        }
        Err(Error::InvalidArgument(msg)) if msg.contains("requires cluster") => {
            write(
                out,
                KafkaErrorCode::InvalidRequest,
                Some("unregister requires cluster"),
            );
        }
        Err(Error::InvalidArgument(msg)) if msg.starts_with("not controller") => {
            write(out, KafkaErrorCode::NotController, Some(&msg));
        }
        Err(Error::InvalidArgument(msg)) => {
            write(out, KafkaErrorCode::InvalidRequest, Some(&msg));
        }
        Err(Error::Protocol(msg)) if msg.contains("not enough replicas") => {
            write(out, KafkaErrorCode::NotEnoughReplicas, Some(&msg));
        }
        Err(e) => write(out, KafkaErrorCode::Unknown, Some(&e.to_string())),
    }
}

fn list_partition_indexes(
    requested: &[i32],
    known: Option<&[crate::broker::PartitionMetadata]>,
) -> Vec<i32> {
    if !requested.is_empty() {
        return requested.to_vec();
    }
    known
        .unwrap_or(&[])
        .iter()
        .map(|p| p.partition_id.0 as i32)
        .collect()
}

/// ElectLeaders v0 classic / v1 flexible. Preferred wraps
/// `Broker::elect_preferred_leader` + `complete_assignment_mutation`.
/// ElectionType 1 (unclean) is refused with 87. TimeoutMs is parsed and
/// ignored. Not Kafka `preferred.leader` election and not a live replica copy.
pub(crate) async fn encode_elect_leaders(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    struct TopicReq {
        name: String,
        partitions: Vec<i32>,
    }
    enum TopicFilter {
        All,
        Named(Vec<TopicReq>),
    }

    let flexible = version >= 1;
    let election_type = if version >= 1 {
        if src.remaining() >= 1 {
            src.get_i8()
        } else {
            0
        }
    } else {
        0
    };
    if src.remaining() >= 4 {
        let _timeout_ms = src.get_i32();
    }

    let filter = if flexible {
        match get_compact_array_len(src) {
            Ok(None) => TopicFilter::All,
            Ok(Some(n)) => {
                let mut topics = Vec::with_capacity(n);
                for _ in 0..n {
                    let name = match get_compact_string(src) {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let mut partitions = Vec::new();
                    match get_compact_array_len(src) {
                        Ok(Some(pc)) => {
                            for _ in 0..pc {
                                if src.remaining() < 4 {
                                    break;
                                }
                                partitions.push(src.get_i32());
                            }
                        }
                        Ok(None) | Err(_) => {}
                    }
                    let _ = skip_tag_buffer(src);
                    topics.push(TopicReq { name, partitions });
                }
                TopicFilter::Named(topics)
            }
            Err(_) => TopicFilter::Named(Vec::new()),
        }
    } else if src.remaining() < 4 {
        TopicFilter::Named(Vec::new())
    } else {
        let n = src.get_i32();
        if n < 0 {
            TopicFilter::All
        } else {
            let mut topics = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let name = match get_string(src) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let mut partitions = Vec::new();
                if src.remaining() >= 4 {
                    let pc = src.get_i32();
                    if pc >= 0 {
                        for _ in 0..pc {
                            if src.remaining() < 4 {
                                break;
                            }
                            partitions.push(src.get_i32());
                        }
                    }
                }
                topics.push(TopicReq { name, partitions });
            }
            TopicFilter::Named(topics)
        }
    };
    if flexible {
        let _ = skip_tag_buffer(src);
    }

    let write_top = |out: &mut BytesMut, code: KafkaErrorCode, n: usize| {
        out.put_i32(0); // throttle
        out.put_i16(code.as_i16());
        if flexible {
            put_compact_array_len(out, n);
        } else {
            out.put_i32(n as i32);
        }
    };

    if broker.cluster_config().is_some() && !broker.is_controller() {
        write_top(out, KafkaErrorCode::NotController, 0);
        if flexible {
            put_empty_tag_buffer(out);
        }
        return;
    }

    let work: Vec<(String, Vec<i32>)> = match filter {
        TopicFilter::All => {
            let snap = broker.metadata(None);
            snap.topics
                .into_iter()
                .map(|t| {
                    let pids = t
                        .partitions
                        .iter()
                        .map(|p| p.partition_id.0 as i32)
                        .collect();
                    (t.name.as_str().to_owned(), pids)
                })
                .collect()
        }
        TopicFilter::Named(topics) => topics.into_iter().map(|t| (t.name, t.partitions)).collect(),
    };

    write_top(out, KafkaErrorCode::None, work.len());
    for (name, partitions) in work {
        if flexible {
            put_compact_string(out, &name);
        } else {
            put_string(out, &name);
        }
        if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(principal),
                ResourceType::Topic,
                &name,
                AclOperation::Alter,
            )
        {
            if flexible {
                put_compact_array_len(out, partitions.len());
            } else {
                out.put_i32(partitions.len() as i32);
            }
            for pid in partitions {
                write_elect_partition(
                    out,
                    flexible,
                    pid,
                    KafkaErrorCode::TopicAuthorizationFailed,
                    Some("topic authorization failed"),
                );
            }
            if flexible {
                put_empty_tag_buffer(out);
            }
            continue;
        }

        if flexible {
            put_compact_array_len(out, partitions.len());
        } else {
            out.put_i32(partitions.len() as i32);
        }
        for pid in partitions {
            let (code, msg) = apply_elect_leader(broker, &name, pid, election_type).await;
            write_elect_partition(out, flexible, pid, code, msg.as_deref());
        }
        if flexible {
            put_empty_tag_buffer(out);
        }
    }
    if flexible {
        put_empty_tag_buffer(out);
    }
}

fn write_elect_partition(
    out: &mut BytesMut,
    flexible: bool,
    partition: i32,
    code: KafkaErrorCode,
    msg: Option<&str>,
) {
    out.put_i32(partition);
    out.put_i16(code.as_i16());
    if flexible {
        put_compact_nullable_string(out, msg);
        put_empty_tag_buffer(out);
    } else {
        put_nullable_string(out, msg);
    }
}

async fn apply_elect_leader(
    broker: &Broker,
    topic: &str,
    partition: i32,
    election_type: i8,
) -> (KafkaErrorCode, Option<String>) {
    if partition < 0 {
        return (
            KafkaErrorCode::UnknownTopicOrPartition,
            Some("unknown topic or partition".into()),
        );
    }
    let pid = partition as u32;
    if election_type != 0 {
        // Unclean (type 1) and unknown types: do not elect outside ISR.
        let known = topic_assignment_partitions(broker, topic)
            .is_some_and(|parts| parts.iter().any(|p| p.partition_id.0 == pid));
        if !known {
            return (
                KafkaErrorCode::UnknownTopicOrPartition,
                Some("unknown topic or partition".into()),
            );
        }
        return (
            KafkaErrorCode::EligibleLeadersNotAvailable,
            Some("unclean leader election refused".into()),
        );
    }

    let prev = snapshot_if_must_wait(broker);
    let gen_before = broker.generation();
    match broker.elect_preferred_leader(topic, pid) {
        Ok(gen) => {
            if gen == gen_before {
                (KafkaErrorCode::None, None)
            } else {
                match complete_assignment_mutation(broker, prev).await {
                    Ok(true) => (KafkaErrorCode::None, None),
                    Ok(false) => (
                        KafkaErrorCode::NotEnoughReplicas,
                        Some("assignment consensus majority failed".into()),
                    ),
                    Err(_) => (KafkaErrorCode::Unknown, None),
                }
            }
        }
        Err(Error::NotFound(msg)) => (KafkaErrorCode::UnknownTopicOrPartition, Some(msg)),
        Err(Error::InvalidArgument(msg)) if msg.starts_with("not controller") => {
            (KafkaErrorCode::NotController, Some(msg))
        }
        Err(Error::InvalidArgument(msg)) if msg.contains("eligible leaders") => {
            (KafkaErrorCode::EligibleLeadersNotAvailable, Some(msg))
        }
        Err(Error::InvalidArgument(msg)) => (KafkaErrorCode::InvalidRequest, Some(msg)),
        Err(_) => (KafkaErrorCode::Unknown, None),
    }
}

fn write_named_topic_reassignments(
    broker: &Broker,
    out: &mut BytesMut,
    name: &str,
    requested: &[i32],
) {
    let known = topic_assignment_partitions(broker, name);
    let Some(parts) = known else {
        // Unknown topic: emit requested indexes as 3, or skip if none.
        put_compact_array_len(out, requested.len());
        for &pid in requested {
            write_list_reassign_partition(
                out,
                pid,
                &[],
                KafkaErrorCode::UnknownTopicOrPartition,
                Some("unknown topic or partition"),
            );
        }
        return;
    };
    let indexes = list_partition_indexes(requested, Some(&parts));
    put_compact_array_len(out, indexes.len());
    for pid in indexes {
        if pid < 0 {
            write_list_reassign_partition(
                out,
                pid,
                &[],
                KafkaErrorCode::UnknownTopicOrPartition,
                Some("unknown topic or partition"),
            );
            continue;
        }
        match parts.iter().find(|p| p.partition_id.0 == pid as u32) {
            Some(p) => {
                write_list_reassign_partition(out, pid, &p.replicas, KafkaErrorCode::None, None)
            }
            None => write_list_reassign_partition(
                out,
                pid,
                &[],
                KafkaErrorCode::UnknownTopicOrPartition,
                Some("unknown topic or partition"),
            ),
        }
    }
}

/// Kafka `ConfigResource.Type`: TOPIC.
const RES_TOPIC: i8 = 2;
/// Kafka `ConfigResource.Type`: BROKER (Phase 99).
const RES_BROKER: i8 = 4;

/// Kafka `DescribeConfigsResponse.ConfigSource` ids (classic).
const CFG_SRC_TOPIC: i8 = 1;
/// DYNAMIC_BROKER_CONFIG — runtime-mutable process knobs (Phase 99).
const CFG_SRC_DYNAMIC_BROKER: i8 = 2;
const CFG_SRC_DEFAULT: i8 = 5;
/// Kafka `DescribeConfigsResponse.ConfigType` ids.
const CFG_TYPE_STRING: i8 = 2;
const CFG_TYPE_LONG: i8 = 5;

pub(crate) fn config_type_for_key(key: &str) -> i8 {
    match key {
        "retention.ms" | "retention.bytes" | "segment.bytes" => CFG_TYPE_LONG,
        // Phase 99 broker knobs are all integer ms / counts.
        k if crate::broker_config::is_known_key(k) => CFG_TYPE_LONG,
        _ => CFG_TYPE_STRING,
    }
}

pub(crate) fn config_documentation(key: &str) -> Option<&'static str> {
    match key {
        "retention.ms" => Some("Log retention time in milliseconds"),
        "retention.bytes" => Some("Log retention size in bytes"),
        "segment.bytes" => Some("Segment roll size in bytes"),
        "cleanup.policy" => Some("delete | compact"),
        other => crate::broker_config::documentation(other),
    }
}

fn unsupported_config_resource_msg() -> &'static str {
    "only TOPIC and BROKER resources supported"
}

/// Phase 103: BROKER resource name is accepted when empty (cluster-default style)
/// or equal to this process's `node_id` as a decimal string.
///
/// Other non-empty names are rejected with `INVALID_REQUEST` — local validation
/// only (no multi-broker fan-out).
fn broker_resource_name_matches(node_id: u32, name: &str) -> bool {
    name.is_empty() || name == node_id.to_string()
}

fn invalid_broker_resource_name_msg(node_id: u32) -> String {
    format!("BROKER resource name must be empty or \"{node_id}\" (this broker's node_id)")
}

pub(crate) fn encode_describe_configs(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // DescribeConfigs classic v0–3 + flexible v4:
    //   request: resources[], include_synonyms (v1+), include_documentation (v3+)
    //   response: throttle (all versions),
    //     [error, error_message, resource_type, resource_name, configs[…]]
    //   config entry: name, value, read_only,
    //     is_default (v0) | config_source (v1+), is_sensitive,
    //     synonyms (v1+), config_type + documentation (v3+)
    // Phase 46: leading throttle + Kafka field order (error_message before type/name).
    // Flexible v4: compact strings/arrays + TAG_BUFFER per nested struct.
    let flexible = version >= 4;
    struct Res {
        rtype: i8,
        name: String,
        keys: Option<Vec<String>>,
    }
    let mut resources = Vec::new();
    let include_docs;

    if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => {
                for _ in 0..n {
                    if src.remaining() < 1 {
                        break;
                    }
                    let rtype = src.get_i8();
                    let name = match get_compact_string(src) {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let keys = match get_compact_array_len(src) {
                        Ok(None) => None,
                        Ok(Some(kc)) => {
                            let mut ks = Vec::new();
                            for _ in 0..kc {
                                match get_compact_string(src) {
                                    Ok(k) => ks.push(k),
                                    Err(_) => break,
                                }
                            }
                            Some(ks)
                        }
                        Err(_) => None,
                    };
                    let _ = skip_tag_buffer(src);
                    resources.push(Res { rtype, name, keys });
                }
            }
            Ok(None) | Err(_) => {}
        }
        if version >= 1 && src.remaining() >= 1 {
            let _include_synonyms = src.get_u8() != 0;
        }
        include_docs = if version >= 3 && src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
        let _ = skip_tag_buffer(src);
    } else {
        if src.remaining() < 4 {
            out.put_i32(0); // throttle
            out.put_i32(0);
            return;
        }
        let n = src.get_i32();
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
        if version >= 1 && src.remaining() >= 1 {
            let _include_synonyms = src.get_u8() != 0;
        }
        include_docs = if version >= 3 && src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
    }

    out.put_i32(0); // throttle
    if flexible {
        put_compact_array_len(out, resources.len());
    } else {
        out.put_i32(resources.len() as i32);
    }
    for r in resources {
        let write_header =
            |out: &mut BytesMut, code: KafkaErrorCode, msg: Option<&str>, rtype: i8, name: &str| {
                out.put_i16(code.as_i16());
                if flexible {
                    put_compact_nullable_string(out, msg);
                    out.put_i8(rtype);
                    put_compact_string(out, name);
                } else {
                    put_nullable_string(out, msg);
                    out.put_i8(rtype);
                    put_string(out, name);
                }
            };
        let write_empty_configs = |out: &mut BytesMut| {
            if flexible {
                put_compact_array_len(out, 0);
                put_empty_tag_buffer(out);
            } else {
                out.put_i32(0);
            }
        };

        if r.rtype == RES_BROKER {
            // Phase 99: BROKER resource — txn/session/sweep knobs.
            // Phase 103: name must be empty or this broker's node_id decimal.
            if !broker_resource_name_matches(broker.node_id(), &r.name) {
                let msg = invalid_broker_resource_name_msg(broker.node_id());
                write_header(
                    out,
                    KafkaErrorCode::InvalidRequest,
                    Some(&msg),
                    r.rtype,
                    &r.name,
                );
                write_empty_configs(out);
                continue;
            }
            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(principal),
                    ResourceType::Cluster,
                    CLUSTER_RESOURCE,
                    AclOperation::Describe,
                )
            {
                write_header(
                    out,
                    KafkaErrorCode::ClusterAuthorizationFailed,
                    None,
                    r.rtype,
                    &r.name,
                );
                write_empty_configs(out);
                continue;
            }
            let mut entries = broker.describe_broker_configs();
            if let Some(filter) = &r.keys {
                entries.retain(|(k, _)| filter.iter().any(|f| f == k));
            }
            write_header(out, KafkaErrorCode::None, None, r.rtype, &r.name);
            if flexible {
                put_compact_array_len(out, entries.len());
            } else {
                out.put_i32(entries.len() as i32);
            }
            for (k, v) in entries {
                let product_default = crate::broker_config::product_default(&k)
                    .map(|d| d.to_string())
                    .unwrap_or_default();
                let is_default = v == product_default;
                if flexible {
                    put_compact_string(out, &k);
                    put_compact_nullable_string(out, Some(&v));
                } else {
                    put_string(out, &k);
                    put_nullable_string(out, Some(&v));
                }
                out.put_u8(0); // read_only
                if version == 0 {
                    out.put_u8(if is_default { 1 } else { 0 });
                } else {
                    // Runtime-mutable process knobs → DYNAMIC_BROKER_CONFIG.
                    out.put_i8(CFG_SRC_DYNAMIC_BROKER);
                }
                out.put_u8(0); // is_sensitive
                if version >= 1 {
                    if flexible {
                        put_compact_array_len(out, 0);
                    } else {
                        out.put_i32(0);
                    }
                }
                if version >= 3 {
                    out.put_i8(config_type_for_key(&k));
                    let doc = if include_docs {
                        config_documentation(&k)
                    } else {
                        None
                    };
                    if flexible {
                        put_compact_nullable_string(out, doc);
                    } else {
                        put_nullable_string(out, doc);
                    }
                }
                if flexible {
                    put_empty_tag_buffer(out);
                }
            }
            if flexible {
                put_empty_tag_buffer(out);
            }
            continue;
        }

        if r.rtype != RES_TOPIC {
            write_header(
                out,
                KafkaErrorCode::InvalidRequest,
                Some(unsupported_config_resource_msg()),
                r.rtype,
                &r.name,
            );
            write_empty_configs(out);
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
            write_empty_configs(out);
            continue;
        }
        match broker.describe_configs(&r.name) {
            Ok((_id, _pc, cfg)) => {
                let mut entries = cfg.to_entries();
                if let Some(filter) = &r.keys {
                    entries.retain(|(k, _)| filter.iter().any(|f| f == k));
                }
                write_header(out, KafkaErrorCode::None, None, r.rtype, &r.name);
                if flexible {
                    put_compact_array_len(out, entries.len());
                } else {
                    out.put_i32(entries.len() as i32);
                }
                for (k, v) in entries {
                    let is_default = v.is_empty();
                    if flexible {
                        put_compact_string(out, &k);
                        if is_default {
                            put_compact_nullable_string(out, None);
                        } else {
                            put_compact_nullable_string(out, Some(&v));
                        }
                    } else {
                        put_string(out, &k);
                        if is_default {
                            put_nullable_string(out, None);
                        } else {
                            put_nullable_string(out, Some(&v));
                        }
                    }
                    out.put_u8(0); // read_only
                    if version == 0 {
                        out.put_u8(if is_default { 1 } else { 0 });
                    } else {
                        out.put_i8(if is_default {
                            CFG_SRC_DEFAULT
                        } else {
                            CFG_SRC_TOPIC
                        });
                    }
                    out.put_u8(0); // is_sensitive
                    if version >= 1 {
                        if flexible {
                            put_compact_array_len(out, 0); // empty synonyms
                        } else {
                            out.put_i32(0);
                        }
                    }
                    if version >= 3 {
                        out.put_i8(config_type_for_key(&k));
                        let doc = if include_docs {
                            config_documentation(&k)
                        } else {
                            None
                        };
                        if flexible {
                            put_compact_nullable_string(out, doc);
                        } else {
                            put_nullable_string(out, doc);
                        }
                    }
                    if flexible {
                        put_empty_tag_buffer(out); // config entry tags
                    }
                }
                if flexible {
                    put_empty_tag_buffer(out); // result tags
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
                write_empty_configs(out);
            }
            Err(_) => {
                write_header(out, KafkaErrorCode::Unknown, None, r.rtype, &r.name);
                write_empty_configs(out);
            }
        }
    }
    if flexible {
        put_empty_tag_buffer(out);
    }
}

/// Encode AlterConfigs response. Returns BROKER fan-out jobs `(generation, entries)`.
pub(crate) fn encode_alter_configs(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) -> Vec<(u64, Vec<(String, String)>)> {
    // AlterConfigs classic v0–1 + flexible v2:
    //   request: resources[{type, name, configs[{name, value}]}], validate_only
    //   response: throttle (all versions), responses[{error, error_message, type, name}]
    // Phase 46: leading throttle (Kafka has throttle on v0+).
    let flexible = version >= 2;
    let mut fanouts: Vec<(u64, Vec<(String, String)>)> = Vec::new();
    struct Res {
        rtype: i8,
        name: String,
        entries: Vec<(String, String)>,
    }
    let mut resources = Vec::new();
    let validate_only;

    if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => {
                for _ in 0..n {
                    if src.remaining() < 1 {
                        break;
                    }
                    let rtype = src.get_i8();
                    let name = match get_compact_string(src) {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let mut entries = Vec::new();
                    if let Ok(Some(ec)) = get_compact_array_len(src) {
                        for _ in 0..ec {
                            let k = match get_compact_string(src) {
                                Ok(s) => s,
                                Err(_) => break,
                            };
                            let v = match get_compact_nullable_string(src) {
                                Ok(Some(s)) => s,
                                Ok(None) => String::new(),
                                Err(_) => String::new(),
                            };
                            let _ = skip_tag_buffer(src);
                            entries.push((k, v));
                        }
                    }
                    let _ = skip_tag_buffer(src);
                    resources.push(Res {
                        rtype,
                        name,
                        entries,
                    });
                }
            }
            Ok(None) | Err(_) => {}
        }
        validate_only = if src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
        let _ = skip_tag_buffer(src);
    } else {
        if src.remaining() < 4 {
            out.put_i32(0); // throttle
            out.put_i32(0);
            return fanouts;
        }
        let n = src.get_i32();
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
        validate_only = if src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
    }

    out.put_i32(0); // throttle
    if flexible {
        put_compact_array_len(out, resources.len());
    } else {
        out.put_i32(resources.len() as i32);
    }
    for r in resources {
        let (code, msg): (i16, Option<String>) = match r.rtype {
            RES_BROKER => {
                // Phase 103: name must be empty or this broker's node_id decimal.
                if !broker_resource_name_matches(broker.node_id(), &r.name) {
                    (
                        KafkaErrorCode::InvalidRequest.as_i16(),
                        Some(invalid_broker_resource_name_msg(broker.node_id())),
                    )
                } else {
                    let (code, msg, fanout) =
                        alter_broker_resource(broker, principal, &r.entries, validate_only);
                    if let Some(job) = fanout {
                        fanouts.push(job);
                    }
                    (code, msg)
                }
            }
            RES_TOPIC => {
                if broker.acls().is_enabled()
                    && !broker.acls().authorize(
                        Some(principal),
                        ResourceType::Topic,
                        &r.name,
                        AclOperation::Alter,
                    )
                {
                    (KafkaErrorCode::TopicAuthorizationFailed.as_i16(), None)
                } else if validate_only {
                    match volant_broker_topic_config_validate(&r.entries) {
                        Ok(()) => (KafkaErrorCode::None.as_i16(), None),
                        Err(msg) => (KafkaErrorCode::InvalidConfig.as_i16(), Some(msg)),
                    }
                } else {
                    match broker.alter_configs(&r.name, &r.entries) {
                        Ok(_) => (KafkaErrorCode::None.as_i16(), None),
                        Err(Error::NotFound(_)) => (
                            KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
                            Some("topic not found".into()),
                        ),
                        Err(Error::InvalidArgument(msg)) => {
                            (KafkaErrorCode::InvalidConfig.as_i16(), Some(msg))
                        }
                        Err(_) => (KafkaErrorCode::Unknown.as_i16(), None),
                    }
                }
            }
            _ => (
                KafkaErrorCode::InvalidRequest.as_i16(),
                Some(unsupported_config_resource_msg().into()),
            ),
        };
        out.put_i16(code);
        if flexible {
            put_compact_nullable_string(out, msg.as_deref());
            out.put_i8(r.rtype);
            put_compact_string(out, &r.name);
            put_empty_tag_buffer(out);
        } else {
            put_nullable_string(out, msg.as_deref());
            out.put_i8(r.rtype);
            put_string(out, &r.name);
        }
    }
    if flexible {
        put_empty_tag_buffer(out);
    }
    fanouts
}

pub(crate) fn volant_broker_topic_config_validate(
    entries: &[(String, String)],
) -> std::result::Result<(), String> {
    crate::topic_config::TopicConfig::from_entries(entries)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// AlterConfigs / IncrementalAlter for BROKER resources (Phase 99–102 + 113).
///
/// Successful non-validate_only merges a sparse durable overlay under
/// `{data_dir}/__broker_config/state.json` (only altered keys).
///
/// Phase 113: cluster mode requires the **controller**; returns Kafka
/// `NOT_CONTROLLER` (41) otherwise. On controller success, the third tuple
/// element is `(generation, entries)` for inter-broker fan-out.
fn alter_broker_resource(
    broker: &Broker,
    principal: &str,
    entries: &[(String, String)],
    validate_only: bool,
) -> (i16, Option<String>, Option<(u64, Vec<(String, String)>)>) {
    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        )
    {
        return (
            KafkaErrorCode::ClusterAuthorizationFailed.as_i16(),
            None,
            None,
        );
    }
    // Phase 113: cluster BROKER alter is controller-only (before name checks on
    // non-controller would still reject with NotController for the right op).
    if !validate_only && broker.cluster_config().is_some() && !broker.is_controller() {
        return (
            KafkaErrorCode::NotController.as_i16(),
            Some(format!(
                "not controller; controller_id={}",
                broker.controller_id()
            )),
            None,
        );
    }
    if validate_only {
        return match crate::broker_config::validate_entries(entries) {
            Ok(()) => (KafkaErrorCode::None.as_i16(), None, None),
            Err(Error::InvalidArgument(msg)) => {
                (KafkaErrorCode::InvalidConfig.as_i16(), Some(msg), None)
            }
            Err(e) => (
                KafkaErrorCode::InvalidConfig.as_i16(),
                Some(e.to_string()),
                None,
            ),
        };
    }
    match broker.alter_broker_configs(entries) {
        Ok(Some(gen)) => (
            KafkaErrorCode::None.as_i16(),
            None,
            Some((gen, entries.to_vec())),
        ),
        Ok(None) => (KafkaErrorCode::None.as_i16(), None, None),
        Err(Error::InvalidArgument(msg)) if msg.starts_with("not controller") => {
            (KafkaErrorCode::NotController.as_i16(), Some(msg), None)
        }
        Err(Error::InvalidArgument(msg)) => {
            (KafkaErrorCode::InvalidConfig.as_i16(), Some(msg), None)
        }
        Err(e) => (KafkaErrorCode::Unknown.as_i16(), Some(e.to_string()), None),
    }
}

/// IncrementalAlterConfigs (API 44) classic v0 + flexible v1.
///
/// Kafka `ConfigOperation`: 0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT.
/// Volant topic configs only support SET and DELETE (clear via empty value).
/// Encode IncrementalAlterConfigs response. Returns BROKER fan-out jobs.
pub(crate) fn encode_incremental_alter_configs(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) -> Vec<(u64, Vec<(String, String)>)> {
    let flexible = version >= 1;
    let mut fanouts: Vec<(u64, Vec<(String, String)>)> = Vec::new();
    /// Kafka ConfigOperation::Set.
    const OP_SET: i8 = 0;
    /// Kafka ConfigOperation::Delete.
    const OP_DELETE: i8 = 1;

    struct Res {
        rtype: i8,
        name: String,
        entries: Vec<(String, String)>,
        parse_err: Option<String>,
    }

    let mut resources = Vec::new();
    let validate_only;

    if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => {
                for _ in 0..n {
                    if src.remaining() < 1 {
                        break;
                    }
                    let rtype = src.get_i8();
                    let name = match get_compact_string(src) {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let mut entries = Vec::new();
                    let mut parse_err = None;
                    if let Ok(Some(cfg_count)) = get_compact_array_len(src) {
                        for _ in 0..cfg_count {
                            let key = match get_compact_string(src) {
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
                            let value = match get_compact_nullable_string(src) {
                                Ok(v) => v.unwrap_or_default(),
                                Err(_) => {
                                    if parse_err.is_none() {
                                        parse_err = Some("invalid config value".into());
                                    }
                                    break;
                                }
                            };
                            let _ = skip_tag_buffer(src);
                            if parse_err.is_some() {
                                continue;
                            }
                            match op {
                                OP_SET => entries.push((key, value)),
                                OP_DELETE => entries.push((key, String::new())),
                                2 | 3 => {
                                    parse_err = Some(
                                        "APPEND/SUBTRACT not supported (no list-typed configs)"
                                            .into(),
                                    );
                                }
                                other => {
                                    parse_err = Some(format!("unknown config operation {other}"));
                                }
                            }
                        }
                    }
                    let _ = skip_tag_buffer(src);
                    resources.push(Res {
                        rtype,
                        name,
                        entries,
                        parse_err,
                    });
                }
            }
            Ok(None) | Err(_) => {}
        }
        validate_only = if src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
        let _ = skip_tag_buffer(src);
    } else {
        if src.remaining() < 4 {
            out.put_i32(0); // throttle
            out.put_i32(0);
            return fanouts;
        }
        let n = src.get_i32();
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
                        parse_err =
                            Some("APPEND/SUBTRACT not supported (no list-typed configs)".into());
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
        validate_only = if src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
    }

    out.put_i32(0); // throttle
    if flexible {
        put_compact_array_len(out, resources.len());
    } else {
        out.put_i32(resources.len() as i32);
    }
    for r in resources {
        let (code, msg): (i16, Option<String>) = if let Some(msg) = r.parse_err {
            (KafkaErrorCode::InvalidConfig.as_i16(), Some(msg))
        } else {
            match r.rtype {
                RES_BROKER => {
                    // Phase 103: name must be empty or this broker's node_id decimal.
                    if !broker_resource_name_matches(broker.node_id(), &r.name) {
                        (
                            KafkaErrorCode::InvalidRequest.as_i16(),
                            Some(invalid_broker_resource_name_msg(broker.node_id())),
                        )
                    } else {
                        let (code, msg, fanout) =
                            alter_broker_resource(broker, principal, &r.entries, validate_only);
                        if let Some(job) = fanout {
                            fanouts.push(job);
                        }
                        (code, msg)
                    }
                }
                RES_TOPIC => {
                    if broker.acls().is_enabled()
                        && !broker.acls().authorize(
                            Some(principal),
                            ResourceType::Topic,
                            &r.name,
                            AclOperation::Alter,
                        )
                    {
                        (KafkaErrorCode::TopicAuthorizationFailed.as_i16(), None)
                    } else if validate_only {
                        match volant_broker_topic_config_validate(&r.entries) {
                            Ok(()) => (KafkaErrorCode::None.as_i16(), None),
                            Err(msg) => (KafkaErrorCode::InvalidConfig.as_i16(), Some(msg)),
                        }
                    } else {
                        match broker.alter_configs(&r.name, &r.entries) {
                            Ok(_) => (KafkaErrorCode::None.as_i16(), None),
                            Err(Error::NotFound(_)) => (
                                KafkaErrorCode::UnknownTopicOrPartition.as_i16(),
                                Some("topic not found".into()),
                            ),
                            Err(Error::InvalidArgument(msg)) => {
                                (KafkaErrorCode::InvalidConfig.as_i16(), Some(msg))
                            }
                            Err(_) => (KafkaErrorCode::Unknown.as_i16(), None),
                        }
                    }
                }
                _ => (
                    KafkaErrorCode::InvalidRequest.as_i16(),
                    Some(unsupported_config_resource_msg().into()),
                ),
            }
        };
        out.put_i16(code);
        if flexible {
            put_compact_nullable_string(out, msg.as_deref());
            out.put_i8(r.rtype);
            put_compact_string(out, &r.name);
            put_empty_tag_buffer(out);
        } else {
            put_nullable_string(out, msg.as_deref());
            out.put_i8(r.rtype);
            put_string(out, &r.name);
        }
    }
    if flexible {
        put_empty_tag_buffer(out);
    }
    fanouts
}

fn kafka_scram_hash(mech: i8) -> Option<ScramHash> {
    match mech {
        KAFKA_SCRAM_SHA_256 => Some(ScramHash::Sha256),
        KAFKA_SCRAM_SHA_512 => Some(ScramHash::Sha512),
        _ => None,
    }
}

fn scram_hash_to_kafka(hash: ScramHash) -> i8 {
    match hash {
        ScramHash::Sha256 => KAFKA_SCRAM_SHA_256,
        ScramHash::Sha512 => KAFKA_SCRAM_SHA_512,
    }
}

fn write_describe_scram_result(
    out: &mut BytesMut,
    user: &str,
    code: KafkaErrorCode,
    msg: Option<&str>,
    infos: &[(ScramHash, u32)],
) {
    put_compact_string(out, user);
    out.put_i16(code.as_i16());
    put_compact_nullable_string(out, msg);
    put_compact_array_len(out, infos.len());
    for &(hash, iter) in infos {
        out.put_i8(scram_hash_to_kafka(hash));
        out.put_i32(iter as i32);
        put_empty_tag_buffer(out);
    }
    put_empty_tag_buffer(out);
}

fn write_alter_scram_result(
    out: &mut BytesMut,
    user: &str,
    code: KafkaErrorCode,
    msg: Option<&str>,
) {
    put_compact_string(out, user);
    out.put_i16(code.as_i16());
    put_compact_nullable_string(out, msg);
    put_empty_tag_buffer(out);
}

/// DescribeUserScramCredentials v0 (always flexible). Empty users = all.
///
/// Unknown user → per-result Kafka **91** `RESOURCE_NOT_FOUND`.
/// ACL: Cluster DESCRIBE (disabled ACLs allow).
pub(crate) fn encode_describe_user_scram_credentials(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    let mut names: Vec<String> = Vec::new();
    let describe_all = match get_compact_array_len(src) {
        Ok(None) | Ok(Some(0)) => true,
        Ok(Some(n)) => {
            for _ in 0..n {
                match get_compact_string(src) {
                    Ok(s) => names.push(s),
                    Err(_) => break,
                }
                let _ = skip_tag_buffer(src);
            }
            false
        }
        Err(_) => true,
    };
    let _ = skip_tag_buffer(src);

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        )
    {
        out.put_i32(0);
        out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        put_compact_nullable_string(out, Some("cluster authorization failed"));
        put_compact_array_len(out, 0);
        put_empty_tag_buffer(out);
        return;
    }

    if describe_all {
        let all = broker.scram().describe_all();
        out.put_i32(0);
        out.put_i16(KafkaErrorCode::None.as_i16());
        put_compact_nullable_string(out, None);
        put_compact_array_len(out, all.len());
        for (user, infos) in &all {
            write_describe_scram_result(out, user, KafkaErrorCode::None, None, infos);
        }
        put_empty_tag_buffer(out);
        return;
    }

    out.put_i32(0);
    out.put_i16(KafkaErrorCode::None.as_i16());
    put_compact_nullable_string(out, None);
    put_compact_array_len(out, names.len());
    for name in names {
        match broker.scram().describe_user(&name) {
            Some(infos) => {
                write_describe_scram_result(out, &name, KafkaErrorCode::None, None, &infos);
            }
            None => {
                write_describe_scram_result(
                    out,
                    &name,
                    KafkaErrorCode::ResourceNotFound,
                    Some("user not found"),
                    &[],
                );
            }
        }
    }
    put_empty_tag_buffer(out);
}

struct ScramDeletion {
    name: String,
    mechanism: i8,
}

struct ScramUpsertion {
    name: String,
    mechanism: i8,
    iterations: i32,
    salt: Vec<u8>,
    salted: Vec<u8>,
}

/// AlterUserScramCredentials v0 (always flexible). Upsert takes saltedPassword.
///
/// One result row per unique user. ACL: Cluster ALTER (disabled ACLs allow).
pub(crate) fn encode_alter_user_scram_credentials(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    let mut deletions = Vec::new();
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                let name = match get_compact_string(src) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                if src.remaining() < 1 {
                    break;
                }
                let mechanism = src.get_i8();
                let _ = skip_tag_buffer(src);
                deletions.push(ScramDeletion { name, mechanism });
            }
        }
        Ok(None) | Err(_) => {}
    }

    let mut upsertions = Vec::new();
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                let name = match get_compact_string(src) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                if src.remaining() < 1 + 4 {
                    break;
                }
                let mechanism = src.get_i8();
                let iterations = src.get_i32();
                let salt = match get_compact_bytes(src) {
                    Ok(Some(b)) => b.to_vec(),
                    Ok(None) | Err(_) => Vec::new(),
                };
                let salted = match get_compact_bytes(src) {
                    Ok(Some(b)) => b.to_vec(),
                    Ok(None) | Err(_) => Vec::new(),
                };
                let _ = skip_tag_buffer(src);
                upsertions.push(ScramUpsertion {
                    name,
                    mechanism,
                    iterations,
                    salt,
                    salted,
                });
            }
        }
        Ok(None) | Err(_) => {}
    }
    let _ = skip_tag_buffer(src);

    let mut order = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for d in &deletions {
        if seen.insert(d.name.clone()) {
            order.push(d.name.clone());
        }
    }
    for u in &upsertions {
        if seen.insert(u.name.clone()) {
            order.push(u.name.clone());
        }
    }

    out.put_i32(0); // throttle
    put_compact_array_len(out, order.len());

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        )
    {
        for user in &order {
            write_alter_scram_result(
                out,
                user,
                KafkaErrorCode::ClusterAuthorizationFailed,
                Some("cluster authorization failed"),
            );
        }
        put_empty_tag_buffer(out);
        return;
    }

    for user in &order {
        let (code, msg) = apply_alter_user_scram(broker, user, &deletions, &upsertions);
        write_alter_scram_result(out, user, code, msg.as_deref());
    }
    put_empty_tag_buffer(out);
}

fn apply_alter_user_scram(
    broker: &Broker,
    user: &str,
    deletions: &[ScramDeletion],
    upsertions: &[ScramUpsertion],
) -> (KafkaErrorCode, Option<String>) {
    let dels: Vec<&ScramDeletion> = deletions.iter().filter(|d| d.name == user).collect();
    let ups: Vec<&ScramUpsertion> = upsertions.iter().filter(|u| u.name == user).collect();

    for d in &dels {
        let Some(hash) = kafka_scram_hash(d.mechanism) else {
            return (
                KafkaErrorCode::InvalidRequest,
                Some("unsupported SCRAM mechanism".into()),
            );
        };
        if !broker.scram().has_mechanism(user, hash) {
            return (
                KafkaErrorCode::ResourceNotFound,
                Some("user or mechanism not found".into()),
            );
        }
    }
    for u in &ups {
        if u.name.is_empty() || u.name.contains(',') || u.name.contains('=') {
            return (
                KafkaErrorCode::InvalidRequest,
                Some("invalid SCRAM username".into()),
            );
        }
        if kafka_scram_hash(u.mechanism).is_none() {
            return (
                KafkaErrorCode::InvalidRequest,
                Some("unsupported SCRAM mechanism".into()),
            );
        }
        if u.iterations <= 0 || u.salt.is_empty() || u.salted.is_empty() {
            return (
                KafkaErrorCode::InvalidRequest,
                Some("invalid iterations, salt, or saltedPassword".into()),
            );
        }
    }

    for d in &dels {
        let hash = kafka_scram_hash(d.mechanism).expect("validated");
        if let Err(e) = broker.scram().delete_mechanism(user, hash) {
            return (KafkaErrorCode::Unknown, Some(e.to_string()));
        }
    }
    for u in &ups {
        let hash = kafka_scram_hash(u.mechanism).expect("validated");
        if let Err(e) =
            broker
                .scram()
                .upsert_from_salted(user, hash, u.iterations as u32, &u.salt, &u.salted)
        {
            return (KafkaErrorCode::InvalidRequest, Some(e.to_string()));
        }
    }
    (KafkaErrorCode::None, None)
}

/// UpdateFeatures v0–1 (always flexible). Parse and reject every feature.
///
/// Does not persist finalized features. ApiVersions SupportedFeatures /
/// FinalizedFeatures stay empty. Not KIP-584.
///
/// Cluster `--cluster-config` + non-controller → top-level **41**.
/// Single-node is allowed and still rejects each feature (**92**).
/// ACL: Cluster **ALTER** (disabled ACLs allow).
pub(crate) fn encode_update_features(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    if src.remaining() >= 4 {
        let _timeout_ms = src.get_i32();
    }

    let mut features = Vec::new();
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                let name = match get_compact_string(src) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                if src.remaining() < 2 {
                    break;
                }
                let _max_version_level = src.get_i16();
                if version >= 1 {
                    if src.remaining() >= 1 {
                        let _upgrade_type = src.get_i8();
                    }
                } else if src.remaining() >= 1 {
                    let _allow_downgrade = src.get_u8();
                }
                let _ = skip_tag_buffer(src);
                features.push(name);
            }
        }
        Ok(None) | Err(_) => {}
    }
    if version >= 1 && src.remaining() >= 1 {
        let _validate_only = src.get_u8();
    }
    let _ = skip_tag_buffer(src);

    let write_top = |out: &mut BytesMut, code: KafkaErrorCode, msg: Option<&str>, n: usize| {
        out.put_i32(0); // throttle
        out.put_i16(code.as_i16());
        put_compact_nullable_string(out, msg);
        put_compact_array_len(out, n);
    };

    if broker.cluster_config().is_some() && !broker.is_controller() {
        let msg = format!("not controller; controller_id={}", broker.controller_id());
        write_top(out, KafkaErrorCode::NotController, Some(&msg), 0);
        put_empty_tag_buffer(out);
        return;
    }

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        )
    {
        write_top(
            out,
            KafkaErrorCode::ClusterAuthorizationFailed,
            Some("cluster authorization failed"),
            0,
        );
        put_empty_tag_buffer(out);
        return;
    }

    write_top(out, KafkaErrorCode::None, None, features.len());
    for name in &features {
        put_compact_string(out, name);
        out.put_i16(KafkaErrorCode::FeatureUpdateFailed.as_i16());
        put_compact_nullable_string(out, Some("empty / not supported"));
        put_empty_tag_buffer(out);
    }
    put_empty_tag_buffer(out);
}

/// DescribeQuorum v0–1 (always flexible). Wraps openraft leader/term/voters.
///
/// Not KRaft `__cluster_metadata` (no invented metadata topic). Per-replica
/// `lastFetch` / `lastCaughtUp` are **-1**. ReplicaDirectoryId is v2-only
/// and is not advertised.
///
/// Empty request `topics` → one synthetic cluster partition **0** (empty
/// name) using `openraft_voter_ids()` when raft is started. Flag off /
/// raft not started: top-level **0**, empty topics. Cluster + not
/// controller: **41**. ACL: Cluster **DESCRIBE**.
pub(crate) fn encode_describe_quorum(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    struct TopicReq {
        name: String,
        partitions: Vec<i32>,
    }

    let mut topics = Vec::new();
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                let name = match get_compact_string(src) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let mut partitions = Vec::new();
                match get_compact_array_len(src) {
                    Ok(Some(pc)) => {
                        for _ in 0..pc {
                            if src.remaining() < 4 {
                                break;
                            }
                            partitions.push(src.get_i32());
                            let _ = skip_tag_buffer(src);
                        }
                    }
                    Ok(None) | Err(_) => {}
                }
                let _ = skip_tag_buffer(src);
                topics.push(TopicReq { name, partitions });
            }
        }
        Ok(None) | Err(_) => {}
    }
    let _ = skip_tag_buffer(src);

    let write_empty = |out: &mut BytesMut, code: KafkaErrorCode| {
        out.put_i16(code.as_i16());
        put_compact_array_len(out, 0);
        put_empty_tag_buffer(out);
    };

    if broker.cluster_config().is_some() && !broker.is_controller() {
        write_empty(out, KafkaErrorCode::NotController);
        return;
    }

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        )
    {
        write_empty(out, KafkaErrorCode::ClusterAuthorizationFailed);
        return;
    }

    // Single-node / overlay-only / raft not started: honest empty + 0.
    if !broker.openraft_started() {
        write_empty(out, KafkaErrorCode::None);
        return;
    }

    let leader = broker
        .openraft_leader_id()
        .map(|id| id as i32)
        .unwrap_or(-1);
    let epoch = i32::try_from(broker.openraft_term()).unwrap_or(i32::MAX);
    let voters = broker.openraft_voter_ids();
    let local_id = broker.node_id();

    let report: Vec<(String, Vec<i32>)> = if topics.is_empty() {
        vec![(String::new(), vec![0])]
    } else {
        topics.into_iter().map(|t| (t.name, t.partitions)).collect()
    };

    out.put_i16(KafkaErrorCode::None.as_i16());
    put_compact_array_len(out, report.len());
    for (name, parts) in &report {
        put_compact_string(out, name);
        put_compact_array_len(out, parts.len());
        for &p in parts {
            let (hwm, local_leo) = local_quorum_offsets(broker, name, p);
            out.put_i32(p);
            out.put_i16(0); // partition error
            out.put_i32(leader);
            out.put_i32(epoch);
            out.put_i64(hwm);
            put_compact_array_len(out, voters.len());
            for &vid in &voters {
                out.put_i32(vid as i32);
                let leo = if vid == local_id { local_leo } else { 0 };
                out.put_i64(leo);
                if version >= 1 {
                    out.put_i64(-1); // lastFetchTimestamp
                    out.put_i64(-1); // lastCaughtUpTimestamp
                }
                put_empty_tag_buffer(out);
            }
            put_compact_array_len(out, 0); // observers
            put_empty_tag_buffer(out);
        }
        put_empty_tag_buffer(out);
    }
    put_empty_tag_buffer(out);
}

fn local_quorum_offsets(broker: &Broker, name: &str, partition: i32) -> (i64, i64) {
    if name.is_empty() || partition < 0 {
        return (0, 0);
    }
    let topic = TopicName::new(name);
    let pid = PartitionId(partition as u32);
    let hwm = broker.high_watermark(&topic, pid).unwrap_or(0) as i64;
    let leo = broker.log_end_offset(&topic, pid).unwrap_or(0) as i64;
    (hwm, leo)
}

/// Default Kafka producer-id block size (`producerIdLen`).
const DEFAULT_PRODUCER_ID_BLOCK_LEN: u32 = 1000;

/// AllocateProducerIds v0 (always flexible). Wraps
/// [`Broker::allocate_producer_ids`] with a default block of 1000.
///
/// BrokerId / BrokerEpoch are parsed and ignored (not KRaft fencing).
/// Cluster + non-controller → **41**. Single-node is allowed (this
/// process is the allocator). ACL: Cluster **ALTER** (disabled ACLs
/// allow). Same persist path as InitProducerId (`__producer_state`).
pub(crate) fn encode_allocate_producer_ids(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    let write = |out: &mut BytesMut, code: KafkaErrorCode, start: i64, len: i32| {
        out.put_i32(0); // throttle
        out.put_i16(code.as_i16());
        out.put_i64(start);
        out.put_i32(len);
        put_empty_tag_buffer(out);
    };

    if src.remaining() < 4 + 8 {
        write(out, KafkaErrorCode::InvalidRequest, 0, 0);
        return;
    }
    let _broker_id = src.get_i32();
    let _broker_epoch = src.get_i64();
    let _ = skip_tag_buffer(src);

    if broker.cluster_config().is_some() && !broker.is_controller() {
        write(out, KafkaErrorCode::NotController, 0, 0);
        return;
    }

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        )
    {
        write(out, KafkaErrorCode::ClusterAuthorizationFailed, 0, 0);
        return;
    }

    let (start, len) = broker.allocate_producer_ids(DEFAULT_PRODUCER_ID_BLOCK_LEN);
    write(out, KafkaErrorCode::None, start as i64, len as i32);
}

/// One partition from an official AlterPartition v0 request.
struct AlterPartitionReq {
    index: i32,
    leader_epoch: i32,
    new_isr: Vec<i32>,
    partition_epoch: i32,
}

/// One topic from an official AlterPartition v0 request.
struct AlterPartitionTopicReq {
    name: String,
    partitions: Vec<AlterPartitionReq>,
}

/// AlterPartition v0 (always flexible). Wraps
/// [`Broker::apply_leader_isr_update`].
///
/// Official Kafka v0 (3.7 schema): TopicName + NewIsr `[]int32`. TopicId
/// is v2, LeaderRecoveryState is v1, NewIsrWithEpochs is v3 — not parsed
/// here. BrokerEpoch is parsed and ignored. Not KRaft NewIsrEpoch / ELR
/// / DirectoryId. Controller only in cluster (per-partition **41**).
/// Single-node is a no-op **0**. ACL: Cluster **ALTER**.
pub(crate) fn encode_alter_partition(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    let broker_id = if src.remaining() >= 4 {
        src.get_i32()
    } else {
        0
    };
    if src.remaining() >= 8 {
        let _broker_epoch = src.get_i64();
    }
    let topics = parse_alter_partition_topics(src);
    let _ = skip_tag_buffer(src);

    let acl_denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );

    out.put_i32(0); // throttleTimeMs
                    // Official v0 has a top-level ErrorCode. ACL deny uses it; native
                    // NotController is already per-partition from apply.
    let top = if acl_denied {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::None
    };
    out.put_i16(top.as_i16());
    put_compact_array_len(out, topics.len());
    for t in &topics {
        put_compact_string(out, &t.name);
        put_compact_array_len(out, t.partitions.len());
        for p in &t.partitions {
            let (err, leader, epoch, isr, part_epoch) = if acl_denied {
                (
                    KafkaErrorCode::ClusterAuthorizationFailed,
                    0,
                    0,
                    Vec::new(),
                    0,
                )
            } else {
                apply_one_alter_partition(broker, broker_id, &t.name, p)
            };
            out.put_i32(p.index);
            out.put_i16(err.as_i16());
            out.put_i32(leader);
            out.put_i32(epoch);
            put_compact_array_len(out, isr.len());
            for id in isr {
                out.put_i32(id);
            }
            out.put_i32(part_epoch);
            put_empty_tag_buffer(out);
        }
        put_empty_tag_buffer(out);
    }
    put_empty_tag_buffer(out);
}

fn parse_alter_partition_topics(src: &mut impl Buf) -> Vec<AlterPartitionTopicReq> {
    let mut topics = Vec::new();
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                let name = match get_compact_string(src) {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let mut partitions = Vec::new();
                match get_compact_array_len(src) {
                    Ok(Some(pc)) => {
                        for _ in 0..pc {
                            if src.remaining() < 4 + 4 {
                                break;
                            }
                            let index = src.get_i32();
                            let leader_epoch = src.get_i32();
                            let mut new_isr = Vec::new();
                            match get_compact_array_len(src) {
                                Ok(Some(ic)) => {
                                    for _ in 0..ic {
                                        if src.remaining() < 4 {
                                            break;
                                        }
                                        new_isr.push(src.get_i32());
                                    }
                                }
                                Ok(None) | Err(_) => {}
                            }
                            // Official v0: PartitionEpoch next (LeaderRecoveryState is v1+).
                            let partition_epoch = if src.remaining() >= 4 {
                                src.get_i32()
                            } else {
                                0
                            };
                            let _ = skip_tag_buffer(src);
                            partitions.push(AlterPartitionReq {
                                index,
                                leader_epoch,
                                new_isr,
                                partition_epoch,
                            });
                        }
                    }
                    Ok(None) | Err(_) => {}
                }
                let _ = skip_tag_buffer(src);
                topics.push(AlterPartitionTopicReq { name, partitions });
            }
        }
        Ok(None) | Err(_) => {}
    }
    topics
}

fn apply_one_alter_partition(
    broker: &Broker,
    broker_id: i32,
    topic: &str,
    p: &AlterPartitionReq,
) -> (KafkaErrorCode, i32, i32, Vec<i32>, i32) {
    if broker_id < 0 || p.index < 0 || p.leader_epoch < 0 || p.new_isr.iter().any(|&id| id < 0) {
        return (KafkaErrorCode::InvalidRequest, 0, 0, Vec::new(), 0);
    }
    let isr: Vec<u32> = p.new_isr.iter().map(|&id| id as u32).collect();
    let generation_hint = if p.partition_epoch < 0 {
        0
    } else {
        p.partition_epoch as u32
    };
    let (code, _gen) = broker.apply_leader_isr_update(
        topic,
        p.index as u32,
        broker_id as u32,
        p.leader_epoch as u32,
        &isr,
        generation_hint,
    );
    let err = map_isr_apply_error(code);
    if err != KafkaErrorCode::None {
        return (err, 0, 0, Vec::new(), 0);
    }
    match current_alter_partition_state(broker, topic, p.index) {
        Some((leader, epoch, cur_isr, part_epoch)) => (err, leader, epoch, cur_isr, part_epoch),
        None => (err, 0, 0, Vec::new(), 0),
    }
}

fn map_isr_apply_error(code: u16) -> KafkaErrorCode {
    match code {
        0 => KafkaErrorCode::None,
        // volant_protocol::ErrorCode
        2 => KafkaErrorCode::UnknownTopicOrPartition, // NotFound
        3 => KafkaErrorCode::InvalidRequest,          // InvalidArg
        13 => KafkaErrorCode::NotLeaderForPartition,
        14 => KafkaErrorCode::NotController,
        19 => KafkaErrorCode::FencedLeaderEpoch, // InvalidProducerEpoch
        _ => KafkaErrorCode::Unknown,
    }
}

fn current_alter_partition_state(
    broker: &Broker,
    topic: &str,
    partition: i32,
) -> Option<(i32, i32, Vec<i32>, i32)> {
    if partition < 0 {
        return None;
    }
    let pid = partition as u32;
    if let Some(asg) = broker.clone_live_assignment() {
        let pa = asg.topics.get(topic).and_then(|t| t.partitions.get(&pid))?;
        return Some((
            pa.leader as i32,
            pa.leader_epoch as i32,
            pa.isr.iter().map(|&id| id as i32).collect(),
            asg.generation as i32,
        ));
    }
    let snap = broker.metadata(Some(&[TopicName::new(topic)]));
    let p = snap
        .topics
        .first()
        .and_then(|t| t.partitions.iter().find(|p| p.partition_id.0 == pid))?;
    Some((
        p.leader as i32,
        p.leader_epoch as i32,
        p.isr.iter().map(|&id| id as i32).collect(),
        0,
    ))
}

/// One topic's partitions from an AssignReplicasToDirs request.
struct AssignReplicasToDirsTopic {
    topic_id: [u8; 16],
    partitions: Vec<i32>,
}

/// One directory assignment from an AssignReplicasToDirs request.
struct AssignReplicasToDirsDirectory {
    id: [u8; 16],
    topics: Vec<AssignReplicasToDirsTopic>,
}

/// AssignReplicasToDirs v0 (always flexible). Single `data_dir`.
///
/// Parses the request and rejects every assignment with **42**
/// `INVALID_REQUEST`. Does not move files. Does not invent DirectoryId
/// storage. Not KRaft. BrokerId / BrokerEpoch are parsed and ignored.
/// Controller is not required (local dirs). ACL: Cluster **ALTER**
/// (disabled ACLs allow). Denied → top-level **31**, empty directories.
pub(crate) fn encode_assign_replicas_to_dirs(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    let directories = parse_assign_replicas_to_dirs(src);

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        )
    {
        write_assign_replicas_to_dirs(out, KafkaErrorCode::ClusterAuthorizationFailed, &[]);
        return;
    }

    // Resolve TopicIds for honesty; unknown ids are still echoed with 42
    // (do not fail the whole request).
    for d in &directories {
        for t in &d.topics {
            let _ = topic_id::name_for_uuid(broker, &t.topic_id);
        }
    }

    write_assign_replicas_to_dirs(out, KafkaErrorCode::None, &directories);
}

fn write_assign_replicas_to_dirs(
    out: &mut BytesMut,
    top: KafkaErrorCode,
    directories: &[AssignReplicasToDirsDirectory],
) {
    out.put_i32(0); // throttleTimeMs
    out.put_i16(top.as_i16());
    put_compact_array_len(out, directories.len());
    for d in directories {
        put_uuid(out, &d.id);
        put_compact_array_len(out, d.topics.len());
        for t in &d.topics {
            put_uuid(out, &t.topic_id);
            put_compact_array_len(out, t.partitions.len());
            for &p in &t.partitions {
                out.put_i32(p);
                out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
                put_empty_tag_buffer(out);
            }
            put_empty_tag_buffer(out);
        }
        put_empty_tag_buffer(out);
    }
    put_empty_tag_buffer(out);
}

fn parse_assign_replicas_to_dirs(src: &mut impl Buf) -> Vec<AssignReplicasToDirsDirectory> {
    let mut directories = Vec::new();
    if src.remaining() < 4 + 8 {
        return directories;
    }
    let _broker_id = src.get_i32();
    let _broker_epoch = src.get_i64();

    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                let id = match get_uuid(src) {
                    Ok(u) => u,
                    Err(_) => break,
                };
                let mut topics = Vec::new();
                match get_compact_array_len(src) {
                    Ok(Some(tn)) => {
                        for _ in 0..tn {
                            let topic_id = match get_uuid(src) {
                                Ok(u) => u,
                                Err(_) => break,
                            };
                            let mut partitions = Vec::new();
                            match get_compact_array_len(src) {
                                Ok(Some(pc)) => {
                                    for _ in 0..pc {
                                        if src.remaining() < 4 {
                                            break;
                                        }
                                        partitions.push(src.get_i32());
                                    }
                                }
                                Ok(None) | Err(_) => {}
                            }
                            let _ = skip_tag_buffer(src);
                            topics.push(AssignReplicasToDirsTopic {
                                topic_id,
                                partitions,
                            });
                        }
                    }
                    Ok(None) | Err(_) => {}
                }
                let _ = skip_tag_buffer(src);
                directories.push(AssignReplicasToDirsDirectory { id, topics });
            }
        }
        Ok(None) | Err(_) => {}
    }
    let _ = skip_tag_buffer(src);
    directories
}

/// One topic's partitions from an AlterReplicaLogDirs request.
struct AlterReplicaLogDirTopic {
    name: String,
    partitions: Vec<i32>,
}

/// AlterReplicaLogDirs v0 classic / v1 flexible. Single `data_dir`.
///
/// Parses the request and rejects every directory move with **42**
/// `INVALID_REQUEST`. Does not move files. Not multi-log.dirs.
/// Controller is not required (local dirs). Official Kafka first
/// flexible version is **2**; Volant treats v1 as first flexible.
/// ACL: Cluster ALTER, or Topic ALTER per named topic. Disabled ACLs
/// allow.
pub(crate) fn encode_alter_replica_log_dirs(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    let flexible = version >= 1;
    let topics = parse_alter_replica_log_dirs(src, flexible);
    if flexible {
        let _ = skip_tag_buffer(src);
    }

    let cluster_ok = !broker.acls().is_enabled()
        || broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );

    out.put_i32(0); // throttle
    if flexible {
        put_compact_array_len(out, topics.len());
        for t in &topics {
            put_compact_string(out, &t.name);
            put_compact_array_len(out, t.partitions.len());
            let code = alter_replica_log_dir_error(broker, principal, cluster_ok, &t.name);
            for &p in &t.partitions {
                out.put_i32(p);
                out.put_i16(code);
                put_empty_tag_buffer(out);
            }
            put_empty_tag_buffer(out);
        }
        put_empty_tag_buffer(out);
    } else {
        out.put_i32(topics.len() as i32);
        for t in &topics {
            put_string(out, &t.name);
            out.put_i32(t.partitions.len() as i32);
            let code = alter_replica_log_dir_error(broker, principal, cluster_ok, &t.name);
            for &p in &t.partitions {
                out.put_i32(p);
                out.put_i16(code);
            }
        }
    }
}

fn alter_replica_log_dir_error(
    broker: &Broker,
    principal: &str,
    cluster_ok: bool,
    topic: &str,
) -> i16 {
    if cluster_ok
        || broker.acls().authorize(
            Some(principal),
            ResourceType::Topic,
            topic,
            AclOperation::Alter,
        )
    {
        KafkaErrorCode::InvalidRequest.as_i16()
    } else {
        KafkaErrorCode::TopicAuthorizationFailed.as_i16()
    }
}

fn parse_alter_replica_log_dirs(
    src: &mut impl Buf,
    flexible: bool,
) -> Vec<AlterReplicaLogDirTopic> {
    let mut topics: Vec<AlterReplicaLogDirTopic> = Vec::new();
    let mut push = |name: String, partitions: Vec<i32>| {
        if let Some(existing) = topics.iter_mut().find(|t| t.name == name) {
            existing.partitions.extend(partitions);
        } else {
            topics.push(AlterReplicaLogDirTopic { name, partitions });
        }
    };

    if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => {
                for _ in 0..n {
                    if get_compact_string(src).is_err() {
                        break;
                    }
                    match get_compact_array_len(src) {
                        Ok(Some(tn)) => {
                            for _ in 0..tn {
                                let name = match get_compact_string(src) {
                                    Ok(s) => s,
                                    Err(_) => break,
                                };
                                let mut partitions = Vec::new();
                                match get_compact_array_len(src) {
                                    Ok(Some(pc)) => {
                                        for _ in 0..pc {
                                            if src.remaining() < 4 {
                                                break;
                                            }
                                            partitions.push(src.get_i32());
                                        }
                                    }
                                    Ok(None) | Err(_) => {}
                                }
                                let _ = skip_tag_buffer(src);
                                push(name, partitions);
                            }
                        }
                        Ok(None) | Err(_) => {}
                    }
                    let _ = skip_tag_buffer(src);
                }
            }
            Ok(None) | Err(_) => {}
        }
    } else {
        if src.remaining() < 4 {
            return topics;
        }
        let n = src.get_i32();
        if n < 0 {
            return topics;
        }
        for _ in 0..n {
            if get_string(src).is_err() {
                break;
            }
            if src.remaining() < 4 {
                break;
            }
            let tn = src.get_i32();
            if tn < 0 {
                continue;
            }
            for _ in 0..tn {
                let name = match get_string(src) {
                    Ok(s) => s,
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
                push(name, partitions);
            }
        }
    }
    topics
}

/// DescribeLogDirs v0 classic / v1 flexible. Local open partitions only.
///
/// `topics = null` → every local partition. Named topic with empty
/// `partitions` → all local partitions of that topic. Unknown topic is
/// omitted (empty). ACL: Cluster DESCRIBE, or Topic DESCRIBE per named
/// topic. Disabled ACLs allow.
pub(crate) fn encode_describe_log_dirs(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    let flexible = version >= 1;
    let filter = parse_describe_log_dirs_topics(src, flexible);
    if flexible {
        let _ = skip_tag_buffer(src);
    }

    let cluster_ok = !broker.acls().is_enabled()
        || broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        );

    let (dir_error, rows) = match &filter {
        LocalLogDirFilter::All => {
            if broker.acls().is_enabled() && !cluster_ok {
                (
                    KafkaErrorCode::ClusterAuthorizationFailed.as_i16(),
                    Vec::new(),
                )
            } else {
                (0, broker.local_log_dir_rows(&filter))
            }
        }
        LocalLogDirFilter::Topics(topics) => {
            if !broker.acls().is_enabled() || cluster_ok {
                (0, broker.local_log_dir_rows(&filter))
            } else {
                let allowed: Vec<(String, Vec<i32>)> = topics
                    .iter()
                    .filter(|(name, _)| {
                        broker.acls().authorize(
                            Some(principal),
                            ResourceType::Topic,
                            name,
                            AclOperation::Describe,
                        )
                    })
                    .cloned()
                    .collect();
                (
                    0,
                    broker.local_log_dir_rows(&LocalLogDirFilter::Topics(allowed)),
                )
            }
        }
    };

    write_describe_log_dirs(
        out,
        flexible,
        dir_error,
        &broker.local_log_dir_path(),
        &rows,
    );
}

fn parse_describe_log_dirs_topics(src: &mut impl Buf, flexible: bool) -> LocalLogDirFilter {
    if flexible {
        match get_compact_array_len(src) {
            Ok(None) => LocalLogDirFilter::All,
            Ok(Some(n)) => {
                let mut topics = Vec::with_capacity(n);
                for _ in 0..n {
                    let name = match get_compact_string(src) {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let mut partitions = Vec::new();
                    match get_compact_array_len(src) {
                        Ok(Some(pc)) => {
                            for _ in 0..pc {
                                if src.remaining() < 4 {
                                    break;
                                }
                                partitions.push(src.get_i32());
                            }
                        }
                        Ok(None) | Err(_) => {}
                    }
                    let _ = skip_tag_buffer(src);
                    topics.push((name, partitions));
                }
                LocalLogDirFilter::Topics(topics)
            }
            Err(_) => LocalLogDirFilter::Topics(Vec::new()),
        }
    } else {
        if src.remaining() < 4 {
            return LocalLogDirFilter::All;
        }
        let n = src.get_i32();
        if n < 0 {
            return LocalLogDirFilter::All;
        }
        let mut topics = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let name = match get_string(src) {
                Ok(s) => s,
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
            topics.push((name, partitions));
        }
        LocalLogDirFilter::Topics(topics)
    }
}

struct QuotaEntity {
    entity_type: String,
    entity_name: Option<String>,
}

struct AlterQuotaEntry {
    entity: Vec<QuotaEntity>,
}

/// DescribeClientQuotas v0 (always flexible). Volant has no quota store.
///
/// Parses the filter, then returns throttle=0, error=0, empty entries
/// (no matching entities). ACL: Cluster DESCRIBE (disabled ACLs allow).
pub(crate) fn encode_describe_client_quotas(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    parse_describe_client_quotas_request(src);

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        )
    {
        out.put_i32(0);
        out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        put_compact_nullable_string(out, Some("cluster authorization failed"));
        put_compact_array_len(out, 0);
        put_empty_tag_buffer(out);
        return;
    }

    out.put_i32(0);
    out.put_i16(KafkaErrorCode::None.as_i16());
    put_compact_nullable_string(out, None);
    put_compact_array_len(out, 0);
    put_empty_tag_buffer(out);
}

fn parse_describe_client_quotas_request(src: &mut impl Buf) {
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                if get_compact_string(src).is_err() {
                    break;
                }
                if src.remaining() < 1 {
                    break;
                }
                let _match_type = src.get_i8();
                let _ = get_compact_nullable_string(src);
                let _ = skip_tag_buffer(src);
            }
        }
        Ok(None) | Err(_) => {}
    }
    if src.has_remaining() {
        let _strict = src.get_u8();
    }
    let _ = skip_tag_buffer(src);
}

/// AlterClientQuotas v0 (always flexible). Volant has no quota store.
///
/// Each parsed entry is rejected with **42** `INVALID_REQUEST`
/// (`quotas not supported`). Nothing is persisted. ACL: Cluster ALTER
/// (disabled ACLs allow).
pub(crate) fn encode_alter_client_quotas(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    let entries = parse_alter_client_quotas_request(src);

    out.put_i32(0);
    put_compact_array_len(out, entries.len());

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );

    for e in &entries {
        if denied {
            out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
            put_compact_nullable_string(out, Some("cluster authorization failed"));
        } else {
            out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
            put_compact_nullable_string(out, Some("quotas not supported"));
        }
        put_compact_array_len(out, e.entity.len());
        for ent in &e.entity {
            put_compact_string(out, &ent.entity_type);
            put_compact_nullable_string(out, ent.entity_name.as_deref());
            put_empty_tag_buffer(out);
        }
        put_empty_tag_buffer(out);
    }
    put_empty_tag_buffer(out);
}

/// DescribeDelegationToken v0 (always flexible residual). Volant has
/// no delegation-token store.
///
/// Official Kafka first flexible version is **2**; Volant treats v0 as
/// flex (compact owners + tagged). Parses the owners filter (null = all)
/// and ignores it. Response matches official field order: errorCode,
/// tokens[], throttleTimeMs, tagged (no errorMessage). Always empty
/// tokens. Nothing persisted. Controller is not required.
/// ACL: Cluster DESCRIBE (disabled ACLs allow). Denied → **31**, empty
/// tokens.
pub(crate) fn encode_describe_delegation_token(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    parse_describe_delegation_token_request(src);

    let error = if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        ) {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::None
    };

    // Official DescribeDelegationTokenResponse.json: ErrorCode, Tokens,
    // ThrottleTimeMs. No errorMessage. Flex tagged trailer (residual v0).
    out.put_i16(error.as_i16());
    put_compact_array_len(out, 0);
    out.put_i32(0);
    put_empty_tag_buffer(out);
}

fn parse_describe_delegation_token_request(src: &mut impl Buf) {
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                if get_compact_string(src).is_err() {
                    break;
                }
                if get_compact_string(src).is_err() {
                    break;
                }
                let _ = skip_tag_buffer(src);
            }
        }
        Ok(None) | Err(_) => {}
    }
    let _ = skip_tag_buffer(src);
}

/// ListClientMetricsResources v0 (always flexible). Volant has no
/// client-metrics resource store (KIP-714).
///
/// Request is a tagged buffer only. Response: throttle=0, error=0,
/// empty resources (official body has no errorMessage). ACL: Cluster
/// DESCRIBE (disabled ACLs allow). Denied → **31**, empty resources.
pub(crate) fn encode_list_client_metrics_resources(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    let _ = skip_tag_buffer(src);

    let error = if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        ) {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::None
    };

    out.put_i32(0);
    out.put_i16(error.as_i16());
    put_compact_array_len(out, 0);
    put_empty_tag_buffer(out);
}

/// GetTelemetrySubscriptions v0 (always flexible). Volant has no client
/// telemetry (not KIP-714).
///
/// Parses `clientInstanceId` and leftover `subscriptionId` if present.
/// Returns error **0**, echoed id, `subscriptionId = 0`, empty
/// `requestedMetrics`, `pushIntervalMs = -1` (do not push),
/// `telemetryMaxBytes = 0`, `deltaTemporality = false`, empty accepted
/// compression. Nothing persisted. Controller is not required.
/// ACL: Cluster **DESCRIBE** (disabled ACLs allow). Denied → **31**;
/// still echo `clientInstanceId`; empty metrics.
pub(crate) fn encode_get_telemetry_subscriptions(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    let client_instance_id = get_uuid(src).unwrap_or(KAFKA_UUID_ZERO);
    // Official Kafka request is ClientInstanceId + tagged. Residual also
    // listed subscriptionId; consume it when present before tags.
    if src.remaining() >= 4 {
        let _subscription_id = src.get_i32();
    }
    let _ = skip_tag_buffer(src);

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        );
    let error = if denied {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::None
    };

    write_get_telemetry_subscriptions(out, error, &client_instance_id);
}

/// PushTelemetry v0 (always flexible). Volant has no client telemetry
/// (not KIP-714).
///
/// Parses official Kafka fields (`clientInstanceId`, `subscriptionId`,
/// `terminating`, `compressionType`, compact `metrics`) and discards
/// them. Returns throttle **0**, error **42** `INVALID_REQUEST`. Official
/// `PushTelemetryResponse.json` has no `errorMessage`. Nothing persisted.
/// Controller is not required. ACL: Cluster **ALTER** (disabled ACLs
/// allow). Denied → **31**; still nothing persisted.
pub(crate) fn encode_push_telemetry(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    let _ = get_uuid(src);
    if src.remaining() >= 4 {
        let _subscription_id = src.get_i32();
    }
    if src.remaining() >= 1 {
        let _terminating = src.get_u8();
    }
    if src.remaining() >= 1 {
        let _compression_type = src.get_i8();
    }
    let _ = get_compact_bytes(src);
    let _ = skip_tag_buffer(src);

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );
    let error = if denied {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::InvalidRequest
    };

    out.put_i32(0); // throttleTimeMs
    out.put_i16(error.as_i16());
    put_empty_tag_buffer(out);
}

/// CreateDelegationToken v0 (always flexible). Volant has no
/// delegation-token store.
///
/// Parses `renewers[] { principalType, principalName }` and
/// `maxLifeTimeMs`, then rejects with **42** `INVALID_REQUEST`
/// (`delegation tokens not supported`). Nothing persisted. Controller
/// is not required. Official Kafka first flexible version is **2**;
/// Volant treats advertised v0 as flexible (same residual class as
/// quotas v0 / DescribeLogDirs v1). Owner/requester fields are v3+
/// and out of advertised range. ACL: Cluster **ALTER** (disabled ACLs
/// allow). Denied → **31**.
pub(crate) fn encode_create_delegation_token(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    parse_create_delegation_token_request(src);

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );
    let error = if denied {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::InvalidRequest
    };

    write_create_delegation_token(out, error);
}

fn parse_create_delegation_token_request(src: &mut impl Buf) {
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                if get_compact_string(src).is_err() {
                    break;
                }
                if get_compact_string(src).is_err() {
                    break;
                }
                let _ = skip_tag_buffer(src);
            }
        }
        Ok(None) | Err(_) => {}
    }
    if src.remaining() >= 8 {
        let _max_life_time_ms = src.get_i64();
    }
    let _ = skip_tag_buffer(src);
}

fn write_create_delegation_token(out: &mut BytesMut, error: KafkaErrorCode) {
    // Official CreateDelegationTokenResponse.json field order (v0–2):
    // error, owner principal, issue/expiry/max timestamps, tokenId, hmac,
    // throttle last. Flexible compact strings/bytes + tagged. Empty token.
    out.put_i16(error.as_i16());
    put_compact_string(out, "");
    put_compact_string(out, "");
    out.put_i64(-1); // issueTimestampMs
    out.put_i64(-1); // expiryTimestampMs
    out.put_i64(-1); // maxTimestampMs
    put_compact_string(out, ""); // tokenId
    put_compact_bytes(out, Some(&[])); // hmac
    out.put_i32(0); // throttleTimeMs
    put_empty_tag_buffer(out);
}

/// ExpireDelegationToken v0 (always flexible). Volant has no
/// delegation-token store.
///
/// Parses `hmac` compact bytes and `expiryTimePeriodMs`, then rejects
/// with **42** `INVALID_REQUEST` (`delegation tokens not supported`).
/// Official `ExpireDelegationTokenResponse.json` has no errorMessage.
/// Nothing persisted. Controller is not required. Official Kafka first
/// flexible version is **2**; Volant treats advertised v0 as flexible
/// (same residual as Create/Describe 38/41). ACL: Cluster **ALTER**
/// (disabled ACLs allow). Denied → **31**.
pub(crate) fn encode_expire_delegation_token(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    parse_expire_delegation_token_request(src);

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );
    let error = if denied {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::InvalidRequest
    };

    write_expire_delegation_token(out, error);
}

fn parse_expire_delegation_token_request(src: &mut impl Buf) {
    let _ = get_compact_bytes(src);
    if src.remaining() >= 8 {
        let _expiry_time_period_ms = src.get_i64();
    }
    let _ = skip_tag_buffer(src);
}

fn write_expire_delegation_token(out: &mut BytesMut, error: KafkaErrorCode) {
    // Official ExpireDelegationTokenResponse.json field order:
    // errorCode, expiryTimestampMs, throttleTimeMs. No errorMessage.
    // Flexible tagged trailer (residual v0).
    out.put_i16(error.as_i16());
    out.put_i64(-1); // expiryTimestampMs
    out.put_i32(0); // throttleTimeMs
    put_empty_tag_buffer(out);
}

/// RenewDelegationToken v0 (always flexible). Volant has no
/// delegation-token store.
///
/// Parses official `hmac` compact bytes + `renewPeriodMs` i64 and
/// discards them. Rejects with **42** `INVALID_REQUEST`
/// (`delegation tokens not supported`). Nothing persisted. Controller
/// is not required. Official Kafka first flexible version is **2**;
/// Volant treats advertised v0 as flexible. Official response
/// (`RenewDelegationTokenResponse.json`): error, expiryTimestampMs,
/// throttle last; no errorMessage. ACL: Cluster **ALTER** (disabled
/// ACLs allow). Denied → **31**.
pub(crate) fn encode_renew_delegation_token(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    parse_renew_delegation_token_request(src);

    let denied = broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        );
    let error = if denied {
        KafkaErrorCode::ClusterAuthorizationFailed
    } else {
        KafkaErrorCode::InvalidRequest
    };

    write_renew_delegation_token(out, error);
}

fn parse_renew_delegation_token_request(src: &mut impl Buf) {
    let _ = get_compact_bytes(src);
    if src.remaining() >= 8 {
        let _renew_period_ms = src.get_i64();
    }
    let _ = skip_tag_buffer(src);
}

fn write_renew_delegation_token(out: &mut BytesMut, error: KafkaErrorCode) {
    // Official RenewDelegationTokenResponse.json field order:
    // error, expiryTimestampMs, throttle last. Flex tagged trailer.
    out.put_i16(error.as_i16());
    out.put_i64(-1); // expiryTimestampMs
    out.put_i32(0); // throttleTimeMs
    put_empty_tag_buffer(out);
}

fn write_get_telemetry_subscriptions(
    out: &mut BytesMut,
    error: KafkaErrorCode,
    client_instance_id: &[u8; 16],
) {
    out.put_i32(0); // throttleTimeMs
    out.put_i16(error.as_i16());
    put_uuid(out, client_instance_id);
    out.put_i32(0); // subscriptionId — no subscription
    put_compact_array_len(out, 0); // acceptedCompressionTypes
    out.put_i32(-1); // pushIntervalMs — do not push
    out.put_i32(0); // telemetryMaxBytes
    out.put_u8(0); // deltaTemporality = false
    put_compact_array_len(out, 0); // requestedMetrics
    put_empty_tag_buffer(out);
}

fn parse_alter_client_quotas_request(src: &mut impl Buf) -> Vec<AlterQuotaEntry> {
    let mut entries = Vec::new();
    match get_compact_array_len(src) {
        Ok(Some(n)) => {
            for _ in 0..n {
                let mut entity = Vec::new();
                match get_compact_array_len(src) {
                    Ok(Some(en)) => {
                        for _ in 0..en {
                            let entity_type = match get_compact_string(src) {
                                Ok(s) => s,
                                Err(_) => break,
                            };
                            let entity_name = match get_compact_nullable_string(src) {
                                Ok(s) => s,
                                Err(_) => None,
                            };
                            let _ = skip_tag_buffer(src);
                            entity.push(QuotaEntity {
                                entity_type,
                                entity_name,
                            });
                        }
                    }
                    Ok(None) | Err(_) => {}
                }
                match get_compact_array_len(src) {
                    Ok(Some(on)) => {
                        for _ in 0..on {
                            if get_compact_string(src).is_err() {
                                break;
                            }
                            if src.remaining() < 8 + 1 {
                                break;
                            }
                            let _value = src.get_f64();
                            let _remove = src.get_u8();
                            let _ = skip_tag_buffer(src);
                        }
                    }
                    Ok(None) | Err(_) => {}
                }
                let _ = skip_tag_buffer(src);
                entries.push(AlterQuotaEntry { entity });
            }
        }
        Ok(None) | Err(_) => {}
    }
    if src.has_remaining() {
        let _validate_only = src.get_u8();
    }
    let _ = skip_tag_buffer(src);
    entries
}

fn write_describe_log_dirs(
    out: &mut BytesMut,
    flexible: bool,
    dir_error: i16,
    path: &str,
    topics: &[LocalLogDirTopic],
) {
    out.put_i32(0); // throttle
    if flexible {
        put_compact_array_len(out, 1);
        out.put_i16(dir_error);
        put_compact_string(out, path);
        put_compact_array_len(out, topics.len());
        for t in topics {
            put_compact_string(out, &t.name);
            put_compact_array_len(out, t.partitions.len());
            for p in &t.partitions {
                out.put_i32(p.partition);
                out.put_i64(p.size);
                out.put_i64(p.offset_lag);
                out.put_u8(u8::from(p.is_future));
                put_empty_tag_buffer(out);
            }
            put_empty_tag_buffer(out);
        }
        put_empty_tag_buffer(out); // dir tags
        put_empty_tag_buffer(out); // top tags
    } else {
        out.put_i32(1);
        out.put_i16(dir_error);
        put_string(out, path);
        out.put_i32(topics.len() as i32);
        for t in topics {
            put_string(out, &t.name);
            out.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                out.put_i32(p.partition);
                out.put_i64(p.size);
                out.put_i64(p.offset_lag);
                out.put_u8(u8::from(p.is_future));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 35: DeleteRecords + ACL admin (Describe/Create/DeleteAcls)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{BrokerEndpoint, ClusterConfig};
    use bytes::Buf;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use volant_storage::StorageConfig;

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "volant-v225-{label}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn pin_openraft_off() {
        if std::env::var("VOLANT_OPENRAFT_METADATA").is_err() {
            std::env::set_var("VOLANT_OPENRAFT_METADATA", "0");
        }
    }

    fn cluster_one(dir: PathBuf) -> Broker {
        pin_openraft_off();
        let cfg = ClusterConfig {
            default_replication_factor: 1,
            min_insync_replicas: 1,
            session_timeout_ms: 2000,
            replica_fetch_max_wait_ms: 50,
            replica_fetch_max_bytes: 1_048_576,
            replica_lag_max_messages: 10_000,
            replica_lag_max_ms: 30_000,
            brokers: vec![BrokerEndpoint {
                id: 1,
                host: "127.0.0.1".into(),
                port: 19001,
                rack: None,
            }],
        };
        Broker::with_cluster(
            StorageConfig {
                data_dir: dir,
                ..StorageConfig::default()
            },
            1,
            cfg,
        )
        .unwrap()
    }

    fn alter_body(topic: &str, partition: i32, replicas: Option<&[i32]>) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(5_000); // TimeoutMs ignored
        put_compact_array_len(&mut body, 1);
        put_compact_string(&mut body, topic);
        put_compact_array_len(&mut body, 1);
        body.put_i32(partition);
        match replicas {
            None => put_unsigned_varint(&mut body, 0),
            Some(ids) => {
                put_compact_array_len(&mut body, ids.len());
                for &id in ids {
                    body.put_i32(id);
                }
            }
        }
        put_empty_tag_buffer(&mut body);
        put_empty_tag_buffer(&mut body);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_part_result(src: &mut impl Buf) -> (i16, i16, Option<String>) {
        assert_eq!(src.get_i32(), 0); // throttle
        let top = src.get_i16();
        let _ = get_compact_nullable_string(src).unwrap();
        assert_eq!(get_compact_array_len(src).unwrap(), Some(1));
        let _name = get_compact_string(src).unwrap();
        assert_eq!(get_compact_array_len(src).unwrap(), Some(1));
        let _pid = src.get_i32();
        let code = src.get_i16();
        let msg = get_compact_nullable_string(src).unwrap();
        (top, code, msg)
    }

    #[tokio::test]
    async fn kafka_alter_reassign_hits_native_path() {
        let dir = temp_dir("native");
        let broker = cluster_one(dir.clone());
        broker.create_topic("events", 1).unwrap();
        let before = broker.clone_live_assignment().unwrap().generation;

        let mut src = alter_body("events", 0, Some(&[1]));
        let mut out = BytesMut::new();
        encode_alter_partition_reassignments(&broker, &mut src, &mut out, "kafka-anonymous").await;
        let mut resp = out.freeze();
        let (top, code, _) = read_part_result(&mut resp);
        assert_eq!(top, 0);
        assert_eq!(code, 0);

        let asg = broker.clone_live_assignment().unwrap();
        assert!(
            asg.generation > before,
            "native reassign must bump generation"
        );
        assert_eq!(asg.topics["events"].partitions[&0].replicas, vec![1]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn kafka_alter_reassign_cancel_is_83() {
        let dir = temp_dir("cancel");
        let broker = cluster_one(dir.clone());
        broker.create_topic("events", 1).unwrap();

        let mut src = alter_body("events", 0, None);
        let mut out = BytesMut::new();
        encode_alter_partition_reassignments(&broker, &mut src, &mut out, "kafka-anonymous").await;
        let mut resp = out.freeze();
        let (top, code, _) = read_part_result(&mut resp);
        assert_eq!(top, 0);
        assert_eq!(code, KafkaErrorCode::NoReassignmentInProgress.as_i16());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn kafka_alter_reassign_unknown_topic_and_bad_replica() {
        let dir = temp_dir("errs");
        let broker = cluster_one(dir.clone());
        broker.create_topic("events", 1).unwrap();

        let mut src = alter_body("missing", 0, Some(&[1]));
        let mut out = BytesMut::new();
        encode_alter_partition_reassignments(&broker, &mut src, &mut out, "kafka-anonymous").await;
        let mut resp = out.freeze();
        let (top, code, _) = read_part_result(&mut resp);
        assert_eq!(top, 0);
        assert_eq!(code, KafkaErrorCode::UnknownTopicOrPartition.as_i16());

        let mut src = alter_body("events", 0, Some(&[99]));
        let mut out = BytesMut::new();
        encode_alter_partition_reassignments(&broker, &mut src, &mut out, "kafka-anonymous").await;
        let mut resp = out.freeze();
        let (top, code, _) = read_part_result(&mut resp);
        assert_eq!(top, 0);
        assert_eq!(code, KafkaErrorCode::InvalidReplicaAssignment.as_i16());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn list_body_topic(topic: &str, partitions: &[i32]) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(5_000);
        put_compact_array_len(&mut body, 1);
        put_compact_string(&mut body, topic);
        put_compact_array_len(&mut body, partitions.len());
        for &p in partitions {
            body.put_i32(p);
        }
        put_empty_tag_buffer(&mut body);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn list_body_all() -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(5_000);
        put_unsigned_varint(&mut body, 0);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_list_part(src: &mut impl Buf) -> (i32, Vec<i32>, Vec<i32>, Vec<i32>, i16) {
        let pid = src.get_i32();
        let n_rep = get_compact_array_len(src).unwrap().unwrap_or(0);
        let mut replicas = Vec::with_capacity(n_rep);
        for _ in 0..n_rep {
            replicas.push(src.get_i32());
        }
        let n_add = get_compact_array_len(src).unwrap().unwrap_or(0);
        let mut adding = Vec::with_capacity(n_add);
        for _ in 0..n_add {
            adding.push(src.get_i32());
        }
        let n_rem = get_compact_array_len(src).unwrap().unwrap_or(0);
        let mut removing = Vec::with_capacity(n_rem);
        for _ in 0..n_rem {
            removing.push(src.get_i32());
        }
        let code = src.get_i16();
        let _ = get_compact_nullable_string(src).unwrap();
        let _ = skip_tag_buffer(src);
        (pid, replicas, adding, removing, code)
    }

    #[test]
    fn kafka_list_reassign_current_replicas_empty_adding_removing() {
        let dir = temp_dir("list-ok");
        let broker = cluster_one(dir.clone());
        broker.create_topic("events", 1).unwrap();

        let mut src = list_body_topic("events", &[0]);
        let mut out = BytesMut::new();
        encode_list_partition_reassignments(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0);
        assert_eq!(resp.get_i16(), 0);
        assert_eq!(get_compact_nullable_string(&mut resp).unwrap(), None);
        assert_eq!(get_compact_array_len(&mut resp).unwrap(), Some(1));
        assert_eq!(get_compact_string(&mut resp).unwrap(), "events");
        assert_eq!(get_compact_array_len(&mut resp).unwrap(), Some(1));
        let (pid, replicas, adding, removing, code) = read_list_part(&mut resp);
        assert_eq!(pid, 0);
        assert_eq!(replicas, vec![1]);
        assert!(adding.is_empty());
        assert!(removing.is_empty());
        assert_eq!(code, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_list_reassign_topics_null_lists_all() {
        let dir = temp_dir("list-all");
        let broker = cluster_one(dir.clone());
        broker.create_topic("events", 1).unwrap();
        broker.create_topic("logs", 2).unwrap();

        let mut src = list_body_all();
        let mut out = BytesMut::new();
        encode_list_partition_reassignments(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0);
        assert_eq!(resp.get_i16(), 0);
        let _ = get_compact_nullable_string(&mut resp).unwrap();
        let n = get_compact_array_len(&mut resp).unwrap().unwrap();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..n {
            let name = get_compact_string(&mut resp).unwrap();
            let pc = get_compact_array_len(&mut resp).unwrap().unwrap();
            for _ in 0..pc {
                let (pid, _r, adding, removing, code) = read_list_part(&mut resp);
                assert!(adding.is_empty());
                assert!(removing.is_empty());
                assert_eq!(code, 0);
                seen.insert((name.clone(), pid));
            }
            let _ = skip_tag_buffer(&mut resp);
        }
        assert!(seen.contains(&("events".into(), 0)));
        assert!(seen.contains(&("logs".into(), 0)));
        assert!(seen.contains(&("logs".into(), 1)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_list_reassign_unknown_topic_is_3() {
        let dir = temp_dir("list-unk");
        let broker = cluster_one(dir.clone());

        let mut src = list_body_topic("missing", &[0]);
        let mut out = BytesMut::new();
        encode_list_partition_reassignments(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0);
        assert_eq!(resp.get_i16(), 0);
        let _ = get_compact_nullable_string(&mut resp).unwrap();
        assert_eq!(get_compact_array_len(&mut resp).unwrap(), Some(1));
        assert_eq!(get_compact_string(&mut resp).unwrap(), "missing");
        assert_eq!(get_compact_array_len(&mut resp).unwrap(), Some(1));
        let (_pid, replicas, adding, removing, code) = read_list_part(&mut resp);
        assert!(replicas.is_empty());
        assert!(adding.is_empty());
        assert!(removing.is_empty());
        assert_eq!(code, KafkaErrorCode::UnknownTopicOrPartition.as_i16());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_list_reassign_not_controller_is_41() {
        let dir = temp_dir("list-nc");
        pin_openraft_off();
        let cfg = ClusterConfig {
            default_replication_factor: 2,
            min_insync_replicas: 1,
            session_timeout_ms: 2000,
            replica_fetch_max_wait_ms: 50,
            replica_fetch_max_bytes: 1_048_576,
            replica_lag_max_messages: 10_000,
            replica_lag_max_ms: 30_000,
            brokers: vec![
                BrokerEndpoint {
                    id: 1,
                    host: "127.0.0.1".into(),
                    port: 19201,
                    rack: None,
                },
                BrokerEndpoint {
                    id: 2,
                    host: "127.0.0.1".into(),
                    port: 19202,
                    rack: None,
                },
            ],
        };
        let broker = Broker::with_cluster(
            StorageConfig {
                data_dir: dir.clone(),
                ..StorageConfig::default()
            },
            2,
            cfg,
        )
        .unwrap();
        assert!(!broker.is_controller());

        let mut src = list_body_all();
        let mut out = BytesMut::new();
        encode_list_partition_reassignments(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0);
        assert_eq!(resp.get_i16(), KafkaErrorCode::NotController.as_i16());
        let _ = get_compact_nullable_string(&mut resp).unwrap();
        assert_eq!(get_compact_array_len(&mut resp).unwrap(), Some(0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn elect_body_v1(topic: &str, partition: i32, election_type: i8) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i8(election_type);
        body.put_i32(5_000);
        put_compact_array_len(&mut body, 1);
        put_compact_string(&mut body, topic);
        put_compact_array_len(&mut body, 1);
        body.put_i32(partition);
        put_empty_tag_buffer(&mut body);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_elect_part_v1(src: &mut impl Buf) -> (i16, i32, i16) {
        assert_eq!(src.get_i32(), 0); // throttle
        let top = src.get_i16();
        assert_eq!(get_compact_array_len(src).unwrap(), Some(1));
        let _name = get_compact_string(src).unwrap();
        assert_eq!(get_compact_array_len(src).unwrap(), Some(1));
        let pid = src.get_i32();
        let code = src.get_i16();
        let _ = get_compact_nullable_string(src).unwrap();
        (top, pid, code)
    }

    #[tokio::test]
    async fn kafka_elect_leaders_preferred_already_leader() {
        let dir = temp_dir("elect-ok");
        let broker = cluster_one(dir.clone());
        broker.create_topic("events", 1).unwrap();
        let before = broker.clone_live_assignment().unwrap();
        let leader_before = before.topics["events"].partitions[&0].leader;

        let mut src = elect_body_v1("events", 0, 0);
        let mut out = BytesMut::new();
        encode_elect_leaders(&broker, &mut src, &mut out, 1, "kafka-anonymous").await;
        let mut resp = out.freeze();
        let (top, pid, code) = read_elect_part_v1(&mut resp);
        assert_eq!(top, 0);
        assert_eq!(pid, 0);
        assert_eq!(code, 0);

        let asg = broker.clone_live_assignment().unwrap();
        assert_eq!(asg.generation, before.generation);
        assert_eq!(asg.topics["events"].partitions[&0].leader, leader_before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn kafka_elect_leaders_unclean_is_87() {
        let dir = temp_dir("elect-unclean");
        let broker = cluster_one(dir.clone());
        broker.create_topic("events", 1).unwrap();
        let before = broker.clone_live_assignment().unwrap();
        let leader_before = before.topics["events"].partitions[&0].leader;

        let mut src = elect_body_v1("events", 0, 1);
        let mut out = BytesMut::new();
        encode_elect_leaders(&broker, &mut src, &mut out, 1, "kafka-anonymous").await;
        let mut resp = out.freeze();
        let (top, pid, code) = read_elect_part_v1(&mut resp);
        assert_eq!(top, 0);
        assert_eq!(pid, 0);
        assert_eq!(code, KafkaErrorCode::EligibleLeadersNotAvailable.as_i16());

        let asg = broker.clone_live_assignment().unwrap();
        assert_eq!(asg.generation, before.generation);
        assert_eq!(asg.topics["events"].partitions[&0].leader, leader_before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn cluster_n2(dir: PathBuf, self_id: u32) -> Broker {
        pin_openraft_off();
        let cfg = ClusterConfig {
            default_replication_factor: 2,
            min_insync_replicas: 1,
            session_timeout_ms: 2000,
            replica_fetch_max_wait_ms: 50,
            replica_fetch_max_bytes: 1_048_576,
            replica_lag_max_messages: 10_000,
            replica_lag_max_ms: 30_000,
            brokers: vec![
                BrokerEndpoint {
                    id: 1,
                    host: "127.0.0.1".into(),
                    port: 19401,
                    rack: None,
                },
                BrokerEndpoint {
                    id: 2,
                    host: "127.0.0.1".into(),
                    port: 19402,
                    rack: None,
                },
            ],
        };
        Broker::with_cluster(
            StorageConfig {
                data_dir: dir,
                ..StorageConfig::default()
            },
            self_id,
            cfg,
        )
        .unwrap()
    }

    fn unregister_body(broker_id: i32) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(broker_id);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn overlay_ids(b: &Broker) -> Vec<u32> {
        b.list_membership().brokers.iter().map(|x| x.id).collect()
    }

    fn update_features_v0(feature: &str) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(5_000);
        put_compact_array_len(&mut body, 1);
        put_compact_string(&mut body, feature);
        body.put_i16(1);
        body.put_u8(0); // allowDowngrade
        put_empty_tag_buffer(&mut body);
        put_empty_tag_buffer(&mut body);
        body
    }

    #[tokio::test]
    async fn kafka_unregister_broker_extra_id() {
        let dir = temp_dir("unreg-ok");
        let broker = cluster_n2(dir.clone(), 1);
        broker
            .add_broker(3, "127.0.0.1".into(), 19403, None)
            .unwrap();
        assert!(overlay_ids(&broker).contains(&3));

        let mut src = unregister_body(3);
        let mut out = BytesMut::new();
        encode_unregister_broker(&broker, &mut src, &mut out, "kafka-anonymous").await;
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0);
        assert_eq!(resp.get_i16(), 0);
        assert_eq!(get_compact_nullable_string(&mut resp).unwrap(), None);

        assert!(!overlay_ids(&broker).contains(&3));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn kafka_unregister_broker_not_controller_is_41() {
        let dir = temp_dir("unreg-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());

        let mut src = unregister_body(1);
        let mut out = BytesMut::new();
        encode_unregister_broker(&broker, &mut src, &mut out, "kafka-anonymous").await;
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0);
        assert_eq!(resp.get_i16(), KafkaErrorCode::NotController.as_i16());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn kafka_unregister_broker_no_cluster_is_42() {
        let dir = temp_dir("unreg-single");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });

        let mut src = unregister_body(2);
        let mut out = BytesMut::new();
        encode_unregister_broker(&broker, &mut src, &mut out, "kafka-anonymous").await;
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0);
        assert_eq!(resp.get_i16(), KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(
            get_compact_nullable_string(&mut resp).unwrap().as_deref(),
            Some("unregister requires cluster")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_update_features_rejects_and_does_not_persist() {
        let dir = temp_dir("upd-feat");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let before: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name()))
            .collect();

        let mut src = update_features_v0("metadata.version");
        let mut out = BytesMut::new();
        encode_update_features(&broker, &mut src, &mut out, 0, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0); // throttle
        assert_eq!(resp.get_i16(), 0); // top-level
        let _ = get_compact_nullable_string(&mut resp).unwrap();
        assert_eq!(get_compact_array_len(&mut resp).unwrap(), Some(1));
        assert_eq!(get_compact_string(&mut resp).unwrap(), "metadata.version");
        assert_eq!(resp.get_i16(), KafkaErrorCode::FeatureUpdateFailed.as_i16());

        let after: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name()))
            .collect();
        assert_eq!(after, before, "UpdateFeatures must not persist features");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn describe_quorum_empty_v0() -> BytesMut {
        let mut body = BytesMut::new();
        put_compact_array_len(&mut body, 0);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn allocate_body(broker_id: i32, broker_epoch: i64) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(broker_id);
        body.put_i64(broker_epoch);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn alter_replica_log_dirs_v0(path: &str, topic: &str, partitions: &[i32]) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(1);
        put_string(&mut body, path);
        body.put_i32(1);
        put_string(&mut body, topic);
        body.put_i32(partitions.len() as i32);
        for &p in partitions {
            body.put_i32(p);
        }
        body
    }

    fn snapshot_dir(root: &std::path::Path) -> Vec<(std::path::PathBuf, u64)> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if let Ok(meta) = e.metadata() {
                    let rel = p.strip_prefix(root).unwrap_or(&p).to_path_buf();
                    out.push((rel, meta.len()));
                }
            }
        }
        out.sort();
        out
    }

    #[test]
    fn kafka_describe_quorum_single_node_raft_off_is_empty_0() {
        let dir = temp_dir("dq-single");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let mut src = describe_quorum_empty_v0();
        let mut out = BytesMut::new();
        encode_describe_quorum(&broker, &mut src, &mut out, 0, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i16(), 0);
        assert_eq!(get_compact_array_len(&mut resp).unwrap(), Some(0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_allocate_producer_ids_single_node_block() {
        let dir = temp_dir("alloc-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });

        let mut src = allocate_body(1, 0);
        let mut out = BytesMut::new();
        encode_allocate_producer_ids(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0);
        assert_eq!(resp.get_i16(), 0);
        let start1 = resp.get_i64();
        let len1 = resp.get_i32();
        assert!(start1 >= 0);
        assert_eq!(len1, 1000);

        let mut src = allocate_body(1, 99);
        let mut out = BytesMut::new();
        encode_allocate_producer_ids(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0);
        assert_eq!(resp.get_i16(), 0);
        let start2 = resp.get_i64();
        let len2 = resp.get_i32();
        assert_eq!(start2, start1 + 1000);
        assert_eq!(len2, 1000);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_describe_quorum_not_controller_is_41() {
        let dir = temp_dir("dq-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());
        let mut src = describe_quorum_empty_v0();
        let mut out = BytesMut::new();
        encode_describe_quorum(&broker, &mut src, &mut out, 0, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i16(), KafkaErrorCode::NotController.as_i16());
        assert_eq!(get_compact_array_len(&mut resp).unwrap(), Some(0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_allocate_producer_ids_not_controller_is_41() {
        let dir = temp_dir("alloc-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());

        let mut src = allocate_body(2, 0);
        let mut out = BytesMut::new();
        encode_allocate_producer_ids(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0);
        assert_eq!(resp.get_i16(), KafkaErrorCode::NotController.as_i16());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_alter_replica_log_dirs_rejects_and_does_not_move() {
        let dir = temp_dir("arld");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker.create_topic("events", 1).unwrap();
        let dest = dir.parent().unwrap().join("volant-arld-dest");
        let before = snapshot_dir(&dir);
        assert!(!dest.exists());

        let mut src = alter_replica_log_dirs_v0(dest.to_str().unwrap(), "events", &[0]);
        let mut out = BytesMut::new();
        encode_alter_replica_log_dirs(&broker, &mut src, &mut out, 0, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0); // throttle
        assert_eq!(resp.get_i32(), 1); // one topic
        assert_eq!(get_string(&mut resp).unwrap(), "events");
        assert_eq!(resp.get_i32(), 1);
        assert_eq!(resp.get_i32(), 0);
        let code = resp.get_i16();
        assert!(
            code == KafkaErrorCode::InvalidRequest.as_i16() || code == 57,
            "per-partition 42 INVALID_REQUEST or 57 LOG_DIR_NOT_FOUND, got {code}"
        );

        assert_eq!(snapshot_dir(&dir), before, "must not move replica files");
        assert!(!dest.exists(), "must not create a destination log dir");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn assign_replicas_to_dirs_v0(
        broker_id: i32,
        broker_epoch: i64,
        dir_id: &[u8; 16],
        topic_id: &[u8; 16],
        partitions: &[i32],
    ) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(broker_id);
        body.put_i64(broker_epoch);
        put_compact_array_len(&mut body, 1);
        put_uuid(&mut body, dir_id);
        put_compact_array_len(&mut body, 1);
        put_uuid(&mut body, topic_id);
        put_compact_array_len(&mut body, partitions.len());
        for &p in partitions {
            body.put_i32(p);
        }
        put_empty_tag_buffer(&mut body);
        put_empty_tag_buffer(&mut body);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn get_telemetry_body(client_instance_id: &[u8; 16], subscription_id: i32) -> BytesMut {
        let mut body = BytesMut::new();
        put_uuid(&mut body, client_instance_id);
        body.put_i32(subscription_id);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_get_telemetry(src: &mut impl Buf) -> (i16, [u8; 16], i32, i32, usize) {
        assert_eq!(src.get_i32(), 0); // throttle
        let error = src.get_i16();
        let id = get_uuid(src).unwrap();
        let subscription_id = src.get_i32();
        let compression_n = get_compact_array_len(src).unwrap().unwrap_or(0);
        let push_interval = src.get_i32();
        assert_eq!(src.get_i32(), 0); // telemetryMaxBytes
        assert_eq!(src.get_u8(), 0); // deltaTemporality
        let metrics_n = get_compact_array_len(src).unwrap().unwrap_or(0);
        skip_tag_buffer(src).unwrap();
        assert_eq!(compression_n, 0);
        (error, id, subscription_id, push_interval, metrics_n)
    }

    fn dir_has_telemetry(root: &std::path::Path) -> bool {
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let name = e.file_name();
                let lower = name.to_string_lossy().to_ascii_lowercase();
                if lower.contains("telemetry") {
                    return true;
                }
                if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    stack.push(e.path());
                }
            }
        }
        false
    }

    #[test]
    fn kafka_assign_replicas_to_dirs_rejects_and_does_not_move() {
        let dir = temp_dir("artd");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker.create_topic("events", 1).unwrap();
        let topic_uuid = topic_id::uuid_for_name(&broker, "events");
        let dir_id = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
            0xff, 0x01,
        ];
        let before = snapshot_dir(&dir);

        let mut src = assign_replicas_to_dirs_v0(0, 0, &dir_id, &topic_uuid, &[0]);
        let mut out = BytesMut::new();
        encode_assign_replicas_to_dirs(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(resp.get_i32(), 0); // throttle
        assert_eq!(resp.get_i16(), 0); // top-level
        assert_eq!(get_compact_array_len(&mut resp).unwrap(), Some(1));
        assert_eq!(get_uuid(&mut resp).unwrap(), dir_id);
        assert_eq!(get_compact_array_len(&mut resp).unwrap(), Some(1));
        assert_eq!(get_uuid(&mut resp).unwrap(), topic_uuid);
        assert_eq!(get_compact_array_len(&mut resp).unwrap(), Some(1));
        assert_eq!(resp.get_i32(), 0);
        assert_eq!(resp.get_i16(), KafkaErrorCode::InvalidRequest.as_i16());

        assert_eq!(snapshot_dir(&dir), before, "must not move replica files");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_get_telemetry_subscriptions_empty_and_does_not_persist() {
        let dir = temp_dir("gts-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let before = snapshot_dir(&dir);
        let mut id = [0u8; 16];
        id[0] = 0x11;
        id[15] = 0x22;

        let mut src = get_telemetry_body(&id, 7);
        let mut out = BytesMut::new();
        encode_get_telemetry_subscriptions(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, echoed, subscription_id, push_interval, metrics_n) =
            read_get_telemetry(&mut resp);
        assert_eq!(error, 0);
        assert_eq!(echoed, id);
        assert_eq!(subscription_id, 0);
        assert_eq!(push_interval, -1);
        assert_eq!(metrics_n, 0);

        assert_eq!(snapshot_dir(&dir), before, "must not persist telemetry");
        assert!(!dir_has_telemetry(&dir), "must not create telemetry files");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_get_telemetry_subscriptions_acl_deny_is_31() {
        let dir = temp_dir("gts-acl");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker
            .configure_acls(true, None, vec![], "token".into())
            .unwrap();
        let mut id = [0u8; 16];
        id[3] = 0xab;

        let mut src = get_telemetry_body(&id, 0);
        let mut out = BytesMut::new();
        encode_get_telemetry_subscriptions(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, echoed, subscription_id, push_interval, metrics_n) =
            read_get_telemetry(&mut resp);
        assert_eq!(error, KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        assert_eq!(echoed, id);
        assert_eq!(subscription_id, 0);
        assert_eq!(push_interval, -1);
        assert_eq!(metrics_n, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn create_delegation_token_body(renewers: &[(&str, &str)], max_life_ms: i64) -> BytesMut {
        let mut body = BytesMut::new();
        put_compact_array_len(&mut body, renewers.len());
        for (ty, name) in renewers {
            put_compact_string(&mut body, ty);
            put_compact_string(&mut body, name);
            put_empty_tag_buffer(&mut body);
        }
        body.put_i64(max_life_ms);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_create_delegation_token(
        src: &mut impl Buf,
    ) -> (i16, String, String, i64, i64, i64, String, usize, i32) {
        let error = src.get_i16();
        let principal_type = get_compact_string(src).unwrap();
        let principal_name = get_compact_string(src).unwrap();
        let issue = src.get_i64();
        let expiry = src.get_i64();
        let max_ts = src.get_i64();
        let token_id = get_compact_string(src).unwrap();
        let hmac_len = get_compact_bytes(src).unwrap().unwrap_or_default().len();
        let throttle = src.get_i32();
        skip_tag_buffer(src).unwrap();
        (
            error,
            principal_type,
            principal_name,
            issue,
            expiry,
            max_ts,
            token_id,
            hmac_len,
            throttle,
        )
    }

    fn dir_has_delegation_token(root: &std::path::Path) -> bool {
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let name = e.file_name();
                let lower = name.to_string_lossy().to_ascii_lowercase();
                if lower.contains("delegation") {
                    return true;
                }
                if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    stack.push(e.path());
                }
            }
        }
        false
    }

    #[test]
    fn kafka_create_delegation_token_rejects_and_does_not_persist() {
        let dir = temp_dir("cdt-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let before = snapshot_dir(&dir);

        let mut src = create_delegation_token_body(&[("User", "alice")], -1);
        let mut out = BytesMut::new();
        encode_create_delegation_token(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, ptype, pname, issue, expiry, max_ts, token_id, hmac_len, throttle) =
            read_create_delegation_token(&mut resp);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert!(ptype.is_empty());
        assert!(pname.is_empty());
        assert_eq!(issue, -1);
        assert_eq!(expiry, -1);
        assert_eq!(max_ts, -1);
        assert!(token_id.is_empty());
        assert_eq!(hmac_len, 0);
        assert_eq!(throttle, 0);

        assert_eq!(snapshot_dir(&dir), before, "must not persist tokens");
        assert!(
            !dir_has_delegation_token(&dir),
            "must not create delegation-token files"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_create_delegation_token_acl_deny_is_31() {
        let dir = temp_dir("cdt-acl");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker
            .configure_acls(true, None, vec![], "token".into())
            .unwrap();

        let mut src = create_delegation_token_body(&[("User", "bob")], 3_600_000);
        let mut out = BytesMut::new();
        encode_create_delegation_token(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, _, _, issue, expiry, max_ts, token_id, hmac_len, throttle) =
            read_create_delegation_token(&mut resp);
        assert_eq!(error, KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        assert_eq!(issue, -1);
        assert_eq!(expiry, -1);
        assert_eq!(max_ts, -1);
        assert!(token_id.is_empty());
        assert_eq!(hmac_len, 0);
        assert_eq!(throttle, 0);
        assert!(!dir_has_delegation_token(&dir));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_create_delegation_token_not_controller_still_42() {
        let dir = temp_dir("cdt-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());

        let mut src = create_delegation_token_body(&[], -1);
        let mut out = BytesMut::new();
        encode_create_delegation_token(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, _, _, issue, expiry, max_ts, token_id, hmac_len, throttle) =
            read_create_delegation_token(&mut resp);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(issue, -1);
        assert_eq!(expiry, -1);
        assert_eq!(max_ts, -1);
        assert!(token_id.is_empty());
        assert_eq!(hmac_len, 0);
        assert_eq!(throttle, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn expire_delegation_token_body(hmac: &[u8], expiry_time_period_ms: i64) -> BytesMut {
        let mut body = BytesMut::new();
        put_compact_bytes(&mut body, Some(hmac));
        body.put_i64(expiry_time_period_ms);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn renew_delegation_token_body(hmac: &[u8], renew_period_ms: i64) -> BytesMut {
        let mut body = BytesMut::new();
        put_compact_bytes(&mut body, Some(hmac));
        body.put_i64(renew_period_ms);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_expire_delegation_token(src: &mut impl Buf) -> (i16, i64, i32) {
        let error = src.get_i16();
        let expiry = src.get_i64();
        let throttle = src.get_i32();
        skip_tag_buffer(src).unwrap();
        (error, expiry, throttle)
    }

    fn read_renew_delegation_token(src: &mut impl Buf) -> (i16, i64, i32) {
        let error = src.get_i16();
        let expiry = src.get_i64();
        let throttle = src.get_i32();
        skip_tag_buffer(src).unwrap();
        (error, expiry, throttle)
    }

    #[test]
    fn kafka_expire_delegation_token_rejects_and_does_not_persist() {
        let dir = temp_dir("edt-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let before = snapshot_dir(&dir);

        let mut src = expire_delegation_token_body(b"hmac", -1);
        let mut out = BytesMut::new();
        encode_expire_delegation_token(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, expiry, throttle) = read_expire_delegation_token(&mut resp);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(expiry, -1);
        assert_eq!(throttle, 0);
        assert_eq!(resp.remaining(), 0);

        assert_eq!(snapshot_dir(&dir), before, "must not persist tokens");
        assert!(
            !dir_has_delegation_token(&dir),
            "must not create delegation-token files"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_renew_delegation_token_rejects_and_does_not_persist() {
        let dir = temp_dir("rdt-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let before = snapshot_dir(&dir);

        let mut src = renew_delegation_token_body(b"hmac-bytes", -1);
        let mut out = BytesMut::new();
        encode_renew_delegation_token(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, expiry, throttle) = read_renew_delegation_token(&mut resp);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(expiry, -1);
        assert_eq!(throttle, 0);

        assert_eq!(snapshot_dir(&dir), before, "must not persist tokens");
        assert!(
            !dir_has_delegation_token(&dir),
            "must not create delegation-token files"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_expire_delegation_token_acl_deny_is_31() {
        let dir = temp_dir("edt-acl");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker
            .configure_acls(true, None, vec![], "token".into())
            .unwrap();

        let mut src = expire_delegation_token_body(b"hmac", 3_600_000);
        let mut out = BytesMut::new();
        encode_expire_delegation_token(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, expiry, throttle) = read_expire_delegation_token(&mut resp);
        assert_eq!(error, KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        assert_eq!(expiry, -1);
        assert_eq!(throttle, 0);
        assert!(!dir_has_delegation_token(&dir));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_renew_delegation_token_acl_deny_is_31() {
        let dir = temp_dir("rdt-acl");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker
            .configure_acls(true, None, vec![], "token".into())
            .unwrap();

        let mut src = renew_delegation_token_body(b"hmac", 3_600_000);
        let mut out = BytesMut::new();
        encode_renew_delegation_token(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, expiry, throttle) = read_renew_delegation_token(&mut resp);
        assert_eq!(error, KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        assert_eq!(expiry, -1);
        assert_eq!(throttle, 0);
        assert!(!dir_has_delegation_token(&dir));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_expire_delegation_token_not_controller_still_42() {
        let dir = temp_dir("edt-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());

        let mut src = expire_delegation_token_body(&[], -1);
        let mut out = BytesMut::new();
        encode_expire_delegation_token(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, expiry, throttle) = read_expire_delegation_token(&mut resp);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(expiry, -1);
        assert_eq!(throttle, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_renew_delegation_token_not_controller_still_42() {
        let dir = temp_dir("rdt-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());

        let mut src = renew_delegation_token_body(&[], -1);
        let mut out = BytesMut::new();
        encode_renew_delegation_token(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, expiry, throttle) = read_renew_delegation_token(&mut resp);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(expiry, -1);
        assert_eq!(throttle, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_get_telemetry_subscriptions_not_controller_still_ok() {
        let dir = temp_dir("gts-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());
        let mut id = [0u8; 16];
        id[7] = 0x42;

        let mut src = get_telemetry_body(&id, 1);
        let mut out = BytesMut::new();
        encode_get_telemetry_subscriptions(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (error, echoed, subscription_id, push_interval, metrics_n) =
            read_get_telemetry(&mut resp);
        assert_eq!(error, 0);
        assert_eq!(echoed, id);
        assert_eq!(subscription_id, 0);
        assert_eq!(push_interval, -1);
        assert_eq!(metrics_n, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn push_telemetry_body(client_instance_id: &[u8; 16], metrics: &[u8]) -> BytesMut {
        let mut body = BytesMut::new();
        put_uuid(&mut body, client_instance_id);
        body.put_i32(1); // subscriptionId
        body.put_u8(0); // terminating
        body.put_i8(0); // compressionType
        crate::kafka::codec::put_compact_bytes(&mut body, Some(metrics));
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_push_telemetry(src: &mut impl Buf) -> i16 {
        assert_eq!(src.get_i32(), 0); // throttle
        let error = src.get_i16();
        skip_tag_buffer(src).unwrap();
        assert_eq!(src.remaining(), 0);
        error
    }

    #[test]
    fn kafka_push_telemetry_rejects_and_does_not_persist() {
        let dir = temp_dir("pt-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let before = snapshot_dir(&dir);
        let mut id = [0u8; 16];
        id[0] = 0x72;
        id[15] = 0x42;

        let mut src = push_telemetry_body(&id, b"otlp-metrics");
        let mut out = BytesMut::new();
        encode_push_telemetry(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(
            read_push_telemetry(&mut resp),
            KafkaErrorCode::InvalidRequest.as_i16()
        );

        assert_eq!(snapshot_dir(&dir), before, "must not persist telemetry");
        assert!(!dir_has_telemetry(&dir), "must not create telemetry files");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_push_telemetry_acl_deny_is_31() {
        let dir = temp_dir("pt-acl");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker
            .configure_acls(true, None, vec![], "token".into())
            .unwrap();
        let mut id = [0u8; 16];
        id[3] = 0xab;
        let before = snapshot_dir(&dir);

        let mut src = push_telemetry_body(&id, b"metrics");
        let mut out = BytesMut::new();
        encode_push_telemetry(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(
            read_push_telemetry(&mut resp),
            KafkaErrorCode::ClusterAuthorizationFailed.as_i16()
        );
        assert_eq!(
            snapshot_dir(&dir),
            before,
            "deny must not persist telemetry"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_push_telemetry_not_controller_still_rejects() {
        let dir = temp_dir("pt-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());
        let mut id = [0u8; 16];
        id[7] = 0x42;

        let mut src = push_telemetry_body(&id, b"metrics");
        let mut out = BytesMut::new();
        encode_push_telemetry(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(
            read_push_telemetry(&mut resp),
            KafkaErrorCode::InvalidRequest.as_i16()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn broker_registration_body(
        broker_id: i32,
        cluster_id: &str,
        incarnation: &[u8; 16],
        listeners: &[(&str, &str, u16, i16)],
        features: &[(&str, i16, i16)],
        rack: Option<&str>,
    ) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(broker_id);
        put_compact_string(&mut body, cluster_id);
        put_uuid(&mut body, incarnation);
        put_compact_array_len(&mut body, listeners.len());
        for (name, host, port, security) in listeners {
            put_compact_string(&mut body, name);
            put_compact_string(&mut body, host);
            body.put_u16(*port);
            body.put_i16(*security);
            put_empty_tag_buffer(&mut body);
        }
        put_compact_array_len(&mut body, features.len());
        for (name, min_v, max_v) in features {
            put_compact_string(&mut body, name);
            body.put_i16(*min_v);
            body.put_i16(*max_v);
            put_empty_tag_buffer(&mut body);
        }
        put_compact_nullable_string(&mut body, rack);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_broker_registration(src: &mut impl Buf) -> (i32, i16, i64) {
        let throttle = src.get_i32();
        let error = src.get_i16();
        let epoch = src.get_i64();
        skip_tag_buffer(src).unwrap();
        assert_eq!(src.remaining(), 0);
        (throttle, error, epoch)
    }

    fn membership_file(dir: &std::path::Path) -> std::path::PathBuf {
        crate::cluster::membership_overlay_path(dir)
    }

    #[test]
    fn kafka_broker_registration_rejects_and_does_not_persist() {
        let dir = temp_dir("breg-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let before_ids = overlay_ids(&broker);
        assert!(!membership_file(&dir).exists());

        let mut incarnation = [0u8; 16];
        incarnation[15] = 0x01;
        let mut src = broker_registration_body(
            4,
            "volant-cluster",
            &incarnation,
            &[("PLAINTEXT", "127.0.0.1", 19094, 0)],
            &[("metadata.version", 1, 20)],
            Some("rack-a"),
        );
        let mut out = BytesMut::new();
        encode_broker_registration(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, epoch) = read_broker_registration(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(epoch, -1);

        assert_eq!(overlay_ids(&broker), before_ids);
        assert!(
            !membership_file(&dir).exists(),
            "BrokerRegistration must not create membership.json"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_broker_registration_existing_brokers_unchanged() {
        let dir = temp_dir("breg-exist");
        let broker = cluster_n2(dir.clone(), 1);
        broker
            .add_broker(3, "127.0.0.1".into(), 19403, None)
            .unwrap();
        let before = overlay_ids(&broker);
        assert!(before.contains(&3));

        let mut incarnation = [0u8; 16];
        incarnation[0] = 0xaa;
        let mut src = broker_registration_body(
            4,
            "volant-cluster",
            &incarnation,
            &[("PLAINTEXT", "127.0.0.1", 19404, 0)],
            &[],
            None,
        );
        let mut out = BytesMut::new();
        encode_broker_registration(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, epoch) = read_broker_registration(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(epoch, -1);

        let after = overlay_ids(&broker);
        assert_eq!(after, before);
        assert!(!after.contains(&4));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_broker_registration_acl_deny_is_31() {
        let dir = temp_dir("breg-acl");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker
            .configure_acls(true, None, vec![], "token".into())
            .unwrap();

        let mut incarnation = [0u8; 16];
        incarnation[1] = 0xbb;
        let mut src = broker_registration_body(2, "c", &incarnation, &[], &[], None);
        let mut out = BytesMut::new();
        encode_broker_registration(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, epoch) = read_broker_registration(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        assert_eq!(epoch, -1);
        assert!(!membership_file(&dir).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_broker_registration_not_controller_still_42() {
        let dir = temp_dir("breg-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());
        let before = overlay_ids(&broker);

        let mut incarnation = [0u8; 16];
        incarnation[2] = 0xcc;
        let mut src = broker_registration_body(
            9,
            "volant-cluster",
            &incarnation,
            &[("PLAINTEXT", "127.0.0.1", 19099, 0)],
            &[],
            None,
        );
        let mut out = BytesMut::new();
        encode_broker_registration(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, epoch) = read_broker_registration(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(epoch, -1);
        assert_eq!(overlay_ids(&broker), before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn broker_heartbeat_body(
        broker_id: i32,
        broker_epoch: i64,
        current_metadata_offset: i64,
        want_fence: bool,
        want_shut_down: bool,
    ) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(broker_id);
        body.put_i64(broker_epoch);
        body.put_i64(current_metadata_offset);
        body.put_i8(i8::from(want_fence));
        body.put_i8(i8::from(want_shut_down));
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_broker_heartbeat(src: &mut impl Buf) -> (i32, i16, bool, bool, bool) {
        let throttle = src.get_i32();
        let error = src.get_i16();
        let is_caught_up = src.get_i8() != 0;
        let is_fenced = src.get_i8() != 0;
        let should_shut_down = src.get_i8() != 0;
        skip_tag_buffer(src).unwrap();
        assert_eq!(src.remaining(), 0);
        (throttle, error, is_caught_up, is_fenced, should_shut_down)
    }

    #[test]
    fn kafka_broker_heartbeat_rejects_and_does_not_persist() {
        let dir = temp_dir("bhb-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let before_ids = overlay_ids(&broker);
        assert!(!membership_file(&dir).exists());

        let mut src = broker_heartbeat_body(4, 7, 99, true, false);
        let mut out = BytesMut::new();
        encode_broker_heartbeat(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, caught_up, fenced, shut_down) = read_broker_heartbeat(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert!(!caught_up);
        assert!(fenced);
        assert!(!shut_down);

        assert_eq!(overlay_ids(&broker), before_ids);
        assert!(
            !membership_file(&dir).exists(),
            "BrokerHeartbeat must not create membership.json"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_broker_heartbeat_existing_brokers_unchanged() {
        let dir = temp_dir("bhb-exist");
        let broker = cluster_n2(dir.clone(), 1);
        broker
            .add_broker(3, "127.0.0.1".into(), 19403, None)
            .unwrap();
        let before = overlay_ids(&broker);
        assert!(before.contains(&3));

        let mut src = broker_heartbeat_body(4, 1, 0, false, true);
        let mut out = BytesMut::new();
        encode_broker_heartbeat(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, caught_up, fenced, shut_down) = read_broker_heartbeat(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert!(!caught_up);
        assert!(fenced);
        assert!(!shut_down);

        let after = overlay_ids(&broker);
        assert_eq!(after, before);
        assert!(!after.contains(&4));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_broker_heartbeat_acl_deny_is_31() {
        let dir = temp_dir("bhb-acl");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker
            .configure_acls(true, None, vec![], "token".into())
            .unwrap();

        let mut src = broker_heartbeat_body(2, -1, 0, false, false);
        let mut out = BytesMut::new();
        encode_broker_heartbeat(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, caught_up, fenced, shut_down) = read_broker_heartbeat(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        assert!(!caught_up);
        assert!(fenced);
        assert!(!shut_down);
        assert!(!membership_file(&dir).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_broker_heartbeat_not_controller_still_42() {
        let dir = temp_dir("bhb-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());
        let before = overlay_ids(&broker);

        let mut src = broker_heartbeat_body(9, 3, 12, true, true);
        let mut out = BytesMut::new();
        encode_broker_heartbeat(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, caught_up, fenced, shut_down) = read_broker_heartbeat(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert!(!caught_up);
        assert!(fenced);
        assert!(!shut_down);
        assert_eq!(overlay_ids(&broker), before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn envelope_body(
        request_data: Option<&[u8]>,
        principal: Option<&[u8]>,
        client_host: Option<&[u8]>,
    ) -> BytesMut {
        let mut body = BytesMut::new();
        put_compact_bytes(&mut body, request_data);
        put_compact_bytes(&mut body, principal);
        put_compact_bytes(&mut body, client_host);
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_envelope(src: &mut impl Buf) -> i16 {
        let data = get_compact_bytes(src).unwrap();
        assert!(data.is_none(), "ResponseData must be null");
        let error = src.get_i16();
        skip_tag_buffer(src).unwrap();
        assert_eq!(src.remaining(), 0);
        error
    }

    fn topic_names(b: &Broker) -> Vec<String> {
        let mut names: Vec<String> = b
            .metadata(None)
            .topics
            .into_iter()
            .map(|t| t.name.as_str().to_owned())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn kafka_envelope_rejects_and_does_not_unwrap() {
        let dir = temp_dir("env-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker.create_topic("events", 1).unwrap();
        let before_topics = topic_names(&broker);
        let before_ids = overlay_ids(&broker);

        // Dummy compact RequestData that looks like an inner request.
        let mut src = envelope_body(Some(b"create-topics-dummy"), None, Some(b"127.0.0.1"));
        let mut out = BytesMut::new();
        encode_envelope(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(
            read_envelope(&mut resp),
            KafkaErrorCode::InvalidRequest.as_i16()
        );

        assert_eq!(topic_names(&broker), before_topics);
        assert_eq!(overlay_ids(&broker), before_ids);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_envelope_acl_deny_is_31() {
        let dir = temp_dir("env-acl");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker
            .configure_acls(true, None, vec![], "token".into())
            .unwrap();
        let before_topics = topic_names(&broker);

        let mut src = envelope_body(Some(b"inner"), None, Some(b"host"));
        let mut out = BytesMut::new();
        encode_envelope(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(
            read_envelope(&mut resp),
            KafkaErrorCode::ClusterAuthorizationFailed.as_i16()
        );
        assert_eq!(topic_names(&broker), before_topics);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_envelope_not_controller_still_42() {
        let dir = temp_dir("env-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());
        let before_topics = topic_names(&broker);
        let before_ids = overlay_ids(&broker);

        let mut src = envelope_body(Some(b"inner"), Some(b"alice"), Some(b"10.0.0.1"));
        let mut out = BytesMut::new();
        encode_envelope(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        assert_eq!(
            read_envelope(&mut resp),
            KafkaErrorCode::InvalidRequest.as_i16()
        );
        assert_eq!(topic_names(&broker), before_topics);
        assert_eq!(overlay_ids(&broker), before_ids);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn fetch_snapshot_body(
        replica_id: i32,
        max_bytes: i32,
        topics: &[(&str, &[(i32, i32, i64, i32, i64)])],
    ) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(replica_id);
        body.put_i32(max_bytes);
        put_compact_array_len(&mut body, topics.len());
        for (name, partitions) in topics {
            put_compact_string(&mut body, name);
            put_compact_array_len(&mut body, partitions.len());
            for (partition, leader_epoch, end_offset, epoch, position) in *partitions {
                body.put_i32(*partition);
                body.put_i32(*leader_epoch);
                body.put_i64(*end_offset);
                body.put_i32(*epoch);
                body.put_i64(*position);
                put_empty_tag_buffer(&mut body);
            }
            put_empty_tag_buffer(&mut body);
        }
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_fetch_snapshot(src: &mut impl Buf) -> (i32, i16) {
        let throttle = src.get_i32();
        let error = src.get_i16();
        assert_eq!(get_compact_array_len(src).unwrap(), Some(0));
        skip_tag_buffer(src).unwrap();
        assert_eq!(src.remaining(), 0);
        (throttle, error)
    }

    fn raft_state(b: &Broker) -> (bool, Option<u32>, u64) {
        (
            b.openraft_started(),
            b.openraft_leader_id(),
            b.openraft_term(),
        )
    }

    #[test]
    fn kafka_fetch_snapshot_rejects_and_does_not_persist() {
        let dir = temp_dir("fsnap-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let before_files = snapshot_dir(&dir);
        let before_raft = raft_state(&broker);

        let mut src = fetch_snapshot_body(-1, 1024, &[("__cluster_metadata", &[(0, 0, 0, 0, 0)])]);
        let mut out = BytesMut::new();
        encode_fetch_snapshot(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error) = read_fetch_snapshot(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());

        assert_eq!(
            snapshot_dir(&dir),
            before_files,
            "must not write snapshot files"
        );
        assert_eq!(raft_state(&broker), before_raft, "openraft state unchanged");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_fetch_snapshot_truncated_still_42() {
        let dir = temp_dir("fsnap-trunc");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let mut src = BytesMut::new();
        src.put_i32(-1);
        let mut out = BytesMut::new();
        encode_fetch_snapshot(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error) = read_fetch_snapshot(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_fetch_snapshot_acl_deny_is_31() {
        let dir = temp_dir("fsnap-acl");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker
            .configure_acls(true, None, vec![], "token".into())
            .unwrap();

        let mut src = fetch_snapshot_body(-1, 1024, &[("events", &[(0, 0, 0, 0, 0)])]);
        let mut out = BytesMut::new();
        encode_fetch_snapshot(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error) = read_fetch_snapshot(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_fetch_snapshot_not_controller_still_42() {
        let dir = temp_dir("fsnap-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());
        let before_raft = raft_state(&broker);

        let mut src = fetch_snapshot_body(-1, 1024, &[("events", &[(0, -1, 0, 0, 0)])]);
        let mut out = BytesMut::new();
        encode_fetch_snapshot(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error) = read_fetch_snapshot(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(raft_state(&broker), before_raft);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn controller_registration_body(
        controller_id: i32,
        incarnation: &[u8; 16],
        zk_migration_ready: bool,
        listeners: &[(&str, &str, u16, i16)],
        features: &[(&str, i16, i16)],
    ) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(controller_id);
        put_uuid(&mut body, incarnation);
        body.put_u8(u8::from(zk_migration_ready));
        put_compact_array_len(&mut body, listeners.len());
        for (name, host, port, security) in listeners {
            put_compact_string(&mut body, name);
            put_compact_string(&mut body, host);
            body.put_u16(*port);
            body.put_i16(*security);
            put_empty_tag_buffer(&mut body);
        }
        put_compact_array_len(&mut body, features.len());
        for (name, min_v, max_v) in features {
            put_compact_string(&mut body, name);
            body.put_i16(*min_v);
            body.put_i16(*max_v);
            put_empty_tag_buffer(&mut body);
        }
        put_empty_tag_buffer(&mut body);
        body
    }

    fn read_controller_registration(src: &mut impl Buf) -> (i32, i16, Option<String>) {
        let throttle = src.get_i32();
        let error = src.get_i16();
        let msg = get_compact_nullable_string(src).unwrap();
        skip_tag_buffer(src).unwrap();
        assert_eq!(src.remaining(), 0);
        (throttle, error, msg)
    }

    #[test]
    fn kafka_controller_registration_rejects_and_does_not_persist() {
        let dir = temp_dir("creg-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let before_ids = overlay_ids(&broker);
        assert!(!membership_file(&dir).exists());

        let mut incarnation = [0u8; 16];
        incarnation[15] = 0x70;
        let mut src = controller_registration_body(
            1,
            &incarnation,
            false,
            &[("CONTROLLER", "127.0.0.1", 19094, 0)],
            &[("metadata.version", 1, 20)],
        );
        let mut out = BytesMut::new();
        encode_controller_registration(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, msg) = read_controller_registration(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(msg.as_deref(), Some("not KRaft controller registration"));

        assert_eq!(overlay_ids(&broker), before_ids);
        assert!(
            !membership_file(&dir).exists(),
            "ControllerRegistration must not create membership.json"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_controller_registration_existing_brokers_unchanged() {
        let dir = temp_dir("creg-exist");
        let broker = cluster_n2(dir.clone(), 1);
        broker
            .add_broker(3, "127.0.0.1".into(), 19403, None)
            .unwrap();
        let before = overlay_ids(&broker);
        assert!(before.contains(&3));

        let mut incarnation = [0u8; 16];
        incarnation[0] = 0xaa;
        let mut src = controller_registration_body(
            4,
            &incarnation,
            true,
            &[("CONTROLLER", "127.0.0.1", 19404, 0)],
            &[],
        );
        let mut out = BytesMut::new();
        encode_controller_registration(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, msg) = read_controller_registration(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(msg.as_deref(), Some("not KRaft controller registration"));

        let after = overlay_ids(&broker);
        assert_eq!(after, before);
        assert!(!after.contains(&4));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_controller_registration_acl_deny_is_31() {
        let dir = temp_dir("creg-acl");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker
            .configure_acls(true, None, vec![], "token".into())
            .unwrap();

        let mut incarnation = [0u8; 16];
        incarnation[1] = 0xbb;
        let mut src = controller_registration_body(2, &incarnation, false, &[], &[]);
        let mut out = BytesMut::new();
        encode_controller_registration(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, msg) = read_controller_registration(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        assert_eq!(msg, None);
        assert!(!membership_file(&dir).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_controller_registration_not_controller_still_42() {
        let dir = temp_dir("creg-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());
        let before = overlay_ids(&broker);

        let mut incarnation = [0u8; 16];
        incarnation[2] = 0xcc;
        let mut src = controller_registration_body(
            9,
            &incarnation,
            false,
            &[("CONTROLLER", "127.0.0.1", 19099, 0)],
            &[],
        );
        let mut out = BytesMut::new();
        encode_controller_registration(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error, msg) = read_controller_registration(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(msg.as_deref(), Some("not KRaft controller registration"));
        assert_eq!(overlay_ids(&broker), before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn update_raft_voter_body(
        cluster_id: Option<&str>,
        current_leader_epoch: i32,
        voter_id: i32,
        voter_directory_id: &[u8; 16],
        listeners: &[(&str, &str, u16)],
        min_supported: i16,
        max_supported: i16,
    ) -> BytesMut {
        let mut body = BytesMut::new();
        put_compact_nullable_string(&mut body, cluster_id);
        body.put_i32(current_leader_epoch);
        body.put_i32(voter_id);
        put_uuid(&mut body, voter_directory_id);
        put_compact_array_len(&mut body, listeners.len());
        for (name, host, port) in listeners {
            put_compact_string(&mut body, name);
            put_compact_string(&mut body, host);
            body.put_u16(*port);
            put_empty_tag_buffer(&mut body);
        }
        body.put_i16(min_supported);
        body.put_i16(max_supported);
        put_empty_tag_buffer(&mut body); // KRaftVersionFeature tags
        put_empty_tag_buffer(&mut body); // request tags
        body
    }

    fn read_update_raft_voter(src: &mut impl Buf) -> (i32, i16) {
        let throttle = src.get_i32();
        let error = src.get_i16();
        skip_tag_buffer(src).unwrap();
        assert_eq!(src.remaining(), 0);
        (throttle, error)
    }

    #[test]
    fn kafka_update_raft_voter_rejects_and_does_not_persist() {
        let dir = temp_dir("urv-ok");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let before_ids = overlay_ids(&broker);
        assert!(!membership_file(&dir).exists());

        let mut directory = [0u8; 16];
        directory[15] = 0x52;
        let mut src = update_raft_voter_body(
            Some("volant-cluster"),
            1,
            2,
            &directory,
            &[("CONTROLLER", "127.0.0.1", 19094)],
            0,
            1,
        );
        let mut out = BytesMut::new();
        encode_update_raft_voter(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error) = read_update_raft_voter(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());

        assert_eq!(overlay_ids(&broker), before_ids);
        assert!(
            !membership_file(&dir).exists(),
            "UpdateRaftVoter must not create membership.json"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_update_raft_voter_existing_brokers_unchanged() {
        let dir = temp_dir("urv-exist");
        let broker = cluster_n2(dir.clone(), 1);
        broker
            .add_broker(3, "127.0.0.1".into(), 19403, None)
            .unwrap();
        let before = overlay_ids(&broker);
        assert!(before.contains(&3));

        let mut directory = [0u8; 16];
        directory[0] = 0xaa;
        let mut src = update_raft_voter_body(
            None,
            3,
            4,
            &directory,
            &[("CONTROLLER", "127.0.0.1", 19404)],
            1,
            1,
        );
        let mut out = BytesMut::new();
        encode_update_raft_voter(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error) = read_update_raft_voter(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());

        let after = overlay_ids(&broker);
        assert_eq!(after, before);
        assert!(!after.contains(&4));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_update_raft_voter_acl_deny_is_31() {
        let dir = temp_dir("urv-acl");
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker
            .configure_acls(true, None, vec![], "token".into())
            .unwrap();

        let mut directory = [0u8; 16];
        directory[1] = 0xbb;
        let mut src = update_raft_voter_body(Some("c"), 0, 2, &directory, &[], 0, 0);
        let mut out = BytesMut::new();
        encode_update_raft_voter(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error) = read_update_raft_voter(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        assert!(!membership_file(&dir).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn kafka_update_raft_voter_not_controller_still_42() {
        let dir = temp_dir("urv-nc");
        let broker = cluster_n2(dir.clone(), 2);
        assert!(!broker.is_controller());
        let before = overlay_ids(&broker);

        let mut directory = [0u8; 16];
        directory[2] = 0xcc;
        let mut src = update_raft_voter_body(
            Some("volant-cluster"),
            9,
            9,
            &directory,
            &[("CONTROLLER", "127.0.0.1", 19099)],
            0,
            1,
        );
        let mut out = BytesMut::new();
        encode_update_raft_voter(&broker, &mut src, &mut out, "kafka-anonymous");
        let mut resp = out.freeze();
        let (throttle, error) = read_update_raft_voter(&mut resp);
        assert_eq!(throttle, 0);
        assert_eq!(error, KafkaErrorCode::InvalidRequest.as_i16());
        assert_eq!(overlay_ids(&broker), before);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
