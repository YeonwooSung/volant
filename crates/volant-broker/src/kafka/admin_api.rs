//! Kafka wire handlers: Create/Delete topics, CreatePartitions,
//! AlterPartitionReassignments, ListPartitionReassignments,
//! ElectLeaders, Describe/AlterUserScramCredentials,
//! Describe/AlterClientQuotas, DescribeLogDirs,
//! DescribeTopicPartitions, UnregisterBroker, UpdateFeatures, configs.

use bytes::{Buf, BufMut, BytesMut};
use volant_core::{Error, TopicName};

use crate::acl::{AclOperation, ResourceType, CLUSTER_RESOURCE};
use crate::broker::{Broker, LocalLogDirFilter, LocalLogDirTopic};
use crate::net::{complete_assignment_mutation, fanout_membership_put, snapshot_if_must_wait};

use crate::scram::ScramHash;

use super::codec::{
    get_compact_array_len, get_compact_bytes, get_compact_nullable_string, get_compact_string,
    get_nullable_string, get_string, get_uuid, put_compact_array_len, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_nullable_string, put_string, put_unsigned_varint,
    skip_tag_buffer, KAFKA_UUID_ZERO,
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
}
