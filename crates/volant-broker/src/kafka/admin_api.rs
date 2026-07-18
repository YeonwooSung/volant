//! Kafka wire handlers: Create/Delete topics, CreatePartitions, configs.

use bytes::{Buf, BufMut, BytesMut};
use volant_core::{Error, TopicName};

use crate::acl::{
    AclOperation, ResourceType, CLUSTER_RESOURCE,
};
use crate::broker::Broker;

use super::codec::{
    get_compact_array_len,
    get_compact_nullable_string, get_compact_string, get_nullable_string, get_string, get_uuid, put_compact_array_len, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_nullable_string, put_string, put_unsigned_varint, skip_tag_buffer, KAFKA_UUID_ZERO,
};
use super::topic_id;
use super::KafkaErrorCode;


/// Default partition count when CreateTopics v4+ sends `num_partitions = -1`.
const DEFAULT_TOPIC_PARTITIONS: u32 = 1;

pub(crate) fn encode_create_topics(
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

        let result = if t.configs.is_empty() {
            broker.create_topic(t.name.as_str(), partitions)
        } else {
            broker.create_topic_with_configs(t.name.as_str(), partitions, &t.configs)
        };

        match result {
            Ok(id) => write_result(
                out,
                KafkaErrorCode::None,
                None,
                partitions as i32,
                1,
                Some(id.0),
            ),
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

pub(crate) fn encode_delete_topics(
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
                        early_err: r
                            .unknown_topic_id
                            .then_some(KafkaErrorCode::UnknownTopicId),
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
                match broker.delete_topic(&TopicName::new(name.clone())) {
                    Ok(()) => (KafkaErrorCode::None, None),
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

pub(crate) fn encode_create_partitions(
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
            )
        {
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
            match broker.create_partitions(&r.topic, r.count as u32) {
                Ok(_) => (KafkaErrorCode::None.as_i16(), None),
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

pub(crate) fn encode_alter_configs(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // AlterConfigs classic v0–1 + flexible v2:
    //   request: resources[{type, name, configs[{name, value}]}], validate_only
    //   response: throttle (all versions), responses[{error, error_message, type, name}]
    // Phase 46: leading throttle (Kafka has throttle on v0+).
    let flexible = version >= 2;
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
            RES_BROKER => alter_broker_resource(broker, principal, &r.entries, validate_only),
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
}

pub(crate) fn volant_broker_topic_config_validate(entries: &[(String, String)]) -> std::result::Result<(), String> {
    crate::topic_config::TopicConfig::from_entries(entries).map(|_| ()).map_err(|e| e.to_string())
}

/// AlterConfigs / IncrementalAlter for BROKER resources (Phase 99).
fn alter_broker_resource(
    broker: &Broker,
    principal: &str,
    entries: &[(String, String)],
    validate_only: bool,
) -> (i16, Option<String>) {
    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        )
    {
        return (KafkaErrorCode::ClusterAuthorizationFailed.as_i16(), None);
    }
    if validate_only {
        return match crate::broker_config::validate_entries(entries) {
            Ok(()) => (KafkaErrorCode::None.as_i16(), None),
            Err(Error::InvalidArgument(msg)) => {
                (KafkaErrorCode::InvalidConfig.as_i16(), Some(msg))
            }
            Err(e) => (KafkaErrorCode::InvalidConfig.as_i16(), Some(e.to_string())),
        };
    }
    match broker.alter_broker_configs(entries) {
        Ok(()) => (KafkaErrorCode::None.as_i16(), None),
        Err(Error::InvalidArgument(msg)) => {
            (KafkaErrorCode::InvalidConfig.as_i16(), Some(msg))
        }
        Err(e) => (KafkaErrorCode::Unknown.as_i16(), Some(e.to_string())),
    }
}

/// IncrementalAlterConfigs (API 44) classic v0 + flexible v1.
///
/// Kafka `ConfigOperation`: 0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT.
/// Volant topic configs only support SET and DELETE (clear via empty value).
pub(crate) fn encode_incremental_alter_configs(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    let flexible = version >= 1;
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
                                    parse_err =
                                        Some(format!("unknown config operation {other}"));
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
                        parse_err = Some(
                            "APPEND/SUBTRACT not supported (no list-typed configs)".into(),
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
                RES_BROKER => alter_broker_resource(broker, principal, &r.entries, validate_only),
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
}

// ---------------------------------------------------------------------------
// Phase 35: DeleteRecords + ACL admin (Describe/Create/DeleteAcls)
// ---------------------------------------------------------------------------
