//! Kafka wire handlers: DeleteRecords and ACL admin APIs.

use bytes::{Buf, BufMut, BytesMut};
use volant_core::Error;

use crate::acl::{
    AclEntry, AclOperation, AclPermission, ResourceType, CLUSTER_RESOURCE,
};
use crate::broker::Broker;

use super::codec::{
    get_compact_array_len,
    get_compact_nullable_string, get_compact_string, get_nullable_string, get_string, put_compact_array_len, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_nullable_string, put_string, skip_tag_buffer,
};
use super::KafkaErrorCode;


/// Kafka ResourceType: Any.
const KAFKA_RT_ANY: i8 = 1;
/// Kafka ResourceType: Topic.
const KAFKA_RT_TOPIC: i8 = 2;
/// Kafka ResourceType: Group.
const KAFKA_RT_GROUP: i8 = 3;
/// Kafka ResourceType: Cluster.
const KAFKA_RT_CLUSTER: i8 = 4;
/// Kafka ResourceType: User (Describe/Create/DeleteAcls v3+).
const KAFKA_RT_USER: i8 = 7;

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

/// Encode Kafka DeleteRecords response.
///
/// Returns successful leader truncates as
/// `(topic, partition, achieved_low_watermark)` so the async accept path can
/// best-effort fan out at the **clamped** log start (Phase 113) — not the
/// client-requested offset when whole-segment delete stops short.
pub(crate) fn encode_delete_records(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) -> Vec<(String, u32, u64)> {
    // DeleteRecords classic v0–1 / flexible v2:
    //   topics[{ name, partitions[{ partition, offset }]}], timeout_ms
    // Response: throttle, topics[{ name, partitions[{ partition, low_watermark, error }]}]
    let flex = version >= 2;
    let mut fanouts: Vec<(String, u32, u64)> = Vec::new();

    let empty = |out: &mut BytesMut| {
        out.put_i32(0); // throttle
        if flex {
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
        } else {
            out.put_i32(0);
        }
    };

    struct PartReq {
        partition: i32,
        offset: i64,
    }
    struct TopicReq {
        name: String,
        parts: Vec<PartReq>,
    }
    let mut topics = Vec::new();

    if flex {
        let topic_count = match get_compact_array_len(src) {
            Ok(Some(n)) => n,
            Ok(None) => 0,
            Err(_) => {
                empty(out);
                return fanouts;
            }
        };
        for _ in 0..topic_count {
            let name = match get_compact_string(src) {
                Ok(n) => n,
                Err(_) => break,
            };
            let pc = match get_compact_array_len(src) {
                Ok(Some(n)) => n,
                Ok(None) => 0,
                Err(_) => break,
            };
            let mut parts = Vec::with_capacity(pc);
            for _ in 0..pc {
                if src.remaining() < 12 {
                    break;
                }
                parts.push(PartReq {
                    partition: src.get_i32(),
                    offset: src.get_i64(),
                });
                let _ = skip_tag_buffer(src);
            }
            let _ = skip_tag_buffer(src);
            topics.push(TopicReq { name, parts });
        }
        if src.remaining() >= 4 {
            let _timeout = src.get_i32();
        }
        let _ = skip_tag_buffer(src);
    } else {
        if src.remaining() < 4 {
            empty(out);
            return fanouts;
        }
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
    }

    out.put_i32(0); // throttle
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
                if flex {
                    put_empty_tag_buffer(out);
                }
                continue;
            }
            if p.partition < 0 || p.offset < 0 {
                out.put_i64(0);
                out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
                if flex {
                    put_empty_tag_buffer(out);
                }
                continue;
            }
            match broker.delete_records(&t.name, p.partition as u32, p.offset as u64) {
                Ok((low, err)) => {
                    out.put_i64(low as i64);
                    let kerr = if err == 0 {
                        KafkaErrorCode::None.as_i16()
                    } else if err == 13 {
                        KafkaErrorCode::NotLeaderForPartition.as_i16()
                    } else {
                        KafkaErrorCode::Unknown.as_i16()
                    };
                    out.put_i16(kerr);
                    // Phase 113: schedule fan-out only after local leader success.
                    // Use achieved `low` (whole-segment clamp), not requested offset.
                    if err == 0 {
                        fanouts.push((t.name.clone(), p.partition as u32, low));
                    }
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
            if flex {
                put_empty_tag_buffer(out);
            }
        }
        if flex {
            put_empty_tag_buffer(out);
        }
    }
    if flex {
        put_empty_tag_buffer(out);
    }
    fanouts
}

pub(crate) fn encode_describe_acls(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // DescribeAcls classic v0–1 / flexible v2–3 (Kafka max):
    //   filter fields → throttle, error, msg, resources[{type, name, pattern, acls[]}]
    // v3: same wire as v2; ResourceType User (7) accepted.
    let flex = version >= 2;

    let write_err = |out: &mut BytesMut, err: i16, msg: Option<&str>| {
        out.put_i32(0); // throttle
        out.put_i16(err);
        if flex {
            put_compact_nullable_string(out, msg);
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
        } else {
            put_nullable_string(out, msg);
            out.put_i32(0);
        }
    };

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        )
    {
        write_err(
            out,
            KafkaErrorCode::ClusterAuthorizationFailed.as_i16(),
            Some("Cluster Describe denied"),
        );
        return;
    }

    // DescribeAcls filter fields are top-level; parse_acl_filter consumes the
    // flexible TAG_BUFFER when flex is true.
    let filter = match parse_acl_filter(src, version, flex) {
        Ok(f) => f,
        Err(msg) => {
            write_err(out, KafkaErrorCode::InvalidRequest.as_i16(), Some(&msg));
            return;
        }
    };

    let matched = filter_acl_entries(broker, &filter);
    let groups = group_acls_by_resource(&matched);

    out.put_i32(0); // throttle
    out.put_i16(KafkaErrorCode::None.as_i16());
    if flex {
        put_compact_nullable_string(out, None);
        put_compact_array_len(out, groups.len());
    } else {
        put_nullable_string(out, None);
        out.put_i32(groups.len() as i32);
    }
    for (rt, name, acls) in groups {
        out.put_i8(rt);
        if flex {
            put_compact_string(out, &name);
        } else {
            put_string(out, &name);
        }
        if version >= 1 {
            out.put_i8(KAFKA_PATTERN_LITERAL);
        }
        if flex {
            put_compact_array_len(out, acls.len());
        } else {
            out.put_i32(acls.len() as i32);
        }
        for e in acls {
            if flex {
                put_compact_string(out, &kafka_principal(&e.principal));
                put_compact_string(out, "*");
            } else {
                put_string(out, &kafka_principal(&e.principal));
                put_string(out, "*");
            }
            out.put_i8(volant_op_to_kafka(e.operation));
            out.put_i8(volant_perm_to_kafka(e.permission));
            if flex {
                put_empty_tag_buffer(out); // acl tags
            }
        }
        if flex {
            put_empty_tag_buffer(out); // resource tags
        }
    }
    if flex {
        put_empty_tag_buffer(out);
    }
}

/// Encode CreateAcls. Returns ACL generation for fan-out when the controller
/// successfully mutates (Phase 113).
pub(crate) fn encode_create_acls(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) -> Option<u64> {
    // CreateAcls classic v0–1 / flexible v2–3 (Kafka max): creations[] →
    // throttle + results[]. v3 wire-identical to v2; User resource type ok.
    let flex = version >= 2;

    let empty = |out: &mut BytesMut| {
        out.put_i32(0); // throttle
        if flex {
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
        } else {
            out.put_i32(0);
        }
    };

    let n = if flex {
        match get_compact_array_len(src) {
            Ok(Some(n)) => n as i32,
            Ok(None) => 0,
            Err(_) => {
                empty(out);
                return None;
            }
        }
    } else {
        if src.remaining() < 4 {
            empty(out);
            return None;
        }
        src.get_i32()
    };

    out.put_i32(0); // throttle

    // Phase 113: cluster ACL mutate is controller-only.
    if broker.cluster_config().is_some() && !broker.is_controller() {
        if flex {
            put_compact_array_len(out, n.max(0) as usize);
        } else {
            out.put_i32(n.max(0));
        }
        for _ in 0..n.max(0) {
            let _ = parse_acl_creation(src, version, flex);
            out.put_i16(KafkaErrorCode::NotController.as_i16());
            if flex {
                put_compact_nullable_string(
                    out,
                    Some(&format!(
                        "not controller; controller_id={}",
                        broker.controller_id()
                    )),
                );
                put_empty_tag_buffer(out);
            } else {
                put_nullable_string(
                    out,
                    Some(&format!(
                        "not controller; controller_id={}",
                        broker.controller_id()
                    )),
                );
            }
        }
        if flex {
            let _ = skip_tag_buffer(src);
            put_empty_tag_buffer(out);
        }
        return None;
    }

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        )
    {
        if flex {
            put_compact_array_len(out, n.max(0) as usize);
        } else {
            out.put_i32(n.max(0));
        }
        for _ in 0..n.max(0) {
            let _ = parse_acl_creation(src, version, flex);
            out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
            if flex {
                put_compact_nullable_string(out, Some("Cluster Alter denied"));
                put_empty_tag_buffer(out);
            } else {
                put_nullable_string(out, Some("Cluster Alter denied"));
            }
        }
        if flex {
            let _ = skip_tag_buffer(src);
            put_empty_tag_buffer(out);
        }
        return None;
    }

    struct CreationResult {
        error: i16,
        message: Option<String>,
    }
    let mut results = Vec::new();
    let mut to_create = Vec::new();

    for _ in 0..n.max(0) {
        match parse_acl_creation(src, version, flex) {
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
    if flex {
        let _ = skip_tag_buffer(src);
    }

    let mut fanout_gen: Option<u64> = None;
    if !to_create.is_empty() {
        match broker.create_acls_admin(to_create) {
            Ok(gen) => fanout_gen = gen,
            Err(Error::InvalidArgument(m)) if m.starts_with("not controller") => {
                for r in results.iter_mut() {
                    if r.error == KafkaErrorCode::None.as_i16() {
                        r.error = KafkaErrorCode::NotController.as_i16();
                        r.message = Some(m.clone());
                    }
                }
            }
            Err(_) => {
                for r in results.iter_mut() {
                    if r.error == KafkaErrorCode::None.as_i16() {
                        r.error = KafkaErrorCode::Unknown.as_i16();
                        r.message = Some("failed to persist ACLs".into());
                    }
                }
            }
        }
    }

    if flex {
        put_compact_array_len(out, results.len());
    } else {
        out.put_i32(results.len() as i32);
    }
    for r in results {
        out.put_i16(r.error);
        if flex {
            put_compact_nullable_string(out, r.message.as_deref());
            put_empty_tag_buffer(out);
        } else {
            put_nullable_string(out, r.message.as_deref());
        }
    }
    if flex {
        put_empty_tag_buffer(out);
    }
    fanout_gen
}

/// Encode DeleteAcls. Returns ACL generation for fan-out when the controller
/// successfully removes at least one binding (Phase 113).
pub(crate) fn encode_delete_acls(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) -> Option<u64> {
    // DeleteAcls classic v0–1 / flexible v2–3 (Kafka max): filters[] →
    // throttle + filter_results[]. v3 wire-identical to v2; User resource ok.
    let flex = version >= 2;

    let empty = |out: &mut BytesMut| {
        out.put_i32(0); // throttle
        if flex {
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
        } else {
            out.put_i32(0);
        }
    };

    let n = if flex {
        match get_compact_array_len(src) {
            Ok(Some(n)) => n as i32,
            Ok(None) => 0,
            Err(_) => {
                empty(out);
                return None;
            }
        }
    } else {
        if src.remaining() < 4 {
            empty(out);
            return None;
        }
        src.get_i32()
    };

    out.put_i32(0); // throttle

    // Phase 113: cluster ACL mutate is controller-only.
    if broker.cluster_config().is_some() && !broker.is_controller() {
        if flex {
            put_compact_array_len(out, n.max(0) as usize);
        } else {
            out.put_i32(n.max(0));
        }
        let msg = format!(
            "not controller; controller_id={}",
            broker.controller_id()
        );
        for _ in 0..n.max(0) {
            let _ = parse_acl_filter(src, version, flex);
            out.put_i16(KafkaErrorCode::NotController.as_i16());
            if flex {
                put_compact_nullable_string(out, Some(&msg));
                put_compact_array_len(out, 0);
                put_empty_tag_buffer(out);
            } else {
                put_nullable_string(out, Some(&msg));
                out.put_i32(0);
            }
        }
        if flex {
            let _ = skip_tag_buffer(src);
            put_empty_tag_buffer(out);
        }
        return None;
    }

    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Alter,
        )
    {
        if flex {
            put_compact_array_len(out, n.max(0) as usize);
        } else {
            out.put_i32(n.max(0));
        }
        for _ in 0..n.max(0) {
            let _ = parse_acl_filter(src, version, flex);
            out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
            if flex {
                put_compact_nullable_string(out, Some("Cluster Alter denied"));
                put_compact_array_len(out, 0);
                put_empty_tag_buffer(out);
            } else {
                put_nullable_string(out, Some("Cluster Alter denied"));
                out.put_i32(0);
            }
        }
        if flex {
            let _ = skip_tag_buffer(src);
            put_empty_tag_buffer(out);
        }
        return None;
    }

    let mut fanout_gen: Option<u64> = None;
    if flex {
        put_compact_array_len(out, n.max(0) as usize);
    } else {
        out.put_i32(n.max(0));
    }
    for _ in 0..n.max(0) {
        let filter = match parse_acl_filter(src, version, flex) {
            Ok(f) => f,
            Err(msg) => {
                out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
                if flex {
                    put_compact_nullable_string(out, Some(&msg));
                    put_compact_array_len(out, 0);
                    put_empty_tag_buffer(out);
                } else {
                    put_nullable_string(out, Some(&msg));
                    out.put_i32(0);
                }
                continue;
            }
        };
        let matched = filter_acl_entries(broker, &filter);
        match broker.delete_acls_admin(&matched) {
            Ok((_n, gen)) => {
                if let Some(g) = gen {
                    fanout_gen = Some(g);
                }
                out.put_i16(KafkaErrorCode::None.as_i16());
                if flex {
                    put_compact_nullable_string(out, None);
                    put_compact_array_len(out, matched.len());
                } else {
                    put_nullable_string(out, None);
                    out.put_i32(matched.len() as i32);
                }
                for e in &matched {
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    if flex {
                        put_compact_nullable_string(out, None);
                    } else {
                        put_nullable_string(out, None);
                    }
                    out.put_i8(volant_rt_to_kafka(e.resource_type));
                    if flex {
                        put_compact_string(
                            out,
                            &kafka_resource_name(e.resource_type, &e.resource),
                        );
                    } else {
                        put_string(out, &kafka_resource_name(e.resource_type, &e.resource));
                    }
                    if version >= 1 {
                        out.put_i8(KAFKA_PATTERN_LITERAL);
                    }
                    if flex {
                        put_compact_string(out, &kafka_principal(&e.principal));
                        put_compact_string(out, "*");
                    } else {
                        put_string(out, &kafka_principal(&e.principal));
                        put_string(out, "*");
                    }
                    out.put_i8(volant_op_to_kafka(e.operation));
                    out.put_i8(volant_perm_to_kafka(e.permission));
                    if flex {
                        put_empty_tag_buffer(out);
                    }
                }
                if flex {
                    put_empty_tag_buffer(out);
                }
            }
            Err(Error::InvalidArgument(m)) if m.starts_with("not controller") => {
                out.put_i16(KafkaErrorCode::NotController.as_i16());
                if flex {
                    put_compact_nullable_string(out, Some(&m));
                    put_compact_array_len(out, 0);
                    put_empty_tag_buffer(out);
                } else {
                    put_nullable_string(out, Some(&m));
                    out.put_i32(0);
                }
            }
            Err(_) => {
                out.put_i16(KafkaErrorCode::Unknown.as_i16());
                if flex {
                    put_compact_nullable_string(out, Some("failed to delete ACLs"));
                    put_compact_array_len(out, 0);
                    put_empty_tag_buffer(out);
                } else {
                    put_nullable_string(out, Some("failed to delete ACLs"));
                    out.put_i32(0);
                }
            }
        }
    }
    if flex {
        let _ = skip_tag_buffer(src);
        put_empty_tag_buffer(out);
    }
    fanout_gen
}

/// Parsed Kafka ACL filter (Describe/Delete).
pub(crate) struct AclFilter {
    resource_type: Option<ResourceType>,
    resource_name: Option<String>,
    principal: Option<String>,
    operation: Option<AclOperation>,
    permission: Option<AclPermission>,
}

pub(crate) fn parse_acl_filter(
    src: &mut impl Buf,
    version: i16,
    flex: bool,
) -> std::result::Result<AclFilter, String> {
    if src.remaining() < 1 {
        return Err("truncated ACL filter".into());
    }
    let rt_raw = src.get_i8();
    let resource_type = kafka_rt_to_volant_filter(rt_raw, version)?;
    let resource_name = if flex {
        match get_compact_nullable_string(src) {
            Ok(Some(s)) if !s.is_empty() => Some(normalize_resource_name(resource_type, &s)),
            Ok(_) => None,
            Err(_) => return Err("invalid resource name filter".into()),
        }
    } else {
        match get_nullable_string(src) {
            Ok(Some(s)) if !s.is_empty() => Some(normalize_resource_name(resource_type, &s)),
            Ok(_) => None,
            Err(_) => return Err("invalid resource name filter".into()),
        }
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
    let principal = if flex {
        match get_compact_nullable_string(src) {
            Ok(Some(s)) if !s.is_empty() => Some(strip_user_prefix(&s)),
            Ok(_) => None,
            Err(_) => return Err("invalid principal filter".into()),
        }
    } else {
        match get_nullable_string(src) {
            Ok(Some(s)) if !s.is_empty() => Some(strip_user_prefix(&s)),
            Ok(_) => None,
            Err(_) => return Err("invalid principal filter".into()),
        }
    };
    // Host filter — ignored (Volant has no host dimension).
    let _host = if flex {
        match get_compact_nullable_string(src) {
            Ok(h) => h,
            Err(_) => return Err("invalid host filter".into()),
        }
    } else {
        match get_nullable_string(src) {
            Ok(h) => h,
            Err(_) => return Err("invalid host filter".into()),
        }
    };
    if src.remaining() < 2 {
        return Err("truncated operation/permission filter".into());
    }
    let op_raw = src.get_i8();
    let perm_raw = src.get_i8();
    let operation = kafka_op_to_volant_filter(op_raw)?;
    let permission = kafka_perm_to_volant_filter(perm_raw)?;
    if flex {
        let _ = skip_tag_buffer(src); // filter struct tags
    }
    Ok(AclFilter {
        resource_type,
        resource_name,
        principal,
        operation,
        permission,
    })
}

pub(crate) fn parse_acl_creation(
    src: &mut impl Buf,
    version: i16,
    flex: bool,
) -> std::result::Result<AclEntry, String> {
    if src.remaining() < 1 {
        return Err("truncated ACL creation".into());
    }
    let rt_raw = src.get_i8();
    let resource_type = kafka_rt_to_volant(rt_raw, version)?;
    let resource_name = if flex {
        match get_compact_string(src) {
            Ok(s) if !s.is_empty() => normalize_resource_name(Some(resource_type), &s),
            Ok(_) => return Err("empty resource name".into()),
            Err(_) => return Err("invalid resource name".into()),
        }
    } else {
        match get_string(src) {
            Ok(s) if !s.is_empty() => normalize_resource_name(Some(resource_type), &s),
            Ok(_) => return Err("empty resource name".into()),
            Err(_) => return Err("invalid resource name".into()),
        }
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
    let principal = if flex {
        match get_compact_string(src) {
            Ok(s) if !s.is_empty() => strip_user_prefix(&s),
            Ok(_) => return Err("empty principal".into()),
            Err(_) => return Err("invalid principal".into()),
        }
    } else {
        match get_string(src) {
            Ok(s) if !s.is_empty() => strip_user_prefix(&s),
            Ok(_) => return Err("empty principal".into()),
            Err(_) => return Err("invalid principal".into()),
        }
    };
    let _host = if flex {
        match get_compact_string(src) {
            Ok(h) => h,
            Err(_) => return Err("invalid host".into()),
        }
    } else {
        match get_string(src) {
            Ok(h) => h,
            Err(_) => return Err("invalid host".into()),
        }
    };
    if src.remaining() < 2 {
        return Err("truncated operation/permission".into());
    }
    let op_raw = src.get_i8();
    let perm_raw = src.get_i8();
    let operation = kafka_op_to_volant(op_raw)?;
    let permission = kafka_perm_to_volant(perm_raw)?;
    if flex {
        let _ = skip_tag_buffer(src); // creation struct tags
    }
    Ok(AclEntry {
        principal,
        resource_type,
        resource: resource_name,
        operation,
        permission,
    })
}

pub(crate) fn filter_acl_entries(broker: &Broker, filter: &AclFilter) -> Vec<AclEntry> {
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

pub(crate) fn group_acls_by_resource(entries: &[AclEntry]) -> Vec<(i8, String, Vec<AclEntry>)> {
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

/// Map Kafka resource type int8 → Volant.
///
/// `User` (7) is only accepted on Describe/Create/DeleteAcls **v3+** (Kafka max).
/// TransactionalId / DelegationToken remain unsupported at every version.
pub(crate) fn kafka_rt_to_volant(
    v: i8,
    version: i16,
) -> std::result::Result<ResourceType, String> {
    match v {
        KAFKA_RT_TOPIC => Ok(ResourceType::Topic),
        KAFKA_RT_GROUP => Ok(ResourceType::Group),
        KAFKA_RT_CLUSTER => Ok(ResourceType::Cluster),
        KAFKA_RT_USER if version >= 3 => Ok(ResourceType::User),
        KAFKA_RT_USER => Err("User resource type requires Describe/Create/DeleteAcls v3+".into()),
        other => Err(format!("unsupported resource type {other}")),
    }
}

pub(crate) fn kafka_rt_to_volant_filter(
    v: i8,
    version: i16,
) -> std::result::Result<Option<ResourceType>, String> {
    if v == KAFKA_RT_ANY {
        return Ok(None);
    }
    kafka_rt_to_volant(v, version).map(Some)
}

pub(crate) fn volant_rt_to_kafka(rt: ResourceType) -> i8 {
    match rt {
        ResourceType::Topic => KAFKA_RT_TOPIC,
        ResourceType::Group => KAFKA_RT_GROUP,
        ResourceType::Cluster => KAFKA_RT_CLUSTER,
        ResourceType::User => KAFKA_RT_USER,
    }
}

pub(crate) fn kafka_op_to_volant(v: i8) -> std::result::Result<AclOperation, String> {
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

pub(crate) fn kafka_op_to_volant_filter(v: i8) -> std::result::Result<Option<AclOperation>, String> {
    if v == KAFKA_OP_ANY {
        return Ok(None);
    }
    kafka_op_to_volant(v).map(Some)
}

pub(crate) fn volant_op_to_kafka(op: AclOperation) -> i8 {
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

pub(crate) fn kafka_perm_to_volant(v: i8) -> std::result::Result<AclPermission, String> {
    match v {
        KAFKA_PERM_DENY => Ok(AclPermission::Deny),
        KAFKA_PERM_ALLOW => Ok(AclPermission::Allow),
        other => Err(format!("unsupported permission type {other}")),
    }
}

pub(crate) fn kafka_perm_to_volant_filter(v: i8) -> std::result::Result<Option<AclPermission>, String> {
    if v == KAFKA_PERM_ANY {
        return Ok(None);
    }
    kafka_perm_to_volant(v).map(Some)
}

pub(crate) fn volant_perm_to_kafka(p: AclPermission) -> i8 {
    match p {
        AclPermission::Deny => KAFKA_PERM_DENY,
        AclPermission::Allow => KAFKA_PERM_ALLOW,
    }
}

pub(crate) fn strip_user_prefix(principal: &str) -> String {
    if let Some(rest) = principal.strip_prefix("User:") {
        rest.to_string()
    } else {
        principal.to_string()
    }
}

pub(crate) fn kafka_principal(volant_principal: &str) -> String {
    if volant_principal.starts_with("User:") {
        volant_principal.to_string()
    } else {
        format!("User:{volant_principal}")
    }
}

pub(crate) fn normalize_resource_name(rt: Option<ResourceType>, name: &str) -> String {
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

pub(crate) fn kafka_resource_name(rt: ResourceType, volant_name: &str) -> String {
    if rt == ResourceType::Cluster {
        KAFKA_CLUSTER_NAME.to_string()
    } else {
        volant_name.to_string()
    }
}
