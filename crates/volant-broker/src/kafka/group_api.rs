//! Kafka wire handlers: group membership, offsets, Describe/List/Delete groups.

use bytes::{Buf, BufMut, BytesMut};

use crate::acl::{AclOperation, ResourceType, CLUSTER_RESOURCE};
use crate::broker::Broker;
use crate::group::static_member_id;

use super::acl_api::volant_op_to_kafka;
use super::codec::{
    decode_consumer_subscription, encode_consumer_assignment, get_bytes, get_compact_array_len,
    get_compact_bytes, get_compact_nullable_string, get_compact_string, get_nullable_string,
    get_string, put_bytes, put_compact_array_len, put_compact_bytes,
    put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, put_nullable_string,
    put_string, skip_tag_buffer,
};
use super::meta_api::AUTH_OPS_OMITTED;
use super::topic_id;
use super::{map_group_error, KafkaErrorCode};

pub(crate) fn encode_join_group(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
    // JoinGroup classic v0–5 / flexible v6–9:
    //   group_id, session_timeout, rebalance_timeout (v1+), member_id,
    //   group_instance_id (v5+), protocol_type, [protocols], reason (v8+)
    // Response: throttle (v2+), error, generation,
    //   protocol_type (v7+), protocol_name, leader, skip_assignment (v9+),
    //   member_id, members[{ member_id, group_instance_id (v5+), metadata }]
    // Flexible (v6+): compact strings/arrays/bytes + TAG_BUFFER per struct;
    // response header v1 (handled in dispatch).
    let flex = version >= 6;
    let write_error_body =
        |out: &mut BytesMut, err: i16, generation: i32, protocol_type: &str, protocol: &str, mid: &str| {
            if version >= 2 {
                out.put_i32(0); // throttle
            }
            out.put_i16(err);
            out.put_i32(generation);
            if flex {
                if version >= 7 {
                    put_compact_nullable_string(out, Some(protocol_type));
                    put_compact_nullable_string(out, Some(protocol));
                } else {
                    put_compact_string(out, protocol);
                }
                put_compact_string(out, ""); // leader
                if version >= 9 {
                    out.put_u8(0); // SkipAssignment = false (classic client assignors)
                }
                put_compact_string(out, mid);
                put_compact_array_len(out, 0);
                put_empty_tag_buffer(out);
            } else {
                put_string(out, protocol);
                put_string(out, "");
                put_string(out, mid);
                out.put_i32(0);
            }
        };

    let group_id = if flex {
        match get_compact_string(src) {
            Ok(g) => g,
            Err(_) => {
                write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "", "");
                return;
            }
        }
    } else {
        match get_string(src) {
            Ok(g) => g,
            Err(_) => {
                write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "", "");
                return;
            }
        }
    };
    if src.remaining() < 4 {
        write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "", "");
        return;
    }
    let session_timeout = src.get_i32().max(0) as u32;
    if version >= 1 {
        if src.remaining() < 4 {
            write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "", "");
            return;
        }
        let _rebalance_timeout = src.get_i32();
    }
    let member_id = if flex {
        match get_compact_string(src) {
            Ok(m) => m,
            Err(_) => {
                write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "", "");
                return;
            }
        }
    } else {
        match get_string(src) {
            Ok(m) => m,
            Err(_) => {
                write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "", "");
                return;
            }
        }
    };
    let group_instance_id = if version >= 5 {
        if flex {
            get_compact_nullable_string(src)
                .ok()
                .flatten()
                .unwrap_or_default()
        } else {
            get_nullable_string(src).ok().flatten().unwrap_or_default()
        }
    } else {
        String::new()
    };
    let protocol_type = if flex {
        match get_compact_string(src) {
            Ok(p) => p,
            Err(_) => {
                write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "", "");
                return;
            }
        }
    } else {
        match get_string(src) {
            Ok(p) => p,
            Err(_) => {
                write_error_body(out, KafkaErrorCode::InvalidRequest.as_i16(), 0, "", "", "");
                return;
            }
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
            &protocol_type,
            "",
            "",
        );
        return;
    }

    let mut selected_protocol = String::from("range");
    let mut topics: Vec<String> = Vec::new();
    if flex {
        let protocol_count = match get_compact_array_len(src) {
            Ok(Some(n)) => n,
            Ok(None) => 0,
            Err(_) => {
                write_error_body(
                    out,
                    KafkaErrorCode::InvalidRequest.as_i16(),
                    0,
                    &protocol_type,
                    "",
                    "",
                );
                return;
            }
        };
        for i in 0..protocol_count {
            let name = match get_compact_string(src) {
                Ok(n) => n,
                Err(_) => break,
            };
            let meta = match get_compact_bytes(src) {
                Ok(b) => b.unwrap_or_default(),
                Err(_) => break,
            };
            let _ = skip_tag_buffer(src); // protocol tags
            if i == 0 {
                selected_protocol = name;
                if let Ok(t) = decode_consumer_subscription(&meta) {
                    topics = t;
                }
            }
        }
        // Reason (v8+): informational only.
        if version >= 8 {
            let _ = get_compact_nullable_string(src);
        }
        let _ = skip_tag_buffer(src); // request top-level tags
    } else {
        if src.remaining() < 4 {
            write_error_body(
                out,
                KafkaErrorCode::InvalidRequest.as_i16(),
                0,
                &protocol_type,
                "",
                "",
            );
            return;
        }
        let protocol_count = src.get_i32();
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
                &protocol_type,
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
            &protocol_type,
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
    if flex {
        // v7+: ProtocolType then nullable ProtocolName; v6: ProtocolName only.
        if version >= 7 {
            put_compact_nullable_string(out, Some(protocol_type.as_str()));
            put_compact_nullable_string(out, Some(selected_protocol.as_str()));
        } else {
            put_compact_string(out, &selected_protocol);
        }
        put_compact_string(out, &leader);
        if version >= 9 {
            // Classic consumer protocol: leader may still run client assignors.
            // Coordinator also assigns; leader SyncGroup payload is ignored.
            out.put_u8(0); // SkipAssignment = false
        }
        put_compact_string(out, &result.member_id);
        if result.member_id == leader {
            put_compact_array_len(out, members_snap.len());
            for m in &members_snap {
                put_compact_string(out, &m.member_id);
                // group_instance_id v5+ (always present for flex v6+)
                if m.member_id == result.member_id {
                    put_compact_nullable_string(out, self_instance);
                } else if let Some(inst) = m.member_id.strip_prefix("static:") {
                    put_compact_nullable_string(out, Some(inst));
                } else {
                    put_compact_nullable_string(out, None);
                }
                put_compact_bytes(out, Some(&[]));
                put_empty_tag_buffer(out); // member tags
            }
        } else {
            put_compact_array_len(out, 0);
        }
        put_empty_tag_buffer(out); // top-level tags
    } else {
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
}

pub(crate) fn encode_sync_group(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // SyncGroup classic v0–3 / flexible v4–5:
    //   group_id, generation, member_id, group_instance_id (v3+),
    //   protocol_type + protocol_name (v5+), [assignments]
    // Response: throttle (v1+), error, protocol_type/name (v5+),
    //   assignment bytes (+ TAG_BUFFER when flex)
    let flex = version >= 4;
    let fail = |out: &mut BytesMut, err: i16, ptype: Option<&str>, pname: Option<&str>| {
        if version >= 1 {
            out.put_i32(0);
        }
        out.put_i16(err);
        if version >= 5 {
            put_compact_nullable_string(out, ptype);
            put_compact_nullable_string(out, pname);
        }
        if flex {
            put_compact_bytes(out, Some(&[]));
            put_empty_tag_buffer(out);
        } else {
            put_bytes(out, Some(&[]));
        }
    };

    let group_id = if flex {
        match get_compact_string(src) {
            Ok(g) => g,
            Err(_) => {
                fail(out, KafkaErrorCode::InvalidRequest.as_i16(), None, None);
                return;
            }
        }
    } else {
        match get_string(src) {
            Ok(g) => g,
            Err(_) => {
                fail(out, KafkaErrorCode::InvalidRequest.as_i16(), None, None);
                return;
            }
        }
    };
    if src.remaining() < 4 {
        fail(out, KafkaErrorCode::InvalidRequest.as_i16(), None, None);
        return;
    }
    let generation = src.get_i32() as u32;
    let mut member_id = if flex {
        match get_compact_string(src) {
            Ok(m) => m,
            Err(_) => {
                fail(out, KafkaErrorCode::InvalidRequest.as_i16(), None, None);
                return;
            }
        }
    } else {
        match get_string(src) {
            Ok(m) => m,
            Err(_) => {
                fail(out, KafkaErrorCode::InvalidRequest.as_i16(), None, None);
                return;
            }
        }
    };
    if version >= 3 {
        let instance = if flex {
            get_compact_nullable_string(src)
                .ok()
                .flatten()
                .unwrap_or_default()
        } else {
            get_nullable_string(src).ok().flatten().unwrap_or_default()
        };
        if member_id.is_empty() && !instance.is_empty() {
            member_id = static_member_id(&instance);
        }
    }
    // ProtocolType / ProtocolName (v5+): echo back; no strict consistency check.
    let (req_protocol_type, req_protocol_name) = if version >= 5 {
        let pt = get_compact_nullable_string(src).ok().flatten();
        let pn = get_compact_nullable_string(src).ok().flatten();
        (pt, pn)
    } else {
        (None, None)
    };
    // Consume leader assignments (ignored — coordinator already assigned).
    if flex {
        match get_compact_array_len(src) {
            Ok(Some(n)) => {
                for _ in 0..n {
                    let _ = get_compact_string(src);
                    let _ = get_compact_bytes(src);
                    let _ = skip_tag_buffer(src);
                }
            }
            Ok(None) | Err(_) => {}
        }
        let _ = skip_tag_buffer(src); // request top-level tags
    } else if src.remaining() >= 4 {
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
        fail(
            out,
            KafkaErrorCode::GroupAuthorizationFailed.as_i16(),
            req_protocol_type.as_deref(),
            req_protocol_name.as_deref(),
        );
        return;
    }

    let result = broker
        .groups()
        .sync_group(&group_id, &member_id, generation);
    if result.error_code != 0 {
        fail(
            out,
            map_group_error(result.error_code),
            req_protocol_type.as_deref(),
            req_protocol_name.as_deref(),
        );
        return;
    }

    let assignment = result.assignment;
    let bytes = encode_consumer_assignment(&assignment);
    if version >= 1 {
        out.put_i32(0); // throttle
    }
    out.put_i16(KafkaErrorCode::None.as_i16());
    if version >= 5 {
        put_compact_nullable_string(out, req_protocol_type.as_deref());
        put_compact_nullable_string(out, req_protocol_name.as_deref());
    }
    if flex {
        put_compact_bytes(out, Some(&bytes));
        put_empty_tag_buffer(out);
    } else {
        put_bytes(out, Some(&bytes));
    }
}

pub(crate) fn encode_heartbeat(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // Heartbeat classic v0–3 / flexible v4:
    //   group_id, generation, member_id, group_instance_id (v3+)
    // Response: throttle (v1+), error (+ TAG_BUFFER when flex)
    let flex = version >= 4;
    let fail = |out: &mut BytesMut, err: i16| {
        if version >= 1 {
            out.put_i32(0);
        }
        out.put_i16(err);
        if flex {
            put_empty_tag_buffer(out);
        }
    };

    let group_id = if flex {
        match get_compact_string(src) {
            Ok(g) => g,
            Err(_) => {
                fail(out, KafkaErrorCode::InvalidRequest.as_i16());
                return;
            }
        }
    } else {
        match get_string(src) {
            Ok(g) => g,
            Err(_) => {
                fail(out, KafkaErrorCode::InvalidRequest.as_i16());
                return;
            }
        }
    };
    if src.remaining() < 4 {
        fail(out, KafkaErrorCode::InvalidRequest.as_i16());
        return;
    }
    let generation = src.get_i32() as u32;
    let mut member_id = if flex {
        match get_compact_string(src) {
            Ok(m) => m,
            Err(_) => {
                fail(out, KafkaErrorCode::InvalidRequest.as_i16());
                return;
            }
        }
    } else {
        match get_string(src) {
            Ok(m) => m,
            Err(_) => {
                fail(out, KafkaErrorCode::InvalidRequest.as_i16());
                return;
            }
        }
    };
    if version >= 3 {
        let instance = if flex {
            get_compact_nullable_string(src)
                .ok()
                .flatten()
                .unwrap_or_default()
        } else {
            get_nullable_string(src).ok().flatten().unwrap_or_default()
        };
        if member_id.is_empty() && !instance.is_empty() {
            member_id = static_member_id(&instance);
        }
    }
    if flex {
        let _ = skip_tag_buffer(src); // request top-level tags
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
    if flex {
        put_empty_tag_buffer(out);
    }
}

pub(crate) fn encode_leave_group(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // LeaveGroup classic v0–3 / flexible v4–5:
    //   v0–2: group_id, member_id
    //   v3+: group_id, members[{ member_id, group_instance_id, reason (v5+) }]
    // Response: throttle (v1+), error, members[] (v3+; compact + tags when flex)
    // v5 response wire-identical to v4 (Reason is request-only).
    let flex = version >= 4;

    let write_leave_error = |out: &mut BytesMut, err: i16, members: &[(String, Option<String>)]| {
        if version >= 1 {
            out.put_i32(0);
        }
        out.put_i16(err);
        if version >= 3 {
            if flex {
                put_compact_array_len(out, members.len());
                for (mid, inst) in members {
                    put_compact_string(out, mid);
                    put_compact_nullable_string(out, inst.as_deref());
                    out.put_i16(err);
                    put_empty_tag_buffer(out);
                }
                put_empty_tag_buffer(out);
            } else {
                out.put_i32(members.len() as i32);
                for (mid, inst) in members {
                    put_string(out, mid);
                    put_nullable_string(out, inst.as_deref());
                    out.put_i16(err);
                }
            }
        } else if flex {
            put_empty_tag_buffer(out);
        }
    };

    let group_id = if flex {
        match get_compact_string(src) {
            Ok(g) => g,
            Err(_) => {
                write_leave_error(out, KafkaErrorCode::InvalidRequest.as_i16(), &[]);
                return;
            }
        }
    } else {
        match get_string(src) {
            Ok(g) => g,
            Err(_) => {
                write_leave_error(out, KafkaErrorCode::InvalidRequest.as_i16(), &[]);
                return;
            }
        }
    };

    // Collect members to leave: (member_id, optional instance_id for response).
    let mut to_leave: Vec<(String, Option<String>)> = Vec::new();
    if version >= 3 {
        if flex {
            let n = match get_compact_array_len(src) {
                Ok(Some(n)) => n,
                Ok(None) => 0,
                Err(_) => {
                    write_leave_error(out, KafkaErrorCode::InvalidRequest.as_i16(), &[]);
                    return;
                }
            };
            for _ in 0..n {
                let mid = match get_compact_string(src) {
                    Ok(m) => m,
                    Err(_) => break,
                };
                let instance = get_compact_nullable_string(src).ok().flatten();
                // Reason (v5+): informational only.
                if version >= 5 {
                    let _ = get_compact_nullable_string(src);
                }
                let _ = skip_tag_buffer(src); // member tags
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
            let _ = skip_tag_buffer(src); // request top-level tags
        } else {
            if src.remaining() < 4 {
                write_leave_error(out, KafkaErrorCode::InvalidRequest.as_i16(), &[]);
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
        }
    } else {
        let member_id = match get_string(src) {
            Ok(m) => m,
            Err(_) => {
                write_leave_error(out, KafkaErrorCode::InvalidRequest.as_i16(), &[]);
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
        write_leave_error(
            out,
            KafkaErrorCode::GroupAuthorizationFailed.as_i16(),
            &to_leave,
        );
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
        if flex {
            put_compact_array_len(out, member_results.len());
            for (mid, inst, err) in &member_results {
                put_compact_string(out, mid);
                put_compact_nullable_string(out, inst.as_deref());
                out.put_i16(*err);
                put_empty_tag_buffer(out);
            }
            put_empty_tag_buffer(out);
        } else {
            out.put_i32(member_results.len() as i32);
            for (mid, inst, err) in member_results {
                put_string(out, &mid);
                put_nullable_string(out, inst.as_deref());
                out.put_i16(err);
            }
        }
    } else if flex {
        put_empty_tag_buffer(out);
    }
}

pub(crate) fn encode_offset_commit(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // OffsetCommit classic v0–7 / flexible v8–10:
    //   v0: group_id, [topic [partition, offset, metadata]]
    //   v1: + generation, member_id; partition commit_timestamp
    //   v2–4: + retention_time_ms (no commit_timestamp)
    //   v5: no retention_time
    //   v6+: + committed_leader_epoch per partition (ignored; not stored)
    //   v7+: + group_instance_id (nullable; maps to static: when member_id empty)
    //   v8–9: compact strings/arrays + TAG_BUFFER; response header v1
    //   v10: TopicId UUID instead of Name (request + response)
    // Response: throttle_time_ms (v3+), [topic [partition, error]] (+ tags when flex)
    let flex = version >= 8;
    let use_topic_id = version >= 10;
    let empty = |out: &mut BytesMut| {
        if version >= 3 {
            out.put_i32(0); // throttle
        }
        if flex {
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
        } else {
            out.put_i32(0);
        }
    };

    let group_id = if flex {
        match get_compact_string(src) {
            Ok(g) => g,
            Err(_) => {
                empty(out);
                return;
            }
        }
    } else {
        match get_string(src) {
            Ok(g) => g,
            Err(_) => {
                empty(out);
                return;
            }
        }
    };

    let mut generation: u32 = 0;
    let mut member_id = String::new();
    if version >= 1 {
        if src.remaining() < 4 {
            empty(out);
            return;
        }
        generation = src.get_i32() as u32;
        member_id = if flex {
            match get_compact_string(src) {
                Ok(m) => m,
                Err(_) => {
                    empty(out);
                    return;
                }
            }
        } else {
            match get_string(src) {
                Ok(m) => m,
                Err(_) => {
                    empty(out);
                    return;
                }
            }
        };
    }
    // v7+: group_instance_id (nullable). Prefer member_id when set; otherwise
    // derive static:{instance} like JoinGroup / Heartbeat.
    if version >= 7 {
        let inst = if flex {
            get_compact_nullable_string(src)
        } else {
            get_nullable_string(src)
        };
        match inst {
            Ok(Some(inst)) if member_id.is_empty() && !inst.is_empty() => {
                member_id = static_member_id(&inst);
            }
            Ok(_) => {}
            Err(_) => {
                empty(out);
                return;
            }
        }
    }
    // Retention only on v2–4 (ignored — broker-controlled retention).
    if (2..=4).contains(&version) {
        if src.remaining() < 8 {
            empty(out);
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

    struct TopicReq {
        /// Wire identity to echo (name or TopicId).
        wire: topic_id::TopicWireId,
        /// Resolved topic name (empty when unknown TopicId).
        topic: String,
        partitions: Vec<i32>,
        unknown_topic_id: bool,
    }
    let mut parsed: Vec<TopicReq> = Vec::new();
    let mut entries: Vec<(String, u32, u64, String)> = Vec::new();

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
            let resolved = match topic_id::read_and_resolve(broker, src, true, use_topic_id) {
                Ok(r) => r,
                Err(_) => break,
            };
            let topic = resolved.name_or_empty().to_string();
            let unknown_topic_id = resolved.is_unknown();
            let wire = resolved.wire;
            let part_count = match get_compact_array_len(src) {
                Ok(Some(n)) => n,
                Ok(None) | Err(_) => 0,
            };
            let mut partitions = Vec::new();
            for _ in 0..part_count {
                if src.remaining() < 4 + 8 {
                    break;
                }
                let partition = src.get_i32();
                let offset = src.get_i64().max(0) as u64;
                // v6+ always for flex v8: committed_leader_epoch
                if src.remaining() < 4 {
                    break;
                }
                let _leader_epoch = src.get_i32();
                let metadata = get_compact_nullable_string(src)
                    .ok()
                    .flatten()
                    .unwrap_or_default();
                let _ = skip_tag_buffer(src); // partition tags
                if !unknown_topic_id {
                    entries.push((topic.clone(), partition as u32, offset, metadata));
                }
                partitions.push(partition);
            }
            let _ = skip_tag_buffer(src); // topic tags
            parsed.push(TopicReq {
                wire,
                topic,
                partitions,
                unknown_topic_id,
            });
        }
        let _ = skip_tag_buffer(src); // request top-level tags
    } else {
        if src.remaining() < 4 {
            empty(out);
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
            parsed.push(TopicReq {
                wire: topic_id::TopicWireId::Name(topic.clone()),
                topic,
                partitions,
                unknown_topic_id: false,
            });
        }
    }

    let has_unknown_id = parsed.iter().any(|t| t.unknown_topic_id);
    let kerr = if auth_denied {
        KafkaErrorCode::GroupAuthorizationFailed.as_i16()
    } else if entries.is_empty() && has_unknown_id {
        // All topics unknown by id — still write per-partition UnknownTopicId below.
        KafkaErrorCode::None.as_i16()
    } else if entries.is_empty() {
        KafkaErrorCode::None.as_i16()
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
    if flex {
        put_compact_array_len(out, parsed.len());
        for t in parsed {
            topic_id::write_wire_id(out, true, &t.wire);
            put_compact_array_len(out, t.partitions.len());
            for p in t.partitions {
                out.put_i32(p);
                let pe = if t.unknown_topic_id {
                    KafkaErrorCode::UnknownTopicId.as_i16()
                } else {
                    kerr
                };
                out.put_i16(pe);
                put_empty_tag_buffer(out); // partition tags
            }
            put_empty_tag_buffer(out); // topic tags
        }
        put_empty_tag_buffer(out); // top-level tags
    } else {
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
}

pub(crate) fn encode_offset_fetch(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // OffsetFetch classic v0–5 / flexible v6–7 (single-group) / multi-group v8–10:
    //   v0–7: group_id, topics nullable (v2+: null=all), require_stable (v7+)
    //   v8+: Groups[] multi-group (no top-level GroupId/Topics); RequireStable
    //   v9+: MemberId + MemberEpoch per group (parsed, ignored)
    //   v10: TopicId UUID instead of Name in Topics[]
    // Response v0–7: throttle (v3+), topics[], top-level error (v2+)
    // Response v8+: throttle, Groups[{ GroupId, Topics, ErrorCode, tags }], tags
    // Flexible: compact arrays/strings + TAG_BUFFER; response header v1
    if version >= 8 {
        encode_offset_fetch_multi(broker, src, out, version, principal);
        return;
    }

    let flex = version >= 6;

    let write_partition = |out: &mut BytesMut, partition: i32, offset: i64, meta: &str| {
        out.put_i32(partition);
        out.put_i64(offset);
        if version >= 5 {
            out.put_i32(-1); // committed_leader_epoch unknown
        }
        if flex {
            put_compact_nullable_string(out, Some(meta));
            out.put_i16(KafkaErrorCode::None.as_i16());
            put_empty_tag_buffer(out);
        } else {
            put_string(out, meta);
            out.put_i16(KafkaErrorCode::None.as_i16());
        }
    };

    let write_topics_header = |out: &mut BytesMut, n: usize| {
        if flex {
            put_compact_array_len(out, n);
        } else {
            out.put_i32(n as i32);
        }
    };

    let write_topic_name = |out: &mut BytesMut, topic: &str| {
        if flex {
            put_compact_string(out, topic);
        } else {
            put_string(out, topic);
        }
    };

    let write_parts_header = |out: &mut BytesMut, n: usize| {
        if flex {
            put_compact_array_len(out, n);
        } else {
            out.put_i32(n as i32);
        }
    };

    let finish = |out: &mut BytesMut, top_error: i16| {
        if version >= 2 {
            out.put_i16(top_error);
        }
        if flex {
            put_empty_tag_buffer(out);
        }
    };

    let empty_error = |out: &mut BytesMut, top_error: i16| {
        if version >= 3 {
            out.put_i32(0);
        }
        write_topics_header(out, 0);
        finish(out, top_error);
    };

    let group_id = if flex {
        match get_compact_string(src) {
            Ok(g) => g,
            Err(_) => {
                empty_error(out, KafkaErrorCode::InvalidRequest.as_i16());
                return;
            }
        }
    } else {
        match get_string(src) {
            Ok(g) => g,
            Err(_) => {
                empty_error(out, KafkaErrorCode::InvalidRequest.as_i16());
                return;
            }
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
        // v0–1: empty topics only; v2+: GroupAuthorizationFailed
        empty_error(
            out,
            if version >= 2 {
                KafkaErrorCode::GroupAuthorizationFailed.as_i16()
            } else {
                0
            },
        );
        return;
    }

    let mut query: Vec<(String, u32)> = Vec::new();
    let mut requested: Vec<(String, Vec<i32>)> = Vec::new();
    let list_all;
    let list_none;

    if flex {
        // Compact nullable topics array: None=all, Some(0)=none, Some(n)=listed.
        match get_compact_array_len(src) {
            Ok(None) => {
                list_all = true;
                list_none = false;
            }
            Ok(Some(0)) => {
                list_all = false;
                list_none = true;
            }
            Ok(Some(n)) => {
                list_all = false;
                list_none = false;
                for _ in 0..n {
                    let topic = match get_compact_string(src) {
                        Ok(t) => t,
                        Err(_) => break,
                    };
                    let pc = match get_compact_array_len(src) {
                        Ok(Some(p)) => p,
                        Ok(None) | Err(_) => 0,
                    };
                    let mut parts = Vec::new();
                    for _ in 0..pc {
                        if src.remaining() < 4 {
                            break;
                        }
                        let p = src.get_i32();
                        parts.push(p);
                        query.push((topic.clone(), p as u32));
                    }
                    let _ = skip_tag_buffer(src); // topic tags
                    requested.push((topic, parts));
                }
            }
            Err(_) => {
                empty_error(out, KafkaErrorCode::InvalidRequest.as_i16());
                return;
            }
        }
        // RequireStable (v7+): ignored — no unstable/pending txn offsets.
        if version >= 7 && src.remaining() >= 1 {
            let _require_stable = src.get_u8();
        }
        let _ = skip_tag_buffer(src); // request top-level tags
    } else {
        if src.remaining() < 4 {
            empty_error(out, KafkaErrorCode::None.as_i16());
            return;
        }
        let topic_count = src.get_i32();
        // Topics array semantics:
        //   v0–1: count <= 0 → all (legacy empty-as-all)
        //   v2+:  count < 0 (null) → all; count == 0 → none; count > 0 → listed
        list_all = if version >= 2 {
            topic_count < 0
        } else {
            topic_count <= 0
        };
        list_none = version >= 2 && topic_count == 0;
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
    }

    if version >= 3 {
        out.put_i32(0); // throttle_time_ms
    }

    if list_none {
        write_topics_header(out, 0);
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
        write_topics_header(out, by_topic.len());
        for (topic, parts) in by_topic {
            write_topic_name(out, &topic);
            write_parts_header(out, parts.len());
            for (p, off, meta) in parts {
                write_partition(out, p as i32, off, &meta);
            }
            if flex {
                put_empty_tag_buffer(out); // topic tags
            }
        }
        finish(out, KafkaErrorCode::None.as_i16());
        return;
    }

    write_topics_header(out, requested.len());
    for (topic, parts) in requested {
        write_topic_name(out, &topic);
        write_parts_header(out, parts.len());
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
        if flex {
            put_empty_tag_buffer(out); // topic tags
        }
    }
    finish(out, KafkaErrorCode::None.as_i16());
}

/// OffsetFetch v8–10 multi-group flexible body.
pub(crate) fn encode_offset_fetch_multi(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // Request: Groups[{ GroupId, MemberId+Epoch (v9+), Topics nullable, TAG }],
    //          RequireStable, TAG
    // Response: Throttle, Groups[{ GroupId, Topics (name|TopicId), ErrorCode, TAG }], TAG
    let use_topic_id = version >= 10;

    struct TopicReq {
        name: String,
        wire: topic_id::TopicWireId,
        parts: Vec<i32>,
        unknown_topic_id: bool,
    }
    struct GroupReq {
        group_id: String,
        list_all: bool,
        list_none: bool,
        query: Vec<(String, u32)>,
        requested: Vec<TopicReq>,
    }

    let mut groups: Vec<GroupReq> = Vec::new();
    let group_count = match get_compact_array_len(src) {
        Ok(Some(n)) => n,
        Ok(None) => 0,
        Err(_) => {
            out.put_i32(0); // throttle
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
            return;
        }
    };
    for _ in 0..group_count {
        let group_id = match get_compact_string(src) {
            Ok(g) => g,
            Err(_) => break,
        };
        // v9+: MemberId (nullable) + MemberEpoch (ignored — no KIP-848 membership).
        if version >= 9 {
            let _member_id = get_compact_nullable_string(src);
            if src.remaining() >= 4 {
                let _member_epoch = src.get_i32();
            }
        }
        let mut query: Vec<(String, u32)> = Vec::new();
        let mut requested: Vec<TopicReq> = Vec::new();
        let (list_all, list_none) = match get_compact_array_len(src) {
            Ok(None) => (true, false),
            Ok(Some(0)) => (false, true),
            Ok(Some(n)) => {
                for _ in 0..n {
                    let resolved = match topic_id::read_and_resolve(broker, src, true, use_topic_id) {
                        Ok(r) => r,
                        Err(_) => break,
                    };
                    let name = resolved.name_or_empty().to_string();
                    let unknown = resolved.is_unknown();
                    let wire = resolved.wire;
                    let pc = match get_compact_array_len(src) {
                        Ok(Some(p)) => p,
                        Ok(None) | Err(_) => 0,
                    };
                    let mut parts = Vec::new();
                    for _ in 0..pc {
                        if src.remaining() < 4 {
                            break;
                        }
                        let p = src.get_i32();
                        parts.push(p);
                        if !unknown {
                            query.push((name.clone(), p as u32));
                        }
                    }
                    let _ = skip_tag_buffer(src); // topic tags
                    requested.push(TopicReq {
                        name,
                        wire,
                        parts,
                        unknown_topic_id: unknown,
                    });
                }
                (false, false)
            }
            Err(_) => (false, true),
        };
        let _ = skip_tag_buffer(src); // group tags
        groups.push(GroupReq {
            group_id,
            list_all,
            list_none,
            query,
            requested,
        });
    }
    // RequireStable (v7+ always present for v8): ignored.
    if src.remaining() >= 1 {
        let _require_stable = src.get_u8();
    }
    let _ = skip_tag_buffer(src); // request top-level tags

    out.put_i32(0); // throttle
    put_compact_array_len(out, groups.len());
    for g in groups {
        put_compact_string(out, &g.group_id);

        let auth_denied = broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(principal),
                ResourceType::Group,
                &g.group_id,
                AclOperation::Read,
            );

        if auth_denied {
            put_compact_array_len(out, 0); // empty topics
            out.put_i16(KafkaErrorCode::GroupAuthorizationFailed.as_i16());
            put_empty_tag_buffer(out);
            continue;
        }

        if g.list_none {
            put_compact_array_len(out, 0);
            out.put_i16(KafkaErrorCode::None.as_i16());
            put_empty_tag_buffer(out);
            continue;
        }

        let fetched = match broker.groups().fetch_offsets(
            &g.group_id,
            if g.list_all { &[] } else { &g.query },
        ) {
            Ok(r) => r.entries,
            Err(_) => Vec::new(),
        };

        if g.list_all {
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
            put_compact_array_len(out, by_topic.len());
            for (topic, parts) in by_topic {
                topic_id::write_wire_id(
                    out,
                    true,
                    &topic_id::wire_id_for_name(broker, &topic, use_topic_id),
                );
                put_compact_array_len(out, parts.len());
                for (p, off, meta) in parts {
                    out.put_i32(p as i32);
                    out.put_i64(off);
                    out.put_i32(-1); // committed_leader_epoch
                    put_compact_nullable_string(out, Some(&meta));
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    put_empty_tag_buffer(out);
                }
                put_empty_tag_buffer(out); // topic tags
            }
        } else {
            put_compact_array_len(out, g.requested.len());
            for t in &g.requested {
                topic_id::write_wire_id(out, true, &t.wire);
                put_compact_array_len(out, t.parts.len());
                for &p in &t.parts {
                    if t.unknown_topic_id {
                        out.put_i32(p);
                        out.put_i64(-1);
                        out.put_i32(-1);
                        put_compact_nullable_string(out, None);
                        out.put_i16(KafkaErrorCode::UnknownTopicId.as_i16());
                        put_empty_tag_buffer(out);
                        continue;
                    }
                    let entry = fetched
                        .iter()
                        .find(|e| e.topic == t.name && e.partition == p as u32);
                    let (off, meta) = match entry {
                        Some(e) if e.offset == u64::MAX => (-1i64, e.metadata.as_str()),
                        Some(e) => (e.offset as i64, e.metadata.as_str()),
                        None => (-1i64, ""),
                    };
                    out.put_i32(p);
                    out.put_i64(off);
                    out.put_i32(-1);
                    put_compact_nullable_string(out, Some(meta));
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    put_empty_tag_buffer(out);
                }
                put_empty_tag_buffer(out); // topic tags
            }
        }
        out.put_i16(KafkaErrorCode::None.as_i16()); // group-level error
        put_empty_tag_buffer(out); // group tags
    }
    put_empty_tag_buffer(out); // top-level tags
}

pub(crate) fn encode_list_groups(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // ListGroups classic v0–2: empty request; response throttle (v1+), error, groups[].
    // Flexible v3: request body is TAG_BUFFER only; response uses compact strings/arrays
    // and response header v1.
    // v4: StatesFilter[] + GroupState on ListedGroup.
    // v5: TypesFilter[] + GroupType on ListedGroup.
    let flexible = version >= 3;
    let mut states_filter: Vec<String> = Vec::new();
    let mut types_filter: Vec<String> = Vec::new();
    if flexible {
        if version >= 4 {
            if let Ok(Some(n)) = get_compact_array_len(src) {
                for _ in 0..n {
                    if let Ok(s) = get_compact_string(src) {
                        states_filter.push(s);
                    } else {
                        break;
                    }
                }
            }
        }
        if version >= 5 {
            if let Ok(Some(n)) = get_compact_array_len(src) {
                for _ in 0..n {
                    if let Ok(s) = get_compact_string(src) {
                        types_filter.push(s);
                    } else {
                        break;
                    }
                }
            }
        }
        let _ = skip_tag_buffer(src);
    }

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
        if flexible {
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
        } else {
            out.put_i32(0);
        }
        return;
    }
    let groups = broker.groups().list_groups();
    // ProtocolType is always "consumer"; GroupType (v5+) is always "classic".
    // State: CompletingRebalance while the v0.215 fence is open, else
    // Stable when members are synced, else Empty.
    let filtered: Vec<_> = groups
        .into_iter()
        .filter(|g| {
            let state = g.state.as_str();
            let gtype = "classic";
            let state_ok = states_filter.is_empty()
                || states_filter
                    .iter()
                    .any(|f| f.eq_ignore_ascii_case(state));
            let type_ok = types_filter.is_empty()
                || types_filter
                    .iter()
                    .any(|f| f.eq_ignore_ascii_case(gtype));
            state_ok && type_ok
        })
        .collect();
    out.put_i16(KafkaErrorCode::None.as_i16());
    if flexible {
        put_compact_array_len(out, filtered.len());
        for g in filtered {
            put_compact_string(out, &g.group_id);
            put_compact_string(out, "consumer");
            if version >= 4 {
                put_compact_string(out, g.state.as_str());
            }
            if version >= 5 {
                put_compact_string(out, "classic");
            }
            put_empty_tag_buffer(out); // ListedGroup tags
        }
        put_empty_tag_buffer(out); // top-level tags
    } else {
        out.put_i32(filtered.len() as i32);
        for g in filtered {
            put_string(out, &g.group_id);
            put_string(out, "consumer"); // protocol_type
        }
    }
}

pub(crate) fn encode_describe_groups(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // DescribeGroups classic v0–4 + flexible v5–6:
    //   request: groups[], include_authorized_operations (v3+)
    //   response: throttle (v1+), groups[{error, group_id, state, protocol_type, protocol,
    //             members[{member_id, group_instance_id (v4+), client_id, client_host,
    //             metadata, assignment}], authorized_operations (v3+),
    //             error_message (v6+)}]
    // Flexible v5+: compact arrays/strings/bytes + TAG_BUFFER.
    let flexible = version >= 5;
    let mut ids = Vec::new();
    let include_ops;
    if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => {
                for _ in 0..n {
                    match get_compact_string(src) {
                        Ok(g) => ids.push(g),
                        Err(_) => break,
                    }
                }
            }
            Ok(None) | Err(_) => {}
        }
        include_ops = if version >= 3 && src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
        let _ = skip_tag_buffer(src);
    } else {
        if src.remaining() < 4 {
            if version >= 1 {
                out.put_i32(0);
            }
            out.put_i32(0);
            return;
        }
        let n = src.get_i32();
        for _ in 0..n.max(0) {
            match get_string(src) {
                Ok(g) => ids.push(g),
                Err(_) => break,
            }
        }
        include_ops = if version >= 3 && src.remaining() >= 1 {
            src.get_u8() != 0
        } else {
            false
        };
    }

    if version >= 1 {
        out.put_i32(0); // throttle
    }
    if flexible {
        put_compact_array_len(out, ids.len());
    } else {
        out.put_i32(ids.len() as i32);
    }
    for group_id in ids {
        let write_strings = |out: &mut BytesMut, err: i16, state: &str, ptype: &str, proto: &str| {
            out.put_i16(err);
            if flexible {
                put_compact_string(out, &group_id);
                put_compact_string(out, state);
                put_compact_string(out, ptype);
                put_compact_string(out, proto);
            } else {
                put_string(out, &group_id);
                put_string(out, state);
                put_string(out, ptype);
                put_string(out, proto);
            }
        };

        let put_group_error_message = |out: &mut BytesMut, msg: Option<&str>| {
            if version >= 6 {
                put_compact_nullable_string(out, msg);
            }
        };

        if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(principal),
                ResourceType::Group,
                &group_id,
                AclOperation::Describe,
            )
        {
            write_strings(
                out,
                KafkaErrorCode::GroupAuthorizationFailed.as_i16(),
                "",
                "",
                "",
            );
            if flexible {
                put_compact_array_len(out, 0);
            } else {
                out.put_i32(0); // members
            }
            if version >= 3 {
                out.put_i32(group_authorized_ops(broker, principal, &group_id, include_ops));
            }
            put_group_error_message(out, Some("Group authorization failed"));
            if flexible {
                put_empty_tag_buffer(out); // DescribedGroup tags
            }
            continue;
        }

        match broker.groups().describe_group(&group_id) {
            Some(desc) => {
                write_strings(
                    out,
                    KafkaErrorCode::None.as_i16(),
                    desc.state.as_str(),
                    "consumer",
                    "range",
                );
                if flexible {
                    put_compact_array_len(out, desc.members.len());
                } else {
                    out.put_i32(desc.members.len() as i32);
                }
                for m in &desc.members {
                    let instance = m.member_id.strip_prefix("static:");
                    let topics: Vec<&str> = m.topics.iter().map(|s| s.as_str()).collect();
                    let meta = super::codec::encode_consumer_subscription(&topics);
                    let asg = encode_consumer_assignment(&m.assignment);
                    if flexible {
                        put_compact_string(out, &m.member_id);
                        if version >= 4 {
                            put_compact_nullable_string(out, instance);
                        }
                        put_compact_string(out, "volant-kafka");
                        put_compact_string(out, "/");
                        put_compact_bytes(out, Some(&meta));
                        put_compact_bytes(out, Some(&asg));
                        put_empty_tag_buffer(out); // member tags
                    } else {
                        put_string(out, &m.member_id);
                        if version >= 4 {
                            put_nullable_string(out, instance);
                        }
                        put_string(out, "volant-kafka");
                        put_string(out, "/");
                        put_bytes(out, Some(&meta));
                        put_bytes(out, Some(&asg));
                    }
                }
                if version >= 3 {
                    out.put_i32(group_authorized_ops(broker, principal, &group_id, include_ops));
                }
                put_group_error_message(out, None);
                if flexible {
                    put_empty_tag_buffer(out); // DescribedGroup tags
                }
            }
            None => {
                // Empty or unknown — check if offsets exist.
                let known = broker
                    .groups()
                    .list_group_ids()
                    .iter()
                    .any(|g| g == &group_id);
                let err_msg = if known {
                    write_strings(out, KafkaErrorCode::None.as_i16(), "Empty", "consumer", "");
                    None
                } else {
                    write_strings(
                        out,
                        KafkaErrorCode::GroupIdNotFound.as_i16(),
                        "Dead",
                        "",
                        "",
                    );
                    Some("Group id not found")
                };
                if flexible {
                    put_compact_array_len(out, 0);
                } else {
                    out.put_i32(0);
                }
                if version >= 3 {
                    out.put_i32(group_authorized_ops(broker, principal, &group_id, include_ops));
                }
                put_group_error_message(out, err_msg);
                if flexible {
                    put_empty_tag_buffer(out);
                }
            }
        }
    }
    if flexible {
        put_empty_tag_buffer(out); // top-level tags
    }
}

/// Kafka authorized-operations bitfield for a consumer group (DescribeGroups v3+).
pub(crate) fn group_authorized_ops(broker: &Broker, principal: &str, group_id: &str, include: bool) -> i32 {
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

pub(crate) fn encode_offset_delete(
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

pub(crate) fn encode_delete_groups(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    // DeleteGroups classic v0–1 + flexible v2–3:
    //   request: groups_names[]
    //   response: throttle_time_ms (all versions),
    //     results[{group_id, error_code, error_message (v3+)}]
    // Kafka includes throttle from v0; Phase 43 corrects the earlier missing field.
    // Flexible v2+: compact arrays/strings + TAG_BUFFER.
    let flexible = version >= 2;
    let mut ids = Vec::new();
    if flexible {
        match get_compact_array_len(src) {
            Ok(Some(n)) => {
                for _ in 0..n {
                    match get_compact_string(src) {
                        Ok(g) => ids.push(g),
                        Err(_) => break,
                    }
                }
            }
            Ok(None) | Err(_) => {}
        }
        let _ = skip_tag_buffer(src);
    } else {
        if src.remaining() < 4 {
            out.put_i32(0); // throttle
            out.put_i32(0); // results
            return;
        }
        let n = src.get_i32();
        for _ in 0..n.max(0) {
            match get_string(src) {
                Ok(g) => ids.push(g),
                Err(_) => break,
            }
        }
    }
    out.put_i32(0); // throttle
    if flexible {
        put_compact_array_len(out, ids.len());
    } else {
        out.put_i32(ids.len() as i32);
    }
    for group_id in ids {
        if flexible {
            put_compact_string(out, &group_id);
        } else {
            put_string(out, &group_id);
        }
        let (err, err_msg) = if broker.acls().is_enabled()
            && !broker.acls().authorize(
                Some(principal),
                ResourceType::Group,
                &group_id,
                AclOperation::Delete,
            )
        {
            (
                KafkaErrorCode::GroupAuthorizationFailed.as_i16(),
                Some("Group authorization failed"),
            )
        } else {
            match broker.groups().delete_group(&group_id) {
                Ok(0) => (KafkaErrorCode::None.as_i16(), None),
                Ok(68) => (
                    KafkaErrorCode::NonEmptyGroup.as_i16(),
                    Some("Group is not empty"),
                ),
                Ok(69) => (
                    KafkaErrorCode::GroupIdNotFound.as_i16(),
                    Some("Group id not found"),
                ),
                Ok(_) => (KafkaErrorCode::Unknown.as_i16(), Some("Unknown error")),
                Err(_) => (KafkaErrorCode::Unknown.as_i16(), Some("Unknown error")),
            }
        };
        out.put_i16(err);
        if version >= 3 {
            put_compact_nullable_string(out, err_msg);
        }
        if flexible {
            put_empty_tag_buffer(out); // DeletableGroupResult tags
        }
    }
    if flexible {
        put_empty_tag_buffer(out); // top-level tags
    }
}

