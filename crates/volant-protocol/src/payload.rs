//! Little-endian payload encode/decode for Phase 2/3 request/response bodies.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_core::{Error, Result};

use crate::codec::checksum;
use crate::frame::{Frame, FrameHeader, PROTOCOL_VERSION};
use crate::request::{
    AclBinding, MembershipBroker, OffsetCommitEntry, OffsetEntry, ProduceMessage, Request,
    RequestOpcode, TxnOffsetCommit,
};
use crate::response::{
    Assignment, BrokerInfo, ClusterPartitionState, ClusterTopicState, ErrorCode, FetchRecord,
    GroupListing, GroupMemberInfo, GroupState, OffsetFetchEntry, OffsetListing, PartitionInfo,
    Response, ResponseOpcode, TopicInfo, TxnProduceResult,
};

/// Maximum accepted payload size (16 MiB).
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024;

fn put_string(dst: &mut BytesMut, s: &str) -> Result<()> {
    let bytes = s.as_bytes();
    if bytes.len() > u16::MAX as usize {
        return Err(Error::Protocol(format!(
            "string too long: {} bytes",
            bytes.len()
        )));
    }
    dst.put_u16_le(bytes.len() as u16);
    dst.extend_from_slice(bytes);
    Ok(())
}

fn put_acl_binding(dst: &mut BytesMut, e: &AclBinding) -> Result<()> {
    put_string(dst, &e.principal)?;
    dst.put_u8(e.resource_type);
    put_string(dst, &e.resource)?;
    dst.put_u8(e.operation);
    dst.put_u8(e.permission);
    Ok(())
}

fn get_acl_binding(src: &mut impl Buf) -> Result<AclBinding> {
    let principal = get_string(src)?;
    if src.remaining() < 1 {
        return Err(Error::Protocol("truncated acl resource_type".into()));
    }
    let resource_type = src.get_u8();
    let resource = get_string(src)?;
    if src.remaining() < 2 {
        return Err(Error::Protocol("truncated acl op/perm".into()));
    }
    let operation = src.get_u8();
    let permission = src.get_u8();
    Ok(AclBinding {
        principal,
        resource_type,
        resource,
        operation,
        permission,
    })
}

fn get_string(src: &mut impl Buf) -> Result<String> {
    if src.remaining() < 2 {
        return Err(Error::Protocol("truncated string length".into()));
    }
    let len = src.get_u16_le() as usize;
    if src.remaining() < len {
        return Err(Error::Protocol("truncated string body".into()));
    }
    let mut buf = vec![0u8; len];
    src.copy_to_slice(&mut buf);
    String::from_utf8(buf).map_err(|e| Error::Protocol(format!("invalid utf-8: {e}")))
}

fn put_bytes(dst: &mut BytesMut, b: &[u8]) {
    dst.put_u32_le(b.len() as u32);
    dst.extend_from_slice(b);
}

fn put_membership_broker(dst: &mut BytesMut, b: &MembershipBroker) -> Result<()> {
    dst.put_u32_le(b.id);
    put_string(dst, &b.host)?;
    dst.put_u16_le(b.port);
    match &b.rack {
        Some(r) => {
            dst.put_u8(1);
            put_string(dst, r)?;
        }
        None => dst.put_u8(0),
    }
    Ok(())
}

fn get_membership_broker(src: &mut impl Buf) -> Result<MembershipBroker> {
    if src.remaining() < 4 {
        return Err(Error::Protocol("truncated membership broker id".into()));
    }
    let id = src.get_u32_le();
    let host = get_string(src)?;
    if src.remaining() < 2 + 1 {
        return Err(Error::Protocol(
            "truncated membership broker port/rack".into(),
        ));
    }
    let port = src.get_u16_le();
    let has_rack = src.get_u8();
    let rack = if has_rack != 0 {
        Some(get_string(src)?)
    } else {
        None
    };
    Ok(MembershipBroker {
        id,
        host,
        port,
        rack,
    })
}

fn put_optional_bytes(dst: &mut BytesMut, b: Option<&[u8]>) {
    match b {
        None => dst.put_u32_le(u32::MAX),
        Some(v) => put_bytes(dst, v),
    }
}

fn get_bytes(src: &mut impl Buf) -> Result<Bytes> {
    if src.remaining() < 4 {
        return Err(Error::Protocol("truncated bytes length".into()));
    }
    let len = src.get_u32_le() as usize;
    if len == u32::MAX as usize {
        return Err(Error::Protocol(
            "unexpected optional null in required bytes".into(),
        ));
    }
    if src.remaining() < len {
        return Err(Error::Protocol("truncated bytes body".into()));
    }
    let mut buf = vec![0u8; len];
    src.copy_to_slice(&mut buf);
    Ok(Bytes::from(buf))
}

fn get_optional_bytes(src: &mut impl Buf) -> Result<Option<Bytes>> {
    if src.remaining() < 4 {
        return Err(Error::Protocol("truncated optional bytes length".into()));
    }
    let len = src.get_u32_le();
    if len == u32::MAX {
        return Ok(None);
    }
    let len = len as usize;
    if src.remaining() < len {
        return Err(Error::Protocol("truncated optional bytes body".into()));
    }
    let mut buf = vec![0u8; len];
    src.copy_to_slice(&mut buf);
    Ok(Some(Bytes::from(buf)))
}

fn put_headers(dst: &mut BytesMut, headers: &[(String, Bytes)]) -> Result<()> {
    dst.put_u32_le(headers.len() as u32);
    for (name, value) in headers {
        put_string(dst, name)?;
        put_bytes(dst, value);
    }
    Ok(())
}

fn get_headers(src: &mut impl Buf) -> Result<Vec<(String, Bytes)>> {
    if src.remaining() < 4 {
        return Err(Error::Protocol("truncated header count".into()));
    }
    let count = src.get_u32_le() as usize;
    let mut headers = Vec::with_capacity(count);
    for _ in 0..count {
        let name = get_string(src)?;
        let value = get_bytes(src)?;
        headers.push((name, value));
    }
    Ok(headers)
}

/// Encode ClusterState-style topics list (shared with AssignmentConsensusNote).
fn put_cluster_topics(dst: &mut BytesMut, topics: &[ClusterTopicState]) -> Result<()> {
    dst.put_u32_le(topics.len() as u32);
    for t in topics {
        put_string(dst, &t.name)?;
        dst.put_u32_le(t.topic_id);
        dst.put_u32_le(t.partitions.len() as u32);
        for p in &t.partitions {
            dst.put_u32_le(p.partition_id);
            dst.put_u32_le(p.leader);
            dst.put_u32_le(p.leader_epoch);
            dst.put_u32_le(p.replicas.len() as u32);
            for r in &p.replicas {
                dst.put_u32_le(*r);
            }
            dst.put_u32_le(p.isr.len() as u32);
            for r in &p.isr {
                dst.put_u32_le(*r);
            }
        }
    }
    Ok(())
}

/// Decode ClusterState-style topics list (shared with AssignmentConsensusNote).
fn get_cluster_topics(src: &mut impl Buf) -> Result<Vec<ClusterTopicState>> {
    if src.remaining() < 4 {
        return Err(Error::Protocol("truncated cluster topics count".into()));
    }
    let topic_count = src.get_u32_le() as usize;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = get_string(src)?;
        if src.remaining() < 4 + 4 {
            return Err(Error::Protocol("truncated cluster topic header".into()));
        }
        let topic_id = src.get_u32_le();
        let part_count = src.get_u32_le() as usize;
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            if src.remaining() < 4 + 4 + 4 + 4 {
                return Err(Error::Protocol("truncated cluster partition header".into()));
            }
            let partition_id = src.get_u32_le();
            let leader = src.get_u32_le();
            let leader_epoch = src.get_u32_le();
            let replica_count = src.get_u32_le() as usize;
            if src.remaining() < replica_count.saturating_mul(4).saturating_add(4) {
                return Err(Error::Protocol("truncated cluster replicas".into()));
            }
            let mut replicas = Vec::with_capacity(replica_count);
            for _ in 0..replica_count {
                replicas.push(src.get_u32_le());
            }
            let isr_count = src.get_u32_le() as usize;
            if src.remaining() < isr_count.saturating_mul(4) {
                return Err(Error::Protocol("truncated cluster isr".into()));
            }
            let mut isr = Vec::with_capacity(isr_count);
            for _ in 0..isr_count {
                isr.push(src.get_u32_le());
            }
            partitions.push(ClusterPartitionState {
                partition_id,
                leader,
                leader_epoch,
                replicas,
                isr,
            });
        }
        topics.push(ClusterTopicState {
            name,
            topic_id,
            partitions,
        });
    }
    Ok(topics)
}

fn finish_payload(dst: BytesMut) -> Result<Bytes> {
    if dst.len() > MAX_PAYLOAD {
        return Err(Error::Protocol(format!(
            "payload too large: {} > {MAX_PAYLOAD}",
            dst.len()
        )));
    }
    Ok(dst.freeze())
}

/// Encode a request body to little-endian payload bytes.
pub fn encode_request(req: &Request) -> Result<Bytes> {
    let mut dst = BytesMut::new();
    match req {
        Request::Produce {
            topic,
            partition,
            acks,
            messages,
            producer_id,
            producer_epoch,
            base_sequence,
        } => {
            put_string(&mut dst, topic)?;
            dst.put_i32_le(*partition);
            dst.put_u8(*acks);
            dst.put_u32_le(messages.len() as u32);
            for m in messages {
                put_optional_bytes(&mut dst, m.key.as_deref());
                put_bytes(&mut dst, &m.value);
                dst.put_i64_le(m.timestamp_ms);
                put_headers(&mut dst, &m.headers)?;
            }
            // Phase 10 idempotent trailer (always written by current encoders).
            dst.put_u64_le(*producer_id);
            dst.put_u16_le(*producer_epoch);
            dst.put_i32_le(*base_sequence);
        }
        Request::Fetch {
            topic,
            partition,
            from_offset,
            max_messages,
            max_bytes,
            max_wait_ms,
            group_id,
            member_id,
        } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u64_le(*from_offset);
            dst.put_u32_le(*max_messages);
            dst.put_u32_le(*max_bytes);
            dst.put_u32_le(*max_wait_ms);
            put_string(&mut dst, group_id)?;
            put_string(&mut dst, member_id)?;
        }
        Request::CreateTopic {
            name,
            partitions,
            configs,
        } => {
            put_string(&mut dst, name)?;
            dst.put_u32_le(*partitions);
            // Phase 13 config trailer (always written by current encoders).
            dst.put_u32_le(configs.len() as u32);
            for (k, v) in configs {
                put_string(&mut dst, k)?;
                put_string(&mut dst, v)?;
            }
        }
        Request::Metadata { topics } => {
            dst.put_u32_le(topics.len() as u32);
            for t in topics {
                put_string(&mut dst, t)?;
            }
        }
        Request::DeleteTopic { name } => {
            put_string(&mut dst, name)?;
        }
        Request::OffsetCommit {
            group_id,
            member_id,
            generation,
            entries,
        } => {
            put_string(&mut dst, group_id)?;
            put_string(&mut dst, member_id)?;
            dst.put_u32_le(*generation);
            dst.put_u32_le(entries.len() as u32);
            for e in entries {
                put_string(&mut dst, &e.topic)?;
                dst.put_u32_le(e.partition);
                dst.put_u64_le(e.offset);
                put_string(&mut dst, &e.metadata)?;
            }
        }
        Request::OffsetFetch { group_id, entries } => {
            put_string(&mut dst, group_id)?;
            dst.put_u32_le(entries.len() as u32);
            for e in entries {
                put_string(&mut dst, &e.topic)?;
                dst.put_u32_le(e.partition);
            }
        }
        Request::JoinGroup {
            group_id,
            member_id,
            session_timeout_ms,
            topics,
            group_instance_id,
            rebalance_timeout_ms,
        } => {
            put_string(&mut dst, group_id)?;
            put_string(&mut dst, member_id)?;
            dst.put_u32_le(*session_timeout_ms);
            dst.put_u32_le(topics.len() as u32);
            for t in topics {
                put_string(&mut dst, t)?;
            }
            // Phase 12 trailing field (always written by current encoders).
            put_string(&mut dst, group_instance_id)?;
            // v0.231 optional trailer (always written by current encoders).
            dst.put_u32_le(*rebalance_timeout_ms);
        }
        Request::Heartbeat {
            group_id,
            member_id,
            generation,
        } => {
            put_string(&mut dst, group_id)?;
            put_string(&mut dst, member_id)?;
            dst.put_u32_le(*generation);
        }
        Request::LeaveGroup {
            group_id,
            member_id,
        } => {
            put_string(&mut dst, group_id)?;
            put_string(&mut dst, member_id)?;
        }
        Request::ReplicaFetch {
            topic,
            partition,
            from_offset,
            max_bytes,
            replica_id,
        } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u64_le(*from_offset);
            dst.put_u32_le(*max_bytes);
            dst.put_u32_le(*replica_id);
        }
        Request::HeartbeatBroker {
            broker_id,
            controller_id_known,
            generation,
            applied_config_generation,
            applied_acl_generation,
            applied_journal_generation,
        } => {
            dst.put_u32_le(*broker_id);
            dst.put_u32_le(*controller_id_known);
            dst.put_u32_le(*generation);
            // Phase 117/131: applied admin + journal gens (backward-compatible trailer).
            dst.put_u64_le(*applied_config_generation);
            dst.put_u64_le(*applied_acl_generation);
            dst.put_u64_le(*applied_journal_generation);
        }
        Request::ClusterState { known_generation } => {
            dst.put_u32_le(*known_generation);
        }
        Request::Auth { token } => {
            put_string(&mut dst, token)?;
        }
        Request::InitProducerId { transactional_id } => {
            // Phase 18 trailer (always written); legacy empty body still decodes.
            put_string(&mut dst, transactional_id)?;
        }
        Request::BeginTxn {
            producer_id,
            producer_epoch,
        } => {
            dst.put_u64_le(*producer_id);
            dst.put_u16_le(*producer_epoch);
        }
        Request::EndTxn {
            producer_id,
            producer_epoch,
            committed,
            offsets,
        } => {
            dst.put_u64_le(*producer_id);
            dst.put_u16_le(*producer_epoch);
            dst.put_u8(if *committed { 1 } else { 0 });
            dst.put_u32_le(offsets.len() as u32);
            for o in offsets {
                put_string(&mut dst, &o.group_id)?;
                put_string(&mut dst, &o.topic)?;
                dst.put_u32_le(o.partition);
                dst.put_u64_le(o.offset);
                put_string(&mut dst, &o.metadata)?;
            }
        }
        Request::DescribeGroup { group_id } => {
            put_string(&mut dst, group_id)?;
        }
        Request::ListGroups => {
            // Empty payload.
        }
        Request::DeleteOffsets { group_id, entries } => {
            put_string(&mut dst, group_id)?;
            dst.put_u32_le(entries.len() as u32);
            for e in entries {
                put_string(&mut dst, &e.topic)?;
                dst.put_u32_le(e.partition);
            }
        }
        Request::DescribeConfigs { topic } => {
            put_string(&mut dst, topic)?;
        }
        Request::AlterConfigs { topic, configs } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(configs.len() as u32);
            for (k, v) in configs {
                put_string(&mut dst, k)?;
                put_string(&mut dst, v)?;
            }
        }
        Request::DeleteRecords {
            topic,
            partition,
            before_offset,
            wait_majority,
        } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u64_le(*before_offset);
            dst.put_u8(*wait_majority);
        }
        Request::CreatePartitions { topic, total_count } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*total_count);
        }
        Request::ListOffsets { topic, partitions } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(partitions.len() as u32);
            for p in partitions {
                dst.put_u32_le(*p);
            }
        }
        Request::CreateAcls { entries } => {
            dst.put_u32_le(entries.len() as u32);
            for e in entries {
                put_acl_binding(&mut dst, e)?;
            }
        }
        Request::DeleteAcls { entries } => {
            dst.put_u32_le(entries.len() as u32);
            for e in entries {
                put_acl_binding(&mut dst, e)?;
            }
        }
        Request::ListAcls {
            principal,
            resource_type,
            resource,
        } => {
            put_string(&mut dst, principal)?;
            dst.put_u8(*resource_type);
            put_string(&mut dst, resource)?;
        }
        Request::ScramFirst {
            username,
            client_nonce,
        } => {
            put_string(&mut dst, username)?;
            put_string(&mut dst, client_nonce)?;
        }
        Request::ScramFinal {
            username,
            combined_nonce,
            client_proof,
        } => {
            put_string(&mut dst, username)?;
            put_string(&mut dst, combined_nonce)?;
            put_bytes(&mut dst, client_proof);
        }
        Request::CreateScramUser {
            username,
            password,
            iterations,
        } => {
            put_string(&mut dst, username)?;
            put_string(&mut dst, password)?;
            dst.put_u32_le(*iterations);
        }
        Request::DeleteScramUser { username } => {
            put_string(&mut dst, username)?;
        }
        Request::ListScramUsers => {
            // Empty payload.
        }
        Request::ReplicaDeleteRecords {
            topic,
            partition,
            before_offset,
            leader_epoch,
        } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u64_le(*before_offset);
            dst.put_i32_le(*leader_epoch);
        }
        Request::ClusterBrokerConfig {
            generation,
            entries,
        } => {
            dst.put_u64_le(*generation);
            if entries.len() > u16::MAX as usize {
                return Err(Error::Protocol(format!(
                    "cluster broker config entry count too large: {}",
                    entries.len()
                )));
            }
            dst.put_u16_le(entries.len() as u16);
            for (k, v) in entries {
                put_string(&mut dst, k)?;
                put_string(&mut dst, v)?;
            }
        }
        Request::ClusterAclSnapshot {
            generation,
            snapshot,
        } => {
            dst.put_u64_le(*generation);
            put_bytes(&mut dst, snapshot);
        }
        Request::TxnParticipantOpen {
            transactional_id,
            producer_id,
            producer_epoch,
            enable_2pc,
            coordinator_node_id,
            install_open,
        } => {
            put_string(&mut dst, transactional_id)?;
            dst.put_u64_le(*producer_id);
            dst.put_u16_le(*producer_epoch);
            dst.put_u8(if *enable_2pc { 1 } else { 0 });
            // Phase 120 trailer (always written by current brokers).
            dst.put_u32_le(*coordinator_node_id);
            dst.put_u8(if *install_open { 1 } else { 0 });
        }
        Request::TxnParticipantPrepare {
            transactional_id,
            producer_id,
            producer_epoch,
            commit,
        } => {
            put_string(&mut dst, transactional_id)?;
            dst.put_u64_le(*producer_id);
            dst.put_u16_le(*producer_epoch);
            dst.put_u8(if *commit { 1 } else { 0 });
        }
        Request::TxnParticipantComplete {
            transactional_id,
            producer_id,
            producer_epoch,
            commit,
        } => {
            put_string(&mut dst, transactional_id)?;
            dst.put_u64_le(*producer_id);
            dst.put_u16_le(*producer_epoch);
            dst.put_u8(if *commit { 1 } else { 0 });
        }
        Request::KafkaFetchForward {
            api_version,
            principal,
            body,
        } => {
            dst.put_i16_le(*api_version);
            put_string(&mut dst, principal)?;
            put_bytes(&mut dst, body);
        }
        Request::KafkaTxnForward {
            api_key,
            api_version,
            principal,
            body,
        } => {
            dst.put_i16_le(*api_key);
            dst.put_i16_le(*api_version);
            put_string(&mut dst, principal)?;
            put_bytes(&mut dst, body);
        }
        Request::TruncateJournalNote {
            topic,
            partition,
            before_offset,
            leader_epoch,
        } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u64_le(*before_offset);
            dst.put_i32_le(*leader_epoch);
        }
        Request::TruncateJournalPush {
            generation,
            snapshot,
        } => {
            dst.put_u64_le(*generation);
            put_bytes(&mut dst, snapshot);
        }
        Request::FetchSessionMirrorPut {
            session_id,
            snapshot,
        } => {
            dst.put_i32_le(*session_id);
            put_bytes(&mut dst, snapshot);
        }
        Request::FetchSessionMirrorDelete { session_id } => {
            dst.put_i32_le(*session_id);
        }
        Request::IsrUpdate {
            topic,
            partition,
            leader_id,
            leader_epoch,
            isr,
            generation_hint,
        } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u32_le(*leader_id);
            dst.put_u32_le(*leader_epoch);
            dst.put_u32_le(isr.len() as u32);
            for id in isr {
                dst.put_u32_le(*id);
            }
            dst.put_u32_le(*generation_hint);
        }
        Request::AssignmentConsensusNote {
            generation,
            controller_id,
            topics,
        } => {
            dst.put_u32_le(*generation);
            dst.put_u32_le(*controller_id);
            put_cluster_topics(&mut dst, topics)?;
        }
        Request::MetadataRaftAppend {
            leader_id,
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } => {
            dst.put_u32_le(*leader_id);
            dst.put_u64_le(*term);
            dst.put_u64_le(*prev_log_index);
            dst.put_u64_le(*prev_log_term);
            dst.put_u32_le(entries.len() as u32);
            for e in entries {
                dst.put_u64_le(e.term);
                dst.put_u64_le(e.index);
                dst.put_u8(e.command_kind);
                dst.put_u32_le(e.generation);
                put_cluster_topics(&mut dst, &e.topics)?;
            }
            dst.put_u64_le(*leader_commit);
        }
        Request::MembershipPut {
            generation,
            brokers,
        } => {
            dst.put_u64_le(*generation);
            dst.put_u32_le(brokers.len() as u32);
            for b in brokers {
                put_membership_broker(&mut dst, b)?;
            }
        }
        Request::AddBroker {
            id,
            host,
            port,
            rack,
        } => {
            put_membership_broker(
                &mut dst,
                &MembershipBroker {
                    id: *id,
                    host: host.clone(),
                    port: *port,
                    rack: rack.clone(),
                },
            )?;
        }
        Request::RemoveBroker { id } => {
            dst.put_u32_le(*id);
        }
        Request::ListMembers => {}
        Request::OpenraftAppend { payload }
        | Request::OpenraftVote { payload }
        | Request::OpenraftInstallSnapshot { payload } => {
            put_bytes(&mut dst, payload);
        }
        Request::ReassignPartitions {
            topic,
            partition,
            replicas,
        } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u32_le(replicas.len() as u32);
            for id in replicas {
                dst.put_u32_le(*id);
            }
        }
        Request::SyncGroup {
            group_id,
            member_id,
            generation,
            assignment_bytes,
        } => {
            put_string(&mut dst, group_id)?;
            put_string(&mut dst, member_id)?;
            dst.put_u32_le(*generation);
            put_bytes(&mut dst, assignment_bytes);
        }
    }
    finish_payload(dst)
}

/// Decode a request body given its opcode.
pub fn decode_request(opcode: u16, payload: &[u8]) -> Result<Request> {
    if payload.len() > MAX_PAYLOAD {
        return Err(Error::Protocol(format!(
            "payload too large: {} > {MAX_PAYLOAD}",
            payload.len()
        )));
    }
    let op = RequestOpcode::from_u16(opcode)
        .ok_or_else(|| Error::Protocol(format!("unknown request opcode {opcode}")))?;
    let mut src = payload;
    match op {
        RequestOpcode::Produce => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 1 + 4 {
                return Err(Error::Protocol("truncated produce header".into()));
            }
            let partition = src.get_i32_le();
            let acks = src.get_u8();
            let message_count = src.get_u32_le() as usize;
            let mut messages = Vec::with_capacity(message_count);
            for _ in 0..message_count {
                let key = get_optional_bytes(&mut src)?;
                let value = get_bytes(&mut src)?;
                if src.remaining() < 8 {
                    return Err(Error::Protocol("truncated produce timestamp".into()));
                }
                let timestamp_ms = src.get_i64_le();
                let headers = get_headers(&mut src)?;
                messages.push(ProduceMessage {
                    key,
                    value,
                    timestamp_ms,
                    headers,
                });
            }
            // Optional Phase 10 trailer: producer_id(u64) + epoch(u16) + base_sequence(i32).
            let (producer_id, producer_epoch, base_sequence) = if src.remaining() >= 8 + 2 + 4 {
                (src.get_u64_le(), src.get_u16_le(), src.get_i32_le())
            } else {
                (0, 0, -1)
            };
            Ok(Request::Produce {
                topic,
                partition,
                acks,
                messages,
                producer_id,
                producer_epoch,
                base_sequence,
            })
        }
        RequestOpcode::Fetch => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 8 + 4 + 4 + 4 {
                return Err(Error::Protocol("truncated fetch request".into()));
            }
            let partition = src.get_u32_le();
            let from_offset = src.get_u64_le();
            let max_messages = src.get_u32_le();
            let max_bytes = src.get_u32_le();
            let max_wait_ms = src.get_u32_le();
            // v0.234 group+member trailer; legacy Fetch omits it.
            let (group_id, member_id) = if src.has_remaining() {
                (get_string(&mut src)?, get_string(&mut src)?)
            } else {
                (String::new(), String::new())
            };
            Ok(Request::Fetch {
                topic,
                partition,
                from_offset,
                max_messages,
                max_bytes,
                max_wait_ms,
                group_id,
                member_id,
            })
        }
        RequestOpcode::CreateTopic => {
            let name = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated create topic partitions".into()));
            }
            let partitions = src.get_u32_le();
            // Phase 13 trailer; legacy payloads omit configs.
            let configs = if src.remaining() >= 4 {
                let n = src.get_u32_le() as usize;
                let mut configs = Vec::with_capacity(n);
                for _ in 0..n {
                    let k = get_string(&mut src)?;
                    let v = get_string(&mut src)?;
                    configs.push((k, v));
                }
                configs
            } else {
                Vec::new()
            };
            Ok(Request::CreateTopic {
                name,
                partitions,
                configs,
            })
        }
        RequestOpcode::Metadata => {
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated metadata topic count".into()));
            }
            let count = src.get_u32_le() as usize;
            let mut topics = Vec::with_capacity(count);
            for _ in 0..count {
                topics.push(get_string(&mut src)?);
            }
            Ok(Request::Metadata { topics })
        }
        RequestOpcode::DeleteTopic => Ok(Request::DeleteTopic {
            name: get_string(&mut src)?,
        }),
        RequestOpcode::OffsetCommit => {
            let group_id = get_string(&mut src)?;
            let member_id = get_string(&mut src)?;
            if src.remaining() < 4 + 4 {
                return Err(Error::Protocol("truncated offset commit header".into()));
            }
            let generation = src.get_u32_le();
            let entry_count = src.get_u32_le() as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let topic = get_string(&mut src)?;
                if src.remaining() < 4 + 8 {
                    return Err(Error::Protocol("truncated offset commit entry".into()));
                }
                let partition = src.get_u32_le();
                let offset = src.get_u64_le();
                let metadata = get_string(&mut src)?;
                entries.push(OffsetCommitEntry {
                    topic,
                    partition,
                    offset,
                    metadata,
                });
            }
            Ok(Request::OffsetCommit {
                group_id,
                member_id,
                generation,
                entries,
            })
        }
        RequestOpcode::OffsetFetch => {
            let group_id = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated offset fetch entry count".into()));
            }
            let entry_count = src.get_u32_le() as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let topic = get_string(&mut src)?;
                if src.remaining() < 4 {
                    return Err(Error::Protocol("truncated offset fetch partition".into()));
                }
                entries.push(OffsetEntry {
                    topic,
                    partition: src.get_u32_le(),
                });
            }
            Ok(Request::OffsetFetch { group_id, entries })
        }
        RequestOpcode::JoinGroup => {
            let group_id = get_string(&mut src)?;
            let member_id = get_string(&mut src)?;
            if src.remaining() < 4 + 4 {
                return Err(Error::Protocol("truncated join group header".into()));
            }
            let session_timeout_ms = src.get_u32_le();
            let topic_count = src.get_u32_le() as usize;
            let mut topics = Vec::with_capacity(topic_count);
            for _ in 0..topic_count {
                topics.push(get_string(&mut src)?);
            }
            // Phase 12 trailing field; legacy payloads omit it.
            let group_instance_id = if src.remaining() > 0 {
                get_string(&mut src)?
            } else {
                String::new()
            };
            // v0.231 trailer after instance id; omitted / short leftover → 0.
            let rebalance_timeout_ms = if src.remaining() >= 4 {
                src.get_u32_le()
            } else {
                0
            };
            Ok(Request::JoinGroup {
                group_id,
                member_id,
                session_timeout_ms,
                topics,
                group_instance_id,
                rebalance_timeout_ms,
            })
        }
        RequestOpcode::Heartbeat => {
            let group_id = get_string(&mut src)?;
            let member_id = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated heartbeat generation".into()));
            }
            Ok(Request::Heartbeat {
                group_id,
                member_id,
                generation: src.get_u32_le(),
            })
        }
        RequestOpcode::LeaveGroup => {
            let group_id = get_string(&mut src)?;
            let member_id = get_string(&mut src)?;
            Ok(Request::LeaveGroup {
                group_id,
                member_id,
            })
        }
        RequestOpcode::ReplicaFetch => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 8 + 4 + 4 {
                return Err(Error::Protocol("truncated replica fetch".into()));
            }
            Ok(Request::ReplicaFetch {
                topic,
                partition: src.get_u32_le(),
                from_offset: src.get_u64_le(),
                max_bytes: src.get_u32_le(),
                replica_id: src.get_u32_le(),
            })
        }
        RequestOpcode::HeartbeatBroker => {
            if src.remaining() < 4 + 4 + 4 {
                return Err(Error::Protocol("truncated heartbeat broker".into()));
            }
            let broker_id = src.get_u32_le();
            let controller_id_known = src.get_u32_le();
            let generation = src.get_u32_le();
            // Phase 117/131 trailer (optional for older peers).
            // 24 bytes: config + acl + journal; 16 bytes: config + acl only.
            let (applied_config_generation, applied_acl_generation, applied_journal_generation) =
                if src.remaining() >= 24 {
                    (src.get_u64_le(), src.get_u64_le(), src.get_u64_le())
                } else if src.remaining() >= 16 {
                    (src.get_u64_le(), src.get_u64_le(), 0)
                } else {
                    (0, 0, 0)
                };
            Ok(Request::HeartbeatBroker {
                broker_id,
                controller_id_known,
                generation,
                applied_config_generation,
                applied_acl_generation,
                applied_journal_generation,
            })
        }
        RequestOpcode::ClusterState => {
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated cluster state request".into()));
            }
            Ok(Request::ClusterState {
                known_generation: src.get_u32_le(),
            })
        }
        RequestOpcode::Auth => Ok(Request::Auth {
            token: get_string(&mut src)?,
        }),
        RequestOpcode::InitProducerId => {
            // Phase 18 trailing transactional_id; legacy empty body → empty id.
            let transactional_id = if src.remaining() > 0 {
                get_string(&mut src)?
            } else {
                String::new()
            };
            Ok(Request::InitProducerId { transactional_id })
        }
        RequestOpcode::BeginTxn => {
            if src.remaining() < 8 + 2 {
                return Err(Error::Protocol("truncated begin txn".into()));
            }
            Ok(Request::BeginTxn {
                producer_id: src.get_u64_le(),
                producer_epoch: src.get_u16_le(),
            })
        }
        RequestOpcode::EndTxn => {
            if src.remaining() < 8 + 2 + 1 + 4 {
                return Err(Error::Protocol("truncated end txn".into()));
            }
            let producer_id = src.get_u64_le();
            let producer_epoch = src.get_u16_le();
            let committed = src.get_u8() != 0;
            let offset_count = src.get_u32_le() as usize;
            let mut offsets = Vec::with_capacity(offset_count);
            for _ in 0..offset_count {
                let group_id = get_string(&mut src)?;
                let topic = get_string(&mut src)?;
                if src.remaining() < 4 + 8 {
                    return Err(Error::Protocol("truncated end txn offset entry".into()));
                }
                let partition = src.get_u32_le();
                let offset = src.get_u64_le();
                let metadata = get_string(&mut src)?;
                offsets.push(TxnOffsetCommit {
                    group_id,
                    topic,
                    partition,
                    offset,
                    metadata,
                });
            }
            Ok(Request::EndTxn {
                producer_id,
                producer_epoch,
                committed,
                offsets,
            })
        }
        RequestOpcode::DescribeGroup => Ok(Request::DescribeGroup {
            group_id: get_string(&mut src)?,
        }),
        RequestOpcode::ListGroups => Ok(Request::ListGroups),
        RequestOpcode::DeleteOffsets => {
            let group_id = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated delete offsets count".into()));
            }
            let entry_count = src.get_u32_le() as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let topic = get_string(&mut src)?;
                if src.remaining() < 4 {
                    return Err(Error::Protocol("truncated delete offsets partition".into()));
                }
                entries.push(OffsetEntry {
                    topic,
                    partition: src.get_u32_le(),
                });
            }
            Ok(Request::DeleteOffsets { group_id, entries })
        }
        RequestOpcode::DescribeConfigs => Ok(Request::DescribeConfigs {
            topic: get_string(&mut src)?,
        }),
        RequestOpcode::AlterConfigs => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated alter configs count".into()));
            }
            let n = src.get_u32_le() as usize;
            let mut configs = Vec::with_capacity(n);
            for _ in 0..n {
                let k = get_string(&mut src)?;
                let v = get_string(&mut src)?;
                configs.push((k, v));
            }
            Ok(Request::AlterConfigs { topic, configs })
        }
        RequestOpcode::DeleteRecords => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 8 {
                return Err(Error::Protocol("truncated delete records".into()));
            }
            let partition = src.get_u32_le();
            let before_offset = src.get_u64_le();
            // Phase 137: optional wait_majority trailer (absent → 0).
            let wait_majority = if src.remaining() >= 1 {
                src.get_u8()
            } else {
                0
            };
            Ok(Request::DeleteRecords {
                topic,
                partition,
                before_offset,
                wait_majority,
            })
        }
        RequestOpcode::CreatePartitions => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated create partitions".into()));
            }
            let total_count = src.get_u32_le();
            Ok(Request::CreatePartitions { topic, total_count })
        }
        RequestOpcode::ListOffsets => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated list offsets count".into()));
            }
            let n = src.get_u32_le() as usize;
            let mut partitions = Vec::with_capacity(n);
            for _ in 0..n {
                if src.remaining() < 4 {
                    return Err(Error::Protocol("truncated list offsets partition".into()));
                }
                partitions.push(src.get_u32_le());
            }
            Ok(Request::ListOffsets { topic, partitions })
        }
        RequestOpcode::CreateAcls => {
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated create acls count".into()));
            }
            let n = src.get_u32_le() as usize;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                entries.push(get_acl_binding(&mut src)?);
            }
            Ok(Request::CreateAcls { entries })
        }
        RequestOpcode::DeleteAcls => {
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated delete acls count".into()));
            }
            let n = src.get_u32_le() as usize;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                entries.push(get_acl_binding(&mut src)?);
            }
            Ok(Request::DeleteAcls { entries })
        }
        RequestOpcode::ListAcls => {
            let principal = get_string(&mut src)?;
            if src.remaining() < 1 {
                return Err(Error::Protocol("truncated list acls resource_type".into()));
            }
            let resource_type = src.get_u8();
            let resource = get_string(&mut src)?;
            Ok(Request::ListAcls {
                principal,
                resource_type,
                resource,
            })
        }
        RequestOpcode::ScramFirst => {
            let username = get_string(&mut src)?;
            let client_nonce = get_string(&mut src)?;
            Ok(Request::ScramFirst {
                username,
                client_nonce,
            })
        }
        RequestOpcode::ScramFinal => {
            let username = get_string(&mut src)?;
            let combined_nonce = get_string(&mut src)?;
            let client_proof = get_bytes(&mut src)?;
            Ok(Request::ScramFinal {
                username,
                combined_nonce,
                client_proof,
            })
        }
        RequestOpcode::CreateScramUser => {
            let username = get_string(&mut src)?;
            let password = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol(
                    "truncated create scram user iterations".into(),
                ));
            }
            let iterations = src.get_u32_le();
            Ok(Request::CreateScramUser {
                username,
                password,
                iterations,
            })
        }
        RequestOpcode::DeleteScramUser => Ok(Request::DeleteScramUser {
            username: get_string(&mut src)?,
        }),
        RequestOpcode::ListScramUsers => Ok(Request::ListScramUsers),
        RequestOpcode::ReplicaDeleteRecords => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 8 + 4 {
                return Err(Error::Protocol("truncated replica delete records".into()));
            }
            Ok(Request::ReplicaDeleteRecords {
                topic,
                partition: src.get_u32_le(),
                before_offset: src.get_u64_le(),
                leader_epoch: src.get_i32_le(),
            })
        }
        RequestOpcode::ClusterBrokerConfig => {
            if src.remaining() < 8 + 2 {
                return Err(Error::Protocol(
                    "truncated cluster broker config header".into(),
                ));
            }
            let generation = src.get_u64_le();
            let n = src.get_u16_le() as usize;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                let k = get_string(&mut src)?;
                let v = get_string(&mut src)?;
                entries.push((k, v));
            }
            Ok(Request::ClusterBrokerConfig {
                generation,
                entries,
            })
        }
        RequestOpcode::ClusterAclSnapshot => {
            if src.remaining() < 8 {
                return Err(Error::Protocol(
                    "truncated cluster acl snapshot generation".into(),
                ));
            }
            let generation = src.get_u64_le();
            let snapshot = get_bytes(&mut src)?;
            Ok(Request::ClusterAclSnapshot {
                generation,
                snapshot,
            })
        }
        RequestOpcode::TxnParticipantOpen => {
            let transactional_id = get_string(&mut src)?;
            if src.remaining() < 8 + 2 + 1 {
                return Err(Error::Protocol("truncated txn participant open".into()));
            }
            let producer_id = src.get_u64_le();
            let producer_epoch = src.get_u16_le();
            let enable_2pc = src.get_u8() != 0;
            // Phase 120 trailer: coordinator_node_id u32 + install_open u8.
            // Legacy peers omit the trailer → coordinator unknown, install open.
            let (coordinator_node_id, install_open) = if src.remaining() >= 5 {
                let c = src.get_u32_le();
                let install = src.get_u8() != 0;
                (c, install)
            } else if src.remaining() >= 4 {
                (src.get_u32_le(), true)
            } else {
                (0, true)
            };
            Ok(Request::TxnParticipantOpen {
                transactional_id,
                producer_id,
                producer_epoch,
                enable_2pc,
                coordinator_node_id,
                install_open,
            })
        }
        RequestOpcode::TxnParticipantPrepare => {
            let transactional_id = get_string(&mut src)?;
            if src.remaining() < 8 + 2 + 1 {
                return Err(Error::Protocol("truncated txn participant prepare".into()));
            }
            let producer_id = src.get_u64_le();
            let producer_epoch = src.get_u16_le();
            let commit = src.get_u8() != 0;
            Ok(Request::TxnParticipantPrepare {
                transactional_id,
                producer_id,
                producer_epoch,
                commit,
            })
        }
        RequestOpcode::TxnParticipantComplete => {
            let transactional_id = get_string(&mut src)?;
            if src.remaining() < 8 + 2 + 1 {
                return Err(Error::Protocol("truncated txn participant complete".into()));
            }
            let producer_id = src.get_u64_le();
            let producer_epoch = src.get_u16_le();
            let commit = src.get_u8() != 0;
            Ok(Request::TxnParticipantComplete {
                transactional_id,
                producer_id,
                producer_epoch,
                commit,
            })
        }
        RequestOpcode::KafkaFetchForward => {
            if src.remaining() < 2 {
                return Err(Error::Protocol(
                    "truncated kafka fetch forward version".into(),
                ));
            }
            let api_version = src.get_i16_le();
            let principal = get_string(&mut src)?;
            let body = get_bytes(&mut src)?;
            Ok(Request::KafkaFetchForward {
                api_version,
                principal,
                body,
            })
        }
        RequestOpcode::KafkaTxnForward => {
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated kafka txn forward header".into()));
            }
            let api_key = src.get_i16_le();
            let api_version = src.get_i16_le();
            let principal = get_string(&mut src)?;
            let body = get_bytes(&mut src)?;
            Ok(Request::KafkaTxnForward {
                api_key,
                api_version,
                principal,
                body,
            })
        }
        RequestOpcode::TruncateJournalNote => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 8 + 4 {
                return Err(Error::Protocol("truncated truncate journal note".into()));
            }
            Ok(Request::TruncateJournalNote {
                topic,
                partition: src.get_u32_le(),
                before_offset: src.get_u64_le(),
                leader_epoch: src.get_i32_le(),
            })
        }
        RequestOpcode::TruncateJournalPush => {
            if src.remaining() < 8 {
                return Err(Error::Protocol(
                    "truncated truncate journal push generation".into(),
                ));
            }
            let generation = src.get_u64_le();
            let snapshot = get_bytes(&mut src)?;
            Ok(Request::TruncateJournalPush {
                generation,
                snapshot,
            })
        }
        RequestOpcode::FetchSessionMirrorPut => {
            if src.remaining() < 4 {
                return Err(Error::Protocol(
                    "truncated fetch session mirror put session_id".into(),
                ));
            }
            let session_id = src.get_i32_le();
            let snapshot = get_bytes(&mut src)?;
            Ok(Request::FetchSessionMirrorPut {
                session_id,
                snapshot,
            })
        }
        RequestOpcode::FetchSessionMirrorDelete => {
            if src.remaining() < 4 {
                return Err(Error::Protocol(
                    "truncated fetch session mirror delete session_id".into(),
                ));
            }
            Ok(Request::FetchSessionMirrorDelete {
                session_id: src.get_i32_le(),
            })
        }
        RequestOpcode::IsrUpdate => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 4 + 4 + 4 {
                return Err(Error::Protocol("truncated isr update header".into()));
            }
            let partition = src.get_u32_le();
            let leader_id = src.get_u32_le();
            let leader_epoch = src.get_u32_le();
            let isr_count = src.get_u32_le() as usize;
            if src.remaining() < isr_count.saturating_mul(4).saturating_add(4) {
                return Err(Error::Protocol(
                    "truncated isr update isr/generation".into(),
                ));
            }
            let mut isr = Vec::with_capacity(isr_count);
            for _ in 0..isr_count {
                isr.push(src.get_u32_le());
            }
            let generation_hint = src.get_u32_le();
            Ok(Request::IsrUpdate {
                topic,
                partition,
                leader_id,
                leader_epoch,
                isr,
                generation_hint,
            })
        }
        RequestOpcode::AssignmentConsensusNote => {
            if src.remaining() < 4 + 4 + 4 {
                return Err(Error::Protocol(
                    "truncated assignment consensus note header".into(),
                ));
            }
            let generation = src.get_u32_le();
            let controller_id = src.get_u32_le();
            let topics = get_cluster_topics(&mut src)?;
            Ok(Request::AssignmentConsensusNote {
                generation,
                controller_id,
                topics,
            })
        }
        RequestOpcode::MetadataRaftAppend => {
            // leader_id(4) + term(8) + prev_idx(8) + prev_term(8) + entries_len(4)
            if src.remaining() < 4 + 8 + 8 + 8 + 4 {
                return Err(Error::Protocol(
                    "truncated metadata raft append header".into(),
                ));
            }
            let leader_id = src.get_u32_le();
            let term = src.get_u64_le();
            let prev_log_index = src.get_u64_le();
            let prev_log_term = src.get_u64_le();
            let entry_count = src.get_u32_le() as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                // term(8) + index(8) + kind(1) + generation(4) + topics
                if src.remaining() < 8 + 8 + 1 + 4 {
                    return Err(Error::Protocol(
                        "truncated metadata raft log entry header".into(),
                    ));
                }
                let e_term = src.get_u64_le();
                let e_index = src.get_u64_le();
                let command_kind = src.get_u8();
                let generation = src.get_u32_le();
                let topics = get_cluster_topics(&mut src)?;
                entries.push(crate::request::MetadataRaftLogEntry {
                    term: e_term,
                    index: e_index,
                    command_kind,
                    generation,
                    topics,
                });
            }
            if src.remaining() < 8 {
                return Err(Error::Protocol(
                    "truncated metadata raft leader_commit".into(),
                ));
            }
            let leader_commit = src.get_u64_le();
            Ok(Request::MetadataRaftAppend {
                leader_id,
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            })
        }
        RequestOpcode::MembershipPut => {
            if src.remaining() < 8 + 4 {
                return Err(Error::Protocol("truncated membership put header".into()));
            }
            let generation = src.get_u64_le();
            let count = src.get_u32_le() as usize;
            let mut brokers = Vec::with_capacity(count);
            for _ in 0..count {
                brokers.push(get_membership_broker(&mut src)?);
            }
            Ok(Request::MembershipPut {
                generation,
                brokers,
            })
        }
        RequestOpcode::AddBroker => {
            let b = get_membership_broker(&mut src)?;
            Ok(Request::AddBroker {
                id: b.id,
                host: b.host,
                port: b.port,
                rack: b.rack,
            })
        }
        RequestOpcode::RemoveBroker => {
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated remove broker".into()));
            }
            Ok(Request::RemoveBroker {
                id: src.get_u32_le(),
            })
        }
        RequestOpcode::ListMembers => Ok(Request::ListMembers),
        RequestOpcode::OpenraftAppend => Ok(Request::OpenraftAppend {
            payload: get_bytes(&mut src)?,
        }),
        RequestOpcode::OpenraftVote => Ok(Request::OpenraftVote {
            payload: get_bytes(&mut src)?,
        }),
        RequestOpcode::OpenraftInstallSnapshot => Ok(Request::OpenraftInstallSnapshot {
            payload: get_bytes(&mut src)?,
        }),
        RequestOpcode::ReassignPartitions => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 4 {
                return Err(Error::Protocol(
                    "truncated reassign partitions header".into(),
                ));
            }
            let partition = src.get_u32_le();
            let count = src.get_u32_le() as usize;
            if src.remaining() < count.saturating_mul(4) {
                return Err(Error::Protocol(
                    "truncated reassign partitions replicas".into(),
                ));
            }
            let mut replicas = Vec::with_capacity(count);
            for _ in 0..count {
                replicas.push(src.get_u32_le());
            }
            Ok(Request::ReassignPartitions {
                topic,
                partition,
                replicas,
            })
        }
        RequestOpcode::SyncGroup => {
            let group_id = get_string(&mut src)?;
            let member_id = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated sync group generation".into()));
            }
            let generation = src.get_u32_le();
            let assignment_bytes = get_bytes(&mut src)?;
            Ok(Request::SyncGroup {
                group_id,
                member_id,
                generation,
                assignment_bytes,
            })
        }
    }
}

/// Encode a response body to little-endian payload bytes.
pub fn encode_response(resp: &Response) -> Result<Bytes> {
    let mut dst = BytesMut::new();
    match resp {
        Response::Produce {
            topic,
            partition,
            base_offset,
            count,
            error_code,
        } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u64_le(*base_offset);
            dst.put_u32_le(*count);
            dst.put_u16_le(*error_code);
        }
        Response::Fetch {
            topic,
            partition,
            high_watermark,
            error_code,
            records,
        } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u64_le(*high_watermark);
            dst.put_u16_le(*error_code);
            dst.put_u32_le(records.len() as u32);
            for r in records {
                dst.put_u64_le(r.offset);
                dst.put_i64_le(r.timestamp_ms);
                put_optional_bytes(&mut dst, r.key.as_deref());
                put_bytes(&mut dst, &r.value);
                put_headers(&mut dst, &r.headers)?;
            }
        }
        Response::CreateTopic {
            topic_id,
            name,
            partitions,
            error_code,
        } => {
            dst.put_u32_le(*topic_id);
            put_string(&mut dst, name)?;
            dst.put_u32_le(*partitions);
            dst.put_u16_le(*error_code);
        }
        Response::DeleteTopic { name, error_code } => {
            put_string(&mut dst, name)?;
            dst.put_u16_le(*error_code);
        }
        Response::Metadata {
            brokers,
            topics,
            controller_id,
        } => {
            dst.put_u32_le(brokers.len() as u32);
            for b in brokers {
                dst.put_u32_le(b.node_id);
                put_string(&mut dst, &b.host)?;
                dst.put_u16_le(b.port);
            }
            dst.put_u32_le(topics.len() as u32);
            for t in topics {
                put_string(&mut dst, &t.name)?;
                dst.put_u32_le(t.topic_id);
                dst.put_u16_le(t.error_code);
                dst.put_u32_le(t.partitions.len() as u32);
                for p in &t.partitions {
                    dst.put_u32_le(p.partition_id);
                    dst.put_u32_le(p.leader);
                    dst.put_u64_le(p.hwm);
                    dst.put_u32_le(p.replicas.len() as u32);
                    for r in &p.replicas {
                        dst.put_u32_le(*r);
                    }
                    dst.put_u32_le(p.isr.len() as u32);
                    for r in &p.isr {
                        dst.put_u32_le(*r);
                    }
                    dst.put_u32_le(p.leader_epoch);
                }
            }
            // v0.77 trailing controller_id (always written by current encoders).
            dst.put_u32_le(*controller_id);
        }
        Response::OffsetCommit { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::OffsetFetch {
            error_code,
            entries,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(entries.len() as u32);
            for e in entries {
                put_string(&mut dst, &e.topic)?;
                dst.put_u32_le(e.partition);
                dst.put_u64_le(e.offset);
                put_string(&mut dst, &e.metadata)?;
            }
        }
        Response::JoinGroup {
            error_code,
            generation,
            member_id,
            assignment,
            revoked,
            members,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(*generation);
            put_string(&mut dst, member_id)?;
            dst.put_u32_le(assignment.len() as u32);
            for a in assignment {
                put_string(&mut dst, &a.topic)?;
                dst.put_u32_le(a.partition);
            }
            // Phase 17 trailing revoked list (always written by current encoders).
            dst.put_u32_le(revoked.len() as u32);
            for a in revoked {
                put_string(&mut dst, &a.topic)?;
                dst.put_u32_le(a.partition);
            }
            // v0.211 trailing live member ids (always written by current encoders).
            dst.put_u32_le(members.len() as u32);
            for id in members {
                put_string(&mut dst, id)?;
            }
        }
        Response::Heartbeat { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::LeaveGroup { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::ReplicaFetch {
            error_code,
            topic,
            partition,
            high_watermark,
            leader_epoch,
            records,
        } => {
            dst.put_u16_le(*error_code);
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u64_le(*high_watermark);
            dst.put_u32_le(*leader_epoch);
            dst.put_u32_le(records.len() as u32);
            for r in records {
                dst.put_u64_le(r.offset);
                dst.put_i64_le(r.timestamp_ms);
                put_optional_bytes(&mut dst, r.key.as_deref());
                put_bytes(&mut dst, &r.value);
                put_headers(&mut dst, &r.headers)?;
            }
        }
        Response::HeartbeatBroker {
            error_code,
            controller_id,
            generation,
            alive_brokers,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(*controller_id);
            dst.put_u32_le(*generation);
            dst.put_u32_le(alive_brokers.len() as u32);
            for id in alive_brokers {
                dst.put_u32_le(*id);
            }
        }
        Response::ClusterState {
            error_code,
            generation,
            controller_id,
            topics,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(*generation);
            dst.put_u32_le(*controller_id);
            put_cluster_topics(&mut dst, topics)?;
        }
        Response::Auth { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::InitProducerId {
            producer_id,
            epoch,
            error_code,
        } => {
            dst.put_u64_le(*producer_id);
            dst.put_u16_le(*epoch);
            dst.put_u16_le(*error_code);
        }
        Response::BeginTxn { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::EndTxn {
            error_code,
            results,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(results.len() as u32);
            for r in results {
                put_string(&mut dst, &r.topic)?;
                dst.put_u32_le(r.partition);
                dst.put_u64_le(r.base_offset);
                dst.put_u32_le(r.count);
            }
        }
        Response::DescribeGroup {
            error_code,
            group_id,
            generation,
            members,
        } => {
            dst.put_u16_le(*error_code);
            put_string(&mut dst, group_id)?;
            dst.put_u32_le(*generation);
            dst.put_u32_le(members.len() as u32);
            for m in members {
                put_string(&mut dst, &m.member_id)?;
                dst.put_u32_le(m.topics.len() as u32);
                for t in &m.topics {
                    put_string(&mut dst, t)?;
                }
                dst.put_u32_le(m.assignment.len() as u32);
                for a in &m.assignment {
                    put_string(&mut dst, &a.topic)?;
                    dst.put_u32_le(a.partition);
                }
            }
        }
        Response::ListGroups { error_code, groups } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(groups.len() as u32);
            for g in groups {
                put_string(&mut dst, &g.group_id)?;
                dst.put_u8(g.state as u8);
                dst.put_u32_le(g.member_count);
                dst.put_u32_le(g.generation);
            }
        }
        Response::DeleteOffsets {
            error_code,
            deleted_count,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(*deleted_count);
        }
        Response::DescribeConfigs {
            error_code,
            topic,
            topic_id,
            partition_count,
            configs,
        } => {
            dst.put_u16_le(*error_code);
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*topic_id);
            dst.put_u32_le(*partition_count);
            dst.put_u32_le(configs.len() as u32);
            for (k, v) in configs {
                put_string(&mut dst, k)?;
                put_string(&mut dst, v)?;
            }
        }
        Response::AlterConfigs { error_code, topic } => {
            dst.put_u16_le(*error_code);
            put_string(&mut dst, topic)?;
        }
        Response::DeleteRecords {
            error_code,
            topic,
            partition,
            low_watermark,
        } => {
            dst.put_u16_le(*error_code);
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u64_le(*low_watermark);
        }
        Response::CreatePartitions {
            error_code,
            topic,
            partitions,
        } => {
            dst.put_u16_le(*error_code);
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partitions);
        }
        Response::ListOffsets {
            error_code,
            topic,
            entries,
        } => {
            dst.put_u16_le(*error_code);
            put_string(&mut dst, topic)?;
            dst.put_u32_le(entries.len() as u32);
            for e in entries {
                dst.put_u32_le(e.partition);
                dst.put_u64_le(e.earliest);
                dst.put_u64_le(e.latest);
            }
        }
        Response::CreateAcls { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::DeleteAcls {
            error_code,
            removed,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(*removed);
        }
        Response::ListAcls {
            error_code,
            entries,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(entries.len() as u32);
            for e in entries {
                put_acl_binding(&mut dst, e)?;
            }
        }
        Response::ScramFirst {
            error_code,
            combined_nonce,
            salt,
            iterations,
        } => {
            dst.put_u16_le(*error_code);
            put_string(&mut dst, combined_nonce)?;
            put_bytes(&mut dst, salt);
            dst.put_u32_le(*iterations);
        }
        Response::ScramFinal {
            error_code,
            server_signature,
        } => {
            dst.put_u16_le(*error_code);
            put_bytes(&mut dst, server_signature);
        }
        Response::CreateScramUser { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::DeleteScramUser { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::ListScramUsers {
            error_code,
            usernames,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(usernames.len() as u32);
            for u in usernames {
                put_string(&mut dst, u)?;
            }
        }
        Response::ReplicaDeleteRecords {
            error_code,
            low_watermark,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u64_le(*low_watermark);
        }
        Response::ClusterBrokerConfig {
            error_code,
            applied_generation,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u64_le(*applied_generation);
        }
        Response::ClusterAclSnapshot {
            error_code,
            applied_generation,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u64_le(*applied_generation);
        }
        Response::TxnParticipantOpen { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::TxnParticipantPrepare { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::TxnParticipantComplete { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::KafkaFetchForward { error_code, body } => {
            dst.put_u16_le(*error_code);
            put_bytes(&mut dst, body);
        }
        Response::KafkaTxnForward { error_code, body } => {
            dst.put_u16_le(*error_code);
            put_bytes(&mut dst, body);
        }
        Response::TruncateJournalNote {
            error_code,
            generation,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u64_le(*generation);
        }
        Response::TruncateJournalPush { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::FetchSessionMirrorPut { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::FetchSessionMirrorDelete { error_code } => {
            dst.put_u16_le(*error_code);
        }
        Response::IsrUpdate {
            error_code,
            generation,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(*generation);
        }
        Response::AssignmentConsensusNote {
            error_code,
            generation,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(*generation);
        }
        Response::MetadataRaftAppend {
            term,
            success,
            match_index,
        } => {
            dst.put_u64_le(*term);
            dst.put_u8(*success);
            dst.put_u64_le(*match_index);
        }
        Response::MembershipPut {
            error_code,
            applied_generation,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u64_le(*applied_generation);
        }
        Response::AddBroker {
            error_code,
            generation,
        }
        | Response::RemoveBroker {
            error_code,
            generation,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u64_le(*generation);
        }
        Response::ListMembers {
            error_code,
            generation,
            brokers,
            live,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u64_le(*generation);
            dst.put_u32_le(brokers.len() as u32);
            for b in brokers {
                put_membership_broker(&mut dst, b)?;
            }
            dst.put_u32_le(live.len() as u32);
            for id in live {
                dst.put_u32_le(*id);
            }
        }
        Response::OpenraftAppend { payload }
        | Response::OpenraftVote { payload }
        | Response::OpenraftInstallSnapshot { payload } => {
            put_bytes(&mut dst, payload);
        }
        Response::ReassignPartitions {
            error_code,
            generation,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(*generation);
        }
        Response::SyncGroup {
            error_code,
            assignment,
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(assignment.len() as u32);
            for a in assignment {
                put_string(&mut dst, &a.topic)?;
                dst.put_u32_le(a.partition);
            }
        }
        Response::Error { code, message } => {
            dst.put_u16_le(*code);
            put_string(&mut dst, message)?;
        }
    }
    finish_payload(dst)
}

/// Decode a response body given its opcode.
pub fn decode_response(opcode: u16, payload: &[u8]) -> Result<Response> {
    if payload.len() > MAX_PAYLOAD {
        return Err(Error::Protocol(format!(
            "payload too large: {} > {MAX_PAYLOAD}",
            payload.len()
        )));
    }
    let op = ResponseOpcode::from_u16(opcode)
        .ok_or_else(|| Error::Protocol(format!("unknown response opcode {opcode}")))?;
    let mut src = payload;
    match op {
        ResponseOpcode::Produce => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 8 + 4 + 2 {
                return Err(Error::Protocol("truncated produce response".into()));
            }
            Ok(Response::Produce {
                topic,
                partition: src.get_u32_le(),
                base_offset: src.get_u64_le(),
                count: src.get_u32_le(),
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::Fetch => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 8 + 2 + 4 {
                return Err(Error::Protocol("truncated fetch response header".into()));
            }
            let partition = src.get_u32_le();
            let high_watermark = src.get_u64_le();
            let error_code = src.get_u16_le();
            let record_count = src.get_u32_le() as usize;
            let mut records = Vec::with_capacity(record_count);
            for _ in 0..record_count {
                if src.remaining() < 8 + 8 {
                    return Err(Error::Protocol("truncated fetch record header".into()));
                }
                let offset = src.get_u64_le();
                let timestamp_ms = src.get_i64_le();
                let key = get_optional_bytes(&mut src)?;
                let value = get_bytes(&mut src)?;
                let headers = get_headers(&mut src)?;
                records.push(FetchRecord {
                    offset,
                    timestamp_ms,
                    key,
                    value,
                    headers,
                });
            }
            Ok(Response::Fetch {
                topic,
                partition,
                high_watermark,
                error_code,
                records,
            })
        }
        ResponseOpcode::CreateTopic => {
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated create topic id".into()));
            }
            let topic_id = src.get_u32_le();
            let name = get_string(&mut src)?;
            if src.remaining() < 4 + 2 {
                return Err(Error::Protocol("truncated create topic tail".into()));
            }
            Ok(Response::CreateTopic {
                topic_id,
                name,
                partitions: src.get_u32_le(),
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::DeleteTopic => {
            let name = get_string(&mut src)?;
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated delete topic error".into()));
            }
            Ok(Response::DeleteTopic {
                name,
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::Metadata => {
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated broker count".into()));
            }
            let broker_count = src.get_u32_le() as usize;
            let mut brokers = Vec::with_capacity(broker_count);
            for _ in 0..broker_count {
                if src.remaining() < 4 {
                    return Err(Error::Protocol("truncated broker node_id".into()));
                }
                let node_id = src.get_u32_le();
                let host = get_string(&mut src)?;
                if src.remaining() < 2 {
                    return Err(Error::Protocol("truncated broker port".into()));
                }
                let port = src.get_u16_le();
                brokers.push(BrokerInfo {
                    node_id,
                    host,
                    port,
                });
            }
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated topic count".into()));
            }
            let topic_count = src.get_u32_le() as usize;
            let mut topics = Vec::with_capacity(topic_count);
            for _ in 0..topic_count {
                let name = get_string(&mut src)?;
                if src.remaining() < 4 + 2 + 4 {
                    return Err(Error::Protocol("truncated topic meta header".into()));
                }
                let topic_id = src.get_u32_le();
                let error_code = src.get_u16_le();
                let partition_count = src.get_u32_le() as usize;
                let mut partitions = Vec::with_capacity(partition_count);
                for _ in 0..partition_count {
                    if src.remaining() < 4 + 4 + 8 + 4 {
                        return Err(Error::Protocol("truncated partition info".into()));
                    }
                    let partition_id = src.get_u32_le();
                    let leader = src.get_u32_le();
                    let hwm = src.get_u64_le();
                    let replica_count = src.get_u32_le() as usize;
                    let mut replicas = Vec::with_capacity(replica_count);
                    for _ in 0..replica_count {
                        if src.remaining() < 4 {
                            return Err(Error::Protocol("truncated replica id".into()));
                        }
                        replicas.push(src.get_u32_le());
                    }
                    if src.remaining() < 4 {
                        return Err(Error::Protocol("truncated isr count".into()));
                    }
                    let isr_count = src.get_u32_le() as usize;
                    let mut isr = Vec::with_capacity(isr_count);
                    for _ in 0..isr_count {
                        if src.remaining() < 4 {
                            return Err(Error::Protocol("truncated isr id".into()));
                        }
                        isr.push(src.get_u32_le());
                    }
                    if src.remaining() < 4 {
                        return Err(Error::Protocol("truncated leader_epoch".into()));
                    }
                    let leader_epoch = src.get_u32_le();
                    partitions.push(PartitionInfo {
                        partition_id,
                        leader,
                        hwm,
                        replicas,
                        isr,
                        leader_epoch,
                    });
                }
                topics.push(TopicInfo {
                    name,
                    topic_id,
                    error_code,
                    partitions,
                });
            }
            // v0.77 trailing controller_id; legacy payloads omit it → 0.
            let controller_id = if src.remaining() >= 4 {
                src.get_u32_le()
            } else {
                0
            };
            Ok(Response::Metadata {
                brokers,
                topics,
                controller_id,
            })
        }
        ResponseOpcode::OffsetCommit => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated offset commit error".into()));
            }
            Ok(Response::OffsetCommit {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::OffsetFetch => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol("truncated offset fetch header".into()));
            }
            let error_code = src.get_u16_le();
            let entry_count = src.get_u32_le() as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let topic = get_string(&mut src)?;
                if src.remaining() < 4 + 8 {
                    return Err(Error::Protocol("truncated offset fetch entry".into()));
                }
                let partition = src.get_u32_le();
                let offset = src.get_u64_le();
                let metadata = get_string(&mut src)?;
                entries.push(OffsetFetchEntry {
                    topic,
                    partition,
                    offset,
                    metadata,
                });
            }
            Ok(Response::OffsetFetch {
                error_code,
                entries,
            })
        }
        ResponseOpcode::JoinGroup => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol("truncated join group header".into()));
            }
            let error_code = src.get_u16_le();
            let generation = src.get_u32_le();
            let member_id = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol(
                    "truncated join group assignment count".into(),
                ));
            }
            let assignment_count = src.get_u32_le() as usize;
            let mut assignment = Vec::with_capacity(assignment_count);
            for _ in 0..assignment_count {
                let topic = get_string(&mut src)?;
                if src.remaining() < 4 {
                    return Err(Error::Protocol("truncated join group partition".into()));
                }
                assignment.push(Assignment {
                    topic,
                    partition: src.get_u32_le(),
                });
            }
            // Phase 17 trailing revoked list; legacy payloads omit it.
            let mut revoked = Vec::new();
            if src.remaining() >= 4 {
                let revoked_count = src.get_u32_le() as usize;
                revoked.reserve(revoked_count);
                for _ in 0..revoked_count {
                    let topic = get_string(&mut src)?;
                    if src.remaining() < 4 {
                        return Err(Error::Protocol(
                            "truncated join group revoked partition".into(),
                        ));
                    }
                    revoked.push(Assignment {
                        topic,
                        partition: src.get_u32_le(),
                    });
                }
            }
            // v0.211 trailing live member ids; legacy payloads omit it.
            let mut members = Vec::new();
            if src.remaining() >= 4 {
                let member_count = src.get_u32_le() as usize;
                members.reserve(member_count);
                for _ in 0..member_count {
                    members.push(get_string(&mut src)?);
                }
            }
            Ok(Response::JoinGroup {
                error_code,
                generation,
                member_id,
                assignment,
                revoked,
                members,
            })
        }
        ResponseOpcode::Heartbeat => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated heartbeat error".into()));
            }
            Ok(Response::Heartbeat {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::LeaveGroup => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated leave group error".into()));
            }
            Ok(Response::LeaveGroup {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::ReplicaFetch => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated replica fetch error".into()));
            }
            let error_code = src.get_u16_le();
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 8 + 4 + 4 {
                return Err(Error::Protocol("truncated replica fetch header".into()));
            }
            let partition = src.get_u32_le();
            let high_watermark = src.get_u64_le();
            let leader_epoch = src.get_u32_le();
            let record_count = src.get_u32_le() as usize;
            let mut records = Vec::with_capacity(record_count);
            for _ in 0..record_count {
                if src.remaining() < 8 + 8 {
                    return Err(Error::Protocol("truncated replica fetch record".into()));
                }
                let offset = src.get_u64_le();
                let timestamp_ms = src.get_i64_le();
                let key = get_optional_bytes(&mut src)?;
                let value = get_bytes(&mut src)?;
                let headers = get_headers(&mut src)?;
                records.push(FetchRecord {
                    offset,
                    timestamp_ms,
                    key,
                    value,
                    headers,
                });
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
        ResponseOpcode::HeartbeatBroker => {
            if src.remaining() < 2 + 4 + 4 + 4 {
                return Err(Error::Protocol(
                    "truncated heartbeat broker response".into(),
                ));
            }
            let error_code = src.get_u16_le();
            let controller_id = src.get_u32_le();
            let generation = src.get_u32_le();
            let alive_count = src.get_u32_le() as usize;
            let mut alive_brokers = Vec::with_capacity(alive_count);
            for _ in 0..alive_count {
                if src.remaining() < 4 {
                    return Err(Error::Protocol("truncated alive broker id".into()));
                }
                alive_brokers.push(src.get_u32_le());
            }
            Ok(Response::HeartbeatBroker {
                error_code,
                controller_id,
                generation,
                alive_brokers,
            })
        }
        ResponseOpcode::ClusterState => {
            if src.remaining() < 2 + 4 + 4 {
                return Err(Error::Protocol("truncated cluster state response".into()));
            }
            let error_code = src.get_u16_le();
            let generation = src.get_u32_le();
            let controller_id = src.get_u32_le();
            let topics = get_cluster_topics(&mut src)?;
            Ok(Response::ClusterState {
                error_code,
                generation,
                controller_id,
                topics,
            })
        }
        ResponseOpcode::Auth => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated auth error".into()));
            }
            Ok(Response::Auth {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::InitProducerId => {
            if src.remaining() < 8 + 2 + 2 {
                return Err(Error::Protocol(
                    "truncated init producer id response".into(),
                ));
            }
            Ok(Response::InitProducerId {
                producer_id: src.get_u64_le(),
                epoch: src.get_u16_le(),
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::BeginTxn => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated begin txn error".into()));
            }
            Ok(Response::BeginTxn {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::EndTxn => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol("truncated end txn header".into()));
            }
            let error_code = src.get_u16_le();
            let result_count = src.get_u32_le() as usize;
            let mut results = Vec::with_capacity(result_count);
            for _ in 0..result_count {
                let topic = get_string(&mut src)?;
                if src.remaining() < 4 + 8 + 4 {
                    return Err(Error::Protocol("truncated end txn result".into()));
                }
                results.push(TxnProduceResult {
                    topic,
                    partition: src.get_u32_le(),
                    base_offset: src.get_u64_le(),
                    count: src.get_u32_le(),
                });
            }
            Ok(Response::EndTxn {
                error_code,
                results,
            })
        }
        ResponseOpcode::DescribeGroup => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated describe group error".into()));
            }
            let error_code = src.get_u16_le();
            let group_id = get_string(&mut src)?;
            if src.remaining() < 4 + 4 {
                return Err(Error::Protocol("truncated describe group header".into()));
            }
            let generation = src.get_u32_le();
            let member_count = src.get_u32_le() as usize;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                let member_id = get_string(&mut src)?;
                if src.remaining() < 4 {
                    return Err(Error::Protocol(
                        "truncated describe group topic count".into(),
                    ));
                }
                let topic_count = src.get_u32_le() as usize;
                let mut topics = Vec::with_capacity(topic_count);
                for _ in 0..topic_count {
                    topics.push(get_string(&mut src)?);
                }
                if src.remaining() < 4 {
                    return Err(Error::Protocol(
                        "truncated describe group assignment count".into(),
                    ));
                }
                let assignment_count = src.get_u32_le() as usize;
                let mut assignment = Vec::with_capacity(assignment_count);
                for _ in 0..assignment_count {
                    let topic = get_string(&mut src)?;
                    if src.remaining() < 4 {
                        return Err(Error::Protocol(
                            "truncated describe group assignment partition".into(),
                        ));
                    }
                    assignment.push(Assignment {
                        topic,
                        partition: src.get_u32_le(),
                    });
                }
                members.push(GroupMemberInfo {
                    member_id,
                    topics,
                    assignment,
                });
            }
            Ok(Response::DescribeGroup {
                error_code,
                group_id,
                generation,
                members,
            })
        }
        ResponseOpcode::ListGroups => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol("truncated list groups header".into()));
            }
            let error_code = src.get_u16_le();
            let group_count = src.get_u32_le() as usize;
            let mut groups = Vec::with_capacity(group_count);
            for _ in 0..group_count {
                let group_id = get_string(&mut src)?;
                if src.remaining() < 1 + 4 + 4 {
                    return Err(Error::Protocol("truncated list groups entry".into()));
                }
                let state = GroupState::from_u8(src.get_u8());
                let member_count = src.get_u32_le();
                let generation = src.get_u32_le();
                groups.push(GroupListing {
                    group_id,
                    state,
                    member_count,
                    generation,
                });
            }
            Ok(Response::ListGroups { error_code, groups })
        }
        ResponseOpcode::DeleteOffsets => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol("truncated delete offsets response".into()));
            }
            Ok(Response::DeleteOffsets {
                error_code: src.get_u16_le(),
                deleted_count: src.get_u32_le(),
            })
        }
        ResponseOpcode::DescribeConfigs => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated describe configs error".into()));
            }
            let error_code = src.get_u16_le();
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 4 + 4 {
                return Err(Error::Protocol("truncated describe configs header".into()));
            }
            let topic_id = src.get_u32_le();
            let partition_count = src.get_u32_le();
            let n = src.get_u32_le() as usize;
            let mut configs = Vec::with_capacity(n);
            for _ in 0..n {
                let k = get_string(&mut src)?;
                let v = get_string(&mut src)?;
                configs.push((k, v));
            }
            Ok(Response::DescribeConfigs {
                error_code,
                topic,
                topic_id,
                partition_count,
                configs,
            })
        }
        ResponseOpcode::AlterConfigs => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated alter configs error".into()));
            }
            let error_code = src.get_u16_le();
            let topic = get_string(&mut src)?;
            Ok(Response::AlterConfigs { error_code, topic })
        }
        ResponseOpcode::DeleteRecords => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated delete records error".into()));
            }
            let error_code = src.get_u16_le();
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 8 {
                return Err(Error::Protocol("truncated delete records body".into()));
            }
            let partition = src.get_u32_le();
            let low_watermark = src.get_u64_le();
            Ok(Response::DeleteRecords {
                error_code,
                topic,
                partition,
                low_watermark,
            })
        }
        ResponseOpcode::CreatePartitions => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated create partitions error".into()));
            }
            let error_code = src.get_u16_le();
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated create partitions count".into()));
            }
            let partitions = src.get_u32_le();
            Ok(Response::CreatePartitions {
                error_code,
                topic,
                partitions,
            })
        }
        ResponseOpcode::ListOffsets => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated list offsets error".into()));
            }
            let error_code = src.get_u16_le();
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated list offsets count".into()));
            }
            let n = src.get_u32_le() as usize;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                if src.remaining() < 4 + 8 + 8 {
                    return Err(Error::Protocol("truncated list offsets entry".into()));
                }
                entries.push(OffsetListing {
                    partition: src.get_u32_le(),
                    earliest: src.get_u64_le(),
                    latest: src.get_u64_le(),
                });
            }
            Ok(Response::ListOffsets {
                error_code,
                topic,
                entries,
            })
        }
        ResponseOpcode::CreateAcls => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated create acls error".into()));
            }
            Ok(Response::CreateAcls {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::DeleteAcls => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol("truncated delete acls".into()));
            }
            Ok(Response::DeleteAcls {
                error_code: src.get_u16_le(),
                removed: src.get_u32_le(),
            })
        }
        ResponseOpcode::ListAcls => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol("truncated list acls header".into()));
            }
            let error_code = src.get_u16_le();
            let n = src.get_u32_le() as usize;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                entries.push(get_acl_binding(&mut src)?);
            }
            Ok(Response::ListAcls {
                error_code,
                entries,
            })
        }
        ResponseOpcode::ScramFirst => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated scram first error".into()));
            }
            let error_code = src.get_u16_le();
            let combined_nonce = get_string(&mut src)?;
            let salt = get_bytes(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated scram first iterations".into()));
            }
            let iterations = src.get_u32_le();
            Ok(Response::ScramFirst {
                error_code,
                combined_nonce,
                salt,
                iterations,
            })
        }
        ResponseOpcode::ScramFinal => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated scram final error".into()));
            }
            let error_code = src.get_u16_le();
            let server_signature = get_bytes(&mut src)?;
            Ok(Response::ScramFinal {
                error_code,
                server_signature,
            })
        }
        ResponseOpcode::CreateScramUser => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated create scram user error".into()));
            }
            Ok(Response::CreateScramUser {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::DeleteScramUser => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated delete scram user error".into()));
            }
            Ok(Response::DeleteScramUser {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::ListScramUsers => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol("truncated list scram users header".into()));
            }
            let error_code = src.get_u16_le();
            let n = src.get_u32_le() as usize;
            let mut usernames = Vec::with_capacity(n);
            for _ in 0..n {
                usernames.push(get_string(&mut src)?);
            }
            Ok(Response::ListScramUsers {
                error_code,
                usernames,
            })
        }
        ResponseOpcode::ReplicaDeleteRecords => {
            if src.remaining() < 2 + 8 {
                return Err(Error::Protocol(
                    "truncated replica delete records response".into(),
                ));
            }
            Ok(Response::ReplicaDeleteRecords {
                error_code: src.get_u16_le(),
                low_watermark: src.get_u64_le(),
            })
        }
        ResponseOpcode::ClusterBrokerConfig => {
            if src.remaining() < 2 + 8 {
                return Err(Error::Protocol(
                    "truncated cluster broker config response".into(),
                ));
            }
            Ok(Response::ClusterBrokerConfig {
                error_code: src.get_u16_le(),
                applied_generation: src.get_u64_le(),
            })
        }
        ResponseOpcode::ClusterAclSnapshot => {
            if src.remaining() < 2 + 8 {
                return Err(Error::Protocol(
                    "truncated cluster acl snapshot response".into(),
                ));
            }
            Ok(Response::ClusterAclSnapshot {
                error_code: src.get_u16_le(),
                applied_generation: src.get_u64_le(),
            })
        }
        ResponseOpcode::TxnParticipantOpen => {
            if src.remaining() < 2 {
                return Err(Error::Protocol(
                    "truncated txn participant open response".into(),
                ));
            }
            Ok(Response::TxnParticipantOpen {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::TxnParticipantPrepare => {
            if src.remaining() < 2 {
                return Err(Error::Protocol(
                    "truncated txn participant prepare response".into(),
                ));
            }
            Ok(Response::TxnParticipantPrepare {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::TxnParticipantComplete => {
            if src.remaining() < 2 {
                return Err(Error::Protocol(
                    "truncated txn participant complete response".into(),
                ));
            }
            Ok(Response::TxnParticipantComplete {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::KafkaFetchForward => {
            if src.remaining() < 2 {
                return Err(Error::Protocol(
                    "truncated kafka fetch forward response".into(),
                ));
            }
            let error_code = src.get_u16_le();
            let body = get_bytes(&mut src)?;
            Ok(Response::KafkaFetchForward { error_code, body })
        }
        ResponseOpcode::KafkaTxnForward => {
            if src.remaining() < 2 {
                return Err(Error::Protocol(
                    "truncated kafka txn forward response".into(),
                ));
            }
            let error_code = src.get_u16_le();
            let body = get_bytes(&mut src)?;
            Ok(Response::KafkaTxnForward { error_code, body })
        }
        ResponseOpcode::TruncateJournalNote => {
            if src.remaining() < 2 + 8 {
                return Err(Error::Protocol(
                    "truncated truncate journal note response".into(),
                ));
            }
            Ok(Response::TruncateJournalNote {
                error_code: src.get_u16_le(),
                generation: src.get_u64_le(),
            })
        }
        ResponseOpcode::TruncateJournalPush => {
            if src.remaining() < 2 {
                return Err(Error::Protocol(
                    "truncated truncate journal push response".into(),
                ));
            }
            Ok(Response::TruncateJournalPush {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::FetchSessionMirrorPut => {
            if src.remaining() < 2 {
                return Err(Error::Protocol(
                    "truncated fetch session mirror put response".into(),
                ));
            }
            Ok(Response::FetchSessionMirrorPut {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::FetchSessionMirrorDelete => {
            if src.remaining() < 2 {
                return Err(Error::Protocol(
                    "truncated fetch session mirror delete response".into(),
                ));
            }
            Ok(Response::FetchSessionMirrorDelete {
                error_code: src.get_u16_le(),
            })
        }
        ResponseOpcode::IsrUpdate => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol("truncated isr update response".into()));
            }
            Ok(Response::IsrUpdate {
                error_code: src.get_u16_le(),
                generation: src.get_u32_le(),
            })
        }
        ResponseOpcode::AssignmentConsensusNote => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol(
                    "truncated assignment consensus note response".into(),
                ));
            }
            Ok(Response::AssignmentConsensusNote {
                error_code: src.get_u16_le(),
                generation: src.get_u32_le(),
            })
        }
        ResponseOpcode::MetadataRaftAppend => {
            // term(8) + success(1) + match_index(8)
            if src.remaining() < 8 + 1 + 8 {
                return Err(Error::Protocol(
                    "truncated metadata raft append response".into(),
                ));
            }
            Ok(Response::MetadataRaftAppend {
                term: src.get_u64_le(),
                success: src.get_u8(),
                match_index: src.get_u64_le(),
            })
        }
        ResponseOpcode::MembershipPut => {
            if src.remaining() < 2 + 8 {
                return Err(Error::Protocol("truncated membership put response".into()));
            }
            Ok(Response::MembershipPut {
                error_code: src.get_u16_le(),
                applied_generation: src.get_u64_le(),
            })
        }
        ResponseOpcode::AddBroker => {
            if src.remaining() < 2 + 8 {
                return Err(Error::Protocol("truncated add broker response".into()));
            }
            Ok(Response::AddBroker {
                error_code: src.get_u16_le(),
                generation: src.get_u64_le(),
            })
        }
        ResponseOpcode::RemoveBroker => {
            if src.remaining() < 2 + 8 {
                return Err(Error::Protocol("truncated remove broker response".into()));
            }
            Ok(Response::RemoveBroker {
                error_code: src.get_u16_le(),
                generation: src.get_u64_le(),
            })
        }
        ResponseOpcode::ListMembers => {
            if src.remaining() < 2 + 8 + 4 {
                return Err(Error::Protocol("truncated list members header".into()));
            }
            let error_code = src.get_u16_le();
            let generation = src.get_u64_le();
            let count = src.get_u32_le() as usize;
            let mut brokers = Vec::with_capacity(count);
            for _ in 0..count {
                brokers.push(get_membership_broker(&mut src)?);
            }
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated list members live count".into()));
            }
            let live_count = src.get_u32_le() as usize;
            if src.remaining() < live_count.saturating_mul(4) {
                return Err(Error::Protocol("truncated list members live ids".into()));
            }
            let mut live = Vec::with_capacity(live_count);
            for _ in 0..live_count {
                live.push(src.get_u32_le());
            }
            Ok(Response::ListMembers {
                error_code,
                generation,
                brokers,
                live,
            })
        }
        ResponseOpcode::OpenraftAppend => Ok(Response::OpenraftAppend {
            payload: get_bytes(&mut src)?,
        }),
        ResponseOpcode::OpenraftVote => Ok(Response::OpenraftVote {
            payload: get_bytes(&mut src)?,
        }),
        ResponseOpcode::OpenraftInstallSnapshot => Ok(Response::OpenraftInstallSnapshot {
            payload: get_bytes(&mut src)?,
        }),
        ResponseOpcode::ReassignPartitions => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol(
                    "truncated reassign partitions response".into(),
                ));
            }
            Ok(Response::ReassignPartitions {
                error_code: src.get_u16_le(),
                generation: src.get_u32_le(),
            })
        }
        ResponseOpcode::SyncGroup => {
            if src.remaining() < 2 + 4 {
                return Err(Error::Protocol("truncated sync group header".into()));
            }
            let error_code = src.get_u16_le();
            let assignment_count = src.get_u32_le() as usize;
            let mut assignment = Vec::with_capacity(assignment_count);
            for _ in 0..assignment_count {
                let topic = get_string(&mut src)?;
                if src.remaining() < 4 {
                    return Err(Error::Protocol("truncated sync group partition".into()));
                }
                assignment.push(Assignment {
                    topic,
                    partition: src.get_u32_le(),
                });
            }
            Ok(Response::SyncGroup {
                error_code,
                assignment,
            })
        }
        ResponseOpcode::Error => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated error code".into()));
            }
            let code = src.get_u16_le();
            let message = get_string(&mut src)?;
            let _ = ErrorCode::from_u16(code);
            Ok(Response::Error { code, message })
        }
    }
}

/// Pack a request into a CRC-protected frame.
pub fn pack_request(corr: u32, req: &Request) -> Result<Frame> {
    let payload = encode_request(req)?;
    let cs = checksum(&payload);
    Ok(Frame {
        header: FrameHeader {
            version: PROTOCOL_VERSION,
            opcode: req.opcode(),
            correlation_id: corr,
            payload_len: payload.len() as u32,
            checksum: cs,
        },
        payload,
    })
}

/// Pack a response into a CRC-protected frame.
pub fn pack_response(corr: u32, resp: &Response) -> Result<Frame> {
    let payload = encode_response(resp)?;
    let cs = checksum(&payload);
    Ok(Frame {
        header: FrameHeader {
            version: PROTOCOL_VERSION,
            opcode: resp.opcode(),
            correlation_id: corr,
            payload_len: payload.len() as u32,
            checksum: cs,
        },
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::{OffsetCommitEntry, OffsetEntry, ProduceMessage};
    use crate::response::ResponseOpcode;
    use bytes::BufMut;

    #[test]
    fn produce_roundtrip() {
        let req = Request::Produce {
            topic: "events".into(),
            partition: -1,
            acks: 1,
            messages: vec![ProduceMessage {
                key: Some(Bytes::from_static(b"k")),
                value: Bytes::from_static(b"v"),
                timestamp_ms: -1,
                headers: vec![("h".into(), Bytes::from_static(b"hv"))],
            }],
            producer_id: 0,
            producer_epoch: 0,
            base_sequence: -1,
        };
        let bytes = encode_request(&req).unwrap();
        let decoded = decode_request(RequestOpcode::Produce as u16, &bytes).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn produce_legacy_without_trailer_decodes() {
        // Manually craft pre-Phase-10 produce payload (no trailer).
        let mut dst = BytesMut::new();
        put_string(&mut dst, "t").unwrap();
        dst.put_i32_le(0);
        dst.put_u8(1);
        dst.put_u32_le(1);
        put_optional_bytes(&mut dst, None);
        put_bytes(&mut dst, b"v");
        dst.put_i64_le(-1);
        put_headers(&mut dst, &[]).unwrap();
        let decoded = decode_request(RequestOpcode::Produce as u16, &dst).unwrap();
        match decoded {
            Request::Produce {
                producer_id,
                producer_epoch,
                base_sequence,
                ..
            } => {
                assert_eq!(producer_id, 0);
                assert_eq!(producer_epoch, 0);
                assert_eq!(base_sequence, -1);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn init_producer_id_roundtrip() {
        let req = Request::InitProducerId {
            transactional_id: "app-1".into(),
        };
        let bytes = encode_request(&req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::InitProducerId as u16, &bytes).unwrap(),
            req
        );
        // Legacy empty InitProducerId body.
        let decoded = decode_request(RequestOpcode::InitProducerId as u16, &Bytes::new()).unwrap();
        assert_eq!(
            decoded,
            Request::InitProducerId {
                transactional_id: String::new(),
            }
        );
        let resp = Response::InitProducerId {
            producer_id: 42,
            epoch: 1,
            error_code: 0,
        };
        let rb = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::InitProducerId as u16, &rb).unwrap(),
            resp
        );
    }

    #[test]
    fn phase18_txn_roundtrip() {
        let begin = Request::BeginTxn {
            producer_id: 7,
            producer_epoch: 1,
        };
        let b = encode_request(&begin).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::BeginTxn as u16, &b).unwrap(),
            begin
        );
        let end = Request::EndTxn {
            producer_id: 7,
            producer_epoch: 1,
            committed: true,
            offsets: vec![TxnOffsetCommit {
                group_id: "g".into(),
                topic: "t".into(),
                partition: 0,
                offset: 9,
                metadata: "m".into(),
            }],
        };
        let b = encode_request(&end).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::EndTxn as u16, &b).unwrap(),
            end
        );
        let br = Response::BeginTxn { error_code: 0 };
        let b = encode_response(&br).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::BeginTxn as u16, &b).unwrap(),
            br
        );
        let er = Response::EndTxn {
            error_code: 0,
            results: vec![TxnProduceResult {
                topic: "t".into(),
                partition: 1,
                base_offset: 10,
                count: 2,
            }],
        };
        let b = encode_response(&er).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::EndTxn as u16, &b).unwrap(),
            er
        );
        assert_eq!(ErrorCode::from_u16(22), ErrorCode::InvalidTxnState);
    }

    #[test]
    fn phase11_describe_group_roundtrip() {
        let req = Request::DescribeGroup {
            group_id: "cg-1".into(),
        };
        let bytes = encode_request(&req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::DescribeGroup as u16, &bytes).unwrap(),
            req
        );

        let resp = Response::DescribeGroup {
            error_code: 0,
            group_id: "cg-1".into(),
            generation: 3,
            members: vec![GroupMemberInfo {
                member_id: "m-a".into(),
                topics: vec!["events".into()],
                assignment: vec![
                    Assignment {
                        topic: "events".into(),
                        partition: 0,
                    },
                    Assignment {
                        topic: "events".into(),
                        partition: 2,
                    },
                ],
            }],
        };
        let rb = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::DescribeGroup as u16, &rb).unwrap(),
            resp
        );
    }

    #[test]
    fn phase13_configs_roundtrip() {
        let create = Request::CreateTopic {
            name: "events".into(),
            partitions: 3,
            configs: vec![
                ("retention.ms".into(), "1000".into()),
                ("segment.bytes".into(), "4096".into()),
            ],
        };
        let b = encode_request(&create).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::CreateTopic as u16, &b).unwrap(),
            create
        );

        // Legacy CreateTopic without config trailer.
        let mut legacy = BytesMut::new();
        put_string(&mut legacy, "t").unwrap();
        legacy.put_u32_le(2);
        let decoded = decode_request(RequestOpcode::CreateTopic as u16, &legacy.freeze()).unwrap();
        match decoded {
            Request::CreateTopic { configs, .. } => assert!(configs.is_empty()),
            other => panic!("unexpected {other:?}"),
        }

        let desc = Request::DescribeConfigs {
            topic: "events".into(),
        };
        let b = encode_request(&desc).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::DescribeConfigs as u16, &b).unwrap(),
            desc
        );
        let desc_resp = Response::DescribeConfigs {
            error_code: 0,
            topic: "events".into(),
            topic_id: 1,
            partition_count: 3,
            configs: vec![("retention.ms".into(), "1000".into())],
        };
        let b = encode_response(&desc_resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::DescribeConfigs as u16, &b).unwrap(),
            desc_resp
        );

        let alt = Request::AlterConfigs {
            topic: "events".into(),
            configs: vec![("retention.bytes".into(), "1024".into())],
        };
        let b = encode_request(&alt).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::AlterConfigs as u16, &b).unwrap(),
            alt
        );
        let alt_resp = Response::AlterConfigs {
            error_code: 0,
            topic: "events".into(),
        };
        let b = encode_response(&alt_resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::AlterConfigs as u16, &b).unwrap(),
            alt_resp
        );
    }

    #[test]
    fn phase14_delete_records_roundtrip() {
        let req = Request::DeleteRecords {
            topic: "events".into(),
            partition: 2,
            before_offset: 100,
            wait_majority: 0,
        };
        let b = encode_request(&req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::DeleteRecords as u16, &b).unwrap(),
            req
        );
        let resp = Response::DeleteRecords {
            error_code: 0,
            topic: "events".into(),
            partition: 2,
            low_watermark: 96,
        };
        let b = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::DeleteRecords as u16, &b).unwrap(),
            resp
        );
    }

    /// Phase 137: legacy payload without trailer decodes wait_majority=0;
    /// trailers 1 and 2 round-trip.
    #[test]
    fn phase137_delete_records_wait_majority_trailer() {
        // Legacy body: topic + partition + before_offset (no trailer).
        let mut legacy = bytes::BytesMut::new();
        put_string(&mut legacy, "events").unwrap();
        legacy.put_u32_le(1);
        legacy.put_u64_le(42);
        let decoded = decode_request(RequestOpcode::DeleteRecords as u16, &legacy).unwrap();
        assert_eq!(
            decoded,
            Request::DeleteRecords {
                topic: "events".into(),
                partition: 1,
                before_offset: 42,
                wait_majority: 0,
            }
        );

        for flag in [1u8, 2u8] {
            let req = Request::DeleteRecords {
                topic: "events".into(),
                partition: 0,
                before_offset: 10,
                wait_majority: flag,
            };
            let b = encode_request(&req).unwrap();
            assert_eq!(
                decode_request(RequestOpcode::DeleteRecords as u16, &b).unwrap(),
                req
            );
        }
    }

    #[test]
    fn phase15_create_partitions_list_offsets_roundtrip() {
        let req = Request::CreatePartitions {
            topic: "events".into(),
            total_count: 8,
        };
        let b = encode_request(&req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::CreatePartitions as u16, &b).unwrap(),
            req
        );
        let resp = Response::CreatePartitions {
            error_code: 0,
            topic: "events".into(),
            partitions: 8,
        };
        let b = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::CreatePartitions as u16, &b).unwrap(),
            resp
        );

        let lo = Request::ListOffsets {
            topic: "events".into(),
            partitions: vec![0, 1],
        };
        let b = encode_request(&lo).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ListOffsets as u16, &b).unwrap(),
            lo
        );
        let lo_resp = Response::ListOffsets {
            error_code: 0,
            topic: "events".into(),
            entries: vec![
                OffsetListing {
                    partition: 0,
                    earliest: 0,
                    latest: 10,
                },
                OffsetListing {
                    partition: 1,
                    earliest: 2,
                    latest: 5,
                },
            ],
        };
        let b = encode_response(&lo_resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::ListOffsets as u16, &b).unwrap(),
            lo_resp
        );
    }

    #[test]
    fn phase20_acl_roundtrip() {
        let entry = AclBinding {
            principal: "alice".into(),
            resource_type: 0,
            resource: "events".into(),
            operation: 2,
            permission: 1,
        };
        let create = Request::CreateAcls {
            entries: vec![entry.clone()],
        };
        let b = encode_request(&create).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::CreateAcls as u16, &b).unwrap(),
            create
        );
        let list = Request::ListAcls {
            principal: "alice".into(),
            resource_type: 255,
            resource: String::new(),
        };
        let b = encode_request(&list).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ListAcls as u16, &b).unwrap(),
            list
        );
        let lr = Response::ListAcls {
            error_code: 0,
            entries: vec![entry],
        };
        let b = encode_response(&lr).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::ListAcls as u16, &b).unwrap(),
            lr
        );
        let dr = Response::DeleteAcls {
            error_code: 0,
            removed: 1,
        };
        let b = encode_response(&dr).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::DeleteAcls as u16, &b).unwrap(),
            dr
        );
    }

    #[test]
    fn phase22_scram_roundtrip() {
        let first = Request::ScramFirst {
            username: "alice".into(),
            client_nonce: "n1".into(),
        };
        let b = encode_request(&first).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ScramFirst as u16, &b).unwrap(),
            first
        );
        let first_resp = Response::ScramFirst {
            error_code: 0,
            combined_nonce: "n1s1".into(),
            salt: Bytes::from(vec![1, 2, 3]),
            iterations: 4096,
        };
        let b = encode_response(&first_resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::ScramFirst as u16, &b).unwrap(),
            first_resp
        );

        let final_req = Request::ScramFinal {
            username: "alice".into(),
            combined_nonce: "n1s1".into(),
            client_proof: Bytes::from(vec![0u8; 32]),
        };
        let b = encode_request(&final_req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ScramFinal as u16, &b).unwrap(),
            final_req
        );
        let final_resp = Response::ScramFinal {
            error_code: 0,
            server_signature: Bytes::from(vec![9u8; 32]),
        };
        let b = encode_response(&final_resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::ScramFinal as u16, &b).unwrap(),
            final_resp
        );

        let create = Request::CreateScramUser {
            username: "bob".into(),
            password: "x".into(),
            iterations: 0,
        };
        let b = encode_request(&create).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::CreateScramUser as u16, &b).unwrap(),
            create
        );
        let create_resp = Response::CreateScramUser { error_code: 0 };
        let b = encode_response(&create_resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::CreateScramUser as u16, &b).unwrap(),
            create_resp
        );

        let del = Request::DeleteScramUser {
            username: "bob".into(),
        };
        let b = encode_request(&del).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::DeleteScramUser as u16, &b).unwrap(),
            del
        );
        let del_resp = Response::DeleteScramUser { error_code: 0 };
        let b = encode_response(&del_resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::DeleteScramUser as u16, &b).unwrap(),
            del_resp
        );

        let list = Request::ListScramUsers;
        let b = encode_request(&list).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ListScramUsers as u16, &b).unwrap(),
            list
        );
        let list_resp = Response::ListScramUsers {
            error_code: 0,
            usernames: vec!["alice".into(), "bob".into()],
        };
        let b = encode_response(&list_resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::ListScramUsers as u16, &b).unwrap(),
            list_resp
        );
    }

    #[test]
    fn phase12_list_delete_static_roundtrip() {
        let list_req = Request::ListGroups;
        let b = encode_request(&list_req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ListGroups as u16, &b).unwrap(),
            list_req
        );

        let list_resp = Response::ListGroups {
            error_code: 0,
            groups: vec![
                GroupListing {
                    group_id: "g1".into(),
                    state: GroupState::Stable,
                    member_count: 2,
                    generation: 5,
                },
                GroupListing {
                    group_id: "g2".into(),
                    state: GroupState::Empty,
                    member_count: 0,
                    generation: 0,
                },
                GroupListing {
                    group_id: "g3".into(),
                    state: GroupState::CompletingRebalance,
                    member_count: 1,
                    generation: 1,
                },
                GroupListing {
                    group_id: "g4".into(),
                    state: GroupState::PreparingRebalance,
                    member_count: 1,
                    generation: 1,
                },
            ],
        };
        let b = encode_response(&list_resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::ListGroups as u16, &b).unwrap(),
            list_resp
        );
        assert_eq!(GroupState::from_u8(0), GroupState::Empty);
        assert_eq!(GroupState::from_u8(1), GroupState::Stable);
        assert_eq!(GroupState::from_u8(2), GroupState::CompletingRebalance);
        assert_eq!(GroupState::from_u8(3), GroupState::PreparingRebalance);
        assert_eq!(GroupState::from_u8(99), GroupState::Empty);
        assert_eq!(GroupState::PreparingRebalance.as_str(), "PreparingRebalance");

        let del = Request::DeleteOffsets {
            group_id: "g1".into(),
            entries: vec![OffsetEntry {
                topic: "events".into(),
                partition: 0,
            }],
        };
        let b = encode_request(&del).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::DeleteOffsets as u16, &b).unwrap(),
            del
        );
        let del_resp = Response::DeleteOffsets {
            error_code: 0,
            deleted_count: 1,
        };
        let b = encode_response(&del_resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::DeleteOffsets as u16, &b).unwrap(),
            del_resp
        );

        let join = Request::JoinGroup {
            group_id: "g1".into(),
            member_id: String::new(),
            session_timeout_ms: 10_000,
            topics: vec!["events".into()],
            group_instance_id: "pod-1".into(),
            rebalance_timeout_ms: 1500,
        };
        let b = encode_request(&join).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::JoinGroup as u16, &b).unwrap(),
            join
        );

        // Legacy JoinGroup without instance trailer still decodes.
        let mut legacy = BytesMut::new();
        put_string(&mut legacy, "g1").unwrap();
        put_string(&mut legacy, "m1").unwrap();
        legacy.put_u32_le(5000);
        legacy.put_u32_le(1);
        put_string(&mut legacy, "t").unwrap();
        let decoded = decode_request(RequestOpcode::JoinGroup as u16, &legacy.freeze()).unwrap();
        match decoded {
            Request::JoinGroup {
                group_instance_id,
                rebalance_timeout_ms,
                ..
            } => {
                assert!(group_instance_id.is_empty());
                assert_eq!(rebalance_timeout_ms, 0);
            }
            other => panic!("unexpected {other:?}"),
        }

        // Instance present, rebalance trailer omitted → 0.
        let mut no_rebalance = BytesMut::new();
        put_string(&mut no_rebalance, "g1").unwrap();
        put_string(&mut no_rebalance, "m1").unwrap();
        no_rebalance.put_u32_le(5000);
        no_rebalance.put_u32_le(1);
        put_string(&mut no_rebalance, "t").unwrap();
        put_string(&mut no_rebalance, "pod-1").unwrap();
        match decode_request(RequestOpcode::JoinGroup as u16, &no_rebalance.freeze()).unwrap() {
            Request::JoinGroup {
                group_instance_id,
                rebalance_timeout_ms,
                ..
            } => {
                assert_eq!(group_instance_id, "pod-1");
                assert_eq!(rebalance_timeout_ms, 0);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn phase10_idempotent_error_codes() {
        assert_eq!(ErrorCode::from_u16(19), ErrorCode::InvalidProducerEpoch);
        assert_eq!(ErrorCode::from_u16(20), ErrorCode::OutOfOrderSequence);
        assert_eq!(ErrorCode::from_u16(21), ErrorCode::UnknownProducerId);
    }

    #[test]
    fn fetch_and_create_roundtrip() {
        let fetch = Request::Fetch {
            topic: "t".into(),
            partition: 2,
            from_offset: 10,
            max_messages: 5,
            max_bytes: 1024,
            max_wait_ms: 0,
            group_id: String::new(),
            member_id: String::new(),
        };
        let b = encode_request(&fetch).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::Fetch as u16, &b).unwrap(),
            fetch
        );

        // Legacy Fetch (no group+member trailer) stays unfiltered.
        let mut legacy = BytesMut::new();
        put_string(&mut legacy, "t").unwrap();
        legacy.put_u32_le(2);
        legacy.put_u64_le(10);
        legacy.put_u32_le(5);
        legacy.put_u32_le(1024);
        legacy.put_u32_le(0);
        match decode_request(RequestOpcode::Fetch as u16, &legacy).unwrap() {
            Request::Fetch {
                group_id,
                member_id,
                ..
            } => {
                assert!(group_id.is_empty());
                assert!(member_id.is_empty());
            }
            other => panic!("unexpected {other:?}"),
        }

        let grouped = Request::Fetch {
            topic: "t".into(),
            partition: 0,
            from_offset: 0,
            max_messages: 1,
            max_bytes: 64,
            max_wait_ms: 0,
            group_id: "g".into(),
            member_id: "m1".into(),
        };
        let b = encode_request(&grouped).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::Fetch as u16, &b).unwrap(),
            grouped
        );

        let create = Request::CreateTopic {
            name: "t".into(),
            partitions: 3,
            configs: vec![],
        };
        let b = encode_request(&create).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::CreateTopic as u16, &b).unwrap(),
            create
        );
    }

    #[test]
    fn metadata_response_roundtrip() {
        let resp = Response::Metadata {
            brokers: vec![BrokerInfo {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 9092,
            }],
            topics: vec![TopicInfo {
                name: "events".into(),
                topic_id: 7,
                error_code: 0,
                partitions: vec![PartitionInfo {
                    partition_id: 0,
                    leader: 1,
                    hwm: 42,
                    replicas: vec![1, 2, 3],
                    isr: vec![1, 2],
                    leader_epoch: 1,
                }],
            }],
            controller_id: 2,
        };
        let b = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::Metadata as u16, &b).unwrap(),
            resp
        );
    }

    #[test]
    fn metadata_response_legacy_without_controller_id() {
        let topics = vec![TopicInfo {
            name: "events".into(),
            topic_id: 7,
            error_code: 0,
            partitions: vec![PartitionInfo {
                partition_id: 0,
                leader: 1,
                hwm: 42,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2],
                leader_epoch: 1,
            }],
        }];
        let encoded = encode_response(&Response::Metadata {
            brokers: vec![BrokerInfo {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 9092,
            }],
            topics: topics.clone(),
            controller_id: 2,
        })
        .unwrap();
        assert!(encoded.len() >= 4);
        let legacy = &encoded[..encoded.len() - 4];
        match decode_response(ResponseOpcode::Metadata as u16, legacy).unwrap() {
            Response::Metadata {
                controller_id,
                topics: decoded_topics,
                ..
            } => {
                assert_eq!(controller_id, 0);
                assert_eq!(decoded_topics, topics);
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    #[test]
    fn pack_request_sets_crc() {
        let req = Request::DeleteTopic {
            name: "gone".into(),
        };
        let frame = pack_request(99, &req).unwrap();
        assert_eq!(frame.header.correlation_id, 99);
        assert_eq!(frame.header.checksum, checksum(&frame.payload));
    }

    #[test]
    fn group_request_roundtrips() {
        let join = Request::JoinGroup {
            group_id: "g1".into(),
            member_id: "".into(),
            session_timeout_ms: 10_000,
            topics: vec!["events".into(), "logs".into()],
            group_instance_id: String::new(),
            rebalance_timeout_ms: 0,
        };
        let b = encode_request(&join).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::JoinGroup as u16, &b).unwrap(),
            join
        );

        let hb = Request::Heartbeat {
            group_id: "g1".into(),
            member_id: "m1".into(),
            generation: 3,
        };
        let b = encode_request(&hb).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::Heartbeat as u16, &b).unwrap(),
            hb
        );

        let leave = Request::LeaveGroup {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        let b = encode_request(&leave).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::LeaveGroup as u16, &b).unwrap(),
            leave
        );

        let commit = Request::OffsetCommit {
            group_id: "g1".into(),
            member_id: "m1".into(),
            generation: 2,
            entries: vec![OffsetCommitEntry {
                topic: "events".into(),
                partition: 1,
                offset: 42,
                metadata: "cli".into(),
            }],
        };
        let b = encode_request(&commit).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::OffsetCommit as u16, &b).unwrap(),
            commit
        );

        let fetch = Request::OffsetFetch {
            group_id: "g1".into(),
            entries: vec![OffsetEntry {
                topic: "events".into(),
                partition: 1,
            }],
        };
        let b = encode_request(&fetch).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::OffsetFetch as u16, &b).unwrap(),
            fetch
        );

        // Empty entry_count = all offsets.
        let fetch_all = Request::OffsetFetch {
            group_id: "g1".into(),
            entries: vec![],
        };
        let b = encode_request(&fetch_all).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::OffsetFetch as u16, &b).unwrap(),
            fetch_all
        );
    }

    #[test]
    fn group_response_roundtrips() {
        let join = Response::JoinGroup {
            error_code: 0,
            generation: 1,
            member_id: "uuid-1".into(),
            assignment: vec![
                Assignment {
                    topic: "events".into(),
                    partition: 0,
                },
                Assignment {
                    topic: "events".into(),
                    partition: 1,
                },
            ],
            revoked: vec![Assignment {
                topic: "events".into(),
                partition: 2,
            }],
            members: vec!["m-a".into(), "uuid-1".into()],
        };
        let b = encode_response(&join).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::JoinGroup as u16, &b).unwrap(),
            join
        );

        // Legacy JoinGroup response without revoked trailer decodes as empty revoked/members.
        let mut legacy = bytes::BytesMut::new();
        legacy.put_u16_le(0);
        legacy.put_u32_le(1);
        put_string(&mut legacy, "uuid-1").unwrap();
        legacy.put_u32_le(1);
        put_string(&mut legacy, "events").unwrap();
        legacy.put_u32_le(0);
        let decoded = decode_response(ResponseOpcode::JoinGroup as u16, &legacy.freeze()).unwrap();
        assert_eq!(
            decoded,
            Response::JoinGroup {
                error_code: 0,
                generation: 1,
                member_id: "uuid-1".into(),
                assignment: vec![Assignment {
                    topic: "events".into(),
                    partition: 0,
                }],
                revoked: vec![],
                members: vec![],
            }
        );

        // Phase 17 revoked trailer without v0.211 members decodes as empty members.
        let mut no_members = bytes::BytesMut::new();
        no_members.put_u16_le(0);
        no_members.put_u32_le(1);
        put_string(&mut no_members, "uuid-1").unwrap();
        no_members.put_u32_le(1);
        put_string(&mut no_members, "events").unwrap();
        no_members.put_u32_le(0);
        no_members.put_u32_le(1);
        put_string(&mut no_members, "events").unwrap();
        no_members.put_u32_le(2);
        let decoded =
            decode_response(ResponseOpcode::JoinGroup as u16, &no_members.freeze()).unwrap();
        assert_eq!(
            decoded,
            Response::JoinGroup {
                error_code: 0,
                generation: 1,
                member_id: "uuid-1".into(),
                assignment: vec![Assignment {
                    topic: "events".into(),
                    partition: 0,
                }],
                revoked: vec![Assignment {
                    topic: "events".into(),
                    partition: 2,
                }],
                members: vec![],
            }
        );

        let of = Response::OffsetFetch {
            error_code: 0,
            entries: vec![OffsetFetchEntry {
                topic: "events".into(),
                partition: 0,
                offset: u64::MAX,
                metadata: "".into(),
            }],
        };
        let b = encode_response(&of).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::OffsetFetch as u16, &b).unwrap(),
            of
        );

        for (resp, op) in [
            (
                Response::OffsetCommit { error_code: 0 },
                ResponseOpcode::OffsetCommit as u16,
            ),
            (
                Response::Heartbeat { error_code: 9 },
                ResponseOpcode::Heartbeat as u16,
            ),
            (
                Response::LeaveGroup { error_code: 0 },
                ResponseOpcode::LeaveGroup as u16,
            ),
        ] {
            let b = encode_response(&resp).unwrap();
            assert_eq!(decode_response(op, &b).unwrap(), resp);
        }
    }

    #[test]
    fn phase6_replica_fetch_roundtrip() {
        let req = Request::ReplicaFetch {
            topic: "events".into(),
            partition: 1,
            from_offset: 10,
            max_bytes: 1024,
            replica_id: 2,
        };
        let b = encode_request(&req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ReplicaFetch as u16, &b).unwrap(),
            req
        );

        let resp = Response::ReplicaFetch {
            error_code: 0,
            topic: "events".into(),
            partition: 1,
            high_watermark: 10,
            leader_epoch: 3,
            records: vec![FetchRecord {
                offset: 10,
                timestamp_ms: 100,
                key: None,
                value: Bytes::from_static(b"v"),
                headers: vec![],
            }],
        };
        let b = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::ReplicaFetch as u16, &b).unwrap(),
            resp
        );
    }

    #[test]
    fn phase6_heartbeat_and_cluster_state_roundtrip() {
        let req = Request::HeartbeatBroker {
            broker_id: 2,
            controller_id_known: 1,
            generation: 5,
            applied_config_generation: 3,
            applied_acl_generation: 2,
            applied_journal_generation: 9,
        };
        let b = encode_request(&req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::HeartbeatBroker as u16, &b).unwrap(),
            req
        );
        // Older peers without Phase 117 trailer still decode (applied gens = 0).
        let mut legacy = bytes::BytesMut::new();
        legacy.put_u32_le(2);
        legacy.put_u32_le(1);
        legacy.put_u32_le(5);
        let legacy_req = decode_request(RequestOpcode::HeartbeatBroker as u16, &legacy).unwrap();
        assert_eq!(
            legacy_req,
            Request::HeartbeatBroker {
                broker_id: 2,
                controller_id_known: 1,
                generation: 5,
                applied_config_generation: 0,
                applied_acl_generation: 0,
                applied_journal_generation: 0,
            }
        );
        // Phase 117-only trailer (16 bytes) defaults journal gen to 0.
        let mut p117 = bytes::BytesMut::new();
        p117.put_u32_le(2);
        p117.put_u32_le(1);
        p117.put_u32_le(5);
        p117.put_u64_le(3);
        p117.put_u64_le(2);
        let p117_req = decode_request(RequestOpcode::HeartbeatBroker as u16, &p117).unwrap();
        assert_eq!(
            p117_req,
            Request::HeartbeatBroker {
                broker_id: 2,
                controller_id_known: 1,
                generation: 5,
                applied_config_generation: 3,
                applied_acl_generation: 2,
                applied_journal_generation: 0,
            }
        );

        let resp = Response::HeartbeatBroker {
            error_code: 0,
            controller_id: 1,
            generation: 6,
            alive_brokers: vec![1, 2, 3],
        };
        let b = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::HeartbeatBroker as u16, &b).unwrap(),
            resp
        );

        let cs_req = Request::ClusterState {
            known_generation: 5,
        };
        let b = encode_request(&cs_req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ClusterState as u16, &b).unwrap(),
            cs_req
        );

        let cs = Response::ClusterState {
            error_code: 0,
            generation: 6,
            controller_id: 1,
            topics: vec![ClusterTopicState {
                name: "events".into(),
                topic_id: 1,
                partitions: vec![ClusterPartitionState {
                    partition_id: 0,
                    leader: 1,
                    leader_epoch: 2,
                    replicas: vec![1, 2, 3],
                    isr: vec![1, 2, 3],
                }],
            }],
        };
        let b = encode_response(&cs).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::ClusterState as u16, &b).unwrap(),
            cs
        );
    }

    #[test]
    fn phase6_error_codes() {
        assert_eq!(ErrorCode::from_u16(13), ErrorCode::NotLeaderForPartition);
        assert_eq!(ErrorCode::from_u16(14), ErrorCode::NotController);
        assert_eq!(ErrorCode::from_u16(15), ErrorCode::NotEnoughReplicas);
        assert_eq!(ErrorCode::from_u16(16), ErrorCode::BrokerNotAvailable);
    }

    #[test]
    fn phase7_auth_roundtrip() {
        let req = Request::Auth {
            token: "s3cret".into(),
        };
        let b = encode_request(&req).unwrap();
        assert_eq!(decode_request(RequestOpcode::Auth as u16, &b).unwrap(), req);

        let resp = Response::Auth { error_code: 0 };
        let b = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::Auth as u16, &b).unwrap(),
            resp
        );

        let fail = Response::Auth {
            error_code: ErrorCode::AuthenticationFailed as u16,
        };
        let b = encode_response(&fail).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::Auth as u16, &b).unwrap(),
            fail
        );
    }

    #[test]
    fn phase7_auth_error_codes() {
        assert_eq!(ErrorCode::from_u16(17), ErrorCode::AuthenticationFailed);
        assert_eq!(ErrorCode::from_u16(18), ErrorCode::AuthenticationRequired);
    }

    /// Random / truncated / oversized inputs must not panic.
    #[test]
    fn chaos_decode_does_not_panic() {
        // Deterministic pseudo-random (xorshift) — no external deps.
        let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
        let mut next = || -> u64 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        // Empty / truncated payloads for every known opcode.
        let req_ops: &[u16] = &[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 22, 24, 30, 32, 34, 36, 38, 40, 42, 99, 0xFFFF,
        ];
        let resp_ops: &[u16] = &[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 21, 23, 25, 31, 33, 35, 37, 39, 41, 43, 0xFFFF, 42,
        ];

        for op in req_ops {
            let _ = decode_request(*op, &[]);
            let _ = decode_request(*op, &[0u8; 1]);
            let _ = decode_request(*op, &[0u8; 3]);
            let _ = decode_request(*op, &[0xff; 16]);
            let _ = decode_request(*op, &[0xff; 256]);
        }
        for op in resp_ops {
            let _ = decode_response(*op, &[]);
            let _ = decode_response(*op, &[0u8; 1]);
            let _ = decode_response(*op, &[0u8; 3]);
            let _ = decode_response(*op, &[0xff; 16]);
            let _ = decode_response(*op, &[0xff; 256]);
        }

        // Random blobs (expanded Phase 9).
        for _ in 0..1000 {
            let len = (next() as usize % 2048) + 1;
            let mut buf = vec![0u8; len];
            for b in &mut buf {
                *b = (next() & 0xff) as u8;
            }
            let op = (next() & 0xffff) as u16;
            let _ = decode_request(op, &buf);
            let _ = decode_response(op, &buf);

            // Truncated frames into decode_frame.
            let mut frame_buf = BytesMut::from(buf.as_slice());
            let _ = crate::codec::decode_frame(&mut frame_buf);
        }

        // Oversized payload length claim (still must not panic).
        let oversized = vec![0u8; 64];
        let _ = decode_request(1, &oversized);
        // Explicit oversize rejection path.
        let huge = vec![0u8; MAX_PAYLOAD + 1];
        assert!(decode_request(1, &huge).is_err());
        assert!(decode_response(1, &huge).is_err());
    }

    /// Extended frame-level chaos (Phase 9): crafted headers + streaming partials.
    #[test]
    fn chaos_frame_decode_extended() {
        use crate::codec::{checksum, encode_frame, HEADER_LEN};
        use crate::frame::{Frame, FrameHeader, FRAME_MAGIC, PROTOCOL_VERSION};

        let mut state: u64 = 0x0123_4567_89AB_CDEF;
        let mut next = || -> u64 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        // Invalid magic.
        {
            let mut bad = BytesMut::from(&[0x00u8; HEADER_LEN][..]);
            assert!(crate::codec::decode_frame(&mut bad).is_err());
        }

        // Valid magic, wrong version.
        {
            let mut buf = BytesMut::new();
            buf.extend_from_slice(&[
                FRAME_MAGIC,
                0xFF, // version
                0,
                1, // opcode
                0,
                0,
                0,
                1, // corr
                0,
                0,
                0,
                0, // payload_len
                0,
                0,
                0,
                0, // checksum
            ]);
            assert!(crate::codec::decode_frame(&mut buf).is_err());
        }

        // Valid frame with correct checksum, then garbage trailing bytes.
        {
            let payload = bytes::Bytes::from_static(b"ok");
            let frame = Frame {
                header: FrameHeader {
                    version: PROTOCOL_VERSION,
                    opcode: 4,
                    correlation_id: 7,
                    payload_len: payload.len() as u32,
                    checksum: checksum(&payload),
                },
                payload,
            };
            let mut buf = BytesMut::new();
            encode_frame(&frame, &mut buf).unwrap();
            buf.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
            let decoded = crate::codec::decode_frame(&mut buf).unwrap();
            assert!(decoded.is_some());
            // Remaining garbage must not panic.
            let _ = crate::codec::decode_frame(&mut buf);
        }

        // Partial header then complete.
        {
            let payload = bytes::Bytes::from_static(b"partial");
            let frame = Frame {
                header: FrameHeader {
                    version: PROTOCOL_VERSION,
                    opcode: 1,
                    correlation_id: 1,
                    payload_len: payload.len() as u32,
                    checksum: checksum(&payload),
                },
                payload,
            };
            let mut full = BytesMut::new();
            encode_frame(&frame, &mut full).unwrap();
            let full_bytes = full.to_vec();
            let mut buf = BytesMut::new();
            // Feed one byte at a time.
            for b in &full_bytes {
                buf.extend_from_slice(&[*b]);
                let r = crate::codec::decode_frame(&mut buf);
                assert!(r.is_ok());
            }
        }

        // Random header-shaped blobs.
        for _ in 0..500 {
            let len = (next() as usize % 128) + 1;
            let mut raw = vec![0u8; len];
            for b in &mut raw {
                *b = (next() & 0xff) as u8;
            }
            // Occasionally force magic byte for deeper path coverage.
            if next() % 3 == 0 && !raw.is_empty() {
                raw[0] = FRAME_MAGIC;
            }
            let mut buf = BytesMut::from(raw.as_slice());
            let _ = crate::codec::decode_frame(&mut buf);
        }
    }

    /// Phase 112: deterministic corpus smoke — same decode paths as `fuzz/`
    /// targets (`decode_frame`, `decode_request`). Must never panic.
    ///
    /// Loads seed files under `{workspace}/fuzz/corpus/{target}/` when present,
    /// and always exercises a built-in seed set so CI stays green without
    /// cargo-fuzz / nightly.
    #[test]
    fn corpus_smoke_decode_paths() {
        use crate::codec::{checksum, decode_frame, encode_frame};
        use crate::frame::{Frame, FrameHeader, FRAME_MAGIC, PROTOCOL_VERSION};
        use std::path::PathBuf;

        // Mirror fuzz_targets/decode_frame.rs
        fn smoke_frame(data: &[u8]) {
            let mut buf = BytesMut::from(data);
            let _ = decode_frame(&mut buf);
            let _ = decode_frame(&mut buf);
        }

        // Mirror fuzz_targets/decode_request.rs
        fn smoke_request(data: &[u8]) {
            if data.is_empty() {
                return;
            }
            let opcode = u16::from_le_bytes([data[0], data.get(1).copied().unwrap_or(0)]);
            let payload = if data.len() > 2 { &data[2..] } else { &[] };
            let _ = decode_request(opcode, payload);
            let _ = decode_response(opcode, payload);
        }

        // Built-in seeds (always run; keep in sync with fuzz/corpus/*).
        let mut frame_seeds: Vec<Vec<u8>> = vec![
            vec![],
            b"V\x01\x00".to_vec(),
            vec![0u8; 16],
            {
                // wrong version
                let mut v = vec![0u8; 16];
                v[0] = FRAME_MAGIC;
                v[1] = 0xFF;
                v
            },
            {
                let payload = bytes::Bytes::from_static(b"");
                let frame = Frame {
                    header: FrameHeader {
                        version: PROTOCOL_VERSION,
                        opcode: 4,
                        correlation_id: 7,
                        payload_len: 0,
                        checksum: checksum(&payload),
                    },
                    payload,
                };
                let mut buf = BytesMut::new();
                encode_frame(&frame, &mut buf).unwrap();
                buf.to_vec()
            },
            {
                let payload = bytes::Bytes::from_static(b"ping");
                let frame = Frame {
                    header: FrameHeader {
                        version: PROTOCOL_VERSION,
                        opcode: 1,
                        correlation_id: 42,
                        payload_len: payload.len() as u32,
                        checksum: checksum(&payload),
                    },
                    payload,
                };
                let mut buf = BytesMut::new();
                encode_frame(&frame, &mut buf).unwrap();
                buf.to_vec()
            },
            {
                // max-size claim, truncated body
                let mut v = vec![0u8; 16];
                v[0] = FRAME_MAGIC;
                v[1] = PROTOCOL_VERSION;
                v[8..12].copy_from_slice(&0x0100_0000u32.to_be_bytes()); // 16 MiB claim
                v
            },
            {
                let payload = bytes::Bytes::from_static(b"ok");
                let frame = Frame {
                    header: FrameHeader {
                        version: PROTOCOL_VERSION,
                        opcode: 1,
                        correlation_id: 1,
                        payload_len: payload.len() as u32,
                        checksum: checksum(&payload),
                    },
                    payload,
                };
                let mut buf = BytesMut::new();
                encode_frame(&frame, &mut buf).unwrap();
                buf.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
                buf.to_vec()
            },
            {
                let mut v = vec![FRAME_MAGIC];
                v.extend(0u8..64);
                v
            },
        ];

        let mut request_seeds: Vec<Vec<u8>> = vec![
            vec![],
            vec![0x01],
            1u16.to_le_bytes().to_vec(),
            2u16.to_le_bytes().to_vec(),
            116u16.to_le_bytes().to_vec(),
            117u16.to_le_bytes().to_vec(),
            {
                let mut v = 0xFFFFu16.to_le_bytes().to_vec();
                v.extend_from_slice(&[0u8; 8]);
                v
            },
            {
                let mut v = 1u16.to_le_bytes().to_vec();
                v.extend_from_slice(&100u16.to_le_bytes());
                v.extend_from_slice(b"ab");
                v
            },
            vec![0xff; 64],
            vec![0u8; 256],
            {
                // length-prefixed "flexible-ish" fields
                let mut v = 3u16.to_le_bytes().to_vec();
                v.extend_from_slice(&5u16.to_le_bytes());
                v.extend_from_slice(b"topic");
                v.extend_from_slice(&3u32.to_le_bytes());
                v.extend_from_slice(&0u16.to_le_bytes());
                v
            },
            {
                let mut v = 1u16.to_le_bytes().to_vec();
                v.extend_from_slice(&0xFFFFu16.to_le_bytes());
                v.push(b'x');
                v
            },
        ];

        // Load on-disk corpus when present (workspace root = two levels above crate).
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .ok();
        if let Some(root) = workspace {
            for (subdir, sink) in [
                ("fuzz/corpus/decode_frame", &mut frame_seeds),
                ("fuzz/corpus/decode_request", &mut request_seeds),
            ] {
                let dir = root.join(subdir);
                if let Ok(entries) = std::fs::read_dir(&dir) {
                    for ent in entries.flatten() {
                        let path = ent.path();
                        if path.is_file() {
                            if let Ok(bytes) = std::fs::read(&path) {
                                sink.push(bytes);
                            }
                        }
                    }
                }
            }
            // v0.15: membership/txn seeds also replay through opcode LE + payload.
            let ext = root.join("fuzz/corpus/decode_extended");
            if let Ok(entries) = std::fs::read_dir(&ext) {
                for ent in entries.flatten() {
                    let path = ent.path();
                    if path.is_file() {
                        if let Ok(bytes) = std::fs::read(&path) {
                            request_seeds.push(bytes);
                        }
                    }
                }
            }
        }

        assert!(
            !frame_seeds.is_empty() && !request_seeds.is_empty(),
            "corpus smoke requires built-in seeds"
        );

        for seed in &frame_seeds {
            smoke_frame(seed);
        }
        for seed in &request_seeds {
            smoke_request(seed);
        }

        // Explicit MAX_PAYLOAD+1 rejection still non-panicking.
        let huge = vec![0u8; MAX_PAYLOAD + 1];
        assert!(decode_request(1, &huge).is_err());
        assert!(decode_response(1, &huge).is_err());
    }

    /// v0.15: replay membership (100–107) + txn (32/50/52) seeds the same way
    /// as `fuzz/fuzz_targets/decode_extended.rs`. No nightly / cargo-fuzz.
    #[test]
    fn corpus_smoke_extended() {
        use std::path::PathBuf;

        const FOCUS: &[u16] = &[32, 50, 52, 100, 101, 102, 103, 104, 105, 106, 107];

        fn smoke_extended(data: &[u8]) {
            if !data.is_empty() {
                let op = FOCUS[(data[0] as usize) % FOCUS.len()];
                let payload = &data[1..];
                let _ = decode_request(op, payload);
                let _ = decode_response(op, payload);
            }
            if data.len() >= 2 {
                let opcode = u16::from_le_bytes([data[0], data[1]]);
                let payload = &data[2..];
                let _ = decode_request(opcode, payload);
                let _ = decode_response(opcode, payload);
            } else if data.len() == 1 {
                let opcode = u16::from_le_bytes([data[0], 0]);
                let _ = decode_request(opcode, &[]);
                let _ = decode_response(opcode, &[]);
            }
        }

        let mut seeds: Vec<Vec<u8>> = vec![
            vec![],
            vec![0x64],
            32u16.to_le_bytes().to_vec(),
            50u16.to_le_bytes().to_vec(),
            52u16.to_le_bytes().to_vec(),
            100u16.to_le_bytes().to_vec(),
            102u16.to_le_bytes().to_vec(),
            104u16.to_le_bytes().to_vec(),
            106u16.to_le_bytes().to_vec(),
            {
                let mut v = 32u16.to_le_bytes().to_vec();
                v.extend_from_slice(&5u16.to_le_bytes());
                v.extend_from_slice(b"txn-1");
                v
            },
            {
                let mut v = 50u16.to_le_bytes().to_vec();
                v.extend_from_slice(&7u64.to_le_bytes());
                v.extend_from_slice(&1u16.to_le_bytes());
                v
            },
            {
                let mut v = 52u16.to_le_bytes().to_vec();
                v.extend_from_slice(&7u64.to_le_bytes());
                v.extend_from_slice(&1u16.to_le_bytes());
                v.push(1);
                v.extend_from_slice(&0u32.to_le_bytes());
                v
            },
            {
                // MembershipPut gen=1, one broker, truncated host
                let mut v = 100u16.to_le_bytes().to_vec();
                v.extend_from_slice(&1u64.to_le_bytes());
                v.extend_from_slice(&1u32.to_le_bytes());
                v.extend_from_slice(&1u32.to_le_bytes());
                v.extend_from_slice(&0xFFFFu16.to_le_bytes());
                v.push(b'x');
                v
            },
            vec![0xff; 64],
            vec![0u8; 256],
        ];

        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .ok();
        if let Some(root) = workspace {
            let dir = root.join("fuzz/corpus/decode_extended");
            if let Ok(entries) = std::fs::read_dir(&dir) {
                for ent in entries.flatten() {
                    let path = ent.path();
                    if path.is_file() {
                        if let Ok(bytes) = std::fs::read(&path) {
                            seeds.push(bytes);
                        }
                    }
                }
            }
        }

        assert!(!seeds.is_empty(), "extended corpus smoke requires seeds");
        for seed in &seeds {
            smoke_extended(seed);
        }
        for op in FOCUS {
            let _ = decode_request(*op, &[]);
            let _ = decode_request(*op, &[0xff; 16]);
            let _ = decode_response(*op, &[]);
            let _ = decode_response(*op, &[0xff; 16]);
        }
        let huge = vec![0u8; MAX_PAYLOAD + 1];
        assert!(decode_request(100, &huge).is_err());
        assert!(decode_response(101, &huge).is_err());
    }

    #[test]
    fn phase113_replica_delete_records_roundtrip() {
        let req = Request::ReplicaDeleteRecords {
            topic: "events".into(),
            partition: 2,
            before_offset: 100,
            leader_epoch: 7,
        };
        let bytes = encode_request(&req).unwrap();
        assert_eq!(req.opcode(), RequestOpcode::ReplicaDeleteRecords as u16);
        assert_eq!(
            decode_request(RequestOpcode::ReplicaDeleteRecords as u16, &bytes).unwrap(),
            req
        );

        let resp = Response::ReplicaDeleteRecords {
            error_code: 0,
            low_watermark: 96,
        };
        let rb = encode_response(&resp).unwrap();
        assert_eq!(resp.opcode(), ResponseOpcode::ReplicaDeleteRecords as u16);
        assert_eq!(
            decode_response(ResponseOpcode::ReplicaDeleteRecords as u16, &rb).unwrap(),
            resp
        );
    }

    #[test]
    fn phase113_cluster_broker_config_roundtrip() {
        let req = Request::ClusterBrokerConfig {
            generation: 42,
            entries: vec![
                ("transaction.max.timeout.ms".into(), "120000".into()),
                ("volant.sweep.interval.ms".into(), "".into()), // DELETE
            ],
        };
        let bytes = encode_request(&req).unwrap();
        assert_eq!(req.opcode(), RequestOpcode::ClusterBrokerConfig as u16);
        assert_eq!(
            decode_request(RequestOpcode::ClusterBrokerConfig as u16, &bytes).unwrap(),
            req
        );

        // Empty entries is valid.
        let empty = Request::ClusterBrokerConfig {
            generation: 1,
            entries: vec![],
        };
        let b = encode_request(&empty).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ClusterBrokerConfig as u16, &b).unwrap(),
            empty
        );

        let resp = Response::ClusterBrokerConfig {
            error_code: 0,
            applied_generation: 42,
        };
        let rb = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::ClusterBrokerConfig as u16, &rb).unwrap(),
            resp
        );
    }

    #[test]
    fn phase113_cluster_acl_snapshot_roundtrip() {
        let req = Request::ClusterAclSnapshot {
            generation: 9,
            snapshot: Bytes::from_static(br#"{"version":1,"entries":[]}"#),
        };
        let bytes = encode_request(&req).unwrap();
        assert_eq!(req.opcode(), RequestOpcode::ClusterAclSnapshot as u16);
        assert_eq!(
            decode_request(RequestOpcode::ClusterAclSnapshot as u16, &bytes).unwrap(),
            req
        );

        let empty_snap = Request::ClusterAclSnapshot {
            generation: 0,
            snapshot: Bytes::new(),
        };
        let b = encode_request(&empty_snap).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ClusterAclSnapshot as u16, &b).unwrap(),
            empty_snap
        );

        let resp = Response::ClusterAclSnapshot {
            error_code: 0,
            applied_generation: 9,
        };
        let rb = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::ClusterAclSnapshot as u16, &rb).unwrap(),
            resp
        );
    }

    #[test]
    fn phase113_truncated_bodies_error() {
        assert!(decode_request(RequestOpcode::ReplicaDeleteRecords as u16, &[]).is_err());
        assert!(decode_request(RequestOpcode::ClusterBrokerConfig as u16, &[]).is_err());
        assert!(decode_request(RequestOpcode::ClusterAclSnapshot as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::ReplicaDeleteRecords as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::ClusterBrokerConfig as u16, &[0]).is_err());
        assert!(decode_response(ResponseOpcode::ClusterAclSnapshot as u16, &[0, 0]).is_err());
    }

    #[test]
    fn phase129_truncate_journal_opcodes_roundtrip() {
        let note = Request::TruncateJournalNote {
            topic: "events".into(),
            partition: 3,
            before_offset: 128,
            leader_epoch: 2,
        };
        let nb = encode_request(&note).unwrap();
        assert_eq!(note.opcode(), RequestOpcode::TruncateJournalNote as u16);
        assert_eq!(
            decode_request(RequestOpcode::TruncateJournalNote as u16, &nb).unwrap(),
            note
        );
        let nr = Response::TruncateJournalNote {
            error_code: 0,
            generation: 42,
        };
        let nrb = encode_response(&nr).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::TruncateJournalNote as u16, &nrb).unwrap(),
            nr
        );

        let push = Request::TruncateJournalPush {
            generation: 7,
            snapshot: Bytes::from_static(b"{\"version\":1}"),
        };
        let pb = encode_request(&push).unwrap();
        assert_eq!(push.opcode(), RequestOpcode::TruncateJournalPush as u16);
        assert_eq!(
            decode_request(RequestOpcode::TruncateJournalPush as u16, &pb).unwrap(),
            push
        );
        let empty_push = Request::TruncateJournalPush {
            generation: 0,
            snapshot: Bytes::new(),
        };
        let epb = encode_request(&empty_push).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::TruncateJournalPush as u16, &epb).unwrap(),
            empty_push
        );

        let pr = Response::TruncateJournalPush { error_code: 14 };
        let prb = encode_response(&pr).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::TruncateJournalPush as u16, &prb).unwrap(),
            pr
        );

        // Truncated bodies.
        assert!(decode_request(RequestOpcode::TruncateJournalNote as u16, &[]).is_err());
        assert!(decode_request(RequestOpcode::TruncateJournalPush as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::TruncateJournalNote as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::TruncateJournalNote as u16, &[0, 0]).is_err());
        assert!(decode_response(ResponseOpcode::TruncateJournalPush as u16, &[]).is_err());
    }

    #[test]
    fn phase138_fetch_session_mirror_opcodes_roundtrip() {
        let put = Request::FetchSessionMirrorPut {
            session_id: 42,
            snapshot: Bytes::from_static(b"{\"id\":42,\"epoch\":1}"),
        };
        let pb = encode_request(&put).unwrap();
        assert_eq!(put.opcode(), RequestOpcode::FetchSessionMirrorPut as u16);
        assert_eq!(
            decode_request(RequestOpcode::FetchSessionMirrorPut as u16, &pb).unwrap(),
            put
        );
        let empty_put = Request::FetchSessionMirrorPut {
            session_id: 0,
            snapshot: Bytes::new(),
        };
        let epb = encode_request(&empty_put).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::FetchSessionMirrorPut as u16, &epb).unwrap(),
            empty_put
        );

        let del = Request::FetchSessionMirrorDelete { session_id: -7 };
        let db = encode_request(&del).unwrap();
        assert_eq!(del.opcode(), RequestOpcode::FetchSessionMirrorDelete as u16);
        assert_eq!(
            decode_request(RequestOpcode::FetchSessionMirrorDelete as u16, &db).unwrap(),
            del
        );

        let pr = Response::FetchSessionMirrorPut { error_code: 0 };
        let prb = encode_response(&pr).unwrap();
        assert_eq!(pr.opcode(), ResponseOpcode::FetchSessionMirrorPut as u16);
        assert_eq!(
            decode_response(ResponseOpcode::FetchSessionMirrorPut as u16, &prb).unwrap(),
            pr
        );
        let dr = Response::FetchSessionMirrorDelete { error_code: 14 };
        let drb = encode_response(&dr).unwrap();
        assert_eq!(dr.opcode(), ResponseOpcode::FetchSessionMirrorDelete as u16);
        assert_eq!(
            decode_response(ResponseOpcode::FetchSessionMirrorDelete as u16, &drb).unwrap(),
            dr
        );

        assert!(decode_request(RequestOpcode::FetchSessionMirrorPut as u16, &[]).is_err());
        assert!(decode_request(RequestOpcode::FetchSessionMirrorDelete as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::FetchSessionMirrorPut as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::FetchSessionMirrorDelete as u16, &[]).is_err());
    }

    #[test]
    fn phase150_assignment_consensus_note_roundtrip() {
        let req = Request::AssignmentConsensusNote {
            generation: 7,
            controller_id: 1,
            topics: vec![ClusterTopicState {
                name: "ac".into(),
                topic_id: 3,
                partitions: vec![ClusterPartitionState {
                    partition_id: 0,
                    leader: 1,
                    leader_epoch: 2,
                    replicas: vec![1, 2, 3],
                    isr: vec![1, 2],
                }],
            }],
        };
        assert_eq!(req.opcode(), RequestOpcode::AssignmentConsensusNote as u16);
        let bytes = encode_request(&req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::AssignmentConsensusNote as u16, &bytes).unwrap(),
            req
        );

        let resp = Response::AssignmentConsensusNote {
            error_code: 0,
            generation: 7,
        };
        assert_eq!(
            resp.opcode(),
            ResponseOpcode::AssignmentConsensusNote as u16
        );
        let rb = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::AssignmentConsensusNote as u16, &rb).unwrap(),
            resp
        );

        assert!(decode_request(RequestOpcode::AssignmentConsensusNote as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::AssignmentConsensusNote as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::AssignmentConsensusNote as u16, &[0, 0]).is_err());
    }

    #[test]
    fn phase154_metadata_raft_append_roundtrip() {
        use crate::request::{metadata_raft_cmd, MetadataRaftLogEntry};
        let req = Request::MetadataRaftAppend {
            leader_id: 1,
            term: 3,
            prev_log_index: 1,
            prev_log_term: 2,
            entries: vec![MetadataRaftLogEntry {
                term: 3,
                index: 2,
                command_kind: metadata_raft_cmd::SET_ASSIGNMENT,
                generation: 5,
                topics: vec![ClusterTopicState {
                    name: "mraft".into(),
                    topic_id: 9,
                    partitions: vec![ClusterPartitionState {
                        partition_id: 0,
                        leader: 1,
                        leader_epoch: 0,
                        replicas: vec![1, 2],
                        isr: vec![1],
                    }],
                }],
            }],
            leader_commit: 1,
        };
        assert_eq!(req.opcode(), RequestOpcode::MetadataRaftAppend as u16);
        let bytes = encode_request(&req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::MetadataRaftAppend as u16, &bytes).unwrap(),
            req
        );

        let resp = Response::MetadataRaftAppend {
            term: 3,
            success: 1,
            match_index: 2,
        };
        assert_eq!(resp.opcode(), ResponseOpcode::MetadataRaftAppend as u16);
        let rb = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::MetadataRaftAppend as u16, &rb).unwrap(),
            resp
        );

        assert!(decode_request(RequestOpcode::MetadataRaftAppend as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::MetadataRaftAppend as u16, &[]).is_err());
    }

    #[test]
    fn phase142_isr_update_roundtrip() {
        let req = Request::IsrUpdate {
            topic: "p142".into(),
            partition: 0,
            leader_id: 2,
            leader_epoch: 3,
            isr: vec![2, 3],
            generation_hint: 7,
        };
        let bytes = encode_request(&req).unwrap();
        assert_eq!(req.opcode(), RequestOpcode::IsrUpdate as u16);
        assert_eq!(
            decode_request(RequestOpcode::IsrUpdate as u16, &bytes).unwrap(),
            req
        );

        let resp = Response::IsrUpdate {
            error_code: 0,
            generation: 42,
        };
        let rb = encode_response(&resp).unwrap();
        assert_eq!(resp.opcode(), ResponseOpcode::IsrUpdate as u16);
        assert_eq!(
            decode_response(ResponseOpcode::IsrUpdate as u16, &rb).unwrap(),
            resp
        );

        assert!(decode_request(RequestOpcode::IsrUpdate as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::IsrUpdate as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::IsrUpdate as u16, &[0, 0]).is_err());
    }

    #[test]
    fn phase114_txn_participant_opcodes_roundtrip() {
        let open = Request::TxnParticipantOpen {
            transactional_id: "app-1".into(),
            producer_id: 7,
            producer_epoch: 1,
            enable_2pc: true,
            coordinator_node_id: 1,
            install_open: true,
        };
        let b = encode_request(&open).unwrap();
        assert_eq!(open.opcode(), RequestOpcode::TxnParticipantOpen as u16);
        assert_eq!(
            decode_request(RequestOpcode::TxnParticipantOpen as u16, &b).unwrap(),
            open
        );
        let or = Response::TxnParticipantOpen { error_code: 0 };
        let orb = encode_response(&or).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::TxnParticipantOpen as u16, &orb).unwrap(),
            or
        );

        let prep = Request::TxnParticipantPrepare {
            transactional_id: "app-1".into(),
            producer_id: 7,
            producer_epoch: 1,
            commit: true,
        };
        let pb = encode_request(&prep).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::TxnParticipantPrepare as u16, &pb).unwrap(),
            prep
        );
        let pr = Response::TxnParticipantPrepare { error_code: 22 };
        let prb = encode_response(&pr).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::TxnParticipantPrepare as u16, &prb).unwrap(),
            pr
        );

        let done = Request::TxnParticipantComplete {
            transactional_id: "app-1".into(),
            producer_id: 7,
            producer_epoch: 1,
            commit: false,
        };
        let db = encode_request(&done).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::TxnParticipantComplete as u16, &db).unwrap(),
            done
        );
        let dr = Response::TxnParticipantComplete { error_code: 0 };
        let drb = encode_response(&dr).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::TxnParticipantComplete as u16, &drb).unwrap(),
            dr
        );

        assert!(decode_request(RequestOpcode::TxnParticipantOpen as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::TxnParticipantPrepare as u16, &[]).is_err());
    }

    #[test]
    fn phase120_kafka_txn_forward_roundtrip() {
        let req = Request::KafkaTxnForward {
            api_key: 26,
            api_version: 0,
            principal: "alice".into(),
            body: Bytes::from_static(b"endtxn-body"),
        };
        let b = encode_request(&req).unwrap();
        assert_eq!(req.opcode(), RequestOpcode::KafkaTxnForward as u16);
        assert_eq!(
            decode_request(RequestOpcode::KafkaTxnForward as u16, &b).unwrap(),
            req
        );
        let resp = Response::KafkaTxnForward {
            error_code: 0,
            body: Bytes::from_static(b"resp"),
        };
        let rb = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::KafkaTxnForward as u16, &rb).unwrap(),
            resp
        );
        assert!(decode_request(RequestOpcode::KafkaTxnForward as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::KafkaTxnForward as u16, &[]).is_err());

        // Legacy open body (no Phase 120 trailer) still decodes with defaults.
        let mut legacy = BytesMut::new();
        put_string(&mut legacy, "t").unwrap();
        legacy.put_u64_le(9);
        legacy.put_u16_le(2);
        legacy.put_u8(1);
        let decoded =
            decode_request(RequestOpcode::TxnParticipantOpen as u16, &legacy.freeze()).unwrap();
        match decoded {
            Request::TxnParticipantOpen {
                coordinator_node_id,
                install_open,
                enable_2pc,
                ..
            } => {
                assert_eq!(coordinator_node_id, 0);
                assert!(install_open);
                assert!(enable_2pc);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn v10_membership_opcodes_roundtrip() {
        use crate::request::MembershipBroker;
        let put = Request::MembershipPut {
            generation: 3,
            brokers: vec![
                MembershipBroker {
                    id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                },
                MembershipBroker {
                    id: 2,
                    host: "127.0.0.1".into(),
                    port: 9093,
                    rack: Some("r1".into()),
                },
            ],
        };
        let b = encode_request(&put).unwrap();
        assert_eq!(put.opcode(), RequestOpcode::MembershipPut as u16);
        assert_eq!(
            decode_request(RequestOpcode::MembershipPut as u16, &b).unwrap(),
            put
        );
        let pr = Response::MembershipPut {
            error_code: 0,
            applied_generation: 3,
        };
        let prb = encode_response(&pr).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::MembershipPut as u16, &prb).unwrap(),
            pr
        );

        let add = Request::AddBroker {
            id: 3,
            host: "10.0.0.3".into(),
            port: 9094,
            rack: Some("r2".into()),
        };
        let ab = encode_request(&add).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::AddBroker as u16, &ab).unwrap(),
            add
        );
        let ar = Response::AddBroker {
            error_code: 0,
            generation: 4,
        };
        let arb = encode_response(&ar).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::AddBroker as u16, &arb).unwrap(),
            ar
        );

        let rem = Request::RemoveBroker { id: 3 };
        let rb = encode_request(&rem).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::RemoveBroker as u16, &rb).unwrap(),
            rem
        );
        let rr = Response::RemoveBroker {
            error_code: 3,
            generation: 0,
        };
        let rrb = encode_response(&rr).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::RemoveBroker as u16, &rrb).unwrap(),
            rr
        );

        let list = Request::ListMembers;
        let lb = encode_request(&list).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ListMembers as u16, &lb).unwrap(),
            list
        );
        let lr = Response::ListMembers {
            error_code: 0,
            generation: 4,
            brokers: vec![MembershipBroker {
                id: 1,
                host: "127.0.0.1".into(),
                port: 9092,
                rack: None,
            }],
            live: vec![1, 2],
        };
        let lrb = encode_response(&lr).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::ListMembers as u16, &lrb).unwrap(),
            lr
        );

        assert!(decode_request(RequestOpcode::MembershipPut as u16, &[]).is_err());
        assert!(decode_request(RequestOpcode::AddBroker as u16, &[]).is_err());
        assert!(decode_request(RequestOpcode::RemoveBroker as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::MembershipPut as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::AddBroker as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::RemoveBroker as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::ListMembers as u16, &[]).is_err());
    }

    #[test]
    fn v18_reassign_opcodes_roundtrip() {
        let req = Request::ReassignPartitions {
            topic: "events".into(),
            partition: u32::MAX,
            replicas: vec![1, 2, 3],
        };
        let b = encode_request(&req).unwrap();
        assert_eq!(req.opcode(), RequestOpcode::ReassignPartitions as u16);
        assert_eq!(
            decode_request(RequestOpcode::ReassignPartitions as u16, &b).unwrap(),
            req
        );
        let auto = Request::ReassignPartitions {
            topic: "events".into(),
            partition: 0,
            replicas: vec![],
        };
        let ab = encode_request(&auto).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::ReassignPartitions as u16, &ab).unwrap(),
            auto
        );
        let resp = Response::ReassignPartitions {
            error_code: 0,
            generation: 7,
        };
        let rb = encode_response(&resp).unwrap();
        assert_eq!(resp.opcode(), ResponseOpcode::ReassignPartitions as u16);
        assert_eq!(
            decode_response(ResponseOpcode::ReassignPartitions as u16, &rb).unwrap(),
            resp
        );
        assert!(decode_request(RequestOpcode::ReassignPartitions as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::ReassignPartitions as u16, &[]).is_err());
    }

    #[test]
    fn v206_sync_group_opcodes_roundtrip() {
        let req = Request::SyncGroup {
            group_id: "g1".into(),
            member_id: "m1".into(),
            generation: 3,
            assignment_bytes: Bytes::new(),
        };
        let b = encode_request(&req).unwrap();
        assert_eq!(req.opcode(), RequestOpcode::SyncGroup as u16);
        assert_eq!(
            decode_request(RequestOpcode::SyncGroup as u16, &b).unwrap(),
            req
        );
        let with_bytes = Request::SyncGroup {
            group_id: "g1".into(),
            member_id: "m1".into(),
            generation: 3,
            assignment_bytes: Bytes::from_static(b"ignored"),
        };
        let wb = encode_request(&with_bytes).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::SyncGroup as u16, &wb).unwrap(),
            with_bytes
        );
        let resp = Response::SyncGroup {
            error_code: 0,
            assignment: vec![Assignment {
                topic: "events".into(),
                partition: 2,
            }],
        };
        let rb = encode_response(&resp).unwrap();
        assert_eq!(resp.opcode(), ResponseOpcode::SyncGroup as u16);
        assert_eq!(
            decode_response(ResponseOpcode::SyncGroup as u16, &rb).unwrap(),
            resp
        );
        let empty = Response::SyncGroup {
            error_code: 10,
            assignment: vec![],
        };
        let eb = encode_response(&empty).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::SyncGroup as u16, &eb).unwrap(),
            empty
        );
        assert!(decode_request(RequestOpcode::SyncGroup as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::SyncGroup as u16, &[]).is_err());
    }

    #[test]
    fn openraft_rpc_roundtrip() {
        let req = Request::OpenraftAppend {
            payload: Bytes::from_static(b"{\"term\":1}"),
        };
        let b = encode_request(&req).unwrap();
        assert_eq!(req.opcode(), RequestOpcode::OpenraftAppend as u16);
        assert_eq!(
            decode_request(RequestOpcode::OpenraftAppend as u16, &b).unwrap(),
            req
        );
        let vote = Request::OpenraftVote {
            payload: Bytes::from_static(b"{\"term\":2}"),
        };
        let vb = encode_request(&vote).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::OpenraftVote as u16, &vb).unwrap(),
            vote
        );
        let resp = Response::OpenraftAppend {
            payload: Bytes::from_static(b"{\"success\":true}"),
        };
        let rb = encode_response(&resp).unwrap();
        assert_eq!(resp.opcode(), ResponseOpcode::OpenraftAppend as u16);
        assert_eq!(
            decode_response(ResponseOpcode::OpenraftAppend as u16, &rb).unwrap(),
            resp
        );
        let vr = Response::OpenraftVote {
            payload: Bytes::from_static(b"{\"vote_granted\":true}"),
        };
        let vrb = encode_response(&vr).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::OpenraftVote as u16, &vrb).unwrap(),
            vr
        );
        assert!(decode_request(RequestOpcode::OpenraftAppend as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::OpenraftVote as u16, &[]).is_err());

        let snap = Request::OpenraftInstallSnapshot {
            payload: Bytes::from_static(b"{\"done\":true}"),
        };
        let sb = encode_request(&snap).unwrap();
        assert_eq!(snap.opcode(), RequestOpcode::OpenraftInstallSnapshot as u16);
        assert_eq!(
            decode_request(RequestOpcode::OpenraftInstallSnapshot as u16, &sb).unwrap(),
            snap
        );
        let sr = Response::OpenraftInstallSnapshot {
            payload: Bytes::from_static(b"{\"vote\":{}}"),
        };
        let srb = encode_response(&sr).unwrap();
        assert_eq!(sr.opcode(), ResponseOpcode::OpenraftInstallSnapshot as u16);
        assert_eq!(
            decode_response(ResponseOpcode::OpenraftInstallSnapshot as u16, &srb).unwrap(),
            sr
        );
        assert!(decode_request(RequestOpcode::OpenraftInstallSnapshot as u16, &[]).is_err());
        assert!(decode_response(ResponseOpcode::OpenraftInstallSnapshot as u16, &[]).is_err());
    }
}
