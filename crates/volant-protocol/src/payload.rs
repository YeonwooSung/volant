//! Little-endian payload encode/decode for Phase 2/3 request/response bodies.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_core::{Error, Result};

use crate::request::{
    OffsetCommitEntry, OffsetEntry, ProduceMessage, Request, RequestOpcode,
};
use crate::response::{
    Assignment, BrokerInfo, ClusterPartitionState, ClusterTopicState, ErrorCode, FetchRecord,
    OffsetFetchEntry, PartitionInfo, Response, ResponseOpcode, TopicInfo,
};
use crate::frame::{Frame, FrameHeader, PROTOCOL_VERSION};
use crate::codec::checksum;

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
        return Err(Error::Protocol("unexpected optional null in required bytes".into()));
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
        }
        Request::Fetch {
            topic,
            partition,
            from_offset,
            max_messages,
            max_bytes,
            max_wait_ms,
        } => {
            put_string(&mut dst, topic)?;
            dst.put_u32_le(*partition);
            dst.put_u64_le(*from_offset);
            dst.put_u32_le(*max_messages);
            dst.put_u32_le(*max_bytes);
            dst.put_u32_le(*max_wait_ms);
        }
        Request::CreateTopic { name, partitions } => {
            put_string(&mut dst, name)?;
            dst.put_u32_le(*partitions);
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
        } => {
            put_string(&mut dst, group_id)?;
            put_string(&mut dst, member_id)?;
            dst.put_u32_le(*session_timeout_ms);
            dst.put_u32_le(topics.len() as u32);
            for t in topics {
                put_string(&mut dst, t)?;
            }
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
        } => {
            dst.put_u32_le(*broker_id);
            dst.put_u32_le(*controller_id_known);
            dst.put_u32_le(*generation);
        }
        Request::ClusterState { known_generation } => {
            dst.put_u32_le(*known_generation);
        }
        Request::Auth { token } => {
            put_string(&mut dst, token)?;
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
            Ok(Request::Produce {
                topic,
                partition,
                acks,
                messages,
            })
        }
        RequestOpcode::Fetch => {
            let topic = get_string(&mut src)?;
            if src.remaining() < 4 + 8 + 4 + 4 + 4 {
                return Err(Error::Protocol("truncated fetch request".into()));
            }
            Ok(Request::Fetch {
                topic,
                partition: src.get_u32_le(),
                from_offset: src.get_u64_le(),
                max_messages: src.get_u32_le(),
                max_bytes: src.get_u32_le(),
                max_wait_ms: src.get_u32_le(),
            })
        }
        RequestOpcode::CreateTopic => {
            let name = get_string(&mut src)?;
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated create topic partitions".into()));
            }
            Ok(Request::CreateTopic {
                name,
                partitions: src.get_u32_le(),
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
            Ok(Request::JoinGroup {
                group_id,
                member_id,
                session_timeout_ms,
                topics,
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
            Ok(Request::HeartbeatBroker {
                broker_id: src.get_u32_le(),
                controller_id_known: src.get_u32_le(),
                generation: src.get_u32_le(),
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
        Response::Metadata { brokers, topics } => {
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
        } => {
            dst.put_u16_le(*error_code);
            dst.put_u32_le(*generation);
            put_string(&mut dst, member_id)?;
            dst.put_u32_le(assignment.len() as u32);
            for a in assignment {
                put_string(&mut dst, &a.topic)?;
                dst.put_u32_le(a.partition);
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
            dst.put_u32_le(topics.len() as u32);
            for t in topics {
                put_string(&mut dst, &t.name)?;
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
        }
        Response::Auth { error_code } => {
            dst.put_u16_le(*error_code);
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
            Ok(Response::Metadata { brokers, topics })
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
                return Err(Error::Protocol("truncated join group assignment count".into()));
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
            Ok(Response::JoinGroup {
                error_code,
                generation,
                member_id,
                assignment,
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
                return Err(Error::Protocol("truncated heartbeat broker response".into()));
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
            if src.remaining() < 2 + 4 + 4 + 4 {
                return Err(Error::Protocol("truncated cluster state response".into()));
            }
            let error_code = src.get_u16_le();
            let generation = src.get_u32_le();
            let controller_id = src.get_u32_le();
            let topic_count = src.get_u32_le() as usize;
            let mut topics = Vec::with_capacity(topic_count);
            for _ in 0..topic_count {
                let name = get_string(&mut src)?;
                if src.remaining() < 4 + 4 {
                    return Err(Error::Protocol("truncated cluster topic header".into()));
                }
                let topic_id = src.get_u32_le();
                let partition_count = src.get_u32_le() as usize;
                let mut partitions = Vec::with_capacity(partition_count);
                for _ in 0..partition_count {
                    if src.remaining() < 4 + 4 + 4 + 4 {
                        return Err(Error::Protocol("truncated cluster partition".into()));
                    }
                    let partition_id = src.get_u32_le();
                    let leader = src.get_u32_le();
                    let leader_epoch = src.get_u32_le();
                    let replica_count = src.get_u32_le() as usize;
                    let mut replicas = Vec::with_capacity(replica_count);
                    for _ in 0..replica_count {
                        if src.remaining() < 4 {
                            return Err(Error::Protocol("truncated cluster replica".into()));
                        }
                        replicas.push(src.get_u32_le());
                    }
                    if src.remaining() < 4 {
                        return Err(Error::Protocol("truncated cluster isr count".into()));
                    }
                    let isr_count = src.get_u32_le() as usize;
                    let mut isr = Vec::with_capacity(isr_count);
                    for _ in 0..isr_count {
                        if src.remaining() < 4 {
                            return Err(Error::Protocol("truncated cluster isr".into()));
                        }
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
        };
        let bytes = encode_request(&req).unwrap();
        let decoded = decode_request(RequestOpcode::Produce as u16, &bytes).unwrap();
        assert_eq!(decoded, req);
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
        };
        let b = encode_request(&fetch).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::Fetch as u16, &b).unwrap(),
            fetch
        );

        let create = Request::CreateTopic {
            name: "t".into(),
            partitions: 3,
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
        };
        let b = encode_response(&resp).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::Metadata as u16, &b).unwrap(),
            resp
        );
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
        };
        let b = encode_response(&join).unwrap();
        assert_eq!(
            decode_response(ResponseOpcode::JoinGroup as u16, &b).unwrap(),
            join
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
        };
        let b = encode_request(&req).unwrap();
        assert_eq!(
            decode_request(RequestOpcode::HeartbeatBroker as u16, &b).unwrap(),
            req
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
        assert_eq!(
            ErrorCode::from_u16(13),
            ErrorCode::NotLeaderForPartition
        );
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
        assert_eq!(
            decode_request(RequestOpcode::Auth as u16, &b).unwrap(),
            req
        );

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
        assert_eq!(
            ErrorCode::from_u16(17),
            ErrorCode::AuthenticationFailed
        );
        assert_eq!(
            ErrorCode::from_u16(18),
            ErrorCode::AuthenticationRequired
        );
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
        let req_ops: &[u16] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 22, 24, 30, 99, 0xFFFF];
        let resp_ops: &[u16] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 21, 23, 25, 31, 0xFFFF, 42];

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
}
