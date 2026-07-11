//! Little-endian payload encode/decode for Phase 2 request/response bodies.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_core::{Error, Result};

use crate::request::{ProduceMessage, Request, RequestOpcode};
use crate::response::{
    BrokerInfo, ErrorCode, FetchRecord, PartitionInfo, Response, ResponseOpcode, TopicInfo,
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
        Request::OffsetCommit | Request::OffsetFetch => {
            return Err(Error::Protocol("offset APIs reserved for Phase 3".into()));
        }
    }
    if dst.len() > MAX_PAYLOAD {
        return Err(Error::Protocol(format!(
            "payload too large: {} > {MAX_PAYLOAD}",
            dst.len()
        )));
    }
    Ok(dst.freeze())
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
        RequestOpcode::OffsetCommit => Ok(Request::OffsetCommit),
        RequestOpcode::OffsetFetch => Ok(Request::OffsetFetch),
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
                }
            }
        }
        Response::OffsetCommit | Response::OffsetFetch => {
            return Err(Error::Protocol("offset APIs reserved for Phase 3".into()));
        }
        Response::Error { code, message } => {
            dst.put_u16_le(*code);
            put_string(&mut dst, message)?;
        }
    }
    if dst.len() > MAX_PAYLOAD {
        return Err(Error::Protocol(format!(
            "payload too large: {} > {MAX_PAYLOAD}",
            dst.len()
        )));
    }
    Ok(dst.freeze())
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
                    if src.remaining() < 4 + 4 + 8 {
                        return Err(Error::Protocol("truncated partition info".into()));
                    }
                    partitions.push(PartitionInfo {
                        partition_id: src.get_u32_le(),
                        leader: src.get_u32_le(),
                        hwm: src.get_u64_le(),
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
        ResponseOpcode::OffsetCommit => Ok(Response::OffsetCommit),
        ResponseOpcode::OffsetFetch => Ok(Response::OffsetFetch),
        ResponseOpcode::Error => {
            if src.remaining() < 2 {
                return Err(Error::Protocol("truncated error code".into()));
            }
            let code = src.get_u16_le();
            let message = get_string(&mut src)?;
            // Validate known mapping without failing on unknown codes.
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
    use crate::request::ProduceMessage;

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
}
