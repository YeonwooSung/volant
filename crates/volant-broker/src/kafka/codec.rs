//! Big-endian Kafka wire primitives and MessageSet (magic 0/1).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher;
use volant_core::{Error, Message, Record, Result};

/// Read a full Kafka request frame (size-prefixed) from a buffer.
///
/// Returns `None` if more bytes are needed. On success, returns the request
/// body (header + payload) without the 4-byte size prefix.
pub fn try_decode_request(buf: &mut BytesMut) -> Result<Option<Bytes>> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if size < 0 {
        return Err(Error::Protocol(format!("negative kafka frame size {size}")));
    }
    let size = size as usize;
    if size > 16 * 1024 * 1024 {
        return Err(Error::Protocol(format!("kafka frame too large: {size}")));
    }
    if buf.len() < 4 + size {
        return Ok(None);
    }
    let _ = buf.split_to(4);
    let body = buf.split_to(size).freeze();
    Ok(Some(body))
}

/// Encode a response body with a 4-byte big-endian size prefix.
pub fn encode_response_frame(body: &[u8]) -> BytesMut {
    let mut out = BytesMut::with_capacity(4 + body.len());
    out.put_i32(body.len() as i32);
    out.extend_from_slice(body);
    out
}

/// Parsed request header (v0/v1 non-flexible).
#[derive(Debug, Clone)]
pub struct RequestHeader {
    /// Kafka API key.
    pub api_key: i16,
    /// Requested API version.
    pub api_version: i16,
    /// Correlation id echoed in the response.
    pub correlation_id: i32,
    /// Optional client id string.
    pub client_id: Option<String>,
}

/// Decode request header from the start of a request body.
pub fn decode_request_header(src: &mut impl Buf) -> Result<RequestHeader> {
    if src.remaining() < 2 + 2 + 4 {
        return Err(Error::Protocol("truncated kafka request header".into()));
    }
    let api_key = src.get_i16();
    let api_version = src.get_i16();
    let correlation_id = src.get_i32();
    let client_id = get_nullable_string(src)?;
    Ok(RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    })
}

/// Write response header v0 (correlation_id only).
pub fn put_response_header(dst: &mut BytesMut, correlation_id: i32) {
    dst.put_i32(correlation_id);
}

/// Write a non-null Kafka string (`i16` length + UTF-8 bytes).
pub fn put_string(dst: &mut BytesMut, s: &str) {
    let b = s.as_bytes();
    dst.put_i16(b.len() as i16);
    dst.extend_from_slice(b);
}

/// Write a nullable Kafka string (`-1` length for null).
pub fn put_nullable_string(dst: &mut BytesMut, s: Option<&str>) {
    match s {
        None => dst.put_i16(-1),
        Some(v) => put_string(dst, v),
    }
}

/// Read a non-null Kafka string.
pub fn get_string(src: &mut impl Buf) -> Result<String> {
    if src.remaining() < 2 {
        return Err(Error::Protocol("truncated kafka string len".into()));
    }
    let len = src.get_i16();
    if len < 0 {
        return Err(Error::Protocol("unexpected null kafka string".into()));
    }
    let len = len as usize;
    if src.remaining() < len {
        return Err(Error::Protocol("truncated kafka string body".into()));
    }
    let mut buf = vec![0u8; len];
    src.copy_to_slice(&mut buf);
    String::from_utf8(buf).map_err(|e| Error::Protocol(format!("kafka string utf8: {e}")))
}

/// Read a nullable Kafka string.
pub fn get_nullable_string(src: &mut impl Buf) -> Result<Option<String>> {
    if src.remaining() < 2 {
        return Err(Error::Protocol("truncated kafka nullable string len".into()));
    }
    let len = src.get_i16();
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if src.remaining() < len {
        return Err(Error::Protocol("truncated kafka nullable string body".into()));
    }
    let mut buf = vec![0u8; len];
    src.copy_to_slice(&mut buf);
    let s = String::from_utf8(buf)
        .map_err(|e| Error::Protocol(format!("kafka string utf8: {e}")))?;
    Ok(Some(s))
}

/// Read nullable Kafka bytes (`i32` length; `-1` = null).
pub fn get_bytes(src: &mut impl Buf) -> Result<Option<Bytes>> {
    if src.remaining() < 4 {
        return Err(Error::Protocol("truncated kafka bytes len".into()));
    }
    let len = src.get_i32();
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if src.remaining() < len {
        return Err(Error::Protocol("truncated kafka bytes body".into()));
    }
    let mut buf = vec![0u8; len];
    src.copy_to_slice(&mut buf);
    Ok(Some(Bytes::from(buf)))
}

/// Write nullable Kafka bytes.
pub fn put_bytes(dst: &mut BytesMut, b: Option<&[u8]>) {
    match b {
        None => dst.put_i32(-1),
        Some(v) => {
            dst.put_i32(v.len() as i32);
            dst.extend_from_slice(v);
        }
    }
}

/// Decode a Kafka MessageSet (magic 0 or 1) into Volant messages.
pub fn decode_message_set(data: &[u8]) -> Result<Vec<Message>> {
    let mut src = data;
    let mut out = Vec::new();
    while src.remaining() >= 12 {
        let _offset = src.get_i64();
        let msg_size = src.get_i32();
        if msg_size < 0 {
            return Err(Error::Protocol("negative message size".into()));
        }
        let msg_size = msg_size as usize;
        if src.remaining() < msg_size {
            // Partial trailing message — stop (common on produce).
            break;
        }
        let msg_bytes = &src[..msg_size];
        src.advance(msg_size);

        if msg_bytes.len() < 6 {
            return Err(Error::Protocol("truncated kafka message".into()));
        }
        let crc = i32::from_be_bytes([
            msg_bytes[0],
            msg_bytes[1],
            msg_bytes[2],
            msg_bytes[3],
        ]) as u32;
        let payload_for_crc = &msg_bytes[4..];
        let mut h = Hasher::new();
        h.update(payload_for_crc);
        let computed = h.finalize();
        if computed != crc {
            return Err(Error::Protocol(format!(
                "kafka message crc mismatch: got {crc:#x} want {computed:#x}"
            )));
        }

        let mut m = payload_for_crc;
        let magic = m.get_i8();
        let _attributes = m.get_i8();
        let timestamp_ms = if magic == 1 {
            if m.remaining() < 8 {
                return Err(Error::Protocol("truncated kafka message timestamp".into()));
            }
            Some(m.get_i64())
        } else if magic == 0 {
            None
        } else {
            return Err(Error::Protocol(format!(
                "unsupported kafka message magic {magic} (MVP supports 0/1 only)"
            )));
        };
        let key = get_bytes(&mut m)?;
        let value = get_bytes(&mut m)?.unwrap_or_default();
        out.push(Message {
            key,
            value,
            timestamp_ms,
            headers: vec![],
        });
    }
    Ok(out)
}

/// Encode records as a Kafka MessageSet (magic 1).
pub fn encode_message_set(records: &[Record]) -> BytesMut {
    let mut out = BytesMut::new();
    for r in records {
        let mut msg = BytesMut::new();
        msg.put_i8(1); // magic
        msg.put_i8(0); // attributes
        msg.put_i64(r.timestamp_ms);
        put_bytes(&mut msg, r.key.as_ref().map(|k| k.as_ref()));
        put_bytes(&mut msg, Some(r.value.as_ref()));

        let mut hasher = Hasher::new();
        hasher.update(&msg);
        let crc = hasher.finalize() as i32;

        out.put_i64(r.offset.raw() as i64);
        out.put_i32((4 + msg.len()) as i32);
        out.put_i32(crc);
        out.extend_from_slice(&msg);
    }
    out
}

/// Build a size-prefixed Kafka request for tests / tooling.
pub fn encode_request(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    body: &[u8],
) -> BytesMut {
    let mut inner = BytesMut::new();
    inner.put_i16(api_key);
    inner.put_i16(api_version);
    inner.put_i32(correlation_id);
    put_nullable_string(&mut inner, client_id);
    inner.extend_from_slice(body);
    let mut out = BytesMut::with_capacity(4 + inner.len());
    out.put_i32(inner.len() as i32);
    out.extend_from_slice(&inner);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use volant_core::Offset;

    #[test]
    fn message_set_roundtrip() {
        let msgs = vec![
            Message {
                key: Some(Bytes::from_static(b"k")),
                value: Bytes::from_static(b"v1"),
                timestamp_ms: Some(1000),
                headers: vec![],
            },
            Message {
                key: None,
                value: Bytes::from_static(b"v2"),
                timestamp_ms: Some(2000),
                headers: vec![],
            },
        ];
        // Encode as records then decode.
        let records: Vec<Record> = msgs
            .iter()
            .enumerate()
            .map(|(i, m)| Record {
                offset: Offset::new(i as u64),
                key: m.key.clone(),
                value: m.value.clone(),
                timestamp_ms: m.timestamp_ms.unwrap_or(0),
                headers: vec![],
            })
            .collect();
        let set = encode_message_set(&records);
        let decoded = decode_message_set(&set).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].key.as_ref().unwrap().as_ref(), b"k");
        assert_eq!(decoded[0].value.as_ref(), b"v1");
        assert_eq!(decoded[1].value.as_ref(), b"v2");
    }

    #[test]
    fn request_frame_roundtrip() {
        let body = encode_request(18, 0, 7, Some("test"), &[]);
        let mut buf = body;
        let req = try_decode_request(&mut buf).unwrap().unwrap();
        let mut src = req;
        let hdr = decode_request_header(&mut src).unwrap();
        assert_eq!(hdr.api_key, 18);
        assert_eq!(hdr.correlation_id, 7);
        assert_eq!(hdr.client_id.as_deref(), Some("test"));
    }
}
