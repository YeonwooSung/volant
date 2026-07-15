//! Big-endian Kafka wire primitives, MessageSet (magic 0/1), and RecordBatch (magic 2).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher as Crc32Ieee;
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

/// Decode a produce record set, auto-detecting MessageSet vs RecordBatch.
///
/// Magic byte lives at offset 16 in both formats.
pub fn decode_records(data: &[u8]) -> Result<Vec<Message>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    if data.len() < 17 {
        return Err(Error::Protocol("kafka records too short".into()));
    }
    let magic = data[16] as i8;
    match magic {
        0 | 1 => decode_message_set(data),
        2 => decode_record_batches(data),
        other => Err(Error::Protocol(format!(
            "unsupported kafka records magic {other}"
        ))),
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
        let mut h = Crc32Ieee::new();
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
                "unsupported kafka message magic {magic}"
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

        let mut hasher = Crc32Ieee::new();
        hasher.update(&msg);
        let crc = hasher.finalize() as i32;

        out.put_i64(r.offset.raw() as i64);
        out.put_i32((4 + msg.len()) as i32);
        out.put_i32(crc);
        out.extend_from_slice(&msg);
    }
    out
}

/// Decode one or more contiguous RecordBatches (magic 2).
pub fn decode_record_batches(data: &[u8]) -> Result<Vec<Message>> {
    let mut src = data;
    let mut out = Vec::new();
    // Minimum empty batch: baseOffset(8)+batchLength(4)+header(~49) ≈ 61.
    while src.remaining() >= 61 {
        let _base_offset = src.get_i64();
        let batch_length = src.get_i32();
        if batch_length < 0 {
            return Err(Error::Protocol("negative record batch length".into()));
        }
        let batch_length = batch_length as usize;
        // batch_length covers from partitionLeaderEpoch to end.
        if src.remaining() < batch_length {
            break; // partial trailing batch
        }
        let batch_body = &src[..batch_length];
        src.advance(batch_length);
        out.extend(decode_one_record_batch(batch_body)?);
    }
    Ok(out)
}

fn decode_one_record_batch(body: &[u8]) -> Result<Vec<Message>> {
    // body starts at partitionLeaderEpoch
    if body.len() < 49 {
        return Err(Error::Protocol("truncated record batch header".into()));
    }
    let mut src = body;
    let _leader_epoch = src.get_i32();
    let magic = src.get_i8();
    if magic != 2 {
        return Err(Error::Protocol(format!(
            "expected record batch magic 2, got {magic}"
        )));
    }
    let crc = src.get_u32();
    // CRC-32C over attributes through end of batch.
    let crc_payload = &body[body.len() - src.remaining()..];
    let computed = crc32c::crc32c(crc_payload);
    if computed != crc {
        return Err(Error::Protocol(format!(
            "record batch crc32c mismatch: got {crc:#x} want {computed:#x}"
        )));
    }

    let attributes = src.get_i16();
    let compression = attributes & 0x07;
    if compression != 0 {
        return Err(Error::Protocol(format!(
            "compressed record batch not supported (attributes={attributes:#x})"
        )));
    }
    let _last_offset_delta = src.get_i32();
    let first_timestamp = src.get_i64();
    let _max_timestamp = src.get_i64();
    let _producer_id = src.get_i64();
    let _producer_epoch = src.get_i16();
    let _base_sequence = src.get_i32();
    let records_count = src.get_i32();
    if records_count < 0 {
        return Err(Error::Protocol("negative records count".into()));
    }

    let mut out = Vec::with_capacity(records_count as usize);
    for _ in 0..records_count {
        out.push(decode_default_record(&mut src, first_timestamp)?);
    }
    Ok(out)
}

fn decode_default_record(src: &mut impl Buf, first_timestamp: i64) -> Result<Message> {
    let length = read_varint(src)?;
    if length < 0 {
        return Err(Error::Protocol("negative record length".into()));
    }
    if src.remaining() < length as usize {
        return Err(Error::Protocol("truncated record body".into()));
    }
    // We don't need length strictly if we parse fields; still bound-check.
    let _attributes = src.get_i8();
    let timestamp_delta = read_varint(src)?;
    let _offset_delta = read_varint(src)?;
    let key_len = read_varint(src)?;
    let key = if key_len < 0 {
        None
    } else {
        Some(read_exact(src, key_len as usize)?)
    };
    let value_len = read_varint(src)?;
    let value = if value_len < 0 {
        Bytes::new()
    } else {
        read_exact(src, value_len as usize)?
    };
    let header_count = read_varint(src)?;
    if header_count < 0 {
        return Err(Error::Protocol("negative header count".into()));
    }
    let mut headers = Vec::with_capacity(header_count as usize);
    for _ in 0..header_count {
        let hk_len = read_varint(src)?;
        if hk_len < 0 {
            return Err(Error::Protocol("negative header key len".into()));
        }
        let hk = read_exact(src, hk_len as usize)?;
        let hk = String::from_utf8(hk.to_vec())
            .map_err(|e| Error::Protocol(format!("header key utf8: {e}")))?;
        let hv_len = read_varint(src)?;
        let hv = if hv_len < 0 {
            Bytes::new()
        } else {
            read_exact(src, hv_len as usize)?
        };
        headers.push((hk, hv));
    }
    Ok(Message {
        key,
        value,
        timestamp_ms: Some(first_timestamp.saturating_add(timestamp_delta as i64)),
        headers,
    })
}

/// Encode records as a single Kafka RecordBatch (magic 2, no compression).
pub fn encode_record_batch(records: &[Record]) -> BytesMut {
    if records.is_empty() {
        return BytesMut::new();
    }
    let base_offset = records[0].offset.raw() as i64;
    let first_timestamp = records[0].timestamp_ms;
    let max_timestamp = records
        .iter()
        .map(|r| r.timestamp_ms)
        .max()
        .unwrap_or(first_timestamp);
    let last_offset_delta = (records.len() as i32).saturating_sub(1);

    let mut records_buf = BytesMut::new();
    for (i, r) in records.iter().enumerate() {
        encode_default_record(
            &mut records_buf,
            r,
            first_timestamp,
            i as i32,
        );
    }

    // Attributes through end of records (CRC payload).
    let mut crc_payload = BytesMut::new();
    crc_payload.put_i16(0); // attributes
    crc_payload.put_i32(last_offset_delta);
    crc_payload.put_i64(first_timestamp);
    crc_payload.put_i64(max_timestamp);
    crc_payload.put_i64(-1); // producerId
    crc_payload.put_i16(-1); // producerEpoch
    crc_payload.put_i32(-1); // baseSequence
    crc_payload.put_i32(records.len() as i32);
    crc_payload.extend_from_slice(&records_buf);

    let crc = crc32c::crc32c(&crc_payload);

    // batchLength = from partitionLeaderEpoch to end
    // partitionLeaderEpoch(4)+magic(1)+crc(4)+crc_payload
    let batch_length = (4 + 1 + 4 + crc_payload.len()) as i32;

    let mut out = BytesMut::new();
    out.put_i64(base_offset);
    out.put_i32(batch_length);
    out.put_i32(-1); // partitionLeaderEpoch
    out.put_i8(2); // magic
    out.put_u32(crc);
    out.extend_from_slice(&crc_payload);
    out
}

fn encode_default_record(
    dst: &mut BytesMut,
    r: &Record,
    first_timestamp: i64,
    offset_delta: i32,
) {
    let mut body = BytesMut::new();
    body.put_i8(0); // attributes
    write_varint(&mut body, (r.timestamp_ms - first_timestamp) as i32);
    write_varint(&mut body, offset_delta);
    match &r.key {
        None => write_varint(&mut body, -1),
        Some(k) => {
            write_varint(&mut body, k.len() as i32);
            body.extend_from_slice(k);
        }
    }
    write_varint(&mut body, r.value.len() as i32);
    body.extend_from_slice(&r.value);
    write_varint(&mut body, r.headers.len() as i32);
    for (k, v) in &r.headers {
        write_varint(&mut body, k.len() as i32);
        body.extend_from_slice(k.as_bytes());
        write_varint(&mut body, v.len() as i32);
        body.extend_from_slice(v);
    }
    write_varint(dst, body.len() as i32);
    dst.extend_from_slice(&body);
}

fn read_exact(src: &mut impl Buf, len: usize) -> Result<Bytes> {
    if src.remaining() < len {
        return Err(Error::Protocol("truncated bytes in record".into()));
    }
    let mut buf = vec![0u8; len];
    src.copy_to_slice(&mut buf);
    Ok(Bytes::from(buf))
}

/// Zig-zag varint (Kafka / protobuf style) — signed i32.
fn read_varint(src: &mut impl Buf) -> Result<i32> {
    let mut shift = 0u32;
    let mut result = 0u32;
    loop {
        if !src.has_remaining() {
            return Err(Error::Protocol("truncated varint".into()));
        }
        let b = src.get_u8();
        result |= u32::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 35 {
            return Err(Error::Protocol("varint too long".into()));
        }
    }
    // zig-zag decode
    Ok(((result >> 1) as i32) ^ (-((result & 1) as i32)))
}

fn write_varint(dst: &mut BytesMut, n: i32) {
    let mut value = ((n << 1) ^ (n >> 31)) as u32;
    loop {
        if (value & !0x7f) == 0 {
            dst.put_u8(value as u8);
            break;
        }
        dst.put_u8(((value & 0x7f) | 0x80) as u8);
        value >>= 7;
    }
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

    #[test]
    fn record_batch_roundtrip_keys_values_headers() {
        let records = vec![
            Record {
                offset: Offset::new(10),
                key: Some(Bytes::from_static(b"k1")),
                value: Bytes::from_static(b"v1"),
                timestamp_ms: 1_700_000_000_000,
                headers: vec![("h".into(), Bytes::from_static(b"hv"))],
            },
            Record {
                offset: Offset::new(11),
                key: None,
                value: Bytes::from_static(b"v2"),
                timestamp_ms: 1_700_000_000_050,
                headers: vec![],
            },
        ];
        let batch = encode_record_batch(&records);
        assert_eq!(batch[16] as i8, 2); // magic
        let decoded = decode_records(&batch).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].key.as_ref().unwrap().as_ref(), b"k1");
        assert_eq!(decoded[0].value.as_ref(), b"v1");
        assert_eq!(decoded[0].timestamp_ms, Some(1_700_000_000_000));
        assert_eq!(decoded[0].headers.len(), 1);
        assert_eq!(decoded[0].headers[0].0, "h");
        assert_eq!(decoded[0].headers[0].1.as_ref(), b"hv");
        assert!(decoded[1].key.is_none());
        assert_eq!(decoded[1].value.as_ref(), b"v2");
        assert_eq!(decoded[1].timestamp_ms, Some(1_700_000_000_050));
    }

    #[test]
    fn decode_records_auto_detects_message_set() {
        let records = vec![Record {
            offset: Offset::new(0),
            key: Some(Bytes::from_static(b"a")),
            value: Bytes::from_static(b"b"),
            timestamp_ms: 99,
            headers: vec![],
        }];
        let set = encode_message_set(&records);
        assert!(matches!(set[16] as i8, 0 | 1));
        let decoded = decode_records(&set).unwrap();
        assert_eq!(decoded[0].value.as_ref(), b"b");
    }

    #[test]
    fn compressed_record_batch_rejected() {
        // Minimal hand-built batch with attributes compression = 1 (gzip).
        let mut crc_payload = BytesMut::new();
        crc_payload.put_i16(1); // attributes: gzip
        crc_payload.put_i32(0); // lastOffsetDelta
        crc_payload.put_i64(0); // firstTimestamp
        crc_payload.put_i64(0); // maxTimestamp
        crc_payload.put_i64(-1);
        crc_payload.put_i16(-1);
        crc_payload.put_i32(-1);
        crc_payload.put_i32(0); // records count
        let crc = crc32c::crc32c(&crc_payload);
        let mut batch = BytesMut::new();
        batch.put_i64(0);
        batch.put_i32((4 + 1 + 4 + crc_payload.len()) as i32);
        batch.put_i32(-1);
        batch.put_i8(2);
        batch.put_u32(crc);
        batch.extend_from_slice(&crc_payload);
        let err = decode_record_batches(&batch).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("compressed"), "{msg}");
    }
}
