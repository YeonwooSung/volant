//! Big-endian Kafka wire primitives, MessageSet (magic 0/1), and RecordBatch (magic 2).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher as Crc32Ieee;
use volant_core::{Error, Message, Record, Result};

use super::compress::{self, CompressionCodec};

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

/// Write response header v1 (flexible): correlation_id + empty TAG_BUFFER.
///
/// Used for flexible API responses **except** ApiVersions (always header v0).
pub fn put_response_header_v1(dst: &mut BytesMut, correlation_id: i32) {
    dst.put_i32(correlation_id);
    put_empty_tag_buffer(dst);
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

/// Read compact nullable bytes / records (`uvarint(0)` = null; else `uvarint(len+1)+bytes`).
///
/// Used for flexible `bytes` and `records` fields (KIP-482).
pub fn get_compact_bytes(src: &mut impl Buf) -> Result<Option<Bytes>> {
    let n = read_unsigned_varint(src)?;
    if n == 0 {
        return Ok(None);
    }
    let len = (n - 1) as usize;
    if src.remaining() < len {
        return Err(Error::Protocol("truncated compact bytes body".into()));
    }
    let mut buf = vec![0u8; len];
    src.copy_to_slice(&mut buf);
    Ok(Some(Bytes::from(buf)))
}

/// Write compact nullable bytes / records.
pub fn put_compact_bytes(dst: &mut BytesMut, b: Option<&[u8]>) {
    match b {
        None => put_unsigned_varint(dst, 0),
        Some(v) => {
            put_unsigned_varint(dst, (v.len() as u32).saturating_add(1));
            dst.extend_from_slice(v);
        }
    }
}

/// Producer identity fields from a RecordBatch header (Phase 29).
///
/// Kafka uses `-1` for all three when the batch is non-idempotent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordBatchProducer {
    /// Kafka producer id (`-1` = none).
    pub producer_id: i64,
    /// Producer epoch (`-1` = none).
    pub producer_epoch: i16,
    /// Base sequence of the first record (`-1` = none).
    pub base_sequence: i32,
}

impl RecordBatchProducer {
    /// Non-idempotent marker (all fields `-1`).
    pub const NONE: Self = Self {
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
    };

    /// Whether this batch carries idempotent produce fields.
    pub fn is_idempotent(self) -> bool {
        self.producer_id >= 0 && self.base_sequence >= 0
    }
}

/// One decoded RecordBatch (messages + producer meta).
#[derive(Debug, Clone)]
pub struct DecodedRecordBatch {
    /// Producer id / epoch / base sequence from the batch header.
    pub producer: RecordBatchProducer,
    /// Records in this batch.
    pub messages: Vec<Message>,
}

/// Decode a produce record set, auto-detecting MessageSet vs RecordBatch.
///
/// Magic byte lives at offset 16 in both formats.
pub fn decode_records(data: &[u8]) -> Result<Vec<Message>> {
    Ok(decode_produce_batches(data)?
        .into_iter()
        .flat_map(|b| b.messages)
        .collect())
}

/// Decode a produce record set into per-batch units (Phase 29).
///
/// MessageSet payloads yield a single batch with [`RecordBatchProducer::NONE`].
pub fn decode_produce_batches(data: &[u8]) -> Result<Vec<DecodedRecordBatch>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    if data.len() < 17 {
        return Err(Error::Protocol("kafka records too short".into()));
    }
    let magic = data[16] as i8;
    match magic {
        0 | 1 => {
            let messages = decode_message_set(data)?;
            Ok(vec![DecodedRecordBatch {
                producer: RecordBatchProducer::NONE,
                messages,
            }])
        }
        2 => decode_record_batches_detailed(data),
        other => Err(Error::Protocol(format!(
            "unsupported kafka records magic {other}"
        ))),
    }
}

/// Decode a Kafka MessageSet (magic 0 or 1) into Volant messages.
///
/// Honors compressed wrapper messages (Phase 33): attributes bits 0–2 ≠ 0 means
/// `value` is a compressed nested MessageSet.
pub fn decode_message_set(data: &[u8]) -> Result<Vec<Message>> {
    decode_message_set_depth(data, 0)
}

const MAX_MESSAGE_SET_COMPRESSION_DEPTH: u8 = 3;

fn decode_message_set_depth(data: &[u8], depth: u8) -> Result<Vec<Message>> {
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
        let attributes = m.get_i8();
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

        // Phase 33: compressed wrapper → nested MessageSet in value.
        let codec = CompressionCodec::from_attributes(i16::from(attributes & 0x07))?;
        if codec != CompressionCodec::None {
            if depth >= MAX_MESSAGE_SET_COMPRESSION_DEPTH {
                return Err(Error::Protocol(
                    "message set compression nesting too deep".into(),
                ));
            }
            let plain = compress::decompress(codec, &value)?;
            let nested = decode_message_set_depth(&plain, depth + 1)?;
            out.extend(nested);
            continue;
        }

        out.push(Message {
            key,
            value,
            timestamp_ms,
            headers: vec![],
        });
    }
    Ok(out)
}

/// Encode records as a Kafka MessageSet (magic 1, uncompressed).
pub fn encode_message_set(records: &[Record]) -> BytesMut {
    encode_message_set_inner(records)
}

fn encode_message_set_inner(records: &[Record]) -> BytesMut {
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

/// Encode a compressed MessageSet wrapper (Phase 33).
///
/// Classic MessageSet has no zstd; [`CompressionCodec::Zstd`] is mapped to lz4.
/// Codec [`CompressionCodec::None`] delegates to [`encode_message_set`].
pub fn encode_message_set_compressed(
    records: &[Record],
    codec: CompressionCodec,
) -> Result<BytesMut> {
    if records.is_empty() {
        return Ok(BytesMut::new());
    }
    let codec = match codec {
        CompressionCodec::None => return Ok(encode_message_set_inner(records)),
        // MessageSet never had zstd; keep fetch env usable.
        CompressionCodec::Zstd => CompressionCodec::Lz4,
        other => other,
    };

    let inner = encode_message_set_inner(records);
    let compressed = compress::compress(codec, &inner)?;
    let last_offset = records.last().map(|r| r.offset.raw() as i64).unwrap_or(0);
    let max_ts = records
        .iter()
        .map(|r| r.timestamp_ms)
        .max()
        .unwrap_or(0);

    let mut msg = BytesMut::new();
    msg.put_i8(1); // magic
    msg.put_i8(codec.as_u8() as i8); // attributes bits 0–2
    msg.put_i64(max_ts);
    put_bytes(&mut msg, None); // null key
    put_bytes(&mut msg, Some(&compressed));

    let mut hasher = Crc32Ieee::new();
    hasher.update(&msg);
    let crc = hasher.finalize() as i32;

    let mut out = BytesMut::new();
    out.put_i64(last_offset);
    out.put_i32((4 + msg.len()) as i32);
    out.put_i32(crc);
    out.extend_from_slice(&msg);
    Ok(out)
}

/// Decode one or more contiguous RecordBatches (magic 2).
pub fn decode_record_batches(data: &[u8]) -> Result<Vec<Message>> {
    Ok(decode_record_batches_detailed(data)?
        .into_iter()
        .flat_map(|b| b.messages)
        .collect())
}

/// Decode contiguous RecordBatches with producer metadata (Phase 29).
pub fn decode_record_batches_detailed(data: &[u8]) -> Result<Vec<DecodedRecordBatch>> {
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
        out.push(decode_one_record_batch(batch_body)?);
    }
    Ok(out)
}

fn decode_one_record_batch(body: &[u8]) -> Result<DecodedRecordBatch> {
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
    let codec = CompressionCodec::from_attributes(attributes)?;
    let _last_offset_delta = src.get_i32();
    let first_timestamp = src.get_i64();
    let _max_timestamp = src.get_i64();
    let producer_id = src.get_i64();
    let producer_epoch = src.get_i16();
    let base_sequence = src.get_i32();
    let records_count = src.get_i32();
    if records_count < 0 {
        return Err(Error::Protocol("negative records count".into()));
    }

    // When compressed, remaining bytes are one compressed blob of DefaultRecords.
    let records_bytes = if codec == CompressionCodec::None {
        let rem = src.remaining();
        src.copy_to_bytes(rem)
    } else {
        let rem = src.remaining();
        let compressed = src.copy_to_bytes(rem);
        let plain = compress::decompress(codec, &compressed)?;
        Bytes::from(plain)
    };

    let mut rec_src = records_bytes;
    let mut messages = Vec::with_capacity(records_count as usize);
    for _ in 0..records_count {
        messages.push(decode_default_record(&mut rec_src, first_timestamp)?);
    }
    Ok(DecodedRecordBatch {
        producer: RecordBatchProducer {
            producer_id,
            producer_epoch,
            base_sequence,
        },
        messages,
    })
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
///
/// Used when Fetch compression is disabled or as a fallback.
pub fn encode_record_batch(records: &[Record]) -> BytesMut {
    encode_record_batch_with_options(records, CompressionCodec::None, RecordBatchProducer::NONE)
        .expect("uncompressed encode cannot fail")
}

/// Encode records as a RecordBatch with the given compression codec (Phases 28/32).
///
/// Used for compressed Produce test payloads and Fetch v4 responses.
pub fn encode_record_batch_compressed(
    records: &[Record],
    codec: CompressionCodec,
) -> Result<BytesMut> {
    encode_record_batch_with_options(records, codec, RecordBatchProducer::NONE)
}

/// Encode an idempotent RecordBatch (Phase 29) for tests / tooling.
pub fn encode_record_batch_idempotent(
    records: &[Record],
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> BytesMut {
    encode_record_batch_with_options(
        records,
        CompressionCodec::None,
        RecordBatchProducer {
            producer_id,
            producer_epoch,
            base_sequence,
        },
    )
    .expect("uncompressed idempotent encode cannot fail")
}

fn encode_record_batch_with_options(
    records: &[Record],
    codec: CompressionCodec,
    producer: RecordBatchProducer,
) -> Result<BytesMut> {
    if records.is_empty() {
        return Ok(BytesMut::new());
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

    let records_payload = if codec == CompressionCodec::None {
        records_buf.freeze()
    } else {
        Bytes::from(compress::compress(codec, &records_buf)?)
    };

    // Attributes through end of records (CRC payload).
    let mut crc_payload = BytesMut::new();
    crc_payload.put_i16(i16::from(codec.as_u8())); // attributes bits 0–2
    crc_payload.put_i32(last_offset_delta);
    crc_payload.put_i64(first_timestamp);
    crc_payload.put_i64(max_timestamp);
    crc_payload.put_i64(producer.producer_id);
    crc_payload.put_i16(producer.producer_epoch);
    crc_payload.put_i32(producer.base_sequence);
    crc_payload.put_i32(records.len() as i32);
    crc_payload.extend_from_slice(&records_payload);

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
    Ok(out)
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

/// Decode Kafka consumer protocol subscription metadata → topic names.
///
/// Layout: `version:i16 | topics:[string] | user_data:bytes`.
pub fn decode_consumer_subscription(data: &[u8]) -> Result<Vec<String>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let mut src = data;
    if src.remaining() < 2 {
        return Err(Error::Protocol("truncated consumer subscription".into()));
    }
    let _version = src.get_i16();
    if src.remaining() < 4 {
        return Err(Error::Protocol("truncated subscription topics".into()));
    }
    let n = src.get_i32();
    if n < 0 {
        return Err(Error::Protocol("negative subscription topic count".into()));
    }
    let mut topics = Vec::with_capacity(n as usize);
    for _ in 0..n {
        topics.push(get_string(&mut src)?);
    }
    // user_data optional / ignored
    let _ = get_bytes(&mut src);
    Ok(topics)
}

/// Encode Kafka consumer protocol member assignment bytes.
///
/// Layout: `version:i16 | [topic [partition:i32]] | user_data:bytes`.
pub fn encode_consumer_assignment(assignment: &[(String, u32)]) -> BytesMut {
    use std::collections::BTreeMap;
    let mut by_topic: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
    for (topic, part) in assignment {
        by_topic.entry(topic.as_str()).or_default().push(*part as i32);
    }
    let mut out = BytesMut::new();
    out.put_i16(0); // version
    out.put_i32(by_topic.len() as i32);
    for (topic, parts) in by_topic {
        put_string(&mut out, topic);
        out.put_i32(parts.len() as i32);
        for p in parts {
            out.put_i32(p);
        }
    }
    put_bytes(&mut out, Some(&[])); // empty user_data
    out
}

/// Decode Kafka consumer protocol member assignment (tests / tooling).
pub fn decode_consumer_assignment(data: &[u8]) -> Result<Vec<(String, u32)>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let mut src = data;
    if src.remaining() < 2 + 4 {
        return Err(Error::Protocol("truncated member assignment".into()));
    }
    let _version = src.get_i16();
    let n = src.get_i32();
    if n < 0 {
        return Err(Error::Protocol("negative assignment topic count".into()));
    }
    let mut out = Vec::new();
    for _ in 0..n {
        let topic = get_string(&mut src)?;
        if src.remaining() < 4 {
            return Err(Error::Protocol("truncated assignment partitions".into()));
        }
        let pc = src.get_i32();
        for _ in 0..pc.max(0) {
            if src.remaining() < 4 {
                return Err(Error::Protocol("truncated assignment partition id".into()));
            }
            let p = src.get_i32();
            out.push((topic.clone(), p as u32));
        }
    }
    Ok(out)
}

/// Encode Kafka consumer subscription metadata (tests / tooling).
pub fn encode_consumer_subscription(topics: &[&str]) -> BytesMut {
    let mut out = BytesMut::new();
    out.put_i16(0); // version
    out.put_i32(topics.len() as i32);
    for t in topics {
        put_string(&mut out, t);
    }
    put_bytes(&mut out, Some(&[]));
    out
}

/// Build a size-prefixed Kafka request for tests / tooling (classic header).
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

/// Build a size-prefixed flexible request (RequestHeader v2 + body).
///
/// ClientId stays classic nullable string; header ends with an empty TAG_BUFFER.
pub fn encode_request_flexible(
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
    // ClientId is never compact in the request header (Kafka special case).
    put_nullable_string(&mut inner, client_id);
    put_empty_tag_buffer(&mut inner);
    inner.extend_from_slice(body);
    let mut out = BytesMut::with_capacity(4 + inner.len());
    out.put_i32(inner.len() as i32);
    out.extend_from_slice(&inner);
    out
}

// ─── Flexible / compact encoding (KIP-482) ───────────────────────────────────

/// Read an unsigned varint (protobuf-style, no zig-zag).
pub fn read_unsigned_varint(src: &mut impl Buf) -> Result<u32> {
    let mut shift = 0u32;
    let mut result = 0u32;
    loop {
        if !src.has_remaining() {
            return Err(Error::Protocol("truncated unsigned varint".into()));
        }
        let b = src.get_u8();
        result |= u32::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift > 28 {
            return Err(Error::Protocol("unsigned varint too long".into()));
        }
    }
}

/// Write an unsigned varint.
pub fn put_unsigned_varint(dst: &mut BytesMut, mut value: u32) {
    loop {
        if (value & !0x7f) == 0 {
            dst.put_u8(value as u8);
            break;
        }
        dst.put_u8(((value & 0x7f) | 0x80) as u8);
        value >>= 7;
    }
}

/// Write a compact (non-null) string: `uvarint(len+1) + bytes`.
pub fn put_compact_string(dst: &mut BytesMut, s: &str) {
    let b = s.as_bytes();
    put_unsigned_varint(dst, (b.len() as u32).saturating_add(1));
    dst.extend_from_slice(b);
}

/// Write a compact nullable string (`uvarint(0)` = null).
pub fn put_compact_nullable_string(dst: &mut BytesMut, s: Option<&str>) {
    match s {
        None => put_unsigned_varint(dst, 0),
        Some(v) => put_compact_string(dst, v),
    }
}

/// Read a compact (non-null) string.
pub fn get_compact_string(src: &mut impl Buf) -> Result<String> {
    let n = read_unsigned_varint(src)?;
    if n == 0 {
        return Err(Error::Protocol("unexpected null compact string".into()));
    }
    let len = (n - 1) as usize;
    if src.remaining() < len {
        return Err(Error::Protocol("truncated compact string body".into()));
    }
    let mut buf = vec![0u8; len];
    src.copy_to_slice(&mut buf);
    String::from_utf8(buf).map_err(|e| Error::Protocol(format!("compact string utf8: {e}")))
}

/// Read a compact nullable string.
pub fn get_compact_nullable_string(src: &mut impl Buf) -> Result<Option<String>> {
    let n = read_unsigned_varint(src)?;
    if n == 0 {
        return Ok(None);
    }
    let len = (n - 1) as usize;
    if src.remaining() < len {
        return Err(Error::Protocol("truncated compact nullable string".into()));
    }
    let mut buf = vec![0u8; len];
    src.copy_to_slice(&mut buf);
    let s = String::from_utf8(buf)
        .map_err(|e| Error::Protocol(format!("compact string utf8: {e}")))?;
    Ok(Some(s))
}

/// Write compact array length (`uvarint(n+1)`; `0` = null).
pub fn put_compact_array_len(dst: &mut BytesMut, n: usize) {
    put_unsigned_varint(dst, (n as u32).saturating_add(1));
}

/// Read compact array length. Returns `None` for null array.
pub fn get_compact_array_len(src: &mut impl Buf) -> Result<Option<usize>> {
    let n = read_unsigned_varint(src)?;
    if n == 0 {
        return Ok(None);
    }
    Ok(Some((n - 1) as usize))
}

/// Write an empty TAG_BUFFER (`uvarint(0)`).
pub fn put_empty_tag_buffer(dst: &mut BytesMut) {
    put_unsigned_varint(dst, 0);
}

/// Skip a TAG_BUFFER (any number of tagged fields).
pub fn skip_tag_buffer(src: &mut impl Buf) -> Result<()> {
    let n = read_unsigned_varint(src)?;
    for _ in 0..n {
        let _tag = read_unsigned_varint(src)?;
        let len = read_unsigned_varint(src)? as usize;
        if src.remaining() < len {
            return Err(Error::Protocol("truncated tagged field body".into()));
        }
        src.advance(len);
    }
    Ok(())
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
    fn unsigned_varint_roundtrip() {
        let cases = [0u32, 1, 127, 128, 255, 300, 16_383, 16_384, u32::MAX / 2];
        for v in cases {
            let mut buf = BytesMut::new();
            put_unsigned_varint(&mut buf, v);
            let mut src = buf.freeze();
            assert_eq!(read_unsigned_varint(&mut src).unwrap(), v);
            assert_eq!(src.remaining(), 0);
        }
    }

    #[test]
    fn compact_string_and_array_roundtrip() {
        let mut buf = BytesMut::new();
        put_compact_string(&mut buf, "hello");
        put_compact_nullable_string(&mut buf, None);
        put_compact_nullable_string(&mut buf, Some(""));
        put_compact_array_len(&mut buf, 3);
        put_empty_tag_buffer(&mut buf);

        let mut src = buf.freeze();
        assert_eq!(get_compact_string(&mut src).unwrap(), "hello");
        assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
        assert_eq!(get_compact_nullable_string(&mut src).unwrap().as_deref(), Some(""));
        assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(3));
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.remaining(), 0);
    }

    #[test]
    fn compact_bytes_roundtrip() {
        let mut buf = BytesMut::new();
        put_compact_bytes(&mut buf, None);
        put_compact_bytes(&mut buf, Some(&[]));
        put_compact_bytes(&mut buf, Some(b"abc"));
        let mut src = buf.freeze();
        assert_eq!(get_compact_bytes(&mut src).unwrap(), None);
        assert_eq!(get_compact_bytes(&mut src).unwrap().unwrap().as_ref(), b"");
        assert_eq!(get_compact_bytes(&mut src).unwrap().unwrap().as_ref(), b"abc");
        assert_eq!(src.remaining(), 0);
    }

    #[test]
    fn flexible_request_header_has_tag_buffer() {
        let body = encode_request_flexible(18, 3, 9, Some("flex"), &[]);
        let mut buf = body;
        let req = try_decode_request(&mut buf).unwrap().unwrap();
        let mut src = req;
        let hdr = decode_request_header(&mut src).unwrap();
        assert_eq!(hdr.api_key, 18);
        assert_eq!(hdr.api_version, 3);
        assert_eq!(hdr.client_id.as_deref(), Some("flex"));
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.remaining(), 0);
    }

    #[test]
    fn response_header_v1_has_tag_buffer() {
        let mut buf = BytesMut::new();
        put_response_header_v1(&mut buf, 42);
        let mut src = buf.freeze();
        assert_eq!(src.get_i32(), 42);
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.remaining(), 0);
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
    fn compressed_message_set_roundtrips_all_codecs() {
        use crate::kafka::compress::CompressionCodec;
        let records = vec![
            Record {
                offset: Offset::new(0),
                key: Some(Bytes::from_static(b"k")),
                value: Bytes::from(b"payload-one ".repeat(40)),
                timestamp_ms: 1000,
                headers: vec![],
            },
            Record {
                offset: Offset::new(1),
                key: None,
                value: Bytes::from_static(b"payload-two"),
                timestamp_ms: 2000,
                headers: vec![],
            },
        ];
        for codec in [
            CompressionCodec::None,
            CompressionCodec::Gzip,
            CompressionCodec::Snappy,
            CompressionCodec::Lz4,
            CompressionCodec::Zstd, // maps to lz4 on encode
        ] {
            let set = encode_message_set_compressed(&records, codec).unwrap();
            if codec != CompressionCodec::None {
                // Wrapper: magic at 16, attributes at 17
                assert_eq!(set[16] as i8, 1);
                let attrs = set[17] as i8;
                let expected = if codec == CompressionCodec::Zstd {
                    CompressionCodec::Lz4.as_u8()
                } else {
                    codec.as_u8()
                };
                assert_eq!(attrs & 0x07, expected as i8, "codec={codec:?}");
            }
            let decoded = decode_message_set(&set).unwrap();
            assert_eq!(decoded.len(), 2, "codec={codec:?}");
            assert_eq!(decoded[0].key.as_ref().unwrap().as_ref(), b"k");
            assert_eq!(decoded[0].value.as_ref(), records[0].value.as_ref());
            assert_eq!(decoded[1].value.as_ref(), b"payload-two");
        }
    }

    #[test]
    fn consumer_subscription_assignment_roundtrip() {
        let sub = encode_consumer_subscription(&["orders", "payments"]);
        let topics = decode_consumer_subscription(&sub).unwrap();
        assert_eq!(topics, vec!["orders".to_string(), "payments".to_string()]);

        let asg = encode_consumer_assignment(&[
            ("orders".into(), 0),
            ("orders".into(), 1),
            ("payments".into(), 0),
        ]);
        let decoded = decode_consumer_assignment(&asg).unwrap();
        assert_eq!(decoded.len(), 3);
        assert!(decoded.contains(&("orders".into(), 0)));
        assert!(decoded.contains(&("payments".into(), 0)));
    }

    #[test]
    fn compressed_record_batch_roundtrips_all_codecs() {
        use crate::kafka::compress::CompressionCodec;

        let records = vec![
            Record {
                offset: Offset::new(0),
                key: Some(Bytes::from_static(b"ck")),
                value: Bytes::from_static(b"compressed-value-payload-0123456789"),
                timestamp_ms: 1_700_000_000_000,
                headers: vec![("h".into(), Bytes::from_static(b"hv"))],
            },
            Record {
                offset: Offset::new(1),
                key: None,
                value: Bytes::from_static(b"second"),
                timestamp_ms: 1_700_000_000_100,
                headers: vec![],
            },
        ];
        for codec in [
            CompressionCodec::None,
            CompressionCodec::Gzip,
            CompressionCodec::Snappy,
            CompressionCodec::Lz4,
            CompressionCodec::Zstd,
        ] {
            let batch = encode_record_batch_compressed(&records, codec).unwrap();
            assert_eq!(batch[16] as i8, 2);
            let decoded = decode_records(&batch).unwrap();
            assert_eq!(decoded.len(), 2, "codec={codec:?}");
            assert_eq!(decoded[0].key.as_ref().unwrap().as_ref(), b"ck");
            assert_eq!(
                decoded[0].value.as_ref(),
                b"compressed-value-payload-0123456789"
            );
            assert_eq!(decoded[0].headers.len(), 1);
            assert_eq!(decoded[1].value.as_ref(), b"second");
        }
    }

    #[test]
    fn idempotent_record_batch_preserves_producer_fields() {
        let records = vec![Record {
            offset: Offset::new(0),
            key: Some(Bytes::from_static(b"k")),
            value: Bytes::from_static(b"v"),
            timestamp_ms: 1_700_000_000_000,
            headers: vec![],
        }];
        let batch = encode_record_batch_idempotent(&records, 7, 3, 11);
        let detailed = decode_record_batches_detailed(&batch).unwrap();
        assert_eq!(detailed.len(), 1);
        assert_eq!(detailed[0].producer.producer_id, 7);
        assert_eq!(detailed[0].producer.producer_epoch, 3);
        assert_eq!(detailed[0].producer.base_sequence, 11);
        assert!(detailed[0].producer.is_idempotent());
        assert_eq!(detailed[0].messages[0].value.as_ref(), b"v");
    }

    #[test]
    fn unknown_compression_codec_rejected() {
        let mut crc_payload = BytesMut::new();
        crc_payload.put_i16(5); // attributes: invalid codec
        crc_payload.put_i32(0);
        crc_payload.put_i64(0);
        crc_payload.put_i64(0);
        crc_payload.put_i64(-1);
        crc_payload.put_i16(-1);
        crc_payload.put_i32(-1);
        crc_payload.put_i32(0);
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
        assert!(
            msg.contains("unsupported") || msg.contains("compression"),
            "{msg}"
        );
    }
}
