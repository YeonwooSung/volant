//! On-disk record encode/decode for segment files.

use bytes::Bytes;
use volant_core::{Error, Message, Offset, Record, Result};

/// Segment file magic number ("VLNT").
pub const MAGIC: u32 = 0x564C_4E54;
/// On-disk format version.
pub const VERSION: u16 = 1;
/// Fixed segment header size in bytes.
pub const HEADER_SIZE: usize = 32;
/// `key_len` value meaning a null (absent) key.
pub const NULL_KEY_LEN: u32 = u32::MAX;

/// Encode a segment file header into a 32-byte buffer.
pub fn encode_header(base_offset: Offset, created_at_ms: i64) -> [u8; HEADER_SIZE] {
    let mut buf = [0u8; HEADER_SIZE];
    buf[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    buf[4..6].copy_from_slice(&VERSION.to_le_bytes());
    buf[6..8].copy_from_slice(&0u16.to_le_bytes()); // flags
    buf[8..16].copy_from_slice(&base_offset.raw().to_le_bytes());
    buf[16..24].copy_from_slice(&created_at_ms.to_le_bytes());
    // bytes 24..32 reserved = 0
    buf
}

/// Decoded segment header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Format version.
    pub version: u16,
    /// Flags bitfield (currently unused).
    pub flags: u16,
    /// Base offset of the segment.
    pub base_offset: Offset,
    /// Segment creation timestamp (unix ms).
    pub created_at_ms: i64,
}

/// Decode and validate a segment header.
pub fn decode_header(buf: &[u8]) -> Result<SegmentHeader> {
    if buf.len() < HEADER_SIZE {
        return Err(Error::Storage(format!(
            "segment header too short: {} bytes",
            buf.len()
        )));
    }
    let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if magic != MAGIC {
        return Err(Error::Storage(format!(
            "bad segment magic: {magic:#x}, expected {MAGIC:#x}"
        )));
    }
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    if version != VERSION {
        return Err(Error::Storage(format!(
            "unsupported segment version: {version}"
        )));
    }
    let flags = u16::from_le_bytes(buf[6..8].try_into().unwrap());
    let base_offset = Offset::new(u64::from_le_bytes(buf[8..16].try_into().unwrap()));
    let created_at_ms = i64::from_le_bytes(buf[16..24].try_into().unwrap());
    Ok(SegmentHeader {
        version,
        flags,
        base_offset,
        created_at_ms,
    })
}

/// Compute the on-disk size of a fully encoded record (including `record_length`).
pub fn encoded_record_size(message: &Message) -> u64 {
    let body = record_body_size(message);
    // record_length (4) + crc (4) + body
    (4 + 4 + body) as u64
}

fn record_body_size(message: &Message) -> usize {
    let key_len = match &message.key {
        Some(k) => k.len(),
        None => 0,
    };
    let key_field = 4 + key_len; // key_len always present; key omitted if null
    // When null key we still write key_len = u32::MAX and omit key bytes.
    let key_field = if message.key.is_none() { 4 } else { key_field };

    let mut headers_size = 4; // header_count
    for (name, value) in &message.headers {
        headers_size += 2 + name.len() + 4 + value.len();
    }

    // offset(8) + timestamp(8) + key + value_len(4) + value + headers
    8 + 8 + key_field + 4 + message.value.len() + headers_size
}

/// Encode a record into `buf`, returning the total bytes written (including length prefix).
pub fn encode_record(
    buf: &mut Vec<u8>,
    offset: Offset,
    timestamp_ms: i64,
    message: &Message,
) -> usize {
    let start = buf.len();

    // Reserve space for record_length + crc32
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());

    let crc_start = buf.len(); // body starts here (fields after crc)

    buf.extend_from_slice(&offset.raw().to_le_bytes());
    buf.extend_from_slice(&timestamp_ms.to_le_bytes());

    match &message.key {
        Some(key) => {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
        }
        None => {
            buf.extend_from_slice(&NULL_KEY_LEN.to_le_bytes());
        }
    }

    buf.extend_from_slice(&(message.value.len() as u32).to_le_bytes());
    buf.extend_from_slice(&message.value);

    buf.extend_from_slice(&(message.headers.len() as u32).to_le_bytes());
    for (name, value) in &message.headers {
        let name_bytes = name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(value);
    }

    let end = buf.len();
    let body = &buf[crc_start..end];
    let crc = crc32fast::hash(body);
    let record_length = (end - start - 4) as u32; // bytes after record_length field

    buf[start..start + 4].copy_from_slice(&record_length.to_le_bytes());
    buf[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());

    end - start
}

/// Result of attempting to decode one record from a buffer at `pos`.
#[derive(Debug)]
pub enum DecodeStatus {
    /// Successfully decoded a record; `next_pos` is the byte after the record.
    Ok {
        /// Decoded record.
        record: Record,
        /// Absolute position of this record's `record_length` field.
        position: u64,
        /// Byte position immediately after this record.
        next_pos: usize,
        /// Total on-disk size of this record (including length prefix).
        size: usize,
    },
    /// Not enough bytes for a complete record (torn tail).
    Incomplete,
    /// Length/CRC/format error at this position (treat as torn/corrupt tail).
    Corrupt,
}

/// Try to decode one record starting at `pos` within `data`.
pub fn decode_record_at(data: &[u8], pos: usize) -> DecodeStatus {
    if data.len() < pos + 4 {
        return DecodeStatus::Incomplete;
    }
    let record_length =
        u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    if record_length < 4 {
        // Must at least hold the crc field
        return DecodeStatus::Corrupt;
    }
    let total = 4 + record_length;
    if data.len() < pos + total {
        return DecodeStatus::Incomplete;
    }

    let rec = &data[pos + 4..pos + total];
    let stored_crc = u32::from_le_bytes(rec[0..4].try_into().unwrap());
    let body = &rec[4..];
    let computed = crc32fast::hash(body);
    if stored_crc != computed {
        return DecodeStatus::Corrupt;
    }

    match parse_body(body) {
        Ok(record) => DecodeStatus::Ok {
            record,
            position: pos as u64,
            next_pos: pos + total,
            size: total,
        },
        Err(_) => DecodeStatus::Corrupt,
    }
}

fn parse_body(mut body: &[u8]) -> Result<Record> {
    let offset = Offset::new(read_u64(&mut body)?);
    let timestamp_ms = read_i64(&mut body)?;
    let key_len = read_u32(&mut body)?;
    let key = if key_len == NULL_KEY_LEN {
        None
    } else {
        let k = read_bytes(&mut body, key_len as usize)?;
        Some(Bytes::copy_from_slice(k))
    };
    let value_len = read_u32(&mut body)?;
    let value = Bytes::copy_from_slice(read_bytes(&mut body, value_len as usize)?);
    let header_count = read_u32(&mut body)? as usize;
    let mut headers = Vec::with_capacity(header_count);
    for _ in 0..header_count {
        let name_len = read_u16(&mut body)? as usize;
        let name_bytes = read_bytes(&mut body, name_len)?;
        let name = std::str::from_utf8(name_bytes)
            .map_err(|e| Error::Storage(format!("header name utf8: {e}")))?
            .to_string();
        let vlen = read_u32(&mut body)? as usize;
        let v = Bytes::copy_from_slice(read_bytes(&mut body, vlen)?);
        headers.push((name, v));
    }
    Ok(Record {
        offset,
        key,
        value,
        timestamp_ms,
        headers,
    })
}

fn read_u16(buf: &mut &[u8]) -> Result<u16> {
    if buf.len() < 2 {
        return Err(Error::Storage("truncated record body".into()));
    }
    let v = u16::from_le_bytes(buf[..2].try_into().unwrap());
    *buf = &buf[2..];
    Ok(v)
}

fn read_u32(buf: &mut &[u8]) -> Result<u32> {
    if buf.len() < 4 {
        return Err(Error::Storage("truncated record body".into()));
    }
    let v = u32::from_le_bytes(buf[..4].try_into().unwrap());
    *buf = &buf[4..];
    Ok(v)
}

fn read_u64(buf: &mut &[u8]) -> Result<u64> {
    if buf.len() < 8 {
        return Err(Error::Storage("truncated record body".into()));
    }
    let v = u64::from_le_bytes(buf[..8].try_into().unwrap());
    *buf = &buf[8..];
    Ok(v)
}

fn read_i64(buf: &mut &[u8]) -> Result<i64> {
    if buf.len() < 8 {
        return Err(Error::Storage("truncated record body".into()));
    }
    let v = i64::from_le_bytes(buf[..8].try_into().unwrap());
    *buf = &buf[8..];
    Ok(v)
}

fn read_bytes<'a>(buf: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if buf.len() < len {
        return Err(Error::Storage("truncated record body".into()));
    }
    let (head, rest) = buf.split_at(len);
    *buf = rest;
    Ok(head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use volant_core::Message;

    #[test]
    fn header_roundtrip() {
        let raw = encode_header(Offset::new(42), 1_700_000_000_000);
        let h = decode_header(&raw).unwrap();
        assert_eq!(h.base_offset, Offset::new(42));
        assert_eq!(h.created_at_ms, 1_700_000_000_000);
        assert_eq!(h.version, VERSION);
    }

    #[test]
    fn record_roundtrip_with_key_and_headers() {
        let msg = Message {
            key: Some(Bytes::from("k")),
            value: Bytes::from("hello"),
            timestamp_ms: Some(123),
            headers: vec![("h".into(), Bytes::from("v"))],
        };
        let mut buf = Vec::new();
        let n = encode_record(&mut buf, Offset::new(7), 123, &msg);
        assert_eq!(n, buf.len());
        assert_eq!(n as u64, encoded_record_size(&msg));

        match decode_record_at(&buf, 0) {
            DecodeStatus::Ok { record, next_pos, .. } => {
                assert_eq!(next_pos, buf.len());
                assert_eq!(record.offset, Offset::new(7));
                assert_eq!(record.key.as_ref().unwrap().as_ref(), b"k");
                assert_eq!(record.value.as_ref(), b"hello");
                assert_eq!(record.timestamp_ms, 123);
                assert_eq!(record.headers.len(), 1);
                assert_eq!(record.headers[0].0, "h");
                assert_eq!(record.headers[0].1.as_ref(), b"v");
            }
            other => panic!("decode failed: {other:?}"),
        }
    }

    #[test]
    fn record_null_key() {
        let msg = Message::from_value("x");
        let mut buf = Vec::new();
        encode_record(&mut buf, Offset::ZERO, 1, &msg);
        match decode_record_at(&buf, 0) {
            DecodeStatus::Ok { record, .. } => {
                assert!(record.key.is_none());
                assert_eq!(record.value.as_ref(), b"x");
            }
            other => panic!("decode failed: {other:?}"),
        }
    }

    #[test]
    fn torn_tail_incomplete() {
        let msg = Message::from_value("payload");
        let mut buf = Vec::new();
        encode_record(&mut buf, Offset::ZERO, 1, &msg);
        let short = &buf[..buf.len() - 3];
        assert!(matches!(
            decode_record_at(short, 0),
            DecodeStatus::Incomplete
        ));
    }

    #[test]
    fn corrupt_crc() {
        let msg = Message::from_value("payload");
        let mut buf = Vec::new();
        encode_record(&mut buf, Offset::ZERO, 1, &msg);
        // Flip a body byte (after length + crc)
        if buf.len() > 10 {
            buf[10] ^= 0xff;
        }
        assert!(matches!(
            decode_record_at(&buf, 0),
            DecodeStatus::Corrupt
        ));
    }
}
