//! Encode and decode protocol frames.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_core::{Error, Result};

use crate::frame::{Frame, FrameHeader, FRAME_MAGIC, PROTOCOL_VERSION};

/// Size of the on-wire frame header in bytes.
pub const HEADER_LEN: usize = 1 + 1 + 2 + 4 + 4 + 4; // magic + version + opcode + corr + len + crc

/// Encode a frame into `dst`.
pub fn encode_frame(frame: &Frame, dst: &mut BytesMut) -> Result<()> {
    dst.reserve(HEADER_LEN + frame.payload.len());
    dst.put_u8(FRAME_MAGIC);
    dst.put_u8(frame.header.version);
    dst.put_u16(frame.header.opcode);
    dst.put_u32(frame.header.correlation_id);
    dst.put_u32(frame.header.payload_len);
    dst.put_u32(frame.header.checksum);
    dst.extend_from_slice(&frame.payload);
    Ok(())
}

/// Attempt to decode a single frame from `src`.
///
/// Returns `Ok(None)` if more bytes are needed.
pub fn decode_frame(src: &mut BytesMut) -> Result<Option<Frame>> {
    if src.len() < HEADER_LEN {
        return Ok(None);
    }

    let magic = src[0];
    if magic != FRAME_MAGIC {
        return Err(Error::Protocol(format!("invalid frame magic: {magic:#x}")));
    }

    let payload_len = u32::from_be_bytes(src[8..12].try_into().unwrap()) as usize;
    let total = HEADER_LEN + payload_len;
    if src.len() < total {
        return Ok(None);
    }

    let mut buf = src.split_to(total);
    let _magic = buf.get_u8();
    let version = buf.get_u8();
    let opcode = buf.get_u16();
    let correlation_id = buf.get_u32();
    let payload_len_u32 = buf.get_u32();
    let checksum_wire = buf.get_u32();
    let payload = Bytes::from(buf.to_vec());

    if version != PROTOCOL_VERSION {
        return Err(Error::Protocol(format!(
            "unsupported protocol version: {version}"
        )));
    }

    // Reject oversized payloads (16 MiB cap).
    if payload.len() > crate::payload::MAX_PAYLOAD {
        return Err(Error::Protocol(format!(
            "payload too large: {} > {}",
            payload.len(),
            crate::payload::MAX_PAYLOAD
        )));
    }

    let expected = checksum(&payload);
    if checksum_wire != expected {
        return Err(Error::Protocol(format!(
            "checksum mismatch: got {checksum_wire:#x}, expected {expected:#x}"
        )));
    }

    Ok(Some(Frame {
        header: FrameHeader {
            version,
            opcode,
            correlation_id,
            payload_len: payload_len_u32,
            checksum: checksum_wire,
        },
        payload,
    }))
}

/// Compute CRC32 over payload bytes.
pub fn checksum(payload: &[u8]) -> u32 {
    crc32fast::hash(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{Frame, FrameHeader, PROTOCOL_VERSION};

    #[test]
    fn frame_roundtrip() {
        let payload = Bytes::from_static(b"ping");
        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_VERSION,
                opcode: 1,
                correlation_id: 7,
                payload_len: payload.len() as u32,
                checksum: checksum(&payload),
            },
            payload: payload.clone(),
        };
        let mut buf = BytesMut::new();
        encode_frame(&frame, &mut buf).unwrap();
        let decoded = decode_frame(&mut buf).unwrap().expect("frame");
        assert_eq!(decoded.header.correlation_id, 7);
        assert_eq!(decoded.payload, payload);
    }
}
