//! Frame layout shared by request and response messages.

/// Magic byte identifying a Volant frame.
pub const FRAME_MAGIC: u8 = b'V';

/// Current protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

/// Fixed-size frame header preceding every payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Protocol version.
    pub version: u8,
    /// Request / response opcode.
    pub opcode: u16,
    /// Correlation id for request/response matching.
    pub correlation_id: u32,
    /// Payload length in bytes (not including this header).
    pub payload_len: u32,
    /// CRC32 of the payload.
    pub checksum: u32,
}

/// A complete protocol frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Frame header.
    pub header: FrameHeader,
    /// Opaque payload bytes (codec-specific).
    pub payload: bytes::Bytes,
}
