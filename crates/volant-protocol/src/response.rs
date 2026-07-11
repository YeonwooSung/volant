//! Broker → client response types.

/// Response opcodes.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseOpcode {
    /// Produce acknowledgement.
    Produce = 1,
    /// Fetch result.
    Fetch = 2,
    /// Create topic result.
    CreateTopic = 3,
    /// Metadata result.
    Metadata = 4,
    /// Offset commit result.
    OffsetCommit = 5,
    /// Offset fetch result.
    OffsetFetch = 6,
    /// Error response.
    Error = 0xFFFF,
}

/// High-level response enum (payload decoding lands in later milestones).
#[derive(Debug, Clone)]
pub enum Response {
    /// Produce response (placeholder).
    Produce,
    /// Fetch response (placeholder).
    Fetch,
    /// Create topic response (placeholder).
    CreateTopic,
    /// Metadata response (placeholder).
    Metadata,
    /// Offset commit response (placeholder).
    OffsetCommit,
    /// Offset fetch response (placeholder).
    OffsetFetch,
    /// Error response (placeholder).
    Error {
        /// Error code.
        code: u16,
        /// Human-readable error message.
        message: String,
    },
}
