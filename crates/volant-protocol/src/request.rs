//! Client → broker request opcodes and payloads.

/// Request opcodes.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOpcode {
    /// Produce messages to a topic partition.
    Produce = 1,
    /// Fetch messages from a topic partition.
    Fetch = 2,
    /// Create a topic.
    CreateTopic = 3,
    /// Metadata for topics / brokers.
    Metadata = 4,
    /// Commit consumer offsets.
    OffsetCommit = 5,
    /// Fetch committed offsets.
    OffsetFetch = 6,
}

/// High-level request enum (payload decoding lands in later milestones).
#[derive(Debug, Clone)]
pub enum Request {
    /// Produce request (placeholder).
    Produce,
    /// Fetch request (placeholder).
    Fetch,
    /// Create topic request (placeholder).
    CreateTopic,
    /// Metadata request (placeholder).
    Metadata,
    /// Offset commit request (placeholder).
    OffsetCommit,
    /// Offset fetch request (placeholder).
    OffsetFetch,
}
