//! Client → broker request opcodes and payloads.

use bytes::Bytes;

/// Request opcodes (Phase 2 wire values).
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
    /// Delete a topic.
    DeleteTopic = 5,
    /// Commit consumer offsets (Phase 3).
    OffsetCommit = 6,
    /// Fetch committed offsets (Phase 3).
    OffsetFetch = 7,
}

impl RequestOpcode {
    /// Parse a raw opcode value.
    pub fn from_u16(v: u16) -> Option<Self> {
        Some(match v {
            1 => Self::Produce,
            2 => Self::Fetch,
            3 => Self::CreateTopic,
            4 => Self::Metadata,
            5 => Self::DeleteTopic,
            6 => Self::OffsetCommit,
            7 => Self::OffsetFetch,
            _ => return None,
        })
    }
}

/// A single produce message on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceMessage {
    /// Optional message key.
    pub key: Option<Bytes>,
    /// Message value.
    pub value: Bytes,
    /// Timestamp ms; `-1` means broker now.
    pub timestamp_ms: i64,
    /// Optional headers.
    pub headers: Vec<(String, Bytes)>,
}

/// High-level request enum with real payload fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// Produce messages.
    Produce {
        /// Topic name.
        topic: String,
        /// Partition; `-1` = broker assigns.
        partition: i32,
        /// Acks mode (0/1); response always sent for simplicity.
        acks: u8,
        /// Messages to append.
        messages: Vec<ProduceMessage>,
    },
    /// Fetch records.
    Fetch {
        /// Topic name.
        topic: String,
        /// Partition id.
        partition: u32,
        /// Start offset.
        from_offset: u64,
        /// Max records to return.
        max_messages: u32,
        /// Soft max bytes (best-effort).
        max_bytes: u32,
        /// Long-poll wait; 0 = non-blocking.
        max_wait_ms: u32,
    },
    /// Create a topic.
    CreateTopic {
        /// Topic name.
        name: String,
        /// Partition count.
        partitions: u32,
    },
    /// Cluster / topic metadata.
    Metadata {
        /// Empty means all topics.
        topics: Vec<String>,
    },
    /// Delete a topic.
    DeleteTopic {
        /// Topic name.
        name: String,
    },
    /// Offset commit (Phase 3 placeholder).
    OffsetCommit,
    /// Offset fetch (Phase 3 placeholder).
    OffsetFetch,
}

impl Request {
    /// Wire opcode for this request variant.
    pub fn opcode(&self) -> u16 {
        match self {
            Self::Produce { .. } => RequestOpcode::Produce as u16,
            Self::Fetch { .. } => RequestOpcode::Fetch as u16,
            Self::CreateTopic { .. } => RequestOpcode::CreateTopic as u16,
            Self::Metadata { .. } => RequestOpcode::Metadata as u16,
            Self::DeleteTopic { .. } => RequestOpcode::DeleteTopic as u16,
            Self::OffsetCommit => RequestOpcode::OffsetCommit as u16,
            Self::OffsetFetch => RequestOpcode::OffsetFetch as u16,
        }
    }
}
