//! Client → broker request opcodes and payloads.

use bytes::Bytes;

/// Request opcodes (Phase 2 + Phase 3 wire values).
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
    /// Commit consumer offsets.
    OffsetCommit = 6,
    /// Fetch committed offsets.
    OffsetFetch = 7,
    /// Join a consumer group.
    JoinGroup = 8,
    /// Heartbeat for a consumer group member.
    Heartbeat = 9,
    /// Leave a consumer group.
    LeaveGroup = 10,
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
            8 => Self::JoinGroup,
            9 => Self::Heartbeat,
            10 => Self::LeaveGroup,
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

/// One offset commit/fetch entry (topic + partition).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetEntry {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
}

/// Offset commit payload entry including committed position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitEntry {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
    /// Next offset to read (committed position).
    pub offset: u64,
    /// Optional metadata string (may be empty).
    pub metadata: String,
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
    /// Commit consumer group offsets.
    OffsetCommit {
        /// Consumer group id.
        group_id: String,
        /// Member id (may be empty for admin commits).
        member_id: String,
        /// Generation; `0` skips generation check (admin/CLI).
        generation: u32,
        /// Offsets to commit.
        entries: Vec<OffsetCommitEntry>,
    },
    /// Fetch committed offsets.
    OffsetFetch {
        /// Consumer group id.
        group_id: String,
        /// Empty means all committed offsets for the group.
        entries: Vec<OffsetEntry>,
    },
    /// Join a consumer group.
    JoinGroup {
        /// Consumer group id.
        group_id: String,
        /// Member id; empty = new member.
        member_id: String,
        /// Session timeout in milliseconds.
        session_timeout_ms: u32,
        /// Subscribed topic names.
        topics: Vec<String>,
    },
    /// Heartbeat for group membership.
    Heartbeat {
        /// Consumer group id.
        group_id: String,
        /// Member id.
        member_id: String,
        /// Current generation.
        generation: u32,
    },
    /// Leave a consumer group.
    LeaveGroup {
        /// Consumer group id.
        group_id: String,
        /// Member id.
        member_id: String,
    },
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
            Self::OffsetCommit { .. } => RequestOpcode::OffsetCommit as u16,
            Self::OffsetFetch { .. } => RequestOpcode::OffsetFetch as u16,
            Self::JoinGroup { .. } => RequestOpcode::JoinGroup as u16,
            Self::Heartbeat { .. } => RequestOpcode::Heartbeat as u16,
            Self::LeaveGroup { .. } => RequestOpcode::LeaveGroup as u16,
        }
    }
}
