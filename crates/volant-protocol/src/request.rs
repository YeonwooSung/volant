//! Client → broker request opcodes and payloads.

use bytes::Bytes;

/// Request opcodes (Phase 2–6 wire values).
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
    /// Follower → leader replica fetch (Phase 6).
    ReplicaFetch = 20,
    /// Broker → controller heartbeat (Phase 6).
    HeartbeatBroker = 22,
    /// Pull / apply cluster assignment state (Phase 6).
    ClusterState = 24,
    /// Shared-token authentication (Phase 7).
    Auth = 30,
    /// Allocate a producer id + epoch for idempotent produce (Phase 10).
    InitProducerId = 32,
    /// Describe a consumer group (Phase 11).
    DescribeGroup = 34,
    /// List known consumer groups (Phase 12).
    ListGroups = 36,
    /// Delete committed consumer offsets (Phase 12).
    DeleteOffsets = 38,
    /// Describe topic configs (Phase 13).
    DescribeConfigs = 40,
    /// Alter topic configs (Phase 13).
    AlterConfigs = 42,
    /// Delete records before an offset (Phase 14).
    DeleteRecords = 44,
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
            20 => Self::ReplicaFetch,
            22 => Self::HeartbeatBroker,
            24 => Self::ClusterState,
            30 => Self::Auth,
            32 => Self::InitProducerId,
            34 => Self::DescribeGroup,
            36 => Self::ListGroups,
            38 => Self::DeleteOffsets,
            40 => Self::DescribeConfigs,
            42 => Self::AlterConfigs,
            44 => Self::DeleteRecords,
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
        /// Acks mode: `0`, `1`, or `255` (all).
        acks: u8,
        /// Messages to append.
        messages: Vec<ProduceMessage>,
        /// Idempotent producer id (`0` = disabled).
        producer_id: u64,
        /// Producer epoch from [`Request::InitProducerId`].
        producer_epoch: u16,
        /// Base sequence for this batch (`-1` = non-idempotent).
        base_sequence: i32,
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
        /// Optional configs (Phase 13); empty = broker defaults.
        /// Keys: `retention.ms`, `retention.bytes`, `segment.bytes`.
        configs: Vec<(String, String)>,
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
        /// Member id; empty = new member (or derived from `group_instance_id`).
        member_id: String,
        /// Session timeout in milliseconds.
        session_timeout_ms: u32,
        /// Subscribed topic names.
        topics: Vec<String>,
        /// Optional static membership id (Phase 12). Empty = dynamic.
        group_instance_id: String,
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
    /// Follower replica fetch from the partition leader.
    ReplicaFetch {
        /// Topic name.
        topic: String,
        /// Partition id.
        partition: u32,
        /// Follower log-end offset (next offset to write).
        from_offset: u64,
        /// Soft max bytes.
        max_bytes: u32,
        /// Follower broker id.
        replica_id: u32,
    },
    /// Inter-broker liveness heartbeat to the controller.
    HeartbeatBroker {
        /// Sender broker id.
        broker_id: u32,
        /// Last known controller id (`0` if unknown).
        controller_id_known: u32,
        /// Last known cluster generation.
        generation: u32,
    },
    /// Request full cluster assignment snapshot.
    ClusterState {
        /// Last applied generation on the requester (`0` if none).
        known_generation: u32,
    },
    /// Authenticate this connection with a shared token.
    Auth {
        /// Shared secret token.
        token: String,
    },
    /// Allocate a producer id for idempotent produce (Phase 10).
    InitProducerId,
    /// Describe a consumer group (Phase 11).
    DescribeGroup {
        /// Consumer group id.
        group_id: String,
    },
    /// List known consumer groups (Phase 12).
    ListGroups,
    /// Delete committed offsets for a group (Phase 12).
    DeleteOffsets {
        /// Consumer group id.
        group_id: String,
        /// Partitions to clear; empty = all offsets for the group.
        entries: Vec<OffsetEntry>,
    },
    /// Describe topic configuration (Phase 13).
    DescribeConfigs {
        /// Topic name.
        topic: String,
    },
    /// Alter topic configuration (Phase 13).
    AlterConfigs {
        /// Topic name.
        topic: String,
        /// Config entries; empty value clears that key.
        configs: Vec<(String, String)>,
    },
    /// Delete records before an offset (Phase 14).
    DeleteRecords {
        /// Topic name.
        topic: String,
        /// Partition id.
        partition: u32,
        /// Drop sealed segments entirely before this offset.
        before_offset: u64,
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
            Self::ReplicaFetch { .. } => RequestOpcode::ReplicaFetch as u16,
            Self::HeartbeatBroker { .. } => RequestOpcode::HeartbeatBroker as u16,
            Self::ClusterState { .. } => RequestOpcode::ClusterState as u16,
            Self::Auth { .. } => RequestOpcode::Auth as u16,
            Self::InitProducerId => RequestOpcode::InitProducerId as u16,
            Self::DescribeGroup { .. } => RequestOpcode::DescribeGroup as u16,
            Self::ListGroups => RequestOpcode::ListGroups as u16,
            Self::DeleteOffsets { .. } => RequestOpcode::DeleteOffsets as u16,
            Self::DescribeConfigs { .. } => RequestOpcode::DescribeConfigs as u16,
            Self::AlterConfigs { .. } => RequestOpcode::AlterConfigs as u16,
            Self::DeleteRecords { .. } => RequestOpcode::DeleteRecords as u16,
        }
    }
}
