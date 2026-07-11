//! Broker → client response types.

use bytes::Bytes;

/// Response opcodes (Phase 2–6 wire values).
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
    /// Delete topic result.
    DeleteTopic = 5,
    /// Offset commit result.
    OffsetCommit = 6,
    /// Offset fetch result.
    OffsetFetch = 7,
    /// Join group result.
    JoinGroup = 8,
    /// Heartbeat result.
    Heartbeat = 9,
    /// Leave group result.
    LeaveGroup = 10,
    /// Replica fetch result (Phase 6).
    ReplicaFetch = 21,
    /// Broker heartbeat result (Phase 6).
    HeartbeatBroker = 23,
    /// Cluster state snapshot (Phase 6).
    ClusterState = 25,
    /// Auth result (Phase 7).
    Auth = 31,
    /// Error response.
    Error = 0xFFFF,
}

impl ResponseOpcode {
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
            21 => Self::ReplicaFetch,
            23 => Self::HeartbeatBroker,
            25 => Self::ClusterState,
            31 => Self::Auth,
            0xFFFF => Self::Error,
            _ => return None,
        })
    }
}

/// Protocol error codes (Error response payload + embedded codes).
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Success (unused in Error frames).
    Ok = 0,
    /// Unknown error.
    Unknown = 1,
    /// Resource not found.
    NotFound = 2,
    /// Invalid argument.
    InvalidArg = 3,
    /// Storage failure.
    Storage = 4,
    /// Protocol failure.
    Protocol = 5,
    /// I/O failure.
    Io = 6,
    /// Timeout.
    Timeout = 7,
    /// Unsupported operation.
    Unsupported = 8,
    /// Group rebalance in progress / generation mismatch.
    RebalanceInProgress = 9,
    /// Unknown group member id.
    UnknownMemberId = 10,
    /// Illegal generation for the group.
    IllegalGeneration = 11,
    /// Inconsistent group protocol (reserved).
    InconsistentGroupProtocol = 12,
    /// Requested broker is not the partition leader.
    NotLeaderForPartition = 13,
    /// Requested broker is not the cluster controller.
    NotController = 14,
    /// ISR smaller than min_insync_replicas for acks=all.
    NotEnoughReplicas = 15,
    /// Target broker is not available.
    BrokerNotAvailable = 16,
    /// Shared-token authentication failed (wrong token).
    AuthenticationFailed = 17,
    /// Auth required before other opcodes on this connection.
    AuthenticationRequired = 18,
}

impl ErrorCode {
    /// Parse raw error code.
    pub fn from_u16(v: u16) -> Self {
        match v {
            0 => Self::Ok,
            2 => Self::NotFound,
            3 => Self::InvalidArg,
            4 => Self::Storage,
            5 => Self::Protocol,
            6 => Self::Io,
            7 => Self::Timeout,
            8 => Self::Unsupported,
            9 => Self::RebalanceInProgress,
            10 => Self::UnknownMemberId,
            11 => Self::IllegalGeneration,
            12 => Self::InconsistentGroupProtocol,
            13 => Self::NotLeaderForPartition,
            14 => Self::NotController,
            15 => Self::NotEnoughReplicas,
            16 => Self::BrokerNotAvailable,
            17 => Self::AuthenticationFailed,
            18 => Self::AuthenticationRequired,
            _ => Self::Unknown,
        }
    }
}

/// A fetched record on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRecord {
    /// Log offset.
    pub offset: u64,
    /// Timestamp ms.
    pub timestamp_ms: i64,
    /// Optional key.
    pub key: Option<Bytes>,
    /// Value bytes.
    pub value: Bytes,
    /// Headers.
    pub headers: Vec<(String, Bytes)>,
}

/// Broker metadata entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    /// Node id.
    pub node_id: u32,
    /// Host.
    pub host: String,
    /// Port.
    pub port: u16,
}

/// Partition metadata entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    /// Partition id.
    pub partition_id: u32,
    /// Leader node id.
    pub leader: u32,
    /// High watermark (committed).
    pub hwm: u64,
    /// Replica broker ids (Phase 6; single-node: `[self]`).
    pub replicas: Vec<u32>,
    /// In-sync replica broker ids (Phase 6; single-node: `[self]`).
    pub isr: Vec<u32>,
    /// Leader epoch (Phase 6; single-node: `0`).
    pub leader_epoch: u32,
}

/// Topic metadata entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicInfo {
    /// Topic name.
    pub name: String,
    /// Topic id.
    pub topic_id: u32,
    /// Per-topic error (0 = ok).
    pub error_code: u16,
    /// Partitions.
    pub partitions: Vec<PartitionInfo>,
}

/// Partition assignment returned by JoinGroup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
}

/// Offset fetch response entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchEntry {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
    /// Committed offset; `u64::MAX` means unknown / not committed.
    pub offset: u64,
    /// Optional metadata.
    pub metadata: String,
}

/// One partition in a cluster-state snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterPartitionState {
    /// Partition id.
    pub partition_id: u32,
    /// Leader broker id.
    pub leader: u32,
    /// Leader epoch.
    pub leader_epoch: u32,
    /// Replica set.
    pub replicas: Vec<u32>,
    /// In-sync replicas.
    pub isr: Vec<u32>,
}

/// One topic in a cluster-state snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTopicState {
    /// Topic name.
    pub name: String,
    /// Topic id.
    pub topic_id: u32,
    /// Partitions.
    pub partitions: Vec<ClusterPartitionState>,
}

/// High-level response enum with real payload fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// Produce acknowledgement.
    Produce {
        /// Topic name.
        topic: String,
        /// Assigned partition.
        partition: u32,
        /// First offset of the batch.
        base_offset: u64,
        /// Number of records written.
        count: u32,
        /// 0 = ok.
        error_code: u16,
    },
    /// Fetch result.
    Fetch {
        /// Topic name.
        topic: String,
        /// Partition.
        partition: u32,
        /// High watermark.
        high_watermark: u64,
        /// 0 = ok.
        error_code: u16,
        /// Records.
        records: Vec<FetchRecord>,
    },
    /// Create topic result.
    CreateTopic {
        /// Assigned topic id.
        topic_id: u32,
        /// Topic name.
        name: String,
        /// Partition count.
        partitions: u32,
        /// 0 = ok.
        error_code: u16,
    },
    /// Delete topic result.
    DeleteTopic {
        /// Topic name.
        name: String,
        /// 0 = ok.
        error_code: u16,
    },
    /// Cluster metadata.
    Metadata {
        /// Known brokers.
        brokers: Vec<BrokerInfo>,
        /// Topic metadata.
        topics: Vec<TopicInfo>,
    },
    /// Offset commit result.
    OffsetCommit {
        /// 0 = ok.
        error_code: u16,
    },
    /// Offset fetch result.
    OffsetFetch {
        /// 0 = ok.
        error_code: u16,
        /// Committed offsets.
        entries: Vec<OffsetFetchEntry>,
    },
    /// Join group result.
    JoinGroup {
        /// 0 = ok.
        error_code: u16,
        /// Group generation.
        generation: u32,
        /// Broker-assigned member id.
        member_id: String,
        /// This member's partition assignment.
        assignment: Vec<Assignment>,
    },
    /// Heartbeat result.
    Heartbeat {
        /// 0 = ok; 9 = rebalance (client should re-JoinGroup).
        error_code: u16,
    },
    /// Leave group result.
    LeaveGroup {
        /// 0 = ok.
        error_code: u16,
    },
    /// Replica fetch result.
    ReplicaFetch {
        /// 0 = ok; 13 = not leader.
        error_code: u16,
        /// Topic name.
        topic: String,
        /// Partition.
        partition: u32,
        /// Leader high watermark (committed).
        high_watermark: u64,
        /// Leader epoch.
        leader_epoch: u32,
        /// Records with exact offsets for the follower to append.
        records: Vec<FetchRecord>,
    },
    /// Broker heartbeat result.
    HeartbeatBroker {
        /// 0 = ok.
        error_code: u16,
        /// Current controller id.
        controller_id: u32,
        /// Cluster generation.
        generation: u32,
        /// Live broker ids.
        alive_brokers: Vec<u32>,
    },
    /// Cluster assignment snapshot.
    ClusterState {
        /// 0 = ok.
        error_code: u16,
        /// Cluster generation.
        generation: u32,
        /// Controller id.
        controller_id: u32,
        /// Topics and partition replica state.
        topics: Vec<ClusterTopicState>,
    },
    /// Auth result.
    Auth {
        /// 0 = ok; 17 = AuthenticationFailed.
        error_code: u16,
    },
    /// Error response.
    Error {
        /// Error code.
        code: u16,
        /// Human-readable error message.
        message: String,
    },
}

impl Response {
    /// Wire opcode for this response variant.
    pub fn opcode(&self) -> u16 {
        match self {
            Self::Produce { .. } => ResponseOpcode::Produce as u16,
            Self::Fetch { .. } => ResponseOpcode::Fetch as u16,
            Self::CreateTopic { .. } => ResponseOpcode::CreateTopic as u16,
            Self::DeleteTopic { .. } => ResponseOpcode::DeleteTopic as u16,
            Self::Metadata { .. } => ResponseOpcode::Metadata as u16,
            Self::OffsetCommit { .. } => ResponseOpcode::OffsetCommit as u16,
            Self::OffsetFetch { .. } => ResponseOpcode::OffsetFetch as u16,
            Self::JoinGroup { .. } => ResponseOpcode::JoinGroup as u16,
            Self::Heartbeat { .. } => ResponseOpcode::Heartbeat as u16,
            Self::LeaveGroup { .. } => ResponseOpcode::LeaveGroup as u16,
            Self::ReplicaFetch { .. } => ResponseOpcode::ReplicaFetch as u16,
            Self::HeartbeatBroker { .. } => ResponseOpcode::HeartbeatBroker as u16,
            Self::ClusterState { .. } => ResponseOpcode::ClusterState as u16,
            Self::Auth { .. } => ResponseOpcode::Auth as u16,
            Self::Error { .. } => ResponseOpcode::Error as u16,
        }
    }
}
