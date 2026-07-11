//! Broker → client response types.

use bytes::Bytes;

/// Response opcodes (Phase 2 wire values).
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
    /// Offset commit result (Phase 3).
    OffsetCommit = 6,
    /// Offset fetch result (Phase 3).
    OffsetFetch = 7,
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
            0xFFFF => Self::Error,
            _ => return None,
        })
    }
}

/// Protocol error codes (Error response payload).
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
    /// High watermark.
    pub hwm: u64,
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
    /// Offset commit (Phase 3 placeholder).
    OffsetCommit,
    /// Offset fetch (Phase 3 placeholder).
    OffsetFetch,
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
            Self::OffsetCommit => ResponseOpcode::OffsetCommit as u16,
            Self::OffsetFetch => ResponseOpcode::OffsetFetch as u16,
            Self::Error { .. } => ResponseOpcode::Error as u16,
        }
    }
}
