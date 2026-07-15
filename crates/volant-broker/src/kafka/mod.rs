//! Kafka wire protocol shim (Phases 23–26).
//!
//! Classic (non-flexible) framing only. Supported APIs: Produce, Fetch,
//! Metadata, ListOffsets, OffsetCommit/Fetch, consumer groups (Join/Sync/
//! Heartbeat/Leave), FindCoordinator, ApiVersions, CreateTopics, DeleteTopics.
//! See `docs/PHASE23_SPEC.md` … `docs/PHASE26_SPEC.md`.

/// Kafka wire primitives, MessageSet (magic 0/1), and RecordBatch (magic 2).
pub mod codec;
mod handler;

pub use handler::serve_kafka_listener;

/// Kafka principal used for ACL checks on the shim port (no SASL).
pub const KAFKA_ANONYMOUS_PRINCIPAL: &str = "kafka-anonymous";

/// Kafka error codes we emit (subset of the official table).
#[repr(i16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaErrorCode {
    /// No error.
    None = 0,
    /// Unexpected server failure.
    Unknown = -1,
    /// Offset out of range.
    OffsetOutOfRange = 1,
    /// Corrupt message.
    CorruptMessage = 2,
    /// Unknown topic or partition.
    UnknownTopicOrPartition = 3,
    /// Invalid message size / args.
    InvalidMessage = 4,
    /// Not leader for partition.
    NotLeaderForPartition = 6,
    /// Request timed out (unused).
    RequestTimedOut = 7,
    /// Broker not available.
    BrokerNotAvailable = 8,
    /// Message too large.
    MessageTooLarge = 10,
    /// Network exception (unused).
    NetworkException = 13,
    /// Invalid topic name / args.
    InvalidTopicException = 17,
    /// Invalid request.
    InvalidRequest = 42,
    /// Unsupported version for this API.
    UnsupportedVersion = 35,
    /// Topic authorization failed.
    TopicAuthorizationFailed = 29,
    /// Cluster authorization failed.
    ClusterAuthorizationFailed = 31,
    /// Topic already exists (CreateTopics).
    TopicAlreadyExists = 36,
    /// Invalid partition count.
    InvalidPartitions = 37,
    /// Invalid timestamp in ListOffsets.
    InvalidTimestamp = 32,
    /// Not coordinator for group.
    NotCoordinator = 16,
    /// Illegal generation.
    IllegalGeneration = 22,
    /// Unknown member id.
    UnknownMemberId = 25,
    /// Rebalance in progress.
    RebalanceInProgress = 27,
    /// Group authorization failed.
    GroupAuthorizationFailed = 30,
}

/// Map Volant group error codes to Kafka wire error codes.
pub(crate) fn map_group_error(volant: u16) -> i16 {
    match volant {
        0 => KafkaErrorCode::None.as_i16(),
        9 => KafkaErrorCode::RebalanceInProgress.as_i16(),
        10 => KafkaErrorCode::UnknownMemberId.as_i16(),
        11 => KafkaErrorCode::IllegalGeneration.as_i16(),
        _ => KafkaErrorCode::Unknown.as_i16(),
    }
}

impl KafkaErrorCode {
    fn as_i16(self) -> i16 {
        self as i16
    }
}

/// Kafka API keys supported by the shim.
#[repr(i16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKey {
    /// Produce.
    Produce = 0,
    /// Fetch.
    Fetch = 1,
    /// ListOffsets.
    ListOffsets = 2,
    /// Metadata.
    Metadata = 3,
    /// OffsetCommit.
    OffsetCommit = 8,
    /// OffsetFetch.
    OffsetFetch = 9,
    /// FindCoordinator.
    FindCoordinator = 10,
    /// JoinGroup.
    JoinGroup = 11,
    /// Heartbeat.
    Heartbeat = 12,
    /// LeaveGroup.
    LeaveGroup = 13,
    /// SyncGroup.
    SyncGroup = 14,
    /// ApiVersions.
    ApiVersions = 18,
    /// CreateTopics.
    CreateTopics = 19,
    /// DeleteTopics.
    DeleteTopics = 20,
}

impl ApiKey {
    fn from_i16(v: i16) -> Option<Self> {
        match v {
            0 => Some(Self::Produce),
            1 => Some(Self::Fetch),
            2 => Some(Self::ListOffsets),
            3 => Some(Self::Metadata),
            8 => Some(Self::OffsetCommit),
            9 => Some(Self::OffsetFetch),
            10 => Some(Self::FindCoordinator),
            11 => Some(Self::JoinGroup),
            12 => Some(Self::Heartbeat),
            13 => Some(Self::LeaveGroup),
            14 => Some(Self::SyncGroup),
            18 => Some(Self::ApiVersions),
            19 => Some(Self::CreateTopics),
            20 => Some(Self::DeleteTopics),
            _ => None,
        }
    }
}

/// Supported version ranges advertised in ApiVersions.
pub const SUPPORTED_APIS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 0, 3),
    (ApiKey::Fetch, 0, 4),
    (ApiKey::ListOffsets, 0, 1),
    (ApiKey::Metadata, 0, 1),
    (ApiKey::OffsetCommit, 0, 2),
    (ApiKey::OffsetFetch, 0, 1),
    (ApiKey::FindCoordinator, 0, 0),
    (ApiKey::JoinGroup, 0, 1),
    (ApiKey::Heartbeat, 0, 0),
    (ApiKey::LeaveGroup, 0, 0),
    (ApiKey::SyncGroup, 0, 0),
    (ApiKey::ApiVersions, 0, 0),
    (ApiKey::CreateTopics, 0, 1),
    (ApiKey::DeleteTopics, 0, 1),
];
