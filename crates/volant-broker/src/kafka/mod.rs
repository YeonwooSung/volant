//! Kafka wire protocol shim (Phases 23–91).
//!
//! Classic framing plus flexible APIs (KIP-482): ApiVersions v3–5 (header
//! stays v0; empty feature tags; v5 ClusterId/NodeId ignored), Metadata
//! v9–13 (TopicId; v13 top-level ErrorCode), FindCoordinator v3–6 (batch v4+;
//! v5–6 wire-identical; no share-group key type), Produce v9–13 (TopicId v13; KIP-951 tags),
//! Fetch v12–18 (TopicId v13+; ReplicaState v15+; NodeEndpoints v16+; CurrentLeader tag;
//! DivergingEpoch tag 0 + fetch sessions Phase 88; omit-unchanged Phase 91),
//! CreateTopics v5–7 / DeleteTopics v4–6 (TopicId), group/offset/admin/config/txn
//! flex (ListGroups 0–5 States/Types filter, DescribeGroups 0–6 ErrorMessage,
//! DeleteGroups 0–3 ErrorMessage), ListOffsets v6–11 (max-timestamp / local / tiered specials),
//! OffsetForLeaderEpoch v4, DeleteRecords v2, ACL admin 0–3 (User resource v3),
//! SaslAuthenticate v2, DescribeCluster 0–2, ListTransactions 0–2,
//! DescribeTransactions v0, DescribeProducers v0, KIP-890-era txn max versions
//! (InitProducerId 0–6 OngoingTxn + prepared 2PC MVP Phase 90, AddPartitions/EndTxn 0–5,
//! AddOffsetsToTxn 0–4 wire-identical v3/v4, TxnOffsetCommit 0–6 TopicId),
//! CreatePartitions 0–3 (v3 = v2 wire; no KIP-599).
//! See `docs/PHASE23_SPEC.md` … `docs/PHASE91_SPEC.md`.

/// Kafka wire primitives, MessageSet (magic 0/1), and RecordBatch (magic 2).
pub mod codec;
/// Compression codecs (gzip / snappy / lz4 / zstd); Fetch codec env.
pub mod compress;
/// In-memory Fetch session state (Phase 88 + 91 omit-unchanged).
pub mod fetch_session;
mod handler;
/// Shared TopicId / topic-name wire identity helpers.
mod topic_id;
/// Transaction API handlers (Init / Add* / End / TxnOffsetCommit).
mod txn;
mod meta_api;
mod produce_fetch;
mod group_api;
mod admin_api;
mod acl_api;
/// Shared classic/flexible wire read helpers.
mod wire;
/// SASL PLAIN + SCRAM-SHA-256/512 state machine (Phases 30 / 34).
pub mod sasl;

pub use handler::{serve_kafka_listener, serve_kafka_listener_until};

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
    /// Group still has active members (DeleteGroups).
    NonEmptyGroup = 68,
    /// Group id not found (DeleteGroups).
    GroupIdNotFound = 69,
    /// Fetch session id not found (Fetch v7+ sessions).
    FetchSessionIdNotFound = 70,
    /// Invalid fetch session epoch (Fetch v7+ sessions).
    InvalidFetchSessionEpoch = 71,
    /// Invalid config.
    InvalidConfig = 40,
    /// Not controller for this request (Phase 113 cluster admin).
    NotController = 41,
    /// Out of order sequence number (idempotent produce).
    OutOfOrderSequenceNumber = 45,
    /// Invalid producer epoch.
    InvalidProducerEpoch = 47,
    /// Invalid transaction state.
    InvalidTxnState = 48,
    /// Client transaction timeout exceeds broker max (Phase 96 /
    /// `transaction.max.timeout.ms`). Kafka `INVALID_TRANSACTION_TIMEOUT`.
    InvalidTransactionTimeout = 50,
    /// Unsupported SASL mechanism.
    UnsupportedSaslMechanism = 33,
    /// SASL authentication failed.
    SaslAuthenticationFailed = 58,
    /// Unknown producer id.
    UnknownProducerId = 59,
    /// Fenced leader epoch (OffsetForLeaderEpoch / Fetch fencing).
    FencedLeaderEpoch = 74,
    /// Unknown leader epoch.
    UnknownLeaderEpoch = 75,
    /// Producer fenced (epoch / transactional id fencing).
    ProducerFenced = 90,
    /// Unknown topic id (Metadata by TopicId).
    UnknownTopicId = 100,
    /// Transactional id not found (DescribeTransactions).
    TransactionalIdNotFound = 105,
    /// Unsupported endpoint type (DescribeCluster v1+).
    UnsupportedEndpointType = 115,
    /// Transaction abortable (KIP-890 / Phase 94 honest subset).
    ///
    /// Emitted after open/prepared timeout auto-abort on produce, EndTxn,
    /// AddPartitionsToTxn, AddOffsetsToTxn, and TxnOffsetCommit when the
    /// producer is still in the abortable set. Not emitted on FindCoordinator.
    TransactionAbortable = 123,
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

/// Map Volant idempotent/txn error codes to Kafka wire codes (Phase 29/94).
pub(crate) fn map_idempotent_error(volant: u16) -> i16 {
    match volant {
        // volant_protocol::ErrorCode values
        19 => KafkaErrorCode::InvalidProducerEpoch.as_i16(),
        20 => KafkaErrorCode::OutOfOrderSequenceNumber.as_i16(),
        21 => KafkaErrorCode::UnknownProducerId.as_i16(),
        22 => KafkaErrorCode::InvalidTxnState.as_i16(),
        24 => KafkaErrorCode::TransactionAbortable.as_i16(),
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
    /// DescribeGroups.
    DescribeGroups = 15,
    /// ListGroups.
    ListGroups = 16,
    /// SaslHandshake.
    SaslHandshake = 17,
    /// ApiVersions.
    ApiVersions = 18,
    /// CreateTopics.
    CreateTopics = 19,
    /// DeleteTopics.
    DeleteTopics = 20,
    /// DeleteRecords.
    DeleteRecords = 21,
    /// InitProducerId.
    InitProducerId = 22,
    /// OffsetForLeaderEpoch.
    OffsetForLeaderEpoch = 23,
    /// AddPartitionsToTxn.
    AddPartitionsToTxn = 24,
    /// AddOffsetsToTxn.
    AddOffsetsToTxn = 25,
    /// EndTxn.
    EndTxn = 26,
    /// TxnOffsetCommit.
    TxnOffsetCommit = 28,
    /// DescribeAcls.
    DescribeAcls = 29,
    /// CreateAcls.
    CreateAcls = 30,
    /// DeleteAcls.
    DeleteAcls = 31,
    /// DescribeConfigs.
    DescribeConfigs = 32,
    /// AlterConfigs.
    AlterConfigs = 33,
    /// SaslAuthenticate.
    SaslAuthenticate = 36,
    /// CreatePartitions.
    CreatePartitions = 37,
    /// DeleteGroups.
    DeleteGroups = 42,
    /// IncrementalAlterConfigs.
    IncrementalAlterConfigs = 44,
    /// OffsetDelete.
    OffsetDelete = 47,
    /// DescribeCluster (always flexible).
    DescribeCluster = 60,
    /// DescribeProducers (always flexible).
    DescribeProducers = 61,
    /// DescribeTransactions (always flexible).
    DescribeTransactions = 65,
    /// ListTransactions (always flexible).
    ListTransactions = 66,
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
            15 => Some(Self::DescribeGroups),
            16 => Some(Self::ListGroups),
            17 => Some(Self::SaslHandshake),
            18 => Some(Self::ApiVersions),
            19 => Some(Self::CreateTopics),
            20 => Some(Self::DeleteTopics),
            21 => Some(Self::DeleteRecords),
            22 => Some(Self::InitProducerId),
            23 => Some(Self::OffsetForLeaderEpoch),
            24 => Some(Self::AddPartitionsToTxn),
            25 => Some(Self::AddOffsetsToTxn),
            26 => Some(Self::EndTxn),
            28 => Some(Self::TxnOffsetCommit),
            29 => Some(Self::DescribeAcls),
            30 => Some(Self::CreateAcls),
            31 => Some(Self::DeleteAcls),
            32 => Some(Self::DescribeConfigs),
            33 => Some(Self::AlterConfigs),
            36 => Some(Self::SaslAuthenticate),
            37 => Some(Self::CreatePartitions),
            42 => Some(Self::DeleteGroups),
            44 => Some(Self::IncrementalAlterConfigs),
            47 => Some(Self::OffsetDelete),
            60 => Some(Self::DescribeCluster),
            61 => Some(Self::DescribeProducers),
            65 => Some(Self::DescribeTransactions),
            66 => Some(Self::ListTransactions),
            _ => None,
        }
    }
}

/// Supported version ranges advertised in ApiVersions.
pub const SUPPORTED_APIS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 0, 13),
    (ApiKey::Fetch, 0, 18),
    (ApiKey::ListOffsets, 0, 11),
    (ApiKey::Metadata, 0, 13),
    (ApiKey::OffsetCommit, 0, 10),
    (ApiKey::OffsetFetch, 0, 10),
    (ApiKey::FindCoordinator, 0, 6),
    (ApiKey::JoinGroup, 0, 9),
    (ApiKey::Heartbeat, 0, 4),
    (ApiKey::LeaveGroup, 0, 5),
    (ApiKey::SyncGroup, 0, 5),
    (ApiKey::DescribeGroups, 0, 6),
    (ApiKey::ListGroups, 0, 5),
    (ApiKey::SaslHandshake, 0, 1),
    (ApiKey::ApiVersions, 0, 5),
    (ApiKey::CreateTopics, 0, 7),
    (ApiKey::DeleteTopics, 0, 6),
    (ApiKey::DeleteRecords, 0, 2),
    (ApiKey::InitProducerId, 0, 6),
    (ApiKey::OffsetForLeaderEpoch, 0, 4),
    (ApiKey::AddPartitionsToTxn, 0, 5),
    (ApiKey::AddOffsetsToTxn, 0, 4),
    (ApiKey::EndTxn, 0, 5),
    (ApiKey::TxnOffsetCommit, 0, 6),
    (ApiKey::DescribeAcls, 0, 3),
    (ApiKey::CreateAcls, 0, 3),
    (ApiKey::DeleteAcls, 0, 3),
    (ApiKey::DescribeConfigs, 0, 4),
    (ApiKey::AlterConfigs, 0, 2),
    (ApiKey::SaslAuthenticate, 0, 2),
    (ApiKey::CreatePartitions, 0, 3),
    (ApiKey::DeleteGroups, 0, 3),
    (ApiKey::IncrementalAlterConfigs, 0, 1),
    (ApiKey::OffsetDelete, 0, 0),
    (ApiKey::DescribeCluster, 0, 2),
    (ApiKey::DescribeProducers, 0, 0),
    (ApiKey::DescribeTransactions, 0, 0),
    (ApiKey::ListTransactions, 0, 2),
];
