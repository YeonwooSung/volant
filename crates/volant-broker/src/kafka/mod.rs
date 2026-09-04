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
//! CreatePartitions 0–3 (v3 = v2 wire; no KIP-599),
//! AlterPartitionReassignments v0 (wraps native opcode 114),
//! ListPartitionReassignments v0 (current replicas; empty adding/removing),
//! ElectLeaders v0–1 (preferred = elect_leader(ISR∩live); unclean refused),
//! DescribeUserScramCredentials / AlterUserScramCredentials v0 (wraps ScramStore),
//! DescribeLogDirs 0–1 (local logs only; v1 flexible),
//! DescribeTopicPartitions v0 (wraps Metadata; key 75),
//! UpdateFeatures v0–1 (always flexible; reject every feature; empty ApiVersions features).
//! See `docs/PHASE23_SPEC.md` … `docs/PHASE91_SPEC.md`, `docs/V225_SPEC.md`,
//! `docs/V228_SPEC.md`, `docs/V233_SPEC.md`, `docs/V235_SPEC.md`,
//! `docs/V236_SPEC.md`, `docs/V237_SPEC.md`, and `docs/V244_SPEC.md`.

mod acl_api;
mod admin_api;
/// Kafka wire primitives, MessageSet (magic 0/1), and RecordBatch (magic 2).
pub mod codec;
/// Compression codecs (gzip / snappy / lz4 / zstd); Fetch codec env.
pub mod compress;
/// Fetch session state (Phase 88 + 91 omit + Phase 95 limits + Phase 115 durable + Phase 119 + Phase 138/139 mirror + v0.25 dual-epoch + v0.30 mirror-only converge).
pub mod fetch_session;
mod group_api;
mod handler;
mod meta_api;
/// Produce / Fetch / ListOffsets / OffsetForLeaderEpoch Kafka wire handlers.
pub(crate) mod produce_fetch;
/// SASL PLAIN + SCRAM-SHA-256/512 state machine (Phases 30 / 34).
pub mod sasl;
/// Shared TopicId / topic-name wire identity helpers.
mod topic_id;
/// Transaction API handlers (Init / Add* / End / TxnOffsetCommit).
pub(crate) mod txn;
/// Shared classic/flexible wire read helpers.
pub(crate) mod wire;

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
    /// Not enough in-sync replicas (Kafka `NOT_ENOUGH_REPLICAS`; Phase 135
    /// DeleteRecords majority-wait failure).
    NotEnoughReplicas = 19,
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
    /// Invalid replica assignment (AlterPartitionReassignments).
    InvalidReplicaAssignment = 39,
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
    /// No reassignment in progress (AlterPartitionReassignments cancel).
    NoReassignmentInProgress = 83,
    /// No eligible leader in ISR ∩ live (ElectLeaders). Kafka
    /// `ELIGIBLE_LEADERS_NOT_AVAILABLE`. Unclean (ElectionType 1) is
    /// refused with this code — Volant does not elect outside ISR.
    EligibleLeadersNotAvailable = 87,
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
    /// Resource not found (Describe/AlterUserScramCredentials unknown user).
    ///
    /// Kafka `RESOURCE_NOT_FOUND` (**91**). Not 68 (`NON_EMPTY_GROUP`).
    ResourceNotFound = 91,
    /// Feature update failed (UpdateFeatures). Kafka `FEATURE_UPDATE_FAILED`
    /// (**92**). Volant does not persist finalized features (not KIP-584).
    FeatureUpdateFailed = 92,
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
    /// DescribeLogDirs (v0 classic; v1 flexible). Local partition logs only.
    DescribeLogDirs = 35,
    /// SaslAuthenticate.
    SaslAuthenticate = 36,
    /// CreatePartitions.
    CreatePartitions = 37,
    /// DeleteGroups.
    DeleteGroups = 42,
    /// ElectLeaders (classic v0; flexible v1).
    ElectLeaders = 43,
    /// IncrementalAlterConfigs.
    IncrementalAlterConfigs = 44,
    /// AlterPartitionReassignments (always flexible; v0 only).
    AlterPartitionReassignments = 45,
    /// ListPartitionReassignments (always flexible; v0 only).
    ListPartitionReassignments = 46,
    /// OffsetDelete.
    OffsetDelete = 47,
    /// DescribeUserScramCredentials (always flexible; v0 only).
    DescribeUserScramCredentials = 50,
    /// AlterUserScramCredentials (always flexible; v0 only).
    AlterUserScramCredentials = 51,
    /// UpdateFeatures (always flexible; v0–1). Rejects every feature.
    UpdateFeatures = 57,
    /// DescribeCluster (always flexible).
    DescribeCluster = 60,
    /// DescribeProducers (always flexible).
    DescribeProducers = 61,
    /// DescribeTransactions (always flexible).
    DescribeTransactions = 65,
    /// ListTransactions (always flexible).
    ListTransactions = 66,
    /// DescribeTopicPartitions (always flexible; v0 only).
    DescribeTopicPartitions = 75,
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
            35 => Some(Self::DescribeLogDirs),
            36 => Some(Self::SaslAuthenticate),
            37 => Some(Self::CreatePartitions),
            42 => Some(Self::DeleteGroups),
            43 => Some(Self::ElectLeaders),
            44 => Some(Self::IncrementalAlterConfigs),
            45 => Some(Self::AlterPartitionReassignments),
            46 => Some(Self::ListPartitionReassignments),
            47 => Some(Self::OffsetDelete),
            50 => Some(Self::DescribeUserScramCredentials),
            51 => Some(Self::AlterUserScramCredentials),
            57 => Some(Self::UpdateFeatures),
            60 => Some(Self::DescribeCluster),
            61 => Some(Self::DescribeProducers),
            65 => Some(Self::DescribeTransactions),
            66 => Some(Self::ListTransactions),
            75 => Some(Self::DescribeTopicPartitions),
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
    (ApiKey::DescribeLogDirs, 0, 1),
    (ApiKey::SaslAuthenticate, 0, 2),
    (ApiKey::CreatePartitions, 0, 3),
    (ApiKey::DeleteGroups, 0, 3),
    (ApiKey::ElectLeaders, 0, 1),
    (ApiKey::IncrementalAlterConfigs, 0, 1),
    (ApiKey::AlterPartitionReassignments, 0, 0),
    (ApiKey::ListPartitionReassignments, 0, 0),
    (ApiKey::OffsetDelete, 0, 0),
    (ApiKey::DescribeUserScramCredentials, 0, 0),
    (ApiKey::AlterUserScramCredentials, 0, 0),
    (ApiKey::UpdateFeatures, 0, 1),
    (ApiKey::DescribeCluster, 0, 2),
    (ApiKey::DescribeProducers, 0, 0),
    (ApiKey::DescribeTransactions, 0, 0),
    (ApiKey::ListTransactions, 0, 2),
    (ApiKey::DescribeTopicPartitions, 0, 0),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_apis_includes_describe_log_dirs_35() {
        assert!(SUPPORTED_APIS.len() >= 46);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::DescribeLogDirs && *min == 0 && *max == 1 }));
        assert_eq!(ApiKey::from_i16(35), Some(ApiKey::DescribeLogDirs));
    }

    #[test]
    fn supported_apis_includes_update_features_57() {
        assert!(SUPPORTED_APIS.len() >= 46);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| *k == ApiKey::UpdateFeatures && *min == 0 && *max == 1));
        assert_eq!(ApiKey::from_i16(57), Some(ApiKey::UpdateFeatures));
        assert_eq!(KafkaErrorCode::FeatureUpdateFailed.as_i16(), 92);
    }

    #[test]
    fn supported_apis_includes_elect_leaders_43_and_describe_topic_partitions_75() {
        assert!(SUPPORTED_APIS.len() >= 46);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| *k == ApiKey::ElectLeaders && *min == 0 && *max == 1));
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| *k == ApiKey::SyncGroup && *min == 0 && *max == 5));
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::AlterPartitionReassignments && *min == 0 && *max == 0
        }));
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::ListPartitionReassignments && *min == 0 && *max == 0
        }));
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::DescribeUserScramCredentials && *min == 0 && *max == 0
        }));
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::AlterUserScramCredentials && *min == 0 && *max == 0
        }));
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::DescribeTopicPartitions && *min == 0 && *max == 0
        }));
        assert_eq!(ApiKey::from_i16(43), Some(ApiKey::ElectLeaders));
        assert_eq!(
            ApiKey::from_i16(45),
            Some(ApiKey::AlterPartitionReassignments)
        );
        assert_eq!(
            ApiKey::from_i16(46),
            Some(ApiKey::ListPartitionReassignments)
        );
        assert_eq!(
            ApiKey::from_i16(50),
            Some(ApiKey::DescribeUserScramCredentials)
        );
        assert_eq!(
            ApiKey::from_i16(51),
            Some(ApiKey::AlterUserScramCredentials)
        );
        assert_eq!(ApiKey::from_i16(57), Some(ApiKey::UpdateFeatures));
        assert_eq!(ApiKey::from_i16(75), Some(ApiKey::DescribeTopicPartitions));
    }
}
