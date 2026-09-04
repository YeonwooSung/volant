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
//! DescribeClientQuotas / AlterClientQuotas v0 (no quota store; describe empty, alter 42),
//! DescribeDelegationToken v0 (no token store; empty tokens; treat v0 as flex),
//! ListClientMetricsResources v0 (no client-metrics store; empty resources),
//! AlterReplicaLogDirs 0–1 (reject every move; single data_dir; v1 flexible),
//! AssignReplicasToDirs v0 (always flexible; reject every assignment; single data_dir),
//! DescribeLogDirs 0–1 (local logs only; v1 flexible),
//! DescribeTopicPartitions v0 (wraps Metadata; key 75),
//! ShareGroupDescribe v1 (key 77 reject; not KIP-932; official v0 removed),
//! UpdateRaftVoter v0 (key 82 reject; not KRaft raft voter),
//! BrokerRegistration v0 (key 62 reject; not KRaft / not AddBroker),
//! BrokerHeartbeat v0 (key 63 reject; not KRaft / not native Heartbeat 12),
//! FetchSnapshot v0 (key 59 reject; not KRaft snapshot / not InstallSnapshot),
//! UnregisterBroker v0 (wraps native remove_broker; key 64; not KRaft incarnation),
//! UpdateFeatures v0–1 (always flexible; reject every feature; empty ApiVersions features),
//! DescribeQuorum v0–1 (always flexible; wraps openraft leader/term/voters; not KRaft),
//! AllocateProducerIds v0 (always flexible; block from next_producer_id; not KRaft),
//! AlterPartition v0 (always flexible; wraps apply_leader_isr_update; not KRaft),
//! WriteTxnMarkers 0–1 (classic v0 / flex v1; replica-local COMMIT/ABORT control
//! batches + soft `__txn_markers`; not EndTxn / not a coordinator),
//! GetTelemetrySubscriptions v0 (always flexible; no client telemetry; empty subscription),
//! PushTelemetry v0 (always flexible; no client telemetry; reject every push),
//! CreateDelegationToken v0 (always flexible; no token store; reject 42),
//! RenewDelegationToken v0 (always flexible; no token store; reject 42),
//! ExpireDelegationToken v0 (always flexible; no token store; reject 42),
//! DescribeDelegationToken v0 (always flexible residual; no token store; empty tokens),
//! ConsumerGroupDescribe v0 (always flexible; classic snapshot wrap; not KIP-848),
//! ConsumerGroupHeartbeat v0 (always flexible; reject 42; not KIP-848),
//! ShareGroupHeartbeat v1 (always flexible; reject 42; not KIP-932),
//! ShareGroupDescribe v1 (key 77 reject; not KIP-932; official v0 removed),
//! ShareAcknowledge v1 (always flexible; reject 42; not KIP-932),
//! Envelope v0 (key 58 reject; forwarding not supported; not KIP-590),
//! ControllerRegistration v0 (key 70 reject; not KRaft / not AddBroker),
//! Vote v0 (key 52 reject; not KRaft vote / not openraft RequestVote),
//! AddRaftVoter v0 (key 80 reject; not KRaft raft voter / not AddBroker),
//! RemoveRaftVoter v0 (key 81 reject; not KRaft / not remove_broker),
//! UpdateRaftVoter v0 (key 82 reject; not KRaft voter set),
//! InitializeShareGroupState v0 (key 83 reject; not KIP-932 share state),
//! ReadShareGroupState v0 (key 84 reject; not KIP-932 share state),
//! WriteShareGroupState v0 (key 85 reject; not KIP-932 share state),
//! DeleteShareGroupState v0 (key 86 reject; not KIP-932 share state),
//! ReadShareGroupStateSummary v0 (key 87 reject; not KIP-932 share state),
//! DescribeShareGroupOffsets v0 (key 90 reject; not KIP-932 share offsets),
//! DeleteShareGroupOffsets v0 (key 92 reject; not KIP-932 share offsets),
//! UnregisterController v0 (key 94 reject; not KRaft / not UnregisterBroker),
//! ShareFetch v1 (key 78 reject; not KIP-932 share fetch / not Fetch 1).
//! See `docs/PHASE23_SPEC.md` … `docs/PHASE91_SPEC.md`, `docs/V225_SPEC.md`,
//! `docs/V228_SPEC.md`, `docs/V233_SPEC.md`, `docs/V235_SPEC.md`,
//! `docs/V236_SPEC.md`, `docs/V237_SPEC.md`, `docs/V241_SPEC.md`,
//! `docs/V242_SPEC.md`, `docs/V244_SPEC.md`, `docs/V245_SPEC.md`,
//! `docs/V246_SPEC.md`, `docs/V249_SPEC.md`, `docs/V250_SPEC.md`,
//! `docs/V251_SPEC.md`, `docs/V252_SPEC.md`, `docs/V253_SPEC.md`,
//! `docs/V255_SPEC.md`, `docs/V257_SPEC.md`, `docs/V258_SPEC.md`,
//! `docs/V259_SPEC.md`, `docs/V260_SPEC.md`, `docs/V261_SPEC.md`,
//! `docs/V263_SPEC.md`, `docs/V264_SPEC.md`, `docs/V265_SPEC.md`,
//! `docs/V266_SPEC.md`, `docs/V267_SPEC.md`, `docs/V268_SPEC.md`,
//! `docs/V269_SPEC.md`, `docs/V270_SPEC.md`, `docs/V271_SPEC.md`,
//! `docs/V272_SPEC.md`, `docs/V273_SPEC.md`, `docs/V274_SPEC.md`,
//! `docs/V275_SPEC.md`, `docs/V276_SPEC.md`, `docs/V277_SPEC.md`,
//! `docs/V278_SPEC.md`, `docs/V279_SPEC.md`, `docs/V280_SPEC.md`,
//! `docs/V281_SPEC.md`, `docs/V282_SPEC.md`, `docs/V283_SPEC.md`,
//! `docs/V284_SPEC.md`, and `docs/V288_SPEC.md`.

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
/// Transaction API handlers (Init / Add* / End / WriteTxnMarkers / TxnOffsetCommit).
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
    /// Committed offset is still unstable (OffsetFetch RequireStable).
    ///
    /// Kafka `UNSTABLE_OFFSET_COMMIT` (**81**). Emitted on OffsetFetch v7+
    /// when `require_stable` is set and the committed offset sits in an
    /// open/prepared write-through txn range. Offset is returned as **-1**.
    UnstableOffsetCommit = 81,
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
    /// Transactional id authorization failed (v0.247). Kafka
    /// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`.
    TransactionalIdAuthorizationFailed = 53,
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
    /// WriteTxnMarkers (classic v0; flexible v1). Replica-local COMMIT/ABORT
    /// control batches + soft `__txn_markers`. Not EndTxn / not a coordinator.
    WriteTxnMarkers = 27,
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
    /// AlterReplicaLogDirs (v0 classic; v1 flexible). Single `data_dir`;
    /// every move is rejected. Official Kafka first flexible version is 2.
    AlterReplicaLogDirs = 34,
    /// DescribeLogDirs (v0 classic; v1 flexible). Local partition logs only.
    DescribeLogDirs = 35,
    /// SaslAuthenticate.
    SaslAuthenticate = 36,
    /// CreatePartitions.
    CreatePartitions = 37,
    /// CreateDelegationToken (always flexible; v0 only). No token store;
    /// every create is rejected. Official Kafka first flexible version is 2.
    CreateDelegationToken = 38,
    /// RenewDelegationToken (always flexible; v0 only). No token store;
    /// every renew is rejected. Official Kafka first flexible version is 2.
    RenewDelegationToken = 39,
    /// ExpireDelegationToken (always flexible; v0 only). No token store;
    /// every expire is rejected. Official Kafka first flexible version is 2.
    ExpireDelegationToken = 40,
    /// DescribeDelegationToken (always flexible residual; v0 only).
    /// Official Kafka first flexible version is 2. No token store.
    DescribeDelegationToken = 41,
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
    /// DescribeClientQuotas (always flexible; v0 only). No quota store.
    DescribeClientQuotas = 48,
    /// AlterClientQuotas (always flexible; v0 only). Rejected; no persist.
    AlterClientQuotas = 49,
    /// DescribeUserScramCredentials (always flexible; v0 only).
    DescribeUserScramCredentials = 50,
    /// AlterUserScramCredentials (always flexible; v0 only).
    AlterUserScramCredentials = 51,
    /// Vote (always flexible; v0 only). Honest reject: not a KRaft
    /// controller; does not wrap openraft RequestVote. No vote granted.
    Vote = 52,
    /// DescribeQuorum (always flexible; v0–1). Wraps openraft
    /// leader/term/voters. Not KRaft `__cluster_metadata`.
    DescribeQuorum = 55,
    /// AlterPartition (always flexible; v0 only). Wraps
    /// `apply_leader_isr_update`. BrokerEpoch parsed and ignored. Not
    /// KRaft NewIsrEpoch / ELR / DirectoryId.
    AlterPartition = 56,
    /// UpdateFeatures (always flexible; v0–1). Rejects every feature.
    UpdateFeatures = 57,
    /// Envelope (always flexible; v0 only). Honest reject: Volant has
    /// no request forwarding (not KIP-590). Embedded RequestData is
    /// discarded; nothing is unwrapped or executed.
    Envelope = 58,
    /// FetchSnapshot (always flexible; v0 only). Honest reject:
    /// not a KRaft controller; does not serve metadata snapshots.
    /// Does not wrap native InstallSnapshot 112/113.
    FetchSnapshot = 59,
    /// DescribeCluster (always flexible).
    DescribeCluster = 60,
    /// DescribeProducers (always flexible).
    DescribeProducers = 61,
    /// BrokerRegistration (always flexible; v0 only). Honest reject:
    /// not KRaft (no incarnation / DirectoryId / features). Does not
    /// wrap native AddBroker. Overlay membership is unchanged.
    BrokerRegistration = 62,
    /// BrokerHeartbeat (always flexible; v0 only). Honest reject:
    /// not KRaft (no fencing / metadata offset / assigned epoch). Does
    /// not wrap native Heartbeat (key 12). Overlay membership is unchanged.
    BrokerHeartbeat = 63,
    /// UnregisterBroker (always flexible; v0 only). Wraps native
    /// `remove_broker`. Not Kafka KRaft incarnation / DirectoryId.
    UnregisterBroker = 64,
    /// DescribeTransactions (always flexible).
    DescribeTransactions = 65,
    /// ListTransactions (always flexible).
    ListTransactions = 66,
    /// AllocateProducerIds (always flexible; v0 only). Block from
    /// `next_producer_id`. BrokerEpoch parsed and ignored. Not KRaft.
    AllocateProducerIds = 67,
    /// ConsumerGroupHeartbeat (always flexible; v0 only). Honest reject:
    /// not KIP-848 consumer protocol. Does not wrap classic Heartbeat 12.
    ConsumerGroupHeartbeat = 68,
    /// ConsumerGroupDescribe (always flexible; v0 only). Wraps
    /// `GroupCoordinator::describe_group` (same snapshot as DescribeGroups).
    /// Not KIP-848: memberEpoch = -1, classic groups only.
    ConsumerGroupDescribe = 69,
    /// ControllerRegistration (always flexible; v0 only). Honest reject:
    /// not a KRaft controller (no incarnation / ZK migration / listener
    /// store). Does not wrap native AddBroker. Overlay membership is
    /// unchanged.
    ControllerRegistration = 70,
    /// GetTelemetrySubscriptions (always flexible; v0 only). No client
    /// telemetry (not KIP-714). Empty subscription; do not push.
    GetTelemetrySubscriptions = 71,
    /// PushTelemetry (always flexible; v0 only). No client telemetry
    /// (not KIP-714). Parse and reject; metrics are discarded.
    PushTelemetry = 72,
    /// AssignReplicasToDirs (always flexible; v0 only). Single `data_dir`;
    /// every assignment is rejected. Not KRaft DirectoryId.
    AssignReplicasToDirs = 73,
    /// ListClientMetricsResources (always flexible; v0 only). No
    /// client-metrics resource store (KIP-714). Empty list.
    ListClientMetricsResources = 74,
    /// DescribeTopicPartitions (always flexible; v0 only).
    DescribeTopicPartitions = 75,
    /// ShareGroupHeartbeat (always flexible; v1 only). Honest reject:
    /// not KIP-932 share group. Does not wrap classic Heartbeat 12 or
    /// ConsumerGroupHeartbeat 68. Official v0 was removed in Kafka 4.1.
    ShareGroupHeartbeat = 76,
    /// ShareGroupDescribe (always flexible; v1 only). Honest reject:
    /// not KIP-932 share groups. Official validVersions is 1 only
    /// (v0 was EA in Kafka 4.0 and removed in 4.1). Does not wrap
    /// `describe_group` / ConsumerGroupDescribe 69 / DescribeGroups 15.
    ShareGroupDescribe = 77,
    /// ShareFetch (always flexible; v1 only). Honest reject: not
    /// KIP-932 share fetch. Does not wrap Kafka Fetch 1 or native
    /// Fetch. Does not acquire records or create a share session.
    /// Official validVersions is 1–2 (v0 EA removed in Kafka 4.1);
    /// Volant advertises 1 only (v2 ShareAcquireMode / Renew is out
    /// of range).
    ShareFetch = 78,
    /// ShareAcknowledge (always flexible; v1 only). Honest reject:
    /// not KIP-932 share acknowledge. Does not wrap OffsetCommit / Fetch.
    /// Offsets and record state are unchanged.
    ShareAcknowledge = 79,
    /// AddRaftVoter (always flexible; v0 only). Honest reject: not a
    /// KRaft raft voter (membership is overlay + native AddBroker).
    /// Does not wrap native AddBroker. Overlay membership is unchanged.
    AddRaftVoter = 80,
    /// RemoveRaftVoter (always flexible; v0 only). Honest reject:
    /// not KRaft (no voter set / DirectoryId). Does not wrap native
    /// `remove_broker`. Overlay membership is unchanged.
    RemoveRaftVoter = 81,
    /// UpdateRaftVoter (always flexible; v0 only). Honest reject:
    /// not a KRaft voter set (no listener / KRaftVersionFeature store).
    UpdateRaftVoter = 82,
    /// InitializeShareGroupState (always flexible; v0 only). Honest
    /// reject: not KIP-932 share-partition state. Does not persist
    /// share state and does not wrap OffsetCommit.
    InitializeShareGroupState = 83,
    /// ReadShareGroupState (always flexible; v0 only). Honest reject:
    /// not KIP-932 share-partition state. Does not persist share
    /// state and does not wrap OffsetFetch / OffsetCommit /
    /// InitializeShareGroupState.
    ReadShareGroupState = 84,
    /// WriteShareGroupState (always flexible; v0 only). Honest
    /// reject: not KIP-932 share-partition state. Does not persist
    /// share state and does not wrap OffsetCommit /
    /// InitializeShareGroupState. Official validVersions is 0–1
    /// (v1 = DeliveryCompleteCount / KIP-1226); Volant advertises v0.
    WriteShareGroupState = 85,
    /// DeleteShareGroupState (always flexible; v0 only). Honest
    /// reject: not KIP-932 share-partition state. Does not persist
    /// and does not wrap OffsetCommit / DeleteGroups /
    /// InitializeShareGroupState.
    DeleteShareGroupState = 86,
    /// ReadShareGroupStateSummary (always flexible; v0 only). Honest
    /// reject: not KIP-932 share-partition state. Does not persist
    /// and does not wrap OffsetFetch / OffsetCommit /
    /// InitializeShareGroupState / ReadShareGroupState. Official
    /// validVersions is 0–1 (v1 adds DeliveryCompleteCount); Volant
    /// advertises v0 only.
    ReadShareGroupStateSummary = 87,
    /// DescribeShareGroupOffsets (always flexible; v0 only). Honest
    /// reject: not KIP-932 share offsets. Does not persist and does
    /// not wrap OffsetFetch 9 / describe_group / ConsumerGroupDescribe
    /// 69 / ShareGroupDescribe 77. Official validVersions is 0–1
    /// (v1 adds Lag / KIP-1226); Volant advertises 0 only.
    DescribeShareGroupOffsets = 90,
    /// DeleteShareGroupOffsets (always flexible; v0 only). Honest
    /// reject: not KIP-932 share offsets. Does not persist and does
    /// not wrap OffsetCommit 8 / DeleteGroups 42 / OffsetDelete 47 /
    /// DescribeShareGroupOffsets 90. Official validVersions is 0 only.
    DeleteShareGroupOffsets = 92,
    /// UnregisterController (always flexible; v0 only). Honest reject:
    /// not a KRaft controller (no unregister record). Does not wrap
    /// native `remove_broker`. Overlay membership is unchanged.
    UnregisterController = 94,
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
            27 => Some(Self::WriteTxnMarkers),
            28 => Some(Self::TxnOffsetCommit),
            29 => Some(Self::DescribeAcls),
            30 => Some(Self::CreateAcls),
            31 => Some(Self::DeleteAcls),
            32 => Some(Self::DescribeConfigs),
            33 => Some(Self::AlterConfigs),
            34 => Some(Self::AlterReplicaLogDirs),
            35 => Some(Self::DescribeLogDirs),
            36 => Some(Self::SaslAuthenticate),
            37 => Some(Self::CreatePartitions),
            38 => Some(Self::CreateDelegationToken),
            39 => Some(Self::RenewDelegationToken),
            40 => Some(Self::ExpireDelegationToken),
            41 => Some(Self::DescribeDelegationToken),
            42 => Some(Self::DeleteGroups),
            43 => Some(Self::ElectLeaders),
            44 => Some(Self::IncrementalAlterConfigs),
            45 => Some(Self::AlterPartitionReassignments),
            46 => Some(Self::ListPartitionReassignments),
            47 => Some(Self::OffsetDelete),
            48 => Some(Self::DescribeClientQuotas),
            49 => Some(Self::AlterClientQuotas),
            50 => Some(Self::DescribeUserScramCredentials),
            51 => Some(Self::AlterUserScramCredentials),
            52 => Some(Self::Vote),
            55 => Some(Self::DescribeQuorum),
            56 => Some(Self::AlterPartition),
            57 => Some(Self::UpdateFeatures),
            58 => Some(Self::Envelope),
            59 => Some(Self::FetchSnapshot),
            60 => Some(Self::DescribeCluster),
            61 => Some(Self::DescribeProducers),
            62 => Some(Self::BrokerRegistration),
            63 => Some(Self::BrokerHeartbeat),
            64 => Some(Self::UnregisterBroker),
            65 => Some(Self::DescribeTransactions),
            66 => Some(Self::ListTransactions),
            67 => Some(Self::AllocateProducerIds),
            68 => Some(Self::ConsumerGroupHeartbeat),
            69 => Some(Self::ConsumerGroupDescribe),
            70 => Some(Self::ControllerRegistration),
            71 => Some(Self::GetTelemetrySubscriptions),
            72 => Some(Self::PushTelemetry),
            73 => Some(Self::AssignReplicasToDirs),
            74 => Some(Self::ListClientMetricsResources),
            75 => Some(Self::DescribeTopicPartitions),
            76 => Some(Self::ShareGroupHeartbeat),
            77 => Some(Self::ShareGroupDescribe),
            78 => Some(Self::ShareFetch),
            79 => Some(Self::ShareAcknowledge),
            80 => Some(Self::AddRaftVoter),
            81 => Some(Self::RemoveRaftVoter),
            82 => Some(Self::UpdateRaftVoter),
            83 => Some(Self::InitializeShareGroupState),
            84 => Some(Self::ReadShareGroupState),
            85 => Some(Self::WriteShareGroupState),
            86 => Some(Self::DeleteShareGroupState),
            87 => Some(Self::ReadShareGroupStateSummary),
            90 => Some(Self::DescribeShareGroupOffsets),
            92 => Some(Self::DeleteShareGroupOffsets),
            94 => Some(Self::UnregisterController),
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
    (ApiKey::WriteTxnMarkers, 0, 1),
    (ApiKey::TxnOffsetCommit, 0, 6),
    (ApiKey::DescribeAcls, 0, 3),
    (ApiKey::CreateAcls, 0, 3),
    (ApiKey::DeleteAcls, 0, 3),
    (ApiKey::DescribeConfigs, 0, 4),
    (ApiKey::AlterConfigs, 0, 2),
    (ApiKey::AlterReplicaLogDirs, 0, 1),
    (ApiKey::DescribeLogDirs, 0, 1),
    (ApiKey::SaslAuthenticate, 0, 2),
    (ApiKey::CreatePartitions, 0, 3),
    (ApiKey::CreateDelegationToken, 0, 0),
    (ApiKey::RenewDelegationToken, 0, 0),
    (ApiKey::ExpireDelegationToken, 0, 0),
    (ApiKey::DescribeDelegationToken, 0, 0),
    (ApiKey::DeleteGroups, 0, 3),
    (ApiKey::ElectLeaders, 0, 1),
    (ApiKey::IncrementalAlterConfigs, 0, 1),
    (ApiKey::AlterPartitionReassignments, 0, 0),
    (ApiKey::ListPartitionReassignments, 0, 0),
    (ApiKey::OffsetDelete, 0, 0),
    (ApiKey::DescribeClientQuotas, 0, 0),
    (ApiKey::AlterClientQuotas, 0, 0),
    (ApiKey::DescribeUserScramCredentials, 0, 0),
    (ApiKey::AlterUserScramCredentials, 0, 0),
    (ApiKey::Vote, 0, 0),
    (ApiKey::DescribeQuorum, 0, 1),
    (ApiKey::AlterPartition, 0, 0),
    (ApiKey::UpdateFeatures, 0, 1),
    (ApiKey::Envelope, 0, 0),
    (ApiKey::FetchSnapshot, 0, 0),
    (ApiKey::DescribeCluster, 0, 2),
    (ApiKey::DescribeProducers, 0, 0),
    (ApiKey::BrokerRegistration, 0, 0),
    (ApiKey::BrokerHeartbeat, 0, 0),
    (ApiKey::UnregisterBroker, 0, 0),
    (ApiKey::DescribeTransactions, 0, 0),
    (ApiKey::ListTransactions, 0, 2),
    (ApiKey::AllocateProducerIds, 0, 0),
    (ApiKey::ConsumerGroupHeartbeat, 0, 0),
    (ApiKey::ConsumerGroupDescribe, 0, 0),
    (ApiKey::ControllerRegistration, 0, 0),
    (ApiKey::GetTelemetrySubscriptions, 0, 0),
    (ApiKey::PushTelemetry, 0, 0),
    (ApiKey::AssignReplicasToDirs, 0, 0),
    (ApiKey::ListClientMetricsResources, 0, 0),
    (ApiKey::DescribeTopicPartitions, 0, 0),
    (ApiKey::ShareGroupHeartbeat, 1, 1),
    (ApiKey::ShareGroupDescribe, 1, 1),
    (ApiKey::ShareFetch, 1, 1),
    (ApiKey::ShareAcknowledge, 1, 1),
    (ApiKey::AddRaftVoter, 0, 0),
    (ApiKey::RemoveRaftVoter, 0, 0),
    (ApiKey::UpdateRaftVoter, 0, 0),
    (ApiKey::InitializeShareGroupState, 0, 0),
    (ApiKey::ReadShareGroupState, 0, 0),
    (ApiKey::WriteShareGroupState, 0, 0),
    (ApiKey::DeleteShareGroupState, 0, 0),
    (ApiKey::ReadShareGroupStateSummary, 0, 0),
    (ApiKey::DescribeShareGroupOffsets, 0, 0),
    (ApiKey::DeleteShareGroupOffsets, 0, 0),
    (ApiKey::UnregisterController, 0, 0),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_apis_includes_alter_replica_log_dirs_34() {
        assert!(SUPPORTED_APIS.len() >= 50);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::AlterReplicaLogDirs && *min == 0 && *max == 1 }));
        assert_eq!(ApiKey::from_i16(34), Some(ApiKey::AlterReplicaLogDirs));
    }

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
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::DescribeClientQuotas && *min == 0 && *max == 0 }));
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::AlterClientQuotas && *min == 0 && *max == 0 }));
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
        assert_eq!(ApiKey::from_i16(48), Some(ApiKey::DescribeClientQuotas));
        assert_eq!(ApiKey::from_i16(49), Some(ApiKey::AlterClientQuotas));
    }

    #[test]
    fn supported_apis_includes_client_quotas_48_49() {
        assert!(SUPPORTED_APIS.len() >= 46);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::DescribeClientQuotas && *min == 0 && *max == 0 }));
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::AlterClientQuotas && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(48), Some(ApiKey::DescribeClientQuotas));
        assert_eq!(ApiKey::from_i16(49), Some(ApiKey::AlterClientQuotas));
    }

    #[test]
    fn supported_apis_includes_broker_registration_62() {
        assert!(SUPPORTED_APIS.len() >= 61);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::BrokerRegistration && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(62), Some(ApiKey::BrokerRegistration));
        assert_eq!(ApiKey::from_i16(64), Some(ApiKey::UnregisterBroker));
    }

    #[test]
    fn supported_apis_includes_broker_heartbeat_63() {
        assert!(SUPPORTED_APIS.len() >= 65);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::BrokerHeartbeat && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(63), Some(ApiKey::BrokerHeartbeat));
        assert_eq!(ApiKey::from_i16(62), Some(ApiKey::BrokerRegistration));
        assert_eq!(ApiKey::from_i16(64), Some(ApiKey::UnregisterBroker));
    }

    #[test]
    fn supported_apis_includes_unregister_broker_64() {
        assert!(SUPPORTED_APIS.len() >= 46);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::UnregisterBroker && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(64), Some(ApiKey::UnregisterBroker));
    }

    #[test]
    fn supported_apis_includes_vote_52() {
        assert!(SUPPORTED_APIS.len() >= 70);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| *k == ApiKey::Vote && *min == 0 && *max == 0));
        assert_eq!(ApiKey::from_i16(52), Some(ApiKey::Vote));
        assert_eq!(ApiKey::from_i16(55), Some(ApiKey::DescribeQuorum));
    }

    #[test]
    fn supported_apis_includes_describe_quorum_55() {
        assert!(SUPPORTED_APIS.len() >= 50);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| *k == ApiKey::DescribeQuorum && *min == 0 && *max == 1));
        assert_eq!(ApiKey::from_i16(55), Some(ApiKey::DescribeQuorum));
    }

    #[test]
    fn supported_apis_includes_allocate_producer_ids_67() {
        assert!(SUPPORTED_APIS.len() >= 50);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::AllocateProducerIds && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(67), Some(ApiKey::AllocateProducerIds));
    }

    #[test]
    fn supported_apis_includes_write_txn_markers_27() {
        assert!(SUPPORTED_APIS.len() >= 53);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::WriteTxnMarkers && *min == 0 && *max == 1 }));
        assert_eq!(ApiKey::from_i16(27), Some(ApiKey::WriteTxnMarkers));
    }

    #[test]
    fn supported_apis_includes_get_telemetry_subscriptions_71() {
        assert!(SUPPORTED_APIS.len() >= 53);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::GetTelemetrySubscriptions && *min == 0 && *max == 0
        }));
        assert_eq!(
            ApiKey::from_i16(71),
            Some(ApiKey::GetTelemetrySubscriptions)
        );
    }

    #[test]
    fn supported_apis_includes_push_telemetry_72() {
        assert!(SUPPORTED_APIS.len() >= 57);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| *k == ApiKey::PushTelemetry && *min == 0 && *max == 0));
        assert_eq!(ApiKey::from_i16(72), Some(ApiKey::PushTelemetry));
    }

    #[test]
    fn supported_apis_includes_assign_replicas_to_dirs_73() {
        assert!(SUPPORTED_APIS.len() >= 53);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::AssignReplicasToDirs && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(73), Some(ApiKey::AssignReplicasToDirs));
    }

    #[test]
    fn supported_apis_includes_list_client_metrics_resources_74() {
        assert!(SUPPORTED_APIS.len() >= 53);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::ListClientMetricsResources && *min == 0 && *max == 0
        }));
        assert_eq!(
            ApiKey::from_i16(74),
            Some(ApiKey::ListClientMetricsResources)
        );
    }

    #[test]
    fn unstable_offset_commit_is_81() {
        assert_eq!(KafkaErrorCode::UnstableOffsetCommit.as_i16(), 81);
    }

    #[test]
    fn supported_apis_includes_alter_partition_56() {
        assert!(SUPPORTED_APIS.len() >= 57);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::AlterPartition && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(56), Some(ApiKey::AlterPartition));
    }

    #[test]
    fn supported_apis_includes_create_delegation_token_38() {
        assert!(SUPPORTED_APIS.len() >= 57);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::CreateDelegationToken && *min == 0 && *max == 0
        }));
        assert_eq!(ApiKey::from_i16(38), Some(ApiKey::CreateDelegationToken));
    }

    #[test]
    fn supported_apis_includes_describe_delegation_token_41() {
        assert!(SUPPORTED_APIS.len() >= 57);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::DescribeDelegationToken && *min == 0 && *max == 0
        }));
        assert_eq!(ApiKey::from_i16(41), Some(ApiKey::DescribeDelegationToken));
    }

    #[test]
    fn supported_apis_includes_consumer_group_describe_69() {
        assert!(SUPPORTED_APIS.len() >= 61);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::ConsumerGroupDescribe && *min == 0 && *max == 0
        }));
        assert_eq!(ApiKey::from_i16(69), Some(ApiKey::ConsumerGroupDescribe));
        assert_eq!(KafkaErrorCode::GroupIdNotFound.as_i16(), 69);
    }

    #[test]
    fn supported_apis_includes_consumer_group_heartbeat_68() {
        assert!(SUPPORTED_APIS.len() >= 65);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::ConsumerGroupHeartbeat && *min == 0 && *max == 0
        }));
        assert_eq!(ApiKey::from_i16(68), Some(ApiKey::ConsumerGroupHeartbeat));
        assert_eq!(ApiKey::from_i16(67), Some(ApiKey::AllocateProducerIds));
        assert_eq!(ApiKey::from_i16(69), Some(ApiKey::ConsumerGroupDescribe));
    }

    #[test]
    fn supported_apis_includes_renew_delegation_token_39() {
        assert!(SUPPORTED_APIS.len() >= 61);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::RenewDelegationToken && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(39), Some(ApiKey::RenewDelegationToken));
    }

    #[test]
    fn supported_apis_includes_expire_delegation_token_40() {
        assert!(SUPPORTED_APIS.len() >= 61);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::ExpireDelegationToken && *min == 0 && *max == 0
        }));
        assert_eq!(ApiKey::from_i16(40), Some(ApiKey::ExpireDelegationToken));
    }

    #[test]
    fn supported_apis_includes_envelope_58() {
        assert!(SUPPORTED_APIS.len() >= 65);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| *k == ApiKey::Envelope && *min == 0 && *max == 0));
        assert_eq!(ApiKey::from_i16(58), Some(ApiKey::Envelope));
        assert_eq!(ApiKey::from_i16(57), Some(ApiKey::UpdateFeatures));
        assert_eq!(ApiKey::from_i16(60), Some(ApiKey::DescribeCluster));
    }

    #[test]
    fn supported_apis_includes_fetch_snapshot_59() {
        assert!(SUPPORTED_APIS.len() >= 65);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::FetchSnapshot && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(59), Some(ApiKey::FetchSnapshot));
        assert_eq!(ApiKey::from_i16(60), Some(ApiKey::DescribeCluster));
    }

    #[test]
    fn supported_apis_includes_controller_registration_70() {
        assert!(SUPPORTED_APIS.len() >= 65);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::ControllerRegistration && *min == 0 && *max == 0
        }));
        assert_eq!(ApiKey::from_i16(70), Some(ApiKey::ControllerRegistration));
    }

    #[test]
    fn supported_apis_includes_share_group_heartbeat_76() {
        assert!(SUPPORTED_APIS.len() >= 75);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::ShareGroupHeartbeat && *min == 1 && *max == 1 }));
        assert_eq!(ApiKey::from_i16(76), Some(ApiKey::ShareGroupHeartbeat));
        assert_eq!(ApiKey::from_i16(75), Some(ApiKey::DescribeTopicPartitions));
        assert_eq!(ApiKey::from_i16(68), Some(ApiKey::ConsumerGroupHeartbeat));
        assert_eq!(ApiKey::from_i16(12), Some(ApiKey::Heartbeat));
    }

    #[test]
    fn supported_apis_includes_share_fetch_78() {
        assert!(SUPPORTED_APIS.len() >= 75);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| *k == ApiKey::ShareFetch && *min == 1 && *max == 1));
        assert_eq!(ApiKey::from_i16(78), Some(ApiKey::ShareFetch));
        assert_eq!(ApiKey::from_i16(1), Some(ApiKey::Fetch));
    }

    #[test]
    fn supported_apis_includes_share_acknowledge_79() {
        assert!(SUPPORTED_APIS.len() >= 75);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::ShareAcknowledge && *min == 1 && *max == 1 }));
        assert_eq!(ApiKey::from_i16(79), Some(ApiKey::ShareAcknowledge));
        assert_eq!(ApiKey::from_i16(75), Some(ApiKey::DescribeTopicPartitions));
        assert_eq!(ApiKey::from_i16(80), Some(ApiKey::AddRaftVoter));
    }

    #[test]
    fn supported_apis_includes_add_raft_voter_80() {
        assert!(SUPPORTED_APIS.len() >= 70);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| *k == ApiKey::AddRaftVoter && *min == 0 && *max == 0));
        assert_eq!(ApiKey::from_i16(80), Some(ApiKey::AddRaftVoter));
        assert_eq!(ApiKey::from_i16(75), Some(ApiKey::DescribeTopicPartitions));
    }

    #[test]
    fn supported_apis_includes_remove_raft_voter_81() {
        assert!(SUPPORTED_APIS.len() >= 70);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::RemoveRaftVoter && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(81), Some(ApiKey::RemoveRaftVoter));
    }

    #[test]
    fn supported_apis_includes_update_raft_voter_82() {
        assert!(SUPPORTED_APIS.len() >= 70);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::UpdateRaftVoter && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(82), Some(ApiKey::UpdateRaftVoter));
    }

    #[test]
    fn supported_apis_includes_initialize_share_group_state_83() {
        assert!(SUPPORTED_APIS.len() >= 75);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::InitializeShareGroupState && *min == 0 && *max == 0
        }));
        assert_eq!(
            ApiKey::from_i16(83),
            Some(ApiKey::InitializeShareGroupState)
        );
        assert_eq!(ApiKey::from_i16(82), Some(ApiKey::UpdateRaftVoter));
        assert_eq!(ApiKey::from_i16(94), Some(ApiKey::UnregisterController));
    }

    #[test]
    fn supported_apis_includes_read_share_group_state_84() {
        assert!(SUPPORTED_APIS.len() >= 80);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::ReadShareGroupState && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(84), Some(ApiKey::ReadShareGroupState));
        assert_eq!(
            ApiKey::from_i16(83),
            Some(ApiKey::InitializeShareGroupState)
        );
        assert_eq!(ApiKey::from_i16(94), Some(ApiKey::UnregisterController));
    }

    #[test]
    fn supported_apis_includes_write_share_group_state_85() {
        assert!(SUPPORTED_APIS.len() >= 80);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::WriteShareGroupState && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(85), Some(ApiKey::WriteShareGroupState));
        assert_eq!(
            ApiKey::from_i16(83),
            Some(ApiKey::InitializeShareGroupState)
        );
        assert_eq!(ApiKey::from_i16(94), Some(ApiKey::UnregisterController));
    }

    #[test]
    fn supported_apis_includes_delete_share_group_state_86() {
        assert!(SUPPORTED_APIS.len() >= 80);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::DeleteShareGroupState && *min == 0 && *max == 0
        }));
        assert_eq!(ApiKey::from_i16(86), Some(ApiKey::DeleteShareGroupState));
        assert_eq!(
            ApiKey::from_i16(83),
            Some(ApiKey::InitializeShareGroupState)
        );
        assert_eq!(ApiKey::from_i16(94), Some(ApiKey::UnregisterController));
    }

    #[test]
    fn supported_apis_includes_read_share_group_state_summary_87() {
        assert!(SUPPORTED_APIS.len() >= 80);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::ReadShareGroupStateSummary && *min == 0 && *max == 0
        }));
        assert_eq!(
            ApiKey::from_i16(87),
            Some(ApiKey::ReadShareGroupStateSummary)
        );
        assert_eq!(
            ApiKey::from_i16(83),
            Some(ApiKey::InitializeShareGroupState)
        );
        assert_eq!(ApiKey::from_i16(94), Some(ApiKey::UnregisterController));
    }

    #[test]
    fn supported_apis_includes_describe_share_group_offsets_90() {
        assert!(SUPPORTED_APIS.len() >= 80);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::DescribeShareGroupOffsets && *min == 0 && *max == 0
        }));
        assert_eq!(
            ApiKey::from_i16(90),
            Some(ApiKey::DescribeShareGroupOffsets)
        );
        assert_eq!(
            ApiKey::from_i16(83),
            Some(ApiKey::InitializeShareGroupState)
        );
        assert_eq!(ApiKey::from_i16(94), Some(ApiKey::UnregisterController));
    }

    #[test]
    fn supported_apis_includes_delete_share_group_offsets_92() {
        assert!(SUPPORTED_APIS.len() >= 85);
        assert!(SUPPORTED_APIS.iter().any(|(k, min, max)| {
            *k == ApiKey::DeleteShareGroupOffsets && *min == 0 && *max == 0
        }));
        assert_eq!(ApiKey::from_i16(92), Some(ApiKey::DeleteShareGroupOffsets));
        assert_eq!(
            ApiKey::from_i16(90),
            Some(ApiKey::DescribeShareGroupOffsets)
        );
        assert_eq!(ApiKey::from_i16(94), Some(ApiKey::UnregisterController));
    }

    #[test]
    fn supported_apis_includes_unregister_controller_94() {
        assert!(SUPPORTED_APIS.len() >= 70);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::UnregisterController && *min == 0 && *max == 0 }));
        assert_eq!(ApiKey::from_i16(94), Some(ApiKey::UnregisterController));
        assert_eq!(ApiKey::from_i16(64), Some(ApiKey::UnregisterBroker));
        assert_eq!(ApiKey::from_i16(70), Some(ApiKey::ControllerRegistration));
    }

    #[test]
    fn supported_apis_includes_share_group_describe_77() {
        assert!(SUPPORTED_APIS.len() >= 75);
        assert!(SUPPORTED_APIS
            .iter()
            .any(|(k, min, max)| { *k == ApiKey::ShareGroupDescribe && *min == 1 && *max == 1 }));
        assert_eq!(ApiKey::from_i16(77), Some(ApiKey::ShareGroupDescribe));
        assert_eq!(ApiKey::from_i16(75), Some(ApiKey::DescribeTopicPartitions));
        assert_eq!(ApiKey::from_i16(80), Some(ApiKey::AddRaftVoter));
        assert_eq!(ApiKey::from_i16(69), Some(ApiKey::ConsumerGroupDescribe));
    }
}
