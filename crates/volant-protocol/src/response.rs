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
    /// Init producer id result (Phase 10).
    InitProducerId = 33,
    /// Describe group result (Phase 11).
    DescribeGroup = 35,
    /// List groups result (Phase 12).
    ListGroups = 37,
    /// Delete offsets result (Phase 12).
    DeleteOffsets = 39,
    /// Describe configs result (Phase 13).
    DescribeConfigs = 41,
    /// Alter configs result (Phase 13).
    AlterConfigs = 43,
    /// Delete records result (Phase 14).
    DeleteRecords = 45,
    /// Create partitions result (Phase 15).
    CreatePartitions = 47,
    /// List offsets result (Phase 15).
    ListOffsets = 49,
    /// Begin transaction result (Phase 18).
    BeginTxn = 51,
    /// End transaction result (Phase 18).
    EndTxn = 53,
    /// Create ACLs result (Phase 20).
    CreateAcls = 55,
    /// Delete ACLs result (Phase 20).
    DeleteAcls = 57,
    /// List ACLs result (Phase 20).
    ListAcls = 59,
    /// SCRAM-SHA-256 first response (Phase 22).
    ScramFirst = 61,
    /// SCRAM-SHA-256 final response (Phase 22).
    ScramFinal = 63,
    /// Create SCRAM user result (Phase 22).
    CreateScramUser = 65,
    /// Delete SCRAM user result (Phase 22).
    DeleteScramUser = 67,
    /// List SCRAM users result (Phase 22).
    ListScramUsers = 69,
    /// Replica DeleteRecords fan-out result (Phase 113).
    ReplicaDeleteRecords = 71,
    /// Cluster BROKER config push result (Phase 113).
    ClusterBrokerConfig = 73,
    /// Cluster ACL snapshot push result (Phase 113).
    ClusterAclSnapshot = 75,
    /// Txn participant open result (Phase 114).
    TxnParticipantOpen = 77,
    /// Txn participant prepare result (Phase 114).
    TxnParticipantPrepare = 79,
    /// Txn participant complete result (Phase 114).
    TxnParticipantComplete = 81,
    /// Kafka Fetch forward result (Phase 119).
    KafkaFetchForward = 83,
    /// Kafka txn API forward result (Phase 120).
    KafkaTxnForward = 85,
    /// Truncate journal note result (Phase 129).
    TruncateJournalNote = 87,
    /// Truncate journal push result (Phase 129).
    TruncateJournalPush = 89,
    /// Fetch session mirror put result (Phase 138).
    FetchSessionMirrorPut = 91,
    /// Fetch session mirror delete result (Phase 138).
    FetchSessionMirrorDelete = 93,
    /// ISR update result (Phase 142).
    IsrUpdate = 95,
    /// Assignment consensus note result (Phase 150).
    AssignmentConsensusNote = 97,
    /// Metadata Raft AppendEntries result (Phase 154).
    MetadataRaftAppend = 99,
    /// Membership overlay push result (v0.10).
    MembershipPut = 101,
    /// Add broker result (v0.10).
    AddBroker = 103,
    /// Remove broker result (v0.10).
    RemoveBroker = 105,
    /// List membership result (v0.10).
    ListMembers = 107,
    /// Inter-broker openraft AppendEntries result (v0.11).
    OpenraftAppend = 109,
    /// Inter-broker openraft RequestVote result (v0.11).
    OpenraftVote = 111,
    /// Inter-broker openraft InstallSnapshot result (v0.17).
    OpenraftInstallSnapshot = 113,
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
            33 => Self::InitProducerId,
            35 => Self::DescribeGroup,
            37 => Self::ListGroups,
            39 => Self::DeleteOffsets,
            41 => Self::DescribeConfigs,
            43 => Self::AlterConfigs,
            45 => Self::DeleteRecords,
            47 => Self::CreatePartitions,
            49 => Self::ListOffsets,
            51 => Self::BeginTxn,
            53 => Self::EndTxn,
            55 => Self::CreateAcls,
            57 => Self::DeleteAcls,
            59 => Self::ListAcls,
            61 => Self::ScramFirst,
            63 => Self::ScramFinal,
            65 => Self::CreateScramUser,
            67 => Self::DeleteScramUser,
            69 => Self::ListScramUsers,
            71 => Self::ReplicaDeleteRecords,
            73 => Self::ClusterBrokerConfig,
            75 => Self::ClusterAclSnapshot,
            77 => Self::TxnParticipantOpen,
            79 => Self::TxnParticipantPrepare,
            81 => Self::TxnParticipantComplete,
            83 => Self::KafkaFetchForward,
            85 => Self::KafkaTxnForward,
            87 => Self::TruncateJournalNote,
            89 => Self::TruncateJournalPush,
            91 => Self::FetchSessionMirrorPut,
            93 => Self::FetchSessionMirrorDelete,
            95 => Self::IsrUpdate,
            97 => Self::AssignmentConsensusNote,
            99 => Self::MetadataRaftAppend,
            101 => Self::MembershipPut,
            103 => Self::AddBroker,
            105 => Self::RemoveBroker,
            107 => Self::ListMembers,
            109 => Self::OpenraftAppend,
            111 => Self::OpenraftVote,
            113 => Self::OpenraftInstallSnapshot,
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
    /// Idempotent produce epoch does not match broker state (Phase 10).
    InvalidProducerEpoch = 19,
    /// Idempotent produce sequence is not the next expected (Phase 10).
    OutOfOrderSequence = 20,
    /// Producer id was not allocated via InitProducerId (Phase 10).
    UnknownProducerId = 21,
    /// Invalid transaction state (Phase 18) — e.g. produce without BeginTxn.
    InvalidTxnState = 22,
    /// Principal not authorized for the operation (Phase 20).
    AuthorizationFailed = 23,
    /// Transaction must be aborted by the client (Phase 94 / KIP-890).
    ///
    /// Mapped to Kafka `TRANSACTION_ABORTABLE` (123). Used after open/prepared
    /// timeout auto-abort so clients clean up local txn state.
    TransactionAbortable = 24,
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
            19 => Self::InvalidProducerEpoch,
            20 => Self::OutOfOrderSequence,
            21 => Self::UnknownProducerId,
            22 => Self::InvalidTxnState,
            23 => Self::AuthorizationFailed,
            24 => Self::TransactionAbortable,
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

/// One member in a DescribeGroup response (Phase 11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMemberInfo {
    /// Member id.
    pub member_id: String,
    /// Subscribed topics.
    pub topics: Vec<String>,
    /// Current partition assignment.
    pub assignment: Vec<Assignment>,
}

/// Group state for ListGroups (Phase 12).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    /// Offsets on disk only; no live members.
    Empty = 0,
    /// At least one live member.
    Stable = 1,
}

impl GroupState {
    /// Parse wire value.
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Stable,
            _ => Self::Empty,
        }
    }
}

/// One group in a ListGroups response (Phase 12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupListing {
    /// Group id.
    pub group_id: String,
    /// Live vs empty (offset-only).
    pub state: GroupState,
    /// Live member count.
    pub member_count: u32,
    /// Current generation (`0` if empty).
    pub generation: u32,
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
        /// Partitions this member lost since its prior assignment (Phase 17).
        /// Empty when unknown or none revoked. Legacy payloads omit the trailer.
        revoked: Vec<Assignment>,
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
    /// Init producer id result (Phase 10).
    InitProducerId {
        /// Allocated producer id.
        producer_id: u64,
        /// Producer epoch.
        epoch: u16,
        /// 0 = ok.
        error_code: u16,
    },
    /// Describe group result (Phase 11).
    DescribeGroup {
        /// 0 = ok; 2 = not found (no live members).
        error_code: u16,
        /// Group id.
        group_id: String,
        /// Current generation (`0` if unknown).
        generation: u32,
        /// Live members.
        members: Vec<GroupMemberInfo>,
    },
    /// List groups result (Phase 12).
    ListGroups {
        /// 0 = ok.
        error_code: u16,
        /// Known groups (live + offset-only).
        groups: Vec<GroupListing>,
    },
    /// Delete offsets result (Phase 12).
    DeleteOffsets {
        /// 0 = ok.
        error_code: u16,
        /// Number of offset files removed.
        deleted_count: u32,
    },
    /// Describe topic configs result (Phase 13).
    DescribeConfigs {
        /// 0 = ok; 2 = not found.
        error_code: u16,
        /// Topic name.
        topic: String,
        /// Topic id (`0` if unknown).
        topic_id: u32,
        /// Partition count.
        partition_count: u32,
        /// Config key/value pairs (empty value = unset).
        configs: Vec<(String, String)>,
    },
    /// Alter topic configs result (Phase 13).
    AlterConfigs {
        /// 0 = ok; 2 = not found.
        error_code: u16,
        /// Topic name.
        topic: String,
    },
    /// Delete records result (Phase 14).
    DeleteRecords {
        /// 0 = ok; 2 = not found; 13 = not leader.
        error_code: u16,
        /// Topic name.
        topic: String,
        /// Partition id.
        partition: u32,
        /// New log start offset after deletion.
        low_watermark: u64,
    },
    /// Create partitions result (Phase 15).
    CreatePartitions {
        /// 0 = ok; 2 = not found; 3 = invalid; 14 = not controller.
        error_code: u16,
        /// Topic name.
        topic: String,
        /// New total partition count (`0` on error).
        partitions: u32,
    },
    /// List offsets result (Phase 15).
    ListOffsets {
        /// 0 = ok; 2 = not found.
        error_code: u16,
        /// Topic name.
        topic: String,
        /// Per-partition earliest/latest.
        entries: Vec<OffsetListing>,
    },
    /// Begin transaction result (Phase 18).
    BeginTxn {
        /// 0 = ok; 19 = bad epoch; 21 = unknown PID; 22 = invalid txn state.
        error_code: u16,
    },
    /// End transaction result (Phase 18).
    EndTxn {
        /// 0 = ok; 19/21/22 on failure.
        error_code: u16,
        /// Per-batch results after commit (empty on abort).
        results: Vec<TxnProduceResult>,
    },
    /// Create ACLs result (Phase 20).
    CreateAcls {
        /// 0 = ok; 3 = invalid; 23 = unauthorized.
        error_code: u16,
    },
    /// Delete ACLs result (Phase 20).
    DeleteAcls {
        /// 0 = ok; 23 = unauthorized.
        error_code: u16,
        /// Number of entries removed.
        removed: u32,
    },
    /// List ACLs result (Phase 20).
    ListAcls {
        /// 0 = ok; 23 = unauthorized.
        error_code: u16,
        /// Matching bindings.
        entries: Vec<crate::request::AclBinding>,
    },
    /// SCRAM-SHA-256 server-first (Phase 22).
    ScramFirst {
        /// 0 = ok; 3 = invalid nonce/username shape.
        error_code: u16,
        /// Client nonce + server nonce.
        combined_nonce: String,
        /// Salt bytes.
        salt: Bytes,
        /// PBKDF2 iterations.
        iterations: u32,
    },
    /// SCRAM-SHA-256 server-final (Phase 22).
    ScramFinal {
        /// 0 = ok; 17 = AuthenticationFailed.
        error_code: u16,
        /// Server signature (empty on failure).
        server_signature: Bytes,
    },
    /// Create SCRAM user result (Phase 22).
    CreateScramUser {
        /// 0 = ok; 3 = invalid; 23 = unauthorized.
        error_code: u16,
    },
    /// Delete SCRAM user result (Phase 22).
    DeleteScramUser {
        /// 0 = ok; 2 = not found; 23 = unauthorized.
        error_code: u16,
    },
    /// List SCRAM users result (Phase 22).
    ListScramUsers {
        /// 0 = ok; 23 = unauthorized.
        error_code: u16,
        /// Registered usernames.
        usernames: Vec<String>,
    },
    /// Replica DeleteRecords fan-out result (Phase 113).
    ReplicaDeleteRecords {
        /// 0 = ok; 13 = not leader; 19 = fenced epoch (when implemented).
        error_code: u16,
        /// Local log start after truncate attempt.
        low_watermark: u64,
    },
    /// Cluster BROKER config push result (Phase 113).
    ClusterBrokerConfig {
        /// 0 = ok; non-zero on reject / apply failure.
        error_code: u16,
        /// Generation applied on the peer (`0` if rejected as stale).
        applied_generation: u64,
    },
    /// Cluster ACL snapshot push result (Phase 113).
    ClusterAclSnapshot {
        /// 0 = ok; non-zero on reject / apply failure.
        error_code: u16,
        /// Generation applied on the peer (`0` if rejected as stale).
        applied_generation: u64,
    },
    /// Txn participant open result (Phase 114).
    TxnParticipantOpen {
        /// 0 = ok.
        error_code: u16,
    },
    /// Txn participant prepare result (Phase 114).
    TxnParticipantPrepare {
        /// 0 = ok.
        error_code: u16,
    },
    /// Txn participant complete result (Phase 114).
    TxnParticipantComplete {
        /// 0 = ok.
        error_code: u16,
    },
    /// Kafka Fetch forward result (Phase 119).
    KafkaFetchForward {
        /// 0 = ok; non-zero = peer could not serve (caller maps to session 70).
        error_code: u16,
        /// Kafka Fetch response body (after the Kafka response header).
        body: Bytes,
    },
    /// Kafka txn API forward result (Phase 120).
    KafkaTxnForward {
        /// 0 = ok; non-zero = peer could not serve.
        error_code: u16,
        /// Kafka response body (after the Kafka response header).
        body: Bytes,
    },
    /// Truncate journal note result (Phase 129).
    TruncateJournalNote {
        /// Protocol error code.
        error_code: u16,
        /// Controller journal generation after merge.
        generation: u64,
    },
    /// Truncate journal push result (Phase 129).
    TruncateJournalPush {
        /// Protocol error code.
        error_code: u16,
    },
    /// Fetch session mirror put result (Phase 138).
    FetchSessionMirrorPut {
        /// Protocol error code.
        error_code: u16,
    },
    /// Fetch session mirror delete result (Phase 138).
    FetchSessionMirrorDelete {
        /// Protocol error code.
        error_code: u16,
    },
    /// ISR update result (Phase 142).
    IsrUpdate {
        /// 0 = ok; 14 = not controller; 13 = not leader; 19 = fenced epoch; 2 = unknown TP.
        error_code: u16,
        /// Controller assignment generation after apply (unchanged on reject).
        generation: u32,
    },
    /// Assignment consensus note result (Phase 150).
    AssignmentConsensusNote {
        /// 0 = ok.
        error_code: u16,
        /// Assignment generation applied / acked by the peer.
        generation: u32,
    },
    /// Metadata Raft AppendEntries result (Phase 154).
    MetadataRaftAppend {
        /// Peer's current term.
        term: u64,
        /// 1 = prev matched and entries accepted; 0 = reject.
        success: u8,
        /// Highest matching log index on the peer.
        match_index: u64,
    },
    /// Membership overlay push result (v0.10).
    MembershipPut {
        /// 0 = ok (including ignored stale generation).
        error_code: u16,
        /// Generation now stored on the peer.
        applied_generation: u64,
    },
    /// Add broker result (v0.10).
    AddBroker {
        /// 0 = ok.
        error_code: u16,
        /// New overlay generation (`0` on error).
        generation: u64,
    },
    /// Remove broker result (v0.10).
    RemoveBroker {
        /// 0 = ok.
        error_code: u16,
        /// New overlay generation (`0` on error).
        generation: u64,
    },
    /// List membership result (v0.10).
    ListMembers {
        /// 0 = ok.
        error_code: u16,
        /// Overlay generation (`0` if toml-only).
        generation: u64,
        /// Effective configured brokers.
        brokers: Vec<crate::request::MembershipBroker>,
        /// Live broker ids.
        live: Vec<u32>,
    },
    /// Inter-broker openraft AppendEntries result (v0.11).
    OpenraftAppend {
        /// `serde_json` of openraft `AppendEntriesResponse`.
        payload: Bytes,
    },
    /// Inter-broker openraft RequestVote result (v0.11).
    OpenraftVote {
        /// `serde_json` of openraft `VoteResponse`.
        payload: Bytes,
    },
    /// Inter-broker openraft InstallSnapshot result (v0.17).
    OpenraftInstallSnapshot {
        /// `serde_json` of openraft `InstallSnapshotResponse`.
        payload: Bytes,
    },
    /// Error response.
    Error {
        /// Error code.
        code: u16,
        /// Human-readable error message.
        message: String,
    },
}

/// One partition in a ListOffsets response (Phase 15).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetListing {
    /// Partition id.
    pub partition: u32,
    /// Log start offset.
    pub earliest: u64,
    /// Log end offset (next write).
    pub latest: u64,
}

/// One flushed produce batch from EndTxn commit (Phase 18).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnProduceResult {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
    /// Log base offset assigned at commit.
    pub base_offset: u64,
    /// Message count.
    pub count: u32,
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
            Self::InitProducerId { .. } => ResponseOpcode::InitProducerId as u16,
            Self::BeginTxn { .. } => ResponseOpcode::BeginTxn as u16,
            Self::EndTxn { .. } => ResponseOpcode::EndTxn as u16,
            Self::DescribeGroup { .. } => ResponseOpcode::DescribeGroup as u16,
            Self::ListGroups { .. } => ResponseOpcode::ListGroups as u16,
            Self::DeleteOffsets { .. } => ResponseOpcode::DeleteOffsets as u16,
            Self::DescribeConfigs { .. } => ResponseOpcode::DescribeConfigs as u16,
            Self::AlterConfigs { .. } => ResponseOpcode::AlterConfigs as u16,
            Self::DeleteRecords { .. } => ResponseOpcode::DeleteRecords as u16,
            Self::CreatePartitions { .. } => ResponseOpcode::CreatePartitions as u16,
            Self::ListOffsets { .. } => ResponseOpcode::ListOffsets as u16,
            Self::CreateAcls { .. } => ResponseOpcode::CreateAcls as u16,
            Self::DeleteAcls { .. } => ResponseOpcode::DeleteAcls as u16,
            Self::ListAcls { .. } => ResponseOpcode::ListAcls as u16,
            Self::ScramFirst { .. } => ResponseOpcode::ScramFirst as u16,
            Self::ScramFinal { .. } => ResponseOpcode::ScramFinal as u16,
            Self::CreateScramUser { .. } => ResponseOpcode::CreateScramUser as u16,
            Self::DeleteScramUser { .. } => ResponseOpcode::DeleteScramUser as u16,
            Self::ListScramUsers { .. } => ResponseOpcode::ListScramUsers as u16,
            Self::ReplicaDeleteRecords { .. } => ResponseOpcode::ReplicaDeleteRecords as u16,
            Self::ClusterBrokerConfig { .. } => ResponseOpcode::ClusterBrokerConfig as u16,
            Self::ClusterAclSnapshot { .. } => ResponseOpcode::ClusterAclSnapshot as u16,
            Self::TxnParticipantOpen { .. } => ResponseOpcode::TxnParticipantOpen as u16,
            Self::TxnParticipantPrepare { .. } => ResponseOpcode::TxnParticipantPrepare as u16,
            Self::TxnParticipantComplete { .. } => ResponseOpcode::TxnParticipantComplete as u16,
            Self::KafkaFetchForward { .. } => ResponseOpcode::KafkaFetchForward as u16,
            Self::KafkaTxnForward { .. } => ResponseOpcode::KafkaTxnForward as u16,
            Self::TruncateJournalNote { .. } => ResponseOpcode::TruncateJournalNote as u16,
            Self::TruncateJournalPush { .. } => ResponseOpcode::TruncateJournalPush as u16,
            Self::FetchSessionMirrorPut { .. } => ResponseOpcode::FetchSessionMirrorPut as u16,
            Self::FetchSessionMirrorDelete { .. } => {
                ResponseOpcode::FetchSessionMirrorDelete as u16
            }
            Self::IsrUpdate { .. } => ResponseOpcode::IsrUpdate as u16,
            Self::AssignmentConsensusNote { .. } => ResponseOpcode::AssignmentConsensusNote as u16,
            Self::MetadataRaftAppend { .. } => ResponseOpcode::MetadataRaftAppend as u16,
            Self::MembershipPut { .. } => ResponseOpcode::MembershipPut as u16,
            Self::AddBroker { .. } => ResponseOpcode::AddBroker as u16,
            Self::RemoveBroker { .. } => ResponseOpcode::RemoveBroker as u16,
            Self::ListMembers { .. } => ResponseOpcode::ListMembers as u16,
            Self::OpenraftAppend { .. } => ResponseOpcode::OpenraftAppend as u16,
            Self::OpenraftVote { .. } => ResponseOpcode::OpenraftVote as u16,
            Self::OpenraftInstallSnapshot { .. } => ResponseOpcode::OpenraftInstallSnapshot as u16,
            Self::Error { .. } => ResponseOpcode::Error as u16,
        }
    }
}
