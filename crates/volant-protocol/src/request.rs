//! Client → broker request opcodes and payloads.

use bytes::Bytes;

use crate::response::ClusterTopicState;

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
    /// Increase topic partition count (Phase 15).
    CreatePartitions = 46,
    /// List earliest/latest offsets (Phase 15).
    ListOffsets = 48,
    /// Begin a producer transaction (Phase 18).
    BeginTxn = 50,
    /// Commit or abort a producer transaction (Phase 18).
    EndTxn = 52,
    /// Create ACL entries (Phase 20).
    CreateAcls = 54,
    /// Delete ACL entries (Phase 20).
    DeleteAcls = 56,
    /// List ACL entries (Phase 20).
    ListAcls = 58,
    /// SCRAM client-first (Phase 22; v0.238 hash trailer).
    ScramFirst = 60,
    /// SCRAM client-final (Phase 22; proof is 32 or 64 bytes).
    ScramFinal = 62,
    /// Create/upsert SCRAM user (Phase 22).
    CreateScramUser = 64,
    /// Delete SCRAM user (Phase 22).
    DeleteScramUser = 66,
    /// List SCRAM usernames (Phase 22).
    ListScramUsers = 68,
    /// Leader → replica DeleteRecords fan-out (Phase 113).
    ReplicaDeleteRecords = 70,
    /// Controller → peer BROKER config push (Phase 113).
    ClusterBrokerConfig = 72,
    /// Controller → peer ACL snapshot push (Phase 113).
    ClusterAclSnapshot = 74,
    /// Coordinator → peer: install producer + open txn (Phase 114 multi-broker 2PC).
    TxnParticipantOpen = 76,
    /// Coordinator → peer: prepare local open ranges (Phase 114).
    TxnParticipantPrepare = 78,
    /// Coordinator → peer: finalize prepared/open ranges (Phase 114).
    TxnParticipantComplete = 80,
    /// Non-owner → session owner: proxy Kafka Fetch body (Phase 119).
    KafkaFetchForward = 82,
    /// Non-coordinator → txn coordinator: proxy Kafka txn API body (Phase 120).
    KafkaTxnForward = 84,
    /// Multi-controller durable note of DeleteRecords truncate watermark (Phase 129/130).
    TruncateJournalNote = 86,
    /// Best-effort full truncate journal snapshot push (Phase 129/130).
    TruncateJournalPush = 88,
    /// Owner → peers: install/update fetch session mirror snapshot (Phase 138).
    FetchSessionMirrorPut = 90,
    /// Owner → peers: remove fetch session mirror (Phase 138).
    FetchSessionMirrorDelete = 92,
    /// Non-controller leader → controller: report live ISR (Phase 142).
    IsrUpdate = 94,
    /// Controller → peers: assignment generation majority consensus note (Phase 150).
    AssignmentConsensusNote = 96,
    /// Controller → peers: KRaft-style metadata Raft AppendEntries (Phase 154).
    MetadataRaftAppend = 98,
    /// Inter-broker membership overlay push (v0.10).
    MembershipPut = 100,
    /// Admin: add a broker endpoint (v0.10).
    AddBroker = 102,
    /// Admin: remove a broker by id (v0.10).
    RemoveBroker = 104,
    /// Admin: list configured + live membership (v0.10).
    ListMembers = 106,
    /// Inter-broker openraft AppendEntries (v0.11).
    OpenraftAppend = 108,
    /// Inter-broker openraft RequestVote (v0.11).
    OpenraftVote = 110,
    /// Inter-broker openraft InstallSnapshot (v0.17).
    OpenraftInstallSnapshot = 112,
    /// Admin: reassign topic partition replicas (v0.18).
    ReassignPartitions = 114,
    /// SyncGroup peek/confirm (Phase 155). Not Kafka CompletingRebalance.
    SyncGroup = 116,
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
            46 => Self::CreatePartitions,
            48 => Self::ListOffsets,
            50 => Self::BeginTxn,
            52 => Self::EndTxn,
            54 => Self::CreateAcls,
            56 => Self::DeleteAcls,
            58 => Self::ListAcls,
            60 => Self::ScramFirst,
            62 => Self::ScramFinal,
            64 => Self::CreateScramUser,
            66 => Self::DeleteScramUser,
            68 => Self::ListScramUsers,
            70 => Self::ReplicaDeleteRecords,
            72 => Self::ClusterBrokerConfig,
            74 => Self::ClusterAclSnapshot,
            76 => Self::TxnParticipantOpen,
            78 => Self::TxnParticipantPrepare,
            80 => Self::TxnParticipantComplete,
            82 => Self::KafkaFetchForward,
            84 => Self::KafkaTxnForward,
            86 => Self::TruncateJournalNote,
            88 => Self::TruncateJournalPush,
            90 => Self::FetchSessionMirrorPut,
            92 => Self::FetchSessionMirrorDelete,
            94 => Self::IsrUpdate,
            96 => Self::AssignmentConsensusNote,
            98 => Self::MetadataRaftAppend,
            100 => Self::MembershipPut,
            102 => Self::AddBroker,
            104 => Self::RemoveBroker,
            106 => Self::ListMembers,
            108 => Self::OpenraftAppend,
            110 => Self::OpenraftVote,
            112 => Self::OpenraftInstallSnapshot,
            114 => Self::ReassignPartitions,
            116 => Self::SyncGroup,
            _ => return None,
        })
    }
}

/// One ACL binding on the wire (Phase 20).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclBinding {
    /// Principal name, or `*`.
    pub principal: String,
    /// 0=Topic, 1=Group, 2=Cluster.
    pub resource_type: u8,
    /// Resource name, or `*`.
    pub resource: String,
    /// 0=All … 7=ClusterAction.
    pub operation: u8,
    /// 0=Deny, 1=Allow.
    pub permission: u8,
}

/// One deferred offset commit inside EndTxn (Phase 18).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommit {
    /// Consumer group id.
    pub group_id: String,
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
    /// Offset to commit.
    pub offset: u64,
    /// Optional metadata.
    pub metadata: String,
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
        /// Consumer group id (v0.234). Empty = unfiltered (admin / CLI / old clients).
        group_id: String,
        /// Group member id (v0.234). Empty = unfiltered.
        member_id: String,
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
        /// Join park budget in milliseconds (v0.231 trailer). `0` = broker default.
        rebalance_timeout_ms: u32,
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
        /// Last applied BROKER-config generation on the sender (Phase 117).
        ///
        /// Older peers omit this on the wire; decoders default to `0`.
        applied_config_generation: u64,
        /// Last applied ACL generation on the sender (Phase 117).
        ///
        /// Older peers omit this on the wire; decoders default to `0`.
        applied_acl_generation: u64,
        /// Last applied truncate-journal generation on the sender (Phase 131).
        ///
        /// Older peers omit this on the wire; decoders default to `0`.
        applied_journal_generation: u64,
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
    ///
    /// Optional `transactional_id` (Phase 18); empty = non-transactional PID.
    InitProducerId {
        /// Transactional id for fencing; empty = plain idempotent producer.
        transactional_id: String,
    },
    /// Begin a producer transaction (Phase 18).
    BeginTxn {
        /// Producer id from InitProducerId.
        producer_id: u64,
        /// Producer epoch.
        producer_epoch: u16,
    },
    /// Commit or abort a producer transaction (Phase 18).
    EndTxn {
        /// Producer id.
        producer_id: u64,
        /// Producer epoch.
        producer_epoch: u16,
        /// `true` = commit, `false` = abort.
        committed: bool,
        /// Deferred offset commits applied only on successful commit.
        offsets: Vec<TxnOffsetCommit>,
    },
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
        /// Phase 137: 0=broker default, 1=force wait, 2=force no-wait.
        wait_majority: u8,
    },
    /// Increase topic partition count (Phase 15).
    CreatePartitions {
        /// Topic name.
        topic: String,
        /// Desired total partition count (must exceed current).
        total_count: u32,
    },
    /// List earliest/latest offsets (Phase 15; v0.239 timestamp; v0.240 isolation).
    ListOffsets {
        /// Topic name.
        topic: String,
        /// Partitions to query; empty = all.
        partitions: Vec<u32>,
        /// Timestamp trailer: missing / `-1` = latest (LEO), `-2` = earliest
        /// (log start), `>= 0` = first record at or after `T` (v0.239).
        timestamp_ms: i64,
        /// Isolation trailer after `timestamp_ms` (v0.240). Missing / `0` =
        /// READ_UNCOMMITTED (latest = LEO). `1` = READ_COMMITTED (latest = LSO).
        isolation: u8,
    },
    /// Create ACL entries (Phase 20).
    CreateAcls {
        /// Bindings to add.
        entries: Vec<AclBinding>,
    },
    /// Delete exact-matching ACL entries (Phase 20).
    DeleteAcls {
        /// Bindings to remove.
        entries: Vec<AclBinding>,
    },
    /// List ACL entries with optional filters (Phase 20).
    ListAcls {
        /// Principal filter; empty = any.
        principal: String,
        /// Resource type filter; `255` = any.
        resource_type: u8,
        /// Resource name filter; empty = any.
        resource: String,
    },
    /// SCRAM client-first (Phase 22; v0.238 optional hash trailer).
    ScramFirst {
        /// Claimed username.
        username: String,
        /// Client nonce (printable ASCII, no commas).
        client_nonce: String,
        /// Hash trailer: missing / 0 / 1 = SHA-256, 2 = SHA-512 (v0.238).
        hash: u8,
    },
    /// SCRAM client-final (Phase 22).
    ScramFinal {
        /// Username (must match ScramFirst).
        username: String,
        /// Combined nonce from ScramFirst response.
        combined_nonce: String,
        /// Client proof (32 bytes SHA-256, 64 bytes SHA-512).
        client_proof: Bytes,
    },
    /// Create or replace a SCRAM user (Phase 22).
    CreateScramUser {
        /// Username (principal after SCRAM auth).
        username: String,
        /// Plaintext password (sent once; never stored).
        password: String,
        /// PBKDF2 iterations; `0` = broker default (4096).
        iterations: u32,
    },
    /// Delete a SCRAM user (Phase 22).
    DeleteScramUser {
        /// Username to remove.
        username: String,
    },
    /// List SCRAM usernames (Phase 22).
    ListScramUsers,
    /// Leader → replica DeleteRecords fan-out (Phase 113).
    ReplicaDeleteRecords {
        /// Topic name.
        topic: String,
        /// Partition id.
        partition: u32,
        /// Drop sealed segments entirely before this offset.
        before_offset: u64,
        /// Leader epoch at the time of truncate (`-1` if unknown).
        leader_epoch: i32,
    },
    /// Controller → peer BROKER config push (Phase 113).
    ///
    /// Empty value string = DELETE / restore product default (same as
    /// IncrementalAlterConfigs DELETE).
    ClusterBrokerConfig {
        /// Controller config generation for this push.
        generation: u64,
        /// Sparse key/value overlay entries.
        entries: Vec<(String, String)>,
    },
    /// Controller → peer ACL snapshot push (Phase 113).
    ClusterAclSnapshot {
        /// Controller ACL generation for this push.
        generation: u64,
        /// Versioned snapshot bytes (typically JSON matching `__acls/acls.json`).
        snapshot: Bytes,
    },
    /// Coordinator → peer: install producer + open txn (Phase 114).
    TxnParticipantOpen {
        /// Transactional id.
        transactional_id: String,
        /// Producer id.
        producer_id: u64,
        /// Producer epoch.
        producer_epoch: u16,
        /// Whether Enable2Pc is set for this producer.
        enable_2pc: bool,
        /// Node id of the txn coordinator (Init owner). `0` = unknown (Phase 120).
        coordinator_node_id: u32,
        /// When true, install empty open txn (Begin/AddPartitions). When false,
        /// only register producer + coordinator (Init registration fan-out).
        install_open: bool,
    },
    /// Coordinator → peer: prepare local open ranges (Phase 114).
    TxnParticipantPrepare {
        /// Transactional id.
        transactional_id: String,
        /// Producer id.
        producer_id: u64,
        /// Producer epoch.
        producer_epoch: u16,
        /// True = PrepareCommit; false = PrepareAbort.
        commit: bool,
    },
    /// Coordinator → peer: finalize prepared (or open) ranges (Phase 114).
    TxnParticipantComplete {
        /// Transactional id.
        transactional_id: String,
        /// Producer id.
        producer_id: u64,
        /// Producer epoch.
        producer_epoch: u16,
        /// True = commit finalize; false = abort finalize.
        commit: bool,
    },
    /// Non-owner → session owner: proxy a Kafka Fetch request body (Phase 119).
    KafkaFetchForward {
        /// Kafka Fetch API version.
        api_version: i16,
        /// ACL principal to apply on the owner (may be anonymous).
        principal: String,
        /// Kafka Fetch request body (after the Kafka request header).
        body: Bytes,
    },
    /// Non-coordinator → txn coordinator: proxy a Kafka txn API body (Phase 120).
    KafkaTxnForward {
        /// Kafka API key (e.g. 26 = EndTxn).
        api_key: i16,
        /// Kafka API version.
        api_version: i16,
        /// ACL principal to apply on the coordinator (may be anonymous).
        principal: String,
        /// Kafka request body (after the Kafka request header).
        body: Bytes,
    },
    /// Leader → controller: record a truncate watermark (Phase 129).
    TruncateJournalNote {
        /// Topic name.
        topic: String,
        /// Partition id.
        partition: u32,
        /// Desired log start (delete-before offset).
        before_offset: u64,
        /// Leader epoch at truncate time (`-1` if unknown).
        leader_epoch: i32,
    },
    /// Controller → peer: full truncate journal snapshot (Phase 129).
    TruncateJournalPush {
        /// Controller journal generation for this push.
        generation: u64,
        /// Versioned snapshot bytes (JSON matching `__truncate_journal/state.json`).
        snapshot: Bytes,
    },
    /// Owner → peers: install/update one durable fetch session snapshot (Phase 138).
    FetchSessionMirrorPut {
        /// Session id mirrored.
        session_id: i32,
        /// JSON snapshot of one durable session entry (id, epoch, topics, omit cache).
        snapshot: Bytes,
    },
    /// Owner → peers: remove a fetch session mirror (Phase 138).
    FetchSessionMirrorDelete {
        /// Session id to remove from the peer mirror table.
        session_id: i32,
    },
    /// Non-controller partition leader → controller: live ISR snapshot (Phase 142).
    IsrUpdate {
        /// Topic name.
        topic: String,
        /// Partition id.
        partition: u32,
        /// Claimed leader broker id.
        leader_id: u32,
        /// Leader epoch at the time of the ISR change.
        leader_epoch: u32,
        /// Full ISR set (must include `leader_id`).
        isr: Vec<u32>,
        /// Optional local assignment generation hint (`0` = none).
        generation_hint: u32,
    },
    /// Controller → peers: propose assignment snapshot for majority commit (Phase 150).
    ///
    /// Wire topics reuse [`ClusterTopicState`] encoding (same body as
    /// ClusterState response topics). Peers apply when `generation >= local`.
    AssignmentConsensusNote {
        /// Assignment generation being proposed.
        generation: u32,
        /// Controller id proposing the snapshot.
        controller_id: u32,
        /// Full assignment topics (leaders / replicas / ISR).
        topics: Vec<ClusterTopicState>,
    },
    /// Controller → peers: simplified Raft AppendEntries for metadata log (Phase 154).
    MetadataRaftAppend {
        /// Leader (controller) broker id.
        leader_id: u32,
        /// Leader term.
        term: u64,
        /// Index of log entry immediately preceding new ones.
        prev_log_index: u64,
        /// Term of `prev_log_index` entry (`0` when prev is empty).
        prev_log_term: u64,
        /// New entries to append (may be empty heartbeat).
        entries: Vec<MetadataRaftLogEntry>,
        /// Leader's commit index.
        leader_commit: u64,
    },
    /// Inter-broker membership overlay push (v0.10).
    MembershipPut {
        /// Overlay generation. Peers apply only if `> local`.
        generation: u64,
        /// Full effective broker list.
        brokers: Vec<MembershipBroker>,
    },
    /// Admin: add a broker endpoint (v0.10).
    AddBroker {
        /// New broker id (must be unique).
        id: u32,
        /// Host for inter-broker and client traffic.
        host: String,
        /// Port.
        port: u16,
        /// Optional rack.
        rack: Option<String>,
    },
    /// Admin: remove a broker by id (v0.10).
    RemoveBroker {
        /// Broker id to drop from the overlay.
        id: u32,
    },
    /// Admin: list configured + live membership (v0.10).
    ListMembers,
    /// Inter-broker openraft AppendEntries (v0.11). JSON body of the raft RPC.
    OpenraftAppend {
        /// `serde_json` of openraft `AppendEntriesRequest`.
        payload: Bytes,
    },
    /// Inter-broker openraft RequestVote (v0.11). JSON body of the raft RPC.
    OpenraftVote {
        /// `serde_json` of openraft `VoteRequest`.
        payload: Bytes,
    },
    /// Inter-broker openraft InstallSnapshot (v0.17). JSON body of the raft RPC.
    OpenraftInstallSnapshot {
        /// `serde_json` of openraft `InstallSnapshotRequest`.
        payload: Bytes,
    },
    /// Admin: reassign replicas for a topic (or one partition) (v0.18).
    ///
    /// Empty `replicas` means auto-place with the current effective broker list
    /// (same placement as CreateTopic). `partition ==`
    /// [`REASSIGN_ALL_PARTITIONS`] applies to every partition of the topic.
    ReassignPartitions {
        /// Topic name.
        topic: String,
        /// Partition id, or [`REASSIGN_ALL_PARTITIONS`] for every partition.
        partition: u32,
        /// Explicit replica broker ids (order = preferred leader first).
        /// Empty → auto-recompute.
        replicas: Vec<u32>,
    },
    /// SyncGroup peek/confirm (Phase 155). Broker ignores `assignment_bytes`.
    SyncGroup {
        /// Consumer group id.
        group_id: String,
        /// Member id.
        member_id: String,
        /// Current generation.
        generation: u32,
        /// Leader assignment bytes (MVP: empty; broker ignores).
        assignment_bytes: Bytes,
    },
}

/// `ReassignPartitions.partition` sentinel: apply to every partition of the topic.
pub const REASSIGN_ALL_PARTITIONS: u32 = u32::MAX;

/// Native ListOffsets `timestamp_ms` trailer: latest = LEO (v0.239).
/// Missing trailer also latest.
pub const LIST_OFFSETS_LATEST: i64 = -1;
/// Native ListOffsets `timestamp_ms` trailer: earliest = log start (v0.239).
pub const LIST_OFFSETS_EARLIEST: i64 = -2;

/// Native ListOffsets `isolation` trailer: READ_UNCOMMITTED (v0.240).
/// Missing trailer also uncommitted (latest = LEO).
pub const LIST_OFFSETS_READ_UNCOMMITTED: u8 = 0;
/// Native ListOffsets `isolation` trailer: READ_COMMITTED (v0.240).
/// Latest (`-1`) returns LSO instead of LEO.
pub const LIST_OFFSETS_READ_COMMITTED: u8 = 1;

/// Native ScramFirst `hash` trailer: SHA-256 (v0.238). Missing / 0 also SHA-256.
pub const SCRAM_HASH_SHA256: u8 = 1;
/// Native ScramFirst `hash` trailer: SHA-512 (v0.238).
pub const SCRAM_HASH_SHA512: u8 = 2;

/// One broker endpoint on the membership overlay wire (v0.10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipBroker {
    /// Unique broker id.
    pub id: u32,
    /// Host.
    pub host: String,
    /// Port.
    pub port: u16,
    /// Optional rack.
    pub rack: Option<String>,
}

/// One metadata Raft log entry on the wire (Phase 154).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRaftLogEntry {
    /// Term that authored this entry.
    pub term: u64,
    /// Log index (1-based).
    pub index: u64,
    /// Command kind: `0` = Noop, `1` = SetAssignment.
    pub command_kind: u8,
    /// Assignment generation (`0` for Noop).
    pub generation: u32,
    /// Topics payload for SetAssignment (empty for Noop).
    pub topics: Vec<ClusterTopicState>,
}

/// Wire command kinds for [`MetadataRaftLogEntry`].
pub mod metadata_raft_cmd {
    /// No-op filler.
    pub const NOOP: u8 = 0;
    /// Full assignment snapshot.
    pub const SET_ASSIGNMENT: u8 = 1;
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
            Self::InitProducerId { .. } => RequestOpcode::InitProducerId as u16,
            Self::BeginTxn { .. } => RequestOpcode::BeginTxn as u16,
            Self::EndTxn { .. } => RequestOpcode::EndTxn as u16,
            Self::DescribeGroup { .. } => RequestOpcode::DescribeGroup as u16,
            Self::ListGroups => RequestOpcode::ListGroups as u16,
            Self::DeleteOffsets { .. } => RequestOpcode::DeleteOffsets as u16,
            Self::DescribeConfigs { .. } => RequestOpcode::DescribeConfigs as u16,
            Self::AlterConfigs { .. } => RequestOpcode::AlterConfigs as u16,
            Self::DeleteRecords { .. } => RequestOpcode::DeleteRecords as u16,
            Self::CreatePartitions { .. } => RequestOpcode::CreatePartitions as u16,
            Self::ListOffsets { .. } => RequestOpcode::ListOffsets as u16,
            Self::CreateAcls { .. } => RequestOpcode::CreateAcls as u16,
            Self::DeleteAcls { .. } => RequestOpcode::DeleteAcls as u16,
            Self::ListAcls { .. } => RequestOpcode::ListAcls as u16,
            Self::ScramFirst { .. } => RequestOpcode::ScramFirst as u16,
            Self::ScramFinal { .. } => RequestOpcode::ScramFinal as u16,
            Self::CreateScramUser { .. } => RequestOpcode::CreateScramUser as u16,
            Self::DeleteScramUser { .. } => RequestOpcode::DeleteScramUser as u16,
            Self::ListScramUsers => RequestOpcode::ListScramUsers as u16,
            Self::ReplicaDeleteRecords { .. } => RequestOpcode::ReplicaDeleteRecords as u16,
            Self::ClusterBrokerConfig { .. } => RequestOpcode::ClusterBrokerConfig as u16,
            Self::ClusterAclSnapshot { .. } => RequestOpcode::ClusterAclSnapshot as u16,
            Self::TxnParticipantOpen { .. } => RequestOpcode::TxnParticipantOpen as u16,
            Self::TxnParticipantPrepare { .. } => RequestOpcode::TxnParticipantPrepare as u16,
            Self::TxnParticipantComplete { .. } => RequestOpcode::TxnParticipantComplete as u16,
            Self::KafkaFetchForward { .. } => RequestOpcode::KafkaFetchForward as u16,
            Self::KafkaTxnForward { .. } => RequestOpcode::KafkaTxnForward as u16,
            Self::TruncateJournalNote { .. } => RequestOpcode::TruncateJournalNote as u16,
            Self::TruncateJournalPush { .. } => RequestOpcode::TruncateJournalPush as u16,
            Self::FetchSessionMirrorPut { .. } => RequestOpcode::FetchSessionMirrorPut as u16,
            Self::FetchSessionMirrorDelete { .. } => RequestOpcode::FetchSessionMirrorDelete as u16,
            Self::IsrUpdate { .. } => RequestOpcode::IsrUpdate as u16,
            Self::AssignmentConsensusNote { .. } => RequestOpcode::AssignmentConsensusNote as u16,
            Self::MetadataRaftAppend { .. } => RequestOpcode::MetadataRaftAppend as u16,
            Self::MembershipPut { .. } => RequestOpcode::MembershipPut as u16,
            Self::AddBroker { .. } => RequestOpcode::AddBroker as u16,
            Self::RemoveBroker { .. } => RequestOpcode::RemoveBroker as u16,
            Self::ListMembers => RequestOpcode::ListMembers as u16,
            Self::OpenraftAppend { .. } => RequestOpcode::OpenraftAppend as u16,
            Self::OpenraftVote { .. } => RequestOpcode::OpenraftVote as u16,
            Self::OpenraftInstallSnapshot { .. } => RequestOpcode::OpenraftInstallSnapshot as u16,
            Self::ReassignPartitions { .. } => RequestOpcode::ReassignPartitions as u16,
            Self::SyncGroup { .. } => RequestOpcode::SyncGroup as u16,
        }
    }
}
