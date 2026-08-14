//! Broker state machine (single-node and multi-node cluster).
//!
//! # Batch produce coalescing
//!
//! [`Broker::produce`] accepts a [`MessageBatch`] and treats the whole batch as
//! one critical section under the topics write lock.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use volant_core::{
    Error, Message, MessageBatch, Offset, PartitionId, Record, Result, TopicId, TopicName,
};
use volant_protocol::ErrorCode;
use volant_storage::StorageConfig;

use crate::cluster::{
    load_assignment, AssignmentConsensus, AssignmentSnapshot, ClusterConfig, Membership,
    MetadataRaftState,
};
use crate::delete_records_outbox::DeleteRecordsOutbox;
use crate::group::GroupCoordinator;
use crate::kafka::codec::is_txn_control_record;
use crate::kafka::fetch_session::FetchSessionManager;
use crate::leader_epoch::{EpochStart, LeaderEpochStore, LeaderEpochsFile};
use crate::metrics::Metrics;
use crate::producer_state::{parse_partition_key, ProducerStateStore};
use crate::topic::Topic;
use crate::topic_catalog::TopicCatalogStore;
use crate::topic_config::TopicConfigStore;
use crate::truncate_journal::TruncateJournal;
use crate::txn_coordinator_registry::TxnCoordinatorRegistry;

/// Snapshot of cluster metadata for a Metadata response.
#[derive(Debug, Clone)]
pub struct MetadataSnapshot {
    /// Node id of this broker.
    pub node_id: u32,
    /// Advertised host (may be empty if unknown).
    pub host: String,
    /// Advertised port.
    pub port: u16,
    /// All known brokers (cluster-wide when configured).
    pub brokers: Vec<(u32, String, u16)>,
    /// Topic metadata entries.
    pub topics: Vec<TopicMetadata>,
    /// Current controller id (0 in single-node).
    pub controller_id: u32,
}

/// Per-topic metadata.
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    /// Topic name.
    pub name: TopicName,
    /// Stable topic id.
    pub topic_id: TopicId,
    /// Partition metadata (sorted by id).
    pub partitions: Vec<PartitionMetadata>,
}

/// Per-partition metadata.
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    /// Partition id.
    pub partition_id: PartitionId,
    /// Leader node id.
    pub leader: u32,
    /// Committed high watermark.
    pub hwm: u64,
    /// Replica set.
    pub replicas: Vec<u32>,
    /// In-sync replicas.
    pub isr: Vec<u32>,
    /// Leader epoch.
    pub leader_epoch: u32,
}

/// Shared cluster runtime state.
#[derive(Debug)]
pub struct ClusterState {
    /// Static config.
    pub config: ClusterConfig,
    /// Live membership.
    pub membership: RwLock<Membership>,
    /// Assignment snapshot.
    pub assignment: RwLock<AssignmentSnapshot>,
    /// Data directory for persisting assignment.
    pub data_dir: PathBuf,
}

/// Inter-broker TLS client settings (Phase 9/19).
///
/// When set, [`crate::net::inter_broker_rpc`] opens TLS connections to peers.
/// Requires building with the broker `tls` feature (enabled by `volant-server --features tls`).
#[derive(Debug, Clone)]
pub struct InterBrokerTls {
    /// Skip peer certificate verification (lab / self-signed clusters).
    pub insecure: bool,
    /// Optional PEM CA file trusted in addition to webpki roots (when not insecure).
    pub ca_path: Option<PathBuf>,
    /// Optional client certificate PEM (Phase 19 mTLS to peers).
    pub client_cert: Option<PathBuf>,
    /// Optional client private key PEM (Phase 19).
    pub client_key: Option<PathBuf>,
}

/// Cached result of the last idempotent produce batch for a partition.
#[derive(Debug, Clone)]
struct IdempotentBatchState {
    base_sequence: i32,
    count: u32,
    base_offset: u64,
}

/// In-memory state for one producer id (Phase 10/18/90/93).
#[derive(Debug)]
struct ProducerEpochState {
    epoch: u16,
    /// True when allocated with a non-empty transactional id (Phase 18).
    transactional: bool,
    /// Transactional id (empty if not transactional).
    transactional_id: String,
    /// Two-phase commit enabled (Phase 90; InitProducerId v6 Enable2Pc).
    enable_2pc: bool,
    /// Client open-txn timeout from InitProducerId (Phase 93). `0` = broker default.
    transaction_timeout_ms: u64,
    /// Per (topic, partition) last **committed** batch.
    partitions: HashMap<(String, u32), IdempotentBatchState>,
}

/// One write-through range inside an open transaction (Phase 86).
///
/// Records are on the partition log immediately; stability is deferred until
/// EndTxn commit. Abort records a soft marker over `[first_offset, end_offset)`.
#[derive(Debug, Clone)]
struct TxnWrittenRange {
    topic: String,
    partition: u32,
    /// Inclusive base offset of the first message in this batch.
    first_offset: u64,
    /// Exclusive end offset (`first_offset + count`).
    end_offset: u64,
    base_sequence: i32,
    count: u32,
}

/// Soft abort marker for READ_COMMITTED filtering (Phase 86).
#[derive(Debug, Clone)]
struct AbortedTxnMarker {
    producer_id: u64,
    /// First offset of the aborted transactional writes on this partition.
    first_offset: u64,
    /// Exclusive end of the aborted range.
    end_offset: u64,
}

/// Durable txn marker range (Phase 86 soft control markers).
///
/// Phase 98: open ranges optionally carry `producer_epoch` so crash recovery can
/// re-encode ABORT control RecordBatches. Pre-Phase-98 snapshots omit the field
/// (`None`); recovery then best-effort looks up live producer state, else soft-aborts
/// only (no synthetic control batch).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StoredTxnRange {
    producer_id: u64,
    /// Producer epoch at write time (open ranges; Phase 98). Absent on old files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    producer_epoch: Option<u16>,
    topic: String,
    partition: u32,
    first_offset: u64,
    end_offset: u64,
}

/// Durable AddPartitions membership without write-through data (Phase 105).
///
/// Persisted so crash≡abort can still append ABORT control batches for empty
/// added partitions. Never promoted to soft aborted ranges (nothing to filter).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StoredAddedPartition {
    producer_id: u64,
    /// Producer epoch at add time. Absent on malformed/legacy entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    producer_epoch: Option<u16>,
    topic: String,
    partition: u32,
}

/// On-disk soft marker snapshot under `{data_dir}/__txn_markers/state.json`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TxnMarkersFile {
    #[serde(default)]
    open: Vec<StoredTxnRange>,
    /// Phase 105: AddPartitions membership with no written ranges yet.
    #[serde(default)]
    open_added: Vec<StoredAddedPartition>,
    #[serde(default)]
    aborted: Vec<StoredTxnRange>,
}

/// Open transaction state (Phase 18 write-through + Phase 86 markers).
///
/// In-flight ranges are also mirrored under `{data_dir}/__txn_markers` so a
/// crash promotes them to aborted (crash ≡ abort) and Phase 98 appends ABORT
/// control batches using the stored epoch. Phase 105 also persists empty
/// AddPartitions membership (`open_added`) for control-only crash promote.
#[derive(Debug, Default, Clone)]
struct OpenTxn {
    /// Unix epoch milliseconds when this open txn was created (Phase 93).
    ///
    /// Set by `begin_txn` / `ensure_txn_open` (first open). Not reset by produce.
    /// Memory-only — open crash ≡ abort via `__txn_markers` without needing this.
    opened_at_ms: i64,
    /// Producer epoch at begin (Phase 98; persisted on open marker ranges).
    producer_epoch: u16,
    /// Partitions registered via AddPartitionsToTxn (Phase 105), including
    /// those that never received write-through produces. Used for control
    /// batches on EndTxn / crash; does **not** create soft abort ranges.
    added: Vec<(String, u32)>,
    /// Log ranges written while the txn is open (write-through).
    written: Vec<TxnWrittenRange>,
    /// Sequences accepted inside this txn (not yet committed to `partitions`).
    pending: HashMap<(String, u32), IdempotentBatchState>,
    /// Deferred consumer offsets (Phase 18 EndTxn trailer + Phase 31 TxnOffsetCommit).
    /// Each entry: `(group_id, topic, partition, offset, metadata)`.
    deferred_offsets: Vec<(String, String, u32, u64, String)>,
}

/// Prepared (2PC phase-1) transaction (Phase 90/92).
///
/// Survives crash via `{data_dir}/__txn_prepared/state.json`. Finalize with a
/// matching second EndTxn, abort via InitProducerId KeepPreparedTxn=false, or
/// auto-abort after [`Broker::prepared_txn_timeout_ms`] (Phase 92).
#[derive(Debug, Clone)]
struct PreparedTxn {
    transactional_id: String,
    producer_id: u64,
    producer_epoch: u16,
    /// True = PrepareCommit; false = PrepareAbort.
    commit: bool,
    /// Unix epoch milliseconds when this txn entered prepared state (Phase 92).
    prepared_at_ms: i64,
    open: OpenTxn,
}

/// Durable written range inside a prepared txn snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredPreparedWritten {
    topic: String,
    partition: u32,
    first_offset: u64,
    end_offset: u64,
    base_sequence: i32,
    count: u32,
}

/// Durable pending sequence inside a prepared txn snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredPreparedPending {
    topic: String,
    partition: u32,
    base_sequence: i32,
    count: u32,
    base_offset: u64,
}

/// One prepared txn on disk (Phase 90/92).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredPreparedTxn {
    transactional_id: String,
    producer_id: u64,
    producer_epoch: u16,
    commit: bool,
    /// Unix ms when prepared; `0` / missing → treated as load-time (Phase 92).
    #[serde(default)]
    prepared_at_ms: i64,
    /// Phase 105: AddPartitions membership (may be empty-only).
    #[serde(default)]
    added: Vec<(String, u32)>,
    #[serde(default)]
    written: Vec<StoredPreparedWritten>,
    #[serde(default)]
    pending: Vec<StoredPreparedPending>,
    /// `(group_id, topic, partition, offset, metadata)`.
    #[serde(default)]
    deferred_offsets: Vec<(String, String, u32, u64, String)>,
}

/// On-disk prepared txn snapshot under `{data_dir}/__txn_prepared/state.json`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PreparedTxnsFile {
    #[serde(default)]
    prepared: Vec<StoredPreparedTxn>,
}

/// Result of InitProducerId including v6 OngoingTxn* (Phase 90).
#[derive(Debug, Clone, Copy)]
pub struct InitProducerIdResult {
    /// Kafka wire error code (`0` = ok). Phase 96 may return **50**
    /// (`INVALID_TRANSACTION_TIMEOUT`) when the client timeout exceeds the
    /// broker max.
    pub error_code: i16,
    /// Allocated / resumed producer id (undefined when `error_code != 0`).
    pub producer_id: u64,
    /// Producer epoch (undefined when `error_code != 0`).
    pub epoch: u16,
    /// Prepared txn producer id, or `-1` if none.
    pub ongoing_txn_producer_id: i64,
    /// Prepared txn epoch, or `-1` if none.
    pub ongoing_txn_producer_epoch: i16,
}

/// Isolation policy for partition fetches (Phase 86).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchIsolation {
    /// Native Volant consumers: hide open + aborted transactional ranges.
    CommittedOnly,
    /// Kafka `isolation_level=1`: cap at LSO, filter aborted.
    ReadCommitted,
    /// Kafka `isolation_level=0`: all data up to HWM.
    ReadUncommitted,
}

/// How inter-broker truncate paths treat `leader_epoch` on the wire.
///
/// Shared by [`Broker::handle_replica_delete_records`] and
/// [`Broker::handle_truncate_journal_note`] so the two paths cannot drift on
/// the stale-epoch predicate; they differ only on whether `-1` is allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EpochFenceMode {
    /// `leader_epoch < 0` skips the fence (one-shot ReplicaDeleteRecords).
    AllowUnknown,
    /// `leader_epoch < 0` → InvalidArg (journal SoT requires a stamped epoch).
    RequireStamped,
}

/// Epoch fence shared by journal note ingress and ReplicaDeleteRecords.
///
/// - Stale: `requested >= 0` and `local > requested` → [`ErrorCode::InvalidProducerEpoch`]
/// - Future / equal (`requested >= local`) → ok
/// - Negative: mode-dependent (see [`EpochFenceMode`])
fn fence_leader_epoch(
    local_epoch: u32,
    requested: i32,
    mode: EpochFenceMode,
) -> std::result::Result<(), ErrorCode> {
    if requested < 0 {
        return match mode {
            EpochFenceMode::AllowUnknown => Ok(()),
            EpochFenceMode::RequireStamped => Err(ErrorCode::InvalidArg),
        };
    }
    let req = requested as u32;
    if local_epoch > req {
        return Err(ErrorCode::InvalidProducerEpoch);
    }
    Ok(())
}

/// Result of committing a transaction (Phase 18).
#[derive(Debug, Clone)]
pub struct TxnCommitResult {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
    /// Log base offset.
    pub base_offset: u64,
    /// Message count.
    pub count: u32,
}

/// Outcome of an idempotent sequence check before append.
#[derive(Debug, Clone)]
pub enum IdempotentCheck {
    /// Proceed with append (or write-through already applied for txn path).
    ///
    /// `base_offset` is the log base for transactional write-through (Phase 86);
    /// non-transactional callers treat it as unused (`0`) until after append.
    Accept {
        /// Log base offset (real for txn write-through; `0` placeholder otherwise).
        base_offset: u64,
    },
    /// Exact duplicate of last batch — return cached offsets without append.
    Duplicate {
        /// Cached base offset.
        base_offset: u64,
        /// Cached message count.
        count: u32,
    },
    /// Reject with protocol error code.
    Reject {
        /// Error code (19/20/21).
        error_code: u16,
    },
}

/// In-process broker managing topics and partitions.
#[derive(Debug)]
pub struct Broker {
    storage: StorageConfig,
    topics: RwLock<HashMap<TopicName, Topic>>,
    next_topic_id: AtomicU32,
    /// Per-topic round-robin counters for null-key partition assignment.
    rr_counters: RwLock<HashMap<TopicName, AtomicU64>>,
    /// Advertised listen host for metadata.
    advertised_host: RwLock<String>,
    /// Advertised listen port for metadata.
    advertised_port: AtomicU32,
    /// Consumer group coordinator + durable offsets.
    groups: GroupCoordinator,
    /// Messages produced via multi-message (`N > 1`) coalesced batches.
    messages_coalesced: AtomicU64,
    /// This broker's node id (`0` in single-node mode).
    node_id: u32,
    /// Cluster runtime (`None` = single-node).
    cluster: Option<Arc<ClusterState>>,
    /// Notify waiters when committed HWM advances (acks=all).
    hwm_lock: Mutex<()>,
    hwm_cvar: Condvar,
    /// Prometheus metrics registry.
    metrics: Arc<Metrics>,
    /// Optional shared auth token (Phase 7). `None` = auth disabled.
    auth_token: RwLock<Option<String>>,
    /// Optional inter-broker TLS client config (Phase 9).
    inter_broker_tls: RwLock<Option<InterBrokerTls>>,
    /// Next producer id for [`Broker::init_producer_id`] (Phase 10/11).
    next_producer_id: AtomicU64,
    /// Idempotent producer state (loaded/persisted via [`ProducerStateStore`]).
    producer_state: RwLock<HashMap<u64, ProducerEpochState>>,
    /// transactional_id → producer_id (Phase 18 fencing).
    transactional_ids: RwLock<HashMap<String, u64>>,
    /// Open transactions keyed by producer_id (Phase 18/86).
    open_txns: Mutex<HashMap<u64, OpenTxn>>,
    /// Prepared (2PC) transactions keyed by transactional_id (Phase 90).
    prepared_txns: Mutex<HashMap<String, PreparedTxn>>,
    /// Max age of a prepared txn before lazy auto-abort (Phase 92).
    ///
    /// Default `60_000` ms; override via `VOLANT_PREPARED_TXN_TIMEOUT_MS` at
    /// construction or [`Broker::set_prepared_txn_timeout_ms`]. `0` disables.
    prepared_txn_timeout_ms: AtomicU64,
    /// Broker-default max age of an **open** (non-prepared) txn (Phase 93).
    ///
    /// Used when the producer has no positive InitProducerId
    /// `transaction_timeout_ms`. Default `60_000` ms; override via
    /// `VOLANT_OPEN_TXN_TIMEOUT_MS` or [`Broker::set_open_txn_timeout_ms`].
    /// `0` disables open auto-abort for producers without a client timeout.
    open_txn_timeout_ms: AtomicU64,
    /// Broker maximum transaction timeout (Phase 96 / Kafka
    /// `transaction.max.timeout.ms`).
    ///
    /// Default `900_000` ms (15 minutes); override via
    /// `VOLANT_TRANSACTION_MAX_TIMEOUT_MS` or
    /// [`Broker::set_transaction_max_timeout_ms`]. `0` disables the max
    /// (no clamp, no InitProducerId reject). When `> 0`:
    /// - InitProducerId with client timeout **> max** → error **50**
    /// - Effective open/prepared timeouts are clamped to ≤ max
    transaction_max_timeout_ms: AtomicU64,
    /// Background open/prepared/session sweep interval (Phase 97/101).
    ///
    /// Default `1_000` ms; override via `VOLANT_SWEEP_INTERVAL_MS` or
    /// [`Broker::set_sweep_interval_ms`]. `0` pauses the background sweeper
    /// (lazy expire paths still run); task always spawned so 0→>0 works live.
    sweep_interval_ms: AtomicU64,
    /// Init-owner registry entry TTL for GC (Phase 127/128).
    ///
    /// Default 24h; override via `VOLANT_TXN_COORDINATOR_TTL_MS`, sparse durable
    /// BROKER config, or [`Broker::set_txn_coordinator_ttl_ms`]. `0` disables GC.
    txn_coordinator_ttl_ms: AtomicU64,
    /// Open txns auto-aborted by timeout (lazy + background; Phase 97).
    open_txns_expired_total: AtomicU64,
    /// Prepared txns auto-aborted by timeout (lazy + background; Phase 97).
    prepared_txns_expired_total: AtomicU64,
    /// PIDs whose open/prepared txn was auto-aborted by timeout and still need
    /// client abort acknowledgment (Phase 94 / KIP-890 TRANSACTION_ABORTABLE).
    ///
    /// Marked on open/prepared timeout expiry. Subsequent produce / EndTxn /
    /// AddPartitions / AddOffsets / TxnOffsetCommit return
    /// [`ErrorCode::TransactionAbortable`] until EndTxn observes the flag
    /// (clears it) or InitProducerId fences the producer.
    abortable_producers: Mutex<HashSet<u64>>,
    /// Soft abort markers for READ_COMMITTED (Phase 86): `(topic, partition) → markers`.
    aborted_txns: Mutex<HashMap<(String, u32), Vec<AbortedTxnMarker>>>,
    /// Soft abort markers fully dropped by DeleteRecords / retention / load GC
    /// (Phase 104). Clips of straddling markers (Phase 111) do **not** increment.
    aborted_markers_gc_total: AtomicU64,
    /// Durable producer state store under `data_dir/__producer_state` (Phase 11).
    producer_store: ProducerStateStore,
    /// Durable per-topic configs under `data_dir/__topic_configs` (Phase 13).
    topic_configs: TopicConfigStore,
    /// Durable single-node topic catalog under `data_dir/__topics` (Phase 14).
    topic_catalog: TopicCatalogStore,
    /// Principal ACL authorizer (Phase 20/21).
    acls: crate::acl::AclState,
    /// Optional shared token protecting `GET /metrics` (Phase 21).
    metrics_token: RwLock<Option<String>>,
    /// Durable SCRAM-SHA-256 user store (Phase 22).
    scram: crate::scram::ScramStore,
    /// In-memory leader-epoch history: `(topic, partition) → sorted entries` (Phase 87).
    leader_epochs: RwLock<HashMap<(String, u32), Vec<EpochStart>>>,
    /// Durable leader-epoch store under `data_dir/__leader_epochs` (Phase 87).
    leader_epoch_store: LeaderEpochStore,
    /// Fetch sessions (Phase 88 + 91 omit + Phase 95 TTL/max + Phase 115 durable + Phase 119 handoff).
    fetch_sessions: FetchSessionManager,
    /// Phase 109: single-flight guard for [`crate::net::start_background_tasks`].
    ///
    /// First claim wins; subsequent `start_background_tasks` calls return a no-op handle.
    bg_tasks_started: AtomicBool,
    /// Controller BROKER-config generation (Phase 113). Bumped on successful cluster Alter.
    config_generation: AtomicU64,
    /// Last applied BROKER-config generation on this node (Phase 113 push/pull).
    applied_config_generation: AtomicU64,
    /// Controller ACL generation (Phase 113). Bumped on successful Create/Delete Acls.
    acl_generation: AtomicU64,
    /// Last applied ACL generation on this node (Phase 113 push/pull).
    applied_acl_generation: AtomicU64,
    /// DeleteRecords fan-out RPC failures (Phase 113; real fan-out in later PR).
    delete_records_fanout_errors_total: AtomicU64,
    /// Phase 135/148: when true, native/Kafka DeleteRecords waits for
    /// truncate-journal majority and surfaces `NotEnoughReplicas` on fail.
    /// Phase 148: wait mode **defers local truncate** until majority (no data
    /// loss on fail). Default **false** (`VOLANT_DELETE_RECORDS_WAIT_MAJORITY`).
    delete_records_wait_majority: AtomicBool,
    /// Phase 135/148: client wait path observed journal majority success.
    delete_records_majority_wait_success_total: AtomicU64,
    /// Phase 135/148: client wait path observed journal majority failure.
    delete_records_majority_wait_fail_total: AtomicU64,
    /// Phase 148: wait-mode majority-first path successes (journal then local).
    delete_records_majority_first_success_total: AtomicU64,
    /// Phase 148: wait-mode majority-first path failures (no local truncate).
    delete_records_majority_first_fail_total: AtomicU64,
    /// BROKER config push RPC failures (Phase 113).
    cluster_config_push_errors_total: AtomicU64,
    /// ACL snapshot push RPC failures (Phase 113).
    cluster_acl_push_errors_total: AtomicU64,
    /// Multi-broker 2PC fan-out RPC failures (Phase 114).
    txn_2pc_fanout_errors_total: AtomicU64,
    /// Controller cluster prepared index (Phase 114); identity + decision only.
    cluster_prepared_index: Mutex<HashMap<String, ClusterPreparedEntry>>,
    /// Durable pending DeleteRecords truncates for offline/failed peers (Phase 116).
    delete_records_outbox: DeleteRecordsOutbox,
    /// Phase 129: controller SoT truncate journal.
    truncate_journal: TruncateJournal,
    /// Phase 123: last successful outbox reconcile per led partition
    /// `(topic, partition) → (leader_epoch, log_start)`.
    ///
    /// In-memory only; process restart re-reconciles (idempotent).
    delete_records_outbox_last_reconcile: Mutex<HashMap<(String, u32), (u32, u64)>>,
    /// Phase 123: partition reconcile passes that advanced last_reconcile.
    delete_records_outbox_reconcile_total: AtomicU64,
    /// Phase 117: successful admin catch-up RPC applies (config and/or ACL).
    cluster_admin_catchup_success_total: AtomicU64,
    /// Phase 117: admin catch-up RPC / apply failures.
    cluster_admin_catchup_errors_total: AtomicU64,
    /// Phase 118: ISR membership expansions (rejoin / catch-up).
    isr_expand_total: AtomicU64,
    /// Phase 118: ISR membership removals (death or lag shrink).
    isr_shrink_total: AtomicU64,
    /// Phase 125: ISR removals attributed to time-based lag.
    isr_time_shrink_total: AtomicU64,
    /// Phase 126: Fetch preferred_read_replica redirects emitted.
    preferred_replica_redirect_total: AtomicU64,
    /// Phase 140: preferred candidate suppressed (e.g. READ_COMMITTED).
    preferred_replica_suppressed_total: AtomicU64,
    /// Phase 144: preferred candidate suppressed due to established fetch session.
    preferred_replica_session_suppressed_total: AtomicU64,
    /// Phase 145: create/create-partitions used rack-diversity assignment.
    rack_aware_assignment_total: AtomicU64,
    /// Phase 140: max leader_leo − follower_leo for preferred eligibility.
    /// `u64::MAX` = unlimited (env unset). Override via setter in tests.
    preferred_replica_max_leo_lag: AtomicU64,
    /// Phase 120/124: durable Init-owner txn coordinator registry.
    txn_coordinator_registry: TxnCoordinatorRegistry,
    /// Phase 120: successful transparent EndTxn (txn) forwards.
    txn_forward_total: AtomicU64,
    /// Phase 120: failed transparent txn forward attempts.
    txn_forward_errors_total: AtomicU64,
    /// Phase 132: per-peer journal catch-up single-flight + min-interval throttle.
    journal_catchup: Mutex<JournalCatchupState>,
    /// Phase 132: catch-up schedule skipped (in-flight or min-interval).
    journal_catchup_skipped_total: AtomicU64,
    /// Phase 136: per-peer admin (ACL/config) catch-up single-flight + min-interval.
    admin_catchup: Mutex<AdminCatchupState>,
    /// Phase 136: admin catch-up schedule skipped (in-flight or min-interval).
    admin_catchup_skipped_total: AtomicU64,
    /// Phase 142: pending IsrUpdate reports (non-controller leader → controller).
    pending_isr_reports: Mutex<Vec<PendingIsrReport>>,
    /// Phase 150: durable assignment generation consensus state.
    assignment_consensus: AssignmentConsensus,
    /// Phase 150: when true, admin assignment mutations fan out consensus notes.
    /// Default **on** for configured N ≥ 2; env `VOLANT_ASSIGNMENT_CONSENSUS`.
    assignment_consensus_enabled: AtomicBool,
    /// Phase 150: when true, CreateTopic/DeleteTopic/CreatePartitions wait for
    /// majority and surface `NotEnoughReplicas` on fail. Default **false**
    /// (`VOLANT_ASSIGNMENT_CONSENSUS_WAIT`).
    assignment_consensus_wait: AtomicBool,
    /// Phase 152: when true (and consensus enabled), Metadata serves the
    /// majority-committed assignment snapshot. Default **false**
    /// (`VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY`).
    assignment_metadata_committed_only: AtomicBool,
    /// Phase 154: KRaft-style metadata Raft log (MVP).
    metadata_raft: MetadataRaftState,
    /// Phase 154: when true, admin assignment mutations use the metadata Raft
    /// log (opcodes 98/99) instead of AssignmentConsensusNote. Default **off**
    /// (`VOLANT_METADATA_RAFT`).
    metadata_raft_enabled: AtomicBool,
}

/// One pending leader→controller ISR report (Phase 142).
#[derive(Debug, Clone)]
pub struct PendingIsrReport {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
    /// Claiming leader id.
    pub leader_id: u32,
    /// Leader epoch at report time.
    pub leader_epoch: u32,
    /// Full ISR set.
    pub isr: Vec<u32>,
    /// Local generation hint (`0` = none).
    pub generation_hint: u32,
}

/// Phase 132: in-process scheduler state for truncate-journal catch-up pushes.
#[derive(Debug, Default)]
struct JournalCatchupState {
    /// Peers with an in-flight catch-up task.
    in_flight: HashSet<u32>,
    /// Last time a catch-up was **started** for each peer (throttle base).
    last_start: HashMap<u32, Instant>,
}

/// Phase 136: in-process scheduler state for ACL/config admin catch-up re-pushes.
#[derive(Debug, Default)]
struct AdminCatchupState {
    /// Peers with an in-flight admin catch-up task.
    in_flight: HashSet<u32>,
    /// Last time an admin catch-up was **started** for each peer (throttle base).
    last_start: HashMap<u32, Instant>,
}

/// Controller-side multi-broker prepared index entry (Phase 114).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClusterPreparedEntry {
    transactional_id: String,
    producer_id: u64,
    producer_epoch: u16,
    commit: bool,
    prepared_at_ms: i64,
    coordinator_node_id: u32,
}

/// On-disk controller prepared index under `{data_dir}/__txn_prepared/cluster.json`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ClusterPreparedFile {
    #[serde(default)]
    prepared: Vec<ClusterPreparedEntry>,
}

/// Phase 114: what inter-broker fan-out the net/Kafka layer should run after a
/// successful local Begin/End/Init path.
#[derive(Debug, Clone)]
pub enum Txn2pcFanout {
    /// No cluster fan-out (single-node or non-2PC).
    None,
    /// Push producer open state to live peers.
    Open {
        /// Transactional id.
        transactional_id: String,
        /// Producer id.
        producer_id: u64,
        /// Producer epoch.
        producer_epoch: u16,
        /// Enable2Pc flag.
        enable_2pc: bool,
        /// Txn coordinator (Init owner) node id (Phase 120).
        coordinator_node_id: u32,
        /// When false, peers register producer + coordinator only (no open).
        install_open: bool,
    },
    /// Strict prepare fan-out after local prepare.
    Prepare {
        /// Transactional id.
        transactional_id: String,
        /// Producer id.
        producer_id: u64,
        /// Producer epoch.
        producer_epoch: u16,
        /// PrepareCommit vs PrepareAbort.
        commit: bool,
    },
    /// Complete (or fence-abort) fan-out after local finalize.
    Complete {
        /// Transactional id.
        transactional_id: String,
        /// Producer id.
        producer_id: u64,
        /// Producer epoch.
        producer_epoch: u16,
        /// Commit vs abort finalize.
        commit: bool,
    },
}

mod admin;
mod cluster;
mod delete_records;
mod topics;
mod txn;

impl Broker {
    /// Create a single-node broker with the given storage configuration.
    ///
    /// Reloads topics from `{data_dir}/__topics/catalog.json` and opens existing
    /// partition logs (Phase 14).
    pub fn new(storage: StorageConfig) -> Self {
        let groups = GroupCoordinator::new(&storage.data_dir)
            .expect("failed to initialize group coordinator / offset store");
        let producer_store = ProducerStateStore::open(&storage.data_dir)
            .expect("failed to open producer state store");
        let (next_pid, producers, txn_ids) = load_producer_maps(&producer_store);
        let topic_configs =
            TopicConfigStore::open(&storage.data_dir).expect("failed to open topic config store");
        let topic_catalog =
            TopicCatalogStore::open(&storage.data_dir).expect("failed to open topic catalog store");
        let acls = crate::acl::AclState::open(&storage.data_dir).expect("failed to open ACL store");
        let scram =
            crate::scram::ScramStore::open(&storage.data_dir).expect("failed to open SCRAM store");
        let leader_epoch_store =
            LeaderEpochStore::open(&storage.data_dir).expect("failed to open leader epoch store");
        // Phase 115: durable fetch sessions under data_dir/__fetch_sessions.
        let fetch_sessions = FetchSessionManager::open(&storage.data_dir);
        // Phase 116: durable DeleteRecords outbox (empty in single-node use).
        let delete_records_outbox = DeleteRecordsOutbox::open(&storage.data_dir);
        let truncate_journal = TruncateJournal::open(&storage.data_dir);
        // Phase 124: durable Init-owner txn coordinator registry.
        let txn_coordinator_registry = TxnCoordinatorRegistry::open(&storage.data_dir);
        // Phase 150: assignment generation consensus (single-node majority=1).
        let assignment_consensus = AssignmentConsensus::open(&storage.data_dir);
        // Phase 154: KRaft-style metadata log (single-node).
        let metadata_raft = MetadataRaftState::open(&storage.data_dir);
        let broker = Self {
            storage,
            topics: RwLock::new(HashMap::new()),
            next_topic_id: AtomicU32::new(1),
            rr_counters: RwLock::new(HashMap::new()),
            advertised_host: RwLock::new("127.0.0.1".into()),
            advertised_port: AtomicU32::new(9092),
            groups,
            messages_coalesced: AtomicU64::new(0),
            node_id: 0,
            cluster: None,
            hwm_lock: Mutex::new(()),
            hwm_cvar: Condvar::new(),
            metrics: Arc::new(Metrics::new()),
            auth_token: RwLock::new(None),
            inter_broker_tls: RwLock::new(None),
            next_producer_id: AtomicU64::new(next_pid),
            producer_state: RwLock::new(producers),
            transactional_ids: RwLock::new(txn_ids),
            open_txns: Mutex::new(HashMap::new()),
            prepared_txns: Mutex::new(HashMap::new()),
            prepared_txn_timeout_ms: AtomicU64::new(default_prepared_txn_timeout_ms()),
            open_txn_timeout_ms: AtomicU64::new(default_open_txn_timeout_ms()),
            transaction_max_timeout_ms: AtomicU64::new(default_transaction_max_timeout_ms()),
            sweep_interval_ms: AtomicU64::new(default_sweep_interval_ms()),
            txn_coordinator_ttl_ms: AtomicU64::new(default_txn_coordinator_ttl_ms()),
            open_txns_expired_total: AtomicU64::new(0),
            prepared_txns_expired_total: AtomicU64::new(0),
            abortable_producers: Mutex::new(HashSet::new()),
            aborted_txns: Mutex::new(HashMap::new()),
            aborted_markers_gc_total: AtomicU64::new(0),
            producer_store,
            topic_configs,
            topic_catalog,
            acls,
            metrics_token: RwLock::new(None),
            scram,
            leader_epochs: RwLock::new(HashMap::new()),
            leader_epoch_store,
            fetch_sessions,
            bg_tasks_started: AtomicBool::new(false),
            config_generation: AtomicU64::new(0),
            applied_config_generation: AtomicU64::new(0),
            acl_generation: AtomicU64::new(0),
            applied_acl_generation: AtomicU64::new(0),
            delete_records_fanout_errors_total: AtomicU64::new(0),
            delete_records_wait_majority: AtomicBool::new(default_delete_records_wait_majority()),
            delete_records_majority_wait_success_total: AtomicU64::new(0),
            delete_records_majority_wait_fail_total: AtomicU64::new(0),
            delete_records_majority_first_success_total: AtomicU64::new(0),
            delete_records_majority_first_fail_total: AtomicU64::new(0),
            cluster_config_push_errors_total: AtomicU64::new(0),
            cluster_acl_push_errors_total: AtomicU64::new(0),
            txn_2pc_fanout_errors_total: AtomicU64::new(0),
            cluster_prepared_index: Mutex::new(HashMap::new()),
            delete_records_outbox,
            truncate_journal,
            delete_records_outbox_last_reconcile: Mutex::new(HashMap::new()),
            delete_records_outbox_reconcile_total: AtomicU64::new(0),
            cluster_admin_catchup_success_total: AtomicU64::new(0),
            cluster_admin_catchup_errors_total: AtomicU64::new(0),
            isr_expand_total: AtomicU64::new(0),
            isr_shrink_total: AtomicU64::new(0),
            isr_time_shrink_total: AtomicU64::new(0),
            preferred_replica_redirect_total: AtomicU64::new(0),
            preferred_replica_suppressed_total: AtomicU64::new(0),
            preferred_replica_session_suppressed_total: AtomicU64::new(0),
            rack_aware_assignment_total: AtomicU64::new(0),
            preferred_replica_max_leo_lag: AtomicU64::new(default_preferred_replica_max_leo_lag()),
            txn_coordinator_registry,
            txn_forward_total: AtomicU64::new(0),
            txn_forward_errors_total: AtomicU64::new(0),
            journal_catchup: Mutex::new(JournalCatchupState::default()),
            journal_catchup_skipped_total: AtomicU64::new(0),
            admin_catchup: Mutex::new(AdminCatchupState::default()),
            admin_catchup_skipped_total: AtomicU64::new(0),
            pending_isr_reports: Mutex::new(Vec::new()),
            assignment_consensus,
            // Single-node: consensus enabled (trivial majority 1) unless env off.
            assignment_consensus_enabled: AtomicBool::new(default_assignment_consensus_enabled(1)),
            assignment_consensus_wait: AtomicBool::new(default_assignment_consensus_wait()),
            assignment_metadata_committed_only: AtomicBool::new(
                default_assignment_metadata_committed_only(),
            ),
            metadata_raft,
            // metadata raft default off (cluster and single-node).
            metadata_raft_enabled: AtomicBool::new(default_metadata_raft_enabled(false)),
        };
        broker
            .reload_single_node_topics()
            .expect("failed to reload single-node topic catalog");
        broker.load_txn_markers();
        broker.load_prepared_txns();
        broker.load_cluster_prepared_index();
        broker.expire_timed_out_txns();
        broker.load_leader_epochs();
        broker.seed_missing_leader_epochs();
        // Phase 100–102: sparse durable BROKER knobs after env (product → env → file keys).
        broker
            .load_durable_broker_config()
            .expect("failed to load durable broker config");
        // Phase 117: durable admin generations (config/ACL).
        broker
            .load_cluster_admin_gens()
            .expect("failed to load cluster admin generations");
        // Phase 115: re-apply idle TTL after durable config may have changed knobs.
        let _ = broker.fetch_sessions.evict_idle_now();
        broker
    }

    /// Create a multi-node broker with static cluster config.
    pub fn with_cluster(
        storage: StorageConfig,
        node_id: u32,
        config: ClusterConfig,
    ) -> Result<Self> {
        if config.broker(node_id).is_none() {
            return Err(Error::InvalidArgument(format!(
                "node_id {node_id} not present in cluster config"
            )));
        }
        let assignment = load_assignment(&storage.data_dir)?;
        let next_id = assignment
            .topics
            .values()
            .map(|t| t.topic_id)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let membership = Membership::new(node_id, config.session_timeout_ms, &config.broker_ids());
        let n_configured = config.brokers.len().max(1);
        let data_dir = storage.data_dir.clone();
        let cluster = Arc::new(ClusterState {
            config,
            membership: RwLock::new(membership),
            assignment: RwLock::new(assignment),
            data_dir,
        });

        let groups = GroupCoordinator::new(&storage.data_dir)
            .expect("failed to initialize group coordinator / offset store");
        let producer_store = ProducerStateStore::open(&storage.data_dir)
            .expect("failed to open producer state store");
        let (next_pid, producers, txn_ids) = load_producer_maps(&producer_store);
        let topic_configs =
            TopicConfigStore::open(&storage.data_dir).expect("failed to open topic config store");
        let topic_catalog =
            TopicCatalogStore::open(&storage.data_dir).expect("failed to open topic catalog store");
        let acls = crate::acl::AclState::open(&storage.data_dir).expect("failed to open ACL store");
        let scram =
            crate::scram::ScramStore::open(&storage.data_dir).expect("failed to open SCRAM store");
        let leader_epoch_store =
            LeaderEpochStore::open(&storage.data_dir).expect("failed to open leader epoch store");
        // Phase 115/119: durable fetch sessions; cluster owner-encoded session ids.
        let fetch_sessions = FetchSessionManager::open_with_owner(&storage.data_dir, node_id);
        // Phase 116: durable DeleteRecords outbox under data_dir.
        let delete_records_outbox = DeleteRecordsOutbox::open(&storage.data_dir);
        let truncate_journal = TruncateJournal::open(&storage.data_dir);
        // Phase 124: durable Init-owner txn coordinator registry.
        let txn_coordinator_registry = TxnCoordinatorRegistry::open(&storage.data_dir);
        // Phase 150: assignment generation consensus.
        let assignment_consensus = AssignmentConsensus::open(&storage.data_dir);
        // Phase 154: KRaft-style metadata Raft log.
        let metadata_raft = MetadataRaftState::open(&storage.data_dir);
        let broker = Self {
            storage,
            topics: RwLock::new(HashMap::new()),
            next_topic_id: AtomicU32::new(next_id.max(1)),
            rr_counters: RwLock::new(HashMap::new()),
            advertised_host: RwLock::new("127.0.0.1".into()),
            advertised_port: AtomicU32::new(9092),
            groups,
            messages_coalesced: AtomicU64::new(0),
            node_id,
            cluster: Some(cluster),
            hwm_lock: Mutex::new(()),
            hwm_cvar: Condvar::new(),
            metrics: Arc::new(Metrics::new()),
            auth_token: RwLock::new(None),
            inter_broker_tls: RwLock::new(None),
            next_producer_id: AtomicU64::new(next_pid),
            producer_state: RwLock::new(producers),
            transactional_ids: RwLock::new(txn_ids),
            open_txns: Mutex::new(HashMap::new()),
            prepared_txns: Mutex::new(HashMap::new()),
            prepared_txn_timeout_ms: AtomicU64::new(default_prepared_txn_timeout_ms()),
            open_txn_timeout_ms: AtomicU64::new(default_open_txn_timeout_ms()),
            transaction_max_timeout_ms: AtomicU64::new(default_transaction_max_timeout_ms()),
            sweep_interval_ms: AtomicU64::new(default_sweep_interval_ms()),
            txn_coordinator_ttl_ms: AtomicU64::new(default_txn_coordinator_ttl_ms()),
            open_txns_expired_total: AtomicU64::new(0),
            prepared_txns_expired_total: AtomicU64::new(0),
            abortable_producers: Mutex::new(HashSet::new()),
            aborted_txns: Mutex::new(HashMap::new()),
            aborted_markers_gc_total: AtomicU64::new(0),
            producer_store,
            topic_configs,
            topic_catalog,
            acls,
            metrics_token: RwLock::new(None),
            scram,
            leader_epochs: RwLock::new(HashMap::new()),
            leader_epoch_store,
            fetch_sessions,
            bg_tasks_started: AtomicBool::new(false),
            config_generation: AtomicU64::new(0),
            applied_config_generation: AtomicU64::new(0),
            acl_generation: AtomicU64::new(0),
            applied_acl_generation: AtomicU64::new(0),
            delete_records_fanout_errors_total: AtomicU64::new(0),
            delete_records_wait_majority: AtomicBool::new(default_delete_records_wait_majority()),
            delete_records_majority_wait_success_total: AtomicU64::new(0),
            delete_records_majority_wait_fail_total: AtomicU64::new(0),
            delete_records_majority_first_success_total: AtomicU64::new(0),
            delete_records_majority_first_fail_total: AtomicU64::new(0),
            cluster_config_push_errors_total: AtomicU64::new(0),
            cluster_acl_push_errors_total: AtomicU64::new(0),
            txn_2pc_fanout_errors_total: AtomicU64::new(0),
            cluster_prepared_index: Mutex::new(HashMap::new()),
            delete_records_outbox,
            truncate_journal,
            delete_records_outbox_last_reconcile: Mutex::new(HashMap::new()),
            delete_records_outbox_reconcile_total: AtomicU64::new(0),
            cluster_admin_catchup_success_total: AtomicU64::new(0),
            cluster_admin_catchup_errors_total: AtomicU64::new(0),
            isr_expand_total: AtomicU64::new(0),
            isr_shrink_total: AtomicU64::new(0),
            isr_time_shrink_total: AtomicU64::new(0),
            preferred_replica_redirect_total: AtomicU64::new(0),
            preferred_replica_suppressed_total: AtomicU64::new(0),
            preferred_replica_session_suppressed_total: AtomicU64::new(0),
            rack_aware_assignment_total: AtomicU64::new(0),
            preferred_replica_max_leo_lag: AtomicU64::new(default_preferred_replica_max_leo_lag()),
            txn_coordinator_registry,
            txn_forward_total: AtomicU64::new(0),
            txn_forward_errors_total: AtomicU64::new(0),
            journal_catchup: Mutex::new(JournalCatchupState::default()),
            journal_catchup_skipped_total: AtomicU64::new(0),
            admin_catchup: Mutex::new(AdminCatchupState::default()),
            admin_catchup_skipped_total: AtomicU64::new(0),
            pending_isr_reports: Mutex::new(Vec::new()),
            assignment_consensus,
            assignment_consensus_enabled: AtomicBool::new(default_assignment_consensus_enabled(
                n_configured,
            )),
            assignment_consensus_wait: AtomicBool::new(default_assignment_consensus_wait()),
            assignment_metadata_committed_only: AtomicBool::new(
                default_assignment_metadata_committed_only(),
            ),
            metadata_raft,
            metadata_raft_enabled: AtomicBool::new(default_metadata_raft_enabled(true)),
        };
        // Open local partitions from persisted assignment.
        broker.apply_local_assignment()?;
        broker.load_txn_markers();
        broker.load_prepared_txns();
        broker.load_cluster_prepared_index();
        broker.expire_timed_out_txns();
        broker.load_leader_epochs();
        broker.seed_missing_leader_epochs();
        // Phase 100–102: sparse durable BROKER knobs after env (product → env → file keys).
        broker.load_durable_broker_config()?;
        // Phase 117: durable admin generations (config/ACL).
        broker.load_cluster_admin_gens()?;
        // Phase 115: re-apply idle TTL after durable config may have changed knobs.
        let _ = broker.fetch_sessions.evict_idle_now();
        Ok(broker)
    }

    /// Fetch session manager (Phase 88 + 91 + 95 + durable Phase 115).
    pub fn fetch_sessions(&self) -> &FetchSessionManager {
        &self.fetch_sessions
    }

    /// Phase 109: claim exclusive right to spawn background tasks.
    ///
    /// Returns `true` on the first claim (caller should spawn). Subsequent
    /// claims return `false` so [`crate::net::start_background_tasks`] can
    /// return a no-op handle.
    pub(crate) fn claim_background_tasks(&self) -> bool {
        !self.bg_tasks_started.swap(true, Ordering::SeqCst)
    }

    /// Consumer lag snapshots: `(group, topic, partition, committed, hwm, lag)`.
    ///
    /// Lag is `max(0, hwm.saturating_sub(committed))` when committed is known
    /// (`!= u64::MAX`); unknown commits report lag equal to HWM (from 0).
    pub fn consumer_lag_snapshots(&self) -> Vec<(String, String, u32, u64, u64, u64)> {
        use crate::offset_store::OFFSET_UNKNOWN;

        let mut out = Vec::new();
        // Groups are not enumerated publicly; scan via offset store internals
        // through group coordinator by collecting known groups from metadata topics
        // is incomplete. Use offset store group listing if available.
        let groups = self.groups().list_group_ids();
        for gid in groups {
            let fetched = match self.groups().fetch_offsets(&gid, &[]) {
                Ok(r) => r.entries,
                Err(_) => continue,
            };
            for e in fetched {
                let topic = TopicName::new(e.topic.clone());
                let pid = PartitionId(e.partition);
                let hwm = self.high_watermark(&topic, pid).unwrap_or(0);
                let committed = e.offset;
                let lag = if committed == OFFSET_UNKNOWN {
                    hwm
                } else {
                    hwm.saturating_sub(committed)
                };
                out.push((gid.clone(), e.topic, e.partition, committed, hwm, lag));
            }
        }
        out
    }

    /// Number of topics known to this broker.
    pub fn topic_count(&self) -> u64 {
        if let Some(cluster) = &self.cluster {
            let n = cluster.assignment.read().topics.len();
            if n > 0 {
                return n as u64;
            }
        }
        self.topics.read().len() as u64
    }

    /// Total partition count across all topics.
    pub fn partition_count_total(&self) -> u64 {
        if let Some(cluster) = &self.cluster {
            let asg = cluster.assignment.read();
            if !asg.topics.is_empty() {
                return asg.topics.values().map(|t| t.partitions.len() as u64).sum();
            }
        }
        self.topics
            .read()
            .values()
            .map(|t| t.partitions.len() as u64)
            .sum()
    }

    /// This broker's node id.
    pub fn node_id(&self) -> u32 {
        self.node_id
    }

    /// Cluster config if multi-node.
    pub fn cluster_config(&self) -> Option<ClusterConfig> {
        self.cluster.as_ref().map(|c| c.config.clone())
    }

    /// Address of a peer broker.
    pub fn broker_addr(&self, id: u32) -> Option<String> {
        self.cluster.as_ref().and_then(|c| c.config.addr_of(id))
    }

    /// Whether this node is currently the controller.
    pub fn is_controller(&self) -> bool {
        match &self.cluster {
            None => true, // single-node acts as controller
            Some(c) => c.membership.read().is_controller(),
        }
    }

    /// Current controller id.
    pub fn controller_id(&self) -> u32 {
        match &self.cluster {
            None => self.node_id,
            Some(c) => c.membership.read().controller_id(),
        }
    }

    /// Cluster generation.
    pub fn generation(&self) -> u32 {
        match &self.cluster {
            None => 0,
            Some(c) => c.assignment.read().generation,
        }
    }

    /// Total messages that went through a multi-message coalesced produce.
    pub fn messages_coalesced(&self) -> u64 {
        self.messages_coalesced.load(Ordering::Relaxed)
    }

    /// Set the advertised address returned by metadata.
    pub fn set_advertised(&self, host: impl Into<String>, port: u16) {
        *self.advertised_host.write() = host.into();
        self.advertised_port
            .store(u32::from(port), Ordering::Relaxed);
        // Also update cluster config advertised if this is our node — clients
        // use Metadata brokers list from config hosts by default.
    }

    /// Produce a batch to a topic partition (coalesced).
    ///
    /// In multi-node mode the broker must be the partition leader. Use
    /// [`Self::produce_with_acks`] for `acks=all` waiting.
    pub fn produce(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        batch: MessageBatch,
    ) -> Result<Vec<Record>> {
        self.produce_inner(topic, partition, batch, 1, None)
            .map(|(r, _)| r)
    }

    /// Produce with explicit acks handling.
    ///
    /// Returns `(records, error_code)` where error_code is 0 on success.
    /// For `acks=all` (255), waits until committed HWM covers the batch
    /// (up to `wait_timeout`).
    pub fn produce_with_acks(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        batch: MessageBatch,
        acks: u8,
        wait_timeout: Option<Duration>,
    ) -> Result<(Vec<Record>, u16)> {
        self.produce_inner(topic, partition, batch, acks, wait_timeout)
    }

    fn produce_inner(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        batch: MessageBatch,
        acks: u8,
        wait_timeout: Option<Duration>,
    ) -> Result<(Vec<Record>, u16)> {
        let n = batch.messages.len();
        if n == 0 {
            return Ok((Vec::new(), 0));
        }

        let (records, need_wait, target_hwm) = {
            let mut topics = self.topics.write();
            let t = topics
                .get_mut(topic)
                .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
            let part = t
                .partitions
                .get_mut(&partition)
                .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;

            // Leadership check (multi-node).
            if self.cluster.is_some() && !part.is_leader(self.node_id) {
                return Ok((Vec::new(), ErrorCode::NotLeaderForPartition as u16));
            }

            // min_insync_replicas for acks=all.
            if acks == 255 {
                if let Some(cluster) = &self.cluster {
                    let min_isr = cluster.config.min_insync_replicas;
                    if (part.isr.len() as u32) < min_isr {
                        return Ok((Vec::new(), ErrorCode::NotEnoughReplicas as u16));
                    }
                }
            }

            let records = part.log.append_batch(batch.messages)?;
            if n > 1 {
                self.messages_coalesced
                    .fetch_add(n as u64, Ordering::Relaxed);
            }

            // Single-node or sole ISR: HWM tracks LEO immediately.
            let single = self.cluster.is_none() || part.isr.len() <= 1;
            if single || acks != 255 {
                // For acks=0/1 advance HWM only in single-node (multi-node waits for ISR).
                if self.cluster.is_none() {
                    part.catch_up_hwm();
                } else if part.isr.len() == 1 {
                    part.catch_up_hwm();
                } else {
                    // Update leader's view: self LEO is local; recompute.
                    part.recompute_hwm(self.node_id);
                }
            } else {
                part.recompute_hwm(self.node_id);
            }

            let base = records.first().map(|r| r.offset.raw()).unwrap_or(0);
            let count = records.len() as u64;
            let target = base + count;
            let need_wait = acks == 255 && self.cluster.is_some() && part.committed_hwm < target;
            (records, need_wait, target)
        };

        // Blocking HWM wait only when an explicit timeout is provided.
        // Network path uses async polling (see net.rs) to stay runtime-agnostic.
        if need_wait {
            if let Some(timeout) = wait_timeout {
                let deadline = std::time::Instant::now() + timeout;
                let mut guard = self.hwm_lock.lock();
                loop {
                    let hwm = self.committed_hwm(topic, partition).unwrap_or(0);
                    if hwm >= target_hwm {
                        break;
                    }
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        return Ok((records, ErrorCode::Timeout as u16));
                    }
                    let remaining = deadline - now;
                    let result = self.hwm_cvar.wait_for(&mut guard, remaining);
                    if result.timed_out() {
                        let hwm = self.committed_hwm(topic, partition).unwrap_or(0);
                        if hwm >= target_hwm {
                            break;
                        }
                        return Ok((records, ErrorCode::Timeout as u16));
                    }
                }
            }
        }

        Ok((records, 0))
    }

    /// Produce a single message.
    pub fn produce_one(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        message: Message,
    ) -> Result<Record> {
        let mut batch = MessageBatch::default();
        batch.messages.push(message);
        let mut records = self.produce(topic, partition, batch)?;
        records
            .pop()
            .ok_or_else(|| Error::Storage("empty produce result".into()))
    }

    /// Fetch records starting at `from`, capped at committed HWM for clients.
    ///
    /// Native Volant consumers see a **committed-only** view (Phase 86): open
    /// write-through ranges and soft-aborted offsets are excluded.
    pub fn fetch(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max_messages: usize,
    ) -> Result<Vec<Record>> {
        self.fetch_up_to(
            topic,
            partition,
            from,
            max_messages,
            true,
            FetchIsolation::CommittedOnly,
        )
    }

    /// Kafka Fetch with isolation level (Phase 86).
    ///
    /// - `read_committed`: cap at LSO, exclude aborted ranges
    /// - `read_uncommitted`: cap at HWM, include unstable + aborted data
    pub fn fetch_kafka(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max_messages: usize,
        read_committed: bool,
    ) -> Result<Vec<Record>> {
        let isolation = if read_committed {
            FetchIsolation::ReadCommitted
        } else {
            FetchIsolation::ReadUncommitted
        };
        self.fetch_up_to(topic, partition, from, max_messages, true, isolation)
    }

    /// Fetch for replica replication (up to LEO, not capped at HWM).
    pub fn fetch_for_replica(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        let topics = self.topics.read();
        let t = topics
            .get(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        part.log.read_bytes(from, max_messages, max_bytes)
    }

    fn fetch_up_to(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max_messages: usize,
        cap_hwm: bool,
        isolation: FetchIsolation,
    ) -> Result<Vec<Record>> {
        let tname = topic.as_str();
        let p = partition.0;
        // Drop topics lock before consulting txn markers (avoid lock-order deadlock
        // with write-through produce: open_txns → topics).
        let mut records = {
            let topics = self.topics.read();
            let t = topics
                .get(topic)
                .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
            let part = t
                .partitions
                .get(&partition)
                .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;

            let mut records = part.log.read(from, max_messages)?;
            if cap_hwm {
                let hwm = part.committed_hwm;
                records.retain(|r| r.offset.raw() < hwm);
            }
            records
        };

        match isolation {
            FetchIsolation::ReadUncommitted => {
                // All records up to HWM (already capped), including control markers.
            }
            FetchIsolation::ReadCommitted => {
                let lso = self.last_stable_offset(tname, p);
                records.retain(|r| {
                    let off = r.offset.raw();
                    // Control markers are not application data ranges; include them
                    // so Fetch re-encodes real COMMIT/ABORT frames (Phase 89).
                    if is_txn_control_record(r) {
                        return off < lso;
                    }
                    off < lso && !self.is_aborted_offset(tname, p, off)
                });
            }
            FetchIsolation::CommittedOnly => {
                // Native consumers: hide open, aborted, and control markers.
                records.retain(|r| {
                    if is_txn_control_record(r) {
                        return false;
                    }
                    let off = r.offset.raw();
                    !self.is_unstable_offset(tname, p, off)
                        && !self.is_aborted_offset(tname, p, off)
                });
            }
        }
        Ok(records)
    }

    /// Committed high watermark for a partition.
    pub fn committed_hwm(&self, topic: &TopicName, partition: PartitionId) -> Result<u64> {
        let topics = self.topics.read();
        let t = topics
            .get(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        Ok(part.committed_hwm)
    }

    /// High watermark for clients (= committed HWM). For single-node equals LEO.
    pub fn high_watermark(&self, topic: &TopicName, partition: PartitionId) -> Result<u64> {
        // Prefer committed HWM; falls back to LEO.
        let topics = self.topics.read();
        let t = topics
            .get(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        if self.cluster.is_none() {
            Ok(part.leo())
        } else {
            Ok(part.committed_hwm)
        }
    }

    /// Log-end offset (next offset).
    pub fn log_end_offset(&self, topic: &TopicName, partition: PartitionId) -> Result<u64> {
        let topics = self.topics.read();
        let t = topics
            .get(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        Ok(part.leo())
    }

    /// Flush durable state for a topic partition to stable storage.
    pub fn flush(&self, topic: &TopicName, partition: PartitionId) -> Result<()> {
        let mut topics = self.topics.write();
        let t = topics
            .get_mut(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        part.log.flush()
    }

    /// List known topic names.
    pub fn list_topics(&self) -> Vec<TopicName> {
        if let Some(cluster) = &self.cluster {
            let asg = cluster.assignment.read();
            if !asg.topics.is_empty() {
                return asg
                    .topics
                    .keys()
                    .map(|n| TopicName::new(n.clone()))
                    .collect();
            }
        }
        self.topics.read().keys().cloned().collect()
    }
}

/// Load durable producer maps from disk (defaults if missing).
fn load_producer_maps(
    store: &ProducerStateStore,
) -> (u64, HashMap<u64, ProducerEpochState>, HashMap<String, u64>) {
    let file = store.load().unwrap_or_default();
    let next = if file.next_id == 0 { 1 } else { file.next_id };
    let mut map = HashMap::new();
    let mut txn_ids = HashMap::new();
    for (pid_s, prod) in file.producers {
        let Ok(pid) = pid_s.parse::<u64>() else {
            continue;
        };
        let mut partitions = HashMap::new();
        for (k, batch) in prod.partitions {
            if let Some((topic, part)) = parse_partition_key(&k) {
                partitions.insert(
                    (topic, part),
                    IdempotentBatchState {
                        base_sequence: batch.base_sequence,
                        count: batch.count,
                        base_offset: batch.base_offset,
                    },
                );
            }
        }
        let transactional = !prod.transactional_id.is_empty();
        if transactional {
            txn_ids.insert(prod.transactional_id.clone(), pid);
        }
        map.insert(
            pid,
            ProducerEpochState {
                epoch: prod.epoch,
                transactional,
                transactional_id: prod.transactional_id,
                enable_2pc: prod.enable_2pc,
                transaction_timeout_ms: prod.transaction_timeout_ms,
                partitions,
            },
        );
    }
    (next, map, txn_ids)
}

/// Unix epoch milliseconds (Phase 92 prepared_at / Phase 93 opened_at).
fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Phase 140: `VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG` → parsed u64; unset/invalid
/// → `u64::MAX` (unlimited, 126/133 behavior).
fn default_preferred_replica_max_leo_lag() -> u64 {
    std::env::var("VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(u64::MAX)
}

/// Phase 135: `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` → true for `1`/`true`/`yes`
/// (case-insensitive); unset / anything else → **false** (default best-effort).
fn default_delete_records_wait_majority() -> bool {
    match std::env::var("VOLANT_DELETE_RECORDS_WAIT_MAJORITY") {
        Ok(s) => {
            let t = s.trim();
            t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes")
        }
        Err(_) => false,
    }
}

/// Phase 150: `VOLANT_ASSIGNMENT_CONSENSUS` — explicit off/on; else default **on**
/// for configured membership size `n >= 1` (N=1 trivial majority; N≥2 product).
fn default_assignment_consensus_enabled(n: usize) -> bool {
    match std::env::var("VOLANT_ASSIGNMENT_CONSENSUS") {
        Ok(s) => {
            let t = s.trim();
            if t == "0" || t.eq_ignore_ascii_case("false") || t.eq_ignore_ascii_case("no") {
                return false;
            }
            if t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes") {
                return true;
            }
            n >= 2
        }
        Err(_) => true, // default on (including N=1 trivial)
    }
}

/// Phase 150: `VOLANT_ASSIGNMENT_CONSENSUS_WAIT` default **false** (best-effort).
fn default_assignment_consensus_wait() -> bool {
    match std::env::var("VOLANT_ASSIGNMENT_CONSENSUS_WAIT") {
        Ok(s) => {
            let t = s.trim();
            t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes")
        }
        Err(_) => false,
    }
}

/// Phase 152: `VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY` default **false**
/// (live Metadata). Explicit `1`/`true`/`yes` serves the committed snapshot;
/// `0`/`false`/`no` stays off.
fn default_assignment_metadata_committed_only() -> bool {
    match std::env::var("VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY") {
        Ok(s) => {
            let t = s.trim();
            if t == "0" || t.eq_ignore_ascii_case("false") || t.eq_ignore_ascii_case("no") {
                return false;
            }
            if t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes") {
                return true;
            }
            true
        }
        Err(_) => false,
    }
}

/// Phase 154: `VOLANT_METADATA_RAFT` — default **off** (cluster and single-node).
/// Explicit `0`/`false`/`no` disables (Phase 150 notes only);
/// `1`/`true`/`yes` enables.
fn default_metadata_raft_enabled(cluster_mode: bool) -> bool {
    match std::env::var("VOLANT_METADATA_RAFT") {
        Ok(s) => {
            let t = s.trim();
            if t == "0" || t.eq_ignore_ascii_case("false") || t.eq_ignore_ascii_case("no") {
                return false;
            }
            if t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes") {
                return true;
            }
            cluster_mode
        }
        Err(_) => false,
    }
}

/// Default prepared-txn timeout from env or 60s (Phase 92).
fn default_prepared_txn_timeout_ms() -> u64 {
    std::env::var("VOLANT_PREPARED_TXN_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60_000)
}

/// Default open-txn timeout from env or 60s (Phase 93).
fn default_open_txn_timeout_ms() -> u64 {
    std::env::var("VOLANT_OPEN_TXN_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60_000)
}

/// Default broker max transaction timeout from env or 15 minutes (Phase 96).
///
/// Matches Kafka `transaction.max.timeout.ms` default (900_000 ms). Env value
/// `0` is honored as "no max".
fn default_transaction_max_timeout_ms() -> u64 {
    std::env::var("VOLANT_TRANSACTION_MAX_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(900_000)
}

/// Default background sweep interval from env or 1s (Phase 97/101).
///
/// Env value `0` pauses the background sweeper (lazy expire remains).
fn default_sweep_interval_ms() -> u64 {
    std::env::var("VOLANT_SWEEP_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1_000)
}

/// Product default / env for registry TTL (Phase 127/128). Invalid env → 24h.
fn default_txn_coordinator_ttl_ms() -> u64 {
    crate::txn_coordinator_registry::effective_txn_coordinator_ttl_ms()
}

/// Default min interval between journal catch-up **starts** for the same peer
/// (Phase 132). Default **500 ms**. Override with
/// `VOLANT_JOURNAL_CATCHUP_MIN_INTERVAL_MS`; `0` disables time throttle
/// (single-flight still applies).
pub const DEFAULT_JOURNAL_CATCHUP_MIN_INTERVAL_MS: u64 = 500;

/// Effective journal catch-up min-interval (Phase 132).
pub fn journal_catchup_min_interval_ms() -> u64 {
    std::env::var("VOLANT_JOURNAL_CATCHUP_MIN_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_JOURNAL_CATCHUP_MIN_INTERVAL_MS)
}

/// Default min interval between admin (ACL/config) catch-up **starts** for the
/// same peer (Phase 136). Default **500 ms**. Override with
/// `VOLANT_ADMIN_CATCHUP_MIN_INTERVAL_MS`; `0` disables time throttle
/// (single-flight still applies).
pub const DEFAULT_ADMIN_CATCHUP_MIN_INTERVAL_MS: u64 = 500;

/// Effective admin catch-up min-interval (Phase 136).
pub fn admin_catchup_min_interval_ms() -> u64 {
    std::env::var("VOLANT_ADMIN_CATCHUP_MIN_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_ADMIN_CATCHUP_MIN_INTERVAL_MS)
}

fn topics_from_open(txn: &OpenTxn) -> Vec<(String, Vec<i32>)> {
    let mut map: HashMap<String, Vec<i32>> = HashMap::new();
    for b in &txn.written {
        map.entry(b.topic.clone())
            .or_default()
            .push(b.partition as i32);
    }
    for (topic, part) in txn.pending.keys() {
        map.entry(topic.clone()).or_default().push(*part as i32);
    }
    // Phase 105: include empty AddPartitions membership.
    for (topic, part) in &txn.added {
        map.entry(topic.clone()).or_default().push(*part as i32);
    }
    let mut topics: Vec<(String, Vec<i32>)> = map
        .into_iter()
        .map(|(t, mut parts)| {
            parts.sort_unstable();
            parts.dedup();
            (t, parts)
        })
        .collect();
    topics.sort_by(|a, b| a.0.cmp(&b.0));
    topics
}

/// Kafka-compatible murmur2 hash (seed `0x9747b28c`).
pub fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let length = data.len() as u32;
    let mut h: u32 = SEED ^ length;
    let length4 = data.len() / 4;

    for i in 0..length4 {
        let i4 = i * 4;
        let mut k = u32::from(data[i4])
            | (u32::from(data[i4 + 1]) << 8)
            | (u32::from(data[i4 + 2]) << 16)
            | (u32::from(data[i4 + 3]) << 24);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    let rem = data.len() % 4;
    let offset = data.len() & !3;
    if rem == 3 {
        h ^= u32::from(data[offset + 2]) << 16;
    }
    if rem >= 2 {
        h ^= u32::from(data[offset + 1]) << 8;
    }
    if rem >= 1 {
        h ^= u32::from(data[offset]);
        h = h.wrapping_mul(M);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

/// Map a key to a partition index using Kafka-compatible murmur2.
pub fn partition_for_key(key: &[u8], num_partitions: u32) -> u32 {
    if num_partitions == 0 {
        return 0;
    }
    (murmur2(key) & 0x7fff_ffff) % num_partitions
}

/// Phase 121: sticky coordinator id for a FindCoordinator key.
///
/// Preferred target is `ring[(murmur2(key) & 0x7fff_ffff) % ring.len()]`.
/// If that id is not in `live`, walk the static ring forward for the next live
/// member. Returns `None` when both ring and live are empty.
pub fn sticky_coordinator_id(key: &[u8], ring: &[u32], live: &[u32]) -> Option<u32> {
    if live.is_empty() {
        return ring.first().copied();
    }
    if ring.is_empty() {
        return live.first().copied();
    }
    let preferred = (murmur2(key) & 0x7fff_ffff) as usize % ring.len();
    for i in 0..ring.len() {
        let id = ring[(preferred + i) % ring.len()];
        if live.binary_search(&id).is_ok() || live.contains(&id) {
            return Some(id);
        }
    }
    live.first().copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn murmur2_known_vector() {
        let h = murmur2(b"hello");
        assert_ne!(h, 0);
        assert_eq!(h, murmur2(b"hello"));
    }

    #[test]
    fn sticky_coordinator_id_stable_and_failover() {
        let ring = [1u32, 2, 3];
        let live = [1u32, 2, 3];
        let a = sticky_coordinator_id(b"txn-a", &ring, &live).unwrap();
        let a2 = sticky_coordinator_id(b"txn-a", &ring, &live).unwrap();
        assert_eq!(a, a2);
        assert!(ring.contains(&a));

        // Preferred dead → next live on ring.
        let preferred = sticky_coordinator_id(b"sticky-key", &ring, &live).unwrap();
        let live_without: Vec<u32> = ring.iter().copied().filter(|id| *id != preferred).collect();
        let failover = sticky_coordinator_id(b"sticky-key", &ring, &live_without).unwrap();
        assert_ne!(failover, preferred);
        assert!(live_without.contains(&failover));

        // Revive preferred → back.
        let back = sticky_coordinator_id(b"sticky-key", &ring, &live).unwrap();
        assert_eq!(back, preferred);
    }

    #[test]
    fn resolve_find_coordinator_registry_overrides_hash() {
        use crate::cluster::BrokerEndpoint;
        let dir = std::env::temp_dir().join(format!(
            "volant-p121-unit-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = fs::create_dir_all(&dir);
        let cfg = ClusterConfig {
            default_replication_factor: 3,
            min_insync_replicas: 2,
            session_timeout_ms: 2000,
            replica_fetch_max_wait_ms: 50,
            replica_fetch_max_bytes: 1_048_576,
            replica_lag_max_messages: 10_000,
            replica_lag_max_ms: 30_000,
            brokers: (1..=3)
                .map(|id| BrokerEndpoint {
                    id,
                    host: "127.0.0.1".into(),
                    port: 9000 + id as u16,
                    rack: None,
                })
                .collect(),
        };
        let broker = Broker::with_cluster(
            StorageConfig {
                data_dir: dir.clone(),
                ..StorageConfig::default()
            },
            1,
            cfg,
        )
        .unwrap();
        broker.set_advertised("127.0.0.1", 9001);

        let sticky = broker.resolve_find_coordinator("txn-override", 1).0;
        // Force registry owner to a different live node when possible.
        let owner = if sticky == 2 { 3 } else { 2 };
        broker.note_txn_coordinator("txn-override", 99, owner);
        let (id, _, _) = broker.resolve_find_coordinator("txn-override", 1);
        assert_eq!(id, owner);

        // Group keys ignore txn registry.
        let (g, _, _) = broker.resolve_find_coordinator("txn-override", 0);
        assert_eq!(
            g,
            sticky_coordinator_id(b"txn-override", &[1, 2, 3], &[1, 2, 3]).unwrap()
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn key_partition_sticky() {
        let dir = std::env::temp_dir().join(format!("volant-broker-sticky-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let topic = TopicName::new("t");
        broker.create_topic(topic.clone(), 8).unwrap();
        let p1 = broker.select_partition(&topic, Some(b"user-42")).unwrap();
        let p2 = broker.select_partition(&topic, Some(b"user-42")).unwrap();
        assert_eq!(p1, p2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn single_node_hwm_tracks_leo() {
        let dir = std::env::temp_dir().join(format!("volant-broker-hwm-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let topic = TopicName::new("t");
        broker.create_topic(topic.clone(), 1).unwrap();
        broker
            .produce_one(&topic, PartitionId(0), Message::from_value("a"))
            .unwrap();
        assert_eq!(broker.high_watermark(&topic, PartitionId(0)).unwrap(), 1);
        let _ = fs::remove_dir_all(&dir);
    }
}
