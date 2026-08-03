//! Broker state machine (single-node and multi-node cluster).
//!
//! # Batch produce coalescing
//!
//! [`Broker::produce`] accepts a [`MessageBatch`] and treats the whole batch as
//! one critical section under the topics write lock.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tracing::warn;
use volant_core::{
    Error, Message, MessageBatch, Offset, PartitionId, Record, Result, TopicId, TopicName,
};
use volant_protocol::{ClusterTopicState, ErrorCode, FetchRecord};
use volant_storage::StorageConfig;

use crate::cluster::{
    assign_replicas, elect_leader, load_assignment, reconcile_isr, save_assignment, shrink_isr,
    shrink_isr_by_time, AssignmentSnapshot, ClusterConfig, Membership, PartitionAssignment,
    TopicAssignment,
};
use crate::group::GroupCoordinator;
use crate::metrics::Metrics;
use crate::producer_state::{
    partition_key, parse_partition_key, ProducerStateFile, ProducerStateStore, StoredBatch,
    StoredProducer,
};
use crate::kafka::codec::{
    txn_control_message, ControlMarkerType, is_txn_control_record,
};
use crate::kafka::fetch_session::FetchSessionManager;
use crate::leader_epoch::{
    self, end_offset_for, ensure_entry, EpochStart, LeaderEpochStore, LeaderEpochsFile,
};
use crate::broker_config::{
    self, KEY_FETCH_SESSION_IDLE_MS, KEY_FETCH_SESSION_MAX, KEY_OPEN_TXN_TIMEOUT_MS,
    KEY_PREPARED_TXN_TIMEOUT_MS, KEY_SWEEP_INTERVAL_MS, KEY_TRANSACTION_MAX_TIMEOUT_MS,
    KEY_TXN_COORDINATOR_TTL_MS,
};
use crate::cluster_admin::{ClusterAdminFile, ClusterAdminStore};
use crate::delete_records_outbox::DeleteRecordsOutbox;
use crate::txn_coordinator_registry::TxnCoordinatorRegistry;
use crate::topic::Topic;
use crate::topic_catalog::{CatalogTopic, TopicCatalogFile, TopicCatalogStore};
use crate::topic_config::{TopicConfig, TopicConfigStore};

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
    /// Phase 120/124: durable Init-owner txn coordinator registry.
    txn_coordinator_registry: TxnCoordinatorRegistry,
    /// Phase 120: successful transparent EndTxn (txn) forwards.
    txn_forward_total: AtomicU64,
    /// Phase 120: failed transparent txn forward attempts.
    txn_forward_errors_total: AtomicU64,
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
        let topic_configs = TopicConfigStore::open(&storage.data_dir)
            .expect("failed to open topic config store");
        let topic_catalog = TopicCatalogStore::open(&storage.data_dir)
            .expect("failed to open topic catalog store");
        let acls = crate::acl::AclState::open(&storage.data_dir)
            .expect("failed to open ACL store");
        let scram = crate::scram::ScramStore::open(&storage.data_dir)
            .expect("failed to open SCRAM store");
        let leader_epoch_store = LeaderEpochStore::open(&storage.data_dir)
            .expect("failed to open leader epoch store");
        // Phase 115: durable fetch sessions under data_dir/__fetch_sessions.
        let fetch_sessions = FetchSessionManager::open(&storage.data_dir);
        // Phase 116: durable DeleteRecords outbox (empty in single-node use).
        let delete_records_outbox = DeleteRecordsOutbox::open(&storage.data_dir);
        // Phase 124: durable Init-owner txn coordinator registry.
        let txn_coordinator_registry = TxnCoordinatorRegistry::open(&storage.data_dir);
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
            cluster_config_push_errors_total: AtomicU64::new(0),
            cluster_acl_push_errors_total: AtomicU64::new(0),
            txn_2pc_fanout_errors_total: AtomicU64::new(0),
            cluster_prepared_index: Mutex::new(HashMap::new()),
            delete_records_outbox,
            delete_records_outbox_last_reconcile: Mutex::new(HashMap::new()),
            delete_records_outbox_reconcile_total: AtomicU64::new(0),
            cluster_admin_catchup_success_total: AtomicU64::new(0),
            cluster_admin_catchup_errors_total: AtomicU64::new(0),
            isr_expand_total: AtomicU64::new(0),
            isr_shrink_total: AtomicU64::new(0),
            isr_time_shrink_total: AtomicU64::new(0),
            preferred_replica_redirect_total: AtomicU64::new(0),
            txn_coordinator_registry,
            txn_forward_total: AtomicU64::new(0),
            txn_forward_errors_total: AtomicU64::new(0),
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
        let topic_configs = TopicConfigStore::open(&storage.data_dir)
            .expect("failed to open topic config store");
        let topic_catalog = TopicCatalogStore::open(&storage.data_dir)
            .expect("failed to open topic catalog store");
        let acls = crate::acl::AclState::open(&storage.data_dir)
            .expect("failed to open ACL store");
        let scram = crate::scram::ScramStore::open(&storage.data_dir)
            .expect("failed to open SCRAM store");
        let leader_epoch_store = LeaderEpochStore::open(&storage.data_dir)
            .expect("failed to open leader epoch store");
        // Phase 115/119: durable fetch sessions; cluster owner-encoded session ids.
        let fetch_sessions = FetchSessionManager::open_with_owner(&storage.data_dir, node_id);
        // Phase 116: durable DeleteRecords outbox under data_dir.
        let delete_records_outbox = DeleteRecordsOutbox::open(&storage.data_dir);
        // Phase 124: durable Init-owner txn coordinator registry.
        let txn_coordinator_registry = TxnCoordinatorRegistry::open(&storage.data_dir);
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
            cluster_config_push_errors_total: AtomicU64::new(0),
            cluster_acl_push_errors_total: AtomicU64::new(0),
            txn_2pc_fanout_errors_total: AtomicU64::new(0),
            cluster_prepared_index: Mutex::new(HashMap::new()),
            delete_records_outbox,
            delete_records_outbox_last_reconcile: Mutex::new(HashMap::new()),
            delete_records_outbox_reconcile_total: AtomicU64::new(0),
            cluster_admin_catchup_success_total: AtomicU64::new(0),
            cluster_admin_catchup_errors_total: AtomicU64::new(0),
            isr_expand_total: AtomicU64::new(0),
            isr_shrink_total: AtomicU64::new(0),
            isr_time_shrink_total: AtomicU64::new(0),
            preferred_replica_redirect_total: AtomicU64::new(0),
            txn_coordinator_registry,
            txn_forward_total: AtomicU64::new(0),
            txn_forward_errors_total: AtomicU64::new(0),
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

    // --- Phase 113 cluster admin generations (fan-out behavior lands in later PRs) ---

    /// Controller (or local) BROKER-config generation.
    pub fn config_generation(&self) -> u64 {
        self.config_generation.load(Ordering::Relaxed)
    }

    /// Last applied BROKER-config generation on this node.
    pub fn applied_config_generation(&self) -> u64 {
        self.applied_config_generation.load(Ordering::Relaxed)
    }

    /// Controller (or local) ACL generation.
    pub fn acl_generation(&self) -> u64 {
        self.acl_generation.load(Ordering::Relaxed)
    }

    /// Last applied ACL generation on this node.
    pub fn applied_acl_generation(&self) -> u64 {
        self.applied_acl_generation.load(Ordering::Relaxed)
    }

    /// DeleteRecords fan-out error counter (Phase 113).
    pub fn delete_records_fanout_errors_total(&self) -> u64 {
        self.delete_records_fanout_errors_total
            .load(Ordering::Relaxed)
    }

    /// Increment DeleteRecords fan-out error counter (Phase 113).
    pub fn note_delete_records_fanout_error(&self) {
        self.delete_records_fanout_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Durable DeleteRecords outbox (Phase 116).
    pub fn delete_records_outbox(&self) -> &DeleteRecordsOutbox {
        &self.delete_records_outbox
    }

    /// Pending outbox depth (Phase 116).
    pub fn delete_records_outbox_depth(&self) -> u64 {
        self.delete_records_outbox.depth()
    }

    /// Outbox enqueue counter (Phase 116).
    pub fn delete_records_outbox_enqueued_total(&self) -> u64 {
        self.delete_records_outbox.enqueued_total()
    }

    /// Outbox retry success counter (Phase 116).
    pub fn delete_records_outbox_retry_success_total(&self) -> u64 {
        self.delete_records_outbox.retry_success_total()
    }

    /// Outbox retry error counter (Phase 116).
    pub fn delete_records_outbox_retry_errors_total(&self) -> u64 {
        self.delete_records_outbox.retry_errors_total()
    }

    /// Outbox capacity-drop counter (Phase 116).
    pub fn delete_records_outbox_drops_total(&self) -> u64 {
        self.delete_records_outbox.drops_total()
    }

    /// Enqueue a pending peer truncate after fan-out failure (Phase 116).
    pub fn enqueue_delete_records_outbox(
        &self,
        replica_id: u32,
        topic: &str,
        partition: u32,
        before_offset: u64,
        leader_epoch: i32,
    ) {
        let _ = self.delete_records_outbox.enqueue(
            replica_id,
            topic,
            partition,
            before_offset,
            leader_epoch,
        );
    }

    /// Pending outbox entries for currently live peers (Phase 116 drain).
    pub fn delete_records_outbox_pending_live(&self) -> Vec<crate::delete_records_outbox::OutboxEntry> {
        let live = self.live_brokers();
        self.delete_records_outbox.pending_for_replicas(&live)
    }

    /// Phase 123: partition reconcile passes that advanced last_reconcile.
    pub fn delete_records_outbox_reconcile_total(&self) -> u64 {
        self.delete_records_outbox_reconcile_total
            .load(Ordering::Relaxed)
    }

    /// Rebuild pending DeleteRecords outbox entries from local log starts
    /// for partitions this node leads (Phase 123 leadership handoff MVP).
    ///
    /// For each led partition with `log_start > 0`, enqueue
    /// `ReplicaDeleteRecords` targets for every assigned peer at
    /// `(before_offset = log_start, leader_epoch = current)` unless this
    /// `(epoch, log_start)` was already reconciled. Returns the number of
    /// partition passes that advanced `last_reconcile`.
    ///
    /// No-op in single-node mode. Safe to call repeatedly; peer apply is
    /// idempotent (log start only advances).
    pub fn reconcile_delete_records_outbox(&self) -> u64 {
        if self.cluster.is_none() {
            return 0;
        }
        // Collect led partitions + targets without holding the outbox lock.
        let targets: Vec<(String, u32, u64, i32, Vec<u32>)> = {
            let topics = self.topics.read();
            let mut out = Vec::new();
            for (name, t) in topics.iter() {
                for (pid, part) in &t.partitions {
                    if !part.is_leader(self.node_id) {
                        continue;
                    }
                    let log_start = part.log.log_start_offset().raw();
                    if log_start == 0 {
                        continue;
                    }
                    let epoch = part.leader_epoch;
                    let peers: Vec<u32> = part
                        .replicas
                        .iter()
                        .copied()
                        .filter(|id| *id != self.node_id && self.broker_addr(*id).is_some())
                        .collect();
                    if peers.is_empty() {
                        continue;
                    }
                    out.push((name.as_str().to_owned(), pid.0, log_start, epoch as i32, peers));
                }
            }
            out
        };

        let mut advanced = 0u64;
        let mut last = self.delete_records_outbox_last_reconcile.lock();
        for (topic, partition, log_start, epoch, peers) in targets {
            let key = (topic.clone(), partition);
            let epoch_u = epoch as u32;
            if last.get(&key) == Some(&(epoch_u, log_start)) {
                continue;
            }
            for peer in peers {
                let _ = self.delete_records_outbox.enqueue(
                    peer,
                    &topic,
                    partition,
                    log_start,
                    epoch,
                );
            }
            last.insert(key, (epoch_u, log_start));
            advanced += 1;
            self.delete_records_outbox_reconcile_total
                .fetch_add(1, Ordering::Relaxed);
        }
        advanced
    }

    /// Current leader epoch for a partition if this node leads it (Phase 123 drain).
    ///
    /// Returns `None` when the partition is unknown or this node is not leader.
    pub fn led_partition_epoch(&self, topic: &str, partition: u32) -> Option<i32> {
        let name = TopicName::new(topic);
        let topics = self.topics.read();
        let part = topics.get(&name)?.partitions.get(&PartitionId(partition))?;
        if part.is_leader(self.node_id) {
            Some(part.leader_epoch as i32)
        } else {
            None
        }
    }

    /// BROKER config push error counter (Phase 113).
    pub fn cluster_config_push_errors_total(&self) -> u64 {
        self.cluster_config_push_errors_total.load(Ordering::Relaxed)
    }

    /// ACL snapshot push error counter (Phase 113).
    pub fn cluster_acl_push_errors_total(&self) -> u64 {
        self.cluster_acl_push_errors_total.load(Ordering::Relaxed)
    }

    /// Admin catch-up success counter (Phase 117).
    pub fn cluster_admin_catchup_success_total(&self) -> u64 {
        self.cluster_admin_catchup_success_total
            .load(Ordering::Relaxed)
    }

    /// Admin catch-up error counter (Phase 117).
    pub fn cluster_admin_catchup_errors_total(&self) -> u64 {
        self.cluster_admin_catchup_errors_total
            .load(Ordering::Relaxed)
    }

    /// Increment admin catch-up success counter (Phase 117).
    pub fn note_cluster_admin_catchup_success(&self) {
        self.cluster_admin_catchup_success_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment admin catch-up error counter (Phase 117).
    pub fn note_cluster_admin_catchup_error(&self) {
        self.cluster_admin_catchup_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// ISR expand counter (Phase 118).
    pub fn isr_expand_total(&self) -> u64 {
        self.isr_expand_total.load(Ordering::Relaxed)
    }

    /// ISR shrink counter (Phase 118).
    pub fn isr_shrink_total(&self) -> u64 {
        self.isr_shrink_total.load(Ordering::Relaxed)
    }

    /// ISR time-lag shrink counter (Phase 125).
    pub fn isr_time_shrink_total(&self) -> u64 {
        self.isr_time_shrink_total.load(Ordering::Relaxed)
    }

    /// Preferred-replica redirect counter (Phase 126).
    pub fn preferred_replica_redirect_total(&self) -> u64 {
        self.preferred_replica_redirect_total.load(Ordering::Relaxed)
    }

    /// Optional rack for a configured broker (cluster.toml); `None` single-node or unset.
    pub fn broker_rack(&self, broker_id: u32) -> Option<String> {
        self.cluster
            .as_ref()?
            .config
            .broker(broker_id)
            .and_then(|b| b.rack.clone())
    }

    /// Phase 126: select a preferred read replica for consumer Fetch (KIP-392 subset).
    ///
    /// Returns a **follower** broker id in the same rack as `client_rack` that is
    /// currently in the local ISR with observed LEO ≥ HWM, when this node is the
    /// partition leader. Empty/`None` rack, single-node, non-leader, or no
    /// eligible peer → `None` (caller leaves PreferredReadReplica = -1).
    pub fn select_preferred_read_replica(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        client_rack: Option<&str>,
    ) -> Option<u32> {
        let rack = client_rack.map(str::trim).filter(|s| !s.is_empty())?;
        let cluster = self.cluster.as_ref()?;
        let topics = self.topics.read();
        let t = topics.get(topic)?;
        let part = t.partitions.get(&partition)?;
        if !part.is_leader(self.node_id) {
            return None;
        }
        let hwm = part.committed_hwm;
        let mut candidates: Vec<u32> = part
            .isr
            .iter()
            .copied()
            .filter(|id| *id != self.node_id)
            .filter(|id| {
                cluster
                    .config
                    .broker(*id)
                    .and_then(|b| b.rack.as_deref())
                    .map(|r| r == rack)
                    .unwrap_or(false)
            })
            .filter(|id| {
                // Require observed LEO ≥ HWM so the follower can serve committed data.
                part.follower_leo
                    .get(id)
                    .copied()
                    .map(|leo| leo >= hwm)
                    .unwrap_or(false)
            })
            .collect();
        if candidates.is_empty() {
            return None;
        }
        candidates.sort_unstable();
        Some(candidates[0])
    }

    pub(crate) fn note_preferred_replica_redirect(&self) {
        self.preferred_replica_redirect_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Effective `replica_lag_max_ms` with optional `VOLANT_REPLICA_LAG_MAX_MS` override.
    pub fn effective_replica_lag_max_ms(&self) -> u64 {
        if let Ok(s) = std::env::var("VOLANT_REPLICA_LAG_MAX_MS") {
            if let Ok(v) = s.parse::<u64>() {
                return v;
            }
        }
        self.cluster
            .as_ref()
            .map(|c| c.config.replica_lag_max_ms)
            .unwrap_or(0)
    }

    fn note_isr_delta(&self, before: &[u32], after: &[u32]) {
        let mut expand = 0u64;
        let mut shrink = 0u64;
        for &id in after {
            if !before.contains(&id) {
                expand += 1;
            }
        }
        for &id in before {
            if !after.contains(&id) {
                shrink += 1;
            }
        }
        if expand > 0 {
            self.isr_expand_total
                .fetch_add(expand, Ordering::Relaxed);
        }
        if shrink > 0 {
            self.isr_shrink_total
                .fetch_add(shrink, Ordering::Relaxed);
        }
    }

    fn note_isr_time_shrink(&self, n: u64) {
        if n > 0 {
            self.isr_time_shrink_total
                .fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Whether a peer's applied gens lag controller SoT (Phase 117).
    pub fn peer_admin_gens_lag(
        &self,
        peer_applied_config: u64,
        peer_applied_acl: u64,
    ) -> (bool, bool) {
        let need_config =
            self.config_generation() > 0 && peer_applied_config < self.config_generation();
        let need_acl = self.acl_generation() > 0 && peer_applied_acl < self.acl_generation();
        (need_config, need_acl)
    }

    /// Load durable admin generations from `{data_dir}/__cluster_admin` (Phase 117).
    fn load_cluster_admin_gens(&self) -> Result<()> {
        let store = ClusterAdminStore::open(&self.storage.data_dir)?;
        let file = store.load()?;
        self.config_generation
            .store(file.config_generation, Ordering::SeqCst);
        self.applied_config_generation
            .store(file.applied_config_generation, Ordering::SeqCst);
        self.acl_generation
            .store(file.acl_generation, Ordering::SeqCst);
        self.applied_acl_generation
            .store(file.applied_acl_generation, Ordering::SeqCst);
        Ok(())
    }

    /// Persist current admin generation atomics (Phase 117).
    pub fn persist_cluster_admin_gens(&self) {
        let file = ClusterAdminFile {
            version: crate::cluster_admin::CLUSTER_ADMIN_FILE_VERSION,
            config_generation: self.config_generation.load(Ordering::SeqCst),
            applied_config_generation: self.applied_config_generation.load(Ordering::SeqCst),
            acl_generation: self.acl_generation.load(Ordering::SeqCst),
            applied_acl_generation: self.applied_acl_generation.load(Ordering::SeqCst),
        };
        match ClusterAdminStore::open(&self.storage.data_dir) {
            Ok(store) => {
                if let Err(e) = store.save(&file) {
                    warn!(error = %e, "persist cluster admin generations failed");
                }
            }
            Err(e) => {
                warn!(error = %e, "open cluster admin store failed");
            }
        }
    }

    /// Peer targets for DeleteRecords fan-out: `(broker_id, addr, leader_epoch)`.
    ///
    /// Empty in single-node mode or when this node is not the partition leader /
    /// does not know the partition. Phase 113 PR2.
    pub fn delete_records_fanout_peers(
        &self,
        topic: &str,
        partition: u32,
    ) -> Vec<(u32, String, i32)> {
        if self.cluster.is_none() {
            return Vec::new();
        }
        let name = TopicName::new(topic);
        let topics = self.topics.read();
        let Some(t) = topics.get(&name) else {
            return Vec::new();
        };
        let Some(part) = t.partitions.get(&PartitionId(partition)) else {
            return Vec::new();
        };
        if !part.is_leader(self.node_id) {
            return Vec::new();
        }
        let epoch = part.leader_epoch as i32;
        let mut out = Vec::new();
        for &id in &part.replicas {
            if id == self.node_id {
                continue;
            }
            if let Some(addr) = self.broker_addr(id) {
                out.push((id, addr, epoch));
            }
        }
        out
    }

    /// Apply inter-broker `ReplicaDeleteRecords` (Phase 113 PR2).
    ///
    /// Truncates local log prefix (whole sealed segments) and runs Phase 104/111
    /// soft-marker GC/clip. Rejects stale leader epochs when `leader_epoch >= 0`
    /// and local epoch is higher ([`ErrorCode::InvalidProducerEpoch`] as fenced).
    ///
    /// Returns `(error_code, low_watermark)`.
    pub fn handle_replica_delete_records(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
        leader_epoch: i32,
    ) -> (u16, u64) {
        let name = TopicName::new(topic);
        let low = {
            let mut topics = self.topics.write();
            let Some(t) = topics.get_mut(&name) else {
                return (ErrorCode::NotFound as u16, 0);
            };
            let Some(part) = t.partitions.get_mut(&PartitionId(partition)) else {
                return (ErrorCode::NotFound as u16, 0);
            };
            // Stale leader: request epoch older than local → refuse truncate.
            if leader_epoch >= 0 {
                let req_epoch = leader_epoch as u32;
                if part.leader_epoch > req_epoch {
                    return (
                        ErrorCode::InvalidProducerEpoch as u16,
                        part.log.log_start_offset().raw(),
                    );
                }
            }
            match part.log.delete_records(Offset::new(before_offset)) {
                Ok(off) => off.raw(),
                Err(e) => {
                    warn!(
                        topic,
                        partition,
                        before_offset,
                        error = %e,
                        "replica delete_records failed"
                    );
                    return (ErrorCode::Storage as u16, part.log.log_start_offset().raw());
                }
            }
        };
        self.gc_and_persist_aborted_markers(topic, partition, low);
        (0, low)
    }

    /// Peers for BROKER config fan-out: live brokers except self (Phase 113 PR3).
    pub fn cluster_broker_config_fanout_peers(&self) -> Vec<(u32, String)> {
        let Some(c) = &self.cluster else {
            return Vec::new();
        };
        let live = c.membership.read().live_brokers();
        let mut out = Vec::new();
        for id in live {
            if id == self.node_id {
                continue;
            }
            if let Some(addr) = self.broker_addr(id) {
                out.push((id, addr));
            }
        }
        out
    }

    /// Increment BROKER config push error counter (Phase 113).
    pub fn note_cluster_config_push_error(&self) {
        self.cluster_config_push_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // --- Phase 114 multi-broker 2PC ---

    /// Multi-broker 2PC fan-out error counter (Phase 114).
    pub fn txn_2pc_fanout_errors_total(&self) -> u64 {
        self.txn_2pc_fanout_errors_total.load(Ordering::Relaxed)
    }

    /// Increment multi-broker 2PC fan-out error counter (Phase 114).
    pub fn note_txn_2pc_fanout_error(&self) {
        self.txn_2pc_fanout_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Controller cluster prepared index size (Phase 114 gauge).
    pub fn cluster_prepared_txn_count(&self) -> usize {
        self.cluster_prepared_index.lock().len()
    }

    /// Live peers for multi-broker 2PC fan-out (all live except self).
    pub fn txn_2pc_fanout_peers(&self) -> Vec<(u32, String)> {
        self.cluster_broker_config_fanout_peers()
    }

    /// Whether multi-broker 2PC fan-out applies (cluster mode).
    pub fn txn_2pc_cluster_enabled(&self) -> bool {
        self.cluster.is_some()
    }

    /// Build an open fan-out payload for a producer that just began/ensured open.
    pub fn txn_2pc_open_fanout(&self, producer_id: u64) -> Txn2pcFanout {
        if self.cluster.is_none() {
            return Txn2pcFanout::None;
        }
        let state = self.producer_state.read();
        let Some(prod) = state.get(&producer_id) else {
            return Txn2pcFanout::None;
        };
        if !prod.transactional || prod.transactional_id.is_empty() {
            return Txn2pcFanout::None;
        }
        // Self is the Init/open coordinator for this fan-out.
        self.note_txn_coordinator(&prod.transactional_id, producer_id, self.node_id);
        Txn2pcFanout::Open {
            transactional_id: prod.transactional_id.clone(),
            producer_id,
            producer_epoch: prod.epoch,
            enable_2pc: prod.enable_2pc,
            coordinator_node_id: self.node_id,
            install_open: true,
        }
    }

    /// Phase 120/124: register txn coordinator (Init owner) for forward resolution.
    ///
    /// Persists under `{data_dir}/__txn_coordinator` when the registry is durable.
    pub fn note_txn_coordinator(
        &self,
        transactional_id: &str,
        producer_id: u64,
        coordinator_node_id: u32,
    ) {
        self.txn_coordinator_registry
            .note(transactional_id, producer_id, coordinator_node_id);
    }

    /// Phase 124: durable txn coordinator registry (Init-owner map).
    pub fn txn_coordinator_registry(&self) -> &TxnCoordinatorRegistry {
        &self.txn_coordinator_registry
    }

    /// Phase 124: entries restored from disk at last open.
    pub fn txn_coordinator_registry_restored(&self) -> u64 {
        self.txn_coordinator_registry.restored()
    }

    /// Phase 124: durable registry persist failures.
    pub fn txn_coordinator_registry_persist_errors_total(&self) -> u64 {
        self.txn_coordinator_registry.persist_errors_total()
    }

    /// Phase 120: resolve txn coordinator node id for EndTxn forward.
    ///
    /// Lookup order: transactional_id map → cluster prepared index
    /// `coordinator_node_id` → producer_id map (durable registry, Phase 124).
    pub fn resolve_txn_coordinator(
        &self,
        transactional_id: &str,
        producer_id: Option<u64>,
    ) -> Option<u32> {
        if !transactional_id.is_empty() {
            if let Some(id) = self.txn_coordinator_registry.resolve_by_id(transactional_id) {
                return Some(id);
            }
            if let Some(entry) = self.cluster_prepared_index.lock().get(transactional_id) {
                if entry.coordinator_node_id != 0 {
                    return Some(entry.coordinator_node_id);
                }
            }
        }
        if let Some(pid) = producer_id {
            if let Some(id) = self.txn_coordinator_registry.resolve_by_pid(pid) {
                return Some(id);
            }
        }
        None
    }

    /// Phase 121: resolve FindCoordinator endpoint for a group or transactional key.
    ///
    /// Lookup order:
    /// 1. Single-node / no cluster → this broker's advertised address.
    /// 2. Transaction key with known Init owner (Phase 120 registry) → that owner.
    /// 3. Sticky murmur2 over sorted **configured** broker ids; skip dead members
    ///    by walking the static ring to the next live broker.
    ///
    /// `key_type`: `0` = group, `1` = transaction (same as Kafka wire).
    pub fn resolve_find_coordinator(
        &self,
        key: &str,
        key_type: i8,
    ) -> (u32, String, u16) {
        let host = self.advertised_host.read().clone();
        let port = self.advertised_port.load(Ordering::Relaxed) as u16;
        let Some(cluster) = &self.cluster else {
            return (self.node_id, host, port);
        };

        // Known transactional_id → Init-owner registry overrides sticky hash.
        if key_type == 1 && !key.is_empty() {
            if let Some(owner) = self.resolve_txn_coordinator(key, None) {
                if let Some(ep) = self.coordinator_endpoint(owner) {
                    return ep;
                }
            }
        }

        let ring = cluster.config.broker_ids();
        let live = cluster.membership.read().live_brokers();
        let chosen = sticky_coordinator_id(key.as_bytes(), &ring, &live)
            .unwrap_or(self.node_id);
        self.coordinator_endpoint(chosen)
            .unwrap_or((self.node_id, host, port))
    }

    /// Host/port for a coordinator node id (self uses advertised).
    fn coordinator_endpoint(&self, node_id: u32) -> Option<(u32, String, u16)> {
        if node_id == self.node_id {
            let host = self.advertised_host.read().clone();
            let port = self.advertised_port.load(Ordering::Relaxed) as u16;
            return Some((node_id, host, port));
        }
        let cluster = self.cluster.as_ref()?;
        let b = cluster.config.broker(node_id)?;
        Some((b.id, b.host.clone(), b.port))
    }

    /// Phase 120: Init registration fan-out (producer + coordinator, no open).
    pub fn txn_2pc_init_register_fanout(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: u16,
        enable_2pc: bool,
    ) -> Txn2pcFanout {
        if self.cluster.is_none() || transactional_id.is_empty() {
            return Txn2pcFanout::None;
        }
        self.note_txn_coordinator(transactional_id, producer_id, self.node_id);
        Txn2pcFanout::Open {
            transactional_id: transactional_id.to_owned(),
            producer_id,
            producer_epoch,
            enable_2pc,
            coordinator_node_id: self.node_id,
            install_open: false,
        }
    }

    /// Successful transparent txn forwards (Phase 120).
    pub fn txn_forward_total(&self) -> u64 {
        self.txn_forward_total.load(Ordering::Relaxed)
    }

    /// Failed transparent txn forward attempts (Phase 120).
    pub fn txn_forward_errors_total(&self) -> u64 {
        self.txn_forward_errors_total.load(Ordering::Relaxed)
    }

    /// Record a successful multi-broker txn forward (Phase 120).
    pub fn record_txn_forward_ok(&self) {
        self.txn_forward_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed multi-broker txn forward (Phase 120).
    pub fn record_txn_forward_error(&self) {
        self.txn_forward_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Apply inter-broker `TxnParticipantOpen` (Phase 114 + Phase 120).
    ///
    /// Installs producer state and optionally empty open txn so remote partition
    /// leaders can accept write-through produce. Idempotent for matching pid/epoch.
    /// Registers txn coordinator for EndTxn forward (Phase 120).
    pub fn handle_txn_participant_open(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: u16,
        enable_2pc: bool,
        coordinator_node_id: u32,
        install_open: bool,
    ) -> u16 {
        {
            let mut state = self.producer_state.write();
            if let Some(prod) = state.get_mut(&producer_id) {
                if prod.epoch > producer_epoch {
                    return ErrorCode::InvalidProducerEpoch as u16;
                }
                // Accept equal or newer epoch from coordinator fan-out.
                prod.epoch = producer_epoch;
                prod.transactional = true;
                prod.transactional_id = transactional_id.to_owned();
                if enable_2pc {
                    prod.enable_2pc = true;
                }
            } else {
                state.insert(
                    producer_id,
                    ProducerEpochState {
                        epoch: producer_epoch,
                        transactional: true,
                        transactional_id: transactional_id.to_owned(),
                        enable_2pc,
                        transaction_timeout_ms: 0,
                        partitions: HashMap::new(),
                    },
                );
            }
        }
        if !transactional_id.is_empty() {
            self.transactional_ids
                .write()
                .insert(transactional_id.to_owned(), producer_id);
        }
        // Phase 120: learn Init owner for transparent EndTxn forward.
        let coord = if coordinator_node_id != 0 {
            coordinator_node_id
        } else {
            // Legacy peers: treat the sender as unknown; keep prior mapping if any.
            0
        };
        if coord != 0 {
            self.note_txn_coordinator(transactional_id, producer_id, coord);
        }
        // Ensure open txn exists (empty) for write-through on this leader.
        if install_open {
            let already_prepared = !transactional_id.is_empty()
                && self.prepared_txns.lock().contains_key(transactional_id);
            {
                let mut open = self.open_txns.lock();
                if let Some(txn) = open.get_mut(&producer_id) {
                    // Already open — keep existing written ranges; refresh epoch.
                    txn.producer_epoch = producer_epoch;
                } else if !already_prepared {
                    open.insert(
                        producer_id,
                        OpenTxn {
                            opened_at_ms: unix_now_ms(),
                            producer_epoch,
                            ..OpenTxn::default()
                        },
                    );
                }
            }
        }
        let _ = self.persist_producer_state();
        0
    }

    /// Apply inter-broker `TxnParticipantPrepare` (Phase 114).
    ///
    /// Moves local open ranges for this pid into prepared (or no-ops if none).
    /// Controller also upserts the cluster prepared index.
    pub fn handle_txn_participant_prepare(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: u16,
        commit: bool,
    ) -> u16 {
        if transactional_id.is_empty() {
            return ErrorCode::InvalidArg as u16;
        }
        // Already prepared with matching decision → idempotent OK.
        {
            let prepared = self.prepared_txns.lock();
            if let Some(prep) = prepared.get(transactional_id) {
                if prep.producer_id != producer_id {
                    return ErrorCode::InvalidTxnState as u16;
                }
                if prep.producer_epoch != producer_epoch {
                    return ErrorCode::InvalidProducerEpoch as u16;
                }
                if prep.commit != commit {
                    return ErrorCode::InvalidTxnState as u16;
                }
                // Still ensure cluster index if we are controller.
                drop(prepared);
                self.upsert_cluster_prepared_index(
                    transactional_id,
                    producer_id,
                    producer_epoch,
                    commit,
                );
                return 0;
            }
        }

        let txn = {
            let mut open = self.open_txns.lock();
            open.remove(&producer_id)
        };
        if let Some(txn) = txn {
            if txn.producer_epoch != 0 && txn.producer_epoch != producer_epoch {
                // Epoch mismatch on open body — put back and reject.
                self.open_txns.lock().insert(producer_id, txn);
                return ErrorCode::InvalidProducerEpoch as u16;
            }
            // Validate producer epoch if known.
            {
                let state = self.producer_state.read();
                if let Some(prod) = state.get(&producer_id) {
                    if prod.epoch != producer_epoch {
                        // Put open back.
                        self.open_txns.lock().insert(producer_id, txn);
                        return ErrorCode::InvalidProducerEpoch as u16;
                    }
                }
            }
            let prep = PreparedTxn {
                transactional_id: transactional_id.to_owned(),
                producer_id,
                producer_epoch,
                commit,
                prepared_at_ms: unix_now_ms(),
                open: txn,
            };
            self.prepared_txns
                .lock()
                .insert(transactional_id.to_owned(), prep);
            self.persist_txn_markers();
            self.persist_prepared_txns();
        } else {
            // No local open ranges — still OK (empty participant). Ensure
            // producer is known for complete/fence later when needed.
            let state = self.producer_state.read();
            if let Some(prod) = state.get(&producer_id) {
                if prod.epoch != producer_epoch {
                    return ErrorCode::InvalidProducerEpoch as u16;
                }
            }
        }
        self.upsert_cluster_prepared_index(
            transactional_id,
            producer_id,
            producer_epoch,
            commit,
        );
        0
    }

    /// Apply inter-broker `TxnParticipantComplete` (Phase 114).
    ///
    /// Finalizes local prepared (or open fallback) for this txn and clears the
    /// controller cluster index entry when present.
    ///
    /// **Fence note:** `commit=false` force-aborts prepared even when the
    /// prepared decision was PrepareCommit (InitProducerId KeepPreparedTxn=false
    /// cluster fan-out). Client EndTxn decision mismatch is rejected **locally**
    /// before fan-out, so peers only see matching completes or fence aborts.
    pub fn handle_txn_participant_complete(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: u16,
        commit: bool,
    ) -> u16 {
        if !transactional_id.is_empty() {
            let prep = {
                let mut prepared = self.prepared_txns.lock();
                prepared.remove(transactional_id)
            };
            if let Some(prep) = prep {
                if prep.producer_id != producer_id {
                    // Put back — wrong identity.
                    self.prepared_txns
                        .lock()
                        .insert(transactional_id.to_owned(), prep);
                    return ErrorCode::InvalidTxnState as u16;
                }
                if prep.producer_epoch != producer_epoch {
                    self.prepared_txns
                        .lock()
                        .insert(transactional_id.to_owned(), prep);
                    return ErrorCode::InvalidProducerEpoch as u16;
                }
                if prep.commit != commit {
                    if commit {
                        // Commit complete against PrepareAbort — reject.
                        self.prepared_txns
                            .lock()
                            .insert(transactional_id.to_owned(), prep);
                        return ErrorCode::InvalidTxnState as u16;
                    }
                    // commit=false with PrepareCommit → force-abort (fence).
                    self.force_abort_prepared(prep);
                    self.clear_cluster_prepared_index(transactional_id);
                    return 0;
                }
                let _ = self.finalize_txn(
                    producer_id,
                    producer_epoch,
                    commit,
                    prep.open,
                    &[],
                );
                self.persist_prepared_txns();
                self.clear_cluster_prepared_index(transactional_id);
                return 0;
            }
        }
        // Fallback: open (non-prepared) ranges — fence abort path may hit peers
        // that never prepared.
        let txn = {
            let mut open = self.open_txns.lock();
            open.remove(&producer_id)
        };
        if let Some(txn) = txn {
            let _ = self.finalize_txn(producer_id, producer_epoch, commit, txn, &[]);
        }
        self.clear_cluster_prepared_index(transactional_id);
        0
    }

    fn cluster_prepared_index_path(&self) -> PathBuf {
        self.storage
            .data_dir
            .join("__txn_prepared")
            .join("cluster.json")
    }

    fn load_cluster_prepared_index(&self) {
        // Only meaningful on controller, but load if file exists (restart race).
        let path = self.cluster_prepared_index_path();
        let Ok(bytes) = fs::read(&path) else {
            return;
        };
        let Ok(file) = serde_json::from_slice::<ClusterPreparedFile>(&bytes) else {
            return;
        };
        let mut map = self.cluster_prepared_index.lock();
        for e in file.prepared {
            map.insert(e.transactional_id.clone(), e);
        }
    }

    fn persist_cluster_prepared_index(&self) {
        // Controllers own the durable index; non-controllers may hold a soft copy.
        let path = self.cluster_prepared_index_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let mut file = ClusterPreparedFile::default();
        {
            let map = self.cluster_prepared_index.lock();
            file.prepared = map.values().cloned().collect();
        }
        let Ok(bytes) = serde_json::to_vec_pretty(&file) else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        if fs::write(&tmp, &bytes).is_ok() {
            let _ = fs::rename(tmp, path);
        }
    }

    fn upsert_cluster_prepared_index(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: u16,
        commit: bool,
    ) {
        // Persist on controller only (SoT). Peers skip durable cluster index.
        if self.cluster.is_some() && !self.is_controller() {
            return;
        }
        let entry = ClusterPreparedEntry {
            transactional_id: transactional_id.to_owned(),
            producer_id,
            producer_epoch,
            commit,
            prepared_at_ms: unix_now_ms(),
            coordinator_node_id: self.node_id,
        };
        self.cluster_prepared_index
            .lock()
            .insert(transactional_id.to_owned(), entry);
        self.persist_cluster_prepared_index();
    }

    fn clear_cluster_prepared_index(&self, transactional_id: &str) {
        if transactional_id.is_empty() {
            return;
        }
        let removed = self.cluster_prepared_index.lock().remove(transactional_id);
        if removed.is_some() {
            self.persist_cluster_prepared_index();
        } else if self.cluster.is_some() && self.is_controller() {
            // Still rewrite so a stale file cannot resurrect the entry after
            // peers completed while controller had no local entry.
            self.persist_cluster_prepared_index();
        }
    }

    /// Roll back a just-local prepare if cluster fan-out failed (Phase 114).
    ///
    /// Moves prepared back to open when possible so the client can retry EndTxn.
    pub fn rollback_local_prepare(&self, transactional_id: &str) {
        let prep = {
            let mut prepared = self.prepared_txns.lock();
            prepared.remove(transactional_id)
        };
        if let Some(prep) = prep {
            let pid = prep.producer_id;
            let mut open = self.open_txns.lock();
            open.insert(pid, prep.open);
            drop(open);
            self.persist_prepared_txns();
            self.persist_txn_markers();
            self.clear_cluster_prepared_index(transactional_id);
        }
    }

    /// Apply inter-broker `ClusterBrokerConfig` (Phase 113 PR3).
    ///
    /// Ignores stale/equal generations (`generation <= applied`). On accept:
    /// apply knobs + sparse durable merge, then record `applied_config_generation`.
    /// Returns `(error_code, applied_generation)`.
    pub fn handle_cluster_broker_config(
        &self,
        generation: u64,
        entries: &[(String, String)],
    ) -> (u16, u64) {
        let applied = self.applied_config_generation.load(Ordering::SeqCst);
        if generation <= applied {
            return (0, applied);
        }
        if let Err(e) = self.apply_and_persist_broker_configs(entries) {
            warn!(
                generation,
                error = %e,
                "cluster broker config apply failed"
            );
            return (ErrorCode::InvalidArg as u16, applied);
        }
        self.applied_config_generation
            .store(generation, Ordering::SeqCst);
        // Mirror SoT gen so a later promote can re-push at the correct generation.
        let cur = self.config_generation.load(Ordering::SeqCst);
        if generation > cur {
            self.config_generation
                .store(generation, Ordering::SeqCst);
        }
        self.persist_cluster_admin_gens();
        (0, generation)
    }

    /// Peers for ACL snapshot fan-out: live brokers except self (Phase 113 PR4).
    pub fn cluster_acl_fanout_peers(&self) -> Vec<(u32, String)> {
        // Same membership set as BROKER config fan-out.
        self.cluster_broker_config_fanout_peers()
    }

    /// Increment ACL snapshot push error counter (Phase 113).
    pub fn note_cluster_acl_push_error(&self) {
        self.cluster_acl_push_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Apply inter-broker `ClusterAclSnapshot` (Phase 113 PR4).
    ///
    /// Ignores stale/equal generations. On accept: install snapshot + persist
    /// local `__acls`, then record `applied_acl_generation`.
    pub fn handle_cluster_acl_snapshot(
        &self,
        generation: u64,
        snapshot: &[u8],
    ) -> (u16, u64) {
        let applied = self.applied_acl_generation.load(Ordering::SeqCst);
        if generation <= applied {
            return (0, applied);
        }
        let snap = match crate::acl::AclState::decode_snapshot_bytes(snapshot) {
            Ok(s) => s,
            Err(e) => {
                warn!(generation, error = %e, "cluster acl snapshot decode failed");
                return (ErrorCode::InvalidArg as u16, applied);
            }
        };
        if let Err(e) = self.acls.install_snapshot(&snap) {
            warn!(generation, error = %e, "cluster acl snapshot install failed");
            return (ErrorCode::Storage as u16, applied);
        }
        self.applied_acl_generation
            .store(generation, Ordering::SeqCst);
        let cur = self.acl_generation.load(Ordering::SeqCst);
        if generation > cur {
            self.acl_generation.store(generation, Ordering::SeqCst);
        }
        self.persist_cluster_admin_gens();
        (0, generation)
    }

    /// Create ACL entries with cluster controller gate (Phase 113 PR4).
    ///
    /// Returns `Some(generation)` for fan-out when running in cluster mode.
    pub fn create_acls_admin(
        &self,
        entries: Vec<crate::acl::AclEntry>,
    ) -> Result<Option<u64>> {
        if self.cluster.is_some() && !self.is_controller() {
            return Err(Error::InvalidArgument("not controller".into()));
        }
        self.acls.create(entries)?;
        Ok(self.bump_acl_generation_if_cluster())
    }

    /// Delete ACL entries with cluster controller gate (Phase 113 PR4).
    ///
    /// Returns `(removed_count, optional generation for fan-out)`.
    pub fn delete_acls_admin(
        &self,
        entries: &[crate::acl::AclEntry],
    ) -> Result<(usize, Option<u64>)> {
        if self.cluster.is_some() && !self.is_controller() {
            return Err(Error::InvalidArgument("not controller".into()));
        }
        let n = self.acls.delete(entries)?;
        // Only bump generation when something changed (or always for consistency
        // of "mutate happened"? Always bump so empty delete still is controller-
        // only with no-op fan-out of same snapshot — skip bump when n==0).
        let gen = if n > 0 {
            self.bump_acl_generation_if_cluster()
        } else {
            None
        };
        Ok((n, gen))
    }

    fn bump_acl_generation_if_cluster(&self) -> Option<u64> {
        if self.cluster.is_none() {
            return None;
        }
        let gen = self.acl_generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.applied_acl_generation
            .store(gen, Ordering::SeqCst);
        self.persist_cluster_admin_gens();
        Some(gen)
    }

    /// JSON snapshot bytes for inter-broker ACL push (Phase 113).
    pub fn acl_snapshot_wire_bytes(&self) -> Result<bytes::Bytes> {
        let v = self.acls.encode_snapshot_bytes()?;
        Ok(bytes::Bytes::from(v))
    }

    /// Current fetch-session idle TTL in milliseconds (Phase 95). `0` disables.
    pub fn fetch_session_idle_ms(&self) -> u64 {
        self.fetch_sessions.idle_timeout_ms()
    }

    /// Override fetch-session idle TTL (Phase 95). `0` disables idle eviction.
    pub fn set_fetch_session_idle_ms(&self, ms: u64) {
        self.fetch_sessions.set_idle_timeout_ms(ms);
    }

    /// Current max concurrent fetch sessions (Phase 95). `0` = unlimited.
    pub fn fetch_session_max(&self) -> usize {
        self.fetch_sessions.max_sessions()
    }

    /// Override max concurrent fetch sessions (Phase 95). `0` = unlimited.
    pub fn set_fetch_session_max(&self, max: usize) {
        self.fetch_sessions.set_max_sessions(max);
    }

    /// Shared metrics registry.
    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// Configure shared-token auth. `None` disables the auth gate.
    pub fn set_auth_token(&self, token: Option<String>) {
        *self.auth_token.write() = token;
    }

    /// Current auth token if configured.
    pub fn auth_token(&self) -> Option<String> {
        self.auth_token.read().clone()
    }

    /// Phase 20 ACL state.
    pub fn acls(&self) -> &crate::acl::AclState {
        &self.acls
    }

    /// Configure ACL enforcement (server startup).
    pub fn configure_acls(
        &self,
        enable: bool,
        file: Option<&std::path::Path>,
        super_users: Vec<String>,
        auth_principal: String,
    ) -> Result<()> {
        self.acls
            .configure(enable, file, super_users, auth_principal)
    }

    /// Principal name applied after successful shared-token Auth.
    pub fn auth_principal_name(&self) -> String {
        self.acls.auth_principal()
    }

    /// SCRAM-SHA-256 user store (Phase 22).
    pub fn scram(&self) -> &crate::scram::ScramStore {
        &self.scram
    }

    /// Whether connections must authenticate (token, SCRAM users, or caller mTLS).
    ///
    /// Callers with mTLS should OR this with their mTLS-enabled flag.
    pub fn auth_required(&self) -> bool {
        self.auth_token().is_some() || self.scram.has_users()
    }

    /// Upsert a SCRAM user at startup (`--scram-user user:pass`).
    pub fn upsert_scram_user(&self, username: &str, password: &str) -> Result<()> {
        self.scram.upsert_user(username, password, 0)
    }

    /// Configure metrics HTTP shared token (Phase 21). `None` = open scrape.
    pub fn set_metrics_token(&self, token: Option<String>) {
        *self.metrics_token.write() = token;
    }

    /// Current metrics token if configured.
    pub fn metrics_token(&self) -> Option<String> {
        self.metrics_token.read().clone()
    }

    /// Configure inter-broker TLS. `None` keeps inter-broker plaintext.
    pub fn set_inter_broker_tls(&self, config: Option<InterBrokerTls>) {
        *self.inter_broker_tls.write() = config;
    }

    /// Current inter-broker TLS settings, if enabled.
    pub fn inter_broker_tls(&self) -> Option<InterBrokerTls> {
        self.inter_broker_tls.read().clone()
    }

    /// Allocate a producer id + epoch for idempotent produce (Phase 10/11).
    ///
    /// State is persisted under `data_dir/__producer_state` (Phase 11).
    pub fn init_producer_id(&self) -> (u64, u16) {
        let r = self.init_producer_id_with_opts("", false, false, 0);
        (r.producer_id, r.epoch)
    }

    /// Allocate (or fence) a producer id, optionally transactional (Phase 18).
    ///
    /// Non-empty `transactional_id` fences any prior owner of that id by bumping
    /// epoch and clearing open transactions / sequences. Does not enable 2PC
    /// (use [`Self::init_producer_id_with_opts`] for InitProducerId v6).
    /// Uses broker-default open-txn timeout (Phase 93).
    pub fn init_producer_id_with_txn(&self, transactional_id: &str) -> (u64, u16) {
        let r = self.init_producer_id_with_opts(transactional_id, false, false, 0);
        (r.producer_id, r.epoch)
    }

    /// Current prepared-txn timeout in milliseconds (Phase 92).
    ///
    /// `0` means auto-abort is disabled.
    pub fn prepared_txn_timeout_ms(&self) -> u64 {
        self.prepared_txn_timeout_ms.load(Ordering::Relaxed)
    }

    /// Override prepared-txn timeout (Phase 92). `0` disables auto-abort.
    pub fn set_prepared_txn_timeout_ms(&self, timeout_ms: u64) {
        self.prepared_txn_timeout_ms
            .store(timeout_ms, Ordering::Relaxed);
    }

    /// Current broker-default open-txn timeout in milliseconds (Phase 93).
    ///
    /// `0` means open auto-abort is disabled for producers without a positive
    /// client `transaction_timeout_ms`.
    pub fn open_txn_timeout_ms(&self) -> u64 {
        self.open_txn_timeout_ms.load(Ordering::Relaxed)
    }

    /// Override broker-default open-txn timeout (Phase 93). `0` disables when
    /// used as the effective timeout.
    pub fn set_open_txn_timeout_ms(&self, timeout_ms: u64) {
        self.open_txn_timeout_ms
            .store(timeout_ms, Ordering::Relaxed);
    }

    /// Current broker max transaction timeout in milliseconds (Phase 96).
    ///
    /// `0` means no max (clamp + InitProducerId over-max reject disabled).
    pub fn transaction_max_timeout_ms(&self) -> u64 {
        self.transaction_max_timeout_ms.load(Ordering::Relaxed)
    }

    /// Override broker max transaction timeout (Phase 96). `0` disables the max.
    pub fn set_transaction_max_timeout_ms(&self, timeout_ms: u64) {
        self.transaction_max_timeout_ms
            .store(timeout_ms, Ordering::Relaxed);
    }

    /// Background sweep interval in milliseconds (Phase 97/101/106).
    ///
    /// `0` pauses the background sweeper (lazy expire remains). The task is
    /// always spawned from [`crate::net::start_background_tasks`] so a later
    /// `0 → >0` transition takes effect without process restart. Shutdown via
    /// [`crate::BackgroundTasks::shutdown`] stops the loop cleanly.
    pub fn sweep_interval_ms(&self) -> u64 {
        self.sweep_interval_ms.load(Ordering::Relaxed)
    }

    /// Override background sweep interval (Phase 97/101/106). `0` pauses
    /// background work; `>0` enables/resumes on the next poll cycle without
    /// restart (until [`crate::BackgroundTasks::shutdown`]).
    pub fn set_sweep_interval_ms(&self, interval_ms: u64) {
        self.sweep_interval_ms.store(interval_ms, Ordering::Relaxed);
    }

    /// Init-owner registry TTL in ms (Phase 127/128). `0` disables GC.
    pub fn txn_coordinator_ttl_ms(&self) -> u64 {
        self.txn_coordinator_ttl_ms.load(Ordering::Relaxed)
    }

    /// Override Init-owner registry TTL (Phase 128 BROKER config / tests).
    pub fn set_txn_coordinator_ttl_ms(&self, ttl_ms: u64) {
        self.txn_coordinator_ttl_ms.store(ttl_ms, Ordering::Relaxed);
    }

    /// Current broker-level config entries for Kafka DescribeConfigs BROKER
    /// (Phase 99–102). Values are live knobs (product → env → sparse durable →
    /// setters/alter).
    pub fn describe_broker_configs(&self) -> Vec<(String, String)> {
        vec![
            (
                KEY_TRANSACTION_MAX_TIMEOUT_MS.into(),
                self.transaction_max_timeout_ms().to_string(),
            ),
            (
                KEY_OPEN_TXN_TIMEOUT_MS.into(),
                self.open_txn_timeout_ms().to_string(),
            ),
            (
                KEY_PREPARED_TXN_TIMEOUT_MS.into(),
                self.prepared_txn_timeout_ms().to_string(),
            ),
            (
                KEY_FETCH_SESSION_IDLE_MS.into(),
                self.fetch_session_idle_ms().to_string(),
            ),
            (
                KEY_FETCH_SESSION_MAX.into(),
                self.fetch_session_max().to_string(),
            ),
            (
                KEY_SWEEP_INTERVAL_MS.into(),
                self.sweep_interval_ms().to_string(),
            ),
            (
                KEY_TXN_COORDINATOR_TTL_MS.into(),
                self.txn_coordinator_ttl_ms().to_string(),
            ),
        ]
    }

    /// Apply broker-level config updates (Phase 99 Alter / IncrementalAlter).
    ///
    /// Empty value restores the **product** default for that key live (not env).
    /// Unknown keys → [`Error::InvalidArgument`].
    ///
    /// Phase 100–102: on success, merges a **sparse** durable overlay under
    /// `{data_dir}/__broker_config/state.json` — only keys present in `entries`
    /// are written (SET) or removed (DELETE/empty). Keys never altered are not
    /// frozen, so env still applies for them on restart. Direct `set_*` setters
    /// remain process-local only.
    ///
    /// Phase 113: in cluster mode only the **controller** may alter; others get
    /// [`Error::InvalidArgument`] `"not controller"`. On controller success,
    /// returns `Some(generation)` for inter-broker fan-out; single-node returns
    /// `None`.
    pub fn alter_broker_configs(&self, entries: &[(String, String)]) -> Result<Option<u64>> {
        if self.cluster.is_some() && !self.is_controller() {
            return Err(Error::InvalidArgument("not controller".into()));
        }
        self.apply_and_persist_broker_configs(entries)?;
        if self.cluster.is_some() {
            let gen = self.config_generation.fetch_add(1, Ordering::SeqCst) + 1;
            self.applied_config_generation
                .store(gen, Ordering::SeqCst);
            self.persist_cluster_admin_gens();
            Ok(Some(gen))
        } else {
            Ok(None)
        }
    }

    /// Apply + sparse-persist BROKER knobs without controller / generation gates.
    fn apply_and_persist_broker_configs(&self, entries: &[(String, String)]) -> Result<()> {
        broker_config::validate_entries(entries)?;
        for (k, v) in entries {
            let val = broker_config::resolve_value(k, v)?;
            self.apply_broker_config_value(k, val)?;
        }
        self.persist_broker_config_sparse(entries)
    }

    /// Apply a single known broker config key (no persist).
    fn apply_broker_config_value(&self, key: &str, val: u64) -> Result<()> {
        match key {
            KEY_TRANSACTION_MAX_TIMEOUT_MS => self.set_transaction_max_timeout_ms(val),
            KEY_OPEN_TXN_TIMEOUT_MS => self.set_open_txn_timeout_ms(val),
            KEY_PREPARED_TXN_TIMEOUT_MS => self.set_prepared_txn_timeout_ms(val),
            KEY_FETCH_SESSION_IDLE_MS => self.set_fetch_session_idle_ms(val),
            KEY_FETCH_SESSION_MAX => {
                // Cap absurd values to usize::MAX on 32-bit; normal paths fit.
                let max = usize::try_from(val).unwrap_or(usize::MAX);
                self.set_fetch_session_max(max);
            }
            KEY_SWEEP_INTERVAL_MS => self.set_sweep_interval_ms(val),
            KEY_TXN_COORDINATOR_TTL_MS => self.set_txn_coordinator_ttl_ms(val),
            _ => {
                return Err(Error::InvalidArgument(format!(
                    "unknown broker config key: {key}"
                )));
            }
        }
        Ok(())
    }

    /// Load sparse durable BROKER knobs from `{data_dir}/__broker_config/state.json`
    /// (Phase 100–102). Applied **after** product default + env at construction;
    /// only keys present in the file override.
    fn load_durable_broker_config(&self) -> Result<()> {
        let store = broker_config::BrokerConfigStore::open(&self.storage.data_dir)?;
        let Some(file) = store.load()? else {
            return Ok(());
        };
        // Apply known keys only; ignore unknown for forward compatibility.
        for key in broker_config::BROKER_CONFIG_KEYS {
            if let Some(val) = file.configs.get(*key) {
                self.apply_broker_config_value(key, *val)?;
            }
        }
        Ok(())
    }

    /// Merge sparse durable overlay for the altered entries (Phase 102).
    ///
    /// SET writes/updates only those keys; DELETE/empty removes them. Empty
    /// overlay removes the file so env can re-apply on next restart.
    fn persist_broker_config_sparse(&self, entries: &[(String, String)]) -> Result<()> {
        let store = broker_config::BrokerConfigStore::open(&self.storage.data_dir)?;
        store.merge_alter(entries)
    }

    /// Live open (non-prepared) transaction count (Phase 97 gauge).
    pub fn open_txn_count(&self) -> usize {
        self.open_txns.lock().len()
    }

    /// Live prepared transaction count (Phase 97 gauge).
    pub fn prepared_txn_count(&self) -> usize {
        self.prepared_txns.lock().len()
    }

    /// Total open txns auto-aborted by timeout (lazy + background; Phase 97).
    pub fn open_txns_expired_total(&self) -> u64 {
        self.open_txns_expired_total.load(Ordering::Relaxed)
    }

    /// Total prepared txns auto-aborted by timeout (lazy + background; Phase 97).
    pub fn prepared_txns_expired_total(&self) -> u64 {
        self.prepared_txns_expired_total.load(Ordering::Relaxed)
    }

    /// Soft abort markers fully dropped because their range was entirely below
    /// log start after DeleteRecords / retention / load GC (Phase 104).
    ///
    /// Phase 111 straddling clips do **not** increment this counter.
    pub fn aborted_markers_gc_total(&self) -> u64 {
        self.aborted_markers_gc_total.load(Ordering::Relaxed)
    }

    /// Count of soft abort markers currently held for a partition (Phase 104 tests).
    pub fn aborted_marker_count(&self, topic: &str, partition: u32) -> usize {
        let aborted = self.aborted_txns.lock();
        aborted
            .get(&(topic.to_owned(), partition))
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Soft abort marker ranges for a partition (Phase 111 tests).
    ///
    /// Returns `(producer_id, first_offset, end_offset)` sorted by first_offset.
    pub fn aborted_marker_ranges(
        &self,
        topic: &str,
        partition: u32,
    ) -> Vec<(u64, u64, u64)> {
        let aborted = self.aborted_txns.lock();
        let Some(list) = aborted.get(&(topic.to_owned(), partition)) else {
            return Vec::new();
        };
        let mut out: Vec<(u64, u64, u64)> = list
            .iter()
            .map(|m| (m.producer_id, m.first_offset, m.end_offset))
            .collect();
        out.sort_by_key(|e| e.1);
        out
    }

    /// Run one open/prepared timeout expiry + idle fetch-session eviction
    /// (Phase 97).
    ///
    /// Used by the background sweeper and tests. Lazy API paths still call
    /// [`Self::expire_timed_out_txns`] independently. Returns
    /// `(open_aborted, prepared_aborted, sessions_idle_evicted)`.
    ///
    /// Phase 127: also runs txn-coordinator registry TTL GC (count not returned
    /// here; see [`Self::expire_txn_coordinator_registry`] / metrics).
    pub fn sweep_timeouts(&self) -> (usize, usize, usize) {
        let (open_n, prep_n) = self.expire_timed_out_txns();
        let idle_n = self.fetch_sessions.evict_idle_now();
        let _ = self.expire_txn_coordinator_registry();
        (open_n, prep_n, idle_n)
    }

    /// Phase 127/128: drop stale Init-owner registry entries older than
    /// [`Self::txn_coordinator_ttl_ms`] (live knob: env → durable → Alter).
    ///
    /// Returns number of map entries removed (id + pid counted separately).
    /// `0` TTL disables GC.
    pub fn expire_txn_coordinator_registry(&self) -> usize {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ttl = self.txn_coordinator_ttl_ms();
        if ttl == 0 {
            return 0;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.txn_coordinator_registry.expire_stale(ttl, now)
    }

    /// Phase 127: cumulative registry GC removals.
    pub fn txn_coordinator_registry_gc_total(&self) -> u64 {
        self.txn_coordinator_registry.gc_total()
    }

    /// InitProducerId with Phase 90 2PC options (Enable2Pc / KeepPreparedTxn)
    /// and Phase 93 open-txn timeout.
    ///
    /// When a prepared txn exists for `transactional_id`:
    /// - `keep_prepared=true`: preserve it, return OngoingTxn* = prepared pid/epoch,
    ///   and **do not** fence (same producer identity).
    /// - `keep_prepared=false`: force-abort prepared, then fence as usual.
    ///
    /// `enable_2pc=true` marks the producer so the first EndTxn prepares rather
    /// than one-shot finalizing.
    ///
    /// `transaction_timeout_ms` (Phase 93): when **> 0**, stored as the
    /// producer open-txn timeout; when **≤ 0**, the producer uses the broker
    /// default (`open_txn_timeout_ms` / `VOLANT_OPEN_TXN_TIMEOUT_MS`).
    ///
    /// Phase 96: when broker max > 0 and client timeout **exceeds** max,
    /// returns `error_code = 50` (`INVALID_TRANSACTION_TIMEOUT`) without
    /// mutating producer state (Kafka-honest reject).
    ///
    /// Phase 92/93: timed-out prepared and open txns are auto-aborted before
    /// KeepPrepared / fence handling.
    pub fn init_producer_id_with_opts(
        &self,
        transactional_id: &str,
        enable_2pc: bool,
        keep_prepared: bool,
        transaction_timeout_ms: i32,
    ) -> InitProducerIdResult {
        self.expire_timed_out_txns();
        let client_timeout = if transaction_timeout_ms > 0 {
            transaction_timeout_ms as u64
        } else {
            0
        };
        // Phase 96: Kafka-honest reject when client timeout exceeds broker max.
        let max = self.transaction_max_timeout_ms.load(Ordering::Relaxed);
        if max > 0 && client_timeout > max {
            return InitProducerIdResult {
                error_code: 50, // INVALID_TRANSACTION_TIMEOUT
                producer_id: 0,
                epoch: 0,
                ongoing_txn_producer_id: -1,
                ongoing_txn_producer_epoch: -1,
            };
        }
        if transactional_id.is_empty() {
            let id = self.next_producer_id.fetch_add(1, Ordering::Relaxed);
            let epoch = 0u16;
            self.producer_state.write().insert(
                id,
                ProducerEpochState {
                    epoch,
                    transactional: false,
                    transactional_id: String::new(),
                    enable_2pc: false,
                    transaction_timeout_ms: client_timeout,
                    partitions: HashMap::new(),
                },
            );
            let _ = self.persist_producer_state();
            return InitProducerIdResult {
                error_code: 0,
                producer_id: id,
                epoch,
                ongoing_txn_producer_id: -1,
                ongoing_txn_producer_epoch: -1,
            };
        }

        // Prepared path: KeepPreparedTxn reuses identity without fencing.
        if keep_prepared {
            let prepared = self.prepared_txns.lock();
            if let Some(prep) = prepared.get(transactional_id) {
                let pid = prep.producer_id;
                let epoch = prep.producer_epoch;
                let ongoing_pid = prep.producer_id as i64;
                let ongoing_epoch = prep.producer_epoch as i16;
                drop(prepared);
                // Ensure producer state reflects enable_2pc + identity + timeout.
                {
                    let mut state = self.producer_state.write();
                    if let Some(prod) = state.get_mut(&pid) {
                        prod.transactional = true;
                        prod.transactional_id = transactional_id.to_owned();
                        prod.epoch = epoch;
                        if enable_2pc {
                            prod.enable_2pc = true;
                        }
                        prod.transaction_timeout_ms = client_timeout;
                    } else {
                        state.insert(
                            pid,
                            ProducerEpochState {
                                epoch,
                                transactional: true,
                                transactional_id: transactional_id.to_owned(),
                                enable_2pc,
                                transaction_timeout_ms: client_timeout,
                                partitions: HashMap::new(),
                            },
                        );
                    }
                }
                self.transactional_ids
                    .write()
                    .insert(transactional_id.to_owned(), pid);
                // Open non-prepared txn for this pid is still fenced/aborted.
                let fenced = self.open_txns.lock().remove(&pid);
                if let Some(txn) = fenced {
                    self.record_aborted_from_txn(pid, &txn);
                    self.append_txn_control_markers(
                        pid,
                        epoch,
                        ControlMarkerType::Abort,
                        &txn,
                    );
                }
                // Phase 94: fence / KeepPrepared clears abortable (new client epoch path).
                self.clear_txn_abortable(pid);
                self.note_txn_coordinator(transactional_id, pid, self.node_id);
                let _ = self.persist_producer_state();
                return InitProducerIdResult {
                    error_code: 0,
                    producer_id: pid,
                    epoch,
                    ongoing_txn_producer_id: ongoing_pid,
                    ongoing_txn_producer_epoch: ongoing_epoch,
                };
            }
        } else {
            // Drop prepared (force abort) before normal fence/allocate.
            // Release the prepared_txns lock before force_abort (it re-locks to persist).
            let dropped = self.prepared_txns.lock().remove(transactional_id);
            if let Some(prep) = dropped {
                let pid = prep.producer_id;
                self.force_abort_prepared(prep);
                // Intentional force-abort is not a timeout abortable signal.
                self.clear_txn_abortable(pid);
            }
        }

        let mut txn_ids = self.transactional_ids.write();
        if let Some(&existing) = txn_ids.get(transactional_id) {
            let mut state = self.producer_state.write();
            if let Some(prod) = state.get_mut(&existing) {
                let old_epoch = prod.epoch;
                prod.epoch = prod.epoch.wrapping_add(1);
                if prod.epoch == 0 {
                    prod.epoch = 1;
                }
                prod.partitions.clear();
                prod.transactional = true;
                prod.transactional_id = transactional_id.to_owned();
                if enable_2pc {
                    prod.enable_2pc = true;
                }
                prod.transaction_timeout_ms = client_timeout;
                // Keep enable_2pc sticky if already set, unless caller is not
                // using v6 — still allow sticky true from prior Init.
                let epoch = prod.epoch;
                drop(state);
                // Fence: open write-through ranges become aborted (Phase 86).
                let fenced = self.open_txns.lock().remove(&existing);
                if let Some(txn) = fenced {
                    self.record_aborted_from_txn(existing, &txn);
                    self.append_txn_control_markers(
                        existing,
                        old_epoch,
                        ControlMarkerType::Abort,
                        &txn,
                    );
                }
                // Phase 94: epoch fence clears abortable for the new identity.
                self.clear_txn_abortable(existing);
                // Phase 120: this broker remains/becomes Init owner for the txn id.
                self.note_txn_coordinator(transactional_id, existing, self.node_id);
                let _ = self.persist_producer_state();
                return InitProducerIdResult {
                    error_code: 0,
                    producer_id: existing,
                    epoch,
                    ongoing_txn_producer_id: -1,
                    ongoing_txn_producer_epoch: -1,
                };
            }
        }
        // Allocate new PID for this transactional id.
        let id = self.next_producer_id.fetch_add(1, Ordering::Relaxed);
        let epoch = 0u16;
        self.producer_state.write().insert(
            id,
            ProducerEpochState {
                epoch,
                transactional: true,
                transactional_id: transactional_id.to_owned(),
                enable_2pc,
                transaction_timeout_ms: client_timeout,
                partitions: HashMap::new(),
            },
        );
        txn_ids.insert(transactional_id.to_owned(), id);
        drop(txn_ids);
        // Phase 120: this broker is the Init owner / txn coordinator.
        self.note_txn_coordinator(transactional_id, id, self.node_id);
        let _ = self.persist_producer_state();
        InitProducerIdResult {
            error_code: 0,
            producer_id: id,
            epoch,
            ongoing_txn_producer_id: -1,
            ongoing_txn_producer_epoch: -1,
        }
    }

    /// Begin a transaction for a transactional producer (Phase 18).
    ///
    /// Returns protocol error code (`0` = ok). Rejects when a prepared txn
    /// exists for this producer (Phase 90). Sets `opened_at_ms` (Phase 93).
    /// Phase 94: producers in the abortable set must EndTxn first.
    pub fn begin_txn(&self, producer_id: u64, producer_epoch: u16) -> u16 {
        self.expire_timed_out_txns();
        let state = self.producer_state.read();
        let Some(prod) = state.get(&producer_id) else {
            return ErrorCode::UnknownProducerId as u16;
        };
        if prod.epoch != producer_epoch {
            return ErrorCode::InvalidProducerEpoch as u16;
        }
        if !prod.transactional {
            return ErrorCode::InvalidTxnState as u16;
        }
        let txn_id = prod.transactional_id.clone();
        drop(state);
        if self.is_txn_abortable(producer_id) {
            return ErrorCode::TransactionAbortable as u16;
        }
        if !txn_id.is_empty() && self.prepared_txns.lock().contains_key(&txn_id) {
            return ErrorCode::InvalidTxnState as u16;
        }
        let mut open = self.open_txns.lock();
        if open.contains_key(&producer_id) {
            return ErrorCode::InvalidTxnState as u16;
        }
        open.insert(
            producer_id,
            OpenTxn {
                opened_at_ms: unix_now_ms(),
                producer_epoch,
                ..OpenTxn::default()
            },
        );
        0
    }

    /// Ensure a transaction is open (Phase 31 / Kafka AddPartitionsToTxn).
    ///
    /// If one is already open for this PID+epoch, returns success. Otherwise
    /// begins a new transaction (Kafka has no separate BeginTxn API).
    /// Phase 93: times out aged open txns first.
    /// Phase 94: if the open was just timed out (abortable set), returns
    /// [`ErrorCode::TransactionAbortable`] instead of silently opening a new
    /// txn — client must EndTxn first (AddOffsets/AddPartitions emit 123).
    pub fn ensure_txn_open(&self, producer_id: u64, producer_epoch: u16) -> u16 {
        // begin_txn also expires; call once here for the prepared/open check path.
        self.expire_timed_out_txns();
        let txn_id = {
            let state = self.producer_state.read();
            let Some(prod) = state.get(&producer_id) else {
                return ErrorCode::UnknownProducerId as u16;
            };
            if prod.epoch != producer_epoch {
                return ErrorCode::InvalidProducerEpoch as u16;
            }
            if !prod.transactional {
                return ErrorCode::InvalidTxnState as u16;
            }
            prod.transactional_id.clone()
        };
        if self.is_txn_abortable(producer_id) {
            return ErrorCode::TransactionAbortable as u16;
        }
        if !txn_id.is_empty() && self.prepared_txns.lock().contains_key(&txn_id) {
            return ErrorCode::InvalidTxnState as u16;
        }
        if self.has_open_txn(producer_id) {
            return 0;
        }
        self.begin_txn(producer_id, producer_epoch)
    }

    /// Record partitions successfully added via AddPartitionsToTxn (Phase 105).
    ///
    /// Membership is tracked even when no produce follows, so EndTxn and
    /// crash≡abort can append Kafka control batches for those partitions.
    /// Soft abort markers are **not** created for empty (no write-through)
    /// partitions. Idempotent: re-adding the same (topic, partition) is a no-op.
    ///
    /// Returns protocol error code (`0` = ok). Caller must already have opened
    /// the txn via [`Self::ensure_txn_open`] / [`Self::begin_txn`].
    pub fn record_txn_added_partitions(
        &self,
        producer_id: u64,
        partitions: &[(String, u32)],
    ) -> u16 {
        if partitions.is_empty() {
            return 0;
        }
        {
            let mut open = self.open_txns.lock();
            let Some(txn) = open.get_mut(&producer_id) else {
                return ErrorCode::InvalidTxnState as u16;
            };
            for (topic, part) in partitions {
                let key = (topic.clone(), *part);
                if !txn.added.iter().any(|(t, p)| t == topic && p == part) {
                    txn.added.push(key);
                }
            }
        }
        self.persist_txn_markers();
        0
    }

    /// Whether this producer currently has an open (non-prepared) transaction.
    pub fn has_open_txn(&self, producer_id: u64) -> bool {
        self.open_txns.lock().contains_key(&producer_id)
    }

    /// List open + prepared transactions for ListTransactions (Phase 65/90).
    ///
    /// State is `"Ongoing"`, `"PrepareCommit"`, or `"PrepareAbort"`.
    /// Phase 92/93: timed-out prepared and open entries are auto-aborted first.
    pub fn list_open_transactions(&self) -> Vec<(String, u64, String)> {
        self.expire_timed_out_txns();
        let open = self.open_txns.lock();
        let prepared = self.prepared_txns.lock();
        let prods = self.producer_state.read();
        let mut out = Vec::with_capacity(open.len() + prepared.len());
        for &pid in open.keys() {
            let Some(prod) = prods.get(&pid) else {
                continue;
            };
            if prod.transactional_id.is_empty() {
                continue;
            }
            out.push((prod.transactional_id.clone(), pid, "Ongoing".to_string()));
        }
        for prep in prepared.values() {
            let state = if prep.commit {
                "PrepareCommit"
            } else {
                "PrepareAbort"
            };
            out.push((
                prep.transactional_id.clone(),
                prep.producer_id,
                state.to_string(),
            ));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Describe one transactional id for DescribeTransactions (Phase 66/90/92/93).
    ///
    /// Returns `None` when the transactional id is unknown. When known:
    /// - `"PrepareCommit"` / `"PrepareAbort"` if prepared (Phase 90)
    /// - `"Ongoing"` if an open txn exists
    /// - else `"Empty"`
    /// - topics/partitions from write-through ranges + pending keys
    /// - prepared: timeout = configured prepared timeout; start = `prepared_at_ms`
    /// - open: timeout = effective open timeout; start = `opened_at_ms` (Phase 93)
    /// - empty: timeout/start remain `0`
    ///
    /// Phase 92/93: timed-out prepared and open entries are auto-aborted first.
    pub fn describe_transaction(
        &self,
        transactional_id: &str,
    ) -> Option<(
        String,                 // state
        i32,                    // timeout_ms
        i64,                    // start_time_ms
        u64,                    // producer_id
        u16,                    // producer_epoch
        Vec<(String, Vec<i32>)>, // topics → partitions
    )> {
        self.expire_timed_out_txns();
        let txn_ids = self.transactional_ids.read();
        let Some(&pid) = txn_ids.get(transactional_id) else {
            return None;
        };
        drop(txn_ids);
        let prods = self.producer_state.read();
        let Some(prod) = prods.get(&pid) else {
            return None;
        };
        let epoch = prod.epoch;
        let open_timeout = self.effective_open_txn_timeout_ms(prod);
        drop(prods);

        // Prepared takes precedence over open (they should be mutually exclusive).
        {
            let prepared = self.prepared_txns.lock();
            if let Some(prep) = prepared.get(transactional_id) {
                let state = if prep.commit {
                    "PrepareCommit"
                } else {
                    "PrepareAbort"
                };
                let topics = topics_from_open(&prep.open);
                // Phase 96: report effective (clamped) prepared timeout.
                let timeout_ms = self.effective_prepared_txn_timeout_ms() as i32;
                return Some((
                    state.to_string(),
                    timeout_ms,
                    prep.prepared_at_ms,
                    prep.producer_id,
                    prep.producer_epoch,
                    topics,
                ));
            }
        }

        let open = self.open_txns.lock();
        if let Some(txn) = open.get(&pid) {
            let topics = topics_from_open(txn);
            Some((
                "Ongoing".to_string(),
                open_timeout as i32,
                txn.opened_at_ms,
                pid,
                epoch,
                topics,
            ))
        } else {
            Some(("Empty".to_string(), 0, 0, pid, epoch, Vec::new()))
        }
    }

    /// Active producers for a partition (DescribeProducers, Phase 66).
    ///
    /// Includes producers with committed sequences on the partition and those
    /// with open-txn write-through activity. Fields:
    /// `(producer_id, epoch, last_sequence, last_timestamp=-1, coordinator_epoch=0, txn_start_offset)`.
    /// `txn_start_offset` is the first open write-through offset when present, else `-1`.
    pub fn describe_producers_for_partition(
        &self,
        topic: &str,
        partition: u32,
    ) -> Vec<(u64, i32, i32, i64, i32, i64)> {
        self.expire_timed_out_txns();
        let key = (topic.to_owned(), partition);
        let prods = self.producer_state.read();
        let open = self.open_txns.lock();
        let prepared = self.prepared_txns.lock();
        let mut out = Vec::new();
        for (&pid, prod) in prods.iter() {
            let mut last_seq = -1i32;
            let mut in_scope = false;
            let mut txn_start = -1i64;
            if let Some(st) = prod.partitions.get(&key) {
                last_seq = st.base_sequence.saturating_add(st.count as i32).saturating_sub(1);
                in_scope = true;
            }
            if let Some(txn) = open.get(&pid) {
                if let Some(st) = txn.pending.get(&key) {
                    last_seq = st.base_sequence.saturating_add(st.count as i32).saturating_sub(1);
                    in_scope = true;
                }
                if let Some(first) = txn
                    .written
                    .iter()
                    .filter(|b| b.topic == topic && b.partition == partition)
                    .map(|b| b.first_offset)
                    .min()
                {
                    in_scope = true;
                    txn_start = first as i64;
                }
            }
            // Phase 90: prepared ranges also count as in-txn.
            if let Some(prep) = prepared.values().find(|p| p.producer_id == pid) {
                if let Some(st) = prep.open.pending.get(&key) {
                    last_seq = st.base_sequence.saturating_add(st.count as i32).saturating_sub(1);
                    in_scope = true;
                }
                if let Some(first) = prep
                    .open
                    .written
                    .iter()
                    .filter(|b| b.topic == topic && b.partition == partition)
                    .map(|b| b.first_offset)
                    .min()
                {
                    in_scope = true;
                    if txn_start < 0 || (first as i64) < txn_start {
                        txn_start = first as i64;
                    }
                }
            }
            if in_scope {
                out.push((pid, i32::from(prod.epoch), last_seq, -1, 0, txn_start));
            }
        }
        out.sort_by_key(|p| p.0);
        out
    }

    /// Whether a topic/partition exists (DescribeProducers).
    pub fn partition_exists(&self, topic: &str, partition: u32) -> bool {
        let name = TopicName::new(topic);
        self.topics
            .read()
            .get(&name)
            .map(|t| t.partitions.contains_key(&PartitionId(partition)))
            .unwrap_or(false)
    }

    /// Resolve topic name from numeric Volant topic id (Metadata TopicId lookup).
    pub fn topic_name_by_id(&self, topic_id: u32) -> Option<String> {
        let map = self.topics.read();
        map.values()
            .find(|t| t.id.0 == topic_id)
            .map(|t| t.name.as_str().to_owned())
    }

    /// Buffer consumer offsets to apply on commit (Phase 31 TxnOffsetCommit).
    ///
    /// Entries: `(group_id, topic, partition, offset, metadata)`.
    /// Phase 94: no open + abortable set → [`ErrorCode::TransactionAbortable`].
    pub fn buffer_txn_offsets(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        offsets: &[(String, String, u32, u64, String)],
    ) -> u16 {
        self.expire_timed_out_txns();
        let txn_id = {
            let state = self.producer_state.read();
            let Some(prod) = state.get(&producer_id) else {
                return ErrorCode::UnknownProducerId as u16;
            };
            if prod.epoch != producer_epoch {
                return ErrorCode::InvalidProducerEpoch as u16;
            }
            if !prod.transactional {
                return ErrorCode::InvalidTxnState as u16;
            }
            prod.transactional_id.clone()
        };
        if !txn_id.is_empty() && self.prepared_txns.lock().contains_key(&txn_id) {
            return ErrorCode::InvalidTxnState as u16;
        }
        let mut open = self.open_txns.lock();
        let Some(txn) = open.get_mut(&producer_id) else {
            return if self.is_txn_abortable(producer_id) {
                ErrorCode::TransactionAbortable as u16
            } else {
                ErrorCode::InvalidTxnState as u16
            };
        };
        txn.deferred_offsets.extend(offsets.iter().cloned());
        0
    }

    /// Whether the producer id is transactional (Phase 18).
    pub fn is_transactional_producer(&self, producer_id: u64) -> bool {
        self.producer_state
            .read()
            .get(&producer_id)
            .map(|p| p.transactional)
            .unwrap_or(false)
    }

    /// Write-through produce inside an open transaction (Phase 18/86).
    ///
    /// Appends to the partition log immediately and records a range that holds
    /// LSO back until EndTxn. On success returns [`IdempotentCheck::Accept`] or
    /// `Duplicate` with the real log base offset.
    pub fn buffer_txn_produce(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        topic: &str,
        partition: u32,
        base_sequence: i32,
        messages: Vec<Message>,
    ) -> IdempotentCheck {
        self.expire_timed_out_txns();
        let message_count = messages.len() as u32;
        if message_count == 0 {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidArg as u16,
            };
        }
        let txn_id = {
            let state = self.producer_state.read();
            let Some(prod) = state.get(&producer_id) else {
                return IdempotentCheck::Reject {
                    error_code: ErrorCode::UnknownProducerId as u16,
                };
            };
            if prod.epoch != producer_epoch {
                return IdempotentCheck::Reject {
                    error_code: ErrorCode::InvalidProducerEpoch as u16,
                };
            }
            if !prod.transactional {
                return IdempotentCheck::Reject {
                    error_code: ErrorCode::InvalidTxnState as u16,
                };
            }
            prod.transactional_id.clone()
        };
        if !txn_id.is_empty() && self.prepared_txns.lock().contains_key(&txn_id) {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidTxnState as u16,
            };
        }
        // Sequence check under the open-txn lock, then append outside it.
        let key = (topic.to_owned(), partition);
        {
            let open = self.open_txns.lock();
            let Some(txn) = open.get(&producer_id) else {
                // Phase 94: timeout auto-abort → TRANSACTION_ABORTABLE; else InvalidTxnState.
                let code = if self.is_txn_abortable(producer_id) {
                    ErrorCode::TransactionAbortable as u16
                } else {
                    ErrorCode::InvalidTxnState as u16
                };
                return IdempotentCheck::Reject {
                    error_code: code,
                };
            };
            let last = txn.pending.get(&key).cloned().or_else(|| {
                self.producer_state
                    .read()
                    .get(&producer_id)
                    .and_then(|p| p.partitions.get(&key).cloned())
            });
            match last {
                None => {}
                Some(last) => {
                    if base_sequence == last.base_sequence && message_count == last.count {
                        return IdempotentCheck::Duplicate {
                            base_offset: last.base_offset,
                            count: last.count,
                        };
                    }
                    let expected = last.base_sequence.saturating_add(last.count as i32);
                    if base_sequence != expected {
                        return IdempotentCheck::Reject {
                            error_code: ErrorCode::OutOfOrderSequence as u16,
                        };
                    }
                }
            }
        }

        // Write-through: append now so HWM advances and LSO can diverge.
        let topic_name = TopicName::new(topic);
        let mut mb = MessageBatch::default();
        mb.messages = messages;
        let (records, error_code) =
            match self.produce_with_acks(&topic_name, PartitionId(partition), mb, 1, None) {
                Ok(v) => v,
                Err(_) => {
                    return IdempotentCheck::Reject {
                        error_code: ErrorCode::Unknown as u16,
                    };
                }
            };
        if error_code != 0 {
            return IdempotentCheck::Reject {
                error_code,
            };
        }
        let base_offset = records.first().map(|r| r.offset.raw()).unwrap_or(0);
        let end_offset = base_offset.saturating_add(message_count as u64);
        let _ = self.flush(&topic_name, PartitionId(partition));

        let mut open = self.open_txns.lock();
        let Some(txn) = open.get_mut(&producer_id) else {
            // Raced with EndTxn/fence/timeout after append — treat as aborted range.
            drop(open);
            self.push_aborted_marker(
                topic,
                partition,
                AbortedTxnMarker {
                    producer_id,
                    first_offset: base_offset,
                    end_offset,
                },
            );
            self.persist_txn_markers();
            let code = if self.is_txn_abortable(producer_id) {
                ErrorCode::TransactionAbortable as u16
            } else {
                ErrorCode::InvalidTxnState as u16
            };
            return IdempotentCheck::Reject {
                error_code: code,
            };
        };
        txn.written.push(TxnWrittenRange {
            topic: topic.to_owned(),
            partition,
            first_offset: base_offset,
            end_offset,
            base_sequence,
            count: message_count,
        });
        txn.pending.insert(
            key,
            IdempotentBatchState {
                base_sequence,
                count: message_count,
                base_offset,
            },
        );
        drop(open);
        self.persist_txn_markers();
        IdempotentCheck::Accept { base_offset }
    }

    /// Commit or abort an open transaction (Phase 18/86/89/90).
    ///
    /// On commit, written ranges become stable (sequences finalized) and deferred
    /// offsets are applied. On abort, soft markers cover written ranges so
    /// READ_COMMITTED / native fetch hide them; data remains on the log for
    /// READ_UNCOMMITTED.
    ///
    /// Phase 89: dual-write Kafka-style control markers (COMMIT/ABORT) onto each
    /// partition that had write-through ranges (on **finalize** only).
    ///
    /// Phase 90: when the producer has `enable_2pc`, the first EndTxn moves the
    /// open txn to **Prepared** (no markers yet). A second EndTxn with the same
    /// decision finalizes. Prepared txns also complete via this path.
    ///
    /// Phase 92/93: timed-out prepared and open txns are auto-aborted before
    /// finalize/prepare.
    ///
    /// Phase 94: when no open/prepared remains and the producer is in the
    /// abortable set (timeout auto-abort), returns
    /// [`ErrorCode::TransactionAbortable`] and clears the flag so a subsequent
    /// begin/ensure can open a new txn.
    ///
    /// `offsets` entries are `(group_id, topic, partition, offset, metadata)`.
    ///
    /// Returns `(error_code, commit_results, cluster_fanout)`. Callers in cluster
    /// mode must run [`Txn2pcFanout`] via inter-broker RPC after a `0` error
    /// (Phase 114). On prepare fan-out failure, call [`Self::rollback_local_prepare`].
    pub fn end_txn(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        committed: bool,
        offsets: &[(String, String, u32, u64, String)],
    ) -> Result<(u16, Vec<TxnCommitResult>, Txn2pcFanout)> {
        self.expire_timed_out_txns();
        let (enable_2pc, transactional_id) = {
            let state = self.producer_state.read();
            let Some(prod) = state.get(&producer_id) else {
                return Ok((
                    ErrorCode::UnknownProducerId as u16,
                    Vec::new(),
                    Txn2pcFanout::None,
                ));
            };
            if prod.epoch != producer_epoch {
                return Ok((
                    ErrorCode::InvalidProducerEpoch as u16,
                    Vec::new(),
                    Txn2pcFanout::None,
                ));
            }
            (prod.enable_2pc, prod.transactional_id.clone())
        };

        // Phase 90: finalize an existing prepared txn (second EndTxn).
        if !transactional_id.is_empty() {
            let mut prepared = self.prepared_txns.lock();
            if let Some(prep) = prepared.get(&transactional_id) {
                if prep.producer_id != producer_id {
                    return Ok((
                        ErrorCode::InvalidTxnState as u16,
                        Vec::new(),
                        Txn2pcFanout::None,
                    ));
                }
                if prep.producer_epoch != producer_epoch {
                    return Ok((
                        ErrorCode::InvalidProducerEpoch as u16,
                        Vec::new(),
                        Txn2pcFanout::None,
                    ));
                }
                if prep.commit != committed {
                    return Ok((
                        ErrorCode::InvalidTxnState as u16,
                        Vec::new(),
                        Txn2pcFanout::None,
                    ));
                }
                let prep = prepared.remove(&transactional_id).expect("just checked");
                drop(prepared);
                // Completing a live prepare clears any stale abortable mark.
                self.clear_txn_abortable(producer_id);
                let results = self.finalize_txn(
                    producer_id,
                    producer_epoch,
                    committed,
                    prep.open,
                    offsets,
                )?;
                self.persist_prepared_txns();
                self.clear_cluster_prepared_index(&transactional_id);
                let fanout = if self.cluster.is_some() {
                    Txn2pcFanout::Complete {
                        transactional_id,
                        producer_id,
                        producer_epoch,
                        commit: committed,
                    }
                } else {
                    Txn2pcFanout::None
                };
                return Ok((0, results, fanout));
            }
        }

        let txn = {
            let mut open = self.open_txns.lock();
            match open.remove(&producer_id) {
                Some(t) => t,
                None => {
                    // Phase 94: timeout already aborted → TRANSACTION_ABORTABLE.
                    if self.take_txn_abortable(producer_id) {
                        return Ok((
                            ErrorCode::TransactionAbortable as u16,
                            Vec::new(),
                            Txn2pcFanout::None,
                        ));
                    }
                    return Ok((
                        ErrorCode::InvalidTxnState as u16,
                        Vec::new(),
                        Txn2pcFanout::None,
                    ));
                }
            }
        };
        // Successful open finalize also clears abortable (defensive).
        self.clear_txn_abortable(producer_id);

        // Phase 90: first EndTxn on a 2PC producer → prepare (durable).
        if enable_2pc && !transactional_id.is_empty() {
            let prep = PreparedTxn {
                transactional_id: transactional_id.clone(),
                producer_id,
                producer_epoch,
                commit: committed,
                prepared_at_ms: unix_now_ms(),
                open: txn,
            };
            self.prepared_txns
                .lock()
                .insert(transactional_id.clone(), prep);
            // Open ranges leave open markers; prepared holds LSO via prepared map.
            self.persist_txn_markers();
            self.persist_prepared_txns();
            self.upsert_cluster_prepared_index(
                &transactional_id,
                producer_id,
                producer_epoch,
                committed,
            );
            let fanout = if self.cluster.is_some() {
                Txn2pcFanout::Prepare {
                    transactional_id,
                    producer_id,
                    producer_epoch,
                    commit: committed,
                }
            } else {
                Txn2pcFanout::None
            };
            return Ok((0, Vec::new(), fanout));
        }

        let results = self.finalize_txn(
            producer_id,
            producer_epoch,
            committed,
            txn,
            offsets,
        )?;
        // Non-2PC one-shot: still fan out complete so peers that held open
        // ranges (from open fan-out) finalize consistently in cluster mode.
        let fanout = if self.cluster.is_some() && !transactional_id.is_empty() {
            Txn2pcFanout::Complete {
                transactional_id,
                producer_id,
                producer_epoch,
                commit: committed,
            }
        } else {
            Txn2pcFanout::None
        };
        Ok((0, results, fanout))
    }

    /// Finalize commit/abort for an open or prepared txn body.
    fn finalize_txn(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        committed: bool,
        txn: OpenTxn,
        offsets: &[(String, String, u32, u64, String)],
    ) -> Result<Vec<TxnCommitResult>> {
        if !committed {
            self.record_aborted_from_txn(producer_id, &txn);
            self.append_txn_control_markers(
                producer_id,
                producer_epoch,
                ControlMarkerType::Abort,
                &txn,
            );
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(txn.written.len());
        for batch in &txn.written {
            self.record_idempotent_produce(
                producer_id,
                producer_epoch,
                &batch.topic,
                batch.partition,
                batch.base_sequence,
                batch.count,
                batch.first_offset,
            );
            results.push(TxnCommitResult {
                topic: batch.topic.clone(),
                partition: batch.partition,
                base_offset: batch.first_offset,
                count: batch.count,
            });
        }

        self.append_txn_control_markers(
            producer_id,
            producer_epoch,
            ControlMarkerType::Commit,
            &txn,
        );

        let mut all_offsets = txn.deferred_offsets;
        for o in offsets {
            all_offsets.push(o.clone());
        }
        for (group_id, topic, partition, offset, metadata) in &all_offsets {
            let _ = self.groups().commit_offsets(
                group_id,
                "",
                0,
                &[(topic.clone(), *partition, *offset, metadata.clone())],
            );
        }

        self.persist_txn_markers();
        Ok(results)
    }

    /// Last stable offset for a partition (Phase 86/90/92/93).
    ///
    /// Equal to HWM when no open/prepared write-through ranges exist; otherwise
    /// the minimum first offset among open **and prepared** transactional writes.
    ///
    /// Phase 92/93: expires timed-out prepared and open txns first so Fetch
    /// isolation advances without a separate txn API call.
    pub fn last_stable_offset(&self, topic: &str, partition: u32) -> u64 {
        self.expire_timed_out_txns();
        let hwm = self
            .high_watermark(&TopicName::new(topic), PartitionId(partition))
            .unwrap_or(0);
        let mut lso = hwm;
        {
            let open = self.open_txns.lock();
            for txn in open.values() {
                for r in &txn.written {
                    if r.topic == topic && r.partition == partition {
                        lso = lso.min(r.first_offset);
                    }
                }
            }
        }
        {
            let prepared = self.prepared_txns.lock();
            for prep in prepared.values() {
                for r in &prep.open.written {
                    if r.topic == topic && r.partition == partition {
                        lso = lso.min(r.first_offset);
                    }
                }
            }
        }
        lso
    }

    /// Aborted transactions overlapping `[fetch_offset, upper_bound)` for Fetch.
    ///
    /// Returns `(producer_id, first_offset)` pairs (Kafka aborted_transactions wire).
    pub fn aborted_transactions_for_fetch(
        &self,
        topic: &str,
        partition: u32,
        fetch_offset: u64,
        upper_bound: u64,
    ) -> Vec<(u64, u64)> {
        let aborted = self.aborted_txns.lock();
        let Some(list) = aborted.get(&(topic.to_owned(), partition)) else {
            return Vec::new();
        };
        let mut out: Vec<(u64, u64)> = list
            .iter()
            .filter(|m| m.first_offset < upper_bound && m.end_offset > fetch_offset)
            .map(|m| (m.producer_id, m.first_offset))
            .collect();
        out.sort_by_key(|e| e.1);
        out.dedup();
        out
    }

    /// Whether `offset` falls in an aborted transactional range on the partition.
    pub fn is_aborted_offset(&self, topic: &str, partition: u32, offset: u64) -> bool {
        let aborted = self.aborted_txns.lock();
        let Some(list) = aborted.get(&(topic.to_owned(), partition)) else {
            return false;
        };
        list.iter()
            .any(|m| offset >= m.first_offset && offset < m.end_offset)
    }

    /// Whether `offset` is still unstable (open or prepared write-through txn).
    pub fn is_unstable_offset(&self, topic: &str, partition: u32, offset: u64) -> bool {
        self.expire_timed_out_txns();
        {
            let open = self.open_txns.lock();
            for txn in open.values() {
                for r in &txn.written {
                    if r.topic == topic
                        && r.partition == partition
                        && offset >= r.first_offset
                        && offset < r.end_offset
                    {
                        return true;
                    }
                }
            }
        }
        {
            let prepared = self.prepared_txns.lock();
            for prep in prepared.values() {
                for r in &prep.open.written {
                    if r.topic == topic
                        && r.partition == partition
                        && offset >= r.first_offset
                        && offset < r.end_offset
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Force-abort a prepared txn (InitProducerId KeepPreparedTxn=false / Phase 92 timeout).
    fn force_abort_prepared(&self, prep: PreparedTxn) {
        self.record_aborted_from_txn(prep.producer_id, &prep.open);
        self.append_txn_control_markers(
            prep.producer_id,
            prep.producer_epoch,
            ControlMarkerType::Abort,
            &prep.open,
        );
        self.persist_prepared_txns();
    }

    /// Lazy expiry of timed-out open **and** prepared txns (Phase 92/93).
    ///
    /// Called at the start of txn/LSO paths and by the Phase 97 background
    /// sweeper. Returns `(open_aborted, prepared_aborted)`.
    pub fn expire_timed_out_txns(&self) -> (usize, usize) {
        let open_n = self.expire_timed_out_open_txns();
        let prep_n = self.expire_timed_out_prepared_txns();
        (open_n, prep_n)
    }

    /// Clamp a positive timeout to the broker max (Phase 96).
    ///
    /// `0` (disabled) is never raised or lowered. When max is `0`, no clamp.
    fn clamp_txn_timeout_ms(&self, timeout_ms: u64) -> u64 {
        if timeout_ms == 0 {
            return 0;
        }
        let max = self.transaction_max_timeout_ms.load(Ordering::Relaxed);
        if max > 0 && timeout_ms > max {
            max
        } else {
            timeout_ms
        }
    }

    /// Effective open-txn timeout for a producer (Phase 93 + 96 clamp).
    ///
    /// Positive client timeout wins; otherwise broker default. Then clamped to
    /// [`Self::transaction_max_timeout_ms`] when max > 0. `0` = disabled.
    fn effective_open_txn_timeout_ms(&self, prod: &ProducerEpochState) -> u64 {
        let raw = if prod.transaction_timeout_ms > 0 {
            prod.transaction_timeout_ms
        } else {
            self.open_txn_timeout_ms.load(Ordering::Relaxed)
        };
        self.clamp_txn_timeout_ms(raw)
    }

    /// Effective prepared-txn timeout (Phase 92 + 96 clamp).
    ///
    /// Configured prepared timeout, clamped to broker max when max > 0.
    /// `0` = disabled.
    fn effective_prepared_txn_timeout_ms(&self) -> u64 {
        self.clamp_txn_timeout_ms(self.prepared_txn_timeout_ms.load(Ordering::Relaxed))
    }

    /// Auto-abort open (non-prepared) transactions older than their effective
    /// timeout (Phase 93 + 96 clamp).
    ///
    /// Returns the number of open txns aborted. Same effect as EndTxn(abort):
    /// soft markers + ABORT control batches; deferred offsets dropped.
    pub fn expire_timed_out_open_txns(&self) -> usize {
        let now = unix_now_ms();
        let expired: Vec<(u64, u16, OpenTxn)> = {
            let mut open = self.open_txns.lock();
            if open.is_empty() {
                return 0;
            }
            let prods = self.producer_state.read();
            let broker_default = self.open_txn_timeout_ms.load(Ordering::Relaxed);
            let mut keys: Vec<u64> = Vec::new();
            for (&pid, txn) in open.iter() {
                let raw = prods
                    .get(&pid)
                    .map(|p| {
                        if p.transaction_timeout_ms > 0 {
                            p.transaction_timeout_ms
                        } else {
                            broker_default
                        }
                    })
                    .unwrap_or(broker_default);
                let timeout = self.clamp_txn_timeout_ms(raw);
                if timeout == 0 {
                    continue;
                }
                let opened = if txn.opened_at_ms > 0 {
                    txn.opened_at_ms
                } else {
                    // Defensive: treat missing clock as "now" (do not mass-abort).
                    now
                };
                if now.saturating_sub(opened) >= timeout as i64 {
                    keys.push(pid);
                }
            }
            keys.into_iter()
                .filter_map(|pid| {
                    let txn = open.remove(&pid)?;
                    let epoch = prods.get(&pid).map(|p| p.epoch).unwrap_or(0);
                    Some((pid, epoch, txn))
                })
                .collect()
        };
        let n = expired.len();
        for (pid, epoch, txn) in expired {
            self.record_aborted_from_txn(pid, &txn);
            self.append_txn_control_markers(pid, epoch, ControlMarkerType::Abort, &txn);
            // Phase 94: client must observe TRANSACTION_ABORTABLE until EndTxn.
            self.mark_txn_abortable(pid);
        }
        if n > 0 {
            self.persist_txn_markers();
            self.open_txns_expired_total
                .fetch_add(n as u64, Ordering::Relaxed);
        }
        n
    }

    /// Auto-abort prepared transactions older than the effective timeout
    /// (Phase 92 + 96 clamp).
    ///
    /// Returns the number of prepared txns aborted. No-op when effective
    /// timeout is `0` (disabled) or the prepared map is empty. Same finalize
    /// path as KeepPreparedTxn=false force-abort. Phase 94 marks abortable
    /// producers.
    pub fn expire_timed_out_prepared_txns(&self) -> usize {
        let timeout_ms = self.effective_prepared_txn_timeout_ms();
        if timeout_ms == 0 {
            return 0;
        }
        let now = unix_now_ms();
        let expired: Vec<PreparedTxn> = {
            let mut map = self.prepared_txns.lock();
            if map.is_empty() {
                return 0;
            }
            let keys: Vec<String> = map
                .iter()
                .filter(|(_, prep)| {
                    now.saturating_sub(prep.prepared_at_ms) >= timeout_ms as i64
                })
                .map(|(k, _)| k.clone())
                .collect();
            keys.into_iter()
                .filter_map(|k| map.remove(&k))
                .collect()
        };
        let n = expired.len();
        for prep in expired {
            // Soft markers + control batches; persist once at the end via last call.
            self.record_aborted_from_txn(prep.producer_id, &prep.open);
            self.append_txn_control_markers(
                prep.producer_id,
                prep.producer_epoch,
                ControlMarkerType::Abort,
                &prep.open,
            );
            self.mark_txn_abortable(prep.producer_id);
        }
        if n > 0 {
            self.persist_prepared_txns();
            self.prepared_txns_expired_total
                .fetch_add(n as u64, Ordering::Relaxed);
        }
        n
    }

    /// Whether this producer is in the Phase 94 abortable set (timeout auto-abort).
    pub fn is_txn_abortable(&self, producer_id: u64) -> bool {
        self.abortable_producers.lock().contains(&producer_id)
    }

    /// Mark producer as needing client abort acknowledgment (Phase 94).
    fn mark_txn_abortable(&self, producer_id: u64) {
        self.abortable_producers.lock().insert(producer_id);
    }

    /// Clear abortable mark without returning whether it was set (Phase 94).
    fn clear_txn_abortable(&self, producer_id: u64) {
        self.abortable_producers.lock().remove(&producer_id);
    }

    /// Clear and return whether the producer was abortable (Phase 94 EndTxn path).
    fn take_txn_abortable(&self, producer_id: u64) -> bool {
        self.abortable_producers.lock().remove(&producer_id)
    }

    /// Backdate a prepared txn's `prepared_at_ms` for tests (Phase 92).
    ///
    /// `age_ms` is subtracted from the current wall clock. Returns `false` when
    /// the transactional id is not prepared.
    pub fn backdate_prepared_txn(&self, transactional_id: &str, age_ms: i64) -> bool {
        let mut map = self.prepared_txns.lock();
        let Some(prep) = map.get_mut(transactional_id) else {
            return false;
        };
        prep.prepared_at_ms = unix_now_ms().saturating_sub(age_ms.max(0));
        // Persist so restart-based tests see the aged timestamp.
        drop(map);
        self.persist_prepared_txns();
        true
    }

    /// Backdate an open txn's `opened_at_ms` for tests (Phase 93).
    ///
    /// `age_ms` is subtracted from the current wall clock. Returns `false` when
    /// the producer has no open txn.
    pub fn backdate_open_txn(&self, producer_id: u64, age_ms: i64) -> bool {
        let mut open = self.open_txns.lock();
        let Some(txn) = open.get_mut(&producer_id) else {
            return false;
        };
        txn.opened_at_ms = unix_now_ms().saturating_sub(age_ms.max(0));
        true
    }

    fn prepared_txns_path(&self) -> PathBuf {
        self.storage
            .data_dir
            .join("__txn_prepared")
            .join("state.json")
    }

    /// Load durable prepared transactions (Phase 90/92). Prepared **survives** crash.
    fn load_prepared_txns(&self) {
        let path = self.prepared_txns_path();
        let Ok(bytes) = fs::read(&path) else {
            return;
        };
        let Ok(file) = serde_json::from_slice::<PreparedTxnsFile>(&bytes) else {
            return;
        };
        let load_now = unix_now_ms();
        let mut map = self.prepared_txns.lock();
        for s in file.prepared {
            let mut pending = HashMap::new();
            for p in s.pending {
                pending.insert(
                    (p.topic, p.partition),
                    IdempotentBatchState {
                        base_sequence: p.base_sequence,
                        count: p.count,
                        base_offset: p.base_offset,
                    },
                );
            }
            let written = s
                .written
                .into_iter()
                .map(|w| TxnWrittenRange {
                    topic: w.topic,
                    partition: w.partition,
                    first_offset: w.first_offset,
                    end_offset: w.end_offset,
                    base_sequence: w.base_sequence,
                    count: w.count,
                })
                .collect();
            // Pre-Phase-92 snapshots lack prepared_at_ms (0) → start clock at load.
            let prepared_at_ms = if s.prepared_at_ms > 0 {
                s.prepared_at_ms
            } else {
                load_now
            };
            map.insert(
                s.transactional_id.clone(),
                PreparedTxn {
                    transactional_id: s.transactional_id,
                    producer_id: s.producer_id,
                    producer_epoch: s.producer_epoch,
                    commit: s.commit,
                    prepared_at_ms,
                    open: OpenTxn {
                        opened_at_ms: 0, // not used once prepared
                        producer_epoch: s.producer_epoch,
                        added: s.added,
                        written,
                        pending,
                        deferred_offsets: s.deferred_offsets,
                    },
                },
            );
        }
    }

    fn persist_prepared_txns(&self) {
        let path = self.prepared_txns_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let mut file = PreparedTxnsFile::default();
        {
            let prepared = self.prepared_txns.lock();
            for prep in prepared.values() {
                let written = prep
                    .open
                    .written
                    .iter()
                    .map(|w| StoredPreparedWritten {
                        topic: w.topic.clone(),
                        partition: w.partition,
                        first_offset: w.first_offset,
                        end_offset: w.end_offset,
                        base_sequence: w.base_sequence,
                        count: w.count,
                    })
                    .collect();
                let pending = prep
                    .open
                    .pending
                    .iter()
                    .map(|((topic, part), st)| StoredPreparedPending {
                        topic: topic.clone(),
                        partition: *part,
                        base_sequence: st.base_sequence,
                        count: st.count,
                        base_offset: st.base_offset,
                    })
                    .collect();
                file.prepared.push(StoredPreparedTxn {
                    transactional_id: prep.transactional_id.clone(),
                    producer_id: prep.producer_id,
                    producer_epoch: prep.producer_epoch,
                    commit: prep.commit,
                    prepared_at_ms: prep.prepared_at_ms,
                    added: prep.open.added.clone(),
                    written,
                    pending,
                    deferred_offsets: prep.open.deferred_offsets.clone(),
                });
            }
        }
        let Ok(bytes) = serde_json::to_vec_pretty(&file) else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        if fs::write(&tmp, &bytes).is_ok() {
            let _ = fs::rename(tmp, path);
        }
    }

    fn record_aborted_from_txn(&self, producer_id: u64, txn: &OpenTxn) {
        // Collapse per-partition first/end for the producer (Kafka lists first offset).
        let mut per_part: HashMap<(String, u32), (u64, u64)> = HashMap::new();
        for r in &txn.written {
            let e = per_part
                .entry((r.topic.clone(), r.partition))
                .or_insert((r.first_offset, r.end_offset));
            e.0 = e.0.min(r.first_offset);
            e.1 = e.1.max(r.end_offset);
        }
        for ((topic, part), (first, end)) in per_part {
            self.push_aborted_marker(
                &topic,
                part,
                AbortedTxnMarker {
                    producer_id,
                    first_offset: first,
                    end_offset: end,
                },
            );
        }
        self.persist_txn_markers();
    }

    /// Append one Kafka-style control marker per partition that participated in
    /// the txn (Phase 89 dual-write with soft markers; Phase 105 includes empty
    /// AddPartitions membership with no write-through data).
    fn append_txn_control_markers(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        marker_type: ControlMarkerType,
        txn: &OpenTxn,
    ) {
        // One marker per (topic, partition), not per batch.
        let mut seen = HashMap::<(String, u32), ()>::new();
        for r in &txn.written {
            let key = (r.topic.clone(), r.partition);
            if seen.contains_key(&key) {
                continue;
            }
            seen.insert(key.clone(), ());
            let msg = txn_control_message(marker_type, producer_id, producer_epoch);
            let topic = TopicName::new(r.topic.clone());
            let _ = self.produce_one(&topic, PartitionId(r.partition), msg);
            let _ = self.flush(&topic, PartitionId(r.partition));
        }
        // Phase 105: control-only for AddPartitions membership without data.
        for (topic_name, partition) in &txn.added {
            let key = (topic_name.clone(), *partition);
            if seen.contains_key(&key) {
                continue;
            }
            seen.insert(key, ());
            let msg = txn_control_message(marker_type, producer_id, producer_epoch);
            let topic = TopicName::new(topic_name.clone());
            let _ = self.produce_one(&topic, PartitionId(*partition), msg);
            let _ = self.flush(&topic, PartitionId(*partition));
        }
    }

    fn push_aborted_marker(&self, topic: &str, partition: u32, marker: AbortedTxnMarker) {
        let mut aborted = self.aborted_txns.lock();
        aborted
            .entry((topic.to_owned(), partition))
            .or_default()
            .push(marker);
    }

    /// GC / clip aborted soft markers against `log_start` (Phase 104 + 111).
    ///
    /// Markers cover `[first_offset, end_offset)`:
    /// - `end_offset <= log_start` → **drop** (Phase 104; no live overlap)
    /// - `first_offset < log_start < end_offset` → **clip** `first_offset =
    ///   log_start` (Phase 111; obsolete prefix no longer on the log)
    /// - `first_offset >= log_start` → unchanged
    ///
    /// Returns the number of markers **mutated** (dropped + clipped). The GC
    /// counter advances for **drops only** (Phase 104 semantics preserved).
    fn gc_aborted_markers_below(&self, topic: &str, partition: u32, log_start: u64) -> usize {
        let key = (topic.to_owned(), partition);
        let mut aborted = self.aborted_txns.lock();
        let Some(list) = aborted.get_mut(&key) else {
            return 0;
        };
        let before = list.len();
        list.retain(|m| m.end_offset > log_start);
        let dropped = before - list.len();
        let mut clipped = 0usize;
        for m in list.iter_mut() {
            if m.first_offset < log_start {
                m.first_offset = log_start;
                clipped += 1;
            }
        }
        if list.is_empty() {
            aborted.remove(&key);
        }
        if dropped > 0 {
            self.aborted_markers_gc_total
                .fetch_add(dropped as u64, Ordering::Relaxed);
        }
        dropped + clipped
    }

    /// GC / clip markers for one partition and persist `__txn_markers` when any
    /// drop or clip occurred (Phase 104 + 111).
    fn gc_and_persist_aborted_markers(&self, topic: &str, partition: u32, log_start: u64) {
        if self.gc_aborted_markers_below(topic, partition, log_start) > 0 {
            self.persist_txn_markers();
        }
    }

    /// GC / clip markers against each partition's current log start
    /// (Phase 104 + 111).
    ///
    /// Used after retention and on load (self-heal). Persists once if anything
    /// was dropped or clipped. Return value is total mutations (drops + clips).
    fn gc_stale_aborted_markers_all(&self) -> usize {
        // Snapshot log starts under the topics read lock, then GC without it
        // (aborted_txns is a separate lock; avoid holding both).
        let starts: Vec<(String, u32, u64)> = {
            let topics = self.topics.read();
            let mut out = Vec::new();
            for t in topics.values() {
                for (pid, part) in &t.partitions {
                    out.push((
                        t.name.as_str().to_owned(),
                        pid.0,
                        part.log.log_start_offset().raw(),
                    ));
                }
            }
            out
        };
        let mut total = 0usize;
        for (topic, part, start) in starts {
            total += self.gc_aborted_markers_below(&topic, part, start);
        }
        if total > 0 {
            self.persist_txn_markers();
        }
        total
    }

    fn txn_markers_path(&self) -> PathBuf {
        self.storage.data_dir.join("__txn_markers").join("state.json")
    }

    /// Load soft markers; promote any stored open ranges to aborted (crash ≡ abort).
    ///
    /// Phase 98: when promoting open → aborted, also append ABORT control
    /// RecordBatches (same dual-write as EndTxn abort). Idempotent across
    /// restarts: only the open list is promoted (and then cleared on persist),
    /// so a second load sees empty `open` and does not re-append.
    ///
    /// Phase 105: empty AddPartitions membership (`open_added`) also gets
    /// ABORT control batches (no soft markers — nothing to filter).
    ///
    /// Phase 104/111: after load, drop markers fully below each partition's
    /// current log start and clip straddlers (self-heal after crash / older files).
    fn load_txn_markers(&self) {
        let path = self.txn_markers_path();
        let Ok(bytes) = fs::read(&path) else {
            // Still run GC in case memory was seeded elsewhere (no-op normally).
            let _ = self.gc_stale_aborted_markers_all();
            return;
        };
        let Ok(file) = serde_json::from_slice::<TxnMarkersFile>(&bytes) else {
            return;
        };
        {
            let mut aborted = self.aborted_txns.lock();
            for m in &file.aborted {
                aborted
                    .entry((m.topic.clone(), m.partition))
                    .or_default()
                    .push(AbortedTxnMarker {
                        producer_id: m.producer_id,
                        first_offset: m.first_offset,
                        end_offset: m.end_offset,
                    });
            }
            // Crash recovery: open ranges → aborted soft markers.
            // open_added (Phase 105 empty membership) is intentionally omitted:
            // control-only; no soft range to promote.
            for m in &file.open {
                aborted
                    .entry((m.topic.clone(), m.partition))
                    .or_default()
                    .push(AbortedTxnMarker {
                        producer_id: m.producer_id,
                        first_offset: m.first_offset,
                        end_offset: m.end_offset,
                    });
            }
        }
        // Phase 98/105: dual-write ABORT control for crash-promoted opens
        // (written ranges + empty AddPartitions membership).
        if !file.open.is_empty() || !file.open_added.is_empty() {
            self.append_crash_abort_control_markers(&file.open, &file.open_added);
        }
        // Phase 104/111: drop markers entirely below current log start; clip
        // straddlers so first_offset is not below live log.
        let mutated = self.gc_stale_aborted_markers_all();
        // Persist cleaned state (no open ranges after recovery; GC/clip applied).
        // gc_stale already persists when mutated > 0; always persist once after
        // load so open→aborted promotion is durable even with zero GC.
        if mutated == 0 {
            self.persist_txn_markers();
        }
    }

    /// Append ABORT control markers for open ranges / empty membership promoted
    /// on crash recovery (Phase 98 + Phase 105). One marker per
    /// (producer_id, topic, partition).
    ///
    /// Epoch resolution order:
    /// 1. `producer_epoch` stored on the open marker (Phase 98 snapshots)
    /// 2. Live producer state epoch (best-effort for pre-98 files)
    /// 3. Skip control batch (soft abort still applied for written ranges)
    fn append_crash_abort_control_markers(
        &self,
        open: &[StoredTxnRange],
        open_added: &[StoredAddedPartition],
    ) {
        // Group written ranges + empty membership by producer_id; track epoch.
        let mut by_pid: HashMap<u64, (Option<u16>, OpenTxn)> = HashMap::new();
        for m in open {
            let entry = by_pid.entry(m.producer_id).or_insert_with(|| {
                (
                    m.producer_epoch,
                    OpenTxn {
                        producer_epoch: m.producer_epoch.unwrap_or(0),
                        ..OpenTxn::default()
                    },
                )
            });
            if entry.0.is_none() {
                entry.0 = m.producer_epoch;
            }
            entry.1.written.push(TxnWrittenRange {
                topic: m.topic.clone(),
                partition: m.partition,
                first_offset: m.first_offset,
                end_offset: m.end_offset,
                base_sequence: 0,
                count: 0,
            });
        }
        for m in open_added {
            let entry = by_pid.entry(m.producer_id).or_insert_with(|| {
                (
                    m.producer_epoch,
                    OpenTxn {
                        producer_epoch: m.producer_epoch.unwrap_or(0),
                        ..OpenTxn::default()
                    },
                )
            });
            if entry.0.is_none() {
                entry.0 = m.producer_epoch;
            }
            let key = (m.topic.clone(), m.partition);
            if !entry.1.added.iter().any(|(t, p)| t == &key.0 && *p == key.1) {
                entry.1.added.push(key);
            }
        }
        for (pid, (stored_epoch, txn)) in by_pid {
            let epoch = match stored_epoch {
                Some(e) => e,
                None => {
                    // Pre-Phase-98 snapshot: best-effort from producer state.
                    let state = self.producer_state.read();
                    match state.get(&pid).map(|p| p.epoch) {
                        Some(e) => e,
                        None => {
                            // Cannot encode a honest control batch without epoch.
                            continue;
                        }
                    }
                }
            };
            self.append_txn_control_markers(pid, epoch, ControlMarkerType::Abort, &txn);
        }
    }

    fn persist_txn_markers(&self) {
        let path = self.txn_markers_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let mut file = TxnMarkersFile::default();
        {
            let open = self.open_txns.lock();
            for (&pid, txn) in open.iter() {
                for r in &txn.written {
                    file.open.push(StoredTxnRange {
                        producer_id: pid,
                        producer_epoch: Some(txn.producer_epoch),
                        topic: r.topic.clone(),
                        partition: r.partition,
                        first_offset: r.first_offset,
                        end_offset: r.end_offset,
                    });
                }
                // Phase 105: empty membership only — skip partitions that already
                // have write-through ranges (those are covered by `open`).
                for (topic, part) in &txn.added {
                    let has_written = txn
                        .written
                        .iter()
                        .any(|r| r.topic == *topic && r.partition == *part);
                    if has_written {
                        continue;
                    }
                    file.open_added.push(StoredAddedPartition {
                        producer_id: pid,
                        producer_epoch: Some(txn.producer_epoch),
                        topic: topic.clone(),
                        partition: *part,
                    });
                }
            }
        }
        {
            let aborted = self.aborted_txns.lock();
            for ((topic, part), list) in aborted.iter() {
                for m in list {
                    file.aborted.push(StoredTxnRange {
                        producer_id: m.producer_id,
                        producer_epoch: None,
                        topic: topic.clone(),
                        partition: *part,
                        first_offset: m.first_offset,
                        end_offset: m.end_offset,
                    });
                }
            }
        }
        let Ok(bytes) = serde_json::to_vec_pretty(&file) else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        if fs::write(&tmp, &bytes).is_ok() {
            let _ = fs::rename(tmp, path);
        }
    }

    /// Check idempotent produce sequence before appending.
    ///
    /// Non-idempotent produces (`producer_id == 0` or `base_sequence < 0`) always
    /// return [`IdempotentCheck::Accept`] without consulting producer state.
    ///
    /// Transactional producers without an open txn are rejected (`InvalidTxnState`).
    /// Callers should route open-txn produces through [`Self::buffer_txn_produce`].
    pub fn check_idempotent_produce(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        topic: &str,
        partition: u32,
        base_sequence: i32,
        message_count: u32,
    ) -> IdempotentCheck {
        if producer_id == 0 || base_sequence < 0 {
            return IdempotentCheck::Accept { base_offset: 0 };
        }
        if message_count == 0 {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidArg as u16,
            };
        }

        let state = self.producer_state.read();
        let Some(prod) = state.get(&producer_id) else {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::UnknownProducerId as u16,
            };
        };
        if prod.epoch != producer_epoch {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidProducerEpoch as u16,
            };
        }
        if prod.transactional {
            // Transactional PIDs must produce only inside BeginTxn…EndTxn.
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidTxnState as u16,
            };
        }

        let key = (topic.to_owned(), partition);
        match prod.partitions.get(&key) {
            None => IdempotentCheck::Accept { base_offset: 0 },
            Some(last) => {
                if base_sequence == last.base_sequence && message_count == last.count {
                    IdempotentCheck::Duplicate {
                        base_offset: last.base_offset,
                        count: last.count,
                    }
                } else {
                    let expected = last
                        .base_sequence
                        .saturating_add(last.count as i32);
                    if base_sequence == expected {
                        IdempotentCheck::Accept { base_offset: 0 }
                    } else {
                        IdempotentCheck::Reject {
                            error_code: ErrorCode::OutOfOrderSequence as u16,
                        }
                    }
                }
            }
        }
    }

    /// Record a successful idempotent produce batch.
    pub fn record_idempotent_produce(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        topic: &str,
        partition: u32,
        base_sequence: i32,
        count: u32,
        base_offset: u64,
    ) {
        if producer_id == 0 || base_sequence < 0 {
            return;
        }
        {
            let mut state = self.producer_state.write();
            let Some(prod) = state.get_mut(&producer_id) else {
                return;
            };
            if prod.epoch != producer_epoch {
                return;
            }
            prod.partitions.insert(
                (topic.to_owned(), partition),
                IdempotentBatchState {
                    base_sequence,
                    count,
                    base_offset,
                },
            );
        }
        let _ = self.persist_producer_state();
    }

    /// Persist current producer map to disk.
    fn persist_producer_state(&self) -> Result<()> {
        let next_id = self.next_producer_id.load(Ordering::Relaxed);
        let state = self.producer_state.read();
        let mut file = ProducerStateFile {
            next_id,
            producers: HashMap::new(),
        };
        for (pid, prod) in state.iter() {
            let mut partitions = HashMap::new();
            for ((topic, part), batch) in &prod.partitions {
                partitions.insert(
                    partition_key(topic, *part),
                    StoredBatch {
                        base_sequence: batch.base_sequence,
                        count: batch.count,
                        base_offset: batch.base_offset,
                    },
                );
            }
            file.producers.insert(
                pid.to_string(),
                StoredProducer {
                    epoch: prod.epoch,
                    transactional_id: prod.transactional_id.clone(),
                    enable_2pc: prod.enable_2pc,
                    transaction_timeout_ms: prod.transaction_timeout_ms,
                    partitions,
                },
            );
        }
        self.producer_store.save(&file)
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
                return asg
                    .topics
                    .values()
                    .map(|t| t.partitions.len() as u64)
                    .sum();
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
        self.advertised_port.store(u32::from(port), Ordering::Relaxed);
        // Also update cluster config advertised if this is our node — clients
        // use Metadata brokers list from config hosts by default.
    }

    /// Create a topic with the given partition count.
    ///
    /// In multi-node mode only the controller may create topics.
    pub fn create_topic(&self, name: impl Into<TopicName>, partitions: u32) -> Result<TopicId> {
        self.create_topic_with_configs(name, partitions, &[])
    }

    /// Create a topic with optional config key/value pairs (Phase 13).
    pub fn create_topic_with_configs(
        &self,
        name: impl Into<TopicName>,
        partitions: u32,
        config_entries: &[(String, String)],
    ) -> Result<TopicId> {
        let name = name.into();
        if partitions == 0 {
            return Err(Error::InvalidArgument(
                "topic must have at least one partition".into(),
            ));
        }
        let topic_cfg = TopicConfig::from_entries(config_entries)?;

        if let Some(cluster) = &self.cluster {
            if !cluster.membership.read().is_controller() {
                return Err(Error::InvalidArgument(format!(
                    "not controller; controller_id={}",
                    cluster.membership.read().controller_id()
                )));
            }
            return self.create_topic_cluster(name, partitions, &topic_cfg);
        }

        // Single-node path.
        let mut topics = self.topics.write();
        if topics.contains_key(&name) {
            return Err(Error::InvalidArgument(format!(
                "topic already exists: {}",
                name.as_str()
            )));
        }
        let id = TopicId(self.next_topic_id.fetch_add(1, Ordering::SeqCst));
        let topic = Topic::create_with_config(
            id,
            name.clone(),
            partitions,
            &self.storage,
            &topic_cfg,
        )?;
        topics.insert(name.clone(), topic);
        self.rr_counters
            .write()
            .insert(name.clone(), AtomicU64::new(0));
        self.topic_configs.save(name.as_str(), &topic_cfg)?;
        // Seed epoch 0 @ start 0 for each new partition (Phase 87).
        {
            let mut epochs = self.leader_epochs.write();
            for pid in 0..partitions {
                let key = (name.as_str().to_owned(), pid);
                let e = epochs.entry(key).or_default();
                ensure_entry(e, 0, 0);
            }
        }
        self.persist_leader_epochs();
        drop(topics);
        self.persist_topic_catalog()?;
        Ok(id)
    }

    fn create_topic_cluster(
        &self,
        name: TopicName,
        partitions: u32,
        topic_cfg: &TopicConfig,
    ) -> Result<TopicId> {
        let cluster = self.cluster.as_ref().expect("cluster");
        {
            let topics = self.topics.read();
            if topics.contains_key(&name) {
                return Err(Error::InvalidArgument(format!(
                    "topic already exists: {}",
                    name.as_str()
                )));
            }
            let asg = cluster.assignment.read();
            if asg.topics.contains_key(name.as_str()) {
                return Err(Error::InvalidArgument(format!(
                    "topic already exists: {}",
                    name.as_str()
                )));
            }
        }

        let broker_ids = cluster.config.broker_ids();
        let rf = cluster
            .config
            .default_replication_factor
            .min(broker_ids.len() as u32)
            .max(1);
        let replica_sets = assign_replicas(name.as_str(), partitions, &broker_ids, rf);
        let id = TopicId(self.next_topic_id.fetch_add(1, Ordering::SeqCst));

        let mut part_map = HashMap::new();
        for (i, replicas) in replica_sets.iter().enumerate() {
            let leader = replicas[0];
            part_map.insert(
                i as u32,
                PartitionAssignment {
                    replicas: replicas.clone(),
                    leader,
                    isr: replicas.clone(),
                    leader_epoch: 0,
                },
            );
        }

        {
            let mut asg = cluster.assignment.write();
            asg.generation = asg.generation.saturating_add(1);
            asg.topics.insert(
                name.as_str().to_owned(),
                TopicAssignment {
                    topic_id: id.0,
                    name: name.as_str().to_owned(),
                    partitions: part_map,
                },
            );
            save_assignment(&cluster.data_dir, &asg)?;
        }

        // Open local partitions.
        {
            let mut topics = self.topics.write();
            let topic = Topic::create_with_replicas(
                id,
                name.clone(),
                partitions,
                &self.storage,
                self.node_id,
                Some(&replica_sets),
                topic_cfg,
            )?;
            topics.insert(name.clone(), topic);
        }
        {
            let mut epochs = self.leader_epochs.write();
            for pid in 0..partitions {
                let key = (name.as_str().to_owned(), pid);
                let e = epochs.entry(key).or_default();
                ensure_entry(e, 0, 0);
            }
        }
        self.persist_leader_epochs();
        self.rr_counters
            .write()
            .insert(name.clone(), AtomicU64::new(0));
        self.topic_configs.save(name.as_str(), topic_cfg)?;
        Ok(id)
    }

    /// Delete a topic and remove its on-disk data directory.
    pub fn delete_topic(&self, name: &TopicName) -> Result<()> {
        let mut topics = self.topics.write();
        let removed = topics
            .remove(name)
            .ok_or_else(|| Error::NotFound(format!("topic {}", name.as_str())))?;
        drop(removed);
        self.rr_counters.write().remove(name);
        if let Some(cluster) = &self.cluster {
            let mut asg = cluster.assignment.write();
            asg.topics.remove(name.as_str());
            asg.generation = asg.generation.saturating_add(1);
            save_assignment(&cluster.data_dir, &asg)?;
        }
        let dir = self.storage.data_dir.join(name.as_str());
        if dir.exists() {
            fs::remove_dir_all(&dir).map_err(|e| {
                Error::Storage(format!(
                    "failed to remove topic dir {}: {e}",
                    dir.display()
                ))
            })?;
        }
        let _ = self.topic_configs.delete(name.as_str());
        drop(topics);
        if self.cluster.is_none() {
            self.persist_topic_catalog()?;
        }
        Ok(())
    }

    /// Increase a topic's partition count to `total_count` (Phase 15).
    ///
    /// `total_count` must be strictly greater than the current count.
    /// Single-node updates the durable catalog; multi-node requires controller.
    pub fn create_partitions(&self, topic: &str, total_count: u32) -> Result<u32> {
        if total_count == 0 {
            return Err(Error::InvalidArgument(
                "total partition count must be at least 1".into(),
            ));
        }
        let name = TopicName::new(topic);

        if let Some(cluster) = &self.cluster {
            if !cluster.membership.read().is_controller() {
                return Err(Error::InvalidArgument(format!(
                    "not controller; controller_id={}",
                    cluster.membership.read().controller_id()
                )));
            }
            return self.create_partitions_cluster(name, total_count);
        }

        let topic_cfg = self.topic_configs.load(topic).unwrap_or_default();
        let mut topics = self.topics.write();
        let t = topics
            .get_mut(&name)
            .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
        let current = t.partitions.len() as u32;
        if total_count <= current {
            return Err(Error::InvalidArgument(format!(
                "total_count {total_count} must be greater than current {current}"
            )));
        }
        t.add_partitions_from(
            current,
            total_count,
            &self.storage,
            self.node_id,
            None,
            &topic_cfg,
        )?;
        drop(topics);
        {
            let mut epochs = self.leader_epochs.write();
            for pid in current..total_count {
                let key = (name.as_str().to_owned(), pid);
                let e = epochs.entry(key).or_default();
                ensure_entry(e, 0, 0);
            }
        }
        self.persist_leader_epochs();
        self.persist_topic_catalog()?;
        Ok(total_count)
    }

    fn create_partitions_cluster(&self, name: TopicName, total_count: u32) -> Result<u32> {
        let cluster = self.cluster.as_ref().expect("cluster");
        let topic_cfg = self
            .topic_configs
            .load(name.as_str())
            .unwrap_or_default();

        let (current, topic_id, mut all_replica_sets) = {
            let asg = cluster.assignment.read();
            let ta = asg
                .topics
                .get(name.as_str())
                .ok_or_else(|| Error::NotFound(format!("topic {}", name.as_str())))?;
            let current = ta.partitions.len() as u32;
            if total_count <= current {
                return Err(Error::InvalidArgument(format!(
                    "total_count {total_count} must be greater than current {current}"
                )));
            }
            let mut sets: Vec<Vec<u32>> = Vec::with_capacity(total_count as usize);
            for i in 0..current {
                let pa = ta.partitions.get(&i).ok_or_else(|| {
                    Error::Storage(format!("missing partition assignment {i}"))
                })?;
                sets.push(pa.replicas.clone());
            }
            (current, ta.topic_id, sets)
        };

        let broker_ids = cluster.config.broker_ids();
        let rf = cluster
            .config
            .default_replication_factor
            .min(broker_ids.len() as u32)
            .max(1);

        let mut new_part_map = HashMap::new();
        for pid in current..total_count {
            // Distinct placement seed per partition id.
            let sets = assign_replicas(
                &format!("{}#{pid}", name.as_str()),
                1,
                &broker_ids,
                rf,
            );
            let replicas = sets.into_iter().next().unwrap_or_else(|| vec![self.node_id]);
            let leader = replicas.first().copied().unwrap_or(self.node_id);
            all_replica_sets.push(replicas.clone());
            new_part_map.insert(
                pid,
                PartitionAssignment {
                    isr: replicas.clone(),
                    replicas,
                    leader,
                    leader_epoch: 0,
                },
            );
        }

        {
            let mut asg = cluster.assignment.write();
            let ta = asg
                .topics
                .get_mut(name.as_str())
                .ok_or_else(|| Error::NotFound(format!("topic {}", name.as_str())))?;
            for (pid, pa) in new_part_map {
                ta.partitions.insert(pid, pa);
            }
            asg.generation = asg.generation.saturating_add(1);
            save_assignment(&cluster.data_dir, &asg)?;
        }

        {
            let mut topics = self.topics.write();
            let t = topics.entry(name.clone()).or_insert_with(|| Topic {
                id: TopicId(topic_id),
                name: name.clone(),
                partitions: HashMap::new(),
            });
            t.add_partitions_from(
                current,
                total_count,
                &self.storage,
                self.node_id,
                Some(&all_replica_sets),
                &topic_cfg,
            )?;
        }
        {
            let mut epochs = self.leader_epochs.write();
            for pid in current..total_count {
                let key = (name.as_str().to_owned(), pid);
                let e = epochs.entry(key).or_default();
                ensure_entry(e, 0, 0);
            }
        }
        self.persist_leader_epochs();
        Ok(total_count)
    }

    /// List earliest/latest offsets for topic partitions (Phase 15).
    ///
    /// Empty `partitions` means all known partitions. Returns
    /// `(partition, earliest, latest)` triples.
    pub fn list_offsets(
        &self,
        topic: &str,
        partitions: &[u32],
    ) -> Result<Vec<(u32, u64, u64)>> {
        let name = TopicName::new(topic);
        let topics = self.topics.read();

        let partition_ids: Vec<u32> = if !partitions.is_empty() {
            partitions.to_vec()
        } else if let Some(cluster) = &self.cluster {
            let asg = cluster.assignment.read();
            let ta = asg
                .topics
                .get(topic)
                .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
            let mut ids: Vec<u32> = ta.partitions.keys().copied().collect();
            ids.sort_unstable();
            ids
        } else {
            let t = topics
                .get(&name)
                .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
            let mut ids: Vec<u32> = t.partitions.keys().map(|p| p.0).collect();
            ids.sort_unstable();
            ids
        };

        // Ensure topic exists in single-node map when filters used.
        if self.cluster.is_none() && !topics.contains_key(&name) {
            return Err(Error::NotFound(format!("topic {topic}")));
        }
        if self.cluster.is_some() {
            let asg = self.cluster.as_ref().unwrap().assignment.read();
            if !asg.topics.contains_key(topic) {
                return Err(Error::NotFound(format!("topic {topic}")));
            }
        }

        let mut out = Vec::with_capacity(partition_ids.len());
        for pid in partition_ids {
            if let Some(t) = topics.get(&name) {
                if let Some(part) = t.partitions.get(&PartitionId(pid)) {
                    let earliest = part.log.log_start_offset().raw();
                    let latest = part.log.log_end_offset().raw();
                    out.push((pid, earliest, latest));
                    continue;
                }
            }
            // Known in assignment but no local log.
            out.push((pid, 0, 0));
        }
        Ok(out)
    }

    /// Offset of the record with the maximum timestamp in a partition (KIP-734).
    ///
    /// Scans the local log. Empty partition → `None`. Returns
    /// `(offset, max_timestamp_ms)`.
    pub fn max_timestamp_offset(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Option<(u64, i64)>> {
        let name = TopicName::new(topic);
        let topics = self.topics.read();
        let t = topics
            .get(&name)
            .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
        let part = t
            .partitions
            .get(&PartitionId(partition))
            .ok_or_else(|| {
                Error::NotFound(format!("partition {topic}/{partition}"))
            })?;
        let start = part.log.log_start_offset();
        let end = part.log.log_end_offset();
        if start.raw() >= end.raw() {
            return Ok(None);
        }
        // Chunked scan — max timestamp wins; ties keep the later offset.
        let mut best: Option<(u64, i64)> = None;
        let mut cursor = start;
        while cursor.raw() < end.raw() {
            let batch = part.log.read(cursor, 512)?;
            if batch.is_empty() {
                break;
            }
            for r in &batch {
                match best {
                    None => best = Some((r.offset.raw(), r.timestamp_ms)),
                    Some((_, ts)) if r.timestamp_ms >= ts => {
                        best = Some((r.offset.raw(), r.timestamp_ms));
                    }
                    _ => {}
                }
            }
            let next = batch.last().map(|r| r.offset.raw().saturating_add(1)).unwrap_or(end.raw());
            if next <= cursor.raw() {
                break;
            }
            cursor = Offset::new(next);
        }
        Ok(best)
    }

    /// Delete records before `before_offset` on a partition (Phase 14).
    ///
    /// Drops whole sealed segments only. Returns `(low_watermark, error_code)`.
    /// Leader-only in cluster mode.
    ///
    /// Phase 104/111: after a successful truncate, drop aborted soft markers
    /// fully below the new log start (`end_offset <= low_watermark`) and clip
    /// straddlers (`first_offset = log_start` when the range still overlaps
    /// live offsets); persist `__txn_markers` when any change occurs.
    ///
    /// Phase 113: this method only mutates the **local** log. Cluster fan-out to
    /// other replicas is best-effort via [`crate::net::fanout_delete_records`]
    /// (native + Kafka request handlers), not here — so in-process unit tests
    /// remain single-node.
    pub fn delete_records(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
    ) -> Result<(u64, u16)> {
        let name = TopicName::new(topic);
        let low = {
            let mut topics = self.topics.write();
            let t = topics
                .get_mut(&name)
                .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
            let part = t
                .partitions
                .get_mut(&PartitionId(partition))
                .ok_or_else(|| {
                    Error::NotFound(format!("partition {topic}/{partition}"))
                })?;
            if self.cluster.is_some() && !part.is_leader(self.node_id) {
                return Ok((0, ErrorCode::NotLeaderForPartition as u16));
            }
            part.log
                .delete_records(Offset::new(before_offset))?
                .raw()
        };
        // Phase 104/111: GC / clip soft markers vs new log start.
        self.gc_and_persist_aborted_markers(topic, partition, low);
        Ok((low, 0))
    }

    /// Reload topics from the durable single-node catalog (Phase 14).
    fn reload_single_node_topics(&self) -> Result<()> {
        if self.cluster.is_some() {
            return Ok(());
        }
        let catalog = self.topic_catalog.load()?;
        let mut topics = self.topics.write();
        for (name, meta) in &catalog.topics {
            if meta.partitions == 0 {
                continue;
            }
            let tname = TopicName::new(name.clone());
            if topics.contains_key(&tname) {
                continue;
            }
            let cfg = self.topic_configs.load(name).unwrap_or_default();
            let topic = Topic::create_with_config(
                TopicId(meta.id),
                tname.clone(),
                meta.partitions,
                &self.storage,
                &cfg,
            )?;
            topics.insert(tname.clone(), topic);
            self.rr_counters
                .write()
                .entry(tname)
                .or_insert_with(|| AtomicU64::new(0));
        }
        let next = catalog.next_id.max(1);
        let cur = self.next_topic_id.load(Ordering::SeqCst);
        if next > cur {
            self.next_topic_id.store(next, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Persist the single-node topic catalog from live topics (Phase 14).
    fn persist_topic_catalog(&self) -> Result<()> {
        if self.cluster.is_some() {
            return Ok(());
        }
        let topics = self.topics.read();
        let mut file = TopicCatalogFile {
            next_id: self.next_topic_id.load(Ordering::SeqCst),
            topics: HashMap::new(),
        };
        for (name, t) in topics.iter() {
            file.topics.insert(
                name.as_str().to_owned(),
                CatalogTopic {
                    id: t.id.0,
                    partitions: t.partitions.len() as u32,
                },
            );
        }
        self.topic_catalog.save(&file)
    }

    /// Describe topic configs (Phase 13).
    pub fn describe_configs(
        &self,
        topic: &str,
    ) -> Result<(u32, u32, TopicConfig)> {
        let name = TopicName::new(topic);
        let (topic_id, partition_count) = {
            if let Some(cluster) = &self.cluster {
                let asg = cluster.assignment.read();
                if let Some(t) = asg.topics.get(topic) {
                    (t.topic_id, t.partitions.len() as u32)
                } else {
                    return Err(Error::NotFound(format!("topic {topic}")));
                }
            } else {
                let topics = self.topics.read();
                let t = topics
                    .get(&name)
                    .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
                (t.id.0, t.partitions.len() as u32)
            }
        };
        let cfg = self.topic_configs.load(topic)?;
        Ok((topic_id, partition_count, cfg))
    }

    /// Alter topic configs and apply to live partitions (Phase 13).
    pub fn alter_configs(
        &self,
        topic: &str,
        entries: &[(String, String)],
    ) -> Result<TopicConfig> {
        let name = TopicName::new(topic);
        // Ensure topic exists.
        {
            if let Some(cluster) = &self.cluster {
                if !cluster.assignment.read().topics.contains_key(topic) {
                    return Err(Error::NotFound(format!("topic {topic}")));
                }
            } else if !self.topics.read().contains_key(&name) {
                return Err(Error::NotFound(format!("topic {topic}")));
            }
        }
        let mut cfg = self.topic_configs.load(topic)?;
        cfg.apply_entries(entries)?;
        self.topic_configs.save(topic, &cfg)?;
        {
            let mut topics = self.topics.write();
            if let Some(t) = topics.get_mut(&name) {
                t.apply_topic_config(&cfg);
            }
        }
        Ok(cfg)
    }

    /// Run retention on all local partition logs (Phase 13 background task).
    ///
    /// Phase 104/111: after retention advances log starts, drop/clip aborted
    /// soft markers vs each partition's new log start and persist when needed.
    pub fn apply_retention_all(&self) -> Result<()> {
        {
            let mut topics = self.topics.write();
            for t in topics.values_mut() {
                t.apply_retention_all()?;
            }
        }
        // Phase 104: same GC rule as DeleteRecords (end_offset <= log_start).
        let _ = self.gc_stale_aborted_markers_all();
        Ok(())
    }

    /// Force key-compaction on topics with `cleanup.policy=compact` (Phase 16).
    pub fn compact_all(&self) -> Result<()> {
        let mut topics = self.topics.write();
        for t in topics.values_mut() {
            t.compact_all()?;
        }
        Ok(())
    }

    /// Number of partitions for a topic.
    pub fn partition_count(&self, topic: &TopicName) -> Result<u32> {
        // Prefer assignment in cluster mode (may not have all partitions local).
        if let Some(cluster) = &self.cluster {
            let asg = cluster.assignment.read();
            if let Some(t) = asg.topics.get(topic.as_str()) {
                return Ok(t.partitions.len() as u32);
            }
        }
        let topics = self.topics.read();
        let t = topics
            .get(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        Ok(t.partitions.len() as u32)
    }

    /// Select a partition for produce when the client sends `partition = -1`.
    pub fn select_partition(
        &self,
        topic: &TopicName,
        key: Option<&[u8]>,
    ) -> Result<PartitionId> {
        let n = self.partition_count(topic)?;
        if n == 0 {
            return Err(Error::InvalidArgument("topic has zero partitions".into()));
        }
        let idx = match key {
            Some(k) => {
                let h = murmur2(k);
                let positive = h & 0x7fff_ffff;
                positive % n
            }
            None => {
                {
                    let mut counters = self.rr_counters.write();
                    counters
                        .entry(topic.clone())
                        .or_insert_with(|| AtomicU64::new(0));
                }
                let counters = self.rr_counters.read();
                let counter = counters
                    .get(topic)
                    .expect("rr counter inserted above");
                let seq = counter.fetch_add(1, Ordering::Relaxed);
                (seq % u64::from(n)) as u32
            }
        };
        Ok(PartitionId(idx))
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

    /// Build a metadata snapshot.
    pub fn metadata(&self, topics: Option<&[TopicName]>) -> MetadataSnapshot {
        let host = self.advertised_host.read().clone();
        let port = self.advertised_port.load(Ordering::Relaxed) as u16;

        let brokers = if let Some(cluster) = &self.cluster {
            cluster
                .config
                .brokers
                .iter()
                .map(|b| {
                    // Prefer live advertised for self.
                    if b.id == self.node_id {
                        (b.id, host.clone(), port)
                    } else {
                        (b.id, b.host.clone(), b.port)
                    }
                })
                .collect()
        } else {
            vec![(self.node_id, host.clone(), port)]
        };

        let controller_id = self.controller_id();

        // Build topic list from assignment (cluster) or local topics.
        let topic_meta = if let Some(cluster) = &self.cluster {
            let asg = cluster.assignment.read();
            let local = self.topics.read();
            let names: Vec<String> = match topics {
                None | Some([]) => asg.topics.keys().cloned().collect(),
                Some(list) => list.iter().map(|t| t.as_str().to_owned()).collect(),
            };
            let mut out = Vec::new();
            for name in names {
                if let Some(t) = asg.topics.get(&name) {
                    let mut partitions: Vec<PartitionMetadata> = t
                        .partitions
                        .iter()
                        .map(|(pid, p)| {
                            let hwm = local
                                .get(&TopicName::new(&name))
                                .and_then(|lt| lt.partitions.get(&PartitionId(*pid)))
                                .map(|lp| lp.committed_hwm)
                                .unwrap_or(0);
                            PartitionMetadata {
                                partition_id: PartitionId(*pid),
                                leader: p.leader,
                                hwm,
                                replicas: p.replicas.clone(),
                                isr: p.isr.clone(),
                                leader_epoch: p.leader_epoch,
                            }
                        })
                        .collect();
                    partitions.sort_by_key(|p| p.partition_id.0);
                    out.push(TopicMetadata {
                        name: TopicName::new(name),
                        topic_id: TopicId(t.topic_id),
                        partitions,
                    });
                }
            }
            out.sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));
            out
        } else {
            let map = self.topics.read();
            let names: Vec<TopicName> = match topics {
                None | Some([]) => map.keys().cloned().collect(),
                Some(list) => list.to_vec(),
            };
            let mut topic_meta = Vec::with_capacity(names.len());
            for name in names {
                if let Some(t) = map.get(&name) {
                    let mut partitions: Vec<PartitionMetadata> = t
                        .partitions
                        .iter()
                        .map(|(pid, p)| PartitionMetadata {
                            partition_id: *pid,
                            leader: p.leader,
                            hwm: p.committed_hwm.max(p.leo()), // single-node: LEO
                            replicas: p.replicas.clone(),
                            isr: p.isr.clone(),
                            leader_epoch: p.leader_epoch,
                        })
                        .collect();
                    partitions.sort_by_key(|p| p.partition_id.0);
                    topic_meta.push(TopicMetadata {
                        name,
                        topic_id: t.id,
                        partitions,
                    });
                }
            }
            topic_meta.sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));
            topic_meta
        };

        MetadataSnapshot {
            node_id: self.node_id,
            host,
            port,
            brokers,
            topics: topic_meta,
            controller_id,
        }
    }

    /// Access the group coordinator.
    pub fn groups(&self) -> &GroupCoordinator {
        &self.groups
    }

    /// Partition count lookup for group assignment (`None` if topic missing).
    pub fn partition_count_opt(&self, topic: &str) -> Option<u32> {
        let name = TopicName::new(topic);
        self.partition_count(&name).ok()
    }

    // ── Cluster protocol handlers ──────────────────────────────────────

    /// Handle ReplicaFetch from a follower (must be leader).
    pub fn handle_replica_fetch(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        max_bytes: u32,
        replica_id: u32,
    ) -> Result<(u16, u64, u32, Vec<FetchRecord>)> {
        let topic_name = TopicName::new(topic);
        let pid = PartitionId(partition);

        let (error, hwm, epoch, records) = {
            let mut topics = self.topics.write();
            let t = match topics.get_mut(&topic_name) {
                Some(t) => t,
                None => {
                    return Ok((ErrorCode::NotFound as u16, 0, 0, vec![]));
                }
            };
            let part = match t.partitions.get_mut(&pid) {
                Some(p) => p,
                None => return Ok((ErrorCode::NotFound as u16, 0, 0, vec![])),
            };
            if !part.is_leader(self.node_id) {
                return Ok((
                    ErrorCode::NotLeaderForPartition as u16,
                    part.committed_hwm,
                    part.leader_epoch,
                    vec![],
                ));
            }

            // Update follower LEO (they request from their current LEO).
            part.follower_leo.insert(replica_id, from_offset);

            // Phase 118/125: offset + time lag shrink + catch-up rejoin.
            if let Some(cluster) = &self.cluster {
                let max_lag = cluster.config.replica_lag_max_messages;
                let max_lag_ms = self.effective_replica_lag_max_ms();
                let leader_leo = part.leo();
                // Stamp last-caught-up when lag is within the message threshold.
                if leader_leo.saturating_sub(from_offset) <= max_lag {
                    part.follower_caught_up_at
                        .insert(replica_id, Instant::now());
                }
                let committed_hwm = part.committed_hwm;
                let leo_map = part.follower_leo.clone();
                let caught_map = part.follower_caught_up_at.clone();
                let replicas = part.replicas.clone();
                let old_isr = part.isr.clone();
                let now = Instant::now();
                let (isr, time_n) = reconcile_isr(
                    part.leader,
                    &old_isr,
                    &replicas,
                    leader_leo,
                    committed_hwm,
                    max_lag,
                    max_lag_ms,
                    now,
                    Some((replica_id, from_offset)),
                    |id| {
                        if id == part.leader {
                            leader_leo
                        } else {
                            *leo_map.get(&id).unwrap_or(&0)
                        }
                    },
                    |id| caught_map.get(&id).copied(),
                );
                if isr != old_isr {
                    self.note_isr_delta(&old_isr, &isr);
                    // Fresh stamp for any newly expanded members.
                    for &id in &isr {
                        if !old_isr.contains(&id) && id != part.leader {
                            part.follower_caught_up_at.insert(id, Instant::now());
                        }
                    }
                    // Drop stamps for members that left.
                    part.follower_caught_up_at
                        .retain(|id, _| isr.contains(id) || *id == part.leader);
                }
                self.note_isr_time_shrink(time_n);
                part.isr = isr;
                part.recompute_hwm(self.node_id);

                // Persist ISR change into assignment (generation bump so peers pull).
                {
                    let mut asg = cluster.assignment.write();
                    if let Some(ta) = asg.topics.get_mut(topic) {
                        if let Some(pa) = ta.partitions.get_mut(&partition) {
                            if pa.isr != part.isr {
                                pa.isr = part.isr.clone();
                                asg.generation = asg.generation.saturating_add(1);
                                let _ = save_assignment(&cluster.data_dir, &asg);
                            }
                        }
                    }
                }
            } else {
                part.catch_up_hwm();
            }

            let hwm = part.committed_hwm;
            let epoch = part.leader_epoch;
            let max_msgs = 10_000usize;
            let recs = part
                .log
                .read_bytes(Offset::new(from_offset), max_msgs, max_bytes as usize)?;
            let wire: Vec<FetchRecord> = recs
                .into_iter()
                .map(|r| FetchRecord {
                    offset: r.offset.raw(),
                    timestamp_ms: r.timestamp_ms,
                    key: r.key,
                    value: r.value,
                    headers: r.headers,
                })
                .collect();
            (0u16, hwm, epoch, wire)
        };

        // Wake acks=all waiters.
        self.hwm_cvar.notify_all();
        Ok((error, hwm, epoch, records))
    }

    /// Append records fetched from the leader onto a follower log.
    pub fn append_replica_records(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        records: &[Record],
        leader_epoch: u32,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut topics = self.topics.write();
        let t = topics
            .get_mut(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        if part.is_leader(self.node_id) {
            return Ok(()); // shouldn't happen
        }
        part.leader_epoch = leader_epoch;
        part.log.append_records_with_offsets(records)?;
        // Follower does not advance committed_hwm past what leader reported separately;
        // client fetch on follower is rare — typically clients go to leader.
        // Still advance committed_hwm conservatively to local LEO only when sole replica.
        if part.isr.len() <= 1 {
            part.catch_up_hwm();
        }
        Ok(())
    }

    /// Handle HeartbeatBroker (controller path).
    pub fn handle_heartbeat_broker(
        &self,
        broker_id: u32,
        _controller_id_known: u32,
        _generation: u32,
    ) -> (u16, u32, u32, Vec<u32>) {
        let Some(cluster) = &self.cluster else {
            return (0, self.node_id, 0, vec![self.node_id]);
        };
        {
            let mut m = cluster.membership.write();
            m.heartbeat(broker_id);
            m.touch_self();
        }
        // Expire dead brokers and handle failover if we are controller.
        let dead = cluster.membership.write().expire();
        if !dead.is_empty() && cluster.membership.read().is_controller() {
            for d in dead {
                let _ = self.on_broker_death(d);
            }
        }
        let m = cluster.membership.read();
        let controller_id = m.controller_id();
        let alive = m.live_brokers();
        let generation = cluster.assignment.read().generation;
        // Only the true controller should accept; others still respond with redirect info.
        (0, controller_id, generation, alive)
    }

    /// Build ClusterState response snapshot.
    pub fn cluster_state_snapshot(&self) -> (u16, u32, u32, Vec<ClusterTopicState>) {
        let Some(cluster) = &self.cluster else {
            return (0, 0, self.node_id, vec![]);
        };
        let asg = cluster.assignment.read();
        let controller_id = cluster.membership.read().controller_id();
        (
            0,
            asg.generation,
            controller_id,
            asg.to_wire_topics(),
        )
    }

    /// Apply a ClusterState snapshot from the controller.
    pub fn apply_cluster_state(
        &self,
        generation: u32,
        controller_id: u32,
        topics: &[ClusterTopicState],
    ) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        {
            let mut asg = cluster.assignment.write();
            if generation < asg.generation {
                return Ok(()); // stale
            }
            asg.apply_wire(generation, topics);
            save_assignment(&cluster.data_dir, &asg)?;
        }
        let _ = controller_id;
        self.apply_local_assignment()?;
        Ok(())
    }

    /// Open/update local partitions from current assignment.
    ///
    /// After overlaying leader/ISR from the assignment, recomputes HWM on
    /// partitions this node leads so ISR shrink (follower death) unblocks
    /// `acks=all` waiters when ClusterState is applied (Phase 108).
    ///
    /// Phase 118: when we lead, preserve previous local ISR members that are
    /// still in-sync (live, lag ≤ max, LEO ≥ HWM) so a controller assignment
    /// that still lists a death-shrunk set does not undo a leader-local rejoin.
    fn apply_local_assignment(&self) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        let asg = cluster.assignment.read().clone();
        let max_lag = cluster.config.replica_lag_max_messages;
        let max_lag_ms = self.effective_replica_lag_max_ms();
        let live: HashSet<u32> = cluster
            .membership
            .read()
            .live_brokers()
            .into_iter()
            .collect();
        let mut topics = self.topics.write();
        let mut hwm_changed = false;
        for (name, ta) in &asg.topics {
            let tname = TopicName::new(name.clone());
            let topic = topics.entry(tname.clone()).or_insert_with(|| Topic {
                id: TopicId(ta.topic_id),
                name: tname.clone(),
                partitions: HashMap::new(),
            });
            topic.id = TopicId(ta.topic_id);
            for (pid, pa) in &ta.partitions {
                // Snapshot leader-local ISR / LEO / caught-up before assignment overwrite.
                let prev = topic.partitions.get(&PartitionId(*pid)).map(|p| {
                    (
                        p.isr.clone(),
                        p.follower_leo.clone(),
                        p.follower_caught_up_at.clone(),
                        p.committed_hwm,
                    )
                });
                topic.ensure_partition(
                    PartitionId(*pid),
                    &self.storage,
                    self.node_id,
                    pa.leader,
                    pa.replicas.clone(),
                    pa.isr.clone(),
                    pa.leader_epoch,
                )?;
                if let Some(part) = topic.partitions.get_mut(&PartitionId(*pid)) {
                    if part.is_leader(self.node_id) {
                        let before = part.committed_hwm;
                        if let Some((prev_isr, prev_leo, prev_caught, prev_hwm)) = prev {
                            // Restore LEO / caught-up observations for candidates we may keep.
                            for (id, leo) in &prev_leo {
                                part.follower_leo.entry(*id).or_insert(*leo);
                            }
                            for (id, at) in &prev_caught {
                                part.follower_caught_up_at.entry(*id).or_insert(*at);
                            }
                            let leader_leo = part.leo();
                            let hwm = part.committed_hwm.max(prev_hwm);
                            let mut isr = part.isr.clone();
                            for &id in &prev_isr {
                                if isr.contains(&id) || id == part.leader {
                                    continue;
                                }
                                if !part.replicas.contains(&id) || !live.contains(&id) {
                                    continue;
                                }
                                let leo = *part.follower_leo.get(&id).unwrap_or(&0);
                                let lag = leader_leo.saturating_sub(leo);
                                if lag <= max_lag && leo >= hwm {
                                    isr.push(id);
                                }
                            }
                            let leo_map = part.follower_leo.clone();
                            let caught_map = part.follower_caught_up_at.clone();
                            let after_offset = shrink_isr(
                                part.leader,
                                &isr,
                                leader_leo,
                                max_lag,
                                |id| {
                                    if id == part.leader {
                                        leader_leo
                                    } else {
                                        *leo_map.get(&id).unwrap_or(&0)
                                    }
                                },
                            );
                            let now = Instant::now();
                            let reconciled = shrink_isr_by_time(
                                part.leader,
                                &after_offset,
                                max_lag_ms,
                                now,
                                |id| caught_map.get(&id).copied(),
                            );
                            let mut time_n = 0u64;
                            for &id in &after_offset {
                                if id != part.leader && !reconciled.contains(&id) {
                                    time_n += 1;
                                }
                            }
                            if reconciled != part.isr {
                                self.note_isr_delta(&part.isr, &reconciled);
                                part.isr = reconciled;
                            }
                            self.note_isr_time_shrink(time_n);
                        }
                        // Drop LEO / caught-up entries for brokers no longer in ISR.
                        part.follower_leo.retain(|id, _| part.isr.contains(id));
                        part.follower_caught_up_at
                            .retain(|id, _| part.isr.contains(id));
                        if part.isr.len() <= 1 {
                            part.catch_up_hwm();
                        } else {
                            part.recompute_hwm(self.node_id);
                        }
                        if part.committed_hwm != before {
                            hwm_changed = true;
                        }
                    }
                }
            }
            // Overlay durable topic config onto local partition logs.
            if let Ok(cfg) = self.topic_configs.load(name) {
                topic.apply_topic_config(&cfg);
            }
            self.rr_counters
                .write()
                .entry(tname)
                .or_insert_with(|| AtomicU64::new(0));
        }
        // Bump next_topic_id.
        let max_id = asg.topics.values().map(|t| t.topic_id).max().unwrap_or(0);
        let cur = self.next_topic_id.load(Ordering::SeqCst);
        if max_id + 1 > cur {
            self.next_topic_id.store(max_id + 1, Ordering::SeqCst);
        }
        drop(topics);
        if hwm_changed {
            self.hwm_cvar.notify_all();
        }
        Ok(())
    }

    /// Remove `dead_id` from every local partition ISR and advance HWM when we lead.
    ///
    /// Called from [`Self::on_broker_death`] on **every** node that observes the death
    /// (not only the controller). Without this, `acks=all` waits forever for a dead
    /// follower's LEO because HWM = min(ISR LEOs) still includes the stale member
    /// (Phase 108). Phase 118 also increments `isr_shrink_total` per removal.
    fn shrink_local_isr_for_dead(&self, dead_id: u32) {
        let mut topics = self.topics.write();
        let mut any = false;
        let mut shrink_n = 0u64;
        for t in topics.values_mut() {
            for part in t.partitions.values_mut() {
                let before = part.isr.len();
                part.isr.retain(|id| *id != dead_id);
                part.follower_leo.remove(&dead_id);
                part.follower_caught_up_at.remove(&dead_id);
                if part.isr.len() == before {
                    continue;
                }
                shrink_n += 1;
                any = true;
                if part.is_leader(self.node_id) {
                    if part.isr.len() <= 1 {
                        part.catch_up_hwm();
                    } else {
                        part.recompute_hwm(self.node_id);
                    }
                }
            }
        }
        drop(topics);
        if shrink_n > 0 {
            self.isr_shrink_total
                .fetch_add(shrink_n, Ordering::Relaxed);
        }
        if any {
            self.hwm_cvar.notify_all();
        }
    }

    /// Handle a dead broker: shrink ISR, elect new leaders from remaining ISR.
    ///
    /// Every observer removes the dead broker from **local** partition ISR and
    /// recomputes HWM (unblocks `acks=all`). The controller additionally updates
    /// the durable assignment (including pure ISR shrink — generation bump) so
    /// peers learn via ClusterState pull.
    pub fn on_broker_death(&self, dead_id: u32) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        // Mark dead first so controller_id recomputes (lowest remaining live id).
        cluster.membership.write().mark_dead(dead_id);
        // Local ISR shrink on every observer (leader may not be controller).
        self.shrink_local_isr_for_dead(dead_id);
        if !cluster.membership.read().is_controller() {
            return Ok(());
        }
        let live = cluster.membership.read().live_brokers();

        // Collect epoch transitions so we can record history with local LEO.
        let mut epoch_bumps: Vec<(String, u32, u32, u64)> = Vec::new();
        let mut changed = false;
        {
            let mut asg = cluster.assignment.write();
            for ta in asg.topics.values_mut() {
                for (pid, pa) in ta.partitions.iter_mut() {
                    // Shrink ISR; restore previous if no live member remains.
                    let isr_before = pa.isr.clone();
                    pa.isr.retain(|id| live.contains(id));
                    if pa.isr.is_empty() {
                        // No live ISR — keep last known, hope for recovery.
                        pa.isr = isr_before;
                        continue;
                    }
                    if pa.isr.len() != isr_before.len() {
                        // Pure follower death must bump generation (Phase 108).
                        changed = true;
                    }
                    if pa.leader == dead_id || !live.contains(&pa.leader) {
                        if let Some(new_leader) = elect_leader(&pa.replicas, &pa.isr, &live) {
                            if pa.leader != new_leader {
                                pa.leader = new_leader;
                                let new_epoch = pa.leader_epoch.saturating_add(1);
                                pa.leader_epoch = new_epoch;
                                if !pa.isr.contains(&new_leader) {
                                    pa.isr.push(new_leader);
                                }
                                let start = self
                                    .topics
                                    .read()
                                    .get(&TopicName::new(ta.name.as_str()))
                                    .and_then(|t| t.partitions.get(&PartitionId(*pid)))
                                    .map(|p| p.leo())
                                    .unwrap_or(0);
                                epoch_bumps.push((ta.name.clone(), *pid, new_epoch, start));
                                changed = true;
                            }
                        }
                    }
                }
            }
            if changed {
                asg.generation = asg.generation.saturating_add(1);
                save_assignment(&cluster.data_dir, &asg)?;
            }
        }
        for (topic, pid, new_epoch, start) in epoch_bumps {
            self.record_epoch_start(&topic, pid, new_epoch, start);
        }
        if changed {
            self.apply_local_assignment()?;
        }
        Ok(())
    }

    /// List (topic, partition, leader_id, local_leo) for partitions we follow.
    pub fn follower_targets(&self) -> Vec<(String, u32, u32, u64)> {
        let mut out = Vec::new();
        let topics = self.topics.read();
        for (name, t) in topics.iter() {
            for (pid, p) in &t.partitions {
                if p.is_replica(self.node_id) && !p.is_leader(self.node_id) {
                    out.push((
                        name.as_str().to_owned(),
                        pid.0,
                        p.leader,
                        p.leo(),
                    ));
                }
            }
        }
        out
    }

    /// Whether this node is leader for the partition.
    pub fn is_partition_leader(&self, topic: &TopicName, partition: PartitionId) -> bool {
        let topics = self.topics.read();
        topics
            .get(topic)
            .and_then(|t| t.partitions.get(&partition))
            .map(|p| p.is_leader(self.node_id))
            .unwrap_or(false)
    }

    /// Set the leader epoch for a partition (tests / controlled epoch bumps).
    ///
    /// Does not change the leader node id — only the epoch counter used for
    /// fencing (`FencedLeaderEpoch` / KIP-951 CurrentLeader).
    ///
    /// When `epoch` advances past the current value, records durable leader-epoch
    /// history with start offset = current LEO (Phase 87).
    pub fn set_partition_leader_epoch(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        epoch: u32,
    ) -> Result<()> {
        let (old_epoch, leo) = {
            let mut topics = self.topics.write();
            let t = topics
                .get_mut(topic)
                .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
            let part = t
                .partitions
                .get_mut(&partition)
                .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
            let old = part.leader_epoch;
            let leo = part.leo();
            part.leader_epoch = epoch;
            (old, leo)
        };
        // Ensure prior epoch is in history, then record the new epoch start.
        if epoch > old_epoch {
            self.ensure_epoch_entry(topic.as_str(), partition.0, old_epoch, 0);
            self.record_epoch_start(topic.as_str(), partition.0, epoch, leo);
        } else if epoch == old_epoch {
            self.ensure_epoch_entry(topic.as_str(), partition.0, epoch, 0);
        } else {
            // Epoch regression (unusual): keep history, just set live epoch.
            self.ensure_epoch_entry(topic.as_str(), partition.0, epoch, 0);
        }
        // Keep cluster assignment in sync when present.
        if let Some(cluster) = &self.cluster {
            let mut asg = cluster.assignment.write();
            if let Some(ta) = asg.topics.get_mut(topic.as_str()) {
                if let Some(pa) = ta.partitions.get_mut(&partition.0) {
                    pa.leader_epoch = epoch;
                }
            }
        }
        Ok(())
    }

    /// Resolve OffsetForLeaderEpoch end offset from durable history (Phase 87).
    ///
    /// Returns `(found_epoch, end_offset)` or `None` when the requested epoch is
    /// strictly greater than the current partition epoch.
    pub fn offset_for_leader_epoch(
        &self,
        topic: &str,
        partition: u32,
        requested_epoch: i32,
    ) -> Option<(i32, i64)> {
        let (current_epoch, hwm) = {
            let topics = self.topics.read();
            let t = topics.get(&TopicName::new(topic))?;
            let part = t.partitions.get(&PartitionId(partition))?;
            let hwm = if self.cluster.is_none() {
                part.committed_hwm.max(part.leo())
            } else {
                part.committed_hwm
            };
            (part.leader_epoch, hwm)
        };
        // Ensure at least epoch-0 seed so lookups work on pre-Phase-87 data dirs.
        self.ensure_epoch_entry(topic, partition, 0, 0);
        let epochs = self.leader_epochs.read();
        let entries = epochs
            .get(&(topic.to_owned(), partition))
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        end_offset_for(entries, current_epoch, hwm, requested_epoch)
    }

    fn load_leader_epochs(&self) {
        let Ok(file) = self.leader_epoch_store.load() else {
            return;
        };
        let mut map = self.leader_epochs.write();
        for (key, entries) in file.partitions {
            if let Some((topic, part)) = crate::producer_state::parse_partition_key(&key) {
                let mut sorted = entries;
                sorted.sort_by_key(|e| e.epoch);
                map.insert((topic, part), sorted);
            }
        }
    }

    fn persist_leader_epochs(&self) {
        let epochs = self.leader_epochs.read();
        let mut file = LeaderEpochsFile::default();
        for ((topic, part), entries) in epochs.iter() {
            file.partitions.insert(
                leader_epoch::partition_key(topic, *part),
                entries.clone(),
            );
        }
        let _ = self.leader_epoch_store.save(&file);
    }

    /// Seed epoch 0 @ 0 for any live partition missing history, and restore
    /// live `Partition.leader_epoch` from the highest stored history entry
    /// (single-node has no assignment file for epochs).
    fn seed_missing_leader_epochs(&self) {
        let mut dirty = false;
        {
            let mut topics = self.topics.write();
            let mut epochs = self.leader_epochs.write();
            for (name, t) in topics.iter_mut() {
                for (pid, part) in t.partitions.iter_mut() {
                    let key = (name.as_str().to_owned(), pid.0);
                    let e = epochs.entry(key).or_default();
                    if e.is_empty() {
                        ensure_entry(e, 0, 0);
                        dirty = true;
                    }
                    // Restore live epoch from durable history when history is ahead
                    // (e.g. single-node restart after set_partition_leader_epoch).
                    if let Some(max) = e.iter().map(|x| x.epoch).max() {
                        if max > part.leader_epoch {
                            part.leader_epoch = max;
                        } else if part.leader_epoch > max {
                            // Live epoch ahead of history (shouldn't happen after
                            // set_partition_leader_epoch) — ensure an entry.
                            ensure_entry(e, part.leader_epoch, part.leo());
                            dirty = true;
                        }
                    }
                }
            }
        }
        if dirty {
            self.persist_leader_epochs();
        }
    }

    fn ensure_epoch_entry(&self, topic: &str, partition: u32, epoch: u32, start_offset: u64) {
        let mut epochs = self.leader_epochs.write();
        let e = epochs
            .entry((topic.to_owned(), partition))
            .or_default();
        let before = e.len();
        ensure_entry(e, epoch, start_offset);
        let changed = e.len() != before;
        drop(epochs);
        if changed {
            self.persist_leader_epochs();
        }
    }

    fn record_epoch_start(&self, topic: &str, partition: u32, epoch: u32, start_offset: u64) {
        let mut epochs = self.leader_epochs.write();
        let e = epochs
            .entry((topic.to_owned(), partition))
            .or_default();
        // Always ensure prior epoch 0 exists for a continuous chain.
        ensure_entry(e, 0, 0);
        let before_len = e.len();
        let had = e.iter().any(|x| x.epoch == epoch);
        if !had {
            ensure_entry(e, epoch, start_offset);
        }
        let changed = e.len() != before_len || !had;
        drop(epochs);
        if changed {
            self.persist_leader_epochs();
        }
    }

    /// Whether a local partition log exists.
    pub fn topics_has_partition(&self, topic: &TopicName, partition: PartitionId) -> bool {
        let topics = self.topics.read();
        topics
            .get(topic)
            .map(|t| t.partitions.contains_key(&partition))
            .unwrap_or(false)
    }

    /// Whether `|ISR| >= min_isr` for the partition (true in single-node).
    pub fn isr_sufficient(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        min_isr: u32,
    ) -> bool {
        if self.cluster.is_none() {
            return true;
        }
        let topics = self.topics.read();
        topics
            .get(topic)
            .and_then(|t| t.partitions.get(&partition))
            .map(|p| (p.isr.len() as u32) >= min_isr)
            .unwrap_or(false)
    }

    /// Simulate marking a broker dead (for tests) and run failover if controller.
    pub fn test_kill_broker(&self, dead_id: u32) -> Result<()> {
        self.on_broker_death(dead_id)
    }

    /// Force-set follower LEO and recompute HWM (unit tests).
    ///
    /// Also stamps last-caught-up when lag ≤ `replica_lag_max_messages` so Phase
    /// 125 time-lag tests can control the clock via sleep after a fresh stamp.
    pub fn test_set_follower_leo(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        replica_id: u32,
        leo: u64,
    ) -> Result<()> {
        let mut topics = self.topics.write();
        let t = topics
            .get_mut(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        part.follower_leo.insert(replica_id, leo);
        let max_lag = self
            .cluster
            .as_ref()
            .map(|c| c.config.replica_lag_max_messages)
            .unwrap_or(u64::MAX);
        let leader_leo = part.leo();
        if leader_leo.saturating_sub(leo) <= max_lag {
            part.follower_caught_up_at
                .insert(replica_id, Instant::now());
        }
        part.recompute_hwm(self.node_id);
        self.hwm_cvar.notify_all();
        Ok(())
    }

    /// Force last-caught-up timestamp age for tests (Phase 125).
    ///
    /// Sets `follower_caught_up_at[replica_id] = now - age_ms`.
    pub fn test_set_follower_caught_up_age_ms(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        replica_id: u32,
        age_ms: u64,
    ) -> Result<()> {
        let mut topics = self.topics.write();
        let t = topics
            .get_mut(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        let at = Instant::now()
            .checked_sub(Duration::from_millis(age_ms))
            .unwrap_or_else(Instant::now);
        part.follower_caught_up_at.insert(replica_id, at);
        Ok(())
    }

    /// Expire sessions / membership (called periodically).
    ///
    /// Phase 110: **every** observer (not only the controller) runs
    /// [`Self::on_broker_death`] for newly expired peers so local ISR shrink +
    /// HWM recompute happen without waiting for a ClusterState pull. The
    /// controller path inside `on_broker_death` still owns durable assignment
    /// updates / generation bumps.
    pub fn tick_cluster(&self) {
        let Some(cluster) = &self.cluster else {
            return;
        };
        cluster.membership.write().touch_self();
        let dead = cluster.membership.write().expire();
        for d in dead {
            let _ = self.on_broker_death(d);
        }
    }

    /// Record a peer as live (e.g. after successful heartbeat response).
    pub fn note_peer_live(&self, peer_id: u32) {
        if let Some(cluster) = &self.cluster {
            cluster.membership.write().heartbeat(peer_id);
        }
    }

    /// Live broker ids from local membership (sorted). Empty when single-node.
    pub fn live_brokers(&self) -> Vec<u32> {
        match &self.cluster {
            None => vec![self.node_id],
            Some(c) => c.membership.read().live_brokers(),
        }
    }

    /// Local partition ISR (may differ from assignment until ClusterState pull).
    pub fn local_partition_isr(
        &self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Result<Vec<u32>> {
        let topics = self.topics.read();
        let t = topics
            .get(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        Ok(part.isr.clone())
    }

    /// Reconcile local membership against the controller's `alive_brokers`
    /// set from a HeartbeatBroker response (Phase 110).
    ///
    /// Brokers previously considered live but **missing** from `alive` are
    /// treated as dead via [`Self::on_broker_death`] (local ISR shrink + HWM
    /// on every observer; durable assignment only if this node is controller).
    /// Peers listed in `alive` are marked live.
    ///
    /// Non-controllers call this on every successful controller heartbeat so
    /// they do not wait for a generation-bumped ClusterState pull to drop a
    /// dead follower from local ISR (unblocks `acks=all`).
    pub fn apply_controller_alive_set(&self, alive: &[u32]) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        let alive_set: std::collections::HashSet<u32> =
            alive.iter().copied().collect();
        let prev_live = cluster.membership.read().live_brokers();
        let missing: Vec<u32> = prev_live
            .into_iter()
            .filter(|id| *id != self.node_id && !alive_set.contains(id))
            .collect();
        for dead_id in missing {
            self.on_broker_death(dead_id)?;
        }
        {
            let mut m = cluster.membership.write();
            for &id in alive {
                m.heartbeat(id);
            }
            m.touch_self();
        }
        Ok(())
    }

    /// Shared cluster state for background tasks.
    pub fn cluster_state(&self) -> Option<Arc<ClusterState>> {
        self.cluster.clone()
    }
}

/// Load durable producer maps from disk (defaults if missing).
fn load_producer_maps(
    store: &ProducerStateStore,
) -> (
    u64,
    HashMap<u64, ProducerEpochState>,
    HashMap<String, u64>,
) {
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
        assert_eq!(g, sticky_coordinator_id(b"txn-override", &[1, 2, 3], &[1, 2, 3]).unwrap());

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
