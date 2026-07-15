//! Broker state machine (single-node and multi-node cluster).
//!
//! # Batch produce coalescing
//!
//! [`Broker::produce`] accepts a [`MessageBatch`] and treats the whole batch as
//! one critical section under the topics write lock.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex, RwLock};
use volant_core::{
    Error, Message, MessageBatch, Offset, PartitionId, Record, Result, TopicId, TopicName,
};
use volant_protocol::{ClusterTopicState, ErrorCode, FetchRecord};
use volant_storage::StorageConfig;

use crate::cluster::{
    assign_replicas, elect_leader, load_assignment, save_assignment, shrink_isr, AssignmentSnapshot,
    ClusterConfig, Membership, PartitionAssignment, TopicAssignment,
};
use crate::group::GroupCoordinator;
use crate::metrics::Metrics;
use crate::producer_state::{
    partition_key, parse_partition_key, ProducerStateFile, ProducerStateStore, StoredBatch,
    StoredProducer,
};
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

/// In-memory state for one producer id (Phase 10/18).
#[derive(Debug)]
struct ProducerEpochState {
    epoch: u16,
    /// True when allocated with a non-empty transactional id (Phase 18).
    transactional: bool,
    /// Transactional id (empty if not transactional).
    transactional_id: String,
    /// Per (topic, partition) last **committed** batch.
    partitions: HashMap<(String, u32), IdempotentBatchState>,
}

/// One buffered produce batch inside an open transaction (Phase 18).
#[derive(Debug, Clone)]
struct TxnBufferedBatch {
    topic: String,
    partition: u32,
    messages: Vec<Message>,
    base_sequence: i32,
}

/// Open transaction state (memory-only; crash ≡ abort).
#[derive(Debug, Default)]
struct OpenTxn {
    batches: Vec<TxnBufferedBatch>,
    /// Sequences accepted inside this txn (not yet committed to `partitions`).
    pending: HashMap<(String, u32), IdempotentBatchState>,
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
    /// Proceed with append; record state after success.
    Accept,
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
    /// Open transactions keyed by producer_id (Phase 18; memory-only).
    open_txns: Mutex<HashMap<u64, OpenTxn>>,
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
            producer_store,
            topic_configs,
            topic_catalog,
            acls,
            metrics_token: RwLock::new(None),
        };
        broker
            .reload_single_node_topics()
            .expect("failed to reload single-node topic catalog");
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
            producer_store,
            topic_configs,
            topic_catalog,
            acls,
            metrics_token: RwLock::new(None),
        };
        // Open local partitions from persisted assignment.
        broker.apply_local_assignment()?;
        Ok(broker)
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
        self.init_producer_id_with_txn("")
    }

    /// Allocate (or fence) a producer id, optionally transactional (Phase 18).
    ///
    /// Non-empty `transactional_id` fences any prior owner of that id by bumping
    /// epoch and clearing open transactions / sequences.
    pub fn init_producer_id_with_txn(&self, transactional_id: &str) -> (u64, u16) {
        if !transactional_id.is_empty() {
            let mut txn_ids = self.transactional_ids.write();
            if let Some(&existing) = txn_ids.get(transactional_id) {
                let mut state = self.producer_state.write();
                if let Some(prod) = state.get_mut(&existing) {
                    prod.epoch = prod.epoch.wrapping_add(1);
                    if prod.epoch == 0 {
                        prod.epoch = 1;
                    }
                    prod.partitions.clear();
                    prod.transactional = true;
                    prod.transactional_id = transactional_id.to_owned();
                    let epoch = prod.epoch;
                    drop(state);
                    self.open_txns.lock().remove(&existing);
                    let _ = self.persist_producer_state();
                    return (existing, epoch);
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
                    partitions: HashMap::new(),
                },
            );
            txn_ids.insert(transactional_id.to_owned(), id);
            drop(txn_ids);
            let _ = self.persist_producer_state();
            return (id, epoch);
        }

        let id = self.next_producer_id.fetch_add(1, Ordering::Relaxed);
        let epoch = 0u16;
        self.producer_state.write().insert(
            id,
            ProducerEpochState {
                epoch,
                transactional: false,
                transactional_id: String::new(),
                partitions: HashMap::new(),
            },
        );
        let _ = self.persist_producer_state();
        (id, epoch)
    }

    /// Begin a transaction for a transactional producer (Phase 18).
    ///
    /// Returns protocol error code (`0` = ok).
    pub fn begin_txn(&self, producer_id: u64, producer_epoch: u16) -> u16 {
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
        drop(state);
        let mut open = self.open_txns.lock();
        if open.contains_key(&producer_id) {
            return ErrorCode::InvalidTxnState as u16;
        }
        open.insert(producer_id, OpenTxn::default());
        0
    }

    /// Whether this producer currently has an open transaction.
    pub fn has_open_txn(&self, producer_id: u64) -> bool {
        self.open_txns.lock().contains_key(&producer_id)
    }

    /// Whether the producer id is transactional (Phase 18).
    pub fn is_transactional_producer(&self, producer_id: u64) -> bool {
        self.producer_state
            .read()
            .get(&producer_id)
            .map(|p| p.transactional)
            .unwrap_or(false)
    }

    /// Buffer a produce batch inside an open transaction (Phase 18).
    ///
    /// On success returns [`IdempotentCheck::Accept`] or `Duplicate` (base_offset 0).
    /// Does not append to the log.
    pub fn buffer_txn_produce(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        topic: &str,
        partition: u32,
        base_sequence: i32,
        messages: Vec<Message>,
    ) -> IdempotentCheck {
        let message_count = messages.len() as u32;
        if message_count == 0 {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidArg as u16,
            };
        }
        {
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
        }
        let mut open = self.open_txns.lock();
        let Some(txn) = open.get_mut(&producer_id) else {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidTxnState as u16,
            };
        };
        let key = (topic.to_owned(), partition);
        // Sequence vs last committed or last pending in this txn.
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
                        base_offset: 0,
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
        txn.batches.push(TxnBufferedBatch {
            topic: topic.to_owned(),
            partition,
            messages,
            base_sequence,
        });
        txn.pending.insert(
            key,
            IdempotentBatchState {
                base_sequence,
                count: message_count,
                base_offset: 0,
            },
        );
        IdempotentCheck::Accept
    }

    /// Commit or abort an open transaction (Phase 18).
    ///
    /// On commit, flushes buffered batches to the log and applies deferred offsets.
    /// `offsets` entries are `(group_id, topic, partition, offset, metadata)`.
    pub fn end_txn(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        committed: bool,
        offsets: &[(String, String, u32, u64, String)],
    ) -> Result<(u16, Vec<TxnCommitResult>)> {
        {
            let state = self.producer_state.read();
            let Some(prod) = state.get(&producer_id) else {
                return Ok((ErrorCode::UnknownProducerId as u16, Vec::new()));
            };
            if prod.epoch != producer_epoch {
                return Ok((ErrorCode::InvalidProducerEpoch as u16, Vec::new()));
            }
        }
        let txn = {
            let mut open = self.open_txns.lock();
            match open.remove(&producer_id) {
                Some(t) => t,
                None => return Ok((ErrorCode::InvalidTxnState as u16, Vec::new())),
            }
        };

        if !committed {
            // Abort: drop buffer; sequences stay at last committed.
            return Ok((0, Vec::new()));
        }

        let mut results = Vec::with_capacity(txn.batches.len());
        for batch in txn.batches {
            let topic = TopicName::new(batch.topic.clone());
            let pid = PartitionId(batch.partition);
            let mut mb = MessageBatch::default();
            mb.messages = batch.messages;
            let (records, error_code) = self.produce_with_acks(&topic, pid, mb, 1, None)?;
            if error_code != 0 {
                // Best-effort: already-flushed batches stay; remaining dropped.
                // Return error; client should fence/retry with new epoch.
                return Ok((error_code, results));
            }
            let base_offset = records.first().map(|r| r.offset.raw()).unwrap_or(0);
            let count = records.len() as u32;
            self.record_idempotent_produce(
                producer_id,
                producer_epoch,
                &batch.topic,
                batch.partition,
                batch.base_sequence,
                count,
                base_offset,
            );
            let _ = self.flush(&topic, pid);
            results.push(TxnCommitResult {
                topic: batch.topic,
                partition: batch.partition,
                base_offset,
                count,
            });
        }

        for (group_id, topic, partition, offset, metadata) in offsets {
            let _ = self.groups().commit_offsets(
                group_id,
                "",
                0,
                &[(topic.clone(), *partition, *offset, metadata.clone())],
            );
        }

        Ok((0, results))
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
            return IdempotentCheck::Accept;
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
            None => IdempotentCheck::Accept,
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
                        IdempotentCheck::Accept
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

    /// Delete records before `before_offset` on a partition (Phase 14).
    ///
    /// Drops whole sealed segments only. Returns `(low_watermark, error_code)`.
    /// Leader-only in cluster mode; followers are not notified.
    pub fn delete_records(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
    ) -> Result<(u64, u16)> {
        let name = TopicName::new(topic);
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
        let low = part
            .log
            .delete_records(Offset::new(before_offset))?;
        Ok((low.raw(), 0))
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
    pub fn apply_retention_all(&self) -> Result<()> {
        let mut topics = self.topics.write();
        for t in topics.values_mut() {
            t.apply_retention_all()?;
        }
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
    pub fn fetch(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max_messages: usize,
    ) -> Result<Vec<Record>> {
        self.fetch_up_to(topic, partition, from, max_messages, true)
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
    ) -> Result<Vec<Record>> {
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

            // ISR shrink / grow based on lag.
            if let Some(cluster) = &self.cluster {
                let max_lag = cluster.config.replica_lag_max_messages;
                let leader_leo = part.leo();
                let leo_map = part.follower_leo.clone();
                let new_isr = shrink_isr(part.leader, &part.isr, leader_leo, max_lag, |id| {
                    if id == part.leader {
                        leader_leo
                    } else {
                        *leo_map.get(&id).unwrap_or(&0)
                    }
                });
                // Re-add follower if caught up and in replicas.
                let mut isr = new_isr;
                if part.replicas.contains(&replica_id) && !isr.contains(&replica_id) {
                    let lag = leader_leo.saturating_sub(from_offset);
                    if lag <= max_lag {
                        isr.push(replica_id);
                    }
                }
                // Ensure leader is in ISR.
                if !isr.contains(&part.leader) {
                    isr.insert(0, part.leader);
                }
                part.isr = isr;
                part.recompute_hwm(self.node_id);

                // Persist ISR change into assignment.
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
    fn apply_local_assignment(&self) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        let asg = cluster.assignment.read().clone();
        let mut topics = self.topics.write();
        for (name, ta) in &asg.topics {
            let tname = TopicName::new(name.clone());
            let topic = topics.entry(tname.clone()).or_insert_with(|| Topic {
                id: TopicId(ta.topic_id),
                name: tname.clone(),
                partitions: HashMap::new(),
            });
            topic.id = TopicId(ta.topic_id);
            for (pid, pa) in &ta.partitions {
                topic.ensure_partition(
                    PartitionId(*pid),
                    &self.storage,
                    self.node_id,
                    pa.leader,
                    pa.replicas.clone(),
                    pa.isr.clone(),
                    pa.leader_epoch,
                )?;
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
        Ok(())
    }

    /// Handle a dead broker: elect new leaders from ISR.
    pub fn on_broker_death(&self, dead_id: u32) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        // Mark dead first so controller_id recomputes (lowest remaining live id).
        cluster.membership.write().mark_dead(dead_id);
        if !cluster.membership.read().is_controller() {
            return Ok(());
        }
        let live = cluster.membership.read().live_brokers();

        let mut changed = false;
        {
            let mut asg = cluster.assignment.write();
            for ta in asg.topics.values_mut() {
                for pa in ta.partitions.values_mut() {
                    // Shrink ISR.
                    pa.isr.retain(|id| live.contains(id));
                    if pa.isr.is_empty() {
                        // No live ISR — keep last known, hope for recovery.
                        continue;
                    }
                    if pa.leader == dead_id || !live.contains(&pa.leader) {
                        if let Some(new_leader) = elect_leader(&pa.replicas, &pa.isr, &live) {
                            pa.leader = new_leader;
                            pa.leader_epoch = pa.leader_epoch.saturating_add(1);
                            if !pa.isr.contains(&new_leader) {
                                pa.isr.push(new_leader);
                            }
                            changed = true;
                        }
                    }
                }
            }
            if changed {
                asg.generation = asg.generation.saturating_add(1);
                save_assignment(&cluster.data_dir, &asg)?;
            }
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
        part.recompute_hwm(self.node_id);
        self.hwm_cvar.notify_all();
        Ok(())
    }

    /// Expire sessions / membership (called periodically).
    pub fn tick_cluster(&self) {
        let Some(cluster) = &self.cluster else {
            return;
        };
        cluster.membership.write().touch_self();
        let dead = cluster.membership.write().expire();
        if cluster.membership.read().is_controller() {
            for d in dead {
                let _ = self.on_broker_death(d);
            }
        }
    }

    /// Record a peer as live (e.g. after successful heartbeat response).
    pub fn note_peer_live(&self, peer_id: u32) {
        if let Some(cluster) = &self.cluster {
            cluster.membership.write().heartbeat(peer_id);
        }
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
                partitions,
            },
        );
    }
    (next, map, txn_ids)
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
