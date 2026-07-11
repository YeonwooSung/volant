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
use crate::topic::Topic;

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
}

impl Broker {
    /// Create a single-node broker with the given storage configuration.
    pub fn new(storage: StorageConfig) -> Self {
        let groups = GroupCoordinator::new(&storage.data_dir)
            .expect("failed to initialize group coordinator / offset store");
        Self {
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
        }
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
        let name = name.into();
        if partitions == 0 {
            return Err(Error::InvalidArgument(
                "topic must have at least one partition".into(),
            ));
        }

        if let Some(cluster) = &self.cluster {
            if !cluster.membership.read().is_controller() {
                return Err(Error::InvalidArgument(format!(
                    "not controller; controller_id={}",
                    cluster.membership.read().controller_id()
                )));
            }
            return self.create_topic_cluster(name, partitions);
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
        let topic = Topic::create(id, name.clone(), partitions, &self.storage)?;
        topics.insert(name.clone(), topic);
        self.rr_counters
            .write()
            .insert(name, AtomicU64::new(0));
        Ok(id)
    }

    fn create_topic_cluster(&self, name: TopicName, partitions: u32) -> Result<TopicId> {
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
            )?;
            topics.insert(name.clone(), topic);
        }
        self.rr_counters
            .write()
            .insert(name, AtomicU64::new(0));
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
