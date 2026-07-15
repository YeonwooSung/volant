//! Topic metadata and partition set.

use std::collections::HashMap;

use volant_core::{PartitionId, TopicId, TopicName};
use volant_storage::{PartitionLog, StorageConfig};

use crate::partition::Partition;
use crate::topic_config::TopicConfig;

/// In-memory topic metadata and live partitions.
#[derive(Debug)]
pub struct Topic {
    /// Stable topic id.
    pub id: TopicId,
    /// Topic name.
    pub name: TopicName,
    /// Partition map.
    pub partitions: HashMap<PartitionId, Partition>,
}

impl Topic {
    /// Create a topic with `num_partitions` empty partition logs (single-node).
    pub fn create(
        id: TopicId,
        name: TopicName,
        num_partitions: u32,
        storage: &StorageConfig,
    ) -> volant_core::Result<Self> {
        Self::create_with_config(id, name, num_partitions, storage, &TopicConfig::default())
    }

    /// Create a topic applying per-topic config (Phase 13).
    pub fn create_with_config(
        id: TopicId,
        name: TopicName,
        num_partitions: u32,
        storage: &StorageConfig,
        topic_cfg: &TopicConfig,
    ) -> volant_core::Result<Self> {
        Self::create_with_replicas(id, name, num_partitions, storage, 0, None, topic_cfg)
    }

    /// Create a topic; when `replica_sets` is `Some`, each partition gets that replica list.
    ///
    /// `replica_sets[i]` is the replica list for partition `i`. Local log is opened only
    /// when `node_id` is in that partition's replicas (always true for single-node).
    pub fn create_with_replicas(
        id: TopicId,
        name: TopicName,
        num_partitions: u32,
        storage: &StorageConfig,
        node_id: u32,
        replica_sets: Option<&[Vec<u32>]>,
        topic_cfg: &TopicConfig,
    ) -> volant_core::Result<Self> {
        let mut partitions = HashMap::new();
        for i in 0..num_partitions {
            let pid = PartitionId(i);
            let replicas = match replica_sets {
                Some(sets) => sets
                    .get(i as usize)
                    .cloned()
                    .unwrap_or_else(|| vec![node_id]),
                None => vec![node_id],
            };
            if !replicas.contains(&node_id) {
                // Not a replica for this partition — skip local log.
                continue;
            }
            let mut cfg = storage.clone();
            cfg.data_dir = storage
                .data_dir
                .join(name.as_str())
                .join(format!("{i}"));
            apply_topic_config_to_storage(&mut cfg, topic_cfg);
            let log = PartitionLog::open(cfg)?;
            let leader = replicas.first().copied().unwrap_or(node_id);
            let mut part = Partition::new_single(pid, log, node_id);
            part.leader = leader;
            part.replicas = replicas.clone();
            part.isr = replicas;
            part.leader_epoch = 0;
            part.committed_hwm = part.leo();
            partitions.insert(pid, part);
        }
        Ok(Self {
            id,
            name,
            partitions,
        })
    }

    /// Ensure a local partition log exists for the given assignment (cluster apply).
    pub fn ensure_partition(
        &mut self,
        pid: PartitionId,
        storage: &StorageConfig,
        node_id: u32,
        leader: u32,
        replicas: Vec<u32>,
        isr: Vec<u32>,
        leader_epoch: u32,
    ) -> volant_core::Result<()> {
        if !replicas.contains(&node_id) {
            // We are not a replica — drop if present.
            self.partitions.remove(&pid);
            return Ok(());
        }
        if let Some(p) = self.partitions.get_mut(&pid) {
            p.leader = leader;
            p.replicas = replicas;
            p.isr = isr;
            p.leader_epoch = leader_epoch;
            return Ok(());
        }
        let mut cfg = storage.clone();
        cfg.data_dir = storage
            .data_dir
            .join(self.name.as_str())
            .join(format!("{}", pid.0));
        // Topic config applied by caller via storage overrides when needed.
        let log = PartitionLog::open(cfg)?;
        let mut part = Partition::new_single(pid, log, node_id);
        part.leader = leader;
        part.replicas = replicas;
        part.isr = isr;
        part.leader_epoch = leader_epoch;
        part.committed_hwm = part.leo();
        self.partitions.insert(pid, part);
        Ok(())
    }

    /// Open partitions in `[from_id, new_total)` on this node (Phase 15).
    ///
    /// `replica_sets[i]` is the replica list for partition `i` (full topic set).
    /// When `replica_sets` is `None` (single-node), every new partition is local.
    pub fn add_partitions_from(
        &mut self,
        from_id: u32,
        new_total: u32,
        storage: &StorageConfig,
        node_id: u32,
        replica_sets: Option<&[Vec<u32>]>,
        topic_cfg: &TopicConfig,
    ) -> volant_core::Result<()> {
        if new_total <= from_id {
            return Ok(());
        }
        for i in from_id..new_total {
            let pid = PartitionId(i);
            if self.partitions.contains_key(&pid) {
                continue;
            }
            let replicas = match replica_sets {
                Some(sets) => sets
                    .get(i as usize)
                    .cloned()
                    .unwrap_or_else(|| vec![node_id]),
                None => vec![node_id],
            };
            if !replicas.contains(&node_id) {
                continue;
            }
            let mut cfg = storage.clone();
            cfg.data_dir = storage
                .data_dir
                .join(self.name.as_str())
                .join(format!("{i}"));
            apply_topic_config_to_storage(&mut cfg, topic_cfg);
            let log = PartitionLog::open(cfg)?;
            let leader = replicas.first().copied().unwrap_or(node_id);
            let mut part = Partition::new_single(pid, log, node_id);
            part.leader = leader;
            part.replicas = replicas.clone();
            part.isr = replicas;
            part.leader_epoch = 0;
            part.committed_hwm = part.leo();
            self.partitions.insert(pid, part);
        }
        Ok(())
    }

    /// Apply retention/segment settings to all local partitions (Phase 13).
    pub fn apply_topic_config(&mut self, topic_cfg: &TopicConfig) {
        for part in self.partitions.values_mut() {
            part.log
                .set_retention(topic_cfg.retention_ms, topic_cfg.retention_bytes);
            if let Some(seg) = topic_cfg.segment_bytes {
                part.log.set_segment_size(seg);
            }
        }
    }

    /// Run retention on all local partitions.
    pub fn apply_retention_all(&mut self) -> volant_core::Result<()> {
        for part in self.partitions.values_mut() {
            part.log.apply_retention()?;
        }
        Ok(())
    }
}

/// Overlay topic config onto a storage config used to open a partition log.
pub fn apply_topic_config_to_storage(cfg: &mut StorageConfig, topic_cfg: &TopicConfig) {
    if let Some(ms) = topic_cfg.retention_ms {
        cfg.retention_ms = Some(ms);
    }
    if let Some(bytes) = topic_cfg.retention_bytes {
        cfg.retention_bytes = Some(bytes);
    }
    if let Some(seg) = topic_cfg.segment_bytes {
        if seg > 0 {
            cfg.segment_size = seg;
        }
    }
}
