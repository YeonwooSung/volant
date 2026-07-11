//! Topic metadata and partition set.

use std::collections::HashMap;

use volant_core::{PartitionId, TopicId, TopicName};
use volant_storage::{PartitionLog, StorageConfig};

use crate::partition::Partition;

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
        Self::create_with_replicas(id, name, num_partitions, storage, 0, None)
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
}
