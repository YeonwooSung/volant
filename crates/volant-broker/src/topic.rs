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
    /// Create a topic with `num_partitions` empty partition logs.
    pub fn create(
        id: TopicId,
        name: TopicName,
        num_partitions: u32,
        storage: &StorageConfig,
    ) -> volant_core::Result<Self> {
        let mut partitions = HashMap::new();
        for i in 0..num_partitions {
            let pid = PartitionId(i);
            let mut cfg = storage.clone();
            cfg.data_dir = storage
                .data_dir
                .join(name.as_str())
                .join(format!("{i}"));
            let log = PartitionLog::open(cfg)?;
            partitions.insert(pid, Partition { id: pid, log });
        }
        Ok(Self {
            id,
            name,
            partitions,
        })
    }
}
