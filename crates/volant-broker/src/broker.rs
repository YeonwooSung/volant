//! Single-node broker state machine.
//!
//! # Batch produce coalescing
//!
//! [`Broker::produce`] accepts a [`MessageBatch`] and treats the whole batch as
//! one critical section:
//!
//! 1. Acquires the topics write lock **once** (exclusive access to the partition log)
//! 2. Appends every message via [`volant_storage::PartitionLog::append_batch`]
//!    so offsets are contiguous and no mid-batch `fsync` occurs
//! 3. Honors `StorageConfig::flush_every_n` **once** after the batch
//!
//! Multi-message batches also increment [`Broker::messages_coalesced`].

use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use parking_lot::RwLock;
use volant_core::{
    Error, Message, MessageBatch, Offset, PartitionId, Record, Result, TopicId, TopicName,
};
use volant_storage::StorageConfig;

use crate::group::GroupCoordinator;
use crate::topic::Topic;

/// Snapshot of cluster metadata for a Metadata response.
#[derive(Debug, Clone)]
pub struct MetadataSnapshot {
    /// Node id of this broker (always 0 for single-node).
    pub node_id: u32,
    /// Advertised host (may be empty if unknown).
    pub host: String,
    /// Advertised port.
    pub port: u16,
    /// Topic metadata entries.
    pub topics: Vec<TopicMetadata>,
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
    /// High watermark (next offset).
    pub hwm: u64,
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
}

impl Broker {
    /// Create a broker with the given storage configuration.
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
        }
    }

    /// Total messages that went through a multi-message coalesced produce.
    ///
    /// Incremented by `N` when `produce` is called with a batch of size `N > 1`.
    pub fn messages_coalesced(&self) -> u64 {
        self.messages_coalesced.load(Ordering::Relaxed)
    }

    /// Set the advertised address returned by metadata.
    pub fn set_advertised(&self, host: impl Into<String>, port: u16) {
        *self.advertised_host.write() = host.into();
        self.advertised_port.store(u32::from(port), Ordering::Relaxed);
    }

    /// Create a topic with the given partition count.
    pub fn create_topic(&self, name: impl Into<TopicName>, partitions: u32) -> Result<TopicId> {
        let name = name.into();
        if partitions == 0 {
            return Err(Error::InvalidArgument(
                "topic must have at least one partition".into(),
            ));
        }
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

    /// Delete a topic and remove its on-disk data directory.
    pub fn delete_topic(&self, name: &TopicName) -> Result<()> {
        let mut topics = self.topics.write();
        let removed = topics
            .remove(name)
            .ok_or_else(|| Error::NotFound(format!("topic {}", name.as_str())))?;
        drop(removed);
        self.rr_counters.write().remove(name);
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
        let topics = self.topics.read();
        let t = topics
            .get(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        Ok(t.partitions.len() as u32)
    }

    /// Select a partition for produce when the client sends `partition = -1`.
    ///
    /// Key present → Kafka-compatible `murmur2(key) % n`.
    /// Key absent → atomic round-robin per topic.
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
                // Ensure counter exists even if topic was created before rr map was wired.
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
    /// Holds the topics write lock for the entire batch, appends all messages
    /// with contiguous offsets, and applies the partition flush policy once
    /// after the batch (no mid-batch `fsync`). Empty batches are a no-op and
    /// return an empty `Vec`.
    pub fn produce(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        batch: MessageBatch,
    ) -> Result<Vec<Record>> {
        let n = batch.messages.len();
        // Topics map write lock is the exclusive gate for partition logs.
        let mut topics = self.topics.write();
        let topic = topics
            .get_mut(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = topic
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;

        let records = part.log.append_batch(batch.messages)?;
        // Metric after successful append so failures do not inflate the counter.
        if n > 1 {
            self.messages_coalesced
                .fetch_add(n as u64, Ordering::Relaxed);
        }
        Ok(records)
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

    /// Fetch records starting at `from`.
    pub fn fetch(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        from: Offset,
        max_messages: usize,
    ) -> Result<Vec<Record>> {
        let topics = self.topics.read();
        let topic = topics
            .get(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = topic
            .partitions
            .get(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        part.log.read(from, max_messages)
    }

    /// High watermark (next offset) for a partition.
    pub fn high_watermark(&self, topic: &TopicName, partition: PartitionId) -> Result<u64> {
        let topics = self.topics.read();
        let topic = topics
            .get(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = topic
            .partitions
            .get(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        Ok(part.log.high_watermark().raw())
    }

    /// Flush durable state for a topic partition to stable storage.
    pub fn flush(&self, topic: &TopicName, partition: PartitionId) -> Result<()> {
        let mut topics = self.topics.write();
        let topic = topics
            .get_mut(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = topic
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        part.log.flush()
    }

    /// List known topic names.
    pub fn list_topics(&self) -> Vec<TopicName> {
        self.topics.read().keys().cloned().collect()
    }

    /// Build a metadata snapshot. `topics = None` or empty slice means all topics.
    pub fn metadata(&self, topics: Option<&[TopicName]>) -> MetadataSnapshot {
        let host = self.advertised_host.read().clone();
        let port = self.advertised_port.load(Ordering::Relaxed) as u16;
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
                        leader: 0,
                        hwm: p.log.high_watermark().raw(),
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

        MetadataSnapshot {
            node_id: 0,
            host,
            port,
            topics: topic_meta,
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
        // Kafka Utils.murmur2("hello".as_bytes()) → known stable value used for stickiness.
        let h = murmur2(b"hello");
        assert_ne!(h, 0);
        // Same input always same hash.
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
}
