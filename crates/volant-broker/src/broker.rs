//! Single-node broker state machine.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::RwLock;
use volant_core::{
    Error, Message, MessageBatch, Offset, PartitionId, Record, Result, TopicId, TopicName,
};
use volant_storage::StorageConfig;

use crate::topic::Topic;

/// In-process broker managing topics and partitions.
#[derive(Debug)]
pub struct Broker {
    storage: StorageConfig,
    topics: RwLock<HashMap<TopicName, Topic>>,
    next_topic_id: AtomicU32,
}

impl Broker {
    /// Create a broker with the given storage configuration.
    pub fn new(storage: StorageConfig) -> Self {
        Self {
            storage,
            topics: RwLock::new(HashMap::new()),
            next_topic_id: AtomicU32::new(1),
        }
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
        topics.insert(name, topic);
        Ok(id)
    }

    /// Produce a batch to a topic partition.
    pub fn produce(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        batch: MessageBatch,
    ) -> Result<Vec<Record>> {
        let mut topics = self.topics.write();
        let topic = topics
            .get_mut(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = topic
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;

        let mut records = Vec::with_capacity(batch.messages.len());
        for msg in batch.messages {
            records.push(part.log.append(msg)?);
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
}
