//! Topic source adapter (GroupConsumer).

use std::collections::HashMap;
use std::sync::Arc;

use volant_client::{Client, FetchedRecord, GroupConsumer};
use volant_core::{Offset, Record, Result};

/// Configuration for consuming a source topic.
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Consumer group id.
    pub group_id: String,
    /// Session timeout for the group (ms). Default 10_000.
    pub session_timeout_ms: u32,
}

impl SourceConfig {
    /// Build config with a group id.
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            session_timeout_ms: 10_000,
        }
    }
}

/// Pulls records from a Volant topic via [`GroupConsumer`].
pub struct TopicSource {
    consumer: GroupConsumer,
    topic: String,
}

impl TopicSource {
    /// Join a consumer group on `topic`.
    pub async fn join(
        client: Arc<Client>,
        topic: impl Into<String>,
        config: SourceConfig,
    ) -> Result<Self> {
        let topic = topic.into();
        let consumer = GroupConsumer::join(
            client,
            config.group_id,
            vec![topic.clone()],
            config.session_timeout_ms,
        )
        .await?;
        Ok(Self { consumer, topic })
    }

    /// Source topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Consumer group id (for transactional offset commit).
    pub fn group_id(&self) -> &str {
        self.consumer.group_id()
    }

    /// Next-read positions as `(topic, partition) → offset`.
    pub fn positions(&self) -> HashMap<(String, u32), u64> {
        self.consumer.positions()
    }

    /// Pending next offsets after the last poll: `(topic, partition, next_offset)`.
    ///
    /// Used by the exactly-once path to [`volant_client::TransactionalProducer::add_offsets`].
    pub fn pending_offsets(&self) -> Vec<(String, u32, u64)> {
        self.consumer
            .positions()
            .iter()
            .map(|((topic, partition), offset)| (topic.clone(), *partition, *offset))
            .collect()
    }

    /// Poll for new records, converted to core [`Record`]s.
    pub async fn poll(&mut self) -> Result<Vec<Record>> {
        let fetched = self.consumer.poll().await?;
        Ok(fetched.into_iter().map(fetched_to_record).collect())
    }

    /// Commit consumer offsets (at-least-once path; call after successful sink produce).
    ///
    /// Exactly-once apps commit offsets via the transactional producer instead.
    pub async fn commit(&self) -> Result<()> {
        self.consumer.commit().await
    }

    /// Leave the consumer group.
    pub async fn leave(self) -> Result<()> {
        self.consumer.leave().await
    }
}

fn fetched_to_record(f: FetchedRecord) -> Record {
    Record {
        offset: Offset::new(f.record.offset),
        key: f.record.key,
        value: f.record.value,
        timestamp_ms: f.record.timestamp_ms,
        headers: f.record.headers,
    }
}

/// Convert a core record (e.g. offline tests) without a network source.
pub fn record_from_value(value: impl Into<bytes::Bytes>, timestamp_ms: i64) -> Record {
    Record {
        offset: Offset::ZERO,
        key: None,
        value: value.into(),
        timestamp_ms,
        headers: Vec::new(),
    }
}
