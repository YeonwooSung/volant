//! Topic sink adapter (produce).

use std::sync::Arc;

use volant_client::{Client, TransactionalProducer};
use volant_core::{Message, Record, Result};

/// Produces pipeline output records to a Volant topic.
pub struct TopicSink {
    client: Arc<Client>,
    topic: String,
}

impl TopicSink {
    /// Create a sink for `topic`.
    pub fn new(client: Arc<Client>, topic: impl Into<String>) -> Self {
        Self {
            client,
            topic: topic.into(),
        }
    }

    /// Sink topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Produce all records (acks=1). Empty batch is a no-op.
    ///
    /// At-least-once path: crash between this and offset commit may redeliver.
    pub async fn send_all(&self, records: &[Record]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        // Produce one-by-one so key-hash routing applies per record.
        // Small batches keep e2e simple; production could coalesce by partition.
        for r in records {
            let msg = record_to_message(r);
            self.client.produce(&self.topic, None, vec![msg]).await?;
        }
        Ok(())
    }

    /// Produce all records inside an open transaction (exactly-once path).
    ///
    /// Empty batch is a no-op (caller may still commit deferred group offsets).
    /// Call [`TransactionalProducer::begin`] before and
    /// [`TransactionalProducer::commit`] / [`TransactionalProducer::abort`] after.
    pub async fn send_all_in_txn(
        &self,
        txn: &TransactionalProducer,
        records: &[Record],
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        for r in records {
            let msg = record_to_message(r);
            txn.produce(&self.topic, None, vec![msg]).await?;
        }
        Ok(())
    }
}

fn record_to_message(r: &Record) -> Message {
    Message {
        key: r.key.clone(),
        value: r.value.clone(),
        timestamp_ms: if r.timestamp_ms > 0 {
            Some(r.timestamp_ms)
        } else {
            None
        },
        headers: r.headers.clone(),
    }
}
