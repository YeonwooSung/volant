//! Thin consumer wrapper around [`Client`](crate::Client).

use volant_core::{Offset, Result};

use crate::client::{Client, FetchResult};
use crate::config::ClientConfig;

/// Async consumer handle for a single topic partition.
#[derive(Debug)]
pub struct Consumer {
    client: Client,
    topic: String,
    partition: u32,
    next_offset: Offset,
}

impl Consumer {
    /// Connect and start consuming from `from_offset` on `topic`/`partition`.
    pub async fn connect(
        config: ClientConfig,
        topic: impl Into<String>,
        partition: u32,
        from_offset: Offset,
    ) -> Result<Self> {
        Ok(Self {
            client: Client::connect(config).await?,
            topic: topic.into(),
            partition,
            next_offset: from_offset,
        })
    }

    /// Wrap an existing client.
    pub fn from_client(
        client: Client,
        topic: impl Into<String>,
        partition: u32,
        from_offset: Offset,
    ) -> Self {
        Self {
            client,
            topic: topic.into(),
            partition,
            next_offset: from_offset,
        }
    }

    /// Poll for up to `max_messages` records (non-blocking if `max_wait_ms = 0`).
    pub async fn poll(&mut self, max_messages: u32, max_wait_ms: u32) -> Result<FetchResult> {
        let result = self
            .client
            .fetch(
                &self.topic,
                self.partition,
                self.next_offset,
                max_messages,
                max_wait_ms,
            )
            .await?;
        if let Some(last) = result.records.last() {
            self.next_offset = Offset::new(last.offset.saturating_add(1));
        }
        Ok(result)
    }

    /// Current next offset to fetch.
    pub fn position(&self) -> Offset {
        self.next_offset
    }
}
