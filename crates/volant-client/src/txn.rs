//! Transactional producer helper (Phase 18).

use std::sync::Arc;

use volant_core::{Message, Result};
use volant_protocol::{TxnOffsetCommit, TxnProduceResult};

use crate::client::{Client, ProduceResult};
use crate::config::ClientConfig;

/// High-level transactional producer: begin → produce* → commit/abort.
#[derive(Debug)]
pub struct TransactionalProducer {
    client: Arc<Client>,
    pending_offsets: Vec<TxnOffsetCommit>,
    open: bool,
}

impl TransactionalProducer {
    /// Connect with the given transactional id (fences prior owners).
    pub async fn connect(
        brokers: Vec<String>,
        transactional_id: impl Into<String>,
    ) -> Result<Self> {
        let client = Client::connect(ClientConfig {
            brokers,
            transactional_id: Some(transactional_id.into()),
            enable_idempotence: true,
            ..ClientConfig::default()
        })
        .await?;
        Ok(Self::from_client(Arc::new(client)))
    }

    /// Wrap an existing client that has `transactional_id` configured.
    pub fn from_client(client: Arc<Client>) -> Self {
        Self {
            client,
            pending_offsets: Vec::new(),
            open: false,
        }
    }

    /// Underlying client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Begin a transaction.
    pub async fn begin(&mut self) -> Result<()> {
        self.client.begin_transaction().await?;
        self.pending_offsets.clear();
        self.open = true;
        Ok(())
    }

    /// Produce inside the open transaction (buffered on broker until commit).
    pub async fn produce(
        &self,
        topic: &str,
        partition: Option<u32>,
        messages: Vec<Message>,
    ) -> Result<ProduceResult> {
        self.client.produce(topic, partition, messages).await
    }

    /// Queue a group offset to commit atomically with the transaction.
    pub fn add_offsets(
        &mut self,
        group_id: impl Into<String>,
        entries: impl IntoIterator<Item = (String, u32, u64)>,
    ) {
        let group_id = group_id.into();
        for (topic, partition, offset) in entries {
            self.pending_offsets.push(TxnOffsetCommit {
                group_id: group_id.clone(),
                topic,
                partition,
                offset,
                metadata: String::new(),
            });
        }
    }

    /// Commit: flush produces and deferred offsets. Returns final log offsets.
    pub async fn commit(&mut self) -> Result<Vec<TxnProduceResult>> {
        let offsets = std::mem::take(&mut self.pending_offsets);
        let results = self.client.commit_transaction(offsets).await?;
        self.open = false;
        Ok(results)
    }

    /// Abort: discard buffered produces and deferred offsets.
    pub async fn abort(&mut self) -> Result<()> {
        self.pending_offsets.clear();
        self.client.abort_transaction().await?;
        self.open = false;
        Ok(())
    }

    /// Whether a transaction is open locally.
    pub fn is_open(&self) -> bool {
        self.open
    }
}
