//! Thin producer wrapper around [`Client`](crate::Client).

use volant_core::{Message, Result};

use crate::client::{Client, ProduceResult};
use crate::config::ClientConfig;

/// Async producer handle.
#[derive(Debug)]
pub struct Producer {
    client: Client,
}

impl Producer {
    /// Connect a producer using the given config.
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        Ok(Self {
            client: Client::connect(config).await?,
        })
    }

    /// Wrap an existing client.
    pub fn from_client(client: Client) -> Self {
        Self { client }
    }

    /// Send a message; broker assigns partition when key-hash/RR is desired.
    pub async fn send(&self, topic: &str, message: Message) -> Result<ProduceResult> {
        self.client.produce(topic, None, vec![message]).await
    }

    /// Send to an explicit partition.
    pub async fn send_to(
        &self,
        topic: &str,
        partition: u32,
        message: Message,
    ) -> Result<ProduceResult> {
        self.client
            .produce(topic, Some(partition), vec![message])
            .await
    }
}
