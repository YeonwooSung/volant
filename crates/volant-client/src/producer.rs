//! Producer API (network path TBD).

use volant_core::{Error, Message, Result};

use crate::config::ClientConfig;

/// Async producer handle.
#[derive(Debug)]
pub struct Producer {
    config: ClientConfig,
}

impl Producer {
    /// Create a producer from config (does not connect yet).
    pub fn new(config: ClientConfig) -> Self {
        Self { config }
    }

    /// Send a message to `topic` (placeholder until protocol transport lands).
    pub async fn send(&self, topic: &str, message: Message) -> Result<()> {
        let _ = (&self.config, topic, message);
        Err(Error::NotImplemented("networked producer send"))
    }
}
