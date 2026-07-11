//! Consumer API (network path TBD).

use volant_core::{Error, Record, Result};

use crate::config::ClientConfig;

/// Async consumer handle.
#[derive(Debug)]
pub struct Consumer {
    config: ClientConfig,
}

impl Consumer {
    /// Create a consumer from config (does not connect yet).
    pub fn new(config: ClientConfig) -> Self {
        Self { config }
    }

    /// Poll for records (placeholder until protocol transport lands).
    pub async fn poll(&self) -> Result<Vec<Record>> {
        let _ = &self.config;
        Err(Error::NotImplemented("networked consumer poll"))
    }
}
