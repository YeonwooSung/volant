//! Static cluster configuration loaded from TOML.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use volant_core::{Error, Result};

/// One broker entry in `cluster.toml`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerEndpoint {
    /// Unique broker id (static membership).
    pub id: u32,
    /// Host for inter-broker and client traffic.
    pub host: String,
    /// Port.
    pub port: u16,
    /// Optional rack (ignored for placement in Phase 6).
    #[serde(default)]
    pub rack: Option<String>,
}

/// Cluster-wide configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    /// Default RF for new topics (capped by broker count).
    #[serde(default = "default_rf")]
    pub default_replication_factor: u32,
    /// Minimum ISR size required for `acks=all` produce.
    #[serde(default = "default_min_isr")]
    pub min_insync_replicas: u32,
    /// Broker session timeout for controller heartbeats (ms).
    #[serde(default = "default_session")]
    pub session_timeout_ms: u32,
    /// Max wait for empty replica fetch (ms).
    #[serde(default = "default_fetch_wait")]
    pub replica_fetch_max_wait_ms: u32,
    /// Max bytes per ReplicaFetch response.
    #[serde(default = "default_fetch_bytes")]
    pub replica_fetch_max_bytes: u32,
    /// Max LEO lag before a follower is removed from ISR.
    #[serde(default = "default_lag")]
    pub replica_lag_max_messages: u64,
    /// Static broker membership.
    pub brokers: Vec<BrokerEndpoint>,
}

fn default_rf() -> u32 {
    3
}
fn default_min_isr() -> u32 {
    2
}
fn default_session() -> u32 {
    3000
}
fn default_fetch_wait() -> u32 {
    500
}
fn default_fetch_bytes() -> u32 {
    1_048_576
}
fn default_lag() -> u64 {
    10_000
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            default_replication_factor: default_rf(),
            min_insync_replicas: default_min_isr(),
            session_timeout_ms: default_session(),
            replica_fetch_max_wait_ms: default_fetch_wait(),
            replica_fetch_max_bytes: default_fetch_bytes(),
            replica_lag_max_messages: default_lag(),
            brokers: vec![],
        }
    }
}

impl ClusterConfig {
    /// Load cluster config from a TOML file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(path.as_ref()).map_err(|e| {
            Error::InvalidArgument(format!(
                "failed to read cluster config {}: {e}",
                path.as_ref().display()
            ))
        })?;
        Self::parse(&raw)
    }

    /// Parse cluster config from a TOML string.
    pub fn parse(raw: &str) -> Result<Self> {
        let cfg: ClusterConfig = toml::from_str(raw)
            .map_err(|e| Error::InvalidArgument(format!("invalid cluster.toml: {e}")))?;
        if cfg.brokers.is_empty() {
            return Err(Error::InvalidArgument(
                "cluster.toml must list at least one broker".into(),
            ));
        }
        let mut ids: Vec<u32> = cfg.brokers.iter().map(|b| b.id).collect();
        ids.sort_unstable();
        for w in ids.windows(2) {
            if w[0] == w[1] {
                return Err(Error::InvalidArgument(format!(
                    "duplicate broker id {}",
                    w[0]
                )));
            }
        }
        if cfg.default_replication_factor == 0 {
            return Err(Error::InvalidArgument(
                "default_replication_factor must be >= 1".into(),
            ));
        }
        if cfg.min_insync_replicas == 0 {
            return Err(Error::InvalidArgument(
                "min_insync_replicas must be >= 1".into(),
            ));
        }
        Ok(cfg)
    }

    /// Look up a broker endpoint by id.
    pub fn broker(&self, id: u32) -> Option<&BrokerEndpoint> {
        self.brokers.iter().find(|b| b.id == id)
    }

    /// Sorted broker ids.
    pub fn broker_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self.brokers.iter().map(|b| b.id).collect();
        ids.sort_unstable();
        ids
    }

    /// Address string `host:port` for a broker.
    pub fn addr_of(&self, id: u32) -> Option<String> {
        self.broker(id).map(|b| format!("{}:{}", b.host, b.port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_cluster() {
        let raw = r#"
default_replication_factor = 3
min_insync_replicas = 2

[[brokers]]
id = 1
host = "127.0.0.1"
port = 9092

[[brokers]]
id = 2
host = "127.0.0.1"
port = 9093

[[brokers]]
id = 3
host = "127.0.0.1"
port = 9094
"#;
        let cfg = ClusterConfig::parse(raw).unwrap();
        assert_eq!(cfg.brokers.len(), 3);
        assert_eq!(cfg.session_timeout_ms, 3000);
        assert_eq!(cfg.broker_ids(), vec![1, 2, 3]);
    }
}
