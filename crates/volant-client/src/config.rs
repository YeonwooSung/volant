//! Client configuration.

use std::path::PathBuf;

/// Connection and client behaviour settings.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Broker addresses (`host:port`).
    pub brokers: Vec<String>,
    /// Client identifier for logging / metrics.
    pub client_id: String,
    /// Default produce acks (`1` = leader only; `255` = all ISR).
    pub acks: u8,
    /// Shared auth token. When set, the client sends Auth on connect.
    pub auth_token: Option<String>,
    /// Enable TLS for broker connections (requires the `tls` feature).
    pub tls: bool,
    /// When TLS is enabled, skip certificate verification (dev/test only).
    pub tls_insecure: bool,
    /// Optional path to a PEM CA file trusted in addition to webpki roots.
    pub tls_ca: Option<PathBuf>,
    /// Max leader-redirect retries after `NotLeaderForPartition` (default 1 extra attempt).
    pub max_redirects: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["127.0.0.1:9092".into()],
            client_id: "volant-client".into(),
            acks: 1,
            auth_token: None,
            tls: false,
            tls_insecure: false,
            tls_ca: None,
            max_redirects: 1,
        }
    }
}
