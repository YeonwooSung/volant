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
    /// Client certificate PEM for mTLS (Phase 19). Requires `tls_key` and feature `tls`.
    pub tls_cert: Option<PathBuf>,
    /// Client private key PEM for mTLS (Phase 19). Requires `tls_cert` and feature `tls`.
    pub tls_key: Option<PathBuf>,
    /// Max leader-redirect retries after `NotLeaderForPartition` (default 1 extra attempt).
    pub max_redirects: u32,
    /// Enable idempotent produce (InitProducerId + per-partition sequences). Phase 10.
    pub enable_idempotence: bool,
    /// Transactional id (Phase 18). When set, enables idempotence and fences
    /// prior owners of the same id. Use with [`crate::TransactionalProducer`].
    pub transactional_id: Option<String>,
    /// Extra produce attempts after the first on transient broker/transport errors.
    pub max_retries: u32,
    /// Sleep between produce retries (milliseconds).
    pub retry_backoff_ms: u64,
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
            tls_cert: None,
            tls_key: None,
            max_redirects: 1,
            enable_idempotence: false,
            transactional_id: None,
            max_retries: 0,
            retry_backoff_ms: 50,
        }
    }
}
