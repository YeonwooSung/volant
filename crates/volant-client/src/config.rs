//! Client configuration.
//!
//! v0.144 adds Fetch knobs (`fetch_max_messages` / `fetch_max_bytes` /
//! `fetch_max_wait_ms`) used by [`crate::Client::fetch_default`].

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
    /// Default Fetch `max_messages` for [`crate::Client::fetch_default`] (128).
    /// [`crate::Client::fetch`] still takes an explicit `max_messages`.
    /// GroupConsumer poll stays historical 100 (v0.76).
    pub fetch_max_messages: u32,
    /// Default Fetch `max_bytes` for [`crate::Client::fetch_default`] (4 MiB).
    /// [`crate::Client::fetch`] still hardcodes 4 MiB; use
    /// [`crate::Client::fetch_opts`] for an explicit size.
    pub fetch_max_bytes: u32,
    /// Default Fetch `max_wait_ms` for [`crate::Client::fetch_default`] (0).
    /// [`crate::Client::fetch`] still takes an explicit `max_wait_ms`.
    pub fetch_max_wait_ms: u32,
    /// Shared auth token. When set, the client sends Auth on connect.
    pub auth_token: Option<String>,
    /// SCRAM-SHA-256 username (Phase 22). Requires [`Self::scram_password`].
    pub scram_username: Option<String>,
    /// SCRAM-SHA-256 password (Phase 22). Requires [`Self::scram_username`].
    pub scram_password: Option<String>,
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
    /// Produce / Fetch / ListOffsets (v0.113) use this for error 13.
    /// ListMembers (v0.120) uses this for error 14 (`NotController`).
    /// DescribeGroup / ListGroups (v0.125) use this for error 14.
    /// Heartbeat (v0.135) uses this for error 14.
    /// LeaveGroup (v0.137) uses this for error 14.
    pub max_redirects: u32,
    /// Enable idempotent produce (InitProducerId + per-partition sequences). Phase 10.
    pub enable_idempotence: bool,
    /// Transactional id (Phase 18). When set, enables idempotence and fences
    /// prior owners of the same id. Use with [`crate::TransactionalProducer`].
    pub transactional_id: Option<String>,
    /// Extra produce / heartbeat / offset-admin / ListOffsets /
    /// LeaveGroup / DescribeGroup / ListGroups / Metadata /
    /// ListMembers / BeginTxn / EndTxn / InitProducerId /
    /// controller-gated admin / Auth / SCRAM handshake /
    /// DeleteRecords attempts after the first on transient
    /// broker/transport errors. DeleteRecords error 13 stays on
    /// [`Self::max_redirects`].
    pub max_retries: u32,
    /// Sleep between produce / heartbeat / offset-admin / ListOffsets /
    /// LeaveGroup / DescribeGroup / ListGroups / Metadata /
    /// ListMembers / BeginTxn / EndTxn / InitProducerId /
    /// controller-gated admin / Auth / SCRAM handshake /
    /// DeleteRecords retries (milliseconds).
    pub retry_backoff_ms: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["127.0.0.1:9092".into()],
            client_id: "volant-client".into(),
            acks: 1,
            fetch_max_messages: 128,
            fetch_max_bytes: 4 * 1024 * 1024,
            fetch_max_wait_ms: 0,
            auth_token: None,
            scram_username: None,
            scram_password: None,
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
