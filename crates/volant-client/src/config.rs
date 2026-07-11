//! Client configuration.

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
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["127.0.0.1:9092".into()],
            client_id: "volant-client".into(),
            acks: 1,
            auth_token: None,
        }
    }
}
