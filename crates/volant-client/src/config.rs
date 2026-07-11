//! Client configuration.

/// Connection and client behaviour settings.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Broker addresses (`host:port`).
    pub brokers: Vec<String>,
    /// Client identifier for logging / metrics.
    pub client_id: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["127.0.0.1:9092".into()],
            client_id: "volant-client".into(),
        }
    }
}
