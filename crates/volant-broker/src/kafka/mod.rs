//! Kafka wire protocol shim (Phase 23 MVP).
//!
//! Classic (non-flexible) framing only. Supported APIs: Produce, Fetch,
//! Metadata, ApiVersions. See `docs/PHASE23_SPEC.md`.

/// Kafka wire primitives and MessageSet encode/decode.
pub mod codec;
mod handler;

pub use handler::serve_kafka_listener;

/// Kafka principal used for ACL checks on the shim port (no SASL).
pub const KAFKA_ANONYMOUS_PRINCIPAL: &str = "kafka-anonymous";

/// Kafka error codes we emit (subset of the official table).
#[repr(i16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaErrorCode {
    /// No error.
    None = 0,
    /// Unexpected server failure.
    Unknown = -1,
    /// Offset out of range.
    OffsetOutOfRange = 1,
    /// Corrupt message.
    CorruptMessage = 2,
    /// Unknown topic or partition.
    UnknownTopicOrPartition = 3,
    /// Invalid message size / args.
    InvalidMessage = 4,
    /// Not leader for partition.
    NotLeaderForPartition = 6,
    /// Request timed out (unused).
    RequestTimedOut = 7,
    /// Broker not available.
    BrokerNotAvailable = 8,
    /// Message too large.
    MessageTooLarge = 10,
    /// Network exception (unused).
    NetworkException = 13,
    /// Unsupported version for this API.
    UnsupportedVersion = 35,
    /// Topic authorization failed.
    TopicAuthorizationFailed = 29,
    /// Cluster authorization failed.
    ClusterAuthorizationFailed = 31,
}

impl KafkaErrorCode {
    fn as_i16(self) -> i16 {
        self as i16
    }
}

/// Kafka API keys supported by the shim.
#[repr(i16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKey {
    /// Produce.
    Produce = 0,
    /// Fetch.
    Fetch = 1,
    /// Metadata.
    Metadata = 3,
    /// ApiVersions.
    ApiVersions = 18,
}

impl ApiKey {
    fn from_i16(v: i16) -> Option<Self> {
        match v {
            0 => Some(Self::Produce),
            1 => Some(Self::Fetch),
            3 => Some(Self::Metadata),
            18 => Some(Self::ApiVersions),
            _ => None,
        }
    }
}

/// Supported version ranges advertised in ApiVersions.
pub const SUPPORTED_APIS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 0, 0),
    (ApiKey::Fetch, 0, 0),
    (ApiKey::Metadata, 0, 1),
    (ApiKey::ApiVersions, 0, 0),
];
