//! Message and record types.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::offset::Offset;

/// A single append-only log record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// Monotonic offset assigned by the partition log.
    pub offset: Offset,
    /// Optional message key (used for partitioning / compaction).
    pub key: Option<Bytes>,
    /// Message payload.
    pub value: Bytes,
    /// Producer timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// Optional opaque headers.
    pub headers: Vec<(String, Bytes)>,
}

/// A produce-side message before an offset is assigned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// Optional message key.
    pub key: Option<Bytes>,
    /// Message payload.
    pub value: Bytes,
    /// Optional producer timestamp; broker may fill if absent.
    pub timestamp_ms: Option<i64>,
    /// Optional opaque headers.
    pub headers: Vec<(String, Bytes)>,
}

/// A batch of messages for efficient produce/fetch.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MessageBatch {
    /// Ordered messages in the batch.
    pub messages: Vec<Message>,
}

impl Message {
    /// Create a message with only a value payload.
    pub fn from_value(value: impl Into<Bytes>) -> Self {
        Self {
            key: None,
            value: value.into(),
            timestamp_ms: None,
            headers: Vec::new(),
        }
    }

    /// Create a keyed message.
    pub fn with_key(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            key: Some(key.into()),
            value: value.into(),
            timestamp_ms: None,
            headers: Vec::new(),
        }
    }
}
