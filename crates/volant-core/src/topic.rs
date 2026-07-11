//! Topic and partition identifiers.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Human-readable topic name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicName(pub String);

impl TopicName {
    /// Create a topic name.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for TopicName {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// Stable numeric topic identifier assigned by the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicId(pub u32);

/// Zero-based partition index within a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId(pub u32);

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
