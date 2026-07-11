//! Core types, errors, and shared utilities for Volant.
//!
//! This crate is dependency-light and is shared across storage, protocol,
//! broker, client, and stream components.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod error;
pub mod message;
pub mod topic;
pub mod offset;

pub use error::{Error, Result};
pub use message::{Message, MessageBatch, Record};
pub use offset::Offset;
pub use topic::{PartitionId, TopicId, TopicName};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn message_from_value() {
        let m = Message::from_value("hello");
        assert_eq!(m.value, Bytes::from("hello"));
        assert!(m.key.is_none());
    }

    #[test]
    fn offset_monotonic() {
        let o = Offset::new(41);
        assert_eq!(o.next().raw(), 42);
    }
}
