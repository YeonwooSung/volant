//! Broker logic: topics, partitions, produce/fetch, and coordination hooks.

#![deny(missing_docs)]

pub mod broker;
pub mod partition;
pub mod topic;

pub use broker::Broker;
