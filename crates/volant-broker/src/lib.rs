//! Broker logic: topics, partitions, produce/fetch, and coordination hooks.

#![deny(missing_docs)]

pub mod broker;
pub mod net;
pub mod partition;
pub mod topic;

pub use broker::{Broker, MetadataSnapshot, PartitionMetadata, TopicMetadata, murmur2, partition_for_key};
pub use net::{run_server, serve_listener};
