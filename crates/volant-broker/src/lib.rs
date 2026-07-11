//! Broker logic: topics, partitions, produce/fetch, and coordination hooks.

#![deny(missing_docs)]

pub mod assignor;
pub mod broker;
pub mod group;
pub mod net;
pub mod offset_store;
pub mod partition;
pub mod topic;

pub use assignor::{range_assign, range_assign_multi};
pub use broker::{
    murmur2, partition_for_key, Broker, MetadataSnapshot, PartitionMetadata, TopicMetadata,
};
pub use group::GroupCoordinator;
pub use net::{run_server, serve_listener};
pub use offset_store::{OffsetStore, StoredOffset, OFFSET_UNKNOWN};
