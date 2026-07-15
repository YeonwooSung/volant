//! Broker logic: topics, partitions, produce/fetch, and coordination hooks.
//!
//! # High-performance produce
//!
//! [`Broker::produce`] coalesces a full [`volant_core::MessageBatch`] under a
//! single topics lock and a single flush-policy evaluation (see module docs on
//! [`broker`]). Prefer batching client-side for sequential append throughput.
//!
//! # Clustering (Phase 6)
//!
//! With [`Broker::with_cluster`], the broker participates in a static multi-node
//! cluster: controller election (lowest live id), ISR replication via
//! ReplicaFetch, and `acks=all` produce waiting on the high watermark.

#![deny(missing_docs)]

pub mod acl;
pub mod assignor;
pub mod broker;
pub mod cluster;
pub mod group;
pub mod metrics;
/// Framed TCP server and inter-broker RPC (public for TLS accept path).
pub mod net;
pub mod offset_store;
pub mod partition;
pub mod producer_state;
pub mod replica;
pub mod topic;
pub mod topic_catalog;
pub mod topic_config;

pub use acl::{
    AclEntry, AclOperation, AclPermission, AclSnapshot, AclState, AclStore, ResourceType,
    CLUSTER_RESOURCE,
};
pub use assignor::{range_assign, range_assign_multi, sticky_assign, sticky_assign_multi};
pub use broker::{
    murmur2, partition_for_key, Broker, ClusterState, IdempotentCheck, InterBrokerTls,
    MetadataSnapshot, PartitionMetadata, TopicMetadata, TxnCommitResult,
};
pub use cluster::{BrokerEndpoint, ClusterConfig};
pub use group::{
    static_member_id, GroupCoordinator, GroupDescription, GroupListEntry, GroupMemberDescription,
    STATIC_MEMBER_PREFIX,
};
pub use topic_catalog::{CatalogTopic, TopicCatalogFile, TopicCatalogStore};
pub use topic_config::{
    TopicConfig, TopicConfigStore, KEY_CLEANUP_POLICY, KEY_RETENTION_BYTES, KEY_RETENTION_MS,
    KEY_SEGMENT_BYTES,
};
pub use metrics::Metrics;
pub use net::{run_metrics_server, run_server, serve_listener, start_background_tasks};
pub use offset_store::{OffsetStore, StoredOffset, OFFSET_UNKNOWN};
