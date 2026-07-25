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
/// Broker-level Kafka Describe/AlterConfigs keys + durable store (Phase 99–100).
pub mod broker_config;
pub mod cluster;
/// Durable DeleteRecords pending-truncate outbox (Phase 116).
pub mod delete_records_outbox;
pub mod group;
/// Kafka wire protocol shim (Phase 23 MVP).
pub mod kafka;
/// Durable leader-epoch history for OffsetForLeaderEpoch (Phase 87).
pub mod leader_epoch;
pub mod metrics;
/// Framed TCP server and inter-broker RPC (public for TLS accept path).
pub mod net;
pub mod offset_store;
pub mod partition;
pub mod producer_state;
pub mod replica;
/// SCRAM credentials and crypto (Phase 22 SHA-256; Phase 34 SHA-512).
pub mod scram;
pub mod topic;
pub mod topic_catalog;
pub mod topic_config;

pub use acl::{
    AclEntry, AclOperation, AclPermission, AclSnapshot, AclState, AclStore, ResourceType,
    CLUSTER_RESOURCE,
};
pub use scram::{
    client_proof_and_server_sig, client_proof_and_server_sig_for, generate_client_nonce,
    ScramChallenge, ScramCredential, ScramHash, ScramStore, DEFAULT_ITERATIONS,
};
pub use assignor::{range_assign, range_assign_multi, sticky_assign, sticky_assign_multi};
pub use broker::{
    murmur2, partition_for_key, Broker, ClusterState, IdempotentCheck, InterBrokerTls,
    MetadataSnapshot, PartitionMetadata, TopicMetadata, Txn2pcFanout, TxnCommitResult,
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
pub use broker_config::{
    BrokerConfigFile, BrokerConfigStore, BROKER_CONFIG_DIR, BROKER_CONFIG_FILE_VERSION,
    BROKER_CONFIG_KEYS, DEFAULT_OPEN_TXN_TIMEOUT_MS, DEFAULT_PREPARED_TXN_TIMEOUT_MS,
    DEFAULT_SWEEP_INTERVAL_MS, DEFAULT_TRANSACTION_MAX_TIMEOUT_MS, KEY_FETCH_SESSION_IDLE_MS,
    KEY_FETCH_SESSION_MAX, KEY_OPEN_TXN_TIMEOUT_MS, KEY_PREPARED_TXN_TIMEOUT_MS,
    KEY_SWEEP_INTERVAL_MS, KEY_TRANSACTION_MAX_TIMEOUT_MS,
};
pub use leader_epoch::{EpochStart, LeaderEpochStore, LeaderEpochsFile};
pub use kafka::{serve_kafka_listener, serve_kafka_listener_until};
pub use metrics::Metrics;
pub use delete_records_outbox::{
    DeleteRecordsOutbox, OutboxEntry, DEFAULT_MAX_ENTRIES as DELETE_RECORDS_OUTBOX_MAX_ENTRIES,
    OUTBOX_DIR as DELETE_RECORDS_OUTBOX_DIR, OUTBOX_FILE as DELETE_RECORDS_OUTBOX_FILE,
};
pub use net::{
    drain_delete_records_outbox, fanout_cluster_acl_snapshot, fanout_cluster_broker_config,
    fanout_delete_records, fanout_txn_participant_complete, fanout_txn_participant_open,
    fanout_txn_participant_prepare, run_metrics_server, run_metrics_server_until, run_server,
    run_txn_2pc_fanout, serve_listener, serve_listener_until, shutdown_signal,
    start_background_tasks, BackgroundTasks,
};
pub use offset_store::{OffsetStore, StoredOffset, OFFSET_UNKNOWN};
