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
/// Durable cluster admin generations (Phase 117).
pub mod cluster_admin;
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
/// Controller SoT DeleteRecords truncate journal (Phase 129).
pub mod truncate_journal;
/// Durable Init-owner txn coordinator registry (Phase 124).
pub mod txn_coordinator_registry;

pub use acl::{
    AclEntry, AclOperation, AclPermission, AclSnapshot, AclState, AclStore, ResourceType,
    CLUSTER_RESOURCE,
};
pub use assignor::{range_assign, range_assign_multi, sticky_assign, sticky_assign_multi};
pub use broker::{
    admin_catchup_min_interval_ms, journal_catchup_min_interval_ms, murmur2, partition_for_key,
    sticky_coordinator_id, transaction_state_topic_enabled_from_env, Broker, ClusterState,
    IdempotentCheck, InterBrokerTls, MembershipSnapshot, MetadataSnapshot, PartitionMetadata,
    PendingIsrReport, TopicMetadata, TransactionStateRecord, Txn2pcFanout, TxnCommitResult,
    DEFAULT_ADMIN_CATCHUP_MIN_INTERVAL_MS, DEFAULT_JOURNAL_CATCHUP_MIN_INTERVAL_MS,
    ENV_TRANSACTION_STATE_TOPIC, TRANSACTION_STATE_HEADER, TRANSACTION_STATE_RECORD_VERSION,
    TRANSACTION_STATE_TOPIC, TXN_STATE_COMPLETE_ABORT, TXN_STATE_COMPLETE_COMMIT, TXN_STATE_EMPTY,
    TXN_STATE_ONGOING, TXN_STATE_PREPARE_ABORT, TXN_STATE_PREPARE_COMMIT,
};
pub use broker_config::{
    BrokerConfigFile, BrokerConfigStore, BROKER_CONFIG_DIR, BROKER_CONFIG_FILE_VERSION,
    BROKER_CONFIG_KEYS, DEFAULT_OPEN_TXN_TIMEOUT_MS, DEFAULT_PREPARED_TXN_TIMEOUT_MS,
    DEFAULT_SWEEP_INTERVAL_MS, DEFAULT_TRANSACTION_MAX_TIMEOUT_MS, KEY_FETCH_SESSION_IDLE_MS,
    KEY_FETCH_SESSION_MAX, KEY_OPEN_TXN_TIMEOUT_MS, KEY_PREPARED_TXN_TIMEOUT_MS,
    KEY_SWEEP_INTERVAL_MS, KEY_TRANSACTION_MAX_TIMEOUT_MS, KEY_TXN_COORDINATOR_TTL_MS,
};
pub use cluster::{
    assignment_path, cluster_metadata_replicas, cluster_metadata_topic_env_enabled,
    load_assignment_from_cluster_metadata, load_membership_overlay, membership_overlay_path,
    openraft_snapshot_logs_since_last, reassign_on_add_enabled, save_membership_overlay,
    AssignmentConsensus, AssignmentConsensusFile, BrokerEndpoint, ClusterConfig, MembershipOverlay,
    MetadataCommand, MetadataLogEntry, MetadataRaftHardState, MetadataRaftState,
    ASSIGNMENT_COMMITTED_SNAPSHOT_FILE, ASSIGNMENT_CONSENSUS_DIR, ASSIGNMENT_CONSENSUS_FILE,
    ASSIGNMENT_CONSENSUS_FILE_VERSION, CLUSTER_METADATA_HEADER, CLUSTER_METADATA_HEADER_VALUE,
    CLUSTER_METADATA_TOPIC, DEFAULT_OPENRAFT_SNAPSHOT_LOGS, ENV_REASSIGN_ON_ADD, METADATA_RAFT_DIR,
    METADATA_RAFT_FILE_VERSION, METADATA_RAFT_HARD_STATE_FILE, METADATA_RAFT_LOG_FILE,
    OPENRAFT_DIR, OPENRAFT_HARD_STATE_FILE, OPENRAFT_LOG_FILE, OPENRAFT_REDB_FILE,
    OPENRAFT_SNAPSHOT_FILE,
};
pub use cluster_admin::{
    ClusterAdminFile, ClusterAdminStore, CLUSTER_ADMIN_DIR, CLUSTER_ADMIN_FILE,
    CLUSTER_ADMIN_FILE_VERSION,
};
pub use delete_records_outbox::{
    DeleteRecordsOutbox, OutboxEntry, DEFAULT_MAX_ENTRIES as DELETE_RECORDS_OUTBOX_MAX_ENTRIES,
    OUTBOX_DIR as DELETE_RECORDS_OUTBOX_DIR, OUTBOX_FILE as DELETE_RECORDS_OUTBOX_FILE,
};
pub use group::{
    static_member_id, GroupCoordinator, GroupDescription, GroupListEntry, GroupMemberDescription,
    STATIC_MEMBER_PREFIX,
};
pub use kafka::{serve_kafka_listener, serve_kafka_listener_until};
pub use leader_epoch::{EpochStart, LeaderEpochStore, LeaderEpochsFile};
pub use metrics::Metrics;
pub use net::{
    catch_up_peer_admin_state, catch_up_peer_truncate_journal, delete_records_fanout_budget,
    drain_delete_records_outbox, fanout_assignment_consensus, fanout_cluster_acl_snapshot,
    fanout_cluster_broker_config, fanout_delete_records, fanout_delete_records_replicas_only,
    fanout_isr_update_reports, fanout_membership_put, fanout_metadata_raft_append,
    fanout_session_mirror_ops, fanout_truncate_journal_note,
    fanout_truncate_journal_note_provisional, fanout_truncate_journal_push,
    fanout_txn_participant_complete, fanout_txn_participant_open, fanout_txn_participant_prepare,
    inter_broker_rpc, inter_broker_rpc_timeout, maybe_fanout_assignment_consensus, render_metrics,
    run_metrics_server, run_metrics_server_until, run_server, run_txn_2pc_fanout,
    schedule_catch_up_peer_admin_state, schedule_catch_up_peer_truncate_journal,
    schedule_isr_update_reports, schedule_session_mirror_fanout, serve_listener,
    serve_listener_until, shutdown_signal, start_background_tasks, BackgroundTasks,
    DeleteRecordsFanoutResult, DEFAULT_DELETE_RECORDS_FANOUT_BUDGET_MS,
    DEFAULT_INTER_BROKER_RPC_TIMEOUT_MS, MAX_INTER_BROKER_TIMEOUT_MS, MIN_INTER_BROKER_TIMEOUT_MS,
};
pub use offset_store::{OffsetStore, StoredOffset, OFFSET_UNKNOWN};
pub use replica::{
    partition_raft_env_enabled, PartitionAppendResult, PartitionRaftEntry, PartitionRaftGroup,
    PartitionRaftHardState, PartitionRaftPayload, PartitionRaftState, PARTITION_RAFT_DIR,
    PARTITION_RAFT_FILE_VERSION, PARTITION_RAFT_HARD_STATE_FILE, PARTITION_RAFT_LOG_FILE,
};
pub use scram::{
    client_proof_and_server_sig, client_proof_and_server_sig_for, generate_client_nonce,
    ScramChallenge, ScramCredential, ScramHash, ScramStore, DEFAULT_ITERATIONS,
};
pub use topic_catalog::{CatalogTopic, TopicCatalogFile, TopicCatalogStore};
pub use topic_config::{
    TopicConfig, TopicConfigStore, KEY_CLEANUP_POLICY, KEY_RETENTION_BYTES, KEY_RETENTION_MS,
    KEY_SEGMENT_BYTES,
};
pub use truncate_journal::{
    TruncateJournal, TruncateJournalEntry, TruncateJournalFile, MAX_TRUNCATE_JOURNAL_ENTRIES,
    MAX_TRUNCATE_JOURNAL_SNAPSHOT_BYTES, TRUNCATE_JOURNAL_DIR, TRUNCATE_JOURNAL_FILE,
    TRUNCATE_JOURNAL_FILE_VERSION,
};
pub use txn_coordinator_registry::{
    effective_txn_coordinator_ttl_ms, TxnCoordinatorEntry, TxnCoordinatorFile,
    TxnCoordinatorRegistry, DEFAULT_TXN_COORDINATOR_TTL_MS, TXN_COORDINATOR_DIR,
    TXN_COORDINATOR_FILE, TXN_COORDINATOR_FILE_VERSION,
};
