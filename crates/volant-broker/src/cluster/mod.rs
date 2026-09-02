//! Static cluster membership, controller election, and assignment state.

pub mod assignment;
pub mod assignment_consensus;
pub mod cluster_metadata;
pub mod config;
pub mod membership;
pub mod metadata_raft;
pub mod openraft_meta;
pub mod overlay;
pub mod state;

pub use assignment::{
    assign_replicas, assign_replicas_round_robin, compute_hwm, distinct_configured_racks,
    elect_leader, expand_isr, isr_rejoin_eligible, rack_aware_assignment_enabled,
    reassign_on_add_enabled, reconcile_isr, shrink_isr, shrink_isr_by_time, topic_hash,
    will_use_rack_aware_assignment, ENV_REASSIGN_ON_ADD,
};
pub use assignment_consensus::{
    AssignmentConsensus, AssignmentConsensusFile, ASSIGNMENT_COMMITTED_SNAPSHOT_FILE,
    ASSIGNMENT_CONSENSUS_DIR, ASSIGNMENT_CONSENSUS_FILE, ASSIGNMENT_CONSENSUS_FILE_VERSION,
};
pub use cluster_metadata::{
    cluster_metadata_replicas, cluster_metadata_topic_env_enabled,
    load_assignment_from_cluster_metadata, CLUSTER_METADATA_HEADER, CLUSTER_METADATA_HEADER_VALUE,
    CLUSTER_METADATA_TOPIC,
};
pub use config::{BrokerEndpoint, ClusterConfig};
pub use membership::Membership;
pub use metadata_raft::{
    AppendEntriesResult, MetadataCommand, MetadataLogEntry, MetadataRaftHardState,
    MetadataRaftState, METADATA_RAFT_DIR, METADATA_RAFT_FILE_VERSION,
    METADATA_RAFT_HARD_STATE_FILE, METADATA_RAFT_LOG_FILE,
};
pub use openraft_meta::{
    default_openraft_metadata_enabled, openraft_snapshot_logs_since_last, MetaRequest,
    MetaResponse, OpenraftGuard, OpenraftMetaHandle, OpenraftMetricsCache,
    DEFAULT_OPENRAFT_SNAPSHOT_LOGS, OPENRAFT_DIR, OPENRAFT_HARD_STATE_FILE, OPENRAFT_LOG_FILE,
    OPENRAFT_REDB_FILE, OPENRAFT_SNAPSHOT_FILE,
};
pub use overlay::{
    load_membership_overlay, membership_overlay_path, save_membership_overlay,
    validate_membership_overlay, MembershipOverlay, MEMBERSHIP_OVERLAY_FILE,
};
pub use state::{
    assignment_path, load_assignment, save_assignment, AssignmentSnapshot, PartitionAssignment,
    TopicAssignment,
};
