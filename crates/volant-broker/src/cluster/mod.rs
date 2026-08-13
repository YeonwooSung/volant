//! Static cluster membership, controller election, and assignment state.

pub mod assignment;
pub mod assignment_consensus;
pub mod config;
pub mod membership;
pub mod state;

pub use assignment::{
    assign_replicas, assign_replicas_round_robin, compute_hwm, distinct_configured_racks,
    elect_leader, expand_isr, isr_rejoin_eligible, rack_aware_assignment_enabled,
    reconcile_isr, shrink_isr, shrink_isr_by_time, topic_hash, will_use_rack_aware_assignment,
};
pub use assignment_consensus::{
    AssignmentConsensus, AssignmentConsensusFile, ASSIGNMENT_COMMITTED_SNAPSHOT_FILE,
    ASSIGNMENT_CONSENSUS_DIR, ASSIGNMENT_CONSENSUS_FILE, ASSIGNMENT_CONSENSUS_FILE_VERSION,
};
pub use config::{BrokerEndpoint, ClusterConfig};
pub use membership::Membership;
pub use state::{
    load_assignment, save_assignment, AssignmentSnapshot, PartitionAssignment, TopicAssignment,
};
