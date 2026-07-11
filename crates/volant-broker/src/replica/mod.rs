//! Leader ISR/HWM tracking and follower ReplicaFetch loop.

pub mod follower;

pub use follower::run_follower_loops;
