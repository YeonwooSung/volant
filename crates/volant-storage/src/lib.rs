//! DMA-friendly append-only log storage for Volant.
//!
//! Design goals:
//! - Memory-mapped segment files for zero-copy reads
//! - Sequential append path optimized for throughput
//! - Future: `io_uring` / O_DIRECT paths for true DMA-style I/O

#![deny(missing_docs)]

pub mod config;
pub mod index;
pub mod log;
pub mod record;
pub mod segment;

pub use config::StorageConfig;
pub use log::PartitionLog;
pub use segment::Segment;
