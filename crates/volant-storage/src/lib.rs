//! DMA-friendly append-only log storage for Volant.
//!
//! Design goals:
//! - Memory-mapped segment files for zero-copy reads
//! - Sequential append path optimized for throughput
//! - Optional `io_uring` / `O_DIRECT` paths for true DMA-style I/O (feature-gated)

#![deny(missing_docs)]

pub mod config;
pub mod index;
pub mod io;
pub mod log;
pub mod pool;
pub mod record;
pub mod segment;

pub use config::StorageConfig;
pub use io::{create_io_backend, IoBackend, IoBackendKind, StdIoBackend};
pub use log::{CompactStats, PartitionLog};
pub use pool::{BufferPool, PooledBuf};
pub use segment::Segment;

#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub use io::UringIoBackend;
