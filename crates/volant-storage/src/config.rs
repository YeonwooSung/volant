//! Storage configuration.

use std::path::PathBuf;

use crate::io::IoBackendKind;

/// Configuration for the partition log store.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Root directory for log data.
    pub data_dir: PathBuf,
    /// Target segment size in bytes before rolling.
    pub segment_size: u64,
    /// Whether to use memory maps for reads (default: true).
    pub use_mmap: bool,
    /// Flush policy: sync every N messages (0 = rely on OS / explicit flush).
    pub flush_every_n: u64,
    /// Write a sparse-index entry every this many payload bytes (default: 4096).
    pub index_interval_bytes: u32,
    /// Drop segments older than this many milliseconds (`None` = disabled).
    pub retention_ms: Option<u64>,
    /// Drop oldest segments until total size is under this many bytes (`None` = disabled).
    pub retention_bytes: Option<u64>,
    /// I/O backend selection (`IoUring` falls back to Std when feature/platform unavailable).
    pub io_backend: IoBackendKind,
    /// Request `O_DIRECT` for active segment opens (requires `direct-io` feature; ignored otherwise).
    pub direct_io: bool,
    /// Number of buffers to pre-allocate in the encode buffer pool (`0` = pool disabled).
    pub buffer_pool_blocks: usize,
    /// Capacity of each pool buffer in bytes (default: 64 KiB). Prefer multiples of 4 KiB for direct I/O.
    pub buffer_pool_block_size: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            segment_size: 256 * 1024 * 1024, // 256 MiB
            use_mmap: true,
            flush_every_n: 0,
            index_interval_bytes: 4096,
            retention_ms: None,
            retention_bytes: None,
            io_backend: IoBackendKind::Std,
            direct_io: false,
            buffer_pool_blocks: 0,
            buffer_pool_block_size: 64 * 1024,
        }
    }
}
