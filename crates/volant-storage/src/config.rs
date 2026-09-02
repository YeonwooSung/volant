//! Storage configuration.

use std::path::PathBuf;

use crate::io::IoBackendKind;

/// Default record threshold when group-commit is on and neither
/// [`StorageConfig::group_commit_max_records`] nor [`StorageConfig::flush_every_n`]
/// is set.
pub const DEFAULT_GROUP_COMMIT_MAX_RECORDS: u64 = 64;

/// Broker / process env for [`StorageConfig::group_commit_max_ms`] (default `0` = off).
pub const GROUP_COMMIT_MS_ENV: &str = "VOLANT_GROUP_COMMIT_MS";

/// Optional env for [`StorageConfig::group_commit_max_records`].
pub const GROUP_COMMIT_MAX_RECORDS_ENV: &str = "VOLANT_GROUP_COMMIT_MAX_RECORDS";

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
    /// Time-based group-commit window in milliseconds (`0` = off, today's behavior).
    pub group_commit_max_ms: u64,
    /// Flush when this many unflushed records accumulate (OR with the time window).
    ///
    /// `0` = inherit [`Self::flush_every_n`] if set, else
    /// [`DEFAULT_GROUP_COMMIT_MAX_RECORDS`] when `group_commit_max_ms > 0`.
    pub group_commit_max_records: u64,
    /// Write a sparse-index entry every this many payload bytes (default: 4096).
    pub index_interval_bytes: u32,
    /// Drop segments older than this many milliseconds (`None` = disabled).
    pub retention_ms: Option<u64>,
    /// Drop oldest segments until total size is under this many bytes (`None` = disabled).
    pub retention_bytes: Option<u64>,
    /// When true, sealed segments are key-compacted (Phase 16). Default delete-only.
    pub compact: bool,
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
            group_commit_max_ms: 0,
            group_commit_max_records: 0,
            index_interval_bytes: 4096,
            retention_ms: None,
            retention_bytes: None,
            compact: false,
            io_backend: IoBackendKind::Std,
            direct_io: false,
            buffer_pool_blocks: 0,
            buffer_pool_block_size: 64 * 1024,
        }
    }
}

impl StorageConfig {
    /// Whether time-based group-commit is enabled.
    pub fn group_commit_enabled(&self) -> bool {
        self.group_commit_max_ms > 0
    }

    /// Effective record threshold used when group-commit is on.
    pub fn effective_group_commit_max_records(&self) -> u64 {
        if self.group_commit_max_records > 0 {
            self.group_commit_max_records
        } else if self.flush_every_n > 0 {
            self.flush_every_n
        } else {
            DEFAULT_GROUP_COMMIT_MAX_RECORDS
        }
    }

    /// Overlay `VOLANT_GROUP_COMMIT_MS` / `VOLANT_GROUP_COMMIT_MAX_RECORDS` when set.
    ///
    /// Invalid / empty values are ignored. Default remains off (`0`).
    pub fn apply_group_commit_env(&mut self) {
        if let Ok(s) = std::env::var(GROUP_COMMIT_MS_ENV) {
            let s = s.trim();
            if !s.is_empty() {
                if let Ok(ms) = s.parse::<u64>() {
                    self.group_commit_max_ms = ms;
                }
            }
        }
        if let Ok(s) = std::env::var(GROUP_COMMIT_MAX_RECORDS_ENV) {
            let s = s.trim();
            if !s.is_empty() {
                if let Ok(n) = s.parse::<u64>() {
                    self.group_commit_max_records = n;
                }
            }
        }
    }
}
