//! Assignment generation majority consensus (Phase 150 MVP).
//!
//! Durable state under `{data_dir}/__assignment_consensus/state.json`.
//! Raft-style majority of **configured** brokers for assignment generations
//! (topics / leaders / ISR snapshots). Not full openraft / KRaft.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// On-disk directory under `data_dir`.
pub const ASSIGNMENT_CONSENSUS_DIR: &str = "__assignment_consensus";
/// Snapshot file name.
pub const ASSIGNMENT_CONSENSUS_FILE: &str = "state.json";
/// File format version.
pub const ASSIGNMENT_CONSENSUS_FILE_VERSION: u32 = 1;

/// Durable consensus generations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AssignmentConsensusFile {
    /// Schema version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Last majority-committed assignment generation.
    #[serde(default)]
    pub committed_generation: u32,
    /// Last proposed (pending) generation; may lag or lead committed.
    #[serde(default)]
    pub pending_generation: u32,
}

fn default_version() -> u32 {
    ASSIGNMENT_CONSENSUS_FILE_VERSION
}

impl Default for AssignmentConsensusFile {
    fn default() -> Self {
        Self {
            version: ASSIGNMENT_CONSENSUS_FILE_VERSION,
            committed_generation: 0,
            pending_generation: 0,
        }
    }
}

/// File-backed assignment consensus state + metrics.
#[derive(Debug)]
pub struct AssignmentConsensus {
    path: PathBuf,
    committed_generation: AtomicU32,
    pending_generation: AtomicU32,
    persist_lock: Mutex<()>,
    success_total: AtomicU64,
    fail_total: AtomicU64,
    persist_errors_total: AtomicU64,
}

impl AssignmentConsensus {
    /// Open or create empty consensus state under `data_dir`.
    pub fn open(data_dir: impl AsRef<Path>) -> Self {
        let dir = data_dir.as_ref().join(ASSIGNMENT_CONSENSUS_DIR);
        let _ = fs::create_dir_all(&dir);
        let path = dir.join(ASSIGNMENT_CONSENSUS_FILE);
        let file = load_file(&path).unwrap_or_default();
        Self {
            path,
            committed_generation: AtomicU32::new(file.committed_generation),
            pending_generation: AtomicU32::new(file.pending_generation),
            persist_lock: Mutex::new(()),
            success_total: AtomicU64::new(0),
            fail_total: AtomicU64::new(0),
            persist_errors_total: AtomicU64::new(0),
        }
    }

    /// Last majority-committed assignment generation.
    pub fn committed_generation(&self) -> u32 {
        self.committed_generation.load(Ordering::Relaxed)
    }

    /// Last proposed generation.
    pub fn pending_generation(&self) -> u32 {
        self.pending_generation.load(Ordering::Relaxed)
    }

    /// Successful majority commits.
    pub fn success_total(&self) -> u64 {
        self.success_total.load(Ordering::Relaxed)
    }

    /// Proposals that failed majority.
    pub fn fail_total(&self) -> u64 {
        self.fail_total.load(Ordering::Relaxed)
    }

    /// Raft-style majority of `n` configured members (`n/2 + 1`).
    pub fn majority(n: usize) -> usize {
        n / 2 + 1
    }

    /// Mark a generation as pending (propose) and persist.
    pub fn set_pending(&self, generation: u32) {
        self.pending_generation
            .store(generation, Ordering::Relaxed);
        self.persist();
    }

    /// Advance committed generation on majority success (never shrinks).
    pub fn commit(&self, generation: u32) {
        let cur = self.committed_generation.load(Ordering::Relaxed);
        if generation > cur {
            self.committed_generation
                .store(generation, Ordering::Relaxed);
        }
        let pend = self.pending_generation.load(Ordering::Relaxed);
        if generation > pend {
            self.pending_generation
                .store(generation, Ordering::Relaxed);
        }
        self.success_total.fetch_add(1, Ordering::Relaxed);
        self.persist();
    }

    /// Record a majority-failure (pending retained).
    pub fn note_fail(&self) {
        self.fail_total.fetch_add(1, Ordering::Relaxed);
    }

    fn persist(&self) {
        let _g = self.persist_lock.lock();
        let file = AssignmentConsensusFile {
            version: ASSIGNMENT_CONSENSUS_FILE_VERSION,
            committed_generation: self.committed_generation.load(Ordering::Relaxed),
            pending_generation: self.pending_generation.load(Ordering::Relaxed),
        };
        if let Err(e) = save_file(&self.path, &file) {
            self.persist_errors_total.fetch_add(1, Ordering::Relaxed);
            warn!(
                path = %self.path.display(),
                error = %e,
                "assignment consensus persist failed"
            );
        }
    }
}

fn load_file(path: &Path) -> Option<AssignmentConsensusFile> {
    let mut f = File::open(path).ok()?;
    let mut s = String::new();
    f.read_to_string(&mut s).ok()?;
    serde_json::from_str(&s).ok()
}

fn save_file(path: &Path, file: &AssignmentConsensusFile) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_vec_pretty(file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&json)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn majority_math() {
        assert_eq!(AssignmentConsensus::majority(1), 1);
        assert_eq!(AssignmentConsensus::majority(2), 2);
        assert_eq!(AssignmentConsensus::majority(3), 2);
        assert_eq!(AssignmentConsensus::majority(5), 3);
    }

    #[test]
    fn durable_commit_roundtrip() {
        let dir = env::temp_dir().join(format!(
            "volant-ac-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        {
            let c = AssignmentConsensus::open(&dir);
            c.set_pending(3);
            c.commit(3);
            assert_eq!(c.committed_generation(), 3);
            assert_eq!(c.success_total(), 1);
        }
        let c2 = AssignmentConsensus::open(&dir);
        assert_eq!(c2.committed_generation(), 3);
        assert_eq!(c2.pending_generation(), 3);
        let _ = fs::remove_dir_all(&dir);
    }
}
