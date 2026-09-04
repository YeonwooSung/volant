//! KRaft-style metadata Raft log MVP (Phase 154).
//!
//! Ordered assignment mutations as log entries with `(term, index)`,
//! majority AppendEntries, and apply only when `commit_index` advances.
//!
//! **Not** full openraft: no true Raft election (controller remains lowest live
//! id), no InstallSnapshot, no dynamic membership. Interface is intentionally
//! small so a later openraft wrapper can replace the storage engine.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::warn;
use volant_protocol::ClusterTopicState;

/// On-disk directory under `data_dir`.
pub const METADATA_RAFT_DIR: &str = "__metadata_raft";
/// Durable log file.
pub const METADATA_RAFT_LOG_FILE: &str = "log.json";
/// Durable hard state (term / commit / last_applied).
pub const METADATA_RAFT_HARD_STATE_FILE: &str = "hard_state.json";
/// File format version.
pub const METADATA_RAFT_FILE_VERSION: u32 = 1;

/// Metadata log command payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetadataCommand {
    /// Full assignment snapshot (MVP; fine-grained ops later).
    SetAssignment {
        /// Assignment generation for this snapshot.
        generation: u32,
        /// Wire topics (leaders / replicas / ISR).
        topics: Vec<ClusterTopicState>,
    },
    /// Empty log filler (reserved).
    Noop,
}

/// One ordered metadata log entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataLogEntry {
    /// Leader term that authored this entry.
    pub term: u64,
    /// Monotonic log index (1-based).
    pub index: u64,
    /// Command payload.
    pub payload: MetadataCommand,
}

/// Durable hard state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataRaftHardState {
    /// Schema version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Current term (leader term for this MVP; no election).
    #[serde(default)]
    pub current_term: u64,
    /// Highest log index known to be committed.
    #[serde(default)]
    pub commit_index: u64,
    /// Highest log index applied to the state machine.
    #[serde(default)]
    pub last_applied: u64,
}

fn default_version() -> u32 {
    METADATA_RAFT_FILE_VERSION
}

impl Default for MetadataRaftHardState {
    fn default() -> Self {
        Self {
            version: METADATA_RAFT_FILE_VERSION,
            current_term: 0,
            commit_index: 0,
            last_applied: 0,
        }
    }
}

/// Durable log file wrapper.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
struct MetadataRaftLogFile {
    #[serde(default = "default_version")]
    version: u32,
    #[serde(default)]
    entries: Vec<MetadataLogEntry>,
}

/// Result of a follower AppendEntries attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendEntriesResult {
    /// Peer's current term (may be higher on reject in a future election).
    pub term: u64,
    /// Whether prev_log matched and entries were accepted.
    pub success: bool,
    /// Highest matching index on this node after the RPC.
    pub match_index: u64,
}

/// File-backed KRaft-style metadata log + hard state + metrics.
#[derive(Debug)]
pub struct MetadataRaftState {
    log_path: PathBuf,
    hard_path: PathBuf,
    inner: Mutex<MetadataRaftInner>,
    append_success_total: AtomicU64,
    append_fail_total: AtomicU64,
}

#[derive(Debug)]
struct MetadataRaftInner {
    current_term: u64,
    /// 1-based log; `entries[i]` has `index == i+1`.
    entries: Vec<MetadataLogEntry>,
    commit_index: u64,
    last_applied: u64,
}

impl MetadataRaftState {
    /// Open existing metadata raft state under `data_dir`, or start empty.
    ///
    /// Does **not** create `{data_dir}/__metadata_raft/` (v0.214). Persist
    /// creates the directory on first write. Use [`Self::open_enabled`] when
    /// homemade 154 is on so the dir exists at boot.
    pub fn open(data_dir: impl AsRef<Path>) -> Self {
        Self::open_inner(data_dir, false)
    }

    /// Open metadata raft state and create `{data_dir}/__metadata_raft/`.
    ///
    /// Used when `VOLANT_METADATA_RAFT` is enabled at broker construction.
    pub fn open_enabled(data_dir: impl AsRef<Path>) -> Self {
        Self::open_inner(data_dir, true)
    }

    fn open_inner(data_dir: impl AsRef<Path>, create_dir: bool) -> Self {
        let dir = data_dir.as_ref().join(METADATA_RAFT_DIR);
        if create_dir {
            let _ = fs::create_dir_all(&dir);
        }
        let log_path = dir.join(METADATA_RAFT_LOG_FILE);
        let hard_path = dir.join(METADATA_RAFT_HARD_STATE_FILE);
        let log = load_log(&log_path).unwrap_or_default();
        let hard = load_hard(&hard_path).unwrap_or_default();
        // Sanity: clamp commit/applied to log length.
        let last_idx = log.entries.last().map(|e| e.index).unwrap_or(0);
        let commit_index = hard.commit_index.min(last_idx);
        let last_applied = hard.last_applied.min(commit_index);
        Self {
            log_path,
            hard_path,
            inner: Mutex::new(MetadataRaftInner {
                current_term: hard.current_term,
                entries: log.entries,
                commit_index,
                last_applied,
            }),
            append_success_total: AtomicU64::new(0),
            append_fail_total: AtomicU64::new(0),
        }
    }

    /// Current term.
    pub fn current_term(&self) -> u64 {
        self.inner.lock().current_term
    }

    /// Last committed log index.
    pub fn commit_index(&self) -> u64 {
        self.inner.lock().commit_index
    }

    /// Last applied log index.
    pub fn last_applied(&self) -> u64 {
        self.inner.lock().last_applied
    }

    /// Last log index (0 if empty).
    pub fn last_index(&self) -> u64 {
        self.inner
            .lock()
            .entries
            .last()
            .map(|e| e.index)
            .unwrap_or(0)
    }

    /// Term of entry at `index`, or 0 if missing / empty.
    pub fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        let g = self.inner.lock();
        g.entries
            .get((index as usize).saturating_sub(1))
            .filter(|e| e.index == index)
            .map(|e| e.term)
            .unwrap_or(0)
    }

    /// Successful majority append commits.
    pub fn append_success_total(&self) -> u64 {
        self.append_success_total.load(Ordering::Relaxed)
    }

    /// Append majority failures.
    pub fn append_fail_total(&self) -> u64 {
        self.append_fail_total.load(Ordering::Relaxed)
    }

    /// Raft-style majority of `n` configured members (`n/2 + 1`).
    pub fn majority(n: usize) -> usize {
        n / 2 + 1
    }

    /// Ensure `current_term` is at least `term` (leader keeps term stable).
    pub fn ensure_term(&self, term: u64) {
        let mut g = self.inner.lock();
        if term > g.current_term {
            g.current_term = term;
            self.persist_locked(&g);
        }
    }

    /// Leader: append a new command at next index using `current_term`.
    pub fn append_command(&self, payload: MetadataCommand) -> MetadataLogEntry {
        let mut g = self.inner.lock();
        // Bootstrap term 1 on first leader append.
        if g.current_term == 0 {
            g.current_term = 1;
        }
        let index = g.entries.last().map(|e| e.index).unwrap_or(0) + 1;
        let entry = MetadataLogEntry {
            term: g.current_term,
            index,
            payload,
        };
        g.entries.push(entry.clone());
        self.persist_locked(&g);
        entry
    }

    /// Leader: advance `commit_index` when majority has matched up to `index`.
    ///
    /// Never shrinks. Does not bump apply — caller runs [`Self::take_entries_to_apply`].
    pub fn advance_commit(&self, index: u64) {
        let mut g = self.inner.lock();
        let last = g.entries.last().map(|e| e.index).unwrap_or(0);
        let target = index.min(last);
        if target > g.commit_index {
            g.commit_index = target;
            self.persist_locked(&g);
        }
    }

    /// Record majority append success metric.
    pub fn note_append_success(&self) {
        self.append_success_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record majority append failure metric (uncommitted entry retained).
    pub fn note_append_fail(&self) {
        self.append_fail_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Follower/leader shared: simplified AppendEntries.
    ///
    /// Rejects when `prev_log_index`/`prev_log_term` do not match. On success,
    /// truncates any conflicting suffix, appends `entries`, and advances
    /// `commit_index` to `min(leader_commit, last_index)`.
    pub fn append_entries(
        &self,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: &[MetadataLogEntry],
        leader_commit: u64,
    ) -> AppendEntriesResult {
        let mut g = self.inner.lock();
        if term > g.current_term {
            g.current_term = term;
        }
        // Prev log consistency check.
        if prev_log_index > 0 {
            let ok = g
                .entries
                .get((prev_log_index as usize).saturating_sub(1))
                .map(|e| e.index == prev_log_index && e.term == prev_log_term)
                .unwrap_or(false);
            if !ok {
                self.persist_locked(&g);
                return AppendEntriesResult {
                    term: g.current_term,
                    success: false,
                    match_index: g.entries.last().map(|e| e.index).unwrap_or(0),
                };
            }
        } else if prev_log_term != 0 {
            // prev_index 0 implies empty prefix; non-zero term is invalid.
            self.persist_locked(&g);
            return AppendEntriesResult {
                term: g.current_term,
                success: false,
                match_index: 0,
            };
        }

        // Truncate from first new entry index and re-append (idempotent).
        if let Some(first) = entries.first() {
            let keep = (first.index as usize).saturating_sub(1);
            if g.entries.len() > keep {
                g.entries.truncate(keep);
            }
            // Require contiguous append after prev.
            let next = g.entries.last().map(|x| x.index).unwrap_or(0) + 1;
            if first.index != next {
                self.persist_locked(&g);
                return AppendEntriesResult {
                    term: g.current_term,
                    success: false,
                    match_index: g.entries.last().map(|x| x.index).unwrap_or(0),
                };
            }
        }
        for e in entries {
            let next = g.entries.last().map(|x| x.index).unwrap_or(0) + 1;
            if e.index != next {
                self.persist_locked(&g);
                return AppendEntriesResult {
                    term: g.current_term,
                    success: false,
                    match_index: g.entries.last().map(|x| x.index).unwrap_or(0),
                };
            }
            g.entries.push(e.clone());
        }

        let last = g.entries.last().map(|e| e.index).unwrap_or(0);
        let new_commit = leader_commit.min(last);
        if new_commit > g.commit_index {
            g.commit_index = new_commit;
        }
        self.persist_locked(&g);
        AppendEntriesResult {
            term: g.current_term,
            success: true,
            match_index: last,
        }
    }

    /// Drain entries `last_applied+1 ..= commit_index` and advance `last_applied`.
    pub fn take_entries_to_apply(&self) -> Vec<MetadataLogEntry> {
        let mut g = self.inner.lock();
        let mut out = Vec::new();
        while g.last_applied < g.commit_index {
            let next = g.last_applied + 1;
            if let Some(e) = g
                .entries
                .get((next as usize).saturating_sub(1))
                .filter(|e| e.index == next)
                .cloned()
            {
                out.push(e);
                g.last_applied = next;
            } else {
                break;
            }
        }
        if !out.is_empty() {
            self.persist_locked(&g);
        }
        out
    }

    /// Clone entry at index if present.
    pub fn entry_at(&self, index: u64) -> Option<MetadataLogEntry> {
        if index == 0 {
            return None;
        }
        let g = self.inner.lock();
        g.entries
            .get((index as usize).saturating_sub(1))
            .filter(|e| e.index == index)
            .cloned()
    }

    /// Snapshot of uncommitted entries after `after_index` (for fan-out).
    pub fn entries_after(&self, after_index: u64) -> Vec<MetadataLogEntry> {
        let g = self.inner.lock();
        g.entries
            .iter()
            .filter(|e| e.index > after_index)
            .cloned()
            .collect()
    }

    fn persist_locked(&self, g: &MetadataRaftInner) {
        let hard = MetadataRaftHardState {
            version: METADATA_RAFT_FILE_VERSION,
            current_term: g.current_term,
            commit_index: g.commit_index,
            last_applied: g.last_applied,
        };
        let log = MetadataRaftLogFile {
            version: METADATA_RAFT_FILE_VERSION,
            entries: g.entries.clone(),
        };
        if let Err(e) = save_hard(&self.hard_path, &hard) {
            warn!(
                path = %self.hard_path.display(),
                error = %e,
                "metadata raft hard_state persist failed"
            );
        }
        if let Err(e) = save_log(&self.log_path, &log) {
            warn!(
                path = %self.log_path.display(),
                error = %e,
                "metadata raft log persist failed"
            );
        }
    }
}

fn load_log(path: &Path) -> Option<MetadataRaftLogFile> {
    let mut f = File::open(path).ok()?;
    let mut s = String::new();
    f.read_to_string(&mut s).ok()?;
    serde_json::from_str(&s).ok()
}

fn save_log(path: &Path, file: &MetadataRaftLogFile) -> std::io::Result<()> {
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

fn load_hard(path: &Path) -> Option<MetadataRaftHardState> {
    let mut f = File::open(path).ok()?;
    let mut s = String::new();
    f.read_to_string(&mut s).ok()?;
    serde_json::from_str(&s).ok()
}

fn save_hard(path: &Path, file: &MetadataRaftHardState) -> std::io::Result<()> {
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
    use volant_protocol::ClusterPartitionState;

    fn sample_topics() -> Vec<ClusterTopicState> {
        vec![ClusterTopicState {
            name: "t".into(),
            topic_id: 1,
            partitions: vec![ClusterPartitionState {
                partition_id: 0,
                leader: 1,
                leader_epoch: 0,
                replicas: vec![1],
                isr: vec![1],
            }],
        }]
    }

    #[test]
    fn majority_math() {
        assert_eq!(MetadataRaftState::majority(1), 1);
        assert_eq!(MetadataRaftState::majority(2), 2);
        assert_eq!(MetadataRaftState::majority(3), 2);
        assert_eq!(MetadataRaftState::majority(5), 3);
    }

    #[test]
    fn open_does_not_create_dir() {
        let dir = env::temp_dir().join(format!("volant-mraft-nodir-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let _s = MetadataRaftState::open(&dir);
        assert!(
            !dir.join(METADATA_RAFT_DIR).exists(),
            "lazy open must not create __metadata_raft"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_enabled_creates_dir() {
        let dir = env::temp_dir().join(format!("volant-mraft-ondir-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let _s = MetadataRaftState::open_enabled(&dir);
        assert!(
            dir.join(METADATA_RAFT_DIR).is_dir(),
            "open_enabled must create __metadata_raft"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_commit_apply_roundtrip() {
        let dir = env::temp_dir().join(format!("volant-mraft-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        {
            let s = MetadataRaftState::open(&dir);
            let e = s.append_command(MetadataCommand::SetAssignment {
                generation: 1,
                topics: sample_topics(),
            });
            assert_eq!(e.index, 1);
            assert_eq!(e.term, 1);
            s.advance_commit(1);
            assert_eq!(s.commit_index(), 1);
            let applied = s.take_entries_to_apply();
            assert_eq!(applied.len(), 1);
            assert_eq!(s.last_applied(), 1);
        }
        let s2 = MetadataRaftState::open(&dir);
        assert_eq!(s2.last_index(), 1);
        assert_eq!(s2.commit_index(), 1);
        assert_eq!(s2.last_applied(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reject_prev_log_mismatch() {
        let dir = env::temp_dir().join(format!("volant-mraft-rej-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let s = MetadataRaftState::open(&dir);
        let e1 = s.append_command(MetadataCommand::Noop);
        assert_eq!(e1.index, 1);

        // Wrong prev term at index 1.
        let bad = MetadataLogEntry {
            term: 1,
            index: 2,
            payload: MetadataCommand::Noop,
        };
        let r = s.append_entries(1, 1, 99, &[bad], 0);
        assert!(!r.success);
        assert_eq!(s.last_index(), 1);

        // Correct prev → accept.
        let good = MetadataLogEntry {
            term: 1,
            index: 2,
            payload: MetadataCommand::SetAssignment {
                generation: 2,
                topics: sample_topics(),
            },
        };
        let r2 = s.append_entries(1, 1, 1, &[good], 2);
        assert!(r2.success);
        assert_eq!(r2.match_index, 2);
        assert_eq!(s.commit_index(), 2);
        let _ = fs::remove_dir_all(&dir);
    }
}
