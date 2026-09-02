//! Per-partition Raft log MVP (v0.12).
//!
//! Tiny in-process log: term, index, vote, commit_index. Leader is the
//! **current partition leader** — no second election. Followers accept
//! AppendEntries-shaped entries. Dual-write only; does **not** replace the
//! mmap [`volant_storage::PartitionLog`] or ISR HWM.
//!
//! Persist: `{data_dir}/__partition_raft/{topic}/{partition}/log.json`.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// On-disk directory under `data_dir`.
pub const PARTITION_RAFT_DIR: &str = "__partition_raft";
/// Durable log file name.
pub const PARTITION_RAFT_LOG_FILE: &str = "log.json";
/// Durable hard state file name.
pub const PARTITION_RAFT_HARD_STATE_FILE: &str = "hard_state.json";
/// File format version.
pub const PARTITION_RAFT_FILE_VERSION: u32 = 1;

/// Produce (or other) payload stored in the partition Raft log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionRaftPayload {
    /// Partition log offset this entry dual-writes.
    pub offset: u64,
    /// CRC of the produce payload (or 0 when unused).
    pub crc: u32,
}

/// One ordered partition Raft log entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionRaftEntry {
    /// Leader term that authored this entry (no election; partition leader term).
    pub term: u64,
    /// Monotonic log index (1-based).
    pub index: u64,
    /// Dual-write payload.
    pub payload: PartitionRaftPayload,
}

/// Durable hard state (term / vote / commit / last_applied).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionRaftHardState {
    /// Schema version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Current term (leader-authored; no RequestVote).
    #[serde(default)]
    pub current_term: u64,
    /// Last vote in `current_term` (`None` = not voted).
    #[serde(default)]
    pub voted_for: Option<u32>,
    /// Highest log index known to be committed.
    #[serde(default)]
    pub commit_index: u64,
    /// Highest log index applied locally.
    #[serde(default)]
    pub last_applied: u64,
}

fn default_version() -> u32 {
    PARTITION_RAFT_FILE_VERSION
}

impl Default for PartitionRaftHardState {
    fn default() -> Self {
        Self {
            version: PARTITION_RAFT_FILE_VERSION,
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
        }
    }
}

/// Durable log file wrapper.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
struct PartitionRaftLogFile {
    #[serde(default = "default_version")]
    version: u32,
    #[serde(default)]
    entries: Vec<PartitionRaftEntry>,
}

/// Result of a follower AppendEntries attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionAppendResult {
    /// Peer's current term.
    pub term: u64,
    /// Whether prev_log matched and entries were accepted.
    pub success: bool,
    /// Highest matching index on this node after the RPC.
    pub match_index: u64,
}

/// File-backed per-partition Raft log + hard state.
#[derive(Debug)]
pub struct PartitionRaftState {
    log_path: PathBuf,
    hard_path: PathBuf,
    inner: Mutex<PartitionRaftInner>,
    append_success_total: AtomicU64,
    append_fail_total: AtomicU64,
}

#[derive(Debug)]
struct PartitionRaftInner {
    current_term: u64,
    voted_for: Option<u32>,
    /// 1-based log; `entries[i]` has `index == i+1`.
    entries: Vec<PartitionRaftEntry>,
    commit_index: u64,
    last_applied: u64,
    /// Leader volatile match_index per replica id (not persisted).
    match_index: HashMap<u32, u64>,
}

impl PartitionRaftState {
    /// Open or create empty state under `{data_dir}/__partition_raft/{topic}/{partition}/`.
    pub fn open(data_dir: impl AsRef<Path>, topic: &str, partition: u32) -> Self {
        let dir = data_dir
            .as_ref()
            .join(PARTITION_RAFT_DIR)
            .join(topic)
            .join(partition.to_string());
        let _ = fs::create_dir_all(&dir);
        let log_path = dir.join(PARTITION_RAFT_LOG_FILE);
        let hard_path = dir.join(PARTITION_RAFT_HARD_STATE_FILE);
        let log = load_log(&log_path).unwrap_or_default();
        let hard = load_hard(&hard_path).unwrap_or_default();
        let last_idx = log.entries.last().map(|e| e.index).unwrap_or(0);
        let commit_index = hard.commit_index.min(last_idx);
        let last_applied = hard.last_applied.min(commit_index);
        Self {
            log_path,
            hard_path,
            inner: Mutex::new(PartitionRaftInner {
                current_term: hard.current_term,
                voted_for: hard.voted_for,
                entries: log.entries,
                commit_index,
                last_applied,
                match_index: HashMap::new(),
            }),
            append_success_total: AtomicU64::new(0),
            append_fail_total: AtomicU64::new(0),
        }
    }

    /// Raft-style majority of `n` members (`n/2 + 1`).
    pub fn majority(n: usize) -> usize {
        n / 2 + 1
    }

    /// Current term.
    pub fn current_term(&self) -> u64 {
        self.inner.lock().current_term
    }

    /// Last vote in the current term.
    pub fn voted_for(&self) -> Option<u32> {
        self.inner.lock().voted_for
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

    /// Term of entry at `index`, or 0 if missing.
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

    /// Successful majority-commit count (tests / metrics).
    pub fn append_success_total(&self) -> u64 {
        self.append_success_total.load(Ordering::Relaxed)
    }

    /// Majority-commit failure count.
    pub fn append_fail_total(&self) -> u64 {
        self.append_fail_total.load(Ordering::Relaxed)
    }

    /// Record a vote for `candidate` in `term` (no election; store only).
    ///
    /// Rejects if already voted for a different candidate in the same term.
    pub fn record_vote(&self, term: u64, candidate: u32) -> bool {
        let mut g = self.inner.lock();
        if term > g.current_term {
            g.current_term = term;
            g.voted_for = None;
        }
        if term < g.current_term {
            return false;
        }
        match g.voted_for {
            None => {
                g.voted_for = Some(candidate);
                self.persist_locked(&g);
                true
            }
            Some(id) if id == candidate => true,
            Some(_) => false,
        }
    }

    /// Ensure `current_term` is at least `term`.
    pub fn ensure_term(&self, term: u64) {
        let mut g = self.inner.lock();
        if term > g.current_term {
            g.current_term = term;
            g.voted_for = None;
            self.persist_locked(&g);
        }
    }

    /// Leader: append a produce dual-write entry at the next index.
    pub fn append_produce(&self, offset: u64, crc: u32) -> PartitionRaftEntry {
        let mut g = self.inner.lock();
        if g.current_term == 0 {
            g.current_term = 1;
        }
        let index = g.entries.last().map(|e| e.index).unwrap_or(0) + 1;
        let entry = PartitionRaftEntry {
            term: g.current_term,
            index,
            payload: PartitionRaftPayload { offset, crc },
        };
        g.entries.push(entry.clone());
        self.persist_locked(&g);
        entry
    }

    /// Record a replica's match index (leader volatile state).
    pub fn record_match(&self, replica_id: u32, index: u64) {
        let mut g = self.inner.lock();
        let e = g.match_index.entry(replica_id).or_insert(0);
        if index > *e {
            *e = index;
        }
    }

    /// Current match index for `replica_id` (0 if unknown).
    pub fn match_index_of(&self, replica_id: u32) -> u64 {
        self.inner
            .lock()
            .match_index
            .get(&replica_id)
            .copied()
            .unwrap_or(0)
    }

    /// Advance `commit_index` when majority of `replica_count` have matched.
    ///
    /// Self match must already be recorded. Never shrinks. Returns whether
    /// `commit_index` advanced.
    pub fn try_commit_majority(&self, replica_count: usize) -> bool {
        let need = Self::majority(replica_count.max(1));
        let mut g = self.inner.lock();
        let last = g.entries.last().map(|e| e.index).unwrap_or(0);
        if last == 0 {
            return false;
        }
        let mut matched: Vec<u64> = g.match_index.values().copied().collect();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let majority_idx = if matched.len() >= need {
            matched[need - 1]
        } else {
            0
        };
        let target = majority_idx.min(last);
        if target > g.commit_index {
            g.commit_index = target;
            self.persist_locked(&g);
            self.append_success_total.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Leader: force-advance `commit_index` (tests / single-replica).
    pub fn advance_commit(&self, index: u64) {
        let mut g = self.inner.lock();
        let last = g.entries.last().map(|e| e.index).unwrap_or(0);
        let target = index.min(last);
        if target > g.commit_index {
            g.commit_index = target;
            self.persist_locked(&g);
        }
    }

    /// Record majority-commit failure (uncommitted entry retained).
    pub fn note_append_fail(&self) {
        self.append_fail_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Follower/leader shared: simplified AppendEntries.
    pub fn append_entries(
        &self,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: &[PartitionRaftEntry],
        leader_commit: u64,
    ) -> PartitionAppendResult {
        let mut g = self.inner.lock();
        if term > g.current_term {
            g.current_term = term;
            g.voted_for = None;
        }
        if prev_log_index > 0 {
            let ok = g
                .entries
                .get((prev_log_index as usize).saturating_sub(1))
                .map(|e| e.index == prev_log_index && e.term == prev_log_term)
                .unwrap_or(false);
            if !ok {
                self.persist_locked(&g);
                return PartitionAppendResult {
                    term: g.current_term,
                    success: false,
                    match_index: g.entries.last().map(|e| e.index).unwrap_or(0),
                };
            }
        } else if prev_log_term != 0 {
            self.persist_locked(&g);
            return PartitionAppendResult {
                term: g.current_term,
                success: false,
                match_index: 0,
            };
        }

        if let Some(first) = entries.first() {
            let keep = (first.index as usize).saturating_sub(1);
            if g.entries.len() > keep {
                g.entries.truncate(keep);
            }
            let next = g.entries.last().map(|x| x.index).unwrap_or(0) + 1;
            if first.index != next {
                self.persist_locked(&g);
                return PartitionAppendResult {
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
                return PartitionAppendResult {
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
        PartitionAppendResult {
            term: g.current_term,
            success: true,
            match_index: last,
        }
    }

    /// Drain entries `last_applied+1 ..= commit_index` and advance `last_applied`.
    pub fn take_entries_to_apply(&self) -> Vec<PartitionRaftEntry> {
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
    pub fn entry_at(&self, index: u64) -> Option<PartitionRaftEntry> {
        if index == 0 {
            return None;
        }
        let g = self.inner.lock();
        g.entries
            .get((index as usize).saturating_sub(1))
            .filter(|e| e.index == index)
            .cloned()
    }

    fn persist_locked(&self, g: &PartitionRaftInner) {
        let hard = PartitionRaftHardState {
            version: PARTITION_RAFT_FILE_VERSION,
            current_term: g.current_term,
            voted_for: g.voted_for,
            commit_index: g.commit_index,
            last_applied: g.last_applied,
        };
        let log = PartitionRaftLogFile {
            version: PARTITION_RAFT_FILE_VERSION,
            entries: g.entries.clone(),
        };
        if let Err(e) = save_hard(&self.hard_path, &hard) {
            warn!(
                path = %self.hard_path.display(),
                error = %e,
                "partition raft hard_state persist failed"
            );
        }
        if let Err(e) = save_log(&self.log_path, &log) {
            warn!(
                path = %self.log_path.display(),
                error = %e,
                "partition raft log persist failed"
            );
        }
    }
}

/// In-process multi-replica group (no OS processes, no election).
///
/// Leader is supplied by the caller (reuse ISR / partition leader).
#[derive(Debug)]
pub struct PartitionRaftGroup {
    leader_id: u32,
    replica_ids: Vec<u32>,
    replicas: HashMap<u32, PartitionRaftState>,
}

impl PartitionRaftGroup {
    /// Open one [`PartitionRaftState`] per replica under `{base}/r{id}/`.
    pub fn open(
        base: impl AsRef<Path>,
        topic: &str,
        partition: u32,
        replica_ids: &[u32],
        leader_id: u32,
    ) -> Self {
        let mut replicas = HashMap::new();
        for &id in replica_ids {
            replicas.insert(
                id,
                PartitionRaftState::open(base.as_ref().join(format!("r{id}")), topic, partition),
            );
        }
        Self {
            leader_id,
            replica_ids: replica_ids.to_vec(),
            replicas,
        }
    }

    /// Configured replica count.
    pub fn len(&self) -> usize {
        self.replica_ids.len()
    }

    /// Whether the group has no replicas.
    pub fn is_empty(&self) -> bool {
        self.replica_ids.is_empty()
    }

    /// Leader replica id.
    pub fn leader_id(&self) -> u32 {
        self.leader_id
    }

    /// Borrow one replica log.
    pub fn replica(&self, id: u32) -> Option<&PartitionRaftState> {
        self.replicas.get(&id)
    }

    /// Leader append + replicate to `ack_ids`. Advances commit only on majority.
    ///
    /// `ack_ids` should include the leader to count its match. Followers not
    /// listed do not receive AppendEntries (minority / partition).
    ///
    /// Returns `(index, committed)`.
    pub fn append_replicated(&self, offset: u64, crc: u32, ack_ids: &[u32]) -> (u64, bool) {
        let leader = self.replicas.get(&self.leader_id).expect("leader replica");
        let entry = leader.append_produce(offset, crc);
        leader.record_match(self.leader_id, entry.index);
        let prev_idx = entry.index.saturating_sub(1);
        let prev_term = leader.term_at(prev_idx);

        for &id in ack_ids {
            if id == self.leader_id {
                continue;
            }
            let Some(f) = self.replicas.get(&id) else {
                continue;
            };
            let r = f.append_entries(
                entry.term,
                prev_idx,
                prev_term,
                std::slice::from_ref(&entry),
                0,
            );
            if r.success {
                leader.record_match(id, r.match_index);
            }
        }

        let committed = leader.try_commit_majority(self.replica_ids.len());
        if !committed {
            leader.note_append_fail();
        } else {
            let commit = leader.commit_index();
            // Push commit so acking followers can apply.
            for &id in ack_ids {
                if id == self.leader_id {
                    continue;
                }
                if let Some(f) = self.replicas.get(&id) {
                    let _ = f.append_entries(entry.term, entry.index, entry.term, &[], commit);
                }
            }
        }
        (entry.index, committed)
    }

    /// Apply committed entries on `id`.
    pub fn apply(&self, id: u32) -> Vec<PartitionRaftEntry> {
        self.replicas
            .get(&id)
            .map(|r| r.take_entries_to_apply())
            .unwrap_or_default()
    }
}

fn load_log(path: &Path) -> Option<PartitionRaftLogFile> {
    let mut f = File::open(path).ok()?;
    let mut s = String::new();
    f.read_to_string(&mut s).ok()?;
    serde_json::from_str(&s).ok()
}

fn save_log(path: &Path, file: &PartitionRaftLogFile) -> std::io::Result<()> {
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

fn load_hard(path: &Path) -> Option<PartitionRaftHardState> {
    let mut f = File::open(path).ok()?;
    let mut s = String::new();
    f.read_to_string(&mut s).ok()?;
    serde_json::from_str(&s).ok()
}

fn save_hard(path: &Path, file: &PartitionRaftHardState) -> std::io::Result<()> {
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

/// Whether `VOLANT_PARTITION_RAFT` is on (`1`/`true`/`yes`). Default **off**.
pub fn partition_raft_env_enabled() -> bool {
    match std::env::var("VOLANT_PARTITION_RAFT") {
        Ok(s) => {
            let t = s.trim();
            t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes")
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "volant-praft-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn majority_math() {
        assert_eq!(PartitionRaftState::majority(1), 1);
        assert_eq!(PartitionRaftState::majority(2), 2);
        assert_eq!(PartitionRaftState::majority(3), 2);
        assert_eq!(PartitionRaftState::majority(5), 3);
    }

    #[test]
    fn persist_roundtrip() {
        let dir = tmp("rt");
        {
            let s = PartitionRaftState::open(&dir, "t", 0);
            let e = s.append_produce(7, 0xabcd);
            assert_eq!(e.index, 1);
            s.record_match(1, 1);
            assert!(s.try_commit_majority(1));
            assert_eq!(s.commit_index(), 1);
            let applied = s.take_entries_to_apply();
            assert_eq!(applied.len(), 1);
            assert_eq!(applied[0].payload.offset, 7);
        }
        let s2 = PartitionRaftState::open(&dir, "t", 0);
        assert_eq!(s2.last_index(), 1);
        assert_eq!(s2.commit_index(), 1);
        assert_eq!(s2.last_applied(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn vote_sticky_in_term() {
        let dir = tmp("vote");
        let s = PartitionRaftState::open(&dir, "t", 0);
        assert!(s.record_vote(1, 2));
        assert_eq!(s.voted_for(), Some(2));
        assert!(s.record_vote(1, 2));
        assert!(!s.record_vote(1, 3));
        assert!(s.record_vote(2, 3));
        assert_eq!(s.voted_for(), Some(3));
        let _ = fs::remove_dir_all(&dir);
    }
}
