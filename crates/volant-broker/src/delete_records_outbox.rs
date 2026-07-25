//! Durable DeleteRecords pending-truncate outbox (Phase 116).
//!
//! Layout: `{data_dir}/__delete_records_outbox/state.json` (atomic replace on write).
//!
//! Leaders enqueue `(replica_id, topic, partition) → before_offset` when
//! Phase 113 best-effort `ReplicaDeleteRecords` fails. A background drain
//! retries live peers at-least-once. Peer apply remains idempotent (log start
//! only advances).

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// On-disk directory name under `data_dir`.
pub const OUTBOX_DIR: &str = "__delete_records_outbox";
/// On-disk snapshot file name.
pub const OUTBOX_FILE: &str = "state.json";
/// File format version.
pub const OUTBOX_FILE_VERSION: u32 = 1;
/// Soft cap on distinct outbox keys (MVP bound).
pub const DEFAULT_MAX_ENTRIES: usize = 10_000;

/// One pending truncate for a single peer replica.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutboxEntry {
    /// Target broker id.
    pub replica_id: u32,
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: u32,
    /// Delete-before offset (desired log start advance).
    pub before_offset: u64,
    /// Leader epoch stamped when the truncate was requested.
    pub leader_epoch: i32,
}

/// Full durable snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutboxFile {
    /// Format version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Pending entries (deduped by key at load).
    #[serde(default)]
    pub entries: Vec<OutboxEntry>,
}

fn default_version() -> u32 {
    OUTBOX_FILE_VERSION
}

impl Default for OutboxFile {
    fn default() -> Self {
        Self {
            version: OUTBOX_FILE_VERSION,
            entries: Vec::new(),
        }
    }
}

/// Map key: `(replica_id, topic, partition)`.
type EntryKey = (u32, String, u32);

fn key_of(e: &OutboxEntry) -> EntryKey {
    (e.replica_id, e.topic.clone(), e.partition)
}

/// File-backed + in-memory DeleteRecords outbox.
#[derive(Debug)]
pub struct DeleteRecordsOutbox {
    path: PathBuf,
    max_entries: usize,
    entries: Mutex<HashMap<EntryKey, OutboxEntry>>,
    enqueued_total: AtomicU64,
    retry_success_total: AtomicU64,
    retry_errors_total: AtomicU64,
    drops_total: AtomicU64,
    persist_errors_total: AtomicU64,
}

impl DeleteRecordsOutbox {
    /// Open (or create empty) outbox under `data_dir/__delete_records_outbox`.
    pub fn open(data_dir: impl AsRef<Path>) -> Self {
        Self::open_with_max(data_dir, DEFAULT_MAX_ENTRIES)
    }

    /// Open with a custom max entry count (tests).
    pub fn open_with_max(data_dir: impl AsRef<Path>, max_entries: usize) -> Self {
        let dir = data_dir.as_ref().join(OUTBOX_DIR);
        let _ = fs::create_dir_all(&dir);
        let path = dir.join(OUTBOX_FILE);
        let map = load_map(&path);
        Self {
            path,
            max_entries: max_entries.max(1),
            entries: Mutex::new(map),
            enqueued_total: AtomicU64::new(0),
            retry_success_total: AtomicU64::new(0),
            retry_errors_total: AtomicU64::new(0),
            drops_total: AtomicU64::new(0),
            persist_errors_total: AtomicU64::new(0),
        }
    }

    /// Number of pending entries.
    pub fn depth(&self) -> u64 {
        self.entries.lock().len() as u64
    }

    /// Successful enqueue / merge count.
    pub fn enqueued_total(&self) -> u64 {
        self.enqueued_total.load(Ordering::Relaxed)
    }

    /// Drain successes.
    pub fn retry_success_total(&self) -> u64 {
        self.retry_success_total.load(Ordering::Relaxed)
    }

    /// Drain transport / peer errors.
    pub fn retry_errors_total(&self) -> u64 {
        self.retry_errors_total.load(Ordering::Relaxed)
    }

    /// Capacity drops.
    pub fn drops_total(&self) -> u64 {
        self.drops_total.load(Ordering::Relaxed)
    }

    /// Persist failures.
    pub fn persist_errors_total(&self) -> u64 {
        self.persist_errors_total.load(Ordering::Relaxed)
    }

    /// Snapshot all pending entries (clone).
    pub fn list(&self) -> Vec<OutboxEntry> {
        self.entries.lock().values().cloned().collect()
    }

    /// Entries whose `replica_id` is in `live_ids` (or all when `live_ids` is empty
    /// and `include_all_if_empty` is true). Prefer live peers for drain.
    pub fn pending_for_replicas(&self, live_ids: &[u32]) -> Vec<OutboxEntry> {
        let live: std::collections::HashSet<u32> = live_ids.iter().copied().collect();
        let guard = self.entries.lock();
        guard
            .values()
            .filter(|e| live.contains(&e.replica_id))
            .cloned()
            .collect()
    }

    /// Enqueue or merge a pending truncate. Returns `true` if the map changed.
    pub fn enqueue(
        &self,
        replica_id: u32,
        topic: &str,
        partition: u32,
        before_offset: u64,
        leader_epoch: i32,
    ) -> bool {
        let k = (replica_id, topic.to_owned(), partition);
        let mut guard = self.entries.lock();
        let mut changed = false;
        if let Some(existing) = guard.get_mut(&k) {
            if before_offset > existing.before_offset {
                existing.before_offset = before_offset;
                existing.leader_epoch = leader_epoch;
                changed = true;
            } else if before_offset == existing.before_offset
                && leader_epoch > existing.leader_epoch
            {
                existing.leader_epoch = leader_epoch;
                changed = true;
            }
            // Count merge/idempotent re-enqueue for metrics.
            self.enqueued_total.fetch_add(1, Ordering::Relaxed);
            if changed {
                self.persist_locked(&guard);
            }
            return true;
        }
        if guard.len() >= self.max_entries {
            self.drops_total.fetch_add(1, Ordering::Relaxed);
            warn!(
                replica_id,
                topic,
                partition,
                max = self.max_entries,
                "delete records outbox full; dropping enqueue"
            );
            return false;
        }
        guard.insert(
            k,
            OutboxEntry {
                replica_id,
                topic: topic.to_owned(),
                partition,
                before_offset,
                leader_epoch,
            },
        );
        self.enqueued_total.fetch_add(1, Ordering::Relaxed);
        self.persist_locked(&guard);
        true
    }

    /// Remove one entry after successful apply (or stale fence drop).
    pub fn remove(
        &self,
        replica_id: u32,
        topic: &str,
        partition: u32,
        before_offset: u64,
    ) -> bool {
        let k = (replica_id, topic.to_owned(), partition);
        let mut guard = self.entries.lock();
        let remove = match guard.get(&k) {
            Some(e) => e.before_offset <= before_offset,
            None => false,
        };
        if remove {
            guard.remove(&k);
            self.persist_locked(&guard);
            true
        } else {
            false
        }
    }

    /// Mark a successful retry (remove + success counter).
    pub fn note_retry_success(
        &self,
        replica_id: u32,
        topic: &str,
        partition: u32,
        before_offset: u64,
    ) {
        if self.remove(replica_id, topic, partition, before_offset) {
            self.retry_success_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Drop a key without success counter (e.g. epoch fence).
    pub fn drop_entry(&self, replica_id: u32, topic: &str, partition: u32) {
        let k = (replica_id, topic.to_owned(), partition);
        let mut guard = self.entries.lock();
        if guard.remove(&k).is_some() {
            self.persist_locked(&guard);
        }
    }

    /// Increment retry error counter.
    pub fn note_retry_error(&self) {
        self.retry_errors_total.fetch_add(1, Ordering::Relaxed);
    }
}

impl DeleteRecordsOutbox {
    fn persist_locked(&self, map: &HashMap<EntryKey, OutboxEntry>) {
        let file = OutboxFile {
            version: OUTBOX_FILE_VERSION,
            entries: map.values().cloned().collect(),
        };
        if save_file(&self.path, &file).is_err() {
            self.persist_errors_total.fetch_add(1, Ordering::Relaxed);
            warn!(path = %self.path.display(), "delete records outbox persist failed");
        }
    }
}

fn load_map(path: &Path) -> HashMap<EntryKey, OutboxEntry> {
    let file = match load_file(path) {
        Ok(f) => f,
        Err(_) => return HashMap::new(),
    };
    let mut map: HashMap<EntryKey, OutboxEntry> = HashMap::new();
    for e in file.entries {
        let k = key_of(&e);
        match map.get(&k) {
            Some(prev) if prev.before_offset >= e.before_offset => {}
            _ => {
                map.insert(k, e);
            }
        }
    }
    map
}

fn load_file(path: &Path) -> Result<OutboxFile, ()> {
    if !path.exists() {
        return Ok(OutboxFile::default());
    }
    let mut f = File::open(path).map_err(|_| ())?;
    let mut buf = String::new();
    f.read_to_string(&mut buf).map_err(|_| ())?;
    if buf.trim().is_empty() {
        return Ok(OutboxFile::default());
    }
    serde_json::from_str(&buf).map_err(|_| ())
}

fn save_file(path: &Path, state: &OutboxFile) -> Result<(), ()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let _ = fs::create_dir_all(parent);
    let tmp = parent.join(format!("{}.tmp", OUTBOX_FILE));
    let json = serde_json::to_string_pretty(state).map_err(|_| ())?;
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|_| ())?;
        f.write_all(json.as_bytes()).map_err(|_| ())?;
        f.sync_all().map_err(|_| ())?;
    }
    fs::rename(&tmp, path).map_err(|_| ())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "volant-dr-outbox-{}-{}",
            std::process::id(),
            nanos
        ))
    }

    #[test]
    fn roundtrip_merge_and_remove() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        let box1 = DeleteRecordsOutbox::open(&dir);
        assert!(box1.enqueue(2, "t", 0, 10, 0));
        assert!(box1.enqueue(2, "t", 0, 20, 1)); // merge max
        assert_eq!(box1.depth(), 1);
        let list = box1.list();
        assert_eq!(list[0].before_offset, 20);
        assert_eq!(list[0].leader_epoch, 1);

        // Reload from disk.
        let box2 = DeleteRecordsOutbox::open(&dir);
        assert_eq!(box2.depth(), 1);
        assert_eq!(box2.list()[0].before_offset, 20);

        box2.note_retry_success(2, "t", 0, 20);
        assert_eq!(box2.depth(), 0);
        assert_eq!(box2.retry_success_total(), 1);

        let box3 = DeleteRecordsOutbox::open(&dir);
        assert_eq!(box3.depth(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn capacity_drop() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        let box1 = DeleteRecordsOutbox::open_with_max(&dir, 1);
        assert!(box1.enqueue(1, "a", 0, 1, 0));
        assert!(!box1.enqueue(2, "b", 0, 1, 0));
        assert_eq!(box1.depth(), 1);
        assert_eq!(box1.drops_total(), 1);
        // merge existing still ok
        assert!(box1.enqueue(1, "a", 0, 5, 0));
        assert_eq!(box1.list()[0].before_offset, 5);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn pending_for_live() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        let box1 = DeleteRecordsOutbox::open(&dir);
        box1.enqueue(2, "t", 0, 1, 0);
        box1.enqueue(3, "t", 0, 1, 0);
        let live = box1.pending_for_replicas(&[3]);
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].replica_id, 3);
        let _ = fs::remove_dir_all(&dir);
    }
}
