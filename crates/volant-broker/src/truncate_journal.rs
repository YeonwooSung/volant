//! Multi-controller truncate journal (Phase 129 + majority consensus Phase 130).
//!
//! Layout: `{data_dir}/__truncate_journal/state.json`
//!
//! Records the **desired log start** (delete-before offset) per
//! `(topic, partition)` with max-merge semantics. Phase 130: any broker can
//! durable-note; a proposer waits for a **majority** of configured brokers to
//! ack the note (Raft-style commit rule), then best-effort snapshot-pushes
//! remaining peers. New leaders reconcile as
//! `max(local log_start, journal watermark)`.
//!
//! Not full Raft log replication / leader election (openraft KRaft still deferred).

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// On-disk directory under `data_dir`.
pub const TRUNCATE_JOURNAL_DIR: &str = "__truncate_journal";
/// Snapshot file name.
pub const TRUNCATE_JOURNAL_FILE: &str = "state.json";
/// File format version.
pub const TRUNCATE_JOURNAL_FILE_VERSION: u32 = 1;

/// One partition truncate watermark.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TruncateJournalEntry {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
    /// Desired log start (max wins).
    pub before_offset: u64,
    /// Leader epoch stamped with the note (`-1` unknown).
    pub leader_epoch: i32,
}

/// Durable journal snapshot (controller SoT + peer cache).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TruncateJournalFile {
    /// Schema version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Monotonic generation (controller bumps on note).
    #[serde(default)]
    pub generation: u64,
    /// Watermarks (deduped by topic+partition at load).
    #[serde(default)]
    pub entries: Vec<TruncateJournalEntry>,
}

fn default_version() -> u32 {
    TRUNCATE_JOURNAL_FILE_VERSION
}

impl Default for TruncateJournalFile {
    fn default() -> Self {
        Self {
            version: TRUNCATE_JOURNAL_FILE_VERSION,
            generation: 0,
            entries: Vec::new(),
        }
    }
}

type EntryKey = (String, u32);

/// File-backed truncate journal.
#[derive(Debug)]
pub struct TruncateJournal {
    path: PathBuf,
    /// Map key → entry (max-merge).
    entries: RwLock<HashMap<EntryKey, TruncateJournalEntry>>,
    /// Serializes durable snapshot writes (unique tmp + rename).
    persist_lock: Mutex<()>,
    /// Controller / last known generation.
    ///
    /// **Weak / process-local** counter (not a Raft commit index). Peers may
    /// diverge; correctness relies on max-merge of watermarks.
    generation: AtomicU64,
    /// Last applied push generation on this peer.
    applied_generation: AtomicU64,
    note_total: AtomicU64,
    push_apply_total: AtomicU64,
    persist_errors_total: AtomicU64,
    /// Phase 130: successful majority commits.
    consensus_success_total: AtomicU64,
    /// Phase 130: proposals that failed to reach majority.
    consensus_fail_total: AtomicU64,
}

impl TruncateJournal {
    /// Open or create empty journal under `data_dir`.
    pub fn open(data_dir: impl AsRef<Path>) -> Self {
        let dir = data_dir.as_ref().join(TRUNCATE_JOURNAL_DIR);
        let _ = fs::create_dir_all(&dir);
        let path = dir.join(TRUNCATE_JOURNAL_FILE);
        let file = load_file(&path).unwrap_or_default();
        let mut map = HashMap::new();
        for e in file.entries {
            if e.before_offset == 0 || e.topic.is_empty() {
                continue;
            }
            let k = (e.topic.clone(), e.partition);
            map.entry(k)
                .and_modify(|cur: &mut TruncateJournalEntry| {
                    if e.before_offset > cur.before_offset {
                        *cur = e.clone();
                    } else if e.before_offset == cur.before_offset
                        && e.leader_epoch > cur.leader_epoch
                    {
                        cur.leader_epoch = e.leader_epoch;
                    }
                })
                .or_insert(e);
        }
        Self {
            path,
            entries: RwLock::new(map),
            persist_lock: Mutex::new(()),
            generation: AtomicU64::new(file.generation),
            applied_generation: AtomicU64::new(file.generation),
            note_total: AtomicU64::new(0),
            push_apply_total: AtomicU64::new(0),
            persist_errors_total: AtomicU64::new(0),
            consensus_success_total: AtomicU64::new(0),
            consensus_fail_total: AtomicU64::new(0),
        }
    }

    /// Current generation (controller SoT or last applied).
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Last applied push generation.
    pub fn applied_generation(&self) -> u64 {
        self.applied_generation.load(Ordering::Relaxed)
    }

    /// Notes recorded / merges.
    pub fn note_total(&self) -> u64 {
        self.note_total.load(Ordering::Relaxed)
    }

    /// Successful snapshot applies.
    pub fn push_apply_total(&self) -> u64 {
        self.push_apply_total.load(Ordering::Relaxed)
    }

    /// Durable persist failure count.
    pub fn persist_errors_total(&self) -> u64 {
        self.persist_errors_total.load(Ordering::Relaxed)
    }

    /// Phase 130: majority consensus successes.
    pub fn consensus_success_total(&self) -> u64 {
        self.consensus_success_total.load(Ordering::Relaxed)
    }

    /// Phase 130: majority consensus failures.
    pub fn consensus_fail_total(&self) -> u64 {
        self.consensus_fail_total.load(Ordering::Relaxed)
    }

    pub(crate) fn note_consensus_success(&self) {
        self.consensus_success_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_consensus_fail(&self) {
        self.consensus_fail_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Raft-style majority of `n` configured members (`n/2 + 1`).
    pub fn majority(n: usize) -> usize {
        n / 2 + 1
    }

    /// Number of watermarks.
    pub fn entry_count(&self) -> usize {
        self.entries.read().len()
    }

    /// Desired before_offset for a partition, if known.
    pub fn watermark(&self, topic: &str, partition: u32) -> Option<u64> {
        self.entries
            .read()
            .get(&(topic.to_owned(), partition))
            .map(|e| e.before_offset)
    }

    /// Max-merge a watermark. When `bump_generation`, increments generation
    /// (controller path). Returns new generation.
    pub fn note(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
        leader_epoch: i32,
        bump_generation: bool,
    ) -> u64 {
        if before_offset == 0 || topic.is_empty() {
            return self.generation();
        }
        let mut map = self.entries.write();
        let key = (topic.to_owned(), partition);
        let mut changed = false;
        map.entry(key)
            .and_modify(|cur| {
                if before_offset > cur.before_offset {
                    cur.before_offset = before_offset;
                    cur.leader_epoch = leader_epoch;
                    changed = true;
                } else if before_offset == cur.before_offset && leader_epoch > cur.leader_epoch {
                    cur.leader_epoch = leader_epoch;
                    changed = true;
                }
            })
            .or_insert_with(|| {
                changed = true;
                TruncateJournalEntry {
                    topic: topic.to_owned(),
                    partition,
                    before_offset,
                    leader_epoch,
                }
            });
        drop(map);
        if changed {
            self.note_total.fetch_add(1, Ordering::Relaxed);
            if bump_generation {
                self.generation.fetch_add(1, Ordering::SeqCst);
            }
            self.persist();
        }
        self.generation()
    }

    /// Encode full snapshot JSON for push.
    pub fn snapshot_bytes(&self) -> Bytes {
        let file = self.to_file();
        let json = serde_json::to_vec(&file).unwrap_or_default();
        Bytes::from(json)
    }

    /// Apply a peer/controller push snapshot (Phase 129/130).
    ///
    /// **Max-merges** entries into the local map (multi-controller safe) and
    /// advances generation to `max(local, push)`. Always merges entry maxes
    /// even for older push generations so a lagging push cannot shrink state.
    pub fn apply_push(&self, generation: u64, snapshot: &[u8]) -> Result<(), String> {
        let file: TruncateJournalFile =
            serde_json::from_slice(snapshot).map_err(|e| format!("parse journal snapshot: {e}"))?;
        let mut changed = false;
        {
            let mut map = self.entries.write();
            for e in file.entries {
                if e.before_offset == 0 || e.topic.is_empty() {
                    continue;
                }
                let key = (e.topic.clone(), e.partition);
                map.entry(key)
                    .and_modify(|cur| {
                        if e.before_offset > cur.before_offset {
                            *cur = e.clone();
                            changed = true;
                        } else if e.before_offset == cur.before_offset
                            && e.leader_epoch > cur.leader_epoch
                        {
                            cur.leader_epoch = e.leader_epoch;
                            changed = true;
                        }
                    })
                    .or_insert_with(|| {
                        changed = true;
                        e
                    });
            }
        }
        // Atomic max: concurrent local note(..., bump=true) may fetch_add between
        // a load and store; never let apply_push regress generation.
        let gen_advanced = atomic_fetch_max(&self.generation, generation);
        let _applied_advanced = atomic_fetch_max(&self.applied_generation, generation);
        if gen_advanced {
            changed = true;
        }
        if changed {
            self.push_apply_total.fetch_add(1, Ordering::Relaxed);
            self.persist();
        }
        Ok(())
    }

    /// List all entries (sorted).
    pub fn list(&self) -> Vec<TruncateJournalEntry> {
        let mut v: Vec<_> = self.entries.read().values().cloned().collect();
        v.sort_by(|a, b| {
            a.topic
                .cmp(&b.topic)
                .then(a.partition.cmp(&b.partition))
        });
        v
    }

    fn to_file(&self) -> TruncateJournalFile {
        TruncateJournalFile {
            version: TRUNCATE_JOURNAL_FILE_VERSION,
            generation: self.generation(),
            entries: self.list(),
        }
    }

    fn persist(&self) {
        // Serialize durability so concurrent multi-controller notes cannot
        // interleave fixed-tmp writes or lose the last rename.
        let _guard = self.persist_lock.lock();
        let file = self.to_file();
        if save_file(&self.path, &file).is_err() {
            self.persist_errors_total.fetch_add(1, Ordering::Relaxed);
            warn!(path = %self.path.display(), "truncate journal persist failed");
        }
    }
}

/// Atomically set `atom` to `max(atom, value)`. Returns true if the value increased.
fn atomic_fetch_max(atom: &AtomicU64, value: u64) -> bool {
    let mut cur = atom.load(Ordering::Relaxed);
    loop {
        if value <= cur {
            return false;
        }
        match atom.compare_exchange_weak(cur, value, Ordering::SeqCst, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(observed) => cur = observed,
        }
    }
}

fn load_file(path: &Path) -> Result<TruncateJournalFile, ()> {
    if !path.exists() {
        return Ok(TruncateJournalFile::default());
    }
    let mut f = File::open(path).map_err(|_| ())?;
    let mut buf = String::new();
    f.read_to_string(&mut buf).map_err(|_| ())?;
    if buf.trim().is_empty() {
        return Ok(TruncateJournalFile::default());
    }
    serde_json::from_str(&buf).map_err(|_| ())
}

fn save_file(path: &Path, state: &TruncateJournalFile) -> Result<(), ()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let _ = fs::create_dir_all(parent);
    // Unique tmp name (persist_lock is primary; unique name is defense in depth).
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp = parent.join(format!(
        "{TRUNCATE_JOURNAL_FILE}.{}-{nanos}.tmp",
        std::process::id()
    ));
    let result = (|| {
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
    })();
    if result.is_err() {
        // Best-effort: do not leave unique tmp files after failed write/rename.
        let _ = fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp() -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("volant-tj-{}-{}", std::process::id(), n))
    }

    #[test]
    fn note_max_merge_and_reload() {
        let dir = temp();
        let _ = fs::remove_dir_all(&dir);
        let j = TruncateJournal::open(&dir);
        let g1 = j.note("t", 0, 10, 1, true);
        assert_eq!(g1, 1);
        let g2 = j.note("t", 0, 5, 2, true); // lower offset ignored
        assert_eq!(g2, 1); // no change → no bump? wait we only bump on change
        assert_eq!(j.watermark("t", 0), Some(10));
        let g3 = j.note("t", 0, 20, 3, true);
        assert_eq!(g3, 2);
        assert_eq!(j.watermark("t", 0), Some(20));

        let j2 = TruncateJournal::open(&dir);
        assert_eq!(j2.watermark("t", 0), Some(20));
        assert_eq!(j2.generation(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn push_snapshot_roundtrip() {
        let dir = temp();
        let _ = fs::remove_dir_all(&dir);
        let src = TruncateJournal::open(dir.join("src"));
        src.note("a", 0, 42, 1, true);
        let snap = src.snapshot_bytes();
        let gen = src.generation();

        let dst = TruncateJournal::open(dir.join("dst"));
        dst.apply_push(gen, &snap).unwrap();
        assert_eq!(dst.watermark("a", 0), Some(42));
        assert_eq!(dst.applied_generation(), gen);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Local notes can bump generation ahead of a lagging peer push; apply_push
    /// must max-merge entries without regressing generation.
    #[test]
    fn apply_push_gen_does_not_regress_under_higher_local() {
        let dir = temp();
        let _ = fs::remove_dir_all(&dir);
        let j = TruncateJournal::open(&dir);

        // Bump local generation high via several notes.
        j.note("t", 0, 10, 1, true);
        j.note("t", 0, 20, 2, true);
        j.note("t", 0, 30, 3, true);
        let high_gen = j.generation();
        assert!(high_gen >= 3, "expected gen >= 3 after three bumps, got {high_gen}");
        assert_eq!(j.watermark("t", 0), Some(30));

        // Lagging push: lower generation, partial/stale entry maxes + a new key.
        let stale = TruncateJournalFile {
            version: TRUNCATE_JOURNAL_FILE_VERSION,
            generation: 1,
            entries: vec![
                TruncateJournalEntry {
                    topic: "t".into(),
                    partition: 0,
                    before_offset: 15, // lower than local 30
                    leader_epoch: 1,
                },
                TruncateJournalEntry {
                    topic: "t".into(),
                    partition: 1,
                    before_offset: 100,
                    leader_epoch: 1,
                },
            ],
        };
        let snap = serde_json::to_vec(&stale).unwrap();
        j.apply_push(1, &snap).unwrap();

        // Generation must not regress below the high local value.
        assert_eq!(j.generation(), high_gen);
        // applied_generation is max-merged with push gen (notes do not bump it).
        assert_eq!(j.applied_generation(), 1);
        // Entries still max-merge.
        assert_eq!(j.watermark("t", 0), Some(30)); // local max kept
        assert_eq!(j.watermark("t", 1), Some(100)); // new entry merged

        let _ = fs::remove_dir_all(&dir);
    }
}
