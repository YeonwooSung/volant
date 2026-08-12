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
//! **Resource bounds:** entry count is capped at
//! [`MAX_TRUNCATE_JOURNAL_ENTRIES`]; wire snapshots larger than
//! [`MAX_TRUNCATE_JOURNAL_SNAPSHOT_BYTES`] or with more than that many entries
//! are rejected. New keys are refused / skipped at the cap; max-merge of
//! existing keys still works. Topic deletion prunes journal entries
//! (`remove_topic`) without bumping generation.
//!
//! **Phase 137 known-topic filter:** [`Self::apply_push_filtered`] can skip
//! snapshot entries whose topic is not in a caller-supplied set so deleted
//! topics cannot resurrect via peer push. Unfiltered [`Self::apply_push`]
//! (`known_topics = None`) still accepts all keys (unit-test / direct use).
//!
//! Not full Raft log replication / leader election (openraft KRaft still deferred).

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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

/// Soft cap on distinct `(topic, partition)` watermarks.
///
/// When the map is at this size, `note` refuses brand-new keys (existing keys
/// still max-merge) and `apply_push` skips brand-new keys beyond the cap.
pub const MAX_TRUNCATE_JOURNAL_ENTRIES: usize = 100_000;

/// Max accepted `TruncateJournalPush` snapshot payload size (4 MiB).
///
/// Tighter than the protocol `MAX_PAYLOAD` (16 MiB) so journal apply cannot
/// force multi-megabyte JSON parse + merge work beyond this bound.
pub const MAX_TRUNCATE_JOURNAL_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;

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
    /// Operational entry cap (defaults to [`MAX_TRUNCATE_JOURNAL_ENTRIES`]).
    max_entries: usize,
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
    /// Phase 131: successful heartbeat rejoin journal catch-up pushes.
    journal_catchup_success_total: AtomicU64,
    /// Phase 131: failed heartbeat rejoin journal catch-up pushes.
    journal_catchup_errors_total: AtomicU64,
    /// Once-per-process warn when new keys are refused / skipped at cap.
    cap_warned: AtomicBool,
}

impl TruncateJournal {
    /// Open or create empty journal under `data_dir`.
    pub fn open(data_dir: impl AsRef<Path>) -> Self {
        Self::open_with_max(data_dir, MAX_TRUNCATE_JOURNAL_ENTRIES)
    }

    /// Open with a custom max entry count (unit tests / lower bound).
    pub fn open_with_max(data_dir: impl AsRef<Path>, max_entries: usize) -> Self {
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
            max_entries: max_entries.max(1),
            entries: RwLock::new(map),
            persist_lock: Mutex::new(()),
            generation: AtomicU64::new(file.generation),
            applied_generation: AtomicU64::new(file.generation),
            note_total: AtomicU64::new(0),
            push_apply_total: AtomicU64::new(0),
            persist_errors_total: AtomicU64::new(0),
            consensus_success_total: AtomicU64::new(0),
            consensus_fail_total: AtomicU64::new(0),
            journal_catchup_success_total: AtomicU64::new(0),
            journal_catchup_errors_total: AtomicU64::new(0),
            cap_warned: AtomicBool::new(false),
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

    /// Phase 131: successful journal rejoin catch-up pushes.
    pub fn journal_catchup_success_total(&self) -> u64 {
        self.journal_catchup_success_total.load(Ordering::Relaxed)
    }

    /// Phase 131: failed journal rejoin catch-up pushes.
    pub fn journal_catchup_errors_total(&self) -> u64 {
        self.journal_catchup_errors_total.load(Ordering::Relaxed)
    }

    pub(crate) fn note_consensus_success(&self) {
        self.consensus_success_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_consensus_fail(&self) {
        self.consensus_fail_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_journal_catchup_success(&self) {
        self.journal_catchup_success_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_journal_catchup_error(&self) {
        self.journal_catchup_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Raft-style majority of `n` configured members (`n/2 + 1`).
    pub fn majority(n: usize) -> usize {
        n / 2 + 1
    }

    /// Number of watermarks.
    pub fn entry_count(&self) -> usize {
        self.entries.read().len()
    }

    /// Operational max entry count (usually [`MAX_TRUNCATE_JOURNAL_ENTRIES`]).
    pub fn max_entries(&self) -> usize {
        self.max_entries
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
    ///
    /// If the map is at [`Self::max_entries`] and `topic`/`partition` is a
    /// **new** key, the insert is refused (current generation returned, no
    /// persist). Max-merge updates of existing keys are always allowed.
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
        if !map.contains_key(&key) && map.len() >= self.max_entries {
            self.warn_cap_once("note refused new key: truncate journal at entry cap");
            drop(map);
            return self.generation();
        }
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

    /// Remove all watermarks for `topic`. Persists if any removed.
    ///
    /// Does **not** bump generation (prune is local hygiene; peers drop the
    /// same keys via their own `delete_topic` / eventual re-push without those
    /// entries). Returns the number of entries removed.
    pub fn remove_topic(&self, topic: &str) -> usize {
        if topic.is_empty() {
            return 0;
        }
        let mut map = self.entries.write();
        let before = map.len();
        map.retain(|k, _| k.0 != topic);
        let removed = before - map.len();
        drop(map);
        if removed > 0 {
            self.persist();
        }
        removed
    }

    /// Remove a single `(topic, partition)` watermark. Persists if removed.
    /// Does not bump generation. Returns whether an entry was present.
    pub fn remove_partition(&self, topic: &str, partition: u32) -> bool {
        if topic.is_empty() {
            return false;
        }
        let mut map = self.entries.write();
        let removed = map.remove(&(topic.to_owned(), partition)).is_some();
        drop(map);
        if removed {
            self.persist();
        }
        removed
    }

    /// Encode full snapshot JSON for push (compact).
    pub fn snapshot_bytes(&self) -> Bytes {
        let file = self.to_file();
        let json = serde_json::to_vec(&file).unwrap_or_default();
        Bytes::from(json)
    }

    /// Apply a peer/controller push snapshot (Phase 129/130).
    ///
    /// Unfiltered: accepts all snapshot keys. Prefer
    /// [`Self::apply_push_filtered`] from the broker push path so deleted
    /// topics cannot resurrect (Phase 137).
    pub fn apply_push(&self, generation: u64, snapshot: &[u8]) -> Result<(), String> {
        self.apply_push_filtered(generation, snapshot, None)
    }

    /// Apply a peer/controller push snapshot with optional known-topic filter.
    ///
    /// **Max-merges** entries into the local map (multi-controller safe) and
    /// advances generation to `max(local, push)`. Always merges entry maxes
    /// even for older push generations so a lagging push cannot shrink state.
    ///
    /// When `known_topics` is `Some(set)`, entries whose `topic` is **not** in
    /// the set are skipped (Phase 137 anti-resurrection). `None` accepts all
    /// keys (backward-compatible unit-test path).
    ///
    /// Rejects oversized payloads (`MAX_TRUNCATE_JOURNAL_SNAPSHOT_BYTES`) and
    /// snapshots with more than `max_entries` raw entries. When the local map
    /// is already at cap, only **existing** keys are updated; brand-new keys
    /// beyond the cap are skipped (with a one-shot warn).
    pub fn apply_push_filtered(
        &self,
        generation: u64,
        snapshot: &[u8],
        known_topics: Option<&HashSet<String>>,
    ) -> Result<(), String> {
        if snapshot.len() > MAX_TRUNCATE_JOURNAL_SNAPSHOT_BYTES {
            return Err(format!(
                "truncate journal snapshot too large: {} bytes > max {}",
                snapshot.len(),
                MAX_TRUNCATE_JOURNAL_SNAPSHOT_BYTES
            ));
        }
        let file: TruncateJournalFile =
            serde_json::from_slice(snapshot).map_err(|e| format!("parse journal snapshot: {e}"))?;
        if file.entries.len() > self.max_entries {
            return Err(format!(
                "truncate journal snapshot has too many entries: {} > max {}",
                file.entries.len(),
                self.max_entries
            ));
        }
        let mut changed = false;
        {
            let mut map = self.entries.write();
            for e in file.entries {
                if e.before_offset == 0 || e.topic.is_empty() {
                    continue;
                }
                if let Some(known) = known_topics {
                    if !known.contains(&e.topic) {
                        continue;
                    }
                }
                let key = (e.topic.clone(), e.partition);
                if !map.contains_key(&key) && map.len() >= self.max_entries {
                    self.warn_cap_once(
                        "apply_push skipped new key: truncate journal at entry cap",
                    );
                    continue;
                }
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

    fn warn_cap_once(&self, msg: &str) {
        if !self.cap_warned.swap(true, Ordering::Relaxed) {
            warn!(
                max_entries = self.max_entries,
                "{msg}"
            );
        }
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
    // Pretty JSON on disk for ops readability; wire uses compact `snapshot_bytes`.
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

    #[test]
    fn remove_topic_prunes_and_reloads_clean() {
        let dir = temp();
        let _ = fs::remove_dir_all(&dir);
        let j = TruncateJournal::open(&dir);
        let gen_before = j.note("keep", 0, 10, 1, true);
        j.note("gone", 0, 20, 1, true);
        j.note("gone", 1, 30, 1, true);
        assert_eq!(j.entry_count(), 3);
        assert_eq!(j.watermark("gone", 0), Some(20));
        assert_eq!(j.watermark("gone", 1), Some(30));

        let removed = j.remove_topic("gone");
        assert_eq!(removed, 2);
        // Prune does not bump generation.
        assert_eq!(j.generation(), gen_before + 2); // two successful notes after first
        assert_eq!(j.entry_count(), 1);
        assert_eq!(j.watermark("keep", 0), Some(10));
        assert_eq!(j.watermark("gone", 0), None);
        assert_eq!(j.list().len(), 1);
        assert_eq!(j.list()[0].topic, "keep");

        // Second remove is a no-op.
        assert_eq!(j.remove_topic("gone"), 0);

        let j2 = TruncateJournal::open(&dir);
        assert_eq!(j2.entry_count(), 1);
        assert_eq!(j2.watermark("keep", 0), Some(10));
        assert_eq!(j2.watermark("gone", 0), None);
        assert!(j2.list().iter().all(|e| e.topic == "keep"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn remove_partition_single_key() {
        let dir = temp();
        let _ = fs::remove_dir_all(&dir);
        let j = TruncateJournal::open(&dir);
        j.note("t", 0, 10, 1, false);
        j.note("t", 1, 20, 1, false);
        assert!(j.remove_partition("t", 0));
        assert!(!j.remove_partition("t", 0));
        assert_eq!(j.watermark("t", 0), None);
        assert_eq!(j.watermark("t", 1), Some(20));

        let j2 = TruncateJournal::open(&dir);
        assert_eq!(j2.watermark("t", 1), Some(20));
        assert_eq!(j2.watermark("t", 0), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn note_refuses_new_keys_at_cap_allows_existing_merge() {
        let dir = temp();
        let _ = fs::remove_dir_all(&dir);
        let j = TruncateJournal::open_with_max(&dir, 2);
        j.note("a", 0, 10, 1, true);
        j.note("b", 0, 20, 1, true);
        assert_eq!(j.entry_count(), 2);
        let gen = j.generation();

        // New key refused at cap.
        let g = j.note("c", 0, 99, 1, true);
        assert_eq!(g, gen);
        assert_eq!(j.entry_count(), 2);
        assert_eq!(j.watermark("c", 0), None);

        // Existing key still max-merges.
        j.note("a", 0, 50, 2, true);
        assert_eq!(j.watermark("a", 0), Some(50));
        assert_eq!(j.entry_count(), 2);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn apply_push_rejects_oversized_byte_slice() {
        let dir = temp();
        let _ = fs::remove_dir_all(&dir);
        let j = TruncateJournal::open(&dir);
        let big = vec![b'x'; MAX_TRUNCATE_JOURNAL_SNAPSHOT_BYTES + 1];
        let err = j.apply_push(1, &big).unwrap_err();
        assert!(
            err.contains("too large"),
            "expected size rejection, got: {err}"
        );
        assert!(err.contains(&MAX_TRUNCATE_JOURNAL_SNAPSHOT_BYTES.to_string()));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn apply_push_rejects_too_many_entries() {
        let dir = temp();
        let _ = fs::remove_dir_all(&dir);
        let j = TruncateJournal::open_with_max(&dir, 3);
        let file = TruncateJournalFile {
            version: TRUNCATE_JOURNAL_FILE_VERSION,
            generation: 1,
            entries: (0..4)
                .map(|i| TruncateJournalEntry {
                    topic: format!("t{i}"),
                    partition: 0,
                    before_offset: 1,
                    leader_epoch: 0,
                })
                .collect(),
        };
        let snap = serde_json::to_vec(&file).unwrap();
        let err = j.apply_push(1, &snap).unwrap_err();
        assert!(
            err.contains("too many entries"),
            "expected entry-count rejection, got: {err}"
        );
        assert_eq!(j.entry_count(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn apply_push_skips_new_keys_past_cap_updates_existing() {
        let dir = temp();
        let _ = fs::remove_dir_all(&dir);
        // Cap 2; local already full. Push payload must have len <= max (else hard reject).
        let j = TruncateJournal::open_with_max(&dir, 2);
        j.note("a", 0, 10, 1, false);
        j.note("b", 0, 20, 1, false);
        assert_eq!(j.entry_count(), 2);

        let file = TruncateJournalFile {
            version: TRUNCATE_JOURNAL_FILE_VERSION,
            generation: 5,
            entries: vec![
                TruncateJournalEntry {
                    topic: "a".into(),
                    partition: 0,
                    before_offset: 100, // update existing
                    leader_epoch: 2,
                },
                TruncateJournalEntry {
                    topic: "c".into(),
                    partition: 0,
                    before_offset: 50, // new — skip at cap
                    leader_epoch: 1,
                },
            ],
        };
        let snap = serde_json::to_vec(&file).unwrap();
        j.apply_push(5, &snap).unwrap();

        assert_eq!(j.entry_count(), 2, "must not grow past cap");
        assert_eq!(j.watermark("a", 0), Some(100));
        assert_eq!(j.watermark("b", 0), Some(20));
        assert_eq!(j.watermark("c", 0), None);
        assert_eq!(j.applied_generation(), 5);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn public_cap_constants_documented() {
        assert_eq!(MAX_TRUNCATE_JOURNAL_ENTRIES, 100_000);
        assert_eq!(MAX_TRUNCATE_JOURNAL_SNAPSHOT_BYTES, 4 * 1024 * 1024);
        assert!(MAX_TRUNCATE_JOURNAL_SNAPSHOT_BYTES < 16 * 1024 * 1024);
    }

    /// Phase 137: filtered apply skips topics not in the known set (anti-resurrection).
    #[test]
    fn apply_push_skips_unknown_topics_when_filtered() {
        let dir = temp();
        let _ = fs::remove_dir_all(&dir);
        let file = TruncateJournalFile {
            version: TRUNCATE_JOURNAL_FILE_VERSION,
            generation: 3,
            entries: vec![
                TruncateJournalEntry {
                    topic: "gone".into(),
                    partition: 0,
                    before_offset: 99,
                    leader_epoch: 1,
                },
                TruncateJournalEntry {
                    topic: "alive".into(),
                    partition: 0,
                    before_offset: 42,
                    leader_epoch: 1,
                },
            ],
        };
        let snap = serde_json::to_vec(&file).unwrap();

        let filtered = TruncateJournal::open(dir.join("filtered"));
        assert_eq!(filtered.entry_count(), 0);
        let mut known = HashSet::new();
        known.insert("alive".to_owned());
        filtered
            .apply_push_filtered(3, &snap, Some(&known))
            .unwrap();
        assert_eq!(filtered.watermark("gone", 0), None);
        assert_eq!(filtered.watermark("alive", 0), Some(42));
        assert_eq!(filtered.applied_generation(), 3);

        // Unfiltered path still accepts all keys.
        let unfiltered = TruncateJournal::open(dir.join("unfiltered"));
        unfiltered.apply_push(3, &snap).unwrap();
        assert_eq!(unfiltered.watermark("gone", 0), Some(99));
        assert_eq!(unfiltered.watermark("alive", 0), Some(42));

        let _ = fs::remove_dir_all(&dir);
    }
}
