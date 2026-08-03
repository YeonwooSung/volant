//! Durable txn coordinator (Init-owner) registry (Phase 124 + GC Phase 127).
//!
//! Layout: `{data_dir}/__txn_coordinator/state.json` (atomic replace on write).
//!
//! Phases 120–122 keep `transactional_id` / `producer_id` → coordinator
//! `node_id` maps for transparent KafkaTxnForward and sticky FindCoordinator
//! override. Phase 124 persists those maps across restart. Phase 127 adds
//! optional TTL GC so completed / stale mappings do not grow without bound.
//!
//! Default TTL is 24h (`VOLANT_TXN_COORDINATOR_TTL_MS`); `0` disables GC.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// On-disk directory name under `data_dir`.
pub const TXN_COORDINATOR_DIR: &str = "__txn_coordinator";
/// On-disk snapshot file name.
pub const TXN_COORDINATOR_FILE: &str = "state.json";
/// File format version (2 = last-touch timestamps for GC).
pub const TXN_COORDINATOR_FILE_VERSION: u32 = 2;
/// Default registry entry TTL: 24 hours.
pub const DEFAULT_TXN_COORDINATOR_TTL_MS: u64 = 24 * 60 * 60 * 1000;

/// One durable mapping (exported for tests / dumps).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxnCoordinatorEntry {
    /// Kafka transactional id (empty if pid-only).
    #[serde(default)]
    pub transactional_id: String,
    /// Producer id (0 if id-only row in list views).
    #[serde(default)]
    pub producer_id: u64,
    /// Init-owner / coordinator broker node id.
    pub coordinator_node_id: u32,
}

/// Full durable snapshot — mirrors the two in-memory maps + last-touch ms.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxnCoordinatorFile {
    /// Format version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// `transactional_id` → coordinator node id.
    #[serde(default)]
    pub by_id: HashMap<String, u32>,
    /// `producer_id` → coordinator node id.
    #[serde(default)]
    pub by_pid: HashMap<u64, u32>,
    /// Last note/touch wall time (unix ms) per transactional_id (Phase 127).
    #[serde(default)]
    pub id_last_ms: HashMap<String, u64>,
    /// Last note/touch wall time (unix ms) per producer_id (Phase 127).
    #[serde(default)]
    pub pid_last_ms: HashMap<u64, u64>,
}

fn default_version() -> u32 {
    TXN_COORDINATOR_FILE_VERSION
}

impl Default for TxnCoordinatorFile {
    fn default() -> Self {
        Self {
            version: TXN_COORDINATOR_FILE_VERSION,
            by_id: HashMap::new(),
            by_pid: HashMap::new(),
            id_last_ms: HashMap::new(),
            pid_last_ms: HashMap::new(),
        }
    }
}

/// File-backed + in-memory Init-owner registry.
#[derive(Debug)]
pub struct TxnCoordinatorRegistry {
    /// Snapshot path; `None` disables durability (tests / ephemeral).
    path: Option<PathBuf>,
    by_id: RwLock<HashMap<String, u32>>,
    by_pid: RwLock<HashMap<u64, u32>>,
    id_last_ms: RwLock<HashMap<String, u64>>,
    pid_last_ms: RwLock<HashMap<u64, u64>>,
    restored: AtomicU64,
    persist_errors_total: AtomicU64,
    /// Entries removed by TTL GC (Phase 127).
    gc_total: AtomicU64,
}

impl Default for TxnCoordinatorRegistry {
    fn default() -> Self {
        Self::memory()
    }
}

impl TxnCoordinatorRegistry {
    /// In-memory only registry (no disk).
    pub fn memory() -> Self {
        Self {
            path: None,
            by_id: RwLock::new(HashMap::new()),
            by_pid: RwLock::new(HashMap::new()),
            id_last_ms: RwLock::new(HashMap::new()),
            pid_last_ms: RwLock::new(HashMap::new()),
            restored: AtomicU64::new(0),
            persist_errors_total: AtomicU64::new(0),
            gc_total: AtomicU64::new(0),
        }
    }

    /// Open (or create empty) registry under `data_dir/__txn_coordinator`.
    pub fn open(data_dir: impl AsRef<Path>) -> Self {
        let dir = data_dir.as_ref().join(TXN_COORDINATOR_DIR);
        let _ = fs::create_dir_all(&dir);
        let path = dir.join(TXN_COORDINATOR_FILE);
        let loaded = load_maps(&path);
        Self {
            path: Some(path),
            by_id: RwLock::new(loaded.by_id),
            by_pid: RwLock::new(loaded.by_pid),
            id_last_ms: RwLock::new(loaded.id_last_ms),
            pid_last_ms: RwLock::new(loaded.pid_last_ms),
            restored: AtomicU64::new(loaded.restored),
            persist_errors_total: AtomicU64::new(0),
            gc_total: AtomicU64::new(0),
        }
    }

    /// Whether this registry writes a durable snapshot.
    pub fn is_durable(&self) -> bool {
        self.path.is_some()
    }

    /// Mapping rows restored from disk at last open (`by_id` + `by_pid` entries).
    pub fn restored(&self) -> u64 {
        self.restored.load(Ordering::Relaxed)
    }

    /// Durable persist failures.
    pub fn persist_errors_total(&self) -> u64 {
        self.persist_errors_total.load(Ordering::Relaxed)
    }

    /// Cumulative entries removed by TTL GC (id + pid removals counted separately).
    pub fn gc_total(&self) -> u64 {
        self.gc_total.load(Ordering::Relaxed)
    }

    /// Number of known transactional_id mappings.
    pub fn id_count(&self) -> usize {
        self.by_id.read().len()
    }

    /// Number of known producer_id mappings.
    pub fn pid_count(&self) -> usize {
        self.by_pid.read().len()
    }

    /// Register or overwrite Init owner for a transactional id and/or pid.
    ///
    /// `coordinator_node_id == 0` is ignored (unknown / legacy trailer).
    pub fn note(
        &self,
        transactional_id: &str,
        producer_id: u64,
        coordinator_node_id: u32,
    ) {
        if coordinator_node_id == 0 {
            return;
        }
        let now = now_ms();
        if !transactional_id.is_empty() {
            self.by_id
                .write()
                .insert(transactional_id.to_owned(), coordinator_node_id);
            self.id_last_ms
                .write()
                .insert(transactional_id.to_owned(), now);
        }
        self.by_pid
            .write()
            .insert(producer_id, coordinator_node_id);
        self.pid_last_ms.write().insert(producer_id, now);
        self.persist();
    }

    /// Resolve coordinator by transactional_id only.
    pub fn resolve_by_id(&self, transactional_id: &str) -> Option<u32> {
        if transactional_id.is_empty() {
            return None;
        }
        self.by_id
            .read()
            .get(transactional_id)
            .copied()
            .filter(|&id| id != 0)
    }

    /// Resolve coordinator by producer_id only.
    pub fn resolve_by_pid(&self, producer_id: u64) -> Option<u32> {
        self.by_pid
            .read()
            .get(&producer_id)
            .copied()
            .filter(|&id| id != 0)
    }

    /// Drop entries whose last note is older than `ttl_ms` (Phase 127).
    ///
    /// `ttl_ms == 0` disables GC (returns 0). `now_ms` is injectable for tests.
    /// Returns number of map entries removed (id + pid counted separately).
    pub fn expire_stale(&self, ttl_ms: u64, now_ms: u64) -> usize {
        if ttl_ms == 0 {
            return 0;
        }
        let cutoff = now_ms.saturating_sub(ttl_ms);
        let mut removed = 0usize;

        {
            let mut by_id = self.by_id.write();
            let mut last = self.id_last_ms.write();
            let stale: Vec<String> = by_id
                .keys()
                .filter(|k| last.get(*k).copied().unwrap_or(0) <= cutoff)
                .cloned()
                .collect();
            for k in stale {
                by_id.remove(&k);
                last.remove(&k);
                removed += 1;
            }
            // Drop orphan last_ms keys.
            last.retain(|k, _| by_id.contains_key(k));
        }
        {
            let mut by_pid = self.by_pid.write();
            let mut last = self.pid_last_ms.write();
            let stale: Vec<u64> = by_pid
                .keys()
                .filter(|k| last.get(*k).copied().unwrap_or(0) <= cutoff)
                .copied()
                .collect();
            for k in stale {
                by_pid.remove(&k);
                last.remove(&k);
                removed += 1;
            }
            last.retain(|k, _| by_pid.contains_key(k));
        }

        if removed > 0 {
            self.gc_total.fetch_add(removed as u64, Ordering::Relaxed);
            self.persist();
        }
        removed
    }

    /// Snapshot entries for tests / dump (id rows + pid-only leftovers).
    pub fn list(&self) -> Vec<TxnCoordinatorEntry> {
        let by_id = self.by_id.read();
        let by_pid = self.by_pid.read();
        let mut out = Vec::new();
        for (txn_id, &coord) in by_id.iter() {
            let pid = by_pid
                .iter()
                .find(|(_, c)| **c == coord)
                .map(|(&p, _)| p)
                .unwrap_or(0);
            out.push(TxnCoordinatorEntry {
                transactional_id: txn_id.clone(),
                producer_id: pid,
                coordinator_node_id: coord,
            });
        }
        for (&pid, &coord) in by_pid.iter() {
            if out
                .iter()
                .any(|e| e.producer_id == pid && e.coordinator_node_id == coord)
            {
                continue;
            }
            out.push(TxnCoordinatorEntry {
                transactional_id: String::new(),
                producer_id: pid,
                coordinator_node_id: coord,
            });
        }
        out.sort_by(|a, b| {
            a.transactional_id
                .cmp(&b.transactional_id)
                .then(a.producer_id.cmp(&b.producer_id))
        });
        out
    }

    /// Test / ops helper: force last-touch timestamp for an id mapping.
    pub fn test_set_id_last_ms(&self, transactional_id: &str, ms: u64) {
        if self.by_id.read().contains_key(transactional_id) {
            self.id_last_ms
                .write()
                .insert(transactional_id.to_owned(), ms);
        }
    }

    /// Test / ops helper: force last-touch timestamp for a pid mapping.
    pub fn test_set_pid_last_ms(&self, producer_id: u64, ms: u64) {
        if self.by_pid.read().contains_key(&producer_id) {
            self.pid_last_ms.write().insert(producer_id, ms);
        }
    }

    fn persist(&self) {
        let Some(path) = &self.path else {
            return;
        };
        let file = TxnCoordinatorFile {
            version: TXN_COORDINATOR_FILE_VERSION,
            by_id: self.by_id.read().clone(),
            by_pid: self.by_pid.read().clone(),
            id_last_ms: self.id_last_ms.read().clone(),
            pid_last_ms: self.pid_last_ms.read().clone(),
        };
        if save_file(path, &file).is_err() {
            self.persist_errors_total.fetch_add(1, Ordering::Relaxed);
            warn!(
                path = %path.display(),
                "txn coordinator registry persist failed"
            );
        }
    }
}

struct LoadedMaps {
    by_id: HashMap<String, u32>,
    by_pid: HashMap<u64, u32>,
    id_last_ms: HashMap<String, u64>,
    pid_last_ms: HashMap<u64, u64>,
    restored: u64,
}

fn load_maps(path: &Path) -> LoadedMaps {
    let file = match load_file(path) {
        Ok(f) => f,
        Err(_) => {
            return LoadedMaps {
                by_id: HashMap::new(),
                by_pid: HashMap::new(),
                id_last_ms: HashMap::new(),
                pid_last_ms: HashMap::new(),
                restored: 0,
            };
        }
    };
    let now = now_ms();
    let mut by_id: HashMap<String, u32> = HashMap::new();
    let mut by_pid: HashMap<u64, u32> = HashMap::new();
    let mut id_last_ms: HashMap<String, u64> = HashMap::new();
    let mut pid_last_ms: HashMap<u64, u64> = HashMap::new();
    for (k, v) in file.by_id {
        if v != 0 && !k.is_empty() {
            let ts = file.id_last_ms.get(&k).copied().filter(|&t| t > 0).unwrap_or(now);
            by_id.insert(k.clone(), v);
            id_last_ms.insert(k, ts);
        }
    }
    for (k, v) in file.by_pid {
        if v != 0 {
            let ts = file.pid_last_ms.get(&k).copied().filter(|&t| t > 0).unwrap_or(now);
            by_pid.insert(k, v);
            pid_last_ms.insert(k, ts);
        }
    }
    let restored = (by_id.len() + by_pid.len()) as u64;
    LoadedMaps {
        by_id,
        by_pid,
        id_last_ms,
        pid_last_ms,
        restored,
    }
}

fn load_file(path: &Path) -> Result<TxnCoordinatorFile, ()> {
    if !path.exists() {
        return Ok(TxnCoordinatorFile::default());
    }
    let mut f = File::open(path).map_err(|_| ())?;
    let mut buf = String::new();
    f.read_to_string(&mut buf).map_err(|_| ())?;
    if buf.trim().is_empty() {
        return Ok(TxnCoordinatorFile::default());
    }
    serde_json::from_str(&buf).map_err(|_| ())
}

fn save_file(path: &Path, state: &TxnCoordinatorFile) -> Result<(), ()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let _ = fs::create_dir_all(parent);
    let tmp = parent.join(format!("{}.tmp", TXN_COORDINATOR_FILE));
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Effective registry TTL from `VOLANT_TXN_COORDINATOR_TTL_MS` or default 24h.
///
/// `0` disables GC. Invalid env values fall back to the default.
pub fn effective_txn_coordinator_ttl_ms() -> u64 {
    match std::env::var("VOLANT_TXN_COORDINATOR_TTL_MS") {
        Ok(s) => match s.parse::<u64>() {
            Ok(v) => v,
            Err(_) => DEFAULT_TXN_COORDINATOR_TTL_MS,
        },
        Err(_) => DEFAULT_TXN_COORDINATOR_TTL_MS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "volant-txn-coord-{}-{}",
            std::process::id(),
            nanos
        ))
    }

    #[test]
    fn roundtrip_note_and_reload() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        let r1 = TxnCoordinatorRegistry::open(&dir);
        r1.note("orders", 7, 2);
        r1.note("payments", 9, 1);
        assert_eq!(r1.resolve_by_id("orders"), Some(2));
        assert_eq!(r1.resolve_by_pid(7), Some(2));
        assert_eq!(r1.resolve_by_id("payments"), Some(1));
        assert!(
            dir.join(TXN_COORDINATOR_DIR)
                .join(TXN_COORDINATOR_FILE)
                .is_file()
        );

        let r2 = TxnCoordinatorRegistry::open(&dir);
        assert!(r2.restored() >= 2);
        assert_eq!(r2.resolve_by_id("orders"), Some(2));
        assert_eq!(r2.resolve_by_pid(7), Some(2));
        assert_eq!(r2.resolve_by_id("payments"), Some(1));
        assert_eq!(r2.resolve_by_pid(9), Some(1));

        let r3 = TxnCoordinatorRegistry::open(&dir);
        assert_eq!(r3.resolve_by_id("orders"), Some(2));
        assert_eq!(r3.id_count(), r2.id_count());
        assert_eq!(r3.pid_count(), r2.pid_count());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn overwrite_on_re_init() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        let r1 = TxnCoordinatorRegistry::open(&dir);
        r1.note("t", 1, 2);
        r1.note("t", 5, 3);
        assert_eq!(r1.resolve_by_id("t"), Some(3));
        assert_eq!(r1.resolve_by_pid(5), Some(3));
        // Old pid remains until TTL GC (Phase 127).
        assert_eq!(r1.resolve_by_pid(1), Some(2));

        let r2 = TxnCoordinatorRegistry::open(&dir);
        assert_eq!(r2.resolve_by_id("t"), Some(3));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn zero_coord_ignored() {
        let r = TxnCoordinatorRegistry::memory();
        r.note("x", 1, 0);
        assert!(r.resolve_by_id("x").is_none());
        assert!(r.resolve_by_pid(1).is_none());
    }

    #[test]
    fn pid_only_note() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        let r1 = TxnCoordinatorRegistry::open(&dir);
        r1.note("", 42, 7);
        assert!(r1.resolve_by_id("").is_none());
        assert_eq!(r1.resolve_by_pid(42), Some(7));
        let r2 = TxnCoordinatorRegistry::open(&dir);
        assert_eq!(r2.resolve_by_pid(42), Some(7));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn ttl_gc_removes_stale_and_persists() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        let r = TxnCoordinatorRegistry::open(&dir);
        r.note("fresh", 10, 1);
        r.note("stale", 20, 2);
        r.test_set_id_last_ms("stale", 1_000);
        r.test_set_pid_last_ms(20, 1_000);
        // Keep fresh at "now".
        let now = 1_000_000u64;
        r.test_set_id_last_ms("fresh", now);
        r.test_set_pid_last_ms(10, now);

        assert_eq!(r.expire_stale(0, now), 0); // disabled
        assert_eq!(r.resolve_by_id("stale"), Some(2));

        let n = r.expire_stale(60_000, now); // 60s TTL
        assert!(n >= 2, "removed {n}");
        assert!(r.resolve_by_id("stale").is_none());
        assert!(r.resolve_by_pid(20).is_none());
        assert_eq!(r.resolve_by_id("fresh"), Some(1));
        assert_eq!(r.resolve_by_pid(10), Some(1));
        assert!(r.gc_total() >= 2);

        let r2 = TxnCoordinatorRegistry::open(&dir);
        assert!(r2.resolve_by_id("stale").is_none());
        assert_eq!(r2.resolve_by_id("fresh"), Some(1));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn v1_file_loads_with_now_timestamps() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        let reg_dir = dir.join(TXN_COORDINATOR_DIR);
        fs::create_dir_all(&reg_dir).unwrap();
        let path = reg_dir.join(TXN_COORDINATOR_FILE);
        // Phase 124 v1 shape (no last_ms maps).
        let v1 = r#"{
  "version": 1,
  "by_id": { "legacy": 3 },
  "by_pid": { "99": 3 }
}"#;
        fs::write(&path, v1).unwrap();
        let r = TxnCoordinatorRegistry::open(&dir);
        assert_eq!(r.resolve_by_id("legacy"), Some(3));
        assert_eq!(r.resolve_by_pid(99), Some(3));
        // Fresh timestamps → not immediately GC'd with 24h TTL.
        assert_eq!(r.expire_stale(DEFAULT_TXN_COORDINATOR_TTL_MS, now_ms()), 0);
        let _ = fs::remove_dir_all(&dir);
    }
}
