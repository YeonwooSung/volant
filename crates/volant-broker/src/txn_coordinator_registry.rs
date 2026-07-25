//! Durable txn coordinator (Init-owner) registry (Phase 124).
//!
//! Layout: `{data_dir}/__txn_coordinator/state.json` (atomic replace on write).
//!
//! Phases 120–122 keep `transactional_id` / `producer_id` → coordinator
//! `node_id` maps for transparent KafkaTxnForward and sticky FindCoordinator
//! override. This module persists those maps so a broker restart on the same
//! `data_dir` restores known ownership without waiting for re-Init or open
//! fan-out.
//!
//! At-least-once / stale entries for completed txns may linger until re-Init
//! overwrites them (no GC in MVP).

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// On-disk directory name under `data_dir`.
pub const TXN_COORDINATOR_DIR: &str = "__txn_coordinator";
/// On-disk snapshot file name.
pub const TXN_COORDINATOR_FILE: &str = "state.json";
/// File format version.
pub const TXN_COORDINATOR_FILE_VERSION: u32 = 1;

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

/// Full durable snapshot — mirrors the two in-memory maps.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxnCoordinatorFile {
    /// Format version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// `transactional_id` → coordinator node id.
    #[serde(default)]
    pub by_id: HashMap<String, u32>,
    /// `producer_id` → coordinator node id (string keys in JSON via serde_json map of string→u32).
    ///
    /// Serde encodes `HashMap<u64, u32>` as a JSON object with string keys.
    #[serde(default)]
    pub by_pid: HashMap<u64, u32>,
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
    restored: AtomicU64,
    persist_errors_total: AtomicU64,
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
            restored: AtomicU64::new(0),
            persist_errors_total: AtomicU64::new(0),
        }
    }

    /// Open (or create empty) registry under `data_dir/__txn_coordinator`.
    pub fn open(data_dir: impl AsRef<Path>) -> Self {
        let dir = data_dir.as_ref().join(TXN_COORDINATOR_DIR);
        let _ = fs::create_dir_all(&dir);
        let path = dir.join(TXN_COORDINATOR_FILE);
        let (by_id, by_pid, n) = load_maps(&path);
        Self {
            path: Some(path),
            by_id: RwLock::new(by_id),
            by_pid: RwLock::new(by_pid),
            restored: AtomicU64::new(n),
            persist_errors_total: AtomicU64::new(0),
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
        if !transactional_id.is_empty() {
            self.by_id
                .write()
                .insert(transactional_id.to_owned(), coordinator_node_id);
        }
        self.by_pid
            .write()
            .insert(producer_id, coordinator_node_id);
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

    /// Snapshot entries for tests / dump (id rows + pid-only leftovers).
    pub fn list(&self) -> Vec<TxnCoordinatorEntry> {
        let by_id = self.by_id.read();
        let by_pid = self.by_pid.read();
        let mut out = Vec::new();
        for (txn_id, &coord) in by_id.iter() {
            // Attach a pid that maps to the same coord if any (best-effort display).
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
            // Skip pids already represented via an id row with same coord+pid.
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

    fn persist(&self) {
        let Some(path) = &self.path else {
            return;
        };
        let file = TxnCoordinatorFile {
            version: TXN_COORDINATOR_FILE_VERSION,
            by_id: self.by_id.read().clone(),
            by_pid: self.by_pid.read().clone(),
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

fn load_maps(path: &Path) -> (HashMap<String, u32>, HashMap<u64, u32>, u64) {
    let file = match load_file(path) {
        Ok(f) => f,
        Err(_) => return (HashMap::new(), HashMap::new(), 0),
    };
    let mut by_id: HashMap<String, u32> = HashMap::new();
    let mut by_pid: HashMap<u64, u32> = HashMap::new();
    for (k, v) in file.by_id {
        if v != 0 && !k.is_empty() {
            by_id.insert(k, v);
        }
    }
    for (k, v) in file.by_pid {
        if v != 0 {
            by_pid.insert(k, v);
        }
    }
    let n = (by_id.len() + by_pid.len()) as u64;
    (by_id, by_pid, n)
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

        // Idempotent second open.
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
        r1.note("t", 5, 3); // new pid, new owner
        assert_eq!(r1.resolve_by_id("t"), Some(3));
        assert_eq!(r1.resolve_by_pid(5), Some(3));
        // Old pid still maps (honest: no GC).
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
}
