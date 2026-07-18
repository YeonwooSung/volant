//! Durable per-partition leader-epoch history (Phase 87).
//!
//! Layout: `{data_dir}/__leader_epochs/state.json` (atomic replace on write).
//! Entries are Kafka-style `(epoch, start_offset)` lists; end offset of epoch E
//! is the start of the next higher epoch (or HWM for the current epoch).

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use volant_core::{Error, Result};

/// One epoch cache entry: leader epoch and the first offset written under it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpochStart {
    /// Leader epoch.
    pub epoch: u32,
    /// First offset of this epoch (end of previous = this value).
    pub start_offset: u64,
}

/// Full durable snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct LeaderEpochsFile {
    /// Keyed as `"{topic}:{partition}"` → sorted `(epoch, start_offset)` entries.
    #[serde(default)]
    pub partitions: HashMap<String, Vec<EpochStart>>,
}

/// File-backed leader-epoch history store.
#[derive(Debug)]
pub struct LeaderEpochStore {
    path: PathBuf,
}

impl LeaderEpochStore {
    /// Open (or create empty) store under `data_dir/__leader_epochs`.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = data_dir.as_ref().join("__leader_epochs");
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!(
                "create leader epoch dir {}: {e}",
                dir.display()
            ))
        })?;
        Ok(Self {
            path: dir.join("state.json"),
        })
    }

    /// Load snapshot; empty defaults if file missing.
    pub fn load(&self) -> Result<LeaderEpochsFile> {
        if !self.path.exists() {
            return Ok(LeaderEpochsFile::default());
        }
        let mut f = File::open(&self.path).map_err(|e| {
            Error::Storage(format!(
                "open leader epochs {}: {e}",
                self.path.display()
            ))
        })?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)
            .map_err(|e| Error::Storage(format!("read leader epochs: {e}")))?;
        if buf.trim().is_empty() {
            return Ok(LeaderEpochsFile::default());
        }
        serde_json::from_str(&buf)
            .map_err(|e| Error::Storage(format!("parse leader epochs: {e}")))
    }

    /// Atomically persist snapshot (write temp + rename).
    pub fn save(&self, state: &LeaderEpochsFile) -> Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let tmp = parent.join("state.json.tmp");
        let json = serde_json::to_string_pretty(state)
            .map_err(|e| Error::Storage(format!("encode leader epochs: {e}")))?;
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| Error::Storage(format!("open leader epochs tmp: {e}")))?;
            f.write_all(json.as_bytes())
                .map_err(|e| Error::Storage(format!("write leader epochs: {e}")))?;
            f.sync_all()
                .map_err(|e| Error::Storage(format!("fsync leader epochs: {e}")))?;
        }
        fs::rename(&tmp, &self.path).map_err(|e| {
            Error::Storage(format!("rename leader epochs: {e}"))
        })?;
        Ok(())
    }
}

/// Encode topic+partition key.
pub fn partition_key(topic: &str, partition: u32) -> String {
    format!("{topic}:{partition}")
}

/// Ensure `entries` is sorted and contains `(epoch, start)` (idempotent).
///
/// If the epoch already exists, leave its start offset unchanged (first write wins).
pub fn ensure_entry(entries: &mut Vec<EpochStart>, epoch: u32, start_offset: u64) {
    if let Some(e) = entries.iter().find(|e| e.epoch == epoch) {
        let _ = e;
        return;
    }
    entries.push(EpochStart {
        epoch,
        start_offset,
    });
    entries.sort_by_key(|e| e.epoch);
}

/// Look up end offset for a requested leader epoch.
///
/// Returns `(found_epoch, end_offset)` where `end_offset` is the start of the
/// next higher epoch, or `current_hwm` when the found epoch is the latest
/// (current) entry / matches `current_epoch`.
///
/// `requested == -1` means latest → `(current_epoch, current_hwm)`.
pub fn end_offset_for(
    entries: &[EpochStart],
    current_epoch: u32,
    current_hwm: u64,
    requested: i32,
) -> Option<(i32, i64)> {
    if requested == -1 {
        return Some((current_epoch as i32, current_hwm as i64));
    }
    if requested < 0 {
        return None;
    }
    let req = requested as u32;
    if req > current_epoch {
        return None; // caller maps to UNKNOWN_LEADER_EPOCH
    }
    if req == current_epoch {
        return Some((current_epoch as i32, current_hwm as i64));
    }

    // Largest entry with epoch <= requested.
    let idx = entries
        .iter()
        .rposition(|e| e.epoch <= req)
        .or_else(|| {
            // No entry ≤ req: if history empty, treat as epoch 0 @ 0.
            if entries.is_empty() && req <= current_epoch {
                return Some(usize::MAX); // sentinel
            }
            None
        });

    match idx {
        None => {
            // Gap before first known epoch: use first entry's start as end? Kafka
            // returns UNDEFINED for epochs older than the oldest; we honestly
            // fall back to the first known epoch's end when possible.
            if let Some(first) = entries.first() {
                let end = entries
                    .get(1)
                    .map(|n| n.start_offset)
                    .unwrap_or(current_hwm);
                Some((first.epoch as i32, end as i64))
            } else {
                // Empty history, requested < current: end unknown → use 0 start.
                Some((req as i32, 0))
            }
        }
        Some(usize::MAX) => Some((0, current_hwm as i64)),
        Some(i) => {
            let found = &entries[i];
            let end = if found.epoch == current_epoch {
                current_hwm
            } else if let Some(next) = entries.get(i + 1) {
                next.start_offset
            } else {
                // Last stored entry but current_epoch is higher without entry —
                // treat last as closed at HWM only if it is current; else HWM is
                // conservative upper bound. Prefer HWM when no next entry.
                current_hwm
            };
            Some((found.epoch as i32, end as i64))
        }
    }
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
            "volant-lepoch-{}-{}",
            std::process::id(),
            nanos
        ))
    }

    #[test]
    fn roundtrip_persist() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        let store = LeaderEpochStore::open(&dir).unwrap();
        let mut state = LeaderEpochsFile::default();
        state.partitions.insert(
            "orders:0".into(),
            vec![
                EpochStart {
                    epoch: 0,
                    start_offset: 0,
                },
                EpochStart {
                    epoch: 2,
                    start_offset: 5,
                },
            ],
        );
        store.save(&state).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded, state);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn end_offset_prior_vs_current() {
        let entries = vec![
            EpochStart {
                epoch: 0,
                start_offset: 0,
            },
            EpochStart {
                epoch: 1,
                start_offset: 3,
            },
        ];
        // Prior epoch 0 ends at start of epoch 1.
        assert_eq!(end_offset_for(&entries, 1, 10, 0), Some((0, 3)));
        // Current epoch → HWM.
        assert_eq!(end_offset_for(&entries, 1, 10, 1), Some((1, 10)));
        // Latest sentinel.
        assert_eq!(end_offset_for(&entries, 1, 10, -1), Some((1, 10)));
        // Gap: request 0 when only 0,1 present — exact.
        assert_eq!(end_offset_for(&entries, 1, 10, 0), Some((0, 3)));
        // Future epoch.
        assert_eq!(end_offset_for(&entries, 1, 10, 99), None);
    }
}
