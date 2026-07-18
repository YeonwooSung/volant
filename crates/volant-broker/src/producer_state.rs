//! Durable idempotent producer state (Phase 11).
//!
//! Layout: `{data_dir}/__producer_state/state.json` (atomic replace on write).

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use volant_core::{Error, Result};

/// One partition's last accepted batch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredBatch {
    /// Last accepted base sequence.
    pub base_sequence: i32,
    /// Message count of that batch.
    pub count: u32,
    /// Log base offset assigned to that batch.
    pub base_offset: u64,
}

/// One producer id's durable state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredProducer {
    /// Producer epoch.
    pub epoch: u16,
    /// Transactional id when non-empty (Phase 18); empty = plain idempotent.
    #[serde(default)]
    pub transactional_id: String,
    /// Two-phase commit enabled for this producer (Phase 90; InitProducerId v6).
    #[serde(default)]
    pub enable_2pc: bool,
    /// Client open-txn timeout from InitProducerId (Phase 93). `0` = use broker default.
    #[serde(default)]
    pub transaction_timeout_ms: u64,
    /// Keyed as `"{topic}:{partition}"`.
    pub partitions: HashMap<String, StoredBatch>,
}

/// Full durable snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProducerStateFile {
    /// Next producer id to allocate.
    pub next_id: u64,
    /// Map producer_id (string) → state.
    pub producers: HashMap<String, StoredProducer>,
}

impl Default for ProducerStateFile {
    fn default() -> Self {
        Self {
            next_id: 1,
            producers: HashMap::new(),
        }
    }
}

/// File-backed producer state store.
#[derive(Debug)]
pub struct ProducerStateStore {
    path: PathBuf,
}

impl ProducerStateStore {
    /// Open (or create empty) store under `data_dir/__producer_state`.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = data_dir.as_ref().join("__producer_state");
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!("create producer state dir {}: {e}", dir.display()))
        })?;
        Ok(Self {
            path: dir.join("state.json"),
        })
    }

    /// Load snapshot; empty defaults if file missing.
    pub fn load(&self) -> Result<ProducerStateFile> {
        if !self.path.exists() {
            return Ok(ProducerStateFile {
                next_id: 1,
                producers: HashMap::new(),
            });
        }
        let mut f = File::open(&self.path).map_err(|e| {
            Error::Storage(format!("open producer state {}: {e}", self.path.display()))
        })?;
        let mut buf = String::new();
        f.read_to_string(&mut buf).map_err(|e| {
            Error::Storage(format!("read producer state: {e}"))
        })?;
        if buf.trim().is_empty() {
            return Ok(ProducerStateFile {
                next_id: 1,
                producers: HashMap::new(),
            });
        }
        serde_json::from_str(&buf).map_err(|e| {
            Error::Storage(format!("parse producer state: {e}"))
        })
    }

    /// Atomically persist snapshot (write temp + rename).
    pub fn save(&self, state: &ProducerStateFile) -> Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let tmp = parent.join("state.json.tmp");
        let json = serde_json::to_string_pretty(state)
            .map_err(|e| Error::Storage(format!("encode producer state: {e}")))?;
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| Error::Storage(format!("open producer state tmp: {e}")))?;
            f.write_all(json.as_bytes())
                .map_err(|e| Error::Storage(format!("write producer state: {e}")))?;
            f.sync_all()
                .map_err(|e| Error::Storage(format!("fsync producer state: {e}")))?;
        }
        fs::rename(&tmp, &self.path).map_err(|e| {
            Error::Storage(format!("rename producer state: {e}"))
        })?;
        Ok(())
    }
}

/// Encode topic+partition key for the JSON map.
pub fn partition_key(topic: &str, partition: u32) -> String {
    format!("{topic}:{partition}")
}

/// Parse `topic:partition` key.
pub fn parse_partition_key(key: &str) -> Option<(String, u32)> {
    let (t, p) = key.rsplit_once(':')?;
    let partition = p.parse().ok()?;
    Some((t.to_owned(), partition))
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
            "volant-pstate-{}-{}",
            std::process::id(),
            nanos
        ))
    }

    #[test]
    fn roundtrip_persist() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        let store = ProducerStateStore::open(&dir).unwrap();
        let mut state = ProducerStateFile {
            next_id: 5,
            producers: HashMap::new(),
        };
        let mut parts = HashMap::new();
        parts.insert(
            partition_key("events", 0),
            StoredBatch {
                base_sequence: 3,
                count: 2,
                base_offset: 10,
            },
        );
        state.producers.insert(
            "1".into(),
            StoredProducer {
                epoch: 0,
                transactional_id: String::new(),
                enable_2pc: false,
                transaction_timeout_ms: 0,
                partitions: parts,
            },
        );
        store.save(&state).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded, state);
        let _ = fs::remove_dir_all(&dir);
    }
}
