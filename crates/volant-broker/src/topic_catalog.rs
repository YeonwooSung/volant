//! Durable single-node topic catalog (Phase 14).
//!
//! Layout: `{data_dir}/__topics/catalog.json` (atomic replace on write).
//! Multi-node brokers use `cluster/assignment.json` instead.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use volant_core::{Error, Result};

/// One topic entry in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogTopic {
    /// Stable topic id.
    pub id: u32,
    /// Partition count.
    pub partitions: u32,
}

/// Full durable catalog snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicCatalogFile {
    /// Next topic id to allocate.
    pub next_id: u32,
    /// Topic name → metadata.
    pub topics: HashMap<String, CatalogTopic>,
}

impl Default for TopicCatalogFile {
    fn default() -> Self {
        Self {
            next_id: 1,
            topics: HashMap::new(),
        }
    }
}

/// File-backed topic catalog store.
#[derive(Debug)]
pub struct TopicCatalogStore {
    path: PathBuf,
}

impl TopicCatalogStore {
    /// Open (or create empty) store under `data_dir/__topics`.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = data_dir.as_ref().join("__topics");
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!("create topic catalog dir {}: {e}", dir.display()))
        })?;
        Ok(Self {
            path: dir.join("catalog.json"),
        })
    }

    /// Load snapshot; empty defaults if file missing.
    pub fn load(&self) -> Result<TopicCatalogFile> {
        if !self.path.exists() {
            return Ok(TopicCatalogFile::default());
        }
        let mut f = File::open(&self.path).map_err(|e| {
            Error::Storage(format!("open topic catalog {}: {e}", self.path.display()))
        })?;
        let mut buf = String::new();
        f.read_to_string(&mut buf).map_err(|e| {
            Error::Storage(format!("read topic catalog: {e}"))
        })?;
        if buf.trim().is_empty() {
            return Ok(TopicCatalogFile::default());
        }
        serde_json::from_str(&buf).map_err(|e| {
            Error::Storage(format!("parse topic catalog: {e}"))
        })
    }

    /// Atomically persist snapshot (write temp + rename).
    pub fn save(&self, state: &TopicCatalogFile) -> Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let tmp = parent.join("catalog.json.tmp");
        let json = serde_json::to_string_pretty(state)
            .map_err(|e| Error::Storage(format!("encode topic catalog: {e}")))?;
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| Error::Storage(format!("open topic catalog tmp: {e}")))?;
            f.write_all(json.as_bytes())
                .map_err(|e| Error::Storage(format!("write topic catalog: {e}")))?;
            f.sync_all()
                .map_err(|e| Error::Storage(format!("fsync topic catalog: {e}")))?;
        }
        fs::rename(&tmp, &self.path).map_err(|e| {
            Error::Storage(format!(
                "rename topic catalog {} -> {}: {e}",
                tmp.display(),
                self.path.display()
            ))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn catalog_roundtrip() {
        let dir = env::temp_dir().join(format!("volant-catalog-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = TopicCatalogStore::open(&dir).unwrap();
        let mut file = TopicCatalogFile::default();
        file.next_id = 5;
        file.topics.insert(
            "orders".into(),
            CatalogTopic {
                id: 1,
                partitions: 3,
            },
        );
        store.save(&file).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded, file);
        let _ = fs::remove_dir_all(&dir);
    }
}
