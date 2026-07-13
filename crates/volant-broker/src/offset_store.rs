//! File-backed durable consumer offset store.
//!
//! Layout: `{data_dir}/__consumer_offsets/{group_id}/{topic}/{partition}`
//! File contents: `u64 offset` (LE) + `u16 metadata_len` (LE) + UTF-8 metadata.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use volant_core::{Error, Result};

/// Sentinel for unknown / not-committed offsets (wire + store).
pub const OFFSET_UNKNOWN: u64 = u64::MAX;

/// One committed offset entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredOffset {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
    /// Committed next-read offset.
    pub offset: u64,
    /// Optional metadata.
    pub metadata: String,
}

/// Durable offset store under a data directory.
#[derive(Debug)]
pub struct OffsetStore {
    root: PathBuf,
    /// Serialize commits (fsync path).
    lock: Mutex<()>,
}

impl OffsetStore {
    /// Create an offset store rooted at `data_dir/__consumer_offsets`.
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self> {
        let root = data_dir.as_ref().join("__consumer_offsets");
        fs::create_dir_all(&root).map_err(|e| {
            Error::Storage(format!("create offset store {}: {e}", root.display()))
        })?;
        Ok(Self {
            root,
            lock: Mutex::new(()),
        })
    }

    fn entry_path(&self, group_id: &str, topic: &str, partition: u32) -> PathBuf {
        self.root
            .join(sanitize(group_id))
            .join(sanitize(topic))
            .join(partition.to_string())
    }

    /// Commit a single offset (fsync).
    pub fn commit(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
        metadata: &str,
    ) -> Result<()> {
        let _guard = self.lock.lock();
        let path = self.entry_path(group_id, topic, partition);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                Error::Storage(format!("create offset dir {}: {e}", parent.display()))
            })?;
        }
        // Write to temp then rename for atomicity.
        let tmp = path.with_extension("tmp");
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| Error::Storage(format!("open offset tmp: {e}")))?;
            let meta_bytes = metadata.as_bytes();
            if meta_bytes.len() > u16::MAX as usize {
                return Err(Error::InvalidArgument("offset metadata too long".into()));
            }
            f.write_all(&offset.to_le_bytes())
                .map_err(|e| Error::Storage(format!("write offset: {e}")))?;
            f.write_all(&(meta_bytes.len() as u16).to_le_bytes())
                .map_err(|e| Error::Storage(format!("write meta len: {e}")))?;
            f.write_all(meta_bytes)
                .map_err(|e| Error::Storage(format!("write meta: {e}")))?;
            f.sync_all()
                .map_err(|e| Error::Storage(format!("fsync offset: {e}")))?;
        }
        fs::rename(&tmp, &path)
            .map_err(|e| Error::Storage(format!("rename offset file: {e}")))?;
        Ok(())
    }

    /// Fetch a single offset; returns `OFFSET_UNKNOWN` if not present.
    pub fn fetch(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> Result<(u64, String)> {
        let path = self.entry_path(group_id, topic, partition);
        if !path.exists() {
            return Ok((OFFSET_UNKNOWN, String::new()));
        }
        let mut f = File::open(&path)
            .map_err(|e| Error::Storage(format!("open offset {}: {e}", path.display())))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)
            .map_err(|e| Error::Storage(format!("read offset: {e}")))?;
        if buf.len() < 8 + 2 {
            return Err(Error::Storage(format!(
                "corrupt offset file {}",
                path.display()
            )));
        }
        let offset = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let meta_len = u16::from_le_bytes(buf[8..10].try_into().unwrap()) as usize;
        if buf.len() < 10 + meta_len {
            return Err(Error::Storage(format!(
                "corrupt offset metadata {}",
                path.display()
            )));
        }
        let metadata = String::from_utf8_lossy(&buf[10..10 + meta_len]).into_owned();
        Ok((offset, metadata))
    }

    /// List group ids that have at least one offset directory on disk.
    pub fn list_group_ids(&self) -> Result<Vec<String>> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for ent in fs::read_dir(&self.root)
            .map_err(|e| Error::Storage(format!("list offset groups: {e}")))?
        {
            let ent = ent.map_err(|e| Error::Storage(e.to_string()))?;
            if ent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                out.push(ent.file_name().to_string_lossy().into_owned());
            }
        }
        out.sort();
        Ok(out)
    }

    /// List all committed offsets for a group.
    pub fn fetch_all(&self, group_id: &str) -> Result<Vec<StoredOffset>> {
        let group_dir = self.root.join(sanitize(group_id));
        if !group_dir.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for topic_ent in fs::read_dir(&group_dir)
            .map_err(|e| Error::Storage(format!("read group offsets: {e}")))?
        {
            let topic_ent = topic_ent.map_err(|e| Error::Storage(e.to_string()))?;
            if !topic_ent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let topic = topic_ent.file_name().to_string_lossy().into_owned();
            for part_ent in fs::read_dir(topic_ent.path())
                .map_err(|e| Error::Storage(format!("read topic offsets: {e}")))?
            {
                let part_ent = part_ent.map_err(|e| Error::Storage(e.to_string()))?;
                if !part_ent.file_type().map(|t| t.is_file()).unwrap_or(false) {
                    continue;
                }
                let name = part_ent.file_name().to_string_lossy().into_owned();
                if name.ends_with(".tmp") {
                    continue;
                }
                let Ok(partition) = name.parse::<u32>() else {
                    continue;
                };
                let (offset, metadata) = self.fetch(group_id, &topic, partition)?;
                if offset != OFFSET_UNKNOWN {
                    out.push(StoredOffset {
                        topic: topic.clone(),
                        partition,
                        offset,
                        metadata,
                    });
                }
            }
        }
        out.sort_by(|a, b| {
            a.topic
                .cmp(&b.topic)
                .then(a.partition.cmp(&b.partition))
        });
        Ok(out)
    }
}

fn sanitize(s: &str) -> String {
    // Avoid path traversal; group/topic names should be simple identifiers.
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
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
        let dir = std::env::temp_dir().join(format!(
            "volant-offsets-{}-{}",
            std::process::id(),
            nanos
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn commit_fetch_durable() {
        let dir = temp_dir();
        {
            let store = OffsetStore::new(&dir).unwrap();
            store
                .commit("g1", "events", 0, 10, "meta")
                .unwrap();
            let (off, meta) = store.fetch("g1", "events", 0).unwrap();
            assert_eq!(off, 10);
            assert_eq!(meta, "meta");
        }
        // Recreate store — durable across reopen.
        {
            let store = OffsetStore::new(&dir).unwrap();
            let (off, meta) = store.fetch("g1", "events", 0).unwrap();
            assert_eq!(off, 10);
            assert_eq!(meta, "meta");
            let all = store.fetch_all("g1").unwrap();
            assert_eq!(all.len(), 1);
            assert_eq!(all[0].offset, 10);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn unknown_offset() {
        let dir = temp_dir();
        let store = OffsetStore::new(&dir).unwrap();
        let (off, _) = store.fetch("g", "t", 0).unwrap();
        assert_eq!(off, OFFSET_UNKNOWN);
        let _ = fs::remove_dir_all(&dir);
    }
}
