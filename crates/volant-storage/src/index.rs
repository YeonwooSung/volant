//! Sparse index for segment files (offset → file position).

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use volant_core::{Error, Result};

/// Size of a single sparse-index entry in bytes.
pub const INDEX_ENTRY_SIZE: usize = 16;

/// One sparse-index entry: offset relative to segment base → byte position in `.log`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    /// `record.offset - base_offset` (fits in u32 for Phase 1 segments).
    pub offset_delta: u32,
    /// Byte offset in the `.log` file where `record_length` begins.
    pub position: u32,
}

impl IndexEntry {
    /// Encode this entry as 16 little-endian bytes.
    pub fn encode(self) -> [u8; INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; INDEX_ENTRY_SIZE];
        buf[0..4].copy_from_slice(&self.offset_delta.to_le_bytes());
        buf[4..8].copy_from_slice(&self.position.to_le_bytes());
        // bytes 8..16 reserved / padding
        buf
    }

    /// Decode one entry from a 16-byte slice.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < INDEX_ENTRY_SIZE {
            return Err(Error::Storage("index entry too short".into()));
        }
        let offset_delta = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let position = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        Ok(Self {
            offset_delta,
            position,
        })
    }
}

/// In-memory sparse index with optional on-disk companion file.
#[derive(Debug, Default)]
pub struct SparseIndex {
    entries: Vec<IndexEntry>,
}

impl SparseIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// All entries in order.
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Append an entry (must be monotonic in offset_delta).
    pub fn push(&mut self, entry: IndexEntry) {
        self.entries.push(entry);
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Find the greatest entry with `offset_delta <= target_delta`.
    ///
    /// Returns the file position to begin scanning, or `None` if the index is empty
    /// (caller should start at the segment header end).
    pub fn lookup(&self, target_delta: u32) -> Option<u32> {
        if self.entries.is_empty() {
            return None;
        }
        // Binary search for rightmost entry with offset_delta <= target_delta.
        let mut lo = 0usize;
        let mut hi = self.entries.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.entries[mid].offset_delta <= target_delta {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            // All entries are > target; still start from first entry if target is
            // before the first indexed record — caller may need header start.
            // Return None so caller uses HEADER_SIZE when target is before first entry.
            if self.entries[0].offset_delta > target_delta {
                return None;
            }
        }
        Some(self.entries[lo - 1].position)
    }

    /// Load an index file from disk. Truncates partial trailing entries.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let mut file = File::open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let complete = bytes.len() - (bytes.len() % INDEX_ENTRY_SIZE);
        let mut entries = Vec::with_capacity(complete / INDEX_ENTRY_SIZE);
        for chunk in bytes[..complete].chunks_exact(INDEX_ENTRY_SIZE) {
            entries.push(IndexEntry::decode(chunk)?);
        }
        Ok(Self { entries })
    }

    /// Rewrite the index file fully from in-memory entries.
    pub fn write_to(&self, path: &Path) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        for entry in &self.entries {
            file.write_all(&entry.encode())?;
        }
        file.sync_all()?;
        Ok(())
    }

    /// Append a single entry to the index file (caller keeps writers in Segment).
    pub fn encode_entry(entry: IndexEntry) -> [u8; INDEX_ENTRY_SIZE] {
        entry.encode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn entry_roundtrip() {
        let e = IndexEntry {
            offset_delta: 10,
            position: 32,
        };
        let raw = e.encode();
        let d = IndexEntry::decode(&raw).unwrap();
        assert_eq!(e, d);
    }

    #[test]
    fn lookup_sparse() {
        let mut idx = SparseIndex::new();
        idx.push(IndexEntry {
            offset_delta: 0,
            position: 32,
        });
        idx.push(IndexEntry {
            offset_delta: 5,
            position: 200,
        });
        idx.push(IndexEntry {
            offset_delta: 10,
            position: 400,
        });

        assert_eq!(idx.lookup(0), Some(32));
        assert_eq!(idx.lookup(3), Some(32));
        assert_eq!(idx.lookup(5), Some(200));
        assert_eq!(idx.lookup(7), Some(200));
        assert_eq!(idx.lookup(10), Some(400));
        assert_eq!(idx.lookup(100), Some(400));
    }

    #[test]
    fn load_write_roundtrip() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = temp_dir().join(format!("volant-idx-{nanos}"));
        let mut idx = SparseIndex::new();
        idx.push(IndexEntry {
            offset_delta: 1,
            position: 64,
        });
        idx.push(IndexEntry {
            offset_delta: 9,
            position: 128,
        });
        idx.write_to(&path).unwrap();
        let loaded = SparseIndex::load(&path).unwrap();
        assert_eq!(loaded.entries(), idx.entries());
        let _ = std::fs::remove_file(&path);
    }
}
