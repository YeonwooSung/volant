//! Durable key-value store backed by [redb](https://docs.rs/redb) (Phase 149).
//!
//! # Why redb
//!
//! Pure-Rust embedded KV, ACID, crash-safe commits, no C/RocksDB toolchain.
//! Chosen over a custom WAL+snapshot to keep MVP small and correctness high.
//!
//! # Durability
//!
//! - Single table `"kv"`; keys and values are raw bytes.
//! - Each [`KeyValueStore::put`] / [`KeyValueStore::delete`] opens a write
//!   transaction and commits with redb's default [`Durability::Immediate`]
//!   (fsync on commit) — auto-flush every mutation for MVP simplicity.
//! - [`DurableStore::flush`] is an explicit no-op barrier: mutations are already
//!   durable after commit. Callers may still invoke it for API symmetry.
//! - Surviving process restart: reopen the same directory path.
//!
//! # Honesty
//!
//! Durable aggregate state ≠ exactly-once processing. At-least-once still
//! applies (duplicate inputs can double-count after crash/replay).

use std::path::{Path, PathBuf};

use bytes::Bytes;
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use thiserror::Error;

use super::KeyValueStore;

const KV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");
const DB_FILE: &str = "kv.redb";

/// Errors from opening or maintaining a [`DurableStore`].
#[derive(Debug, Error)]
pub enum StreamStateError {
    /// Filesystem error creating the store directory or path.
    #[error("stream state io: {0}")]
    Io(#[from] std::io::Error),
    /// redb database open / create failure.
    #[error("stream state database: {0}")]
    Database(String),
    /// redb transaction failure.
    #[error("stream state transaction: {0}")]
    Transaction(String),
    /// redb table open / mutation failure.
    #[error("stream state table: {0}")]
    Table(String),
    /// redb commit failure.
    #[error("stream state commit: {0}")]
    Commit(String),
}

/// redb-backed durable [`KeyValueStore`].
///
/// Store root is a **directory**. The redb file is `{path}/kv.redb`.
pub struct DurableStore {
    path: PathBuf,
    db: Database,
}

impl DurableStore {
    /// Open or create a durable store under `path` (directory).
    ///
    /// Creates `path` if missing. Initializes the `"kv"` table on first open.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StreamStateError> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;
        let db_path = path.join(DB_FILE);
        let db = Database::create(&db_path).map_err(|e| StreamStateError::Database(e.to_string()))?;
        // Ensure the table exists so subsequent reads do not fail with TableDoesNotExist.
        {
            let txn = db
                .begin_write()
                .map_err(|e| StreamStateError::Transaction(e.to_string()))?;
            {
                let _table = txn
                    .open_table(KV)
                    .map_err(|e| StreamStateError::Table(e.to_string()))?;
            }
            txn.commit()
                .map_err(|e| StreamStateError::Commit(e.to_string()))?;
        }
        Ok(Self { path, db })
    }

    /// Flush durable state to disk.
    ///
    /// MVP: each put/delete already commits with Immediate durability (fsync).
    /// This method remains for API symmetry and future batching.
    pub fn flush(&self) -> Result<(), StreamStateError> {
        Ok(())
    }

    /// Directory path of this store.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn panic_ctx(op: &str, err: impl std::fmt::Display) -> ! {
        panic!("DurableStore {op} failed: {err}");
    }
}

impl KeyValueStore for DurableStore {
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        let txn = self
            .db
            .begin_read()
            .unwrap_or_else(|e| Self::panic_ctx("begin_read", e));
        let table = match txn.open_table(KV) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return None,
            Err(e) => Self::panic_ctx("open_table(read)", e),
        };
        match table.get(key) {
            Ok(Some(v)) => Some(Bytes::copy_from_slice(v.value())),
            Ok(None) => None,
            Err(e) => Self::panic_ctx("get", e),
        }
    }

    fn put(&mut self, key: Bytes, value: Bytes) {
        let txn = self
            .db
            .begin_write()
            .unwrap_or_else(|e| Self::panic_ctx("begin_write", e));
        {
            let mut table = txn
                .open_table(KV)
                .unwrap_or_else(|e| Self::panic_ctx("open_table(write)", e));
            table
                .insert(key.as_ref(), value.as_ref())
                .unwrap_or_else(|e| Self::panic_ctx("insert", e));
        }
        txn.commit()
            .unwrap_or_else(|e| Self::panic_ctx("commit", e));
    }

    fn delete(&mut self, key: &[u8]) {
        let txn = self
            .db
            .begin_write()
            .unwrap_or_else(|e| Self::panic_ctx("begin_write", e));
        {
            let mut table = txn
                .open_table(KV)
                .unwrap_or_else(|e| Self::panic_ctx("open_table(write)", e));
            table
                .remove(key)
                .unwrap_or_else(|e| Self::panic_ctx("remove", e));
        }
        txn.commit()
            .unwrap_or_else(|e| Self::panic_ctx("commit", e));
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + '_> {
        let txn = self
            .db
            .begin_read()
            .unwrap_or_else(|e| Self::panic_ctx("begin_read", e));
        let table = match txn.open_table(KV) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Box::new(std::iter::empty());
            }
            Err(e) => Self::panic_ctx("open_table(read)", e),
        };
        let items: Vec<(Bytes, Bytes)> = table
            .iter()
            .unwrap_or_else(|e| Self::panic_ctx("iter", e))
            .map(|item| {
                let (k, v) = item.unwrap_or_else(|e| Self::panic_ctx("iter item", e));
                (
                    Bytes::copy_from_slice(k.value()),
                    Bytes::copy_from_slice(v.value()),
                )
            })
            .collect();
        Box::new(items.into_iter())
    }

    fn len(&self) -> usize {
        let txn = self
            .db
            .begin_read()
            .unwrap_or_else(|e| Self::panic_ctx("begin_read", e));
        let table = match txn.open_table(KV) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return 0,
            Err(e) => Self::panic_ctx("open_table(read)", e),
        };
        table
            .len()
            .unwrap_or_else(|e| Self::panic_ctx("len", e)) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_store_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("volant-stream-state-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn durable_put_get_delete_len_iter() {
        let dir = temp_store_dir("crud");
        let mut store = DurableStore::open(&dir).expect("open");
        assert!(store.is_empty());
        store.put(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        store.put(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));
        assert_eq!(store.len(), 2);
        assert_eq!(store.get(b"k1").as_deref(), Some(b"v1".as_ref()));
        let pairs: Vec<_> = store.iter().collect();
        assert_eq!(pairs.len(), 2);
        store.delete(b"k1");
        assert_eq!(store.len(), 1);
        store.flush().expect("flush");
        assert_eq!(store.path(), dir.as_path());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_survives_restart() {
        let dir = temp_store_dir("restart");
        {
            let mut store = DurableStore::open(&dir).expect("open");
            store.put(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
            store.put(Bytes::from_static(b"b"), Bytes::from_static(b"2"));
            store.flush().expect("flush");
        }
        {
            let store = DurableStore::open(&dir).expect("reopen");
            assert_eq!(store.get(b"a").as_deref(), Some(b"1".as_ref()));
            assert_eq!(store.get(b"b").as_deref(), Some(b"2".as_ref()));
            assert_eq!(store.len(), 2);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
