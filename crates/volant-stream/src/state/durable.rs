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
//! - **Outside a checkpoint:** each [`KeyValueStore::put`] /
//!   [`KeyValueStore::delete`] opens a write transaction and commits with
//!   redb's default [`Durability::Immediate`] (fsync on commit) — ALO /
//!   non-EOS path.
//! - **Inside a checkpoint (Phase 153):** puts/deletes stage in an in-memory
//!   overlay; [`commit_checkpoint`] applies them in one Immediate write txn;
//!   [`abort_checkpoint`] discards the overlay. Process-local staging only —
//!   not distributed 2PC with the broker.
//! - [`DurableStore::flush`] is an explicit no-op barrier for API symmetry
//!   (does not commit a checkpoint).
//! - Surviving process restart: reopen the same directory path.
//!
//! # Honesty
//!
//! Durable aggregate state ≠ exactly-once processing by itself. Pair with
//! EOS (Phase 151) + checkpoint ordering (Phase 153) so durable state does not
//! advance before a successful EndTxn. At-least-once still uses immediate puts.

use std::collections::{BTreeMap, BTreeSet};
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

/// In-memory overlay while a checkpoint is open.
#[derive(Default)]
struct Staging {
    puts: BTreeMap<Vec<u8>, Bytes>,
    deletes: BTreeSet<Vec<u8>>,
}

/// redb-backed durable [`KeyValueStore`].
///
/// Store root is a **directory**. The redb file is `{path}/kv.redb`.
pub struct DurableStore {
    path: PathBuf,
    db: Database,
    /// When `Some`, puts/deletes touch the overlay only (Phase 153).
    staging: Option<Staging>,
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
        Ok(Self {
            path,
            db,
            staging: None,
        })
    }

    /// Flush durable state to disk.
    ///
    /// Outside a checkpoint, each put/delete already commits with Immediate
    /// durability. Does **not** commit an open checkpoint — use
    /// [`KeyValueStore::commit_checkpoint`].
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

    fn disk_get(&self, key: &[u8]) -> Option<Bytes> {
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

    fn disk_len(&self) -> usize {
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

    fn disk_iter_map(&self) -> BTreeMap<Vec<u8>, Bytes> {
        let txn = self
            .db
            .begin_read()
            .unwrap_or_else(|e| Self::panic_ctx("begin_read", e));
        let table = match txn.open_table(KV) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return BTreeMap::new(),
            Err(e) => Self::panic_ctx("open_table(read)", e),
        };
        table
            .iter()
            .unwrap_or_else(|e| Self::panic_ctx("iter", e))
            .map(|item| {
                let (k, v) = item.unwrap_or_else(|e| Self::panic_ctx("iter item", e));
                (
                    k.value().to_vec(),
                    Bytes::copy_from_slice(v.value()),
                )
            })
            .collect()
    }

    fn put_immediate(&mut self, key: Bytes, value: Bytes) {
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

    fn delete_immediate(&mut self, key: &[u8]) {
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

    fn apply_staging(&mut self, staging: Staging) -> Result<(), StreamStateError> {
        if staging.puts.is_empty() && staging.deletes.is_empty() {
            return Ok(());
        }
        let txn = self
            .db
            .begin_write()
            .map_err(|e| StreamStateError::Transaction(e.to_string()))?;
        {
            let mut table = txn
                .open_table(KV)
                .map_err(|e| StreamStateError::Table(e.to_string()))?;
            for key in &staging.deletes {
                table
                    .remove(key.as_slice())
                    .map_err(|e| StreamStateError::Table(e.to_string()))?;
            }
            for (key, value) in &staging.puts {
                table
                    .insert(key.as_slice(), value.as_ref())
                    .map_err(|e| StreamStateError::Table(e.to_string()))?;
            }
        }
        txn.commit()
            .map_err(|e| StreamStateError::Commit(e.to_string()))?;
        Ok(())
    }
}

impl KeyValueStore for DurableStore {
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        if let Some(st) = &self.staging {
            if st.deletes.contains(key) {
                return None;
            }
            if let Some(v) = st.puts.get(key) {
                return Some(v.clone());
            }
        }
        self.disk_get(key)
    }

    fn put(&mut self, key: Bytes, value: Bytes) {
        if let Some(st) = &mut self.staging {
            let k = key.to_vec();
            st.deletes.remove(&k);
            st.puts.insert(k, value);
            return;
        }
        self.put_immediate(key, value);
    }

    fn delete(&mut self, key: &[u8]) {
        if let Some(st) = &mut self.staging {
            st.puts.remove(key);
            st.deletes.insert(key.to_vec());
            return;
        }
        self.delete_immediate(key);
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + '_> {
        let mut map = self.disk_iter_map();
        if let Some(st) = &self.staging {
            for d in &st.deletes {
                map.remove(d);
            }
            for (k, v) in &st.puts {
                map.insert(k.clone(), v.clone());
            }
        }
        Box::new(
            map.into_iter()
                .map(|(k, v)| (Bytes::from(k), v)),
        )
    }

    fn len(&self) -> usize {
        let Some(st) = &self.staging else {
            return self.disk_len();
        };
        // disk − deletes-present-on-disk + puts-absent-on-disk
        // (put removes key from deletes; overwrite of existing key is zero delta)
        let mut n = self.disk_len();
        for d in &st.deletes {
            if self.disk_get(d).is_some() {
                n = n.saturating_sub(1);
            }
        }
        for k in st.puts.keys() {
            if self.disk_get(k).is_none() {
                n = n.saturating_add(1);
            }
        }
        n
    }

    fn begin_checkpoint(&mut self) {
        if self.staging.is_none() {
            self.staging = Some(Staging::default());
        }
    }

    fn commit_checkpoint(&mut self) -> Result<(), StreamStateError> {
        let Some(staging) = self.staging.take() else {
            return Ok(());
        };
        if let Err(e) = self.apply_staging(staging) {
            // Leave not-in-checkpoint on failure; staged data is lost — caller
            // already committed the broker txn in the EOS path (honesty residual).
            return Err(e);
        }
        Ok(())
    }

    fn abort_checkpoint(&mut self) {
        self.staging = None;
    }

    fn in_checkpoint(&self) -> bool {
        self.staging.is_some()
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

    #[test]
    fn checkpoint_abort_leaves_disk_unchanged() {
        let dir = temp_store_dir("ckpt-abort");
        let mut store = DurableStore::open(&dir).expect("open");
        store.put(Bytes::from_static(b"seed"), Bytes::from_static(b"1"));
        store.begin_checkpoint();
        store.put(Bytes::from_static(b"new"), Bytes::from_static(b"2"));
        assert_eq!(store.get(b"new").as_deref(), Some(b"2".as_ref()));
        store.abort_checkpoint();
        assert_eq!(store.get(b"new"), None);
        assert_eq!(store.get(b"seed").as_deref(), Some(b"1".as_ref()));
        drop(store);
        let store = DurableStore::open(&dir).expect("reopen");
        assert_eq!(store.get(b"new"), None);
        assert_eq!(store.get(b"seed").as_deref(), Some(b"1".as_ref()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpoint_commit_persists() {
        let dir = temp_store_dir("ckpt-commit");
        {
            let mut store = DurableStore::open(&dir).expect("open");
            store.begin_checkpoint();
            store.put(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
            store.commit_checkpoint().expect("commit");
        }
        let store = DurableStore::open(&dir).expect("reopen");
        assert_eq!(store.get(b"k").as_deref(), Some(b"v".as_ref()));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
