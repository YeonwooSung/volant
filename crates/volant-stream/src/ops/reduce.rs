//! Keyed reduce operator.

use std::path::Path;

use bytes::Bytes;
use volant_core::{Error, Offset, Record, Result};

use crate::operator::Operator;
use crate::state::{DurableStore, KeyValueStore, MemoryStore, StreamStateError};

/// Keyed reduce: aggregates values per record key (empty key → `""`).
///
/// `init` produces the starting aggregate for a new key.
/// `add` folds an input record's value into the stored aggregate.
/// Aggregate is stored and emitted as UTF-8 / raw bytes in `record.value`.
pub struct Reduce<S, Init, Add> {
    store: S,
    init: Init,
    add: Add,
}

impl<S, Init, Add> Reduce<S, Init, Add> {
    /// Create a reduce operator with a custom store.
    pub fn with_store(store: S, init: Init, add: Add) -> Self {
        Self { store, init, add }
    }

    /// Borrow the underlying store (tests / inspection).
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Mutable borrow of the underlying store.
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }
}

/// Build a reduce operator backed by [`MemoryStore`].
pub fn reduce<Init, Add>(init: Init, add: Add) -> Reduce<MemoryStore, Init, Add>
where
    Init: FnMut() -> Bytes + Send + 'static,
    Add: FnMut(&Bytes, &Record) -> Result<Bytes> + Send + 'static,
{
    Reduce::with_store(MemoryStore::new(), init, add)
}

/// Build a reduce operator with an injected store (memory or durable).
pub fn reduce_with_store<S, Init, Add>(store: S, init: Init, add: Add) -> Reduce<S, Init, Add>
where
    S: KeyValueStore,
    Init: FnMut() -> Bytes + Send + 'static,
    Add: FnMut(&Bytes, &Record) -> Result<Bytes> + Send + 'static,
{
    Reduce::with_store(store, init, add)
}

/// Init fn pointer for word-count reduce (`"0"`).
pub type CountInit = fn() -> Bytes;
/// Add fn pointer for word-count reduce (decimal sum).
pub type CountAdd = fn(&Bytes, &Record) -> Result<Bytes>;

fn count_init() -> Bytes {
    Bytes::from_static(b"0")
}

fn count_add(agg: &Bytes, record: &Record) -> Result<Bytes> {
    let prev = parse_count(agg);
    let delta = parse_count(&record.value);
    Ok(Bytes::from(format!("{}", prev + delta)))
}

/// Word-count style reduce: parse decimal counts, sum by key, emit decimal.
///
/// Input convention: key = group key, value = decimal integer (or empty/`"1"` → 1).
/// Output: key unchanged, value = running total as UTF-8 decimal bytes.
pub fn count_reduce() -> Reduce<MemoryStore, CountInit, CountAdd> {
    Reduce::with_store(MemoryStore::new(), count_init, count_add)
}

/// Word-count reduce backed by an existing [`KeyValueStore`].
pub fn count_reduce_with_store<S>(store: S) -> Reduce<S, CountInit, CountAdd>
where
    S: KeyValueStore,
{
    Reduce::with_store(store, count_init, count_add)
}

/// Word-count reduce with a new [`DurableStore`] at `path` (directory).
///
/// Aggregates survive process restart when reopened at the same path.
/// At-least-once still applies — durable state is not exactly-once.
pub fn count_reduce_durable(
    path: impl AsRef<Path>,
) -> std::result::Result<Reduce<DurableStore, CountInit, CountAdd>, StreamStateError> {
    let store = DurableStore::open(path)?;
    Ok(count_reduce_with_store(store))
}

fn parse_count(v: &Bytes) -> u64 {
    if v.is_empty() {
        return 1;
    }
    std::str::from_utf8(v)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1)
}

impl<S, Init, Add> Operator for Reduce<S, Init, Add>
where
    S: KeyValueStore,
    Init: FnMut() -> Bytes + Send,
    Add: FnMut(&Bytes, &Record) -> Result<Bytes> + Send,
{
    fn process(&mut self, record: Record) -> Result<Vec<Record>> {
        let key_bytes = record
            .key
            .clone()
            .unwrap_or_else(|| Bytes::from_static(b""));
        let current = self.store.get(&key_bytes).unwrap_or_else(|| (self.init)());
        let next = (self.add)(&current, &record)?;
        self.store.put(key_bytes.clone(), next.clone());
        Ok(vec![Record {
            offset: Offset::ZERO,
            key: if key_bytes.is_empty() {
                None
            } else {
                Some(key_bytes)
            },
            value: next,
            timestamp_ms: record.timestamp_ms,
            headers: record.headers,
        }])
    }

    fn name(&self) -> &str {
        "reduce"
    }

    fn begin_checkpoint(&mut self) {
        self.store.begin_checkpoint();
    }

    fn commit_checkpoint(&mut self) -> Result<()> {
        self.store
            .commit_checkpoint()
            .map_err(|e| Error::Storage(e.to_string()))
    }

    fn abort_checkpoint(&mut self) {
        self.store.abort_checkpoint();
    }

    fn staged_changelog(&self) -> Vec<(Bytes, Option<Bytes>)> {
        self.store.staged_changelog()
    }

    fn apply_changelog(&mut self, key: Bytes, value: Option<Bytes>) {
        self.store.apply_changelog(key, value);
    }
}

/// Snapshot of current aggregates (for tests / debugging).
impl<S, Init, Add> Reduce<S, Init, Add>
where
    S: KeyValueStore,
{
    /// Read current aggregate for a key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.store.get(key)
    }

    /// Iterate all aggregates.
    pub fn snapshot(&self) -> Vec<(Bytes, Bytes)> {
        self.store.iter().collect()
    }
}
