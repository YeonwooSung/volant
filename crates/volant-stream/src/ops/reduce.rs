//! Keyed reduce operator.

use bytes::Bytes;
use volant_core::{Offset, Record, Result};

use crate::operator::Operator;
use crate::state::{KeyValueStore, MemoryStore};

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
}

/// Build a reduce operator backed by [`MemoryStore`].
pub fn reduce<Init, Add>(init: Init, add: Add) -> Reduce<MemoryStore, Init, Add>
where
    Init: FnMut() -> Bytes + Send + 'static,
    Add: FnMut(&Bytes, &Record) -> Result<Bytes> + Send + 'static,
{
    Reduce::with_store(MemoryStore::new(), init, add)
}

/// Word-count style reduce: parse decimal counts, sum by key, emit decimal.
///
/// Input convention: key = group key, value = decimal integer (or empty/`"1"` → 1).
/// Output: key unchanged, value = running total as UTF-8 decimal bytes.
pub fn count_reduce() -> Reduce<MemoryStore, impl FnMut() -> Bytes, impl FnMut(&Bytes, &Record) -> Result<Bytes>>
{
    reduce(
        || Bytes::from_static(b"0"),
        |agg, record| {
            let prev = parse_count(agg);
            let delta = parse_count(&record.value);
            Ok(Bytes::from(format!("{}", prev + delta)))
        },
    )
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
        let current = self
            .store
            .get(&key_bytes)
            .unwrap_or_else(|| (self.init)());
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
