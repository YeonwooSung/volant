//! In-memory key-value state store for stream operators.

use std::collections::BTreeMap;

use bytes::Bytes;

/// Key-value store used by stateful operators (`reduce`, windows).
pub trait KeyValueStore: Send {
    /// Look up a value by key.
    fn get(&self, key: &[u8]) -> Option<Bytes>;
    /// Insert or replace a key.
    fn put(&mut self, key: Bytes, value: Bytes);
    /// Remove a key if present.
    fn delete(&mut self, key: &[u8]);
    /// Iterate all entries in key order.
    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + '_>;
    /// Number of entries.
    fn len(&self) -> usize;
    /// Whether the store is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// In-process [`HashMap`](std::collections::HashMap)-backed store (ordered via `BTreeMap`).
#[derive(Debug, Default, Clone)]
pub struct MemoryStore {
    map: BTreeMap<Vec<u8>, Bytes>,
}

impl MemoryStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }
}

impl KeyValueStore for MemoryStore {
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).cloned()
    }

    fn put(&mut self, key: Bytes, value: Bytes) {
        self.map.insert(key.to_vec(), value);
    }

    fn delete(&mut self, key: &[u8]) {
        self.map.remove(key);
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + '_> {
        Box::new(
            self.map
                .iter()
                .map(|(k, v)| (Bytes::copy_from_slice(k), v.clone())),
        )
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}
