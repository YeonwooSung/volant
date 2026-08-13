//! Process-local in-memory key-value store.

use std::collections::BTreeMap;

use bytes::Bytes;

use super::KeyValueStore;

/// In-process ordered store (`BTreeMap`). State is lost on process exit.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_crud() {
        let mut s = MemoryStore::new();
        assert!(s.is_empty());
        s.put(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        assert_eq!(s.get(b"a").as_deref(), Some(b"1".as_ref()));
        assert_eq!(s.len(), 1);
        s.delete(b"a");
        assert!(s.is_empty());
    }
}
