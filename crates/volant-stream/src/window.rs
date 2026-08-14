//! Tumbling window aggregator.

use std::path::Path;

use bytes::Bytes;
use volant_core::{Error, Offset, Record, Result};

use crate::operator::Operator;
use crate::state::{DurableStore, KeyValueStore, MemoryStore, StreamStateError};

/// Metadata key for highest event time seen.
const META_MAX_EVENT: &[u8] = b"\x00max_event_ms";
/// Metadata key for the window size this store was written with.
const META_SIZE: &[u8] = b"\x00size_ms";
/// Bucket key prefix: `0x01 || i64_be(window_start) || record_key`.
const BUCKET_PREFIX: u8 = 0x01;

/// Tumbling window that sums per-key counts and emits at window boundaries.
///
/// Event time is `record.timestamp_ms`; if `0`, processing time from
/// [`punctuate`](Operator::punctuate) is used when available, otherwise `0`.
///
/// On each input the value is treated as a decimal count (empty/`"1"` → 1).
/// When event time advances past a window end, or on `punctuate`, closed
/// windows emit one record per key: key = word, value = decimal total.
///
/// Default [`TumblingWindow::new`] uses [`MemoryStore`] (lost on restart).
/// [`TumblingWindow::durable`] persists open buckets via [`DurableStore`] so
/// a process reopening the same directory restores them. In-process only —
/// not distributed EOS / cluster 2PC.
pub struct TumblingWindow<S = MemoryStore> {
    size_ms: i64,
    store: S,
}

impl TumblingWindow<MemoryStore> {
    /// Create an in-memory tumbling window of `size_ms` milliseconds (must be > 0).
    pub fn new(size_ms: i64) -> Self {
        Self::with_store(size_ms, MemoryStore::new())
    }
}

impl TumblingWindow<DurableStore> {
    /// Create a durable tumbling window under `path` (directory).
    ///
    /// Buckets, max event time, and `size_ms` survive process restart when
    /// reopened at the same path. A different `size_ms` returns
    /// [`StreamStateError::WindowSizeMismatch`]. Use a distinct directory from
    /// other durable operators (they share one redb table).
    ///
    /// Outside a checkpoint, each mutation is immediate (ALO). Under EOS
    /// checkpoint hooks, puts/deletes stage until commit (process-local,
    /// not distributed 2PC).
    pub fn durable(
        size_ms: i64,
        path: impl AsRef<Path>,
    ) -> std::result::Result<Self, StreamStateError> {
        if size_ms <= 0 {
            return Err(StreamStateError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "window size must be positive",
            )));
        }
        let store = DurableStore::open(path)?;
        let mut window = Self::with_store(size_ms, store);
        window.bind_size_ms()?;
        Ok(window)
    }
}

impl<S> TumblingWindow<S> {
    /// Create a tumbling window with an injected store (memory or durable).
    pub fn with_store(size_ms: i64, store: S) -> Self {
        assert!(size_ms > 0, "window size must be positive");
        Self { size_ms, store }
    }

    /// Borrow the underlying store (tests / inspection).
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Mutable borrow of the underlying store.
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Window size in milliseconds.
    pub fn size_ms(&self) -> i64 {
        self.size_ms
    }
}

impl<S: KeyValueStore> TumblingWindow<S> {
    fn window_start(&self, ts: i64) -> i64 {
        if ts < 0 {
            return 0;
        }
        (ts / self.size_ms) * self.size_ms
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

    fn bucket_key(start: i64, key: &[u8]) -> Bytes {
        let mut buf = Vec::with_capacity(1 + 8 + key.len());
        buf.push(BUCKET_PREFIX);
        buf.extend_from_slice(&start.to_be_bytes());
        buf.extend_from_slice(key);
        Bytes::from(buf)
    }

    fn parse_bucket_key(raw: &[u8]) -> Option<(i64, Vec<u8>)> {
        if raw.first().copied() != Some(BUCKET_PREFIX) || raw.len() < 9 {
            return None;
        }
        let start = i64::from_be_bytes(raw[1..9].try_into().ok()?);
        Some((start, raw[9..].to_vec()))
    }

    fn encode_u64(n: u64) -> Bytes {
        Bytes::copy_from_slice(&n.to_be_bytes())
    }

    fn decode_u64(v: &[u8]) -> Option<u64> {
        let arr: [u8; 8] = v.try_into().ok()?;
        Some(u64::from_be_bytes(arr))
    }

    fn encode_i64(n: i64) -> Bytes {
        Bytes::copy_from_slice(&n.to_be_bytes())
    }

    fn decode_i64(v: &[u8]) -> Option<i64> {
        let arr: [u8; 8] = v.try_into().ok()?;
        Some(i64::from_be_bytes(arr))
    }

    /// Highest event time seen (restored from the store after restart).
    pub fn max_event_ms(&self) -> i64 {
        self.store
            .get(META_MAX_EVENT)
            .and_then(|v| Self::decode_i64(&v))
            .unwrap_or(0)
    }

    fn set_max_event_ms(&mut self, ts: i64) {
        self.store
            .put(Bytes::from_static(META_MAX_EVENT), Self::encode_i64(ts));
    }

    /// Persist `size_ms` on first use; error if the store was written with another size.
    fn bind_size_ms(&mut self) -> std::result::Result<(), StreamStateError> {
        match self.store.get(META_SIZE) {
            None => {
                self.store.put(
                    Bytes::from_static(META_SIZE),
                    Self::encode_i64(self.size_ms),
                );
                Ok(())
            }
            Some(v) => {
                let stored = Self::decode_i64(&v).ok_or_else(|| {
                    StreamStateError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "corrupt window size metadata",
                    ))
                })?;
                if stored != self.size_ms {
                    return Err(StreamStateError::WindowSizeMismatch {
                        stored,
                        requested: self.size_ms,
                    });
                }
                Ok(())
            }
        }
    }

    /// Snapshot of open buckets: `(window_start, key, count)`.
    pub fn buckets(&self) -> Vec<(i64, Bytes, u64)> {
        let mut out: Vec<(i64, Bytes, u64)> = self
            .store
            .iter()
            .filter_map(|(k, v)| {
                let (start, key) = Self::parse_bucket_key(&k)?;
                let count = Self::decode_u64(&v)?;
                Some((start, Bytes::from(key), count))
            })
            .collect();
        out.sort_by(|(a_s, a_k, _), (b_s, b_k, _)| a_s.cmp(b_s).then(a_k.cmp(b_k)));
        out
    }

    fn emit_closed(&mut self, up_to_exclusive: i64) -> Vec<Record> {
        let size = self.size_ms;
        let mut closed: Vec<(i64, Vec<u8>, u64)> = self
            .store
            .iter()
            .filter_map(|(k, v)| {
                let (start, key) = Self::parse_bucket_key(&k)?;
                if start + size <= up_to_exclusive {
                    let count = Self::decode_u64(&v)?;
                    Some((start, key, count))
                } else {
                    None
                }
            })
            .collect();
        closed.sort_by(|(a_s, a_k, _), (b_s, b_k, _)| a_s.cmp(b_s).then(a_k.cmp(b_k)));

        let mut out = Vec::new();
        for (start, key, count) in closed {
            self.store.delete(Self::bucket_key(start, &key).as_ref());
            out.push(Record {
                offset: Offset::ZERO,
                key: if key.is_empty() {
                    None
                } else {
                    Some(Bytes::from(key))
                },
                value: Bytes::from(count.to_string()),
                timestamp_ms: start,
                headers: Vec::new(),
            });
        }
        out
    }
}

impl<S: KeyValueStore> Operator for TumblingWindow<S> {
    fn process(&mut self, record: Record) -> Result<Vec<Record>> {
        self.bind_size_ms()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let max_event = self.max_event_ms();
        let ts = if record.timestamp_ms > 0 {
            record.timestamp_ms
        } else {
            max_event
        };
        if ts > max_event {
            self.set_max_event_ms(ts);
        }
        // Emit windows that ended before this event.
        let out = self.emit_closed(ts);

        let start = self.window_start(ts);
        let key = record.key.as_ref().map(|k| k.to_vec()).unwrap_or_default();
        let delta = Self::parse_count(&record.value);
        let bk = Self::bucket_key(start, &key);
        let prev = self
            .store
            .get(&bk)
            .and_then(|v| Self::decode_u64(&v))
            .unwrap_or(0);
        self.store.put(bk, Self::encode_u64(prev + delta));
        Ok(out)
    }

    fn punctuate(&mut self, now_ms: i64) -> Result<Vec<Record>> {
        self.bind_size_ms()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let watermark = now_ms.max(self.max_event_ms());
        // Emit all windows that have fully ended by `now_ms`.
        // Use now_ms + 1 style: windows with end <= now are closed when
        // punctuate is called at/after window end.
        Ok(self.emit_closed(watermark.saturating_add(1)))
    }

    fn name(&self) -> &str {
        "tumbling_window"
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(key: &[u8], value: &str, ts: i64) -> Record {
        Record {
            offset: Offset::ZERO,
            key: Some(Bytes::copy_from_slice(key)),
            value: Bytes::from(value.to_owned()),
            timestamp_ms: ts,
            headers: vec![],
        }
    }

    #[test]
    fn memory_emits_at_boundary() {
        let mut w = TumblingWindow::new(1000);
        assert!(w.process(rec(b"foo", "1", 100)).unwrap().is_empty());
        assert!(w.process(rec(b"foo", "1", 200)).unwrap().is_empty());
        let out = w.process(rec(b"bar", "1", 1500)).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].key.as_deref(), Some(b"foo".as_ref()));
        assert_eq!(out[0].value.as_ref(), b"2");
        let flushed = w.punctuate(2000).unwrap();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].key.as_deref(), Some(b"bar".as_ref()));
        assert_eq!(flushed[0].value.as_ref(), b"1");
    }
}
