//! Tumbling window aggregator.

use std::collections::HashMap;

use bytes::Bytes;
use volant_core::{Offset, Record, Result};

use crate::operator::Operator;

/// Tumbling window that sums per-key counts and emits at window boundaries.
///
/// Event time is `record.timestamp_ms`; if `0`, processing time from
/// [`punctuate`](Operator::punctuate) is used when available, otherwise `0`.
///
/// On each input the value is treated as a decimal count (empty/`"1"` → 1).
/// When event time advances past a window end, or on `punctuate`, closed
/// windows emit one record per key: key = word, value = decimal total.
pub struct TumblingWindow {
    size_ms: i64,
    /// (window_start, key) → count
    buckets: HashMap<(i64, Vec<u8>), u64>,
    /// Highest event time seen (for advancing).
    max_event_ms: i64,
}

impl TumblingWindow {
    /// Create a tumbling window of `size_ms` milliseconds (must be > 0).
    pub fn new(size_ms: i64) -> Self {
        assert!(size_ms > 0, "window size must be positive");
        Self {
            size_ms,
            buckets: HashMap::new(),
            max_event_ms: 0,
        }
    }

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

    fn emit_closed(&mut self, up_to_exclusive: i64) -> Vec<Record> {
        let size = self.size_ms;
        let mut closed_starts: Vec<i64> = self
            .buckets
            .keys()
            .map(|(start, _)| *start)
            .filter(|start| start + size <= up_to_exclusive)
            .collect();
        closed_starts.sort_unstable();
        closed_starts.dedup();

        let mut out = Vec::new();
        for start in closed_starts {
            let keys: Vec<Vec<u8>> = self
                .buckets
                .keys()
                .filter(|(s, _)| *s == start)
                .map(|(_, k)| k.clone())
                .collect();
            for key in keys {
                if let Some(count) = self.buckets.remove(&(start, key.clone())) {
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
            }
        }
        out
    }
}

impl Operator for TumblingWindow {
    fn process(&mut self, record: Record) -> Result<Vec<Record>> {
        let ts = if record.timestamp_ms > 0 {
            record.timestamp_ms
        } else {
            self.max_event_ms
        };
        if ts > self.max_event_ms {
            self.max_event_ms = ts;
        }
        // Emit windows that ended before this event.
        let out = self.emit_closed(ts);

        let start = self.window_start(ts);
        let key = record
            .key
            .as_ref()
            .map(|k| k.to_vec())
            .unwrap_or_default();
        let delta = Self::parse_count(&record.value);
        *self.buckets.entry((start, key)).or_insert(0) += delta;
        Ok(out)
    }

    fn punctuate(&mut self, now_ms: i64) -> Result<Vec<Record>> {
        let watermark = now_ms.max(self.max_event_ms);
        // Emit all windows that have fully ended by `now_ms`.
        // Use now_ms + 1 style: windows with end <= now are closed when
        // punctuate is called at/after window end.
        Ok(self.emit_closed(watermark.saturating_add(1)))
    }

    fn name(&self) -> &str {
        "tumbling_window"
    }
}
