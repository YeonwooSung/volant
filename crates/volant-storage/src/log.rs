//! Partition log: ordered collection of segments.

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use volant_core::{Error, Message, Offset, Record, Result};

use crate::config::StorageConfig;
use crate::record::encoded_record_size;
use crate::segment::{list_segment_bases, Segment};

/// Append-only partition log backed by durable segments on disk.
#[derive(Debug)]
pub struct PartitionLog {
    config: StorageConfig,
    segments: Vec<Segment>,
    next_offset: Offset,
    appends_since_flush: u64,
}

impl PartitionLog {
    /// Open or create a partition log under `config.data_dir`.
    pub fn open(config: StorageConfig) -> Result<Self> {
        fs::create_dir_all(&config.data_dir)?;

        let bases = list_segment_bases(&config.data_dir)?;
        let mut segments = Vec::new();

        if bases.is_empty() {
            let mut seg = Segment::create(
                &config.data_dir,
                Offset::ZERO,
                now_ms(),
                config.index_interval_bytes,
            )?;
            seg.set_use_mmap(config.use_mmap);
            let next_offset = seg.next_offset();
            segments.push(seg);
            return Ok(Self {
                config,
                segments,
                next_offset,
                appends_since_flush: 0,
            });
        }

        let last = bases.len() - 1;
        for (i, base) in bases.iter().enumerate() {
            let sealed = i != last;
            let seg = if sealed {
                Segment::open_sealed(
                    &config.data_dir,
                    *base,
                    config.index_interval_bytes,
                    config.use_mmap,
                )?
            } else {
                Segment::open(
                    &config.data_dir,
                    *base,
                    config.index_interval_bytes,
                    config.use_mmap,
                )?
            };
            segments.push(seg);
        }

        // Validate continuity of segment ranges.
        for w in segments.windows(2) {
            if w[0].next_offset() != w[1].base_offset() {
                return Err(Error::Storage(format!(
                    "segment gap: segment ending at {} followed by base {}",
                    w[0].next_offset(),
                    w[1].base_offset()
                )));
            }
        }

        let next_offset = segments
            .last()
            .map(|s| s.next_offset())
            .unwrap_or(Offset::ZERO);

        Ok(Self {
            config,
            segments,
            next_offset,
            appends_since_flush: 0,
        })
    }

    /// Append a single message; returns the assigned record.
    pub fn append(&mut self, message: Message) -> Result<Record> {
        let timestamp_ms = message.timestamp_ms.unwrap_or_else(now_ms);
        let record_size = encoded_record_size(&message);

        if self.needs_roll(record_size) {
            self.roll()?;
        }

        let offset = self.next_offset;
        let active = self
            .segments
            .last_mut()
            .ok_or_else(|| Error::Storage("no active segment".into()))?;
        let record = active.append(offset, &message, timestamp_ms)?;
        self.next_offset = offset.next();

        self.appends_since_flush += 1;
        if self.config.flush_every_n > 0 && self.appends_since_flush >= self.config.flush_every_n
        {
            self.flush()?;
        }

        Ok(record)
    }

    /// Read up to `max_messages` records starting at `from`.
    pub fn read(&self, from: Offset, max_messages: usize) -> Result<Vec<Record>> {
        self.read_bytes(from, max_messages, usize::MAX)
    }

    /// Read records with both message-count and approximate byte limits.
    pub fn read_bytes(
        &self,
        from: Offset,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        let log_start = self.log_start_offset();
        let from = if from.raw() < log_start.raw() {
            log_start
        } else {
            from
        };

        if from.raw() >= self.high_watermark().raw() || max_messages == 0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        let mut bytes_acc = 0usize;

        for seg in &self.segments {
            if seg.next_offset().raw() <= from.raw() {
                continue;
            }
            if out.len() >= max_messages {
                break;
            }
            if max_bytes != usize::MAX && bytes_acc >= max_bytes && !out.is_empty() {
                break;
            }

            let remaining_msgs = max_messages - out.len();
            let remaining_bytes = if max_bytes == usize::MAX {
                usize::MAX
            } else {
                max_bytes.saturating_sub(bytes_acc)
            };

            let start = if from.raw() < seg.base_offset().raw() {
                seg.base_offset()
            } else {
                from
            };

            let recs = seg.read_from(start, remaining_msgs, remaining_bytes)?;
            for r in recs {
                let approx = r.value.len()
                    + r.key.as_ref().map(|k| k.len()).unwrap_or(0)
                    + 32;
                bytes_acc = bytes_acc.saturating_add(approx);
                out.push(r);
            }
        }

        Ok(out)
    }

    /// High-water mark (next offset to be written).
    pub fn high_watermark(&self) -> Offset {
        self.next_offset
    }

    /// Earliest available offset (base of the first segment).
    pub fn log_start_offset(&self) -> Offset {
        self.segments
            .first()
            .map(|s| s.base_offset())
            .unwrap_or(Offset::ZERO)
    }

    /// Flush (fsync) the active segment data and index.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(active) = self.segments.last_mut() {
            active.flush()?;
        }
        self.appends_since_flush = 0;
        Ok(())
    }

    /// Drop whole segments whose records are entirely before `before_offset`.
    ///
    /// Returns the new log start offset.
    pub fn delete_records(&mut self, before_offset: Offset) -> Result<Offset> {
        // Segments with next_offset <= before_offset contain only offsets < before_offset.
        let mut keep_from = 0usize;
        for (i, seg) in self.segments.iter().enumerate() {
            if seg.next_offset().raw() <= before_offset.raw() {
                keep_from = i + 1;
            } else {
                break;
            }
        }

        if keep_from == 0 {
            return Ok(self.log_start_offset());
        }

        let to_delete: Vec<Segment> = self.segments.drain(..keep_from).collect();
        for seg in &to_delete {
            seg.delete_files()?;
        }

        if self.segments.is_empty() {
            // Recreate an empty active segment at the high watermark.
            let base = self.next_offset;
            let mut seg = Segment::create(
                &self.config.data_dir,
                base,
                now_ms(),
                self.config.index_interval_bytes,
            )?;
            seg.set_use_mmap(self.config.use_mmap);
            self.segments.push(seg);
        }

        Ok(self.log_start_offset())
    }

    /// Apply configured size and/or time retention policies.
    pub fn apply_retention(&mut self) -> Result<()> {
        // Time-based: drop oldest segments whose last timestamp is older than cutoff.
        if let Some(retention_ms) = self.config.retention_ms {
            let now = now_ms();
            let cutoff = now.saturating_sub(retention_ms as i64);
            let mut drop_count = 0usize;
            let n = self.segments.len();
            for (i, seg) in self.segments.iter().enumerate() {
                // Always retain the active (last) segment.
                if i + 1 == n {
                    break;
                }
                if seg.last_timestamp_ms() < cutoff {
                    drop_count = i + 1;
                } else {
                    break;
                }
            }
            if drop_count > 0 {
                let before = self.segments[drop_count].base_offset();
                self.delete_records(before)?;
            }
        }

        // Size-based: drop oldest segments until total size <= retention_bytes.
        if let Some(limit) = self.config.retention_bytes {
            loop {
                let total: u64 = self.segments.iter().map(|s| s.size()).sum();
                if total <= limit {
                    break;
                }
                if self.segments.len() <= 1 {
                    break;
                }
                let before = self.segments[1].base_offset();
                self.delete_records(before)?;
            }
        }

        Ok(())
    }

    fn needs_roll(&self, record_size: u64) -> bool {
        let Some(active) = self.segments.last() else {
            return false;
        };
        // Always allow at least one record per segment.
        if active.next_offset() == active.base_offset() {
            return false;
        }
        active.size().saturating_add(record_size) > self.config.segment_size
    }

    fn roll(&mut self) -> Result<()> {
        let use_mmap = self.config.use_mmap;
        let interval = self.config.index_interval_bytes;
        let dir = self.config.data_dir.clone();
        let base = self.next_offset;

        let active = self
            .segments
            .last_mut()
            .ok_or_else(|| Error::Storage("no active segment to roll".into()))?;
        active.seal()?;

        let mut new_seg = Segment::create(&dir, base, now_ms(), interval)?;
        new_seg.set_use_mmap(use_mmap);
        self.segments.push(new_seg);
        Ok(())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use volant_core::Message;

    fn cfg(dir: &std::path::Path) -> StorageConfig {
        StorageConfig {
            data_dir: dir.to_path_buf(),
            segment_size: 256 * 1024 * 1024,
            use_mmap: true,
            flush_every_n: 0,
            index_interval_bytes: 4096,
            retention_ms: None,
            retention_bytes: None,
        }
    }

    fn tmp() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("volant-log-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn append_read_roundtrip() {
        let dir = tmp();
        let mut log = PartitionLog::open(cfg(&dir)).unwrap();
        let r = log.append(Message::from_value("hi")).unwrap();
        assert_eq!(r.offset.raw(), 0);
        let got = log.read(Offset::ZERO, 10).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].value.as_ref(), b"hi");
        let _ = fs::remove_dir_all(&dir);
    }
}
