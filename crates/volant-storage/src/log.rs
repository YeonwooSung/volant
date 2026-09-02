//! Partition log: ordered collection of segments.

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use parking_lot::Mutex;
use volant_core::{Error, Message, Offset, Record, Result};

use crate::config::StorageConfig;
use crate::group_commit::{GroupCommit, GroupCommitTicket};
use crate::pool::BufferPool;
use crate::record::encoded_record_size;
use crate::segment::{list_segment_bases, Segment, SegmentOptions};

/// Stats from a compaction pass (Phase 16).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactStats {
    /// Records read from sealed segments before compact.
    pub input_records: u64,
    /// Records written to the compacted segment.
    pub output_records: u64,
    /// Sealed segments removed.
    pub segments_removed: u64,
}

/// Append-only partition log backed by durable segments on disk.
#[derive(Debug)]
pub struct PartitionLog {
    config: StorageConfig,
    segments: Vec<Segment>,
    next_offset: Offset,
    appends_since_flush: u64,
    /// Shared encode buffer pool (`None` when `buffer_pool_blocks == 0`).
    pool: Option<Arc<BufferPool>>,
    /// Group-commit coordinator (always present; no-op when `max_ms == 0`).
    group: Arc<GroupCommit>,
    /// Successful `flush()` / fsync count (test hook).
    fsync_count: u64,
}

impl PartitionLog {
    /// Open or create a partition log under `config.data_dir`.
    pub fn open(config: StorageConfig) -> Result<Self> {
        fs::create_dir_all(&config.data_dir)?;

        let pool = if config.buffer_pool_blocks > 0 {
            Some(Arc::new(BufferPool::with_capacity(
                config.buffer_pool_blocks,
                config.buffer_pool_block_size,
            )))
        } else {
            None
        };

        let bases = list_segment_bases(&config.data_dir)?;
        let mut segments = Vec::new();

        let seg_opts = |pool: &Option<Arc<BufferPool>>, sealed: bool| SegmentOptions {
            index_interval_bytes: config.index_interval_bytes,
            use_mmap: config.use_mmap,
            // Direct I/O only for the active (unsealed) segment.
            direct_io: config.direct_io && !sealed,
            io_backend: config.io_backend,
            pool: pool.clone(),
        };

        let group = Arc::new(GroupCommit::new(
            config.group_commit_max_ms,
            config.effective_group_commit_max_records(),
        ));

        if bases.is_empty() {
            let mut seg = Segment::create_with_options(
                &config.data_dir,
                Offset::ZERO,
                now_ms(),
                seg_opts(&pool, false),
            )?;
            seg.set_use_mmap(config.use_mmap);
            let next_offset = seg.next_offset();
            segments.push(seg);
            return Ok(Self {
                config,
                segments,
                next_offset,
                appends_since_flush: 0,
                pool,
                group,
                fsync_count: 0,
            });
        }

        let last = bases.len() - 1;
        for (i, base) in bases.iter().enumerate() {
            let sealed = i != last;
            let seg = Segment::open_with_options(
                &config.data_dir,
                *base,
                seg_opts(&pool, sealed),
                sealed,
            )?;
            segments.push(seg);
        }

        // Validate segment ranges. Compaction may leave holes so sealed
        // next_offset can be strictly less than the next base; never greater.
        for w in segments.windows(2) {
            if w[0].next_offset().raw() > w[1].base_offset().raw() {
                return Err(Error::Storage(format!(
                    "segment overlap: segment ending at {} followed by base {}",
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
            pool,
            group,
            fsync_count: 0,
        })
    }

    /// Append a single message; returns the assigned record.
    ///
    /// Honors `flush_every_n` after this message when group-commit is off.
    /// When `group_commit_max_ms > 0`, waits for a shared group-commit flush
    /// (exclusive `&mut self` cannot coalesce with other appenders — use
    /// [`SharedPartitionLog`] for cross-caller sharing).
    /// Prefer [`Self::append_batch`] when writing multiple messages so the
    /// flush policy runs once per batch.
    pub fn append(&mut self, message: Message) -> Result<Record> {
        let record = self.append_one(message)?;
        self.finish_append(1, true)?;
        Ok(record)
    }

    /// Append a batch of messages with a single flush-policy check at the end.
    ///
    /// Messages receive contiguous offsets. No intermediate `fsync` is issued
    /// between messages; `flush_every_n` / group-commit is evaluated once after
    /// the whole batch (using the cumulative `appends_since_flush` counter).
    pub fn append_batch(
        &mut self,
        messages: impl IntoIterator<Item = Message>,
    ) -> Result<Vec<Record>> {
        self.append_batch_inner(messages, true)
    }

    /// Write a batch without waiting for group-commit (broker produce path).
    ///
    /// Still honors `flush_every_n` when group-commit is off. When group-commit
    /// is on, records stay dirty until [`Self::await_group_commit`] or
    /// [`Self::flush`].
    pub fn append_batch_uncommitted(
        &mut self,
        messages: impl IntoIterator<Item = Message>,
    ) -> Result<Vec<Record>> {
        self.append_batch_inner(messages, false)
    }

    fn append_batch_inner(
        &mut self,
        messages: impl IntoIterator<Item = Message>,
        wait_group: bool,
    ) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        for message in messages {
            records.push(self.append_one(message)?);
        }
        if !records.is_empty() {
            self.finish_append(records.len() as u64, wait_group)?;
        }
        Ok(records)
    }

    fn finish_append(&mut self, n: u64, wait_group: bool) -> Result<()> {
        self.appends_since_flush = self.appends_since_flush.saturating_add(n);
        if self.group_commit_enabled() {
            self.group.add_pending(n);
            if wait_group {
                self.await_group_commit()?;
            }
        } else if self.config.flush_every_n > 0
            && self.appends_since_flush >= self.config.flush_every_n
        {
            self.flush()?;
        }
        Ok(())
    }

    /// Append a message at an exact offset (follower replication path).
    ///
    /// Requires `offset == next_offset` (contiguous LEO). Returns an error on gap
    /// or if the offset is already present.
    pub fn append_with_offset(&mut self, offset: Offset, message: Message) -> Result<Record> {
        if offset.raw() != self.next_offset.raw() {
            return Err(Error::Storage(format!(
                "append_with_offset gap: expected offset {}, got {}",
                self.next_offset.raw(),
                offset.raw()
            )));
        }
        let record = self.append_one(message)?;
        // Replication path: honor flush_every_n only. Group-commit wait is for
        // produce callers (`append` / `append_batch` / `SharedPartitionLog`).
        self.appends_since_flush = self.appends_since_flush.saturating_add(1);
        if self.group_commit_enabled() {
            self.group.add_pending(1);
        } else if self.config.flush_every_n > 0
            && self.appends_since_flush >= self.config.flush_every_n
        {
            self.flush()?;
        }
        Ok(record)
    }

    /// Append multiple records at their exact offsets (must be contiguous from LEO).
    pub fn append_records_with_offsets(&mut self, records: &[Record]) -> Result<Vec<Record>> {
        let mut out = Vec::with_capacity(records.len());
        for r in records {
            let msg = Message {
                key: r.key.clone(),
                value: r.value.clone(),
                timestamp_ms: Some(r.timestamp_ms),
                headers: r.headers.clone(),
            };
            out.push(self.append_with_offset(r.offset, msg)?);
        }
        Ok(out)
    }

    /// Encode and write one message without updating the flush counter.
    ///
    /// Uses `self.next_offset` as the assigned offset.
    fn append_one(&mut self, message: Message) -> Result<Record> {
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
        Ok(record)
    }

    /// Log-end offset (next offset to be written); alias of [`Self::high_watermark`].
    pub fn log_end_offset(&self) -> Offset {
        self.next_offset
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
                let approx = r.value.len() + r.key.as_ref().map(|k| k.len()).unwrap_or(0) + 32;
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
        let n = self.appends_since_flush;
        self.appends_since_flush = 0;
        self.fsync_count = self.fsync_count.saturating_add(1);
        self.group.notify_flushed(n);
        Ok(())
    }

    /// Whether time-based group-commit is enabled for this log.
    pub fn group_commit_enabled(&self) -> bool {
        self.config.group_commit_enabled()
    }

    /// Records written since the last `flush()` (not yet group-committed).
    pub fn has_uncommitted(&self) -> bool {
        self.appends_since_flush > 0
    }

    /// Successful `fsync` count (includes explicit `flush` and group-commit).
    pub fn fsync_count(&self) -> u64 {
        self.fsync_count
    }

    /// Group-commit flush count (`0` when the window is off).
    pub fn group_commit_flushes(&self) -> u64 {
        self.group.flushes()
    }

    /// Records covered by group-commit flushes.
    pub fn group_commit_records(&self) -> u64 {
        self.group.records()
    }

    /// Cloneable handle to the group-commit coordinator.
    pub fn group_commit_handle(&self) -> Arc<GroupCommit> {
        Arc::clone(&self.group)
    }

    /// Register as a waiter for the next group-commit generation.
    pub fn register_group_waiter(&self) -> GroupCommitTicket {
        self.group.register_waiter()
    }

    /// Wait for a shared group-commit flush (no-op when the window is off).
    ///
    /// Exclusive `&mut self` cannot coalesce with other appenders; prefer
    /// [`SharedPartitionLog`] or call this after releasing a higher-level lock.
    pub fn await_group_commit(&mut self) -> Result<()> {
        if !self.group_commit_enabled() {
            return Ok(());
        }
        let handle = Arc::clone(&self.group);
        let ticket = handle.register_waiter();
        handle.wait_or_lead(ticket, || self.flush())
    }

    /// Shared buffer pool, if configured.
    pub fn buffer_pool(&self) -> Option<&Arc<BufferPool>> {
        self.pool.as_ref()
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
            let mut seg = Segment::create_with_options(
                &self.config.data_dir,
                base,
                now_ms(),
                SegmentOptions {
                    index_interval_bytes: self.config.index_interval_bytes,
                    use_mmap: self.config.use_mmap,
                    direct_io: self.config.direct_io,
                    io_backend: self.config.io_backend,
                    pool: self.pool.clone(),
                },
            )?;
            seg.set_use_mmap(self.config.use_mmap);
            self.segments.push(seg);
        }

        Ok(self.log_start_offset())
    }

    /// Current storage config (segment size, retention, etc.).
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Update retention policy fields (Phase 13). Does not run retention immediately.
    pub fn set_retention(&mut self, retention_ms: Option<u64>, retention_bytes: Option<u64>) {
        self.config.retention_ms = retention_ms;
        self.config.retention_bytes = retention_bytes;
    }

    /// Enable or disable key compaction on sealed segments (Phase 16).
    pub fn set_compact(&mut self, compact: bool) {
        self.config.compact = compact;
    }

    /// Whether compaction is enabled.
    pub fn compact_enabled(&self) -> bool {
        self.config.compact
    }

    /// Update target segment roll size (Phase 13). `0` is ignored.
    pub fn set_segment_size(&mut self, segment_size: u64) {
        if segment_size > 0 {
            self.config.segment_size = segment_size;
        }
    }

    /// Number of on-disk segments.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Total size of all segment files in bytes.
    pub fn total_size(&self) -> u64 {
        self.segments.iter().map(|s| s.size()).sum()
    }

    /// Apply configured size and/or time retention policies, then compaction.
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

        if self.config.compact {
            let _ = self.compact_sealed()?;
        }

        Ok(())
    }

    /// Compact sealed segments: keep latest value per key; empty value = tombstone.
    ///
    /// Null-key records are retained. Active segment is not modified. Preserves
    /// original offsets of survivors (sparse holes allowed).
    pub fn compact_sealed(&mut self) -> Result<CompactStats> {
        if self.segments.len() < 2 {
            return Ok(CompactStats::default());
        }
        let sealed_count = self.segments.len() - 1;
        let active_base = self.segments[sealed_count].base_offset();
        let compact_base = self.segments[0].base_offset();

        // Collect all sealed records.
        let mut input: Vec<Record> = Vec::new();
        for seg in &self.segments[..sealed_count] {
            let recs = seg.read_from(seg.base_offset(), usize::MAX, usize::MAX)?;
            input.extend(recs);
        }
        let input_records = input.len() as u64;
        if input_records == 0 {
            return Ok(CompactStats::default());
        }

        // Latest keyed value; empty value removes key. Null keys all kept.
        let mut latest: HashMap<Bytes, Record> = HashMap::new();
        let mut null_keys: Vec<Record> = Vec::new();
        for r in input {
            match &r.key {
                None => null_keys.push(r),
                Some(k) => {
                    if r.value.is_empty() {
                        latest.remove(k);
                    } else {
                        latest.insert(k.clone(), r);
                    }
                }
            }
        }
        let mut survivors: Vec<Record> = latest.into_values().collect();
        survivors.extend(null_keys);
        survivors.sort_by_key(|r| r.offset.raw());
        let output_records = survivors.len() as u64;

        // Skip rewrite if nothing removed (still OK to no-op).
        if output_records == input_records && sealed_count == 1 {
            // Single sealed segment, no drops — nothing to do.
            return Ok(CompactStats {
                input_records,
                output_records,
                segments_removed: 0,
            });
        }

        let opts = SegmentOptions {
            index_interval_bytes: self.config.index_interval_bytes,
            use_mmap: self.config.use_mmap,
            direct_io: false,
            io_backend: self.config.io_backend,
            pool: self.pool.clone(),
        };
        let dir = self.config.data_dir.clone();
        let tmp_dir = dir.join(format!(".compact-{}", compact_base.raw()));
        if tmp_dir.exists() {
            fs::remove_dir_all(&tmp_dir)
                .map_err(|e| Error::Storage(format!("remove compact tmp: {e}")))?;
        }
        fs::create_dir_all(&tmp_dir)
            .map_err(|e| Error::Storage(format!("create compact tmp: {e}")))?;

        let mut new_seg =
            Segment::create_with_options(&tmp_dir, compact_base, now_ms(), opts.clone())?;
        for r in &survivors {
            let msg = Message {
                key: r.key.clone(),
                value: r.value.clone(),
                timestamp_ms: Some(r.timestamp_ms),
                headers: r.headers.clone(),
            };
            new_seg.append_allow_gap(r.offset, &msg, r.timestamp_ms)?;
        }
        // Stretch logical end to active base so open continuity holds.
        new_seg.set_next_offset(active_base)?;
        new_seg.seal()?;
        // Drop handle before moving files.
        drop(new_seg);

        // Remove old sealed segments from memory and disk.
        let old_sealed: Vec<Segment> = self.segments.drain(..sealed_count).collect();
        let segments_removed = old_sealed.len() as u64;
        for seg in &old_sealed {
            seg.delete_files()?;
        }

        // Move compacted segment into place (same base offset name).
        let src_log = tmp_dir.join(format!("{:020}.log", compact_base.raw()));
        let src_idx = tmp_dir.join(format!("{:020}.index", compact_base.raw()));
        let dst_log = dir.join(format!("{:020}.log", compact_base.raw()));
        let dst_idx = dir.join(format!("{:020}.index", compact_base.raw()));
        // Old files already deleted; rename into place.
        fs::rename(&src_log, &dst_log)
            .map_err(|e| Error::Storage(format!("rename compact log: {e}")))?;
        fs::rename(&src_idx, &dst_idx)
            .map_err(|e| Error::Storage(format!("rename compact index: {e}")))?;
        let _ = fs::remove_dir_all(&tmp_dir);

        let mut sealed = Segment::open_with_options(&dir, compact_base, opts, true)?;
        // Recovery sets next from last record; stretch to active base.
        sealed.set_next_offset(active_base)?;
        self.segments.insert(0, sealed);

        Ok(CompactStats {
            input_records,
            output_records,
            segments_removed,
        })
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
        let opts = SegmentOptions {
            index_interval_bytes: self.config.index_interval_bytes,
            use_mmap: self.config.use_mmap,
            direct_io: self.config.direct_io,
            io_backend: self.config.io_backend,
            pool: self.pool.clone(),
        };
        let dir = self.config.data_dir.clone();
        let base = self.next_offset;

        let active = self
            .segments
            .last_mut()
            .ok_or_else(|| Error::Storage("no active segment to roll".into()))?;
        active.seal()?;

        let mut new_seg = Segment::create_with_options(&dir, base, now_ms(), opts)?;
        new_seg.set_use_mmap(self.config.use_mmap);
        self.segments.push(new_seg);
        Ok(())
    }
}

/// Shareable partition log: appenders release the log lock before waiting
/// so concurrent callers on the same partition can share one `fsync`.
#[derive(Debug, Clone)]
pub struct SharedPartitionLog {
    inner: Arc<Mutex<PartitionLog>>,
}

impl SharedPartitionLog {
    /// Open or create a partition log under `config.data_dir`.
    pub fn open(config: StorageConfig) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(PartitionLog::open(config)?)),
        })
    }

    /// Wrap an existing exclusive log.
    pub fn from_log(log: PartitionLog) -> Self {
        Self {
            inner: Arc::new(Mutex::new(log)),
        }
    }

    /// Append one message; waits for group-commit when the window is on.
    pub fn append(&self, message: Message) -> Result<Record> {
        let mut log = self.inner.lock();
        if !log.group_commit_enabled() {
            return log.append(message);
        }
        let record = log.append_one(message)?;
        log.finish_append(1, false)?;
        let handle = log.group_commit_handle();
        let ticket = handle.register_waiter();
        drop(log);
        handle.wait_or_lead(ticket, || self.inner.lock().flush())?;
        Ok(record)
    }

    /// Append a batch with a single group-commit wait at the end.
    pub fn append_batch(&self, messages: impl IntoIterator<Item = Message>) -> Result<Vec<Record>> {
        let mut log = self.inner.lock();
        if !log.group_commit_enabled() {
            return log.append_batch(messages);
        }
        let records = log.append_batch_uncommitted(messages)?;
        if records.is_empty() {
            return Ok(records);
        }
        let handle = log.group_commit_handle();
        let ticket = handle.register_waiter();
        drop(log);
        handle.wait_or_lead(ticket, || self.inner.lock().flush())?;
        Ok(records)
    }

    /// Read records starting at `from`.
    pub fn read(&self, from: Offset, max_messages: usize) -> Result<Vec<Record>> {
        self.inner.lock().read(from, max_messages)
    }

    /// Immediate fsync of the active segment.
    pub fn flush(&self) -> Result<()> {
        self.inner.lock().flush()
    }

    /// Successful `fsync` count.
    pub fn fsync_count(&self) -> u64 {
        self.inner.lock().fsync_count()
    }

    /// Group-commit flush count.
    pub fn group_commit_flushes(&self) -> u64 {
        self.inner.lock().group_commit_flushes()
    }

    /// Records covered by group-commit flushes.
    pub fn group_commit_records(&self) -> u64 {
        self.inner.lock().group_commit_records()
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
            ..StorageConfig::default()
        }
    }

    fn tmp() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("volant-log-{}-{}-{}", std::process::id(), n, nanos));
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

    #[test]
    fn append_with_pool_enabled() {
        let dir = tmp();
        let mut c = cfg(&dir);
        c.buffer_pool_blocks = 8;
        c.buffer_pool_block_size = 64 * 1024;
        let mut log = PartitionLog::open(c).unwrap();
        assert!(log.buffer_pool().is_some());
        for i in 0..50 {
            log.append(Message::from_value(format!("p{i}"))).unwrap();
        }
        log.flush().unwrap();
        let got = log.read(Offset::ZERO, 100).unwrap();
        assert_eq!(got.len(), 50);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_with_offset_contiguous() {
        let dir = tmp();
        let mut log = PartitionLog::open(cfg(&dir)).unwrap();
        let r0 = log
            .append_with_offset(Offset::ZERO, Message::from_value("a"))
            .unwrap();
        assert_eq!(r0.offset.raw(), 0);
        let r1 = log
            .append_with_offset(Offset::new(1), Message::from_value("b"))
            .unwrap();
        assert_eq!(r1.offset.raw(), 1);
        assert_eq!(log.log_end_offset().raw(), 2);
        // gap rejected
        assert!(log
            .append_with_offset(Offset::new(5), Message::from_value("x"))
            .is_err());
        let got = log.read(Offset::ZERO, 10).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].value.as_ref(), b"b");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_records_with_offsets_batch() {
        let dir = tmp();
        let mut leader = PartitionLog::open(cfg(&dir.join("leader"))).unwrap();
        leader.append(Message::from_value("a")).unwrap();
        leader.append(Message::from_value("b")).unwrap();
        leader.append(Message::from_value("c")).unwrap();
        let recs = leader.read(Offset::ZERO, 10).unwrap();

        let mut follower = PartitionLog::open(cfg(&dir.join("follower"))).unwrap();
        follower.append_records_with_offsets(&recs).unwrap();
        assert_eq!(follower.log_end_offset().raw(), 3);
        let got = follower.read(Offset::ZERO, 10).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[2].value.as_ref(), b"c");
        let _ = fs::remove_dir_all(&dir);
    }
}
