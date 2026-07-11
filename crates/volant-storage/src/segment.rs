//! Log segment: append-only `.log` + sparse `.index` pair.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memmap2::Mmap;
use volant_core::{Error, Message, Offset, Record, Result};

use crate::index::{IndexEntry, SparseIndex};
use crate::io::{
    align_up, apply_direct_io_flag, create_io_backend, IoBackend, IoBackendKind, DIRECT_IO_ALIGN,
};
use crate::pool::BufferPool;
use crate::record::{
    decode_header, decode_record_at, encode_header, encode_record, encoded_record_size,
    DecodeStatus, HEADER_SIZE,
};

/// Options controlling how a segment is opened / written.
#[derive(Debug, Clone)]
pub struct SegmentOptions {
    /// Sparse index interval in payload bytes.
    pub index_interval_bytes: u32,
    /// Use mmap for sealed / read paths.
    pub use_mmap: bool,
    /// Request O_DIRECT on the log file (feature-gated; safe fallback).
    pub direct_io: bool,
    /// I/O backend kind for unbuffered / direct writes and fsync.
    pub io_backend: IoBackendKind,
    /// Optional shared encode buffer pool.
    pub pool: Option<Arc<BufferPool>>,
}

impl Default for SegmentOptions {
    fn default() -> Self {
        Self {
            index_interval_bytes: 4096,
            use_mmap: true,
            direct_io: false,
            io_backend: IoBackendKind::Std,
            pool: None,
        }
    }
}

/// An appendable (or sealed) log segment on disk.
pub struct Segment {
    dir: PathBuf,
    log_path: PathBuf,
    index_path: PathBuf,
    base_offset: Offset,
    next_offset: Offset,
    /// Current logical size of the `.log` file in bytes (including header; excludes O_DIRECT pad).
    size: u64,
    last_timestamp_ms: i64,
    created_at_ms: i64,
    index_interval_bytes: u32,
    /// Bytes of payload written since the last index entry.
    bytes_since_index: u64,
    index: SparseIndex,
    /// Buffered writer for the active (unsealed) segment log file (non-direct path).
    log_writer: Option<std::io::BufWriter<File>>,
    /// Raw file handle for direct-I/O / backend write path.
    log_file: Option<File>,
    /// Buffered writer for the active segment index file.
    index_writer: Option<std::io::BufWriter<File>>,
    /// Memory map of the log file (sealed segments, or refreshed for reads).
    mmap: Option<Mmap>,
    use_mmap: bool,
    sealed: bool,
    /// Shared encode buffer pool (optional).
    pool: Option<Arc<BufferPool>>,
    /// True when the log was opened with O_DIRECT (or direct write mode).
    direct_io: bool,
    /// Physical bytes written to the log file (always multiple of DIRECT_IO_ALIGN when direct).
    physical_size: u64,
    /// Pending (not yet durable-aligned) encode bytes for direct-I/O path.
    pending: Vec<u8>,
    /// I/O backend for pwrite / fsync on the direct path (and optional fsync on buffered).
    backend: Option<Box<dyn IoBackend>>,
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment")
            .field("log_path", &self.log_path)
            .field("base_offset", &self.base_offset)
            .field("next_offset", &self.next_offset)
            .field("size", &self.size)
            .field("sealed", &self.sealed)
            .field("direct_io", &self.direct_io)
            .finish()
    }
}

impl Segment {
    /// Create a new empty segment starting at `base_offset`.
    pub fn create(
        dir: &Path,
        base_offset: Offset,
        created_at_ms: i64,
        index_interval_bytes: u32,
    ) -> Result<Self> {
        Self::create_with_options(
            dir,
            base_offset,
            created_at_ms,
            SegmentOptions {
                index_interval_bytes,
                ..SegmentOptions::default()
            },
        )
    }

    /// Create a new empty segment with full options (pool, direct I/O, backend).
    pub fn create_with_options(
        dir: &Path,
        base_offset: Offset,
        created_at_ms: i64,
        opts: SegmentOptions,
    ) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let log_path = segment_log_path(dir, base_offset);
        let index_path = segment_index_path(dir, base_offset);

        let mut open_opts = OpenOptions::new();
        open_opts.read(true).write(true).create_new(true);
        let used_direct = apply_direct_io_flag(&mut open_opts, opts.direct_io);
        let mut file = open_opts.open(&log_path).or_else(|e| {
            // O_DIRECT open can fail on some FS; fall back to normal open.
            if used_direct {
                tracing::warn!(
                    error = %e,
                    path = %log_path.display(),
                    "O_DIRECT open failed; falling back to buffered open"
                );
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&log_path)
            } else {
                Err(e)
            }
        })?;

        let header = encode_header(base_offset, created_at_ms);
        let effective_direct = used_direct && file_is_direct_candidate(used_direct);

        let (log_writer, log_file, physical_size, pending, backend) =
            if effective_direct {
                // Write header via backend with alignment padding.
                let mut backend = create_io_backend(opts.io_backend)?;
                let mut aligned = header.to_vec();
                let pad_to = align_up(HEADER_SIZE, DIRECT_IO_ALIGN);
                aligned.resize(pad_to, 0);
                backend.write_all_at(&file, 0, &aligned)?;
                // Logical size is HEADER_SIZE; physical is pad_to. Pending starts empty
                // but subsequent records append after logical size — we track pending
                // as data after the last aligned physical write.
                // Simpler model: physical file starts at pad_to with header+zeros;
                // logical size = HEADER_SIZE; pending holds bytes from HEADER_SIZE onward
                // that haven't been written as a full aligned chunk yet.
                // Re-model: keep pending as unflushed record bytes; physical always aligned.
                // Initial: write header padded; physical = pad_to; logical = HEADER_SIZE;
                // The zeros between HEADER_SIZE and pad_to are padding that recovery will
                // treat as incomplete if no records follow — but we need records to start
                // at HEADER_SIZE. So we must NOT pad the header alone on disk at physical
                // offset 0 with zeros after header inside the same block — records must
                // continue at byte HEADER_SIZE.
                //
                // Correct approach: accumulate header+records in pending, write aligned
                // chunks from offset 0. Initial pending = header.
                let _ = aligned; // rewritten below
                let _ = backend;
                // Reset file and use pending accumulation from the start.
                file.set_len(0)?;
                let backend = create_io_backend(opts.io_backend)?;
                (
                    None,
                    Some(file),
                    0u64,
                    header.to_vec(),
                    Some(backend),
                )
            } else {
                file.write_all(&header)?;
                file.flush()?;
                (
                    Some(std::io::BufWriter::new(file)),
                    None,
                    HEADER_SIZE as u64,
                    Vec::new(),
                    // Optional backend for fsync path even when buffered.
                    Some(create_io_backend(opts.io_backend)?),
                )
            };

        // If direct path, flush pending header if it already fills an aligned block
        // (unlikely for 32-byte header) — leave in pending.
        let mut seg = Self {
            dir: dir.to_path_buf(),
            log_path,
            index_path: index_path.clone(),
            base_offset,
            next_offset: base_offset,
            size: HEADER_SIZE as u64,
            last_timestamp_ms: created_at_ms,
            created_at_ms,
            index_interval_bytes: opts.index_interval_bytes,
            bytes_since_index: 0,
            index: SparseIndex::new(),
            log_writer,
            log_file,
            index_writer: {
                let index_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&index_path)?;
                Some(std::io::BufWriter::new(index_file))
            },
            mmap: None,
            use_mmap: opts.use_mmap,
            sealed: false,
            pool: opts.pool,
            direct_io: effective_direct,
            physical_size,
            pending,
            backend,
        };

        if seg.direct_io {
            seg.flush_pending_aligned(false)?;
        }

        Ok(seg)
    }

    /// Open an existing segment, recovering from a torn tail if needed.
    ///
    /// When `readonly` is true the segment is sealed (mmap for reads, no writers).
    pub fn open(
        dir: &Path,
        base_offset: Offset,
        index_interval_bytes: u32,
        use_mmap: bool,
    ) -> Result<Self> {
        Self::open_with_options(
            dir,
            base_offset,
            SegmentOptions {
                index_interval_bytes,
                use_mmap,
                ..SegmentOptions::default()
            },
            false,
        )
    }

    /// Open as a sealed (read-only) segment.
    pub fn open_sealed(
        dir: &Path,
        base_offset: Offset,
        index_interval_bytes: u32,
        use_mmap: bool,
    ) -> Result<Self> {
        Self::open_with_options(
            dir,
            base_offset,
            SegmentOptions {
                index_interval_bytes,
                use_mmap,
                ..SegmentOptions::default()
            },
            true,
        )
    }

    /// Open with full options.
    pub fn open_with_options(
        dir: &Path,
        base_offset: Offset,
        opts: SegmentOptions,
        sealed: bool,
    ) -> Result<Self> {
        let log_path = segment_log_path(dir, base_offset);
        let index_path = segment_index_path(dir, base_offset);

        // Recovery always uses a normal (non-O_DIRECT) open so short reads work.
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log_path)?;

        let mut header_buf = [0u8; HEADER_SIZE];
        file.read_exact(&mut header_buf)?;
        let header = decode_header(&header_buf)?;
        if header.base_offset != base_offset {
            return Err(Error::Storage(format!(
                "segment base_offset mismatch: file has {}, expected {}",
                header.base_offset, base_offset
            )));
        }

        // Read full file for recovery scan.
        file.seek(SeekFrom::Start(0))?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let (valid_end, next_offset, last_ts, index) =
            recover_and_index(&data, base_offset, opts.index_interval_bytes)?;

        // Truncate torn tail on log + rewrite index.
        if valid_end < data.len() {
            file.set_len(valid_end as u64)?;
            file.seek(SeekFrom::Start(valid_end as u64))?;
        } else {
            file.seek(SeekFrom::Start(valid_end as u64))?;
        }
        file.sync_all()?;
        index.write_to(&index_path)?;

        let bytes_since_index = compute_bytes_since_index(&data[..valid_end], &index);

        let want_direct = opts.direct_io && !sealed;
        let (log_writer, log_file, direct_io, physical_size, pending, backend) = if sealed {
            (None, None, false, valid_end as u64, Vec::new(), None)
        } else if want_direct {
            // Re-open with O_DIRECT for subsequent writes.
            drop(file);
            let mut open_opts = OpenOptions::new();
            open_opts.read(true).write(true);
            let used = apply_direct_io_flag(&mut open_opts, true);
            let file = match open_opts.open(&log_path) {
                Ok(f) => f,
                Err(e) if used => {
                    tracing::warn!(
                        error = %e,
                        "O_DIRECT re-open failed; using buffered writer"
                    );
                    let f = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&log_path)?;
                    let mut f = f;
                    f.seek(SeekFrom::Start(valid_end as u64))?;
                    return Ok(Self {
                        dir: dir.to_path_buf(),
                        log_path,
                        index_path: index_path.clone(),
                        base_offset,
                        next_offset,
                        size: valid_end as u64,
                        last_timestamp_ms: last_ts.unwrap_or(header.created_at_ms),
                        created_at_ms: header.created_at_ms,
                        index_interval_bytes: opts.index_interval_bytes,
                        bytes_since_index,
                        index,
                        log_writer: Some(std::io::BufWriter::new(f)),
                        log_file: None,
                        index_writer: {
                            let mut index_file = OpenOptions::new()
                                .read(true)
                                .write(true)
                                .open(&index_path)?;
                            index_file.seek(SeekFrom::End(0))?;
                            Some(std::io::BufWriter::new(index_file))
                        },
                        mmap: None,
                        use_mmap: opts.use_mmap,
                        sealed: false,
                        pool: opts.pool,
                        direct_io: false,
                        physical_size: valid_end as u64,
                        pending: Vec::new(),
                        backend: Some(create_io_backend(opts.io_backend)?),
                    });
                }
                Err(e) => return Err(Error::from(e)),
            };
            // Direct path: physical size may include padding; truncate to valid_end
            // then pad physical to alignment for subsequent aligned writes.
            let physical = valid_end as u64;
            (
                None,
                Some(file),
                used,
                physical,
                Vec::new(),
                Some(create_io_backend(opts.io_backend)?),
            )
        } else {
            (
                Some(std::io::BufWriter::new(file)),
                None,
                false,
                valid_end as u64,
                Vec::new(),
                Some(create_io_backend(opts.io_backend)?),
            )
        };

        let mmap = if sealed && opts.use_mmap {
            let f = File::open(&log_path)?;
            Some(unsafe { Mmap::map(&f)? })
        } else {
            None
        };

        let index_writer = if sealed {
            None
        } else {
            let mut index_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&index_path)?;
            index_file.seek(SeekFrom::End(0))?;
            Some(std::io::BufWriter::new(index_file))
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            log_path,
            index_path,
            base_offset,
            next_offset,
            size: valid_end as u64,
            last_timestamp_ms: last_ts.unwrap_or(header.created_at_ms),
            created_at_ms: header.created_at_ms,
            index_interval_bytes: opts.index_interval_bytes,
            bytes_since_index,
            index,
            log_writer,
            log_file,
            index_writer,
            mmap,
            use_mmap: opts.use_mmap,
            sealed,
            pool: opts.pool,
            direct_io,
            physical_size,
            pending,
            backend,
        })
    }

    /// Append a record at the given absolute `offset`.
    pub fn append(
        &mut self,
        offset: Offset,
        message: &Message,
        timestamp_ms: i64,
    ) -> Result<Record> {
        if self.sealed {
            return Err(Error::Storage("cannot append to sealed segment".into()));
        }
        if offset != self.next_offset {
            return Err(Error::Storage(format!(
                "append offset mismatch: got {}, expected {}",
                offset, self.next_offset
            )));
        }

        let need = encoded_record_size(message) as usize;
        let written = if let Some(pool) = self.pool.clone() {
            let mut pooled = pool.acquire();
            if pooled.capacity() < need {
                // Buffer too small: encode into a fresh vec (not returned if oversized).
                let mut buf = Vec::with_capacity(need);
                let n = encode_record(&mut buf, offset, timestamp_ms, message);
                self.write_encoded(&buf)?;
                n
            } else {
                let n = {
                    let v = pooled.as_mut_vec();
                    encode_record(v, offset, timestamp_ms, message)
                };
                self.write_encoded(pooled.as_slice())?;
                n
            }
        } else {
            let mut buf = Vec::with_capacity(need);
            let n = encode_record(&mut buf, offset, timestamp_ms, message);
            self.write_encoded(&buf)?;
            n
        };

        let position = self.size - written as u64;

        // Sparse index: first record, or every index_interval_bytes of payload.
        let should_index = self.index.is_empty()
            || self.bytes_since_index >= self.index_interval_bytes as u64;
        if should_index {
            let delta = offset
                .raw()
                .checked_sub(self.base_offset.raw())
                .ok_or_else(|| Error::Storage("offset before base".into()))?;
            if delta > u32::MAX as u64 {
                return Err(Error::Storage("offset_delta exceeds u32".into()));
            }
            if position > u32::MAX as u64 {
                return Err(Error::Storage("segment position exceeds u32".into()));
            }
            let entry = IndexEntry {
                offset_delta: delta as u32,
                position: position as u32,
            };
            self.index.push(entry);
            if let Some(iw) = self.index_writer.as_mut() {
                iw.write_all(&entry.encode())?;
            }
            self.bytes_since_index = 0;
        }
        self.bytes_since_index += written as u64;

        self.next_offset = offset.next();
        self.last_timestamp_ms = timestamp_ms;
        // Invalidate stale mmap after mutation.
        self.mmap = None;

        // Flush buffered path so concurrent &self reads observe data.
        if let Some(w) = self.log_writer.as_mut() {
            w.flush()?;
        }
        if let Some(w) = self.index_writer.as_mut() {
            w.flush()?;
        }

        Ok(Record {
            offset,
            key: message.key.clone(),
            value: message.value.clone(),
            timestamp_ms,
            headers: message.headers.clone(),
        })
    }

    /// Write already-encoded record bytes to the log (buffered or direct).
    fn write_encoded(&mut self, buf: &[u8]) -> Result<()> {
        if self.direct_io {
            self.pending.extend_from_slice(buf);
            self.size += buf.len() as u64;
            self.flush_pending_aligned(false)?;
        } else if let Some(writer) = self.log_writer.as_mut() {
            writer.write_all(buf)?;
            self.size += buf.len() as u64;
            self.physical_size = self.size;
        } else if let Some(file) = self.log_file.as_ref() {
            // Unbuffered backend path without O_DIRECT alignment constraints.
            let offset = self.size;
            let backend = self
                .backend
                .as_mut()
                .ok_or_else(|| Error::Storage("no I/O backend".into()))?;
            backend.write_all_at(file, offset, buf)?;
            self.size += buf.len() as u64;
            self.physical_size = self.size;
        } else {
            return Err(Error::Storage("segment has no writer".into()));
        }
        Ok(())
    }

    /// Flush pending direct-I/O bytes. When `force`, pad with zeros to alignment.
    fn flush_pending_aligned(&mut self, force: bool) -> Result<()> {
        if !self.direct_io {
            return Ok(());
        }
        let file = self
            .log_file
            .as_ref()
            .ok_or_else(|| Error::Storage("direct_io segment missing file".into()))?;
        let backend = self
            .backend
            .as_mut()
            .ok_or_else(|| Error::Storage("direct_io segment missing backend".into()))?;

        let align = DIRECT_IO_ALIGN;
        if force {
            if self.pending.is_empty() {
                return Ok(());
            }
            let pad_to = align_up(self.pending.len(), align);
            self.pending.resize(pad_to, 0);
        }

        // Write as many full aligned chunks as possible.
        let writable = (self.pending.len() / align) * align;
        if writable == 0 {
            return Ok(());
        }

        // Ensure write offset is aligned. physical_size should already be aligned.
        let offset = self.physical_size;
        if offset as usize % align != 0 {
            return Err(Error::Storage(format!(
                "direct_io physical offset {offset} not {align}-aligned"
            )));
        }

        let chunk = &self.pending[..writable];
        backend.write_all_at(file, offset, chunk)?;
        self.physical_size += writable as u64;
        self.pending.drain(..writable);
        Ok(())
    }

    /// Read records with `offset >= from`, up to message and byte limits.
    pub fn read_from(
        &self,
        from: Offset,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        if max_messages == 0 {
            return Ok(Vec::new());
        }
        if from.raw() >= self.next_offset.raw() {
            return Ok(Vec::new());
        }
        if self.next_offset == self.base_offset {
            return Ok(Vec::new());
        }

        let data = self.load_data()?;
        if data.len() < HEADER_SIZE {
            return Err(Error::Storage("segment data shorter than header".into()));
        }

        let start_pos = if from.raw() <= self.base_offset.raw() {
            HEADER_SIZE
        } else {
            let delta = (from.raw() - self.base_offset.raw()) as u32;
            match self.index.lookup(delta) {
                Some(pos) => pos as usize,
                None => HEADER_SIZE,
            }
        };

        let mut pos = start_pos;
        let mut out = Vec::new();
        let mut bytes_acc = 0usize;

        // Only decode up to logical size (exclude O_DIRECT padding).
        let logical_end = self.size.min(data.len() as u64) as usize;

        while pos < logical_end && out.len() < max_messages {
            match decode_record_at(&data[..logical_end], pos) {
                DecodeStatus::Ok {
                    record,
                    next_pos,
                    size,
                    ..
                } => {
                    if record.offset.raw() < from.raw() {
                        pos = next_pos;
                        continue;
                    }
                    if record.offset.raw() >= self.next_offset.raw() {
                        break;
                    }
                    // Respect max_bytes but always allow the first message.
                    if !out.is_empty()
                        && max_bytes != usize::MAX
                        && bytes_acc.saturating_add(size) > max_bytes
                    {
                        break;
                    }
                    bytes_acc = bytes_acc.saturating_add(size);
                    pos = next_pos;
                    out.push(record);
                }
                DecodeStatus::Incomplete | DecodeStatus::Corrupt => break,
            }
        }
        Ok(out)
    }

    /// Flush buffered data and fsync log + index.
    pub fn flush(&mut self) -> Result<()> {
        if self.direct_io {
            self.flush_pending_aligned(true)?;
            if let (Some(file), Some(backend)) = (self.log_file.as_ref(), self.backend.as_mut()) {
                backend.fsync(file)?;
            }
            // After padding flush, truncate logical tail zeros? Keep physical pad so
            // next append can continue — but logical size excludes pad. Next write
            // must not overwrite records: pending may be empty; physical includes pad
            // past logical size. Subsequent records go into pending; when written,
            // they must start at logical size which may be inside the last padded
            // block — so we need to re-read that block into pending or rewrite.
            //
            // Simpler fix after force pad: set physical back by re-opening strategy —
            // keep padding only as durable image of pending; after fsync with pad,
            // put the pad region back as "already written" and clear pending.
            // Logical size is correct. Next append extends pending; when we write,
            // offset = physical_size which is past the pad — creating a hole/gap
            // in the file at logical_size..physical_size filled with zeros, and new
            // data at physical_size. Recovery would stop at zeros after last record
            // — CORRECT (zeros after last record = end). But then new data after
            // pad would be unreachable because recovery stops at zeros!
            //
            // Critical: after force-pad flush we must NOT leave zero gaps between
            // records. Force pad is only safe on seal (no more appends) OR we must
            // rewrite the last partial block on next append.
            //
            // Implement rewrite: after force pad, remember pad_start = logical size
            // within the last block. On next append, if physical > logical, seek
            // rewrite from floor(logical) aligned offset: load is not needed if we
            // keep the unpadded tail in a side buffer.
            //
            // Better approach for force flush:
            // - Write aligned pad
            // - Keep `stale_pad = physical_size - size` (bytes of zero pad after logical)
            // - On next write_encoded, prepend to pending is wrong; instead reduce
            //   physical_size back to align_down(size) and re-populate pending from
            //   the partial block that was written with padding.
            //
            // Easiest correct approach: on force flush, after writing padded data,
            // set physical_size = align_down(size) and restore pending to the
            // unpadded tail that lives in the last partial block (without zeros).
            let logical = self.size;
            let align = DIRECT_IO_ALIGN as u64;
            let aligned_logical_floor = (logical / align) * align;
            // The data from aligned_logical_floor..logical was written with zero pad
            // up to physical_size. Restore it as pending and rewind physical.
            if self.physical_size > aligned_logical_floor {
                // We don't have the tail bytes in memory (they were drained). For force
                // flush after append, pending was padded in place then drained fully.
                // Rebuild pending by reading from file — expensive but rare (explicit flush).
                if logical > aligned_logical_floor {
                    let mut tail = vec![0u8; (logical - aligned_logical_floor) as usize];
                    read_at_exact(
                        self.log_file.as_ref().unwrap(),
                        aligned_logical_floor,
                        &mut tail,
                    )?;
                    self.pending = tail;
                } else {
                    self.pending.clear();
                }
                self.physical_size = aligned_logical_floor;
            }
        } else if let Some(w) = self.log_writer.as_mut() {
            w.flush()?;
            if let Some(backend) = self.backend.as_mut() {
                backend.fsync(w.get_ref())?;
            } else {
                w.get_ref().sync_all()?;
            }
        }
        if let Some(w) = self.index_writer.as_mut() {
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Base offset of the first record in this segment.
    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    /// Next offset to be assigned in this segment.
    pub fn next_offset(&self) -> Offset {
        self.next_offset
    }

    /// Current `.log` file logical size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Timestamp of the last appended record (or creation time if empty).
    pub fn last_timestamp_ms(&self) -> i64 {
        self.last_timestamp_ms
    }

    /// Segment creation timestamp.
    pub fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    /// Directory containing this segment's files.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Path to the `.log` file.
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    /// Path to the `.index` file.
    pub fn index_path(&self) -> &Path {
        &self.index_path
    }

    /// Whether this segment is sealed (read-only).
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Whether this segment is using the direct-I/O write path.
    pub fn is_direct_io(&self) -> bool {
        self.direct_io
    }

    /// Configure whether mmap is used when sealing / opening for reads.
    pub fn set_use_mmap(&mut self, use_mmap: bool) {
        self.use_mmap = use_mmap;
    }

    /// Attach (or replace) the encode buffer pool.
    pub fn set_pool(&mut self, pool: Option<Arc<BufferPool>>) {
        self.pool = pool;
    }

    /// Seal the segment: flush, drop writers, optionally mmap for reads.
    pub fn seal(&mut self) -> Result<()> {
        if self.sealed {
            return Ok(());
        }
        self.flush()?;
        // On seal with direct_io, permanently pad and leave physical >= logical.
        if self.direct_io {
            // Final pad write without restoring pending (no more appends).
            if !self.pending.is_empty() {
                let align = DIRECT_IO_ALIGN;
                let pad_to = align_up(self.pending.len(), align);
                self.pending.resize(pad_to, 0);
                let file = self.log_file.as_ref().unwrap();
                let backend = self.backend.as_mut().unwrap();
                let chunk = self.pending.clone();
                backend.write_all_at(file, self.physical_size, &chunk)?;
                self.physical_size += chunk.len() as u64;
                self.pending.clear();
                backend.fsync(file)?;
            }
            // Truncate file to logical size so mmap/recovery sees clean data
            // without zero padding (recovery would stop at zeros anyway, but
            // truncating keeps size() consistent with on-disk).
            if let Some(file) = self.log_file.as_ref() {
                file.set_len(self.size)?;
                file.sync_all()?;
            }
        }
        self.log_writer = None;
        self.log_file = None;
        self.index_writer = None;
        self.backend = None;
        if self.use_mmap {
            let f = File::open(&self.log_path)?;
            self.mmap = Some(unsafe { Mmap::map(&f)? });
        }
        self.sealed = true;
        Ok(())
    }

    /// Ensure writers are flushed so on-disk bytes are complete for reading.
    fn prepare_read(&self) -> Result<()> {
        Ok(())
    }

    /// Load segment bytes for reading (mmap copy, pending merge, or full file read).
    fn load_data(&self) -> Result<Vec<u8>> {
        self.prepare_read()?;
        if let Some(mmap) = &self.mmap {
            return Ok(mmap[..].to_vec());
        }
        if self.direct_io {
            // Physical file + in-memory pending tail.
            let mut data = Vec::with_capacity(self.size as usize);
            if self.physical_size > 0 {
                let mut file = File::open(&self.log_path)?;
                let mut disk = vec![0u8; self.physical_size as usize];
                file.read_exact(&mut disk)?;
                data.extend_from_slice(&disk);
            }
            data.extend_from_slice(&self.pending);
            // Truncate to logical size (exclude any historical pad if present).
            if data.len() > self.size as usize {
                data.truncate(self.size as usize);
            }
            return Ok(data);
        }
        // Active or non-mmap path: read file from disk.
        let mut file = File::open(&self.log_path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Ok(data)
    }

    /// Delete this segment's files from disk.
    pub fn delete_files(&self) -> Result<()> {
        if self.log_path.exists() {
            fs::remove_file(&self.log_path)?;
        }
        if self.index_path.exists() {
            fs::remove_file(&self.index_path)?;
        }
        Ok(())
    }
}

fn file_is_direct_candidate(used_direct_flag: bool) -> bool {
    used_direct_flag
}

/// Read `buf.len()` bytes from `file` at absolute `offset`.
fn read_at_exact(file: &File, offset: u64, buf: &mut [u8]) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset).map_err(Error::from)
    }
    #[cfg(not(unix))]
    {
        let mut f = file.try_clone().map_err(Error::from)?;
        f.seek(SeekFrom::Start(offset)).map_err(Error::from)?;
        f.read_exact(buf).map_err(Error::from)
    }
}

/// Build `{base_offset:020}.log` path.
pub fn segment_log_path(dir: &Path, base_offset: Offset) -> PathBuf {
    dir.join(format!("{:020}.log", base_offset.raw()))
}

/// Build `{base_offset:020}.index` path.
pub fn segment_index_path(dir: &Path, base_offset: Offset) -> PathBuf {
    dir.join(format!("{:020}.index", base_offset.raw()))
}

/// Scan directory for segment base offsets (from `.log` filenames).
pub fn list_segment_bases(dir: &Path) -> Result<Vec<Offset>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut bases = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(stem) = name.strip_suffix(".log") {
            if stem.len() == 20 && stem.chars().all(|c| c.is_ascii_digit()) {
                if let Ok(v) = stem.parse::<u64>() {
                    bases.push(Offset::new(v));
                }
            }
        }
    }
    bases.sort();
    Ok(bases)
}

/// Recover valid prefix of segment data and rebuild sparse index.
fn recover_and_index(
    data: &[u8],
    base_offset: Offset,
    index_interval_bytes: u32,
) -> Result<(usize, Offset, Option<i64>, SparseIndex)> {
    if data.len() < HEADER_SIZE {
        return Err(Error::Storage("segment file truncated in header".into()));
    }
    let mut pos = HEADER_SIZE;
    let mut next_offset = base_offset;
    let mut last_ts = None;
    let mut index = SparseIndex::new();
    let mut bytes_since_index = 0u64;

    while pos < data.len() {
        match decode_record_at(data, pos) {
            DecodeStatus::Ok {
                record,
                next_pos,
                size,
                position,
                ..
            } => {
                if record.offset != next_offset {
                    // Unexpected gap/mismatch — treat as corruption at this point.
                    break;
                }
                let should_index =
                    index.is_empty() || bytes_since_index >= index_interval_bytes as u64;
                if should_index {
                    let delta = record.offset.raw() - base_offset.raw();
                    if delta <= u32::MAX as u64 && position <= u32::MAX as u64 {
                        index.push(IndexEntry {
                            offset_delta: delta as u32,
                            position: position as u32,
                        });
                    }
                    bytes_since_index = 0;
                }
                bytes_since_index += size as u64;
                last_ts = Some(record.timestamp_ms);
                next_offset = record.offset.next();
                pos = next_pos;
            }
            DecodeStatus::Incomplete | DecodeStatus::Corrupt => break,
        }
    }
    Ok((pos, next_offset, last_ts, index))
}

fn compute_bytes_since_index(data: &[u8], index: &SparseIndex) -> u64 {
    if data.len() <= HEADER_SIZE {
        return 0;
    }
    let last_pos = index
        .entries()
        .last()
        .map(|e| e.position as usize)
        .unwrap_or(HEADER_SIZE);
    if data.len() > last_pos {
        (data.len() - last_pos) as u64
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    use volant_core::Message;

    fn tmp() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "volant-seg-{}-{}-{}",
            std::process::id(),
            n,
            nanos
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn create_append_read() {
        let dir = tmp();
        let mut seg = Segment::create(&dir, Offset::ZERO, 1000, 4096).unwrap();
        let msg = Message::from_value("hello");
        let rec = seg.append(Offset::ZERO, &msg, 1000).unwrap();
        assert_eq!(rec.offset.raw(), 0);
        seg.flush().unwrap();
        let got = seg.read_from(Offset::ZERO, 10, usize::MAX).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].value.as_ref(), b"hello");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_with_buffer_pool() {
        let dir = tmp();
        let pool = Arc::new(BufferPool::with_capacity(4, 64 * 1024));
        let mut seg = Segment::create_with_options(
            &dir,
            Offset::ZERO,
            1000,
            SegmentOptions {
                index_interval_bytes: 4096,
                pool: Some(Arc::clone(&pool)),
                ..SegmentOptions::default()
            },
        )
        .unwrap();
        for i in 0..20 {
            let msg = Message::from_value(format!("pooled-{i}"));
            seg.append(Offset::new(i), &msg, 1000 + i as i64)
                .unwrap();
        }
        seg.flush().unwrap();
        let got = seg.read_from(Offset::ZERO, 100, usize::MAX).unwrap();
        assert_eq!(got.len(), 20);
        assert_eq!(got[19].value.as_ref(), b"pooled-19");
        // Pool should have free buffers again.
        assert!(pool.free_count() >= 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_tail_on_open() {
        let dir = tmp();
        let mut seg = Segment::create(&dir, Offset::ZERO, 1000, 4096).unwrap();
        seg.append(Offset::ZERO, &Message::from_value("one"), 1)
            .unwrap();
        seg.append(Offset::new(1), &Message::from_value("two"), 2)
            .unwrap();
        seg.flush().unwrap();
        let path = seg.log_path().to_path_buf();
        let size = seg.size();
        drop(seg);

        // Truncate last few bytes of the second record.
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(size - 3).unwrap();
        drop(file);

        let seg = Segment::open(&dir, Offset::ZERO, 4096, true).unwrap();
        assert_eq!(seg.next_offset(), Offset::new(1));
        let got = seg.read_from(Offset::ZERO, 10, usize::MAX).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].value.as_ref(), b"one");
        let _ = fs::remove_dir_all(&dir);
    }
}
