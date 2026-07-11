//! Log segment: append-only `.log` + sparse `.index` pair.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use volant_core::{Error, Message, Offset, Record, Result};

use crate::index::{IndexEntry, SparseIndex};
use crate::record::{
    decode_header, decode_record_at, encode_header, encode_record, encoded_record_size,
    DecodeStatus, HEADER_SIZE,
};

/// An appendable (or sealed) log segment on disk.
pub struct Segment {
    dir: PathBuf,
    log_path: PathBuf,
    index_path: PathBuf,
    base_offset: Offset,
    next_offset: Offset,
    /// Current size of the `.log` file in bytes (including header).
    size: u64,
    last_timestamp_ms: i64,
    created_at_ms: i64,
    index_interval_bytes: u32,
    /// Bytes of payload written since the last index entry.
    bytes_since_index: u64,
    index: SparseIndex,
    /// Buffered writer for the active (unsealed) segment log file.
    log_writer: Option<std::io::BufWriter<File>>,
    /// Buffered writer for the active segment index file.
    index_writer: Option<std::io::BufWriter<File>>,
    /// Memory map of the log file (sealed segments, or refreshed for reads).
    mmap: Option<Mmap>,
    use_mmap: bool,
    sealed: bool,
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment")
            .field("log_path", &self.log_path)
            .field("base_offset", &self.base_offset)
            .field("next_offset", &self.next_offset)
            .field("size", &self.size)
            .field("sealed", &self.sealed)
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
        fs::create_dir_all(dir)?;
        let log_path = segment_log_path(dir, base_offset);
        let index_path = segment_index_path(dir, base_offset);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&log_path)?;
        let header = encode_header(base_offset, created_at_ms);
        file.write_all(&header)?;
        file.flush()?;

        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&index_path)?;

        Ok(Self {
            dir: dir.to_path_buf(),
            log_path,
            index_path,
            base_offset,
            next_offset: base_offset,
            size: HEADER_SIZE as u64,
            last_timestamp_ms: created_at_ms,
            created_at_ms,
            index_interval_bytes,
            bytes_since_index: 0,
            index: SparseIndex::new(),
            log_writer: Some(std::io::BufWriter::new(file)),
            index_writer: Some(std::io::BufWriter::new(index_file)),
            mmap: None,
            use_mmap: true,
            sealed: false,
        })
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
        Self::open_internal(dir, base_offset, index_interval_bytes, use_mmap, false)
    }

    /// Open as a sealed (read-only) segment.
    pub fn open_sealed(
        dir: &Path,
        base_offset: Offset,
        index_interval_bytes: u32,
        use_mmap: bool,
    ) -> Result<Self> {
        Self::open_internal(dir, base_offset, index_interval_bytes, use_mmap, true)
    }

    fn open_internal(
        dir: &Path,
        base_offset: Offset,
        index_interval_bytes: u32,
        use_mmap: bool,
        sealed: bool,
    ) -> Result<Self> {
        let log_path = segment_log_path(dir, base_offset);
        let index_path = segment_index_path(dir, base_offset);

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
            recover_and_index(&data, base_offset, index_interval_bytes)?;

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

        let (log_writer, index_writer, mmap) = if sealed {
            let mmap = if use_mmap {
                // Re-open cleanly for mmap after truncation.
                let f = File::open(&log_path)?;
                Some(unsafe { Mmap::map(&f)? })
            } else {
                None
            };
            (None, None, mmap)
        } else {
            let index_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&index_path)?;
            // Position index writer at end.
            let mut index_file = index_file;
            index_file.seek(SeekFrom::End(0))?;
            (
                Some(std::io::BufWriter::new(file)),
                Some(std::io::BufWriter::new(index_file)),
                None,
            )
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
            index_interval_bytes,
            bytes_since_index,
            index,
            log_writer,
            index_writer,
            mmap,
            use_mmap,
            sealed,
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

        let mut buf = Vec::with_capacity(encoded_record_size(message) as usize);
        let written = encode_record(&mut buf, offset, timestamp_ms, message);
        let position = self.size;

        {
            let writer = self
                .log_writer
                .as_mut()
                .ok_or_else(|| Error::Storage("segment has no writer".into()))?;
            writer.write_all(&buf)?;
        }

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

        self.size += written as u64;
        self.next_offset = offset.next();
        self.last_timestamp_ms = timestamp_ms;
        // Invalidate stale mmap after mutation.
        self.mmap = None;

        // Flush to OS (not necessarily fsync) so concurrent &self reads observe data.
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

        while pos < data.len() && out.len() < max_messages {
            match decode_record_at(&data, pos) {
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
        if let Some(w) = self.log_writer.as_mut() {
            w.flush()?;
            w.get_ref().sync_all()?;
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

    /// Current `.log` file size in bytes.
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

    /// Configure whether mmap is used when sealing / opening for reads.
    pub fn set_use_mmap(&mut self, use_mmap: bool) {
        self.use_mmap = use_mmap;
    }

    /// Seal the segment: flush, drop writers, optionally mmap for reads.
    pub fn seal(&mut self) -> Result<()> {
        if self.sealed {
            return Ok(());
        }
        self.flush()?;
        self.log_writer = None;
        self.index_writer = None;
        if self.use_mmap {
            let f = File::open(&self.log_path)?;
            self.mmap = Some(unsafe { Mmap::map(&f)? });
        }
        self.sealed = true;
        Ok(())
    }

    /// Ensure writers are flushed so on-disk bytes are complete for reading.
    fn prepare_read(&self) -> Result<()> {
        // Flush via interior mutability is not available; callers that hold &mut
        // should flush first. For &self reads on the active segment, PartitionLog
        // flushes before calling read_from. As a safety net, try reading what is
        // already on disk (BufWriter may have unflushed data — PartitionLog handles this).
        Ok(())
    }

    /// Load segment bytes for reading (mmap copy or full file read).
    fn load_data(&self) -> Result<Vec<u8>> {
        self.prepare_read()?;
        if let Some(mmap) = &self.mmap {
            // Phase 1: copy mmap data into owned buffer (safe).
            return Ok(mmap[..].to_vec());
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
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("volant-seg-{nanos}"));
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
