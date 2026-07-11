# Phase 1 — Durable Log Spec (binding for implementers)

## Directory layout

```
{data_dir}/
  00000000000000000000.log      # segment data, name = base_offset zero-padded 20 digits
  00000000000000000000.index    # sparse index companion
  00000000000000000100.log
  00000000000000000100.index
```

## Segment file (`.log`)

### Header (32 bytes, little-endian)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | magic `u32` = `0x564C4E54` ("VLNT") |
| 4 | 2 | version `u16` = 1 |
| 6 | 2 | flags `u16` = 0 |
| 8 | 8 | base_offset `u64` |
| 16 | 8 | created_at_ms `i64` (unix ms) |
| 24 | 8 | reserved (0) |

### Record entry (after header)

All multi-byte integers little-endian.

```
record_length: u32   # bytes following this field (not including itself)
crc32:         u32   # CRC32 of all fields after crc32
offset:        u64
timestamp_ms:  i64
key_len:       u32   # u32::MAX means null key
key:           [u8; key_len]  # omitted if null
value_len:     u32
value:         [u8; value_len]
header_count:  u32
headers:       repeated (name_len: u16, name: [u8], value_len: u32, value: [u8])
```

`record_length` = size of (crc32 … end of headers).

On recovery: if trailing bytes are shorter than a full record or CRC fails, truncate file to last good record end.

## Sparse index (`.index`)

Binary entries, 16 bytes each, little-endian:

| offset_delta: u32 | position: u32 |

- `offset_delta` = `record.offset - base_offset`
- `position` = byte offset in `.log` file where `record_length` begins
- Write an index entry every `index_interval_bytes` of payload (config, default 4096)

## Public API (volant-storage)

```rust
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub segment_size: u64,           // default 256 MiB
    pub use_mmap: bool,              // default true
    pub flush_every_n: u64,          // 0 = only explicit flush; N = fsync every N appends
    pub index_interval_bytes: u32,   // default 4096
    pub retention_ms: Option<u64>,   // None = no time retention
    pub retention_bytes: Option<u64>,// None = no size retention
}

impl PartitionLog {
    pub fn open(config: StorageConfig) -> Result<Self>;
    pub fn append(&mut self, message: Message) -> Result<Record>;
    pub fn read(&self, from: Offset, max_messages: usize) -> Result<Vec<Record>>;
    pub fn read_bytes(&self, from: Offset, max_messages: usize, max_bytes: usize) -> Result<Vec<Record>>;
    pub fn high_watermark(&self) -> Offset;
    pub fn log_start_offset(&self) -> Offset;
    pub fn flush(&mut self) -> Result<()>;
    pub fn delete_records(&mut self, before_offset: Offset) -> Result<Offset>; // new start
    pub fn apply_retention(&mut self) -> Result<()>;
}
```

### Semantics

- `append`: assign next offset; if `message.timestamp_ms` is None, use current unix ms.
- Segment roll when active segment size (including header) would exceed `segment_size` after append (always allow at least one record per segment).
- `read` / `read_bytes`: return records with `offset >= from`, in order, up to limits. Empty vec if `from >= high_watermark`. If `from < log_start_offset`, clamp to `log_start_offset`.
- `flush`: fsync active segment data + index.
- `delete_records(before)`: drop whole segments whose last offset < before; segment granularity. Return new log start offset.
- `apply_retention`: delete oldest whole segments until total size is under `retention_bytes` (if set) and/or every remaining segment is newer than `retention_ms` (if set).

### Segment API

```rust
impl Segment {
    pub fn create(dir: &Path, base_offset: Offset, created_at_ms: i64, index_interval_bytes: u32) -> Result<Self>;
    pub fn open(dir: &Path, base_offset: Offset, index_interval_bytes: u32, use_mmap: bool) -> Result<Self>;
    pub fn append(&mut self, offset: Offset, message: &Message, timestamp_ms: i64) -> Result<Record>;
    pub fn read_from(&self, from: Offset, max_messages: usize, max_bytes: usize) -> Result<Vec<Record>>;
    pub fn flush(&mut self) -> Result<()>;
    pub fn base_offset(&self) -> Offset;
    pub fn next_offset(&self) -> Offset;
    pub fn size(&self) -> u64;
    pub fn last_timestamp_ms(&self) -> i64;
    pub fn seal(&mut self) -> Result<()>; // readonly for roll
}
```

## Implementation notes

- Prefer safe Rust; copy mmap bytes into `Bytes` for Phase 1.
- Use `std::fs::OpenOptions`, `std::io::Write`, and `memmap2::Mmap` for sealed/read segments. The active segment may use `File` + `BufWriter` and re-map (or read from the file) when serving reads.
- Active segment strategy: keep `File` open for append; on read of the active segment, either flush buffers and read from file/mmap, or maintain an in-memory cache of positions. Simplest path: `BufWriter` + on read flush to the OS, then read via mmap or `pread`.

## Tests required

1. append + read roundtrip
2. multi-segment roll (tiny `segment_size`)
3. reopen recovery preserves data
4. torn-tail recovery
5. `delete_records` / retention
6. broker produce + fetch after durable append

## Micro-benchmark

Single-partition append throughput is measured by the `volant-bench` workspace binary
(≥100k messages, ~100-byte values). Phase 1 exit target: ≥ 200k msgs/s on a laptop.

```bash
cargo run -p volant-bench --release
```

## Non-goals

No replication, no concurrent writers on the same partition, no consumer groups.
