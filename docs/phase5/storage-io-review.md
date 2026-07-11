# Phase 5 Storage I/O — Review

## Iteration 1

### Checklist

- [x] `pool.rs` — `BufferPool` + `PooledBuf` (return on drop)
  - Free list of fixed-capacity `Vec<u8>`; soft cap on free-list growth
  - Oversized buffers (capacity > 2× block_size) discarded on drop
  - Unit tests: acquire/release, churn, concurrent churn, `into_vec`, zero block size
- [x] `io.rs` — `IoBackend` trait, `StdIoBackend`, optional `UringIoBackend`
  - `write_all_at` via `FileExt::write_at` (unix) / seek+write (other)
  - `fsync` via `File::sync_all` or io_uring `Fsync` SQE
  - Uring path: sync submit+wait (no async runtime in storage)
- [x] Cargo features: `io-uring`, `direct-io` (default = none)
  - `io-uring` dep is Linux-only (`target_os = "linux"`); non-Linux + feature → clear `compile_error!`
  - `direct-io` compiles everywhere; O_DIRECT flag applied only on Linux
- [x] `StorageConfig` fields: `io_backend`, `direct_io`, `buffer_pool_blocks`, `buffer_pool_block_size`
- [x] Segment append uses pool for encode scratch when pool enabled
  - `PartitionLog::open` builds `Arc<BufferPool>` when `buffer_pool_blocks > 0`
  - `Segment::append` acquires `PooledBuf`, encodes, writes, returns on drop
- [x] O_DIRECT open flags when feature+config (Linux); safe fallback otherwise
  - `apply_direct_io_flag` + open fallback if O_DIRECT fails
  - Aligned pending buffer + pad-on-flush for direct path
  - Sealed segments keep mmap reads
- [x] Unit tests for pool; existing durable tests still pass
- [x] Default macOS build green

### Findings

1. **None blocking.** Default path (BufWriter, no pool) unchanged in behavior.
2. **Note:** Direct-I/O path is a Phase 5 hook; full production hardening of partial-block
   rewrite after force-flush is implemented but only active when `direct_io` + feature + Linux.
3. **Note:** `IoBackendKind::IoUring` without the feature logs a warning and falls back to Std
   (except when the feature is forced on non-Linux, which is a hard compile error).
4. **Note:** Workspace call sites already use `..StorageConfig::default()`, so new fields
   did not require edits outside `volant-storage` (durable_log helper updated with `..default()`).

### Test results (iteration 1)

```
cargo test -p volant-storage
# unit: 23 passed
# integration durable_log: 6 passed
# total: 29 passed; 0 failed

cargo test -p volant-storage --features direct-io
# 29 passed (macOS; O_DIRECT is no-op / fallback)

cargo check -p volant-storage --features io-uring
# error: feature "io-uring" is only supported on Linux (target_os = "linux")
# (expected on macOS)
```

### Decision

No code fixes required after review. Done in 1 iteration.

## Iteration log

| Iter | Action | Result |
|------|--------|--------|
| 1 | Plan → implement pool/io/config/segment/log → test | Green (29 tests); io-uring compile_error on macOS; direct-io feature green |
| — | (compile fix during impl: ambiguous integer in pool concurrent test; unused imports in io.rs) | Fixed before review tests |
