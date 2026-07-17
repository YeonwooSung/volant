# Phase 5 Storage I/O — Plan

## Scope

Implement buffer pool, `IoBackend`, feature-gated `io-uring` / `O_DIRECT` hooks in
`crates/volant-storage` only. Default macOS build must stay green.

## Files

| Path | Action |
|------|--------|
| `docs/phase5/storage-io-plan.md` | Create (this file) |
| `docs/phase5/storage-io-review.md` | Create after implementation |
| `crates/volant-storage/Cargo.toml` | Features `io-uring`, `direct-io`; optional `io-uring` dep |
| `crates/volant-storage/src/pool.rs` | `BufferPool` + `PooledBuf` (return on drop) |
| `crates/volant-storage/src/io.rs` | `IoBackend` trait, `StdIoBackend`, optional `UringIoBackend` |
| `crates/volant-storage/src/config.rs` | `IoBackendKind` + pool / direct_io / io_backend fields |
| `crates/volant-storage/src/segment.rs` | Pool encode scratch; O_DIRECT open; backend write/fsync hooks |
| `crates/volant-storage/src/log.rs` | Create pool from config; pass to segments |
| `crates/volant-storage/src/lib.rs` | Module exports |
| `crates/volant-storage/tests/durable_log.rs` | Update `StorageConfig` struct literals |

## Design

### Features

```toml
[features]
default = []
io-uring = ["dep:io-uring"]
direct-io = []
```

- `io-uring`: Linux-only real backend; non-Linux → `compile_error!` when feature forced.
- `direct-io`: enables O_DIRECT open-flag path on Linux; no-op / ignore elsewhere.

### Buffer pool

- Free list of `Vec<u8>` with capacity = power-of-two `block_size`.
- `acquire()` → `PooledBuf`; `Drop` returns buffer to pool (cleared).
- `buffer_pool_blocks == 0` → pool disabled; append falls back to stack `Vec`.
- Default block size: 64 KiB.

### IoBackend

```rust
pub trait IoBackend: Send {
    fn write_all_at(&mut self, file: &File, offset: u64, buf: &[u8]) -> Result<()>;
    fn fsync(&mut self, file: &File) -> Result<()>;
}
```

- `StdIoBackend`: `write_at` (unix) / seek+write (other); `sync_all`.
- `UringIoBackend` (`cfg(all(feature="io-uring", target_os="linux"))`): sync submit+wait
  for `Write` + `Fsync` SQEs.
- `create_io_backend(kind)` factory: IoUring falls back to Std when feature off.

### Config

```rust
pub enum IoBackendKind { Std, IoUring }
// + io_backend, direct_io, buffer_pool_blocks, buffer_pool_block_size
```

### Segment hooks

1. **Pool**: `append` acquires pooled encode scratch when `pool` is `Some`.
2. **O_DIRECT**: on Linux + `direct-io` feature + `config.direct_io`, open log with
   `O_DIRECT` via `OpenOptionsExt::custom_flags`. Else normal open (safe fallback).
3. **Backend**: active segment can write via `IoBackend::write_all_at` when not using
   the buffered path; flush uses `IoBackend::fsync` when a backend is present.
4. Sealed segments keep mmap reads (unchanged).

### Direct-I/O write strategy

O_DIRECT requires 4 KiB alignment. When `direct_io` is active:

- Use unbuffered `File` (no `BufWriter`) + accumulate in an aligned pending buffer.
- Flush full 4 KiB multiples via backend; on `flush`/`seal`, pad with zeros to
  alignment (recovery already treats zero/torn tail as incomplete).
- Track logical `size` separately from physical file length.

When `direct_io` is false (default): keep existing `BufWriter` path; pool only for encode.

### Tests

1. Unit: pool acquire/release under churn; no capacity leak.
2. Existing durable_log + unit tests still pass (default features).
3. Optional `#[cfg(feature = "direct-io")]` smoke only where supported.

## Constraints

- Do not break macOS default build.
- Prefer sync uring submit+wait (no async runtime in storage).
- Ownership: `crates/volant-storage/**` + these phase5 docs only.

## Iteration budget

Max 3 plan→code→review→test→fix loops.
