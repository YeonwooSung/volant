# Phase 5 — DMA & High-Performance I/O (binding)

## Goals

Minimize copies and improve sequential I/O efficiency:

1. Feature-gated **Linux `io_uring`** append path (optional)
2. Feature-gated **O_DIRECT** aligned write path (optional)
3. **Batch produce coalescing** in the broker
4. **Buffer pool / slab** for encode buffers and headers
5. Optional **thread-per-core / CPU affinity** hooks (feature-gated)
6. Expanded **`volant-bench`** + **tuning guide**

**DMA** here = fewer user↔kernel copies and device-friendly I/O — not a custom driver.

## Non-goals (research only)

- DPDK / AF_XDP (document in `docs/tuning.md` as future research)
- Replacing Tokio network stack entirely

## Feature flags

### `volant-storage`

| Feature | Default | Platforms | Effect |
|---------|---------|-----------|--------|
| `mmap` (implicit) | on | all | existing mmap reads |
| `io-uring` | off | Linux only | `tokio-uring` or `io-uring` crate for async/sync append batch submit |
| `direct-io` | off | Linux/Unix | open segments with `O_DIRECT`; aligned buffers from pool |

On non-Linux, enabling `io-uring` must **fail at compile** with clear `compile_error!` or be a no-op stub documented as unsupported.

**Default `cargo build --workspace` must succeed on macOS** without extra features.

### `volant-broker`

| Feature | Default | Effect |
|---------|---------|--------|
| `batch-coalesce` | on (or always-on code) | coalesce produce batch already; improve multi-message encode path |
| `thread-per-core` | off | pin accept/worker threads via `core_affinity` or `nix` |

### `volant-bench`

CLI modes:

```
volant-bench append [--count N] [--value-size B] [--flush-every N] [--direct-io] [--io-uring]
volant-bench fetch  [--count N]  # sequential read throughput
volant-bench produce-batch [--batch-size B]  # in-process broker batch produce
```

Print msgs/s and MB/s. Document release runs.

## Storage design

### Buffer pool (`pool.rs`)

```rust
pub struct BufferPool {
  // free list of BytesMut or Vec<u8> with capacity power-of-two
}
impl BufferPool {
  pub fn with_capacity(blocks: usize, block_size: usize) -> Self;
  pub fn acquire(&self) -> PooledBuf; // returns to pool on drop
}
```

Use pool for record encode scratch buffers when available; fallback to alloc.

### O_DIRECT path (`direct-io` feature)

- Align writes to 4 KiB (or `libc::statvfs` / 4096)
- Preallocate aligned buffers from pool
- File open flags: `O_DIRECT` on Linux
- Read path may still use mmap for sealed segments **or** aligned pread — document choice: **sealed segments keep mmap; active segment direct writes**

### io_uring path (`io-uring` feature, Linux)

- Provide `IoBackend` trait:

```rust
pub trait IoBackend: Send + Sync {
  fn write_all_at(&mut self, file: &File, offset: u64, buf: &[u8]) -> Result<()>;
  fn fsync(&mut self, file: &File) -> Result<()>;
}
pub struct StdIoBackend;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub struct UringIoBackend { ... }
```

- Wire `Segment` append to use backend when feature enabled
- If full async uring is too heavy, **sync `io_uring` submit+wait** for append batches is acceptable for Phase 5

### Config additions

```rust
pub struct StorageConfig {
  // existing fields...
  pub io_backend: IoBackendKind, // Std | IoUring (ignored if feature off)
  pub direct_io: bool,           // requires feature
  pub buffer_pool_blocks: usize, // 0 = disabled
  pub buffer_pool_block_size: usize, // default 64 KiB
}
```

## Broker: batch produce coalescing

- `Broker::produce` already takes `MessageBatch` — ensure single lock acquisition and optional single flush after batch
- Add `produce_coalesced` or improve `produce` to:
  - encode/append all messages without intermediate flush
  - honor `flush_every_n` once per batch
- Optional small write-behind queue (stretch): document if not implemented

## Thread-per-core (feature `thread-per-core`)

- Env or config: `VOLANT_CPU_LIST=0,1,2`
- On server start, pin main runtime worker threads if feature enabled
- macOS: best-effort or no-op with warning log

## Docs

- `docs/tuning.md`: ulimit, `vm.dirty_*`, disk scheduler, `O_DIRECT` alignment, when to enable io_uring, huge pages note, DPDK research pointer
- ROADMAP Phase 5 ✅ / Phase 6 next
- README feature flags section

## Tests

1. Buffer pool acquire/release / no leak under churn
2. Default path regression: existing durable_log tests still pass
3. `#[cfg(feature = "direct-io")]` tests only on supported OS (or ignore)
4. Batch produce: N messages one produce call → contiguous offsets
5. Bench builds without features and with `--features` on Linux CI (document)

## Exit criteria checklist

- [x] Feature flags exist and default build works on macOS
- [x] Bench suite multi-mode
- [x] Tuning guide published
- [x] Batch produce path efficient
- [ ] Published numbers in README (run release bench once and paste)

## Implementation split for agents

1. **storage-io**: pool, IoBackend, feature flags, config, segment hooks
2. **broker-batch**: produce coalescing + tests
3. **bench**: multi-command bench harness
4. **docs**: tuning.md, ROADMAP, README, optional server affinity
