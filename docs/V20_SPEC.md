# v0.20 — Produce group-commit (storage fsync coalescing)

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the v0.2 leftover “group-commit not implemented” without
opening Phase 155, touching openraft, Kafka API keys, or membership.

This is **storage group-commit**: coalesce `fsync` across concurrent /
back-to-back produce appends so many records share one disk sync. It is
**not** consumer `group commit`.

## Goals

1. **Time-based group commit** on `PartitionLog` plus a shareable
   `SharedPartitionLog` wrapper (and broker produce/flush join the same
   coordinator after releasing the topics write lock).
2. **Config:** `StorageConfig.group_commit_max_ms` (`0` = off, today's
   behavior). Broker env `VOLANT_GROUP_COMMIT_MS` (default **0**).
   Optional `group_commit_max_records` (flush when either threshold hits).
   Default records = existing `flush_every_n` if set, else **64** when
   `ms > 0`.
3. **Semantics:** `acks=1` / `acks=all` produce that needs durability may
   wait up to `group_commit_max_ms` for a shared `flush()` instead of
   fsync-per-batch. Concurrent appenders on the **same** partition share
   one fsync (condvar + generation counter).
4. `flush_every_n == 0` **and** `group_commit_max_ms == 0` → no implicit
   fsync (today).
5. **Crash honesty:** records not yet group-committed may be lost (same as
   unflushed `flush_every_n`).
6. `append_batch` still single-flush-at-end. Group-commit is for
   **cross-caller** coalescing (`SharedPartitionLog` / broker flush).
7. Metrics: `volant_group_commit_flushes_total` and
   `volant_group_commit_records_total` on `/metrics`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Default-on group commit | Would change durability/latency vs v0.2 benches |
| Background flusher thread | Empty / no waiters must not spin |
| Consumer group commit | Different feature |
| Kafka API keys / openraft / membership | Sibling slices; do not open Phase 155 |
| Per-topic group-commit override | Broker-wide env + `StorageConfig` is enough |
| Counting I/O backend in production | `PartitionLog::fsync_count` test hook is enough |

## Config

| Knob | Default | Meaning |
|------|---------|---------|
| `StorageConfig.group_commit_max_ms` / `VOLANT_GROUP_COMMIT_MS` | `0` (off) | Max wait for a shared fsync |
| `StorageConfig.group_commit_max_records` / `VOLANT_GROUP_COMMIT_MAX_RECORDS` | `0` (inherit) | Flush when this many dirty records accumulate |
| inherit when records = 0 | `flush_every_n` if `> 0`, else `64` | Documented default |

`Broker::new` / `Broker::with_cluster` call
`StorageConfig::apply_group_commit_env()` so the server process picks up
the env without a new CLI flag.

## Path

1. Append writes into the page cache (`append` / `append_batch` /
   `append_batch_uncommitted`).
2. A waiter registers `flush_gen` and either **leads** (sleep remaining
   `max_ms` or until `max_records`, then `flush()`, bump gen, notify) or
   **follows** the condvar.
3. Broker produce holds the topics lock only for the write; the wait
   happens in `Broker::flush` after release so two produce RPCs can share
   one fsync.
4. Native dispatch skips a second `flush()` when group-commit already
   ran inside `produce_with_acks`.

## Tests

```bash
cargo test -p volant-storage --test v20_group_commit -- --test-threads=1
cargo test -p volant-storage --test durable_log -- --test-threads=1
cargo test -p volant-broker --test v20_group_commit -- --test-threads=1
```

## Honesty leftovers

- Sequential exclusive `PartitionLog::append` with the window on pays the
  wait (no concurrent joiner). Use `SharedPartitionLog` or the broker path
  for coalescing.
- Two concurrent appenders may still issue **1 or 2** fsyncs if they miss
  the same window; tests assert `fsyncs <= appends`.
- Kafka produce historically did not call `flush()` unless
  `flush_every_n > 0`. With the window **on**, `produce_with_acks` now
  waits for group-commit when `acks != 0`.
- No published perf numbers vs the aspirational table; this slice ships
  the mechanism, not a new baseline.
- Uncommitted records (acks=0, or crash before the window fires) may be
  lost — same honesty as `flush_every_n = 0`.
