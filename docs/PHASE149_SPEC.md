# Phase 149 — Durable stream state store (MVP)

**Status:** ✅ Shipped  
**Theme:** Persist stateful stream operator aggregates (`reduce` / `count_reduce`)
across process restart via a pure-Rust embedded KV store. Sibling Phase 150
covers broker consensus — **out of scope** here.

## Problem

`volant-stream` stateful operators used [`MemoryStore`] only. Aggregates were
lost on process exit. ROADMAP left an open choice: RocksDB / redb / custom mmap.

## Goals

1. **`DurableStore`** implementing full `KeyValueStore` (get/put/delete/iter/len)
2. **Reduce integration** — `Reduce::with_store`, `count_reduce_with_store`,
   `count_reduce_durable(path)`
3. **Light topology hook** — `StreamBuilder::state_dir` + optional
   `reduce_count_durable`; path stored on `Topology::state_dir`
4. **Tests** — CRUD, restart, durable reduce, MemoryStore regression
5. **Docs** — honesty: durable state ≠ exactly-once

## Non-goals

| Deferred | Why |
|----------|-----|
| Exactly-once processing | At-least-once still applies; separate product bet |
| Distributed stream workers | Topology still in-process |
| Durable window buckets | Stretch; not required for reduce MVP |
| RocksDB | C++ toolchain; redb is pure Rust |
| Broker / consensus changes | Sibling Phase 150 |

## Design choice: redb

| Option | Verdict |
|--------|---------|
| **A) redb** (chosen) | Pure Rust, ACID, crash-safe Immediate commits, small API |
| B) custom WAL + snapshot | More code, higher bug surface for MVP |
| RocksDB | Explicit non-goal (native deps) |

Workspace dep: `redb = "2"`. Single table `"kv"`; on-disk file `{state_dir}/kv.redb`.

### Durability guarantees

- Each `put` / `delete` opens a write transaction and **commits with redb
  `Durability::Immediate`** (fsync on commit) — auto-flush every mutation for MVP.
- `DurableStore::flush()` is an explicit no-op barrier (mutations already durable);
  retained for API symmetry / future batching.
- Reopen the same directory after process exit → keys present.
- **Honesty:** durable aggregates do **not** imply exactly-once. Crash between
  sink produce and offset commit can still replay inputs and double-count.

### API surface

```rust
// crates/volant-stream
pub struct DurableStore { /* redb Database + path */ }
impl DurableStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StreamStateError>;
    pub fn flush(&self) -> Result<(), StreamStateError>;
    pub fn path(&self) -> &Path;
}
impl KeyValueStore for DurableStore { /* get/put/delete/iter/len */ }

pub fn count_reduce_with_store<S: KeyValueStore>(store: S) -> Reduce<S, ...>;
pub fn count_reduce_durable(path) -> Result<Reduce<DurableStore, ...>, StreamStateError>;

impl StreamBuilder {
    pub fn state_dir(self, path) -> Self;
    pub fn reduce_count_durable(self) -> Result<Self, StreamStateError>; // needs state_dir
}
// Topology.state_dir: Option<PathBuf>
```

`KeyValueStore` trait methods remain infallible; storage failures after a
successful `open` panic with a clear message (trait signature preserved).

## Exit criteria

1. [x] `DurableStore` put/get/delete/len/iter  
2. [x] Restart: open → put → drop → reopen → data present  
3. [x] `count_reduce` / pipeline with `DurableStore`; aggregates after restart  
4. [x] `MemoryStore` + existing e2e still green  
5. [x] Docs (this file, TODO, ROADMAP, features, README, PHASE_HISTORY, INDEX)

## Tests

- Unit: `state::durable`, `state::memory`
- Integration: `crates/volant-stream/tests/phase149_durable_state.rs`
- Regression: `cargo test -p volant-stream` (incl. `e2e_word_count`)

## Honest residual

- At-least-once only; no transactional produce/commit coupling to state
- One redb process lock per store path (cannot double-open same DB)
- Windows / window operator buckets still process-local
- No distributed changelog / standby task
