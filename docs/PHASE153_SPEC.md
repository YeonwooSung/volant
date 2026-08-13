# Phase 153 — EOS + durable stream state atomic boundary

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Stage durable stream state during an exactly-once step and commit it
only after a successful EndTxn, so crash after durable put cannot leave
**state ahead of offsets**. Sibling Phase 154 covers openraft/KRaft metadata —
**out of scope** here (no broker consensus changes).

## Problem

Phase 151 EOS commits produce + group offsets atomically, but Phase 149
`DurableStore` commits each `put` immediately (redb `Durability::Immediate`).
Crash after a durable put but before/after the broker txn can leave aggregates
ahead of committed offsets (or inconsistent with EOS replay expectations).

## Goals

1. **Checkpoint-capable `KeyValueStore`** — default no-op
   `begin_checkpoint` / `commit_checkpoint` / `abort_checkpoint` / `in_checkpoint`
2. **`DurableStore` staging** — overlay while checkpoint open; single Immediate
   write txn on commit; discard on abort; immediate put outside checkpoint (ALO)
3. **Operator + Pipeline hooks** — `Reduce` forwards to store; `Pipeline` fans out
4. **EOS runtime order** — begin checkpoint → process → txn → commit checkpoint
   (or abort on empty / fail)
5. **Tests** — staging abort/commit, reduce/pipeline order, MemoryStore no-op
6. **Docs** — honesty: process-local staging, not distributed 2PC

## Non-goals

| Deferred | Why |
|----------|-----|
| openraft / KRaft / assignment consensus | Sibling Phase 154 |
| Broker protocol changes | Stream-local only |
| Distributed 2PC state ↔ broker | Staging is process-local |
| Durable window buckets | Stretch; reduce MVP only |
| ALO batching via checkpoint | ALO keeps immediate put |

## Design

### KeyValueStore

```rust
fn begin_checkpoint(&mut self) {}
fn commit_checkpoint(&mut self) -> Result<(), StreamStateError> { Ok(()) }
fn abort_checkpoint(&mut self) {}
fn in_checkpoint(&self) -> bool { false }
```

`MemoryStore` leaves defaults (in-memory already ephemeral).

### DurableStore staging

| Mode | put / delete | get / iter / len |
|------|--------------|------------------|
| Outside checkpoint | Immediate redb write txn | Disk |
| Inside checkpoint | Overlay only (`BTreeMap` + delete set) | Overlay then disk |
| `commit_checkpoint` | Apply overlay in one Immediate txn; clear | — |
| `abort_checkpoint` | Clear overlay | View = last committed disk |

### EOS step order (`step_exactly_once`)

```
1. pipeline.begin_checkpoint()
2. poll → process → punctuate   // DurableStore puts stage only
3. if empty skip: abort_checkpoint(); return
4. txn.begin → produce → add_offsets → commit
5. on txn success: pipeline.commit_checkpoint()
6. on txn fail: abort txn; pipeline.abort_checkpoint(); return Err
```

**Never** commit durable state before successful EndTxn.

ALO path: no checkpoint (DurableStore remains immediate-put).

### Operator / Pipeline

```rust
// Operator defaults
fn begin_checkpoint(&mut self) {}
fn commit_checkpoint(&mut self) -> Result<()> { Ok(()) }
fn abort_checkpoint(&mut self) {}

// Reduce forwards to KeyValueStore
// Pipeline::begin/commit/abort_checkpoint fan out to operators
```

## API surface

```rust
// KeyValueStore + DurableStore checkpoint methods (see above)
Operator::{begin_checkpoint, commit_checkpoint, abort_checkpoint}
Pipeline::{begin_checkpoint, commit_checkpoint, abort_checkpoint}
// StreamApp::step_exactly_once — order above (internal)
```

## Honesty / limitations

- Checkpoint is **process-local staging**, not distributed 2PC with the broker
- If EndTxn succeeds and `commit_checkpoint` then fails, offsets are committed
  while local state may lag (rare I/O residual; returned as `Err`)
- ALO durable still **immediate** per put
- In-process only; no distributed stream workers / changelog
- Window operator buckets still process-local
- Depends on Phase 151 EOS + Phase 149 redb store

## Exit criteria

1. [x] `KeyValueStore` checkpoint defaults  
2. [x] `DurableStore` staging + commit/abort  
3. [x] `Reduce` / `Pipeline` hooks  
4. [x] EOS step order: commit durable **after** EndTxn  
5. [x] Tests `phase153_eos_durable_atomic`  
6. [x] Phase 149 / 151 still green  
7. [x] Docs (this file, TODO, ROADMAP, features, README, PHASE_HISTORY, INDEX)

## Tests

- Integration: `crates/volant-stream/tests/phase153_eos_durable_atomic.rs`
- Unit: `state::durable` checkpoint unit tests
- Regression: `phase149_durable_state`, `phase151_exactly_once`, `cargo test -p volant-stream`
