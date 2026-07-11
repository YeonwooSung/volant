# Phase 4 — Docs + E2E Stream Tests Review

## Iterations

| Iter | Outcome | Notes |
|------|---------|-------|
| 1 | **Green** | Implemented full `volant-stream` stack (ops, state, window, source/sink, topology, runtime), offline + live word-count e2e, example binary, ROADMAP/README. Fixed minor unused-`mut` warnings. |

**Iteration count: 1**

## Deliverables checklist

### Spec / plan / review
- [x] `docs/PHASE4_SPEC.md` (binding; pre-existed, still accurate)
- [x] `docs/phase4/docs-e2e-plan.md`
- [x] `docs/phase4/docs-e2e-review.md` (this file)

### Tests (`crates/volant-stream/tests/e2e_word_count.rs`)
- [x] Offline word-count pipeline (required)
- [x] Unit-style map/filter/flat_map/foreach
- [x] Reduce counts keys
- [x] Tumbling window emits at boundary
- [x] Live e2e: boot broker → produce lines → topology steps → fetch counts

### Example
- [x] `crates/volant-stream/examples/word_count.rs`
  (`cargo run -p volant-stream --example word_count -- --broker 127.0.0.1:9092`)

### Docs
- [x] `ROADMAP.md` Phase 4 ✅; Phase 5 marked next
- [x] `README.md` Phase 4 section, programming model, word-count runbook

### Supporting stack (implemented because placeholders)
- [x] `Operator::punctuate` + `Pipeline::punctuate`
- [x] Stateless ops: map/filter/flat_map/foreach
- [x] Stateful: `reduce` / `count_reduce`, `MemoryStore`
- [x] `TumblingWindow`
- [x] `TopicSource` / `TopicSink`
- [x] `StreamBuilder` / `Topology` / `StreamApp` (at-least-once)

## Test results

```
cargo test -p volant-stream
# e2e_word_count: 7 passed (offline + live)

cargo test --workspace
# broker 13 + durable 3 + inprocess 1 + partition 2
# client e2e_group 3 + e2e_tcp 3
# core 2 + protocol 7 + storage 11 + durable_log 6
# stream e2e_word_count 7
# all green
```

## Design notes / deviations

1. **Example location:** `crates/volant-stream/examples/word_count.rs` rather
   than a separate `volant-examples` crate (PHASE4_SPEC allows either).

2. **Reduce emit semantics:** every input emits the running aggregate for that
   key. Offline/live tests take the **last** value per key as the final count.

3. **Sink produce:** one produce RPC per output record so key-hash routing
   applies; fine for e2e, not optimized for throughput.

4. **At-least-once only:** commit after successful sink; crash window can
   duplicate. Exactly-once deferred.

5. **State store:** in-memory `BTreeMap` (`MemoryStore`) only — no RocksDB /
   snapshots.

## Open / deferred

- Exactly-once / transactional produce
- RocksDB or file-backed state snapshots
- Hopping windows
- WASM / plugin operators
- CLI `volant stream word-count` (example binary covers the demo)

## Blockers

None. Workspace tests green.
