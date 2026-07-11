# Phase 4 — Topology + Runtime Review

**Iteration:** 1 (complete — tests green, no open findings)  
**Binding:** `docs/PHASE4_SPEC.md`  
**Plan:** `docs/phase4/runtime-plan.md`

## Checklist

- [x] `SourceConfig` + `TopicSource` (GroupConsumer wrapper)
  - `open` / `poll` / `commit` / `leave` / `assignment`
  - `FetchedRecord` → `volant_core::Record` conversion
  - `max_messages` / `max_wait_ms` stored for forward-compat (GroupConsumer hardcodes 100/0)
- [x] `TopicSink` (produce via `Arc<Client>`)
  - `send` / `send_batch`; empty batch no-op; acks=1 via client default
- [x] `StreamBuilder` / `Topology`
  - `new` → `source_topic` → `then` / map/filter/flat_map/foreach → `sink_topic` → `build`
  - Validation: missing source/sink, empty topics, empty group_id
- [x] `StreamApp` run loop (at-least-once)
  - poll → `process_with_punctuate` → sink produce → commit
  - Idle sleep + punctuate on empty poll
  - `run` / `run_for` / `run_once` / `process_offline`
  - `Topology::run` / `run_for` convenience
- [x] At-least-once documented in `runtime.rs` + `lib.rs` module docs
- [x] `Operator::punctuate` default + pipeline stage-wise punctuation
- [x] Closure operators (map/filter/flat_map/foreach) for fluent API without waiting on `ops/`
- [x] `#![deny(missing_docs)]` satisfied
- [x] No client crate changes required

## Files touched

| Path | Change |
|------|--------|
| `crates/volant-stream/src/source.rs` | **new** TopicSource + SourceConfig |
| `crates/volant-stream/src/sink.rs` | **new** TopicSink |
| `crates/volant-stream/src/topology.rs` | **new** StreamBuilder / Topology |
| `crates/volant-stream/src/runtime.rs` | **new** StreamApp at-least-once loop |
| `crates/volant-stream/src/operator.rs` | punctuate + closure ops |
| `crates/volant-stream/src/pipeline.rs` | process_with_punctuate |
| `crates/volant-stream/src/lib.rs` | module exports + docs |
| `crates/volant-stream/Cargo.toml` | dev-dep volant-protocol for tests |
| `docs/phase4/runtime-plan.md` | plan |
| `docs/phase4/runtime-review.md` | this review |

## Tests

| Suite | Result |
|-------|--------|
| `cargo test -p volant-stream` | **18 passed**, 0 failed, 3 ignored doc-tests |

Coverage:

1. Operator unit: map / filter / flat_map / foreach / punctuate default  
2. Pipeline composition map+filter  
3. Builder happy path + validation (missing source/sink) + custom `.then`  
4. Offline word-split via builder pipeline  
5. SourceConfig defaults + FetchedRecord conversion  
6. record_to_message key/value/timestamp  
7. process_offline map/filter + at-least-once ordering contract + RunConfig  

Live broker integration: **not run** (optional per plan; GroupConsumer e2e already covered in client/broker crates).

## Findings

None open after iteration 1.

### Notes / deferred

1. **`SourceConfig.max_messages` / `max_wait_ms`** not wired into `GroupConsumer` (client hardcodes). Documented; wire when client gains config struct.
2. **`ops/` reduce / window / state** owned elsewhere — pipeline remains trait-based; runtime already calls `punctuate`.
3. **word-count example binary** out of this agent's ownership.
4. **ctrl-c graceful shutdown** — `run` loops until error; `run_for(max_iterations)` for bounded runs. Signal handling can wrap `run_for` later.

## At-least-once verification

`StreamApp::run_once` ordering (source of truth):

```text
poll → process_with_punctuate → sink.send_batch → source.commit
```

On sink error, commit is not reached → redelivery → possible sink duplicates. Module docs state this explicitly.

## Iteration log

- **Iteration 1:** Plan → implement source/sink/topology/runtime + operator/pipeline extensions → unit tests → review.  
  Result: `cargo test -p volant-stream` 18 green. No further iterations needed.
