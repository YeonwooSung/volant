# Phase 4 — Topology + Runtime Plan

**Agent:** Topology + Runtime  
**Iteration budget:** 3  
**Binding spec:** `docs/PHASE4_SPEC.md`

## Scope (this agent)

| Path | Role |
|------|------|
| `crates/volant-stream/src/source.rs` | `SourceConfig`, `TopicSource` (GroupConsumer wrapper) |
| `crates/volant-stream/src/sink.rs` | `TopicSink` (produce via `Client`) |
| `crates/volant-stream/src/topology.rs` | `StreamBuilder` / `Topology` |
| `crates/volant-stream/src/runtime.rs` | `StreamApp` run loop (at-least-once) |
| `crates/volant-stream/src/lib.rs` | Module exports |
| `crates/volant-stream/src/operator.rs` | Add `punctuate` (runtime needs it) |
| `crates/volant-stream/src/pipeline.rs` | `process` + stage-wise `punctuate`; helpers |
| `Cargo.toml` | Dev-deps for tests if needed |

**Out of scope (other agents / later):** full `ops/` package, `window.rs`, `state.rs`, word-count example binary, ROADMAP/README polish.

**Operators:** Scaffold only has `Operator` + `Pipeline`. Keep fluent API trait-based (`.then(op)`). Add thin map/filter/flat_map/foreach closures on the builder so unit tests and demos work without waiting for `ops/`. Document that full reduce/window ops land separately.

## API design

```rust
// Config
pub struct SourceConfig {
    pub group_id: String,
    pub session_timeout_ms: u32, // default 10_000
    pub max_messages: u32,       // documented; GroupConsumer currently hardcodes 100
    pub max_wait_ms: u32,        // documented; GroupConsumer currently hardcodes 0
}

// Builder
StreamBuilder::new(name)
  .source_topic(topic, SourceConfig { ... })
  .then(op) | .map(f) | .filter(f) | .flat_map(f) | .foreach(f)
  .sink_topic(topic)
  .build() -> Result<Topology>

// Runtime
StreamApp::run(topology, Arc<Client>).await?;
// also: run_once, process_offline for tests
```

### Topology fields

- `name: String`
- `source_topic: String`
- `source_config: SourceConfig`
- `pipeline: Pipeline`
- `sink_topic: String`

### TopicSource

- Wraps `GroupConsumer`
- `open(client, topic, config) -> Self`
- `poll() -> Vec<Record>` (convert `FetchRecord` → `volant_core::Record`)
- `commit()`, `leave()`, `assignment()`

### TopicSink

- Holds `Arc<Client>` + sink topic
- `send_batch(records: &[Record])` → map to `Message`, produce with acks=1 (client default)
- Empty batch is a no-op

### Runtime loop (at-least-once)

1. Poll source → records  
2. `pipeline.process` each record through operators; after each stage call `punctuate(now_ms)`  
3. Produce outputs to sink topic  
4. `source.commit()`  
5. On crash between 3 and 4: sink may have duplicates on redelivery — **at-least-once**

Idle: short sleep when poll returns empty (avoid busy-spin).

Stop: `run` loops until error; `run_until` accepts a cancel flag / max iterations for tests.

### At-least-once documentation

Module docs on `runtime.rs` and `lib.rs` state clearly:

- Offsets commit **only after** successful sink produce  
- Duplicate sink records possible after crash post-produce pre-commit  
- Exactly-once is a non-goal (Phase 4)

## Tests

1. **Builder unit:** `source_topic` + `then` + `sink_topic` → `Topology` fields correct  
2. **Builder validation:** missing source or sink → `InvalidArgument`  
3. **Offline process:** map/filter through `StreamApp::process_offline` / pipeline  
4. **At-least-once ordering doc test / unit:** commit is not called when sink fails (mock via offline simulation of steps, or inject failing sink if we expose hooks)

Integration with live broker is optional (heavy); skip unless trivial.

## Implementation order

1. Extend `Operator::punctuate` + `Pipeline` stage processing  
2. Closure operators (map/filter/flat_map/foreach) for fluent helpers  
3. `source.rs` / `sink.rs`  
4. `topology.rs`  
5. `runtime.rs`  
6. Unit tests  
7. `cargo test -p volant-stream`  
8. Review doc

## Client light-touch

Prefer no client changes. `GroupConsumer::join(client, group_id, topics, session_timeout_ms)` and `Client::produce` are sufficient. `SourceConfig.max_messages` / `max_wait_ms` stored for forward-compat; documented as not yet wired into `GroupConsumer` (hardcoded 100 / 0 today).

## Risks

| Risk | Mitigation |
|------|------------|
| `Client` not `Clone` | Share via `Arc<Client>` (already used by GroupConsumer) |
| No async_trait | Concrete types only; no source/sink traits |
| Ops crate missing | Trait-based `.then` + closure helpers |
| Busy loop on empty poll | `tokio::time::sleep` idle backoff |

## Iteration log

- **Iteration 1 (plan):** this document

### Iteration 1 result

- Implemented all modules; 18 unit tests green.
- No client changes; no open review findings.
- Status: **done** (no iteration 2/3 required).
