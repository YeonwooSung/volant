# Phase 4 Operators + State — Plan

## Scope

Implement **stateless/stateful operators**, **MemoryStore**, and **tumbling windows**
for `volant-stream`. Topology runtime, source/sink, and examples are owned by other agents.

## Ownership (this agent)

| Path | Action |
|------|--------|
| `docs/phase4/ops-plan.md` | Create (this file) |
| `docs/phase4/ops-review.md` | Create after implementation |
| `crates/volant-stream/src/operator.rs` | Add `punctuate` default |
| `crates/volant-stream/src/pipeline.rs` | Extend with `punctuate` + tests |
| `crates/volant-stream/src/ops/**` | map/filter/flat_map/foreach/reduce |
| `crates/volant-stream/src/state.rs` | `KeyValueStore` + `MemoryStore` |
| `crates/volant-stream/src/window.rs` | `TumblingWindow` |
| `crates/volant-stream/src/lib.rs` | Re-export public API |

## Non-ownership

- `source.rs`, `sink.rs`, `topology.rs`, `runtime.rs`
- Example binaries / `volant-examples`

## Design

### Operator trait

```rust
pub trait Operator: Send {
    fn process(&mut self, record: Record) -> Result<Vec<Record>>;
    fn name(&self) -> &str { "operator" }
    fn punctuate(&mut self, now_ms: i64) -> Result<Vec<Record>> { Ok(vec![]) }
}
```

### Stateless constructors (`ops/`)

| Constructor | Behavior |
|-------------|----------|
| `map(f)` | `f(record) -> Record` → single output |
| `filter(f)` | keep if `f(&record)` |
| `flat_map(f)` | `f(record) -> Vec<Record>` |
| `foreach(f)` | side-effect on `&Record`; terminal (no output) |

### Stateful reduce

- Key = `record.key` (missing/empty → `""`)
- Aggregate stored in `KeyValueStore` as `Bytes`
- Adder: `FnMut(Option<Bytes>, &Record) -> Result<Bytes>`
- Emits running aggregate record on every input
- `Reduce::new(adder)` uses `MemoryStore`; `with_store` for injection

### MemoryStore

```rust
pub trait KeyValueStore: Send {
    fn get(&self, key: &[u8]) -> Option<Bytes>;
    fn put(&mut self, key: Bytes, value: Bytes);
    fn delete(&mut self, key: &[u8]);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + '_>;
}

pub struct MemoryStore { map: HashMap<Bytes, Bytes> }
```

### TumblingWindow

- `size_ms: i64` fixed windows
- Event time = `record.timestamp_ms`; if `0`, use last `punctuate` time (processing time)
- Window start = `(event_ms / size_ms) * size_ms`
- State: `(key, window_start) -> Bytes` aggregate via same adder shape as reduce
- `process`: update aggregate; do **not** emit immediately
- `punctuate(now_ms)`: emit all windows with `window_end <= now_ms`, then drop them
- Output: key = original key, value = aggregate bytes, `timestamp_ms` = window_end - 1

### Pipeline extension

```rust
pub fn punctuate(&mut self, now_ms: i64) -> Result<Vec<Record>>
```

Call `punctuate` on each operator in order; feed emitted records through remaining downstream operators.

## Tests (unit, offline)

1. `map` transforms value
2. `filter` drops non-matching
3. `flat_map` splits
4. `foreach` runs side-effect, emits nothing
5. `MemoryStore` get/put/delete/iter
6. `Reduce` counts keys
7. `TumblingWindow` emits at boundary via `punctuate`
8. Offline word-count pipeline: flat_map words → reduce count

## Iteration log

| Iter | Action |
|------|--------|
| 1 | PLAN + CODE + TEST (13 ok) + REVIEW |
| 2 | FIX minor cleanup; re-TEST |

## Done criteria

- `cargo test -p volant-stream` green
- All deliverables present and exported from `lib.rs`
