# Phase 4 Operators + State — Review

## Iteration log

| Iter | Result | Notes |
|------|--------|-------|
| 1 | PASS | PLAN → CODE → `cargo test -p volant-stream` (13 ok) → REVIEW cleanup |
| 2 | PASS | Simplified `count_adder`; removed dead code in pipeline punctuate test |

**Total iterations: 2**

## Spec checklist

| Deliverable | Status | Location |
|-------------|--------|----------|
| `map` constructor | ✅ | `ops/map.rs` → `ops::map` / crate root |
| `filter` constructor | ✅ | `ops/filter.rs` |
| `flat_map` constructor | ✅ | `ops/flat_map.rs` |
| `foreach` constructor | ✅ | `ops/foreach.rs` (terminal: no output) |
| `Reduce` keyed aggregate via `KeyValueStore` | ✅ | `ops/reduce.rs` |
| `MemoryStore` implements `KeyValueStore` | ✅ | `state.rs` |
| `TumblingWindow` with `punctuate` | ✅ | `window.rs` |
| `Operator::punctuate` default | ✅ | `operator.rs` |
| Unit tests: each op + reduce + window + offline WC | ✅ | 13 tests green |
| Topology / source / sink / examples | N/A | Other agents |

## Review findings (iter 1 → fixed in iter 2)

1. **Minor** — `count_adder` had redundant delta logic → simplified.
2. **Minor** — `pipeline_punctuate_feeds_downstream` constructed an unused pipeline → removed.
3. **Design note** — `foreach` is terminal (emits `[]`); matches Kafka Streams and spec listing as side-effect op.
4. **Design note** — `Pipeline::punctuate` feeds window emissions through **downstream** ops only (not re-running upstream), which is correct for timer-driven flushes.
5. **Design note** — Tumbling window defers all emission to `punctuate` (no early emit on process); runtime must call `punctuate` each poll per PHASE4_SPEC.

## Test results

```
cargo test -p volant-stream
running 13 tests
... all ok
```

## Public API surface (`lib.rs`)

- Traits: `Operator`, `KeyValueStore`
- Types: `Pipeline`, `MemoryStore`, `Map`, `Filter`, `FlatMap`, `ForEach`, `Reduce`, `TumblingWindow`
- Constructors: `map`, `filter`, `flat_map`, `foreach`, `reduce`, `tumbling_window`
- Helpers: `count_adder`, `parse_count`

## Residual risks / non-blockers

- No integration with live broker (out of scope for this agent).
- Window processing-time fallback depends on a prior `punctuate` call when `timestamp_ms == 0`.
- `Reduce` always emits running aggregate (changelog-style); no suppress-until-window mode (use `TumblingWindow` for that).
