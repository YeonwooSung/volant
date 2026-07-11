# Phase 4 — Word-Count Example Review

## Iteration 1

### Checklist vs deliverables

| Deliverable | Status | Notes |
|-------------|--------|-------|
| Topology: lines → tokenize flat_map → count → sink counts | **OK** | `word_count_pipeline()` + binary loop |
| CLI: `--broker`, `--group`, `--source`, `--sink` | **OK** | clap defaults match plan |
| README snippet or `--help` | **OK** | `cargo run -p volant-examples --bin word-count -- --help` |
| Offline test without broker | **OK** | stream unit tests + `tests/word_count_offline.rs` |
| `cargo build -p volant-examples` | **OK** | green |

### Code layout

| Path | Role |
|------|------|
| `crates/volant-stream/src/ops/*` | map / filter / flat_map / foreach / reduce / count |
| `crates/volant-stream/src/word_count.rs` | tokenize + pipeline helper + final_counts |
| `crates/volant-stream/src/operator.rs` | `punctuate` default added |
| `crates/volant-stream/src/pipeline.rs` | `process` + `punctuate` fan-out |
| `crates/volant-examples/src/word_count.rs` | live GroupConsumer → pipeline → produce loop |
| `crates/volant-examples/tests/word_count_offline.rs` | offline counts assertion |
| Root `Cargo.toml` | `volant-examples` workspace member |

### Tests run

```
cargo build -p volant-examples          # ok
cargo test -p volant-stream             # 8 unit + 1 doctest ok
cargo test -p volant-examples           # word_count_offline ok
cargo run -p volant-examples --bin word-count -- --help  # ok
```

### Issues found (iteration 1)

1. **foreach unit test lifetime** — `foreach` requires `'static` closure; test borrowed a local `u32`.
   - **Fix:** use `Arc<AtomicU32>` + `move` closure. Re-test green.

### Spec compliance notes

- Record conventions match PHASE4_SPEC (line → word key + `b"1"` → decimal count).
- At-least-once: binary commits **after** sink produce.
- Full `StreamBuilder` / `TopicSource` / `TopicSink` not introduced (plan non-goal); example uses
  `Pipeline` + `GroupConsumer` + `Client::produce` which is sufficient for the exit criterion.
- Windowing / RocksDB / EOS not required for word-count.

### Residual risks

- Live e2e against broker not automated (manual steps documented in binary rustdoc + plan).
- Stateful counts are **in-process memory only** — app restart resets aggregates (expected Phase 4 MVP).
- Crash between produce and commit can redeliver and re-increment (documented at-least-once).

### Iteration count

**1** (one fix for test compile; no second product code loop needed).

### Verdict

**Accept** — example binary builds, offline pipeline proof exists, CLI flags and help present.
