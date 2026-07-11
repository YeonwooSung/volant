# Phase 5 — volant-bench review

## Iterations: 1 (no fix loop required)

### Iteration 1

| Step | Result |
|------|--------|
| PLAN | `docs/phase5/bench-plan.md` |
| CODE | Multi-mode clap CLI in `crates/volant-bench` |
| BUILD | `cargo build -p volant-bench` ✅ |
| HELP | Top-level + per-mode `--help` documents flags ✅ |
| SMOKE | `append/fetch/produce-batch --count 1000` ✅ |
| RELEASE | `cargo run -p volant-bench --release -- append` ✅ |
| CLEANUP | No leftover `volant-bench-*` under temp dir ✅ |

## Deliverables checklist

- [x] `volant-bench append [--count N] [--value-size B] [--flush-every N]`
- [x] `volant-bench fetch [--count N] [--value-size B]`
- [x] `volant-bench produce-batch [--count N] [--batch-size B]` (+ optional `--value-size`)
- [x] Prints msgs/s and MB/s
- [x] Temp dirs cleaned via `Drop` guard (`TempDir`)
- [x] Works on macOS without optional storage features
- [x] `--help` documents modes

## Release sample (macOS, default append)

```
volant-bench — append
  messages   : 100000
  value      : 100 bytes
  payload    : 10000000 bytes
  elapsed    : 184.926ms
  throughput : 540758 msgs/s
  bandwidth  : 51.57 MB/s
  flush_every : 0
  high_watermark : 100000
```

## Design notes

- **RAII cleanup**: `TempDir` removes the directory on drop so both success and early error paths clean up.
- **Fetch timing**: only the sequential read phase is timed; pre-fill is setup.
- **Final flush**: append and produce-batch include a final `flush` inside the timed region so durability cost is visible when `flush_every=0` (OS page cache writeback still applies; explicit fsync once at end).
- **No feature flags on bench crate**: default workspace build remains macOS-safe.
- **Deferred**: PHASE5_SPEC optional `--direct-io` / `--io-uring` flags are not wired yet — storage config currently has no `direct_io` / `io_backend` fields. Revisit when storage-io lands.

## Files

| Path | Change |
|------|--------|
| `crates/volant-bench/Cargo.toml` | clap + volant-broker deps |
| `crates/volant-bench/src/main.rs` | multi-command harness |
| `docs/phase5/bench-plan.md` | plan |
| `docs/phase5/bench-review.md` | this review |

## Issues found

None blocking. No second iteration required.

## Verdict

**Done.** `cargo run -p volant-bench --release -- append` works; iterations = **1**.
