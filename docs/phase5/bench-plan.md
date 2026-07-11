# Phase 5 — volant-bench multi-mode plan

## Goal

Expand `volant-bench` from a single hard-coded append loop into a clap-driven
multi-mode micro-benchmark harness that measures storage and broker hot paths
on macOS without optional storage features (`io-uring` / `direct-io`).

## CLI surface

```
volant-bench append [--count N] [--value-size B] [--flush-every N]
volant-bench fetch  [--count N] [--value-size B]
volant-bench produce-batch [--count N] [--batch-size B] [--value-size B]
```

`--help` must document each mode. Defaults keep runs useful but short enough
for CI / agent loops:

| Flag | Default |
|------|---------|
| `--count` | 100_000 |
| `--value-size` | 100 |
| `--flush-every` | 0 (no per-append fsync; final flush optional) |
| `--batch-size` | 100 |

## Modes

### `append`

1. Create unique temp dir under `std::env::temp_dir()`.
2. Open `PartitionLog` with large `segment_size` (256 MiB) so rolls do not dominate.
3. Append `count` messages of `value_size` bytes with a varying 4-byte tag.
4. Honor `flush_every` via `StorageConfig::flush_every_n`.
5. Print elapsed, msgs/s, MB/s (payload bytes / elapsed).
6. `remove_dir_all` temp dir (best-effort after success; still try on error via RAII guard).

### `fetch`

1. Temp dir + open log.
2. Pre-populate with `count` messages of `value_size` (not timed, or reported separately as "setup").
3. Flush so mmap/read path sees data.
4. Time sequential `read` from offset 0 until all messages consumed (chunked reads of e.g. 1024).
5. Print msgs/s and MB/s for the read phase only.
6. Cleanup temp dir.

### `produce-batch`

1. Temp dir + `Broker::new` with large segment config.
2. Create single-partition topic `bench`.
3. Time loop: build `MessageBatch` of `batch_size` messages; call `Broker::produce` until `count` messages total (last batch may be smaller).
4. Print msgs/s, MB/s, batches/s.
5. Cleanup temp dir.

## Dependencies

- `clap` (workspace) — derive CLI
- `volant-core`, `volant-storage` (existing)
- `volant-broker` — for produce-batch
- `anyhow` (existing)

## Metrics

```
msgs/s  = count / elapsed_secs
MB/s    = (count * value_size) / elapsed_secs / 1_048_576
```

Use f64; print with reasonable precision. On zero elapsed, print `inf`.

## Platform constraints

- No feature flags required on the bench crate.
- Do not enable `io-uring` / `direct-io` (storage may not expose them yet; macOS default build must succeed).
- Optional flags from the Phase 5 spec (`--direct-io`, `--io-uring`) are deferred until storage wires those backends; document in review if omitted.

## Files touched

- `crates/volant-bench/Cargo.toml` — add clap, volant-broker
- `crates/volant-bench/src/main.rs` — full rewrite to multi-command CLI
- `docs/phase5/bench-plan.md` (this file)
- `docs/phase5/bench-review.md` — after CODE + TEST

## Test plan

1. `cargo build -p volant-bench`
2. `cargo run -p volant-bench -- --help`
3. `cargo run -p volant-bench --release -- append` (default count)
4. Quick smoke: `append --count 1000`, `fetch --count 1000`, `produce-batch --count 1000 --batch-size 50`
5. Confirm temp dirs under `/tmp` (or macOS temp) are removed after runs

## Iteration budget

Max 3 PLAN → CODE → REVIEW → TEST → FIX loops; record each in review.
