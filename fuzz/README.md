# Protocol fuzz targets (Phase 9 + Phase 112 + v0.15)

Optional [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) harness for
`volant-protocol` decode paths. **Not** a workspace member — full mutation
fuzzing requires nightly + `cargo-fuzz`.

## Targets

| Target | Path | What it exercises |
|--------|------|-------------------|
| `decode_frame` | `fuzz_targets/decode_frame.rs` | `codec::decode_frame` (partial + second pass) |
| `decode_request` | `fuzz_targets/decode_request.rs` | `decode_request` / `decode_response` by opcode |
| `decode_extended` | `fuzz_targets/decode_extended.rs` | membership **100–107** + txn **32/50/52** (v0.15) |

## Seed corpus (Phase 112)

Checked-in seeds under `corpus/`:

```
fuzz/corpus/decode_frame/   # empty, partial, invalid magic, wrong version,
                            # valid frames, max-size claim, trailing garbage
fuzz/corpus/decode_request/ # empty, truncated, unknown opcode, length-prefix
                            # edge cases, oversize len claims
fuzz/corpus/decode_extended/ # membership 100–107 + txn 32/50/52: empty,
                             # truncated, oversize, valid-ish (v0.15)
```

These are **deterministic edge cases**, not a long-running AFL/libFuzzer corpus.

## CI / deterministic smoke (no nightly)

Workspace CI always runs:

1. `cargo test --workspace --all-targets`
2. Explicit `cargo test -p volant-protocol corpus_smoke`

The `corpus_smoke_decode_paths` unit test replays built-in seeds **and** any
files under `fuzz/corpus/*` through the **same** decode entry points as the
fuzz targets. It must never panic. See also expanded chaos tests:

- `chaos_decode_does_not_panic`
- `chaos_frame_decode_extended`

```bash
# From repository root (stable toolchain)
cargo test -p volant-protocol corpus_smoke
# includes corpus_smoke_decode_paths + corpus_smoke_extended
# or
./scripts/fuzz_corpus_smoke.sh test
```

## Local full cargo-fuzz (optional)

```bash
rustup install nightly
cargo install cargo-fuzz

# Unbounded (local research only — not CI)
cargo +nightly fuzz run decode_frame
cargo +nightly fuzz run decode_request
cargo +nightly fuzz run decode_extended

# Short capped smoke (Phase 112 local helper)
FUZZ_SMOKE_RUNS=200 ./scripts/fuzz_corpus_smoke.sh fuzz
# Capped wall-clock campaign (v0.15). CI does **not** run this on push/PR.
FUZZ_LONG_SECS=30 ./scripts/fuzz_corpus_smoke.sh long
# or
cargo +nightly fuzz run decode_frame -- -runs=200
cargo +nightly fuzz run decode_request -- -runs=200
cargo +nightly fuzz run decode_extended -- -max_total_time=30
```

## Still deferred

- Multi-hour CI fuzz campaigns / corpus minimization automation
- Chaos Mesh **in CI** (operator YAMLs live under `deploy/chaos/`; not applied by Actions)
- Disk-full / slow-disk Chaos Mesh experiments
- Kafka wire-protocol fuzz targets (native protocol only today)
