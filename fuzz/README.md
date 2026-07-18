# Protocol fuzz targets (Phase 9 + Phase 112)

Optional [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) harness for
`volant-protocol` decode paths. **Not** a workspace member — full mutation
fuzzing requires nightly + `cargo-fuzz`.

## Targets

| Target | Path | What it exercises |
|--------|------|-------------------|
| `decode_frame` | `fuzz_targets/decode_frame.rs` | `codec::decode_frame` (partial + second pass) |
| `decode_request` | `fuzz_targets/decode_request.rs` | `decode_request` / `decode_response` by opcode |

## Seed corpus (Phase 112)

Checked-in seeds under `corpus/`:

```
fuzz/corpus/decode_frame/   # empty, partial, invalid magic, wrong version,
                            # valid frames, max-size claim, trailing garbage
fuzz/corpus/decode_request/ # empty, truncated, unknown opcode, length-prefix
                            # edge cases, oversize len claims
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

# Short capped smoke (Phase 112 local helper)
FUZZ_SMOKE_RUNS=200 ./scripts/fuzz_corpus_smoke.sh fuzz
# or
cargo +nightly fuzz run decode_frame -- -runs=200
cargo +nightly fuzz run decode_request -- -runs=200
```

## Still deferred

- Multi-hour CI fuzz campaigns / corpus minimization automation
- Chaos-mesh (partition loss, disk full, slow disk)
- Kafka wire-protocol fuzz targets (native protocol only today)
