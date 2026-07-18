# Phase 112 — cargo-fuzz corpus smoke + CI (MVP)

## Goals

1. Inventory existing `fuzz/` targets and protocol chaos unit tests.
2. Add a **deterministic corpus smoke** path that does not require long fuzz runs
   or nightly/cargo-fuzz on CI.
3. Check in a small seed corpus under `fuzz/corpus/` covering empty / partial /
   invalid / valid / max-size-claim / length-prefix edge cases.
4. Add a GitHub Actions workflow that runs workspace tests + the corpus smoke.
5. Document local optional short `cargo fuzz run … -- -runs=N` for research.
6. Living docs honesty: cargo-fuzz **corpus CI MVP** closed; long campaigns and
   chaos-mesh remain deferred.

## Non-goals

- Multi-hour CI fuzz campaigns / corpus minimization bots
- Chaos-mesh (partition loss, disk full, slow disk)
- Kafka wire-protocol fuzz targets
- Multi-lang clients / multi-broker 2PC / session affinity

## Inventory (pre-existing)

| Asset | Location | Notes |
|-------|----------|-------|
| Fuzz scaffold | `fuzz/` (Phase 9) | Not a workspace member; needs nightly + cargo-fuzz |
| `decode_frame` | `fuzz/fuzz_targets/decode_frame.rs` | `codec::decode_frame` twice |
| `decode_request` | `fuzz/fuzz_targets/decode_request.rs` | opcode LE + payload → req/resp decode |
| Chaos unit tests | `volant-protocol` | `chaos_decode_does_not_panic`, `chaos_frame_decode_extended` |

## Design

### Deterministic smoke (CI path)

Unit test `corpus_smoke_decode_paths` in `volant-protocol`:

1. Built-in seed vectors (always run).
2. Load any files under `{workspace}/fuzz/corpus/decode_frame/` and
   `…/decode_request/` when present.
3. Feed each seed through the **same** entry points as the cargo-fuzz targets.
4. Assert `MAX_PAYLOAD + 1` still rejects without panic.

No network, no broker, no nightly.

### Seed corpus

Checked in under `fuzz/corpus/{decode_frame,decode_request}/` so local
`cargo +nightly fuzz run <target>` also picks them up as libFuzzer seeds.

### Optional local cargo-fuzz

`scripts/fuzz_corpus_smoke.sh`:

| Mode | Behavior |
|------|----------|
| `test` (default) | `cargo test -p volant-protocol corpus_smoke` |
| `fuzz` | `cargo +nightly fuzz run … -- -runs=${FUZZ_SMOKE_RUNS:-200}` |
| `all` | both |

CI does **not** install cargo-fuzz.

### CI workflow

`.github/workflows/ci.yml`:

1. `cargo test --workspace --all-targets --no-fail-fast` (stable; skips doctests)
2. Explicit `cargo test -p volant-protocol corpus_smoke -- --nocapture`

Timeout capped at 45 minutes for the whole job (normal unit/integration suite,
not fuzz time).

## Exit criteria

1. `cargo test -p volant-protocol corpus_smoke` green on stable
2. Seed files present under `fuzz/corpus/`
3. `.github/workflows/ci.yml` present and sensible
4. `fuzz/README.md` + `scripts/fuzz_corpus_smoke.sh` document CI vs local paths
5. Docs: PHASE112_SPEC + ROADMAP / PHASE_HISTORY / ops / INDEX / README;
   prior “cargo-fuzz corpus CI” deferred notes closed for the **MVP subset**
6. Commit + push

## Honest limitations

- Smoke is **seed replay + short optional local runs**, not a security audit
- Native protocol only (`volant-protocol`); Kafka shim codecs not fuzzed here
- CI does not run libFuzzer mutation loops
- Full chaos-mesh / multi-hour campaigns remain deferred

## Test plan

```bash
cargo test -p volant-protocol corpus_smoke
cargo test -p volant-protocol chaos_
./scripts/fuzz_corpus_smoke.sh test
# optional if nightly + cargo-fuzz installed:
# FUZZ_SMOKE_RUNS=100 ./scripts/fuzz_corpus_smoke.sh fuzz
```

## Still deferred after this

- Multi-language clients
- Chaos-mesh / long fuzz campaigns / corpus minimization automation
- Multi-broker 2PC / session affinity / BROKER config fan-out
- Kafka wire fuzz targets
