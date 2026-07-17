# Phase 4 — Docs + E2E Stream Tests Plan (iteration 1)

## Goal

Document Phase 4 and ship stream e2e / integration tests for Volant. Fill
gaps in `volant-stream` so offline word-count and optional live e2e pass.
Update ROADMAP/README to mark Phase 4 complete and Phase 5 next.

Binding: **[docs/PHASE4_SPEC.md](../PHASE4_SPEC.md)**.

## Deliverables

| Item | Path / action |
|------|----------------|
| Plan | `docs/phase4/docs-e2e-plan.md` (this file) |
| Spec | `docs/PHASE4_SPEC.md` (exists; polish if needed) |
| Review | `docs/phase4/docs-e2e-review.md` |
| Offline word-count | `crates/volant-stream/tests/e2e_word_count.rs` |
| Optional live e2e | same file, `#[tokio::test]` boot broker |
| Example | `crates/volant-stream/examples/word_count.rs` |
| ROADMAP | Phase 4 ✅, Phase 5 next |
| README | programming model + how to run word-count |

## Stream crate gaps (status at plan time)

| Module | Required | Status | Action |
|--------|----------|--------|--------|
| `operator` | trait + `punctuate` | trait only, no punctuate | extend |
| `pipeline` | process + punctuate | process only | extend |
| `ops/*` | map/filter/flat_map/foreach/reduce | missing | implement |
| `state` | `KeyValueStore` + `MemoryStore` | missing | implement |
| `window` | tumbling window | missing | implement |
| `source` / `sink` | topic adapters | missing | implement |
| `topology` | `StreamBuilder` | missing | implement |
| `runtime` | at-least-once run loop | missing | implement |
| unit/e2e tests | offline + optional live | missing | add |
| example binary | word-count | missing | add |

## Test strategy

### Offline (required)

File: `crates/volant-stream/tests/e2e_word_count.rs`

1. Build pipeline: `flat_map` (split line → words) → `reduce` (count by key)
2. Feed several text lines as `Record`s (no broker)
3. Assert final counts for known words (`the`, `quick`, …)
4. Unit-style coverage for map/filter/foreach and tumbling window emit

### Live e2e (optional, preferred if green)

Same file, async:

1. Boot broker on `127.0.0.1:0` with unique temp `data_dir`
2. Create topics `lines`, `counts`
3. Produce lines via client
4. Run topology briefly (poll → process → sink → commit)
5. Fetch `counts` and assert word totals

## ROADMAP / README

- Phase 4 milestones checked when green
- Exactly-once / RocksDB / WASM remain non-goals / stretch
- Next phase pointer → Phase 5 DMA / high-performance I/O
- README: Phase 4 section, programming model snippet, word-count runbook

## Implementation order

1. Extend `Operator` / `Pipeline` (`punctuate`)
2. Stateless ops + state store + reduce + tumbling window
3. Source / sink / topology / runtime
4. Offline + live e2e tests
5. Example binary
6. Docs (ROADMAP, README, review)
7. `cargo test --workspace`

## Non-goals

Exactly-once / transactions, WASM plugins, RocksDB, distributed stream
workers, hopping windows, CLI `volant stream word-count`.

## Success criteria

- Offline word-count pipeline test passes
- Optional live e2e passes (or documented skip with reason)
- `cargo test --workspace` green
- ROADMAP Phase 4 complete / Phase 5 next
- README documents how to run word-count
