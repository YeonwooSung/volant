# Phase 111 — Clip straddling soft abort markers to log_start (MVP)

## Goals

1. When GC runs (DeleteRecords, retention, load self-heal), for soft abort
   markers with `first_offset < log_start < end_offset`, **clip**
   `first_offset = log_start` (keep `end_offset`). Only tighten the soft range;
   do not invent fake data.
2. Fully below (`end_offset <= log_start`) still **drop** entirely (Phase 104).
3. Fully live (`first_offset >= log_start`) unchanged.
4. Persist `__txn_markers` when any clip or drop occurs.
5. `volant_aborted_markers_gc_total` continues to count **drops only** (Phase 104
   semantics preserved; clips do not increment).
6. READ_COMMITTED aborted list / LSO behavior remains correct for the remaining
   live range.
7. Tests (`phase111_straddle_marker_clip.rs`) + living docs honesty.

## Non-goals

- Rewriting / compacting Kafka control batches on the data log
- Splitting one marker into multiple markers
- Multi-broker marker consensus / fan-out
- Separate clip metric (optional later; not required for MVP)
- Multi-lang clients / fuzz CI / multi-broker 2PC

## Problem (Phase 104 honesty gap)

Phase 104 GC'd soft abort markers only when **fully** below log start
(`end_offset <= log_start`). Markers that **straddled** log_start
(`first_offset < log_start < end_offset`) were retained **whole**, so durable
`__txn_markers` / memory still held an obsolete prefix that no longer exists on
the log. READ_COMMITTED still worked (filtering offsets below log start is
harmless), but markers never shrank until fully GC'd.

## Design

### Marker range

Unchanged from Phase 86/104: soft markers cover **`[first_offset, end_offset)`**
(end exclusive).

### GC / clip rule

| Condition | Action |
|-----------|--------|
| `end_offset <= log_start` | **Drop** marker (Phase 104) |
| `first_offset < log_start < end_offset` | **Clip** `first_offset = log_start` (this phase) |
| `first_offset >= log_start` | **Unchanged** |

`log_start` is the partition’s **actual** low watermark after truncate (whole
sealed segments only — Phase 14), not the client’s requested `before_offset`
when no segments were dropped.

### Hooks

Same call sites as Phase 104:

| Path | Behavior |
|------|----------|
| `Broker::delete_records` | After successful log delete, GC/clip that partition; persist if mutated |
| `Broker::apply_retention_all` | After retention, GC/clip all partitions vs current log starts |
| `Broker::load_txn_markers` | After load (+ crash open→abort promote), GC/clip all partitions |

### Persistence

`persist_txn_markers()` rewrites `{data_dir}/__txn_markers/state.json` with
clipped `first_offset` values and without fully dropped ranges.

### Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `volant_aborted_markers_gc_total` | counter | Markers **fully dropped** (Phase 104). Clips do **not** increment. |

Also: `Broker::aborted_markers_gc_total()`, `Broker::aborted_marker_count`,
`Broker::aborted_marker_ranges` (test helper for clip assertions).

## Exit criteria

1. Abort multi-offset range → DeleteRecords into middle → marker retained with
   `first_offset == log_start`, `end_offset` unchanged; still listed for live
   remainder
2. Full GC still drops when `log_start >= end_offset`; drop counter advances
3. Durable reload preserves clip
4. Load self-heals straddling durable markers by clipping
5. Fully live markers unchanged; Phase 104 full-drop / retain-when-not-past still green
6. `phase111_*` + `phase104_*` + phase86 smoke green
7. Docs: PHASE111_SPEC + living docs / ROADMAP / README status ceiling

## Honest limitations

- Whole-segment DeleteRecords only (Phase 14) — clip advances only when log
  start actually moves into a marker range
- Control batches on the log are **not** rewritten or GC'd
- Single-node marker store; no multi-broker replication of GC/clip
- No dedicated clip counter (ops still see drop-only metric)
- Retention GC/clip is opportunistic (same 5s background path as Phase 13)

## Test plan

`crates/volant-broker/tests/phase111_straddle_marker_clip.rs`:

1. Straddle DeleteRecords → clip first_offset to log_start; end unchanged;
   READ_COMMITTED list uses clipped first; drop counter unchanged
2. Clip persists across broker reload (`__txn_markers`)
3. Full GC still drops when log_start past end; counter advances
4. Load injects straddling durable marker → self-heal clip
5. Fully live marker unchanged after non-overlapping delete

Regression: `phase104_marker_gc` remains green (full drop / retain / load heal).

## Still deferred after this

- Multi-broker 2PC / multi-lang / fuzz CI
- Multi-broker BROKER config fan-out
- Session affinity / durable sessions
- Control-batch log rewrite / compaction
