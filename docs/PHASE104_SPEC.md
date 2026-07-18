# Phase 104 — Aborted soft-marker GC with DeleteRecords (MVP)

## Goals

1. When **DeleteRecords** truncates a partition (and when **retention** drops
   segments), **drop aborted soft markers** for that `(topic, partition)` whose
   range is entirely below the new log start.
2. Persist updated `__txn_markers` after GC.
3. Do **not** remove markers that still overlap live log offsets (needed for
   `READ_COMMITTED` aborted list / LSO filtering).
4. **Self-heal on load**: after loading markers, GC any fully below current log
   start (crash / older files).
5. Keep control-batch log data as-is (do not rewrite history); this phase is
   about **soft markers** only.
6. Metrics: `volant_aborted_markers_gc_total` counter (consistent with Phase 97
   style).
7. Tests (`phase104_marker_gc.rs`) + living docs honesty.

## Non-goals

- Rewriting / compacting Kafka control batches on the data log
- Truncating partially-overlapping markers (keep whole marker if any overlap)
- Multi-broker marker consensus / fan-out
- Empty AddPartitions control markers → **closed by Phase 105**
- Graceful sweeper join on stop
- Multi-lang clients / fuzz CI / multi-broker 2PC

## Problem (Phase 86 honesty gap)

Soft abort markers for `READ_COMMITTED` (`aborted_txns` + durable
`__txn_markers`) grow as transactions abort. After log truncation via
DeleteRecords (or retention that drops old offsets), markers whose ranges are
entirely below the new log start are useless but were still retained — unbounded
memory + durable file growth.

Phase 86 noted: *“DeleteRecords may leave stale aborted markers…”* — closed here.

## Design

### Marker range

Soft markers cover **`[first_offset, end_offset)`** (end exclusive), same as
Phase 86.

### GC rule

| Condition | Action |
|-----------|--------|
| `end_offset <= log_start` | **Drop** marker (no overlap with live log) |
| `end_offset > log_start` | **Retain** whole marker (may still cover live offsets) |

`log_start` is the partition’s **actual** low watermark after truncate (whole
sealed segments only — Phase 14), not the client’s requested `before_offset`
when no segments were dropped.

### Hooks

| Path | Behavior |
|------|----------|
| `Broker::delete_records` | After successful log delete, GC that partition; persist if any dropped |
| `Broker::apply_retention_all` | After retention on all topics, GC all partitions vs current log starts |
| `Broker::load_txn_markers` | After load (+ crash open→abort promote), GC all partitions (self-heal) |

### Persistence

`persist_txn_markers()` rewrites `{data_dir}/__txn_markers/state.json` without
the dropped aborted ranges (open ranges unchanged by GC).

### Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `volant_aborted_markers_gc_total` | counter | Markers removed by DeleteRecords / retention / load GC |

Also: `Broker::aborted_markers_gc_total()` and `Broker::aborted_marker_count`
for tests/ops.

## Exit criteria

1. Abort → markers present → DeleteRecords past marker range → markers gone
   (memory + durable reload)
2. Abort → DeleteRecords that does **not** cover marker end → markers retained
3. `READ_COMMITTED` aborted list empty after GC; fresh aborts still listed
4. DeleteRecords without markers still works (regression)
5. Load self-heals stale durable markers below log start
6. `phase104_*` + phase14 / phase86 / phase89 green
7. Docs: PHASE104_SPEC + living docs / ROADMAP / README status ceiling

## Honest limitations

- Whole-segment DeleteRecords only (Phase 14) — GC advances only when log start
  actually moves past `end_offset`
- Partially overlapping markers are **not** truncated to the live prefix
- Control batches on the log are **not** rewritten or GC’d
- Single-node marker store; no multi-broker replication of GC
- Retention GC is opportunistic (same 5s background path as Phase 13)

## Test plan

`crates/volant-broker/tests/phase104_marker_gc.rs`:

1. DeleteRecords past marker → memory empty + durable reload empty + GC counter
2. DeleteRecords not past marker → retained
3. DeleteRecords without markers → still works; GC counter unchanged
4. Load injects stale marker below log start → self-heal drop
5. Partial overlap / progressive delete retains when `end > log_start`
6. Kafka Fetch READ_COMMITTED aborted list clears after GC; new abort listed

## Phase 105 ideas

- Graceful sweeper shutdown / join on server stop
- Empty-AddPartitions control markers → **closed by Phase 105**
- Multi-broker config fan-out / multi-broker 2PC
- Multi-lang clients / cargo-fuzz corpus CI
