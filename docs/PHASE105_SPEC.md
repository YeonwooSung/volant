# Phase 105 — Control batches for empty AddPartitions (MVP)

## Goals

1. Track partitions registered via **AddPartitionsToTxn** (membership) even when
   no produce write-through occurs on them.
2. On **EndTxn commit/abort**, **fence**, **timeout auto-abort**, and
   **crash≡abort open promote** (Phase 98 path), append Kafka **control
   RecordBatches** for **all** registered partitions — including empty ones.
3. Soft abort markers: for empty partitions, **control batch only** — do **not**
   invent fake data ranges / soft markers (nothing to filter under READ_COMMITTED).
4. Keep existing dual-write for partitions with written data (soft + control).
5. Tests (`phase105_empty_add_partitions_control.rs`) + living docs honesty.
6. No multi-broker 2PC expansion.

## Non-goals

- Multi-broker marker consensus / KRaft txn log
- Multi-lang clients / cargo-fuzz CI
- Graceful sweeper join on stop → **closed by Phase 106**
- Multi-broker BROKER config fan-out
- Reconstructing control batches for historical empty membership that predate
  Phase 105 (no `open_added` on disk)

## Problem

Phases 89/98 dual-write control batches keyed off **written ranges** only.
A transactional producer that called AddPartitionsToTxn but never produced to a
partition would see EndTxn / crash-abort omit COMMIT/ABORT control frames for
that partition. Soft markers also skipped empty partitions. Clients / tools that
inspect control markers on all txn partitions saw honesty gaps.
Phase 98 test `unit_begin_only_no_control_on_crash` (formerly
`unit_add_partitions_only_no_control_on_crash`) documented begin-without-membership;
true empty-AddPartitions control is closed here.

## Design

### Membership on `OpenTxn`

```text
OpenTxn {
  ...
  added: Vec<(topic, partition)>,  // Phase 105
  written: Vec<TxnWrittenRange>,   // Phase 86
  ...
}
```

| Source | Effect |
|--------|--------|
| AddPartitionsToTxn success | `record_txn_added_partitions` appends to `added` (idempotent) |
| Write-through produce | `written` as today (control still covers via written) |
| EndTxn / fence / timeout | `append_txn_control_markers` = **union** of written + added (dedup) |
| Soft abort | **written only** (empty membership → no soft range) |

### Durable snapshot (`__txn_markers/state.json`)

```json
{
  "open": [ /* written ranges (Phase 86/98) */ ],
  "open_added": [
    {
      "producer_id": 1,
      "producer_epoch": 0,
      "topic": "t",
      "partition": 0
    }
  ],
  "aborted": [ /* soft markers */ ]
}
```

| Field | Role |
|-------|------|
| `open` | Write-through ranges; crash → soft aborted + ABORT control |
| `open_added` | Empty membership only (no written range for that partition); crash → **ABORT control only** |
| `aborted` | Soft markers (unchanged) |

Pre-Phase-105 files omit `open_added` → deserializes empty (no synthetic membership).

Partitions that both were added **and** written appear only under `open` (not
duplicated in `open_added`).

### Prepared txn snapshot

`StoredPreparedTxn.added` carries membership across prepare → complete so second
EndTxn still emits control for empty partitions.

### Control append (`append_txn_control_markers`)

1. One control per distinct `(topic, partition)` in `written`
2. Then one control per distinct entry in `added` not already covered
3. Soft markers still only from `written` (`record_aborted_from_txn`)

### Crash promote (`load_txn_markers`)

```text
1. Load aborted soft markers
2. Promote open written ranges → soft aborted
3. If open or open_added non-empty:
     group by producer_id → append_txn_control_markers(ABORT)
     (open_added → OpenTxn.added; open → OpenTxn.written)
4. Persist cleaned snapshot (open = [], open_added = [], aborted includes promoted)
```

Idempotency: open + open_added cleared after promote; second restart does not
re-append.

## Semantics table

| Case | Soft markers | Control batches |
|------|--------------|-----------------|
| AddPartitions only → EndTxn **abort** | none | ABORT per added partition |
| AddPartitions only → EndTxn **commit** | none | COMMIT per added partition |
| AddPartitions only → crash reload | none | ABORT per open_added |
| AddPartitions + produce → EndTxn abort | yes (written) | one ABORT (dedup written∪added) |
| begin_txn only (no add, no produce) → crash | none | none |
| Fence / open timeout with empty membership | none | ABORT per added |
| Produce without explicit AddPartitions (native) | yes if abort | control via written (unchanged) |

## Exit criteria

1. AddPartitions → EndTxn abort/commit → control present; no soft range for empty
2. AddPartitions → crash reload → ABORT control; second reload no duplicate
3. AddPartitions + produce → single control + soft marker (regression)
4. begin_txn without membership still invents no control
5. `cargo test` green for phase105 + phase98 + phase89 + phase86 smoke
6. Docs: PHASE105_SPEC + living docs / ROADMAP / README status ceiling

## Honest limitations

- Membership is recorded only when AddPartitionsToTxn succeeds (or unit API
  `record_txn_added_partitions`); native produce without AddPartitions still
  relies on written ranges only
- Pre-105 crash snapshots without `open_added` cannot reconstruct empty membership
- Partial crash mid-control-append before open lists cleared could theoretically
  re-append on next restart (MVP; rare; soft markers remain correct for written)
- Coordinator epoch always 0; no multi-broker txn log / marker consensus
- Control batches on the data log are still not GC'd with DeleteRecords (Phase 104
  is soft-marker only)

## Test plan

`crates/volant-broker/tests/phase105_empty_add_partitions_control.rs`:

1. AddPartitions only → EndTxn abort → ABORT control; soft aborted empty
2. AddPartitions only → EndTxn commit → COMMIT control
3. AddPartitions only → unit crash reload → ABORT control; idempotent second load
4. Wire AddPartitions only → crash reload → Fetch ABORT control
5. AddPartitions + produce → EndTxn abort → one control + soft marker
6. Produce without explicit AddPartitions still one control (regression)

Phase 98: `unit_begin_only_no_control_on_crash` keeps the zero-membership guard.

## Phase 106 ideas

- Graceful sweeper shutdown / join on server stop → **closed by Phase 106**
- Multi-broker 2PC / sessions / multi-lang / fuzz CI
- Stronger crash-promote idempotency (content-hash before append)
- Control-batch log GC with DeleteRecords (optional; clients rarely need it)
