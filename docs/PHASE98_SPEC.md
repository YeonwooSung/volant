# Phase 98 — Control batches for crash≡abort open txns (MVP)

## Goals

1. On broker load, when promoting stored **open** write-through ranges to
   **aborted** soft markers (crash recovery), also append **ABORT control
   RecordBatches** to the affected partitions — the same dual-write as EndTxn
   abort / Phase 89 — using stored `producer_id` / `producer_epoch` from the
   marker snapshot when available.
2. Extend `__txn_markers` open-range schema with optional `producer_epoch` so
   control batches can be re-encoded after crash. Upgrade path for missing
   fields: best-effort producer-state epoch lookup; if still unknown, skip
   control batch (soft abort remains).
3. Isolation unchanged: READ_COMMITTED filters aborted data; control batches
   visible under RU/RC per Phase 89; native committed-only hides control markers.
4. Empty AddPartitions-only (no write-through ranges) still no control batch in
   Phase 98 → **closed by Phase 105** (membership + `open_added` persistence).
5. **Idempotent reload:** only append when promoting open→aborted; open list is
   cleared after promotion so a second restart does not re-append forever.
6. Tests (`phase98_*.rs`) + living docs honesty.

## Non-goals

- Multi-broker marker consensus
- Multi-lang / fuzz CI
- Control batches for empty AddPartitions without data → **closed by Phase 105**
- Full KRaft txn log
- Reconstructing control batches for pre-existing aborted soft markers that
  never got EndTxn control frames before Phase 98 shipped

## Design

### Dual-write on crash promote (extends Phase 89)

| Layer | Role |
|-------|------|
| Soft markers (`__txn_markers/state.json`) | Crash recovery SoT for LSO / aborted list / READ_COMMITTED filter |
| Control marker records on partition log | Kafka-shaped ABORT frames for clients that inspect markers |

### Schema (`StoredTxnRange`)

```json
{
  "open": [
    {
      "producer_id": 1,
      "producer_epoch": 0,
      "topic": "t",
      "partition": 0,
      "first_offset": 0,
      "end_offset": 1
    }
  ],
  "aborted": [ /* epoch optional / omitted */ ]
}
```

| Field | Open ranges | Aborted ranges |
|-------|-------------|----------------|
| `producer_id` | required | required |
| `producer_epoch` | **Phase 98** optional i16/u16; written on persist | omitted |
| topic / partition / first / end | as Phase 86 | as Phase 86 |

**Upgrade:** pre-Phase-98 JSON without `producer_epoch` deserializes as
`None`. Recovery order for epoch:

1. Stored open-range `producer_epoch` if present
2. Live `producer_state` epoch for that pid (best-effort)
3. Skip control batch (soft abort still applied)

`OpenTxn` also retains `producer_epoch` in memory (set at `begin_txn`) so
persist always has the field for new open writes.

### Load path (`load_txn_markers`)

```text
1. Load aborted soft markers into memory
2. Promote every stored open range → soft aborted
3. If open list non-empty:
     group by producer_id → append_txn_control_markers(ABORT)
4. Persist cleaned snapshot (open = [], aborted includes promoted)
```

Idempotency: step 3 only runs when the on-disk **open** list is non-empty.
After step 4, a subsequent restart sees empty open → no re-append.

### Semantics table

| Case | Behavior |
|------|----------|
| Crash with open write-through | Soft promote open→aborted **+** ABORT control per written partition |
| Second restart after promote | Soft markers only reload; **no** extra control batches |
| EndTxn abort / commit | Unchanged (Phase 89 dual-write on finalize) |
| Empty open (AddPartitions only, no produce) | Phase 98: nothing; **Phase 105**: ABORT control via `open_added` |
| Pre-98 open snapshot, pid still in producer_state | Soft abort + ABORT control (best-effort epoch) |
| Pre-98 open snapshot, pid unknown | Soft abort only (no control batch) |
| Fetch RU/RC | Control batches visible; aborted data filtered under RC |
| Native `Broker::fetch` | Hide open + aborted + control markers |

## Exit criteria

1. Open write-through + drop/reload → partition log has ABORT control; soft
   aborted list non-empty; READ_COMMITTED hides data, shows control
2. Second reload does not duplicate control markers
3. EndTxn abort/commit paths unchanged (single control per finalize)
4. Empty open (no written ranges) invents no control batch in Phase 98;
   empty **AddPartitions membership** control closed by **Phase 105**
5. Legacy open markers without epoch still recover via producer_state when possible
6. `cargo test` green for phase86 + phase89 + phase98 + prior broker phase tests

## Honest limitations

- Control markers for empty AddPartitions → **closed by Phase 105**
- Pre-98 open files without epoch and without producer_state may still lack control batches
- Partial crash mid-control-append before open list is cleared could theoretically
  re-append on next restart (MVP; rare; soft markers remain correct)
- Coordinator epoch always 0; no multi-broker txn log / marker consensus
- Aborted soft markers still not compacted with log deletes
- No reconstruction of historical crash-aborts that predate Phase 98

## Phase 99 ideas

- Control batches for empty AddPartitions (coordinator-only partitions) → **closed by Phase 105**
- Marker compaction / GC with DeleteRecords → **closed by Phase 104**
- Stronger crash-promote idempotency (e.g. promote flag / content-hash before append)
- Admin / DescribeConfigs for txn timeout + sweep knobs → **closed by Phase 99**
- Multi-broker marker consensus / KRaft-shaped txn log
- Graceful sweeper shutdown → **closed by Phase 106**
