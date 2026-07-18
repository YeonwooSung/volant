# Phase 86 — True control-marker READ_COMMITTED (MVP)

## Goals

1. Differentiate **Fetch `isolation_level`**: `READ_UNCOMMITTED` (0) vs
   `READ_COMMITTED` (1) with honest, testable semantics
2. Move transactional produces from **buffer-until-commit** to **write-through**
   with soft **control markers** (aborted ranges + open-txn first offsets)
3. **READ_COMMITTED** Fetch:
   - Report **LSO ≠ HWM** while a transactional write is open on the partition
   - Cap returned records at LSO (hide unstable / in-flight txn data)
   - Exclude aborted transactional offset ranges
   - Return a **non-empty `aborted_transactions`** list when aborted ranges
     overlap the fetch window
4. **READ_UNCOMMITTED** Fetch: return all records up to HWM (including
   uncommitted and aborted data still on the log)
5. Keep deferred **TxnOffsetCommit** buffer-until-EndTxn
6. Native Volant fetch remains **committed-only** (filters open + aborted ranges)
7. Tests + docs honesty

## Non-goals

- Real Kafka **control batch** bytes on the partition log (magic-2 control
  records with `COMMIT`/`ABORT` types)
- Cluster replication of markers / KRaft transaction log
- Real 2PC / prepared transactions (InitProducerId v6 flags still ignored)
- Durable OffsetForLeaderEpoch history, fetch sessions, multi-lang clients,
  cargo-fuzz CI
- Client-side aborted filtering via transactional RecordBatch attributes
  (Volant Fetch re-encodes without producer metadata — broker filters)

## Wire / semantics table

| Case | Behavior |
|------|----------|
| Txn Produce (open txn) | Append to log immediately; response base_offset = real log offset |
| Open txn on partition | LSO = min first offset of open written ranges; HWM advances |
| EndTxn **commit** | Finalize sequences; ranges become stable; LSO advances to HWM (if no other open txns) |
| EndTxn **abort** | Soft abort marker `(producer_id, first_offset, end_offset)` retained; records stay on log |
| Fetch READ_COMMITTED | Records with `offset < LSO` and not in any aborted range; aborted list filled |
| Fetch READ_UNCOMMITTED | Records with `offset < HWM`; aborted list empty; includes unstable + aborted |
| ListOffsets isolation=1 latest | Return LSO (not HWM) when open txn holds LSO back |
| ListOffsets isolation=0 latest | HWM (unchanged) |
| Native `Broker::fetch` | Committed-only: hide open ranges + aborted ranges |
| Crash with open write-through | Open ranges recovered as **aborted** (crash ≡ abort) via `__txn_markers` (+ ABORT control batches in **Phase 98**) |
| Control batches on log | Soft markers only in Phase 86; EndTxn control in **Phase 89**; crash-promote control in **Phase 98** |

### Fetch partition response (v4+)

```
last_stable_offset: INT64   # true LSO (may be < HWM)
aborted_transactions: [{ producer_id: INT64, first_offset: INT64 }]
```

Empty aborted list when isolation is READ_UNCOMMITTED or no overlapping aborts.

## Implementation notes

- `OpenTxn.written`: per-batch ranges after write-through
- `aborted_txns`: `(topic, partition) → Vec<{producer_id, first_offset, end_offset}>`
- Persist open + aborted markers under `{data_dir}/__txn_markers/state.json`
- On broker load: promote any stored **open** ranges to **aborted**
- Fencing (`InitProducerId` same transactional id): open ranges → aborted

## Exit criteria

1. Open transactional produce: HWM advances; LSO stays at first unstable offset
2. READ_COMMITTED fetch during open txn: no unstable records; LSO < HWM
3. READ_UNCOMMITTED fetch during open txn: sees uncommitted records
4. Abort: READ_COMMITTED returns empty (or non-aborted only) + non-empty aborted list;
   HWM includes aborted data; LSO == HWM after abort
5. Commit: records visible under both isolations; LSO == HWM
6. Prior phase tests updated for write-through (base_offset ≠ 0; abort HWM)
7. `cargo test` green for broker phase tests + workspace

## Honest limitations

- Soft markers were isolation SoT at ship; Kafka control batches added later
  (EndTxn: **Phase 89**; crash open promote: **Phase 98**)
- Fetch re-encode omits transactional attributes / producer id on batches
- Aborted markers retained in memory + JSON file until GC with log deletes
  (**closed by Phase 104** — DeleteRecords / retention / load drop markers with
  `end_offset <= log_start`)
- Single-node coordinator; no cross-broker marker consensus
- 2PC / prepared txn still absent at Phase 86 ship (closed by Phase 90 MVP)
- DeleteRecords stale markers → **closed by Phase 104**
