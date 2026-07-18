# Phase 89 — Kafka control batches on the data log (MVP)

## Goals

1. On **EndTxn commit** and **abort**, append Kafka-style **control RecordBatch**(es)
   (magic 2, control + transactional attributes, COMMIT / ABORT marker type) to
   each partition that participated in the txn write-through ranges — durable on
   the partition log as Volant records that re-encode to real control frames.
2. **Dual-write** with Phase 86 soft markers (`__txn_markers`): soft markers remain
   the source of truth for LSO / READ_COMMITTED aborted-range filtering and crash
   recovery; control batches raise wire honesty for clients that inspect markers.
3. **Fetch re-encode path**: control batches visible under **READ_UNCOMMITTED** and
   **READ_COMMITTED** (Kafka clients skip them application-side); aborted *data*
   still filtered under READ_COMMITTED; MessageSet Fetch (v0–3) omits control
   markers (control frames are magic-2 only).
4. Native Volant fetch remains **committed-only** and **hides** control markers
   (not application data).
5. No new Produce/Fetch/txn API max versions.
6. Tests (`phase89_*.rs`) + living docs honesty.

## Non-goals

- Full multi-broker transaction log / KRaft txn coordinator
- Real 2PC / prepared transactions
- Multi-lang clients, cargo-fuzz CI
- Omit-unchanged fetch session cache → **closed by Phase 91 (MVP)**
- Writing control batches for partitions that were AddPartitionsToTxn'd but never
  produced to → **closed by Phase 105** (membership tracked; control on EndTxn/crash)
- Reconstructing missing control batches for crash≡abort of open txns that never
  reached EndTxn → **closed by Phase 98** (open promote also appends ABORT control)

## Design choice: dual-write (soft + control)

| Layer | Role |
|-------|------|
| Soft markers (`__txn_markers/state.json`) | Crash recovery, LSO, aborted list, READ_COMMITTED range filter |
| Control marker records on partition log | Durable Kafka-shaped COMMIT/ABORT frames on Fetch re-encode |

Filtering does **not** migrate fully to control-batch scan (would require
walking the log for every fetch). Soft markers stay authoritative for isolation.

## On-disk control marker (Volant record)

Stored as a normal Volant `Message` with:

| Field | Content |
|-------|---------|
| key | Kafka control key: `version:i16=0` + `type:i16` (`0=ABORT`, `1=COMMIT`) |
| value | Kafka control value: `version:i16=0` + `coordinator_epoch:i32=0` |
| headers | `volant.control=txn_marker`, `volant.txn.pid` (i64 BE), `volant.txn.epoch` (i16 BE), `volant.txn.marker` (`abort`/`commit`) |

Detection: header `volant.control == txn_marker`.

## Wire control RecordBatch (Fetch v4+)

```
attributes = CONTROL (0x20) | TRANSACTIONAL (0x10) = 0x30
magic = 2
producerId / producerEpoch from marker; baseSequence = -1
records_count = 1
record: key/value as above; no headers; offset_delta=0
```

Multiple data + control records in one Fetch window are re-encoded as
**interleaved** RecordBatches (data batch(es) + control batch per marker).
Control batches are **never compressed**.

## Semantics table

| Case | Behavior |
|------|----------|
| EndTxn **commit** | Soft: drop open ranges; append COMMIT control marker per written partition |
| EndTxn **abort** | Soft abort marker + append ABORT control marker per written partition |
| Fence (InitProducerId) | Open ranges → soft aborted + ABORT control markers (best-effort) |
| Crash open write-through | Soft promote open→aborted; **Phase 98** also appends ABORT control batches |
| Fetch READ_COMMITTED | Cap LSO; filter aborted data; **include** control markers as control batches |
| Fetch READ_UNCOMMITTED | All data + control markers up to HWM |
| Fetch MessageSet (v0–3) | Control markers omitted from record set |
| Native `Broker::fetch` | Hide open + aborted + control markers |
| Restart after EndTxn | Soft markers reloaded; control markers already on log |

## Exit criteria

1. Abort EndTxn: partition log has ABORT control marker; Fetch v4 re-encodes
   attributes with control bit; soft aborted list still non-empty
2. Commit EndTxn: COMMIT control marker on log; data visible under both isolations
3. Restart preserves control markers on log + soft markers
4. Isolation still correct (aborted data hidden under READ_COMMITTED)
5. `cargo test` green for phase86 + phase89 + prior broker phase tests

## Honest limitations

- Control markers for empty AddPartitions → **closed by Phase 105**
- Crash≡abort control batches closed by **Phase 98** (pre-98 gap: soft markers only)
- Fetch re-encode still omits producer metadata on **data** batches
- Coordinator epoch always 0; no transaction log / multi-broker marker consensus
- 2PC / prepared txn still absent (closed later by Phase 90 MVP)
- Aborted soft markers still not compacted with log deletes
