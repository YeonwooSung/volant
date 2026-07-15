# Phase 29 — Kafka InitProducerId & idempotent Produce

## Goals

1. **InitProducerId** (API key 22) on `--kafka-listen` so modern clients can
   allocate a producer id + epoch
2. **Honor** RecordBatch `producerId` / `producerEpoch` / `baseSequence` on
   Produce (magic 2), mapping onto Volant's Phase 10/11 de-dupe path
3. Duplicate sequence → success with cached base offset (no re-append)
4. Tests + docs honesty

## Non-goals

- Full Kafka transactions on the shim (`BeginTxn`, `AddPartitionsToTxn`,
  `EndTxn`, `WriteTxnMarkers`, control batches)
- Idempotence for legacy MessageSet (magic 0/1)
- Flexible (compact) versions of InitProducerId
- Kafka SASL

## Wire

### InitProducerId (key 22, versions 0–1)

Request (classic):

```
transactional_id: NULLABLE_STRING   # empty/null = plain idempotent
transaction_timeout_ms: INT32      # ignored (Volant has no txn timeout)
```

Response:

```
throttle_time_ms: INT32 = 0
error_code: INT16
producer_id: INT64
producer_epoch: INT16
```

Maps to `Broker::init_producer_id_with_txn(transactional_id)`.
Non-empty transactional id fences prior owners (Phase 18 semantics) but
**Kafka transactional produce APIs are still absent** — transactional PIDs
will reject ordinary Produce with `INVALID_TXN_STATE` until a later phase.

ACL: Cluster `Write` when ACLs enabled (same spirit as native InitProducerId).

### Produce (RecordBatch magic 2)

From each batch header:

| Field | Non-idempotent | Idempotent |
|-------|----------------|------------|
| producerId | `-1` | ≥ 0 (Volant allocates from 1) |
| producerEpoch | `-1` | matching epoch |
| baseSequence | `-1` | ≥ 0 |

Flow per partition record-set batch:

1. Decode batch (existing compression path)
2. If `producerId < 0` or `baseSequence < 0` → ordinary `produce_with_acks`
3. Else `check_idempotent_produce` → Accept / Duplicate / Reject
4. Accept → append + `record_idempotent_produce`
5. Duplicate → return cached `base_offset` without append
6. Reject → Kafka error code (see below)

Multiple contiguous RecordBatches in one produce partition are processed in
order; response base offset is the first successful batch's base.

### Error mapping (Volant → Kafka)

| Volant | Kafka wire |
|--------|------------|
| 19 InvalidProducerEpoch | 47 |
| 20 OutOfOrderSequence | 45 |
| 21 UnknownProducerId | 59 |
| 22 InvalidTxnState | 48 |

## Encode helpers

- `encode_record_batch` remains non-idempotent (`producerId=-1`) for Fetch
- `encode_record_batch_idempotent(records, pid, epoch, base_seq)` for tests

## Exit criteria

1. InitProducerId returns stable pid/epoch; advertised in ApiVersions
2. Produce with matching sequences de-dupes (same base offset, one append)
3. Unknown PID / wrong epoch / out-of-order surface Kafka errors
4. Non-idempotent path (pid=-1) still green
5. `cargo test --workspace` green

## Honest limitations

- No Kafka transactions beyond InitProducerId fencing of transactional_id
- MessageSet produce cannot carry PID/sequence
- Volant reserves producer id `0` as non-idempotent (allocation starts at 1)
- transaction_timeout_ms ignored
