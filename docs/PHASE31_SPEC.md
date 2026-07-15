# Phase 31 — Kafka transactions on the shim

## Goals

1. **Transactional produce path** on `--kafka-listen` using Volant Phase 18
   buffer-until-commit semantics
2. Wire **AddPartitionsToTxn** (24), **AddOffsetsToTxn** (25), **EndTxn** (26),
   **TxnOffsetCommit** (28)
3. **FindCoordinator v1** with `key_type` (group=0, transaction=1)
4. InitProducerId with non-empty `transactional_id` + transactional Produce
   (RecordBatch PID/epoch/sequence) buffers off-log until EndTxn commit
5. Tests + docs honesty

## Non-goals

- Kafka control batches / `WriteTxnMarkers` / `READ_COMMITTED` fetch filtering
- Persistent in-flight txn recovery (crash ≡ abort, same as Phase 18)
- Flexible (compact) versions of txn APIs
- Cross-broker distributed transactions / 2PC
- Tracking per-partition “in-txn” membership beyond open/closed state

## Client flow (no BeginTxn API)

Kafka has no separate BeginTxn. The shim maps:

```
InitProducerId(transactional_id)
  → allocate/fence PID (Phase 18)

AddPartitionsToTxn / AddOffsetsToTxn
  → ensure_txn_open(PID, epoch)   # first call opens; later are no-ops

Produce (RecordBatch with PID)
  → buffer_txn_produce (off-log; base_offset=0 in response)

TxnOffsetCommit
  → buffer_txn_offsets on OpenTxn (applied only on commit)

EndTxn(committed=true|false)
  → end_txn: flush batches + deferred offsets, or drop on abort
```

## Wire (classic / non-flexible)

### FindCoordinator (10) v0–1

Request v1: `key: STRING`, `key_type: INT8` (`0` group, `1` transaction).  
Response v1: `throttle_time_ms`, `error_code`, `error_message` (nullable),
`node_id`, `host`, `port`.

Both key types return this broker’s advertised address (single-node coordinator).

### AddPartitionsToTxn (24) v0

```
transactional_id: STRING
producer_id: INT64
producer_epoch: INT16
topics: [{ name: STRING, partitions: [INT32] }]
```

Response: `throttle_time_ms`, per-topic per-partition `error_code`.

Calls `Broker::ensure_txn_open`. Partition membership is not tracked separately;
any produce under the open txn is buffered.

### AddOffsetsToTxn (25) v0

```
transactional_id, producer_id, producer_epoch, group_id
```

Response: `throttle_time_ms`, `error_code`.

Also ensures the txn is open (idempotent). Actual offsets arrive via
TxnOffsetCommit.

### EndTxn (26) v0

```
transactional_id, producer_id, producer_epoch, committed: BOOLEAN
```

Response: `throttle_time_ms`, `error_code`.

Maps to `Broker::end_txn(..., offsets=[])`. Deferred offsets from
TxnOffsetCommit are applied on commit only.

### TxnOffsetCommit (28) v0

```
transactional_id, group_id, producer_id, producer_epoch
topics: [{ name, partitions: [{ partition, offset, metadata }] }]
```

Response: `throttle_time_ms`, per-partition `error_code`.

Buffers into `OpenTxn.deferred_offsets`; applied in EndTxn commit after
produce flushes.

### Produce (transactional PID)

When `is_transactional_producer(pid)`:

1. `buffer_txn_produce` (requires open txn)
2. Response base offset `0` (final log offsets assigned at commit)
3. Without open txn → `INVALID_TXN_STATE` (48)

Non-transactional idempotent Produce remains Phase 29.

## Error mapping

Same as Phase 29 (`map_idempotent_error`):

| Volant | Kafka |
|--------|-------|
| InvalidProducerEpoch | 47 |
| OutOfOrderSequence | 45 |
| UnknownProducerId | 59 |
| InvalidTxnState | 48 |

## ACL

- AddPartitionsToTxn / EndTxn: Cluster `Write` (and per-topic Write for
  partition results when ACLs on)
- AddOffsetsToTxn / TxnOffsetCommit: Group `Read`
- InitProducerId unchanged (Cluster Write)

## Exit criteria

1. ApiVersions advertises 24, 25, 26, 28 and FindCoordinator max 1
2. Commit path: AddPartitions → Produce → EndTxn(true) → records visible
3. Abort path: Produce buffered then EndTxn(false) → log empty
4. TxnOffsetCommit offsets appear only after EndTxn commit
5. Transactional produce without AddPartitions → error 48
6. `cargo test --workspace` green

## Honest limitations

- No control markers / aborted-txn filtering on Fetch (`READ_COMMITTED` ≈ HWM)
- Crash drops open txns (abort)
- `transaction_timeout_ms` still ignored
- AddPartitions does not enforce a closed partition set for later produces
- No flexible versions; classic framing only
- MessageSet cannot carry transactional PID metadata
