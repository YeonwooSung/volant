# Phase 18 — Transactions MVP (binding)

## Goals

1. **Transactional id** on `InitProducerId` with epoch fencing for the same id
2. **BeginTxn / EndTxn** — open a transaction, then commit or abort
3. **Broker-side buffer** — transactional produces stay off-log until commit
4. **Multi-partition atomicity** — commit flushes all buffered batches or none (abort)
5. **Deferred offset commits** inside a transaction (apply on commit only)
6. Client helper + tests + docs honesty

## Non-goals

- Kafka transaction coordinator / control markers / `READ_COMMITTED` fetch filtering
- Persistent in-flight txn recovery across broker crash (crash ≡ abort)
- Cross-broker distributed transactions / 2PC
- SCRAM / mTLS / Kafka shim

## Semantics

### InitProducerId

Request trailer (backward compatible):

```
transactional_id: string   # empty = plain idempotent PID (Phase 10)
```

- Empty id: allocate new PID, epoch 0 (unchanged).
- Non-empty id: fence prior owner of that id (bump epoch, clear open txn +
  sequences), return same or new PID with new epoch. Only the latest epoch
  may begin transactions / produce.

### BeginTxn

```
producer_id: u64
producer_epoch: u16
```

- PID must exist and be transactional (allocated with non-empty transactional_id).
- Epoch must match.
- No nested txns: already-open → `InvalidTxnState` (22).

### Produce (transactional)

When the PID has an **open** transaction:

1. Run normal idempotent sequence checks against last **committed** sequences
   (buffered batches also advance the in-txn expected sequence).
2. **Do not** append to the partition log.
3. Buffer `(topic, partition, messages, base_sequence)` on the broker.
4. Response: `base_offset = 0`, `count = N`, `error_code = 0`.
   Final log offsets are only assigned at **commit** (returned on EndTxn).

Non-transactional PIDs and produces without an open txn behave as Phase 10/11.

Producing with a transactional PID **without** BeginTxn → `InvalidTxnState`.

### OffsetCommit (transactional)

When `member_id` is empty and generation is `0` **or** when the commit is
associated via EndTxn pending list:

Phase 18 wires deferred offsets through **EndTxn request trailer**:

```
offset_count: u32
  for each: group_id, topic, partition, offset, metadata
```

Applied **only** on successful commit, after all produce flushes.

(Direct OffsetCommit during an open txn is unchanged / not deferred — clients
should use the EndTxn trailer or `TransactionalProducer::commit_offsets`.)

### EndTxn

```
producer_id: u64
producer_epoch: u16
committed: u8          # 1 = commit, 0 = abort
# optional trailer: deferred offsets (see above)
```

**Commit:**

1. Append each buffered batch to its partition (acks path as normal produce).
2. Record idempotent state for each batch.
3. Apply deferred offset commits.
4. Clear open txn; return per-batch results.

**Abort:**

1. Drop buffer and deferred offsets.
2. Clear open txn (sequences for uncommitted batches are discarded; next txn
   continues from last **committed** sequence).

Response:

```
error_code: u16
result_count: u32
  for each: topic, partition, base_offset, count
```

(Abort returns `result_count = 0`.)

## Error codes

| Code | Name |
|------|------|
| 22 | `InvalidTxnState` |
| 23 | `InvalidProducerEpoch` (existing 19 also used) |

## Client

```rust
ClientConfig { transactional_id: Some("app-1".into()), enable_idempotence: true, .. }
// or
let mut tp = TransactionalProducer::init(client, "app-1").await?;
tp.begin().await?;
tp.produce("events", Some(0), msgs).await?;
tp.produce("events", Some(1), msgs2).await?;
tp.add_offsets("cg", entries)?;
let results = tp.commit().await?; // Vec<ProduceResult>
// or tp.abort().await?;
```

## CLI

Optional smoke:

```bash
volant txn produce --transactional-id app-1 \
  --topic events --partition 0 --value a \
  --topic events --partition 1 --value b
```

(Single begin → produces → commit.)

## Exit criteria

1. Commit makes multi-partition produces visible atomically to fetch
2. Abort leaves no records from the aborted txn
3. Fencing: second InitProducerId with same transactional_id invalidates old epoch
4. Deferred offsets applied only on commit
5. `cargo test --workspace` green

## Honest limitations

- In-flight txn state is **memory-only**; broker crash aborts open txns
- No `READ_COMMITTED` isolation (uncommitted data never hits the log)
- No Kafka control records / aborted-txn lists on fetch
- Single-node coordinator only (no dedicated txn coordinator partition)
- Produce-in-txn responses do not carry final log offsets (see EndTxn results)
