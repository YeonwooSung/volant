# Phase 75 — KIP-890-era transaction API max versions

## Goals

1. Raise transaction API max versions for modern client negotiation with
   **honest shallow semantics** (buffer-until-commit unchanged):
   - **InitProducerId** 0–**5** (NOT 6 — 2PC deferred)
   - **AddPartitionsToTxn** 0–**5** (v4–5 batch wire)
   - **EndTxn** 0–**5** (v5 response pid/epoch echo)
   - **TxnOffsetCommit** 0–**5** (name path; TopicId is Phase 76)
2. **AddOffsetsToTxn** stays **0–3**
3. Classic v0–2 / Phase 62 flex paths unchanged
4. Error codes: `ProducerFenced = 90`, `TransactionAbortable = 123` (defined;
   not required to emit)
5. Tests + docs honesty

## Non-goals

- InitProducerId **v6** (OngoingTxn* / 2PC)
- Real resume of ProducerId/Epoch on InitProducerId v3–5 (parsed, ignored)
- Real `TRANSACTION_ABORTABLE` emission / KIP-890 abortable semantics
- TxnOffsetCommit **v6 TopicId** (Phase 76)
- Real `VerifyOnly` semantics on AddPartitionsToTxn (always add path)
- Control markers / READ_COMMITTED fetch filter
- Raising AddOffsetsToTxn beyond 0–3

## Wire summary

### InitProducerId v3–5

**Request** (flexible; same as v2 plus resume fields after timeout):

```
TransactionalId (compact nullable), TransactionTimeoutMs,
ProducerId (i64), ProducerEpoch (i16), tags
```

Resume fields are **parsed and ignored** — always allocate via
`broker.init_producer_id_with_txn` (same as v0–2).

**Response** (unchanged from v2): throttle, error, producer_id, producer_epoch,
tags. No OngoingTxn* fields (those are v6).

v6 → UnsupportedVersion.

### EndTxn v4–5

**Request** wire same as v3: transactional_id, pid, epoch, committed, tags.

**Response:**

| Version | Shape |
|--------:|-------|
| v3–4 | throttle, error_code, tags |
| **v5** | throttle, error_code, **ProducerId (i64)**, **ProducerEpoch (i16)**, tags |

v5 echoes request pid/epoch on success and on error (request values).

v6 → UnsupportedVersion.

### AddPartitionsToTxn v4–5 (batch shape)

**Request:**

```
Transactions[{
  TransactionalId, ProducerId, ProducerEpoch, VerifyOnly,
  Topics[{ Name, Partitions[] }], tags
}], tags
```

**Response:**

```
Throttle, ErrorCode (top-level),
ResultsByTransaction[{
  TransactionalId,
  TopicResults[{ Name, ResultsByPartition[{ Partition, Error, tags }], tags }],
  tags
}], tags
```

- **VerifyOnly=true**: still runs `ensure_txn_open` / auth; no separate verify
  logic (honest: always "add" path).
- v0–3 path unchanged (flat V3AndBelow fields).
- Empty tags only.

v6 → UnsupportedVersion.

### TxnOffsetCommit v4–5

Wire **identical to v3** (name-based topics). Raise max to 5; handle 4–5 with
the same code path as v3. TopicId (v6) deferred to Phase 76.

### Error codes

| Code | Name | Notes |
|-----:|------|-------|
| 90 | ProducerFenced | Defined; may map fencing later |
| 123 | TransactionAbortable | KIP-890; defined for honesty, not emitted yet |

## Exit criteria

1. ApiVersions maxes: Init **5**, AddPartitions **5**, EndTxn **5**;
   AddOffsets stays **3**. TxnOffsetCommit name path through **v5** in this
   phase; max raised to **6** by Phase 76 TopicId.
2. InitProducerId v5 with ProducerId/Epoch fields succeeds
3. EndTxn v5 response includes pid/epoch
4. AddPartitionsToTxn v4 batch (single transaction) succeeds
5. TxnOffsetCommit v5 name path works
6. Init/AddPartitions/EndTxn **v6** and TxnOffsetCommit **v7** → header v1 +
   UnsupportedVersion (35)
7. phase75 + phase62 + phase47 green

## Honest limitations

- Same buffer-until-commit semantics as classic (crash ≡ abort)
- InitProducerId resume fields ignored (always re-allocate)
- VerifyOnly ignored (always add)
- No TRANSACTION_ABORTABLE emission
- No 2PC / InitProducerId v6
- No TxnOffsetCommit TopicId
- No READ_COMMITTED
- Empty tag buffers only
