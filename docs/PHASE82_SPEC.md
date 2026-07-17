# Phase 82 — AddOffsetsToTxn v4 (version ratchet)

## Goals

1. Raise **AddOffsetsToTxn** max from **0–3** to **0–4** (Apache Kafka max)
2. Accept flexible **v4** with the same wire framing as **v3** (compact + tags)
3. Response header **v1** for v3–4 (already true for v ≥ 3)
4. v0–3 paths unchanged
5. v5 → UnsupportedVersion with response header v1
6. Tests + docs honesty

## Non-goals

- Emitting **TRANSACTION_ABORTABLE** (123) on AddOffsetsToTxn (KIP-890)
- Real 2PC / prepared transaction state
- READ_COMMITTED / control markers
- Raising other txn APIs (InitProducerId / AddPartitionsToTxn / EndTxn)
- ApiVersions v4–5 / Fetch v14+ / multi-lang clients / cargo-fuzz CI

## Wire summary

Apache Kafka documents AddOffsetsToTxn **v4** as:

> Version 4 adds support for new error code TRANSACTION_ABORTABLE (KIP-890).

Request/response field layout is otherwise unchanged from flexible **v3**.

### Request (flexible v3+)

```
TransactionalId: COMPACT_STRING,
ProducerId: INT64,
ProducerEpoch: INT16,
GroupId: COMPACT_STRING,
TAG_BUFFER
```

### Response (flexible v3+)

```
ThrottleTimeMs: INT32,          # always 0 on Volant
ErrorCode: INT16,
TAG_BUFFER
```

**v4 delta vs v3:** none on the wire. Volant never returns error code
`TRANSACTION_ABORTABLE` (123). Semantics remain buffer-until-commit
(`ensure_txn_open` only; no durable offset-group binding beyond existing
deferred-offset path).

## Semantics (honest)

| Case | Behavior |
|------|----------|
| v4 success (open txn) | Same as v3 → error 0 |
| v4 unknown / fenced producer | Same as v3 → mapped idempotent error |
| v4 group ACL deny | GroupAuthorizationFailed (same as v3) |
| TRANSACTION_ABORTABLE | Never emitted |
| Classic v0–2 / flex v3 | Unchanged |

## Exit criteria

1. ApiVersions: AddOffsetsToTxn **0–4**
2. AddOffsetsToTxn **v4** success after InitProducerId
3. AddOffsetsToTxn **v4** error path never returns code 123
4. AddOffsetsToTxn **v3** still works
5. AddOffsetsToTxn **v5** → header v1 + UnsupportedVersion (35)
6. phase82 + phase31 / phase47 / phase62 / phase75 green after max-version updates
7. ROADMAP / README / ops / KAFKA_COMPAT honesty

## Honest limitations

- No TRANSACTION_ABORTABLE emission (KIP-890)
- ThrottleTimeMs always 0
- No real 2PC / abortable-txn defense beyond existing fencing maps
- Empty tag buffers only
- Buffer-until-commit transaction model unchanged
