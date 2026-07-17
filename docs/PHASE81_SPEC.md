# Phase 81 — FindCoordinator v5–6 (version ratchet)

## Goals

1. Raise **FindCoordinator** max from **0–4** to **0–6** (Apache Kafka max)
2. Accept flexible **v5** and **v6** with the same batch wire as **v4**
3. Response header **v1** for v3–6 (already true for v ≥ 3)
4. v0–4 paths unchanged
5. v7 → UnsupportedVersion with response header v1
6. Tests + docs honesty

## Non-goals

- Emitting **TRANSACTION_ABORTABLE** (123) on FindCoordinator (KIP-890)
- Share groups / KIP-932 coordinator key type **SHARE (2)**
- Real multi-broker coordinator placement (always this broker)
- AddOffsetsToTxn v4 (still deferred; wire-identical KIP-890 ratchet)
- ApiVersions v4–5 / Fetch v14+ / multi-lang clients / READ_COMMITTED / 2PC

## Wire summary

Apache Kafka documents:

> Version 5 adds support for new error code TRANSACTION_ABORTABLE (KIP-890).
>
> Version 6 adds support for share groups (KIP-932).
> For key type SHARE (2), the coordinator key format is
> `"groupId:topicId:partition"`.

### Request (flexible v4+)

```
KeyType: INT8,                      # 0=group, 1=transaction, 2=share
CoordinatorKeys: COMPACT_ARRAY[COMPACT_STRING],
TAG_BUFFER
```

### Response (flexible v4+)

```
ThrottleTimeMs: INT32,              # always 0 on Volant
Coordinators: COMPACT_ARRAY[{
  Key: COMPACT_STRING,
  NodeId: INT32,
  Host: COMPACT_STRING,
  Port: INT32,
  ErrorCode: INT16,
  ErrorMessage: COMPACT_NULLABLE_STRING,
  TAG_BUFFER
}],
TAG_BUFFER
```

**v5–6 delta vs v4:** none on the wire for group/transaction keys. Volant never
returns error code `TRANSACTION_ABORTABLE` (123). Key type **2 (share)** is
rejected with `InvalidRequest` (same as any other unsupported key type).

## Semantics (honest)

| Case | Behavior |
|------|----------|
| v5/v6 group (type 0) | Same as v4 → this broker |
| v5/v6 transaction (type 1) | Same as v4 → this broker |
| v5/v6 share (type 2) | InvalidRequest + ErrorMessage |
| Empty keys batch | Empty Coordinators array |
| TRANSACTION_ABORTABLE | Never emitted |
| Classic v0–2 / flex v3 | Unchanged |

## Exit criteria

1. ApiVersions: FindCoordinator **0–6**
2. FindCoordinator **v5** and **v6** batch success (group + txn keys)
3. FindCoordinator **v6** share key_type → InvalidRequest
4. FindCoordinator **v4** still works
5. FindCoordinator **v7** → header v1 + UnsupportedVersion (35)
6. phase81 + phase52 / phase31 / phase44 green after max-version updates
7. ROADMAP / README / ops / KAFKA_COMPAT honesty

## Honest limitations

- No share-group coordinators (KIP-932); key type 2 rejected
- No TRANSACTION_ABORTABLE emission (KIP-890)
- ThrottleTimeMs always 0
- Always resolves to the local broker (single-node / no coordinator affinity)
- Empty tag buffers only
