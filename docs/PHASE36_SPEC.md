# Phase 36 — Kafka OffsetDelete + Fetch isolation honesty

## Goals

1. **OffsetDelete** (API key 47, v0) on the Kafka shim, mapped to Volant Phase 12
   `GroupCoordinator::delete_offsets`
2. **Fetch isolation_level** (v4): accept `READ_UNCOMMITTED` (0) and
   `READ_COMMITTED` (1); document honest buffer-until-commit semantics
3. Tests + docs honesty

## Non-goals

- Kafka control batches / `WriteTxnMarkers` / aborted-txn markers in the log
- Changing Phase 18 buffer-until-commit (uncommitted data never hits disk)
- Flexible (compact) Kafka versions
- OffsetDelete of “all offsets” via empty topic list (Kafka lists partitions
  explicitly; empty topics = no-op)

## OffsetDelete wire (classic)

**Request (v0):**

```
group_id: STRING
topics: [{ name: STRING, partitions: [{ partition_index: INT32 }] }]
```

**Response (v0):**

```
error_code: INT16          # top-level
throttle_time_ms: INT32
topics: [{ name, partitions: [{ partition_index, error_code }] }]
```

### Behavior

- ACL: Group **Delete** (same spirit as DeleteGroups; consumers that may reset
  offsets) — use Group **Read** if Delete is too strict? Kafka uses DELETE on
  the group for OffsetDelete. Volant ACL has Delete on Group. Use **Delete**.
- Call `delete_offsets(group, [(topic, partition), ...])` with the listed pairs
- Missing offsets are success (idempotent delete)
- Unknown/empty group still returns success per-partition (native delete is
  best-effort file remove)
- Empty `topics` array → top-level success, empty topics response (no “delete all”)

## Fetch isolation (v4)

Kafka isolation levels:

| Value | Name |
|------:|------|
| 0 | READ_UNCOMMITTED |
| 1 | READ_COMMITTED |

Volant Phase 18 / 31 **never writes uncommitted transactional records** to the
log (buffer until EndTxn commit; abort drops buffers). Therefore:

| Field | READ_UNCOMMITTED | READ_COMMITTED |
|-------|------------------|----------------|
| high_watermark | partition HWM | same |
| last_stable_offset | HWM | HWM (no unstable data on log) |
| aborted_transactions | empty | empty |

Invalid isolation values → `INVALID_REQUEST` for the whole request (or treat as
READ_UNCOMMITTED). We reject values other than 0/1 with top-level empty topics
and no crash.

Clients using `isolation.level=read_committed` get a correct view: only
committed data is ever readable; LSO equals HWM.

## Exit criteria

1. ApiVersions advertises OffsetDelete (47) v0
2. OffsetCommit → OffsetDelete → OffsetFetch shows `-1` / unknown for deleted
3. Per-partition errors when Group Delete ACL denied
4. Fetch v4 with isolation 0 and 1 both return LSO = HWM, empty aborted list
5. Transactional abort still leaves no records; READ_COMMITTED fetch empty
6. `cargo test` green for phase36 + broker tests

## Honest limitations

- No control markers; no aborted producer id lists (nothing to abort on disk)
- LSO always equals HWM (by design of buffer-until-commit)
- OffsetDelete does not require the group to be Empty (same as native)
- No flexible versions
