# Phase 74 — ListOffsets v7–11 (special timestamps)

## Goals

1. **ListOffsets** max **0–11** (flexible from v6)
2. v7: **MAX_TIMESTAMP (-3)** — offset of record with largest timestamp (KIP-734)
3. v8: **EARLIEST_LOCAL (-4)** — same as earliest (no remote/tiered storage)
4. v9: **LATEST_TIERED (-5)** — no remote → offset/timestamp **-1**
5. v10: **TimeoutMs** request field parsed and ignored
6. v11: **EARLIEST_PENDING_UPLOAD (-6)** — no remote → **-1/-1**
7. Classic earliest/latest + flexible v6 paths unchanged
8. Tests + docs honesty

## Non-goals

- Timestamp-indexed lookup for positive timestamps (still InvalidTimestamp)
- Real tiered / remote storage
- Enforcing TimeoutMs for remote awaits
- ListOffsets v12+

## Wire summary

Request flexible body (v6+):

```
ReplicaId, IsolationLevel,
Topics[{ Name, Partitions[{ PartitionIndex, CurrentLeaderEpoch, Timestamp, tags }], tags }],
TimeoutMs (v10+), tags
```

Response shape unchanged from v6 (throttle, topics, partition fields, tags).

### Special timestamps

| Value | Name | Min version | Volant behavior |
|------:|------|-------------|-----------------|
| -1 | LATEST | all | log end / HWM |
| -2 | EARLIEST | all | log start |
| -3 | MAX_TIMESTAMP | **v7** | scan log; return `(offset, max_ts)`; empty → `-1/-1` |
| -4 | EARLIEST_LOCAL | **v8** | ≡ earliest |
| -5 | LATEST_TIERED | **v9** | no remote → `-1/-1` |
| -6 | EARLIEST_PENDING_UPLOAD | **v11** | no remote → `-1/-1` |

Other / positive timestamps → **InvalidTimestamp**.

MAX_TIMESTAMP response uses the **actual max timestamp** (not the `-3` sentinel)
in the Timestamp field.

## Exit criteria

1. ApiVersions ListOffsets max **11**
2. v7 MAX_TIMESTAMP returns correct offset + real timestamp
3. v8 EARLIEST_LOCAL matches earliest
4. v9/v11 tiered specials return -1/-1
5. v10 TimeoutMs does not break parse
6. v6 earliest/latest still work; v12 → UnsupportedVersion
7. phase74 + phase63 + phase40 green

## Honest limitations

- MAX_TIMESTAMP is a full local log scan (no time index)
- No tiered/remote storage; specials -5/-6 always empty
- TimeoutMs ignored
- No positive-timestamp binary search
