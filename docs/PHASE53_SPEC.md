# Phase 53 — Flexible Produce v9

## Goals

1. Support **Produce v9** (first flexible Produce; KIP-482 compact framing)
2. Use **response header v1** for Produce v9+
3. Advertise Produce max **9**; keep classic **0–8** unchanged
4. Tests + docs honesty

## Non-goals

- Produce v10 (KIP-951 CurrentLeader / NodeEndpoints tagged fields)
- Flexible Fetch v12+
- Flexible group / txn / admin APIs
- Per-record `record_errors` detail (still empty)

## Wire summary

### Request (flexible header + body)

```
TransactionalId: COMPACT_NULLABLE_STRING   # v3+
Acks: INT16
TimeoutMs: INT32
TopicData: COMPACT_ARRAY[{
  Name: COMPACT_STRING
  PartitionData: COMPACT_ARRAY[{
    Index: INT32
    Records: COMPACT_RECORDS               # uvarint(len+1)+bytes; 0=null
    TAG_BUFFER
  }]
  TAG_BUFFER
}]
TAG_BUFFER
```

### Response (header v1 + flexible body)

```
Responses: COMPACT_ARRAY[{
  Name: COMPACT_STRING
  PartitionResponses: COMPACT_ARRAY[{
    Index, ErrorCode, BaseOffset,
    LogAppendTimeMs,                       # always -1 (CreateTime)
    LogStartOffset,
    RecordErrors: COMPACT_ARRAY[…]         # always empty
    ErrorMessage: COMPACT_NULLABLE_STRING  # always null
    TAG_BUFFER
  }]
  TAG_BUFFER
}]
ThrottleTimeMs: INT32                      # always 0
TAG_BUFFER
```

Classic Produce **0–8** unchanged (i32 arrays, classic strings/bytes).

## Exit criteria

1. ApiVersions advertises Produce max **9**
2. Produce v9 round-trip: compact records in, compact response + header tags out
3. Produced records visible on the log
4. Produce v8 still classic (header v0, classic framing)
5. Produce v10 → UnsupportedVersion (with response header v1 for version ≥9)
6. phase53 tests green

## Honest limitations

- No Produce v10 CurrentLeader / NodeEndpoints
- record_errors always empty; throttle always 0
- Fetch and other APIs still classic-only at their max versions
