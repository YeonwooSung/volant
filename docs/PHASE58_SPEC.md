# Phase 58 — OffsetFetch multi-group v8

## Goals

1. Support **OffsetFetch v8** multi-group `Groups[]` request/response
2. Keep single-group flexible **v6–7** and classic **0–5** unchanged
3. Advertise OffsetFetch max **8**
4. Per-group ACL errors (other groups still succeed)
5. Tests + docs honesty

## Non-goals

- OffsetFetch v9 (MemberId / MemberEpoch for KIP-848)
- Real RequireStable / UNSTABLE_OFFSET_COMMIT
- Storing committed leader epochs

## Wire summary

### Request (flexible header + body)

```
Groups: COMPACT_ARRAY[{
  GroupId: COMPACT_STRING
  Topics: COMPACT_NULLABLE_ARRAY[{   # null=all, empty=none
    Name: COMPACT_STRING
    PartitionIndexes: COMPACT_ARRAY[INT32]
    TAG_BUFFER
  }]
  TAG_BUFFER
}]
RequireStable: BOOL                 # ignored
TAG_BUFFER
```

### Response (header v1)

```
ThrottleTimeMs: INT32
Groups: COMPACT_ARRAY[{
  GroupId: COMPACT_STRING
  Topics: COMPACT_ARRAY[{
    Name: COMPACT_STRING
    Partitions: COMPACT_ARRAY[{
      PartitionIndex, CommittedOffset, CommittedLeaderEpoch=-1,
      Metadata: COMPACT_NULLABLE_STRING, ErrorCode, TAG_BUFFER
    }]
    TAG_BUFFER
  }]
  ErrorCode: INT16                  # group-level
  TAG_BUFFER
}]
TAG_BUFFER
```

No top-level ErrorCode outside Groups (unlike v2–7).

## Exit criteria

1. ApiVersions OffsetFetch max **8**
2. Two groups in one request with correct per-group offsets
3. Empty topics array → none; null topics → all
4. Per-group GROUP_AUTHORIZATION_FAILED when ACL denies one group
5. v7 still single-group layout
6. v9 → UnsupportedVersion (header v1)
7. phase58 tests green

## Honest limitations

- No MemberId/MemberEpoch (v9+)
- RequireStable ignored
- Leader epoch always -1
- Empty tag buffers only
