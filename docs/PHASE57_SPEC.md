# Phase 57 — Flexible OffsetCommit + OffsetFetch

## Goals

1. Support **OffsetCommit v8** (first flexible OffsetCommit)
2. Support **OffsetFetch v6–7** (first flexible + RequireStable)
3. Response header v1 for those flexible versions
4. Keep classic OffsetCommit 0–7 and OffsetFetch 0–5 unchanged
5. Tests + docs honesty

## Non-goals

- OffsetCommit v9 (KIP-848 STALE_MEMBER_EPOCH — wire-identical to v8)
- OffsetFetch multi-group **v8+** (Groups[] structure)
- OffsetFetch v9 MemberId/MemberEpoch
- Storing or fencing on CommittedLeaderEpoch
- Real RequireStable / UNSTABLE_OFFSET_COMMIT (no pending txn offsets)

## Wire summary

### OffsetCommit v8

Request (flexible header + body):

```
GroupId: COMPACT_STRING
GenerationIdOrMemberEpoch: INT32
MemberId: COMPACT_STRING
GroupInstanceId: COMPACT_NULLABLE_STRING
Topics: COMPACT_ARRAY[{
  Name: COMPACT_STRING
  Partitions: COMPACT_ARRAY[{
    PartitionIndex: INT32
    CommittedOffset: INT64
    CommittedLeaderEpoch: INT32   # ignored
    CommittedMetadata: COMPACT_NULLABLE_STRING
    TAG_BUFFER
  }]
  TAG_BUFFER
}]
TAG_BUFFER
```

Response (header v1):

```
ThrottleTimeMs: INT32
Topics: COMPACT_ARRAY[{
  Name: COMPACT_STRING
  Partitions: COMPACT_ARRAY[{ PartitionIndex, ErrorCode, TAG_BUFFER }]
  TAG_BUFFER
}]
TAG_BUFFER
```

### OffsetFetch v6–7

Request:

```
GroupId: COMPACT_STRING
Topics: COMPACT_NULLABLE_ARRAY[{   # null=all, empty=none
  Name: COMPACT_STRING
  PartitionIndexes: COMPACT_ARRAY[INT32]
  TAG_BUFFER
}]
RequireStable: BOOL               # v7+ only; ignored
TAG_BUFFER
```

Response (header v1):

```
ThrottleTimeMs: INT32
Topics: COMPACT_ARRAY[{
  Name: COMPACT_STRING
  Partitions: COMPACT_ARRAY[{
    PartitionIndex, CommittedOffset, CommittedLeaderEpoch=-1,
    Metadata: COMPACT_NULLABLE_STRING, ErrorCode, TAG_BUFFER
  }]
  TAG_BUFFER
}]
ErrorCode: INT16                  # top-level
TAG_BUFFER
```

## Exit criteria

1. ApiVersions: OffsetCommit max **8**, OffsetFetch max **7**
2. Commit v8 + fetch v7 round-trip with compact framing
3. Fetch v6 null topics = all
4. Commit v7 still classic (header v0)
5. Commit v9 / Fetch v8 → UnsupportedVersion (header v1)
6. phase57 tests green

## Honest limitations

- No multi-group OffsetFetch (v8+)
- Leader epoch not stored; always -1 on fetch
- RequireStable always treated as false (no unstable offsets)
- Empty tag buffers only
