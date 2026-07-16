# Phase 55 — Flexible group consumer APIs

## Goals

1. Support first flexible versions of consumer group lifecycle APIs:
   - **JoinGroup v6**
   - **SyncGroup v4**
   - **Heartbeat v4**
   - **LeaveGroup v4**
2. Use **response header v1** for those flexible versions
3. Advertise raised max versions; keep classic ranges unchanged
4. Tests + docs honesty

## Non-goals

- JoinGroup v7+ (ProtocolType echo, Reason, SkipAssignment)
- SyncGroup v5 (ProtocolType / ProtocolName on request/response)
- LeaveGroup v5 (Reason field)
- Flexible OffsetCommit / OffsetFetch / DescribeGroups
- Changing coordinator semantics (assignment, static membership)

## Wire summary

### JoinGroup v6 (flexible header + body)

```
GroupId: COMPACT_STRING
SessionTimeoutMs: INT32
RebalanceTimeoutMs: INT32
MemberId: COMPACT_STRING
GroupInstanceId: COMPACT_NULLABLE_STRING
ProtocolType: COMPACT_STRING
Protocols: COMPACT_ARRAY[{
  Name: COMPACT_STRING
  Metadata: COMPACT_BYTES
  TAG_BUFFER
}]
TAG_BUFFER
```

Response (header v1):

```
ThrottleTimeMs: INT32
ErrorCode: INT16
GenerationId: INT32
ProtocolName: COMPACT_STRING
Leader: COMPACT_STRING
MemberId: COMPACT_STRING
Members: COMPACT_ARRAY[{
  MemberId: COMPACT_STRING
  GroupInstanceId: COMPACT_NULLABLE_STRING
  Metadata: COMPACT_BYTES
  TAG_BUFFER
}]
TAG_BUFFER
```

### SyncGroup v4

```
GroupId, MemberId: COMPACT_STRING
GenerationId: INT32
GroupInstanceId: COMPACT_NULLABLE_STRING
Assignments: COMPACT_ARRAY[{ MemberId, Assignment: COMPACT_BYTES, TAG_BUFFER }]
TAG_BUFFER
```

Response: throttle, error, Assignment COMPACT_BYTES, TAG_BUFFER

### Heartbeat v4

```
GroupId, MemberId: COMPACT_STRING
GenerationId: INT32
GroupInstanceId: COMPACT_NULLABLE_STRING
TAG_BUFFER
```

Response: throttle, error, TAG_BUFFER

### LeaveGroup v4

```
GroupId: COMPACT_STRING
Members: COMPACT_ARRAY[{
  MemberId: COMPACT_STRING
  GroupInstanceId: COMPACT_NULLABLE_STRING
  TAG_BUFFER
}]
TAG_BUFFER
```

Response: throttle, error, Members compact array (+ per-member tags), TAG_BUFFER

Classic JoinGroup **0–5**, Sync/Heartbeat/Leave **0–3** unchanged.

## Exit criteria

1. ApiVersions advertises JoinGroup max **6**, Sync/Heartbeat/Leave max **4**
2. Full flexible lifecycle: join → sync → heartbeat → leave
3. JoinGroup v5 still classic (header v0 + classic strings)
4. JoinGroup v7 / SyncGroup v5 → UnsupportedVersion with response header v1
5. phase55 tests green

## Honest limitations

- No ProtocolType on Join response (v7+) or Sync request/response (v5+)
- No Reason / SkipAssignment fields
- Empty tag buffers only (no tagged extensions)
- Coordinator assignment semantics unchanged from classic
