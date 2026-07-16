# Phase 56 — Flexible group field completeness

## Goals

1. Raise flexible group APIs to Kafka-current field sets:
   - **JoinGroup v7–9** (ProtocolType, Reason, SkipAssignment)
   - **SyncGroup v5** (ProtocolType / ProtocolName request + response)
   - **LeaveGroup v5** (Reason on members)
2. Keep Heartbeat max **4** (no higher Kafka versions)
3. Keep classic ranges and first-flex framing from Phase 55
4. Tests + docs honesty

## Non-goals

- Strict ProtocolType/Name consistency rejection on SyncGroup
- Server-side client assignor skip (SkipAssignment always `false`)
- Flexible OffsetCommit / OffsetFetch / DescribeGroups
- KIP-848 new consumer group protocol

## Wire deltas (vs Phase 55)

### JoinGroup

| Version | Request | Response |
|---------|---------|----------|
| v6 | (Phase 55) | ProtocolName COMPACT_STRING (non-null) |
| v7 | same as v6 | **+ ProtocolType** COMPACT_NULLABLE; ProtocolName becomes nullable |
| v8 | **+ Reason** COMPACT_NULLABLE (ignored) | same as v7 |
| v9 | same as v8 | **+ SkipAssignment** BOOL after Leader (always `false`) |

Field order (v9 response):

```
Throttle, Error, GenerationId,
ProtocolType, ProtocolName, Leader, SkipAssignment, MemberId,
Members[{ MemberId, GroupInstanceId, Metadata, TAG }],
TAG_BUFFER
```

### SyncGroup v5

Request: after GroupInstanceId:

```
ProtocolType: COMPACT_NULLABLE_STRING
ProtocolName: COMPACT_NULLABLE_STRING
Assignments: …
TAG_BUFFER
```

Response: after ErrorCode:

```
ProtocolType, ProtocolName (echoed; no consistency check)
Assignment: COMPACT_BYTES
TAG_BUFFER
```

### LeaveGroup v5

Per-member request: `Reason` COMPACT_NULLABLE after GroupInstanceId (ignored).
Response wire-identical to v4.

## Exit criteria

1. ApiVersions: JoinGroup max **9**, Sync/Leave max **5**, Heartbeat max **4**
2. Join v9 lifecycle with ProtocolType + SkipAssignment=0
3. Join v7 has ProtocolType, no SkipAssignment field
4. Join v6 still ProtocolName-only (no ProtocolType)
5. Sync v5 echoes ProtocolType/Name
6. Leave v5 accepts Reason
7. Join v10 / Sync v6 → UnsupportedVersion (header v1)
8. phase56 tests green

## Honest limitations

- SkipAssignment always false (classic client-assignor model)
- Sync ProtocolType/Name not validated against join-time values
- Reason fields parsed and discarded
- Empty tag buffers only
