# Phase 52 — Flexible Metadata v9 + FindCoordinator v3–4

## Goals

1. Ship **Metadata v9** (first flexible Metadata; compact arrays/strings + tags)
2. Ship **FindCoordinator v3–4** (flexible single-key + KIP-699 batch)
3. Use **response header v1** (correlation + TAG_BUFFER) for these flexible APIs
4. Advertise Metadata max **9**, FindCoordinator max **4**; tests + docs honesty

## Non-goals

- Metadata v10+ (TopicId UUID; nullable topic name)
- Flexible Produce / Fetch / group / txn / admin APIs
- Real SupportedFeatures / quota throttling
- Changing ApiVersions response header (stays v0)

## Response header

| API | Flexible body | Response header |
|-----|---------------|-----------------|
| ApiVersions v3 | yes | **v0** (Kafka special case) |
| Metadata v9 | yes | **v1** (corr + TAG_BUFFER) |
| FindCoordinator v3–4 | yes | **v1** |

Request header for flexible versions remains RequestHeader v2: classic ClientId
+ header TAG_BUFFER.

## Metadata v9

### Request (flexible)

```
Topics: COMPACT_NULLABLE_ARRAY[{
  Name: COMPACT_STRING,
  TAG_BUFFER
}]
AllowAutoTopicCreation: BOOL          # ignored
IncludeClusterAuthorizedOperations: BOOL
IncludeTopicAuthorizedOperations: BOOL
TAG_BUFFER
```

Null topics array → all topics; empty array → no topics (same as classic v1+).

### Response (header v1 + flexible body)

```
ThrottleTimeMs: INT32
Brokers: COMPACT_ARRAY[{ NodeId, Host: COMPACT_STRING, Port, Rack: COMPACT_NULLABLE_STRING, TAG_BUFFER }]
ClusterId: COMPACT_NULLABLE_STRING    # "volant"
ControllerId: INT32
Topics: COMPACT_ARRAY[{
  ErrorCode, Name: COMPACT_STRING, IsInternal,
  Partitions: COMPACT_ARRAY[{
    ErrorCode, PartitionIndex, LeaderId, LeaderEpoch=-1,
    ReplicaNodes: COMPACT_ARRAY[INT32],
    IsrNodes: COMPACT_ARRAY[INT32],
    OfflineReplicas: COMPACT_ARRAY[INT32],
    TAG_BUFFER
  }],
  TopicAuthorizedOperations: INT32,
  TAG_BUFFER
}]
ClusterAuthorizedOperations: INT32
TAG_BUFFER
```

Classic Metadata **0–8** unchanged.

## FindCoordinator v3–4

### v3 request / response

```
# request
Key: COMPACT_STRING
KeyType: INT8                         # 0=group, 1=transaction
TAG_BUFFER

# response (header v1)
ThrottleTimeMs: INT32
ErrorCode: INT16
ErrorMessage: COMPACT_NULLABLE_STRING
NodeId: INT32
Host: COMPACT_STRING
Port: INT32
TAG_BUFFER
```

### v4 batch (KIP-699)

```
# request
KeyType: INT8
CoordinatorKeys: COMPACT_ARRAY[COMPACT_STRING]
TAG_BUFFER

# response
ThrottleTimeMs: INT32
Coordinators: COMPACT_ARRAY[{
  Key: COMPACT_STRING, NodeId, Host: COMPACT_STRING, Port,
  ErrorCode, ErrorMessage: COMPACT_NULLABLE_STRING, TAG_BUFFER
}]
TAG_BUFFER
```

All keys resolve to this broker (single-node coordinator). Classic **0–2** unchanged.

## Exit criteria

1. ApiVersions advertises Metadata max **9**, FindCoordinator max **4**
2. Metadata v9 round-trip: response header tag + compact brokers/topics
3. FindCoordinator v3 round-trip: compact host + body tags
4. FindCoordinator v4 batch returns one coordinator entry per key
5. Classic Metadata v8 / FindCoordinator v2 still work
6. Metadata v10 / FindCoordinator v5 → UnsupportedVersion
7. phase52 tests green

## Honest limitations

- No TopicId (Metadata v10+)
- No real multi-coordinator topology
- Other APIs remain classic-only
- throttle always 0; empty tag buffers only
