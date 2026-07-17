# Phase 84 — Fetch v14–18 (Kafka max)

## Goals

1. Raise **Fetch** max from **0–13** to **0–18** (Apache Kafka max)
2. Accept flexible **v14–18** with honest wire framing
3. Parse new request fields; ignore when Volant has no equivalent
4. Emit response fields with correct framing and honest defaults
5. **v16+**: Fetch top-level **NodeEndpoints** (tag 0) on leader errors (mirror Produce Phase 78)
6. Keep classic **0–11** and flexible **v12–13** paths unchanged
7. **v19** → UnsupportedVersion with response header **v1**
8. Tests + docs honesty

## Non-goals

- Emitting `OffsetMovedToTieredStorage` (no tiered storage)
- Real ReplicaState / follower fetch semantics (ReplicaId / ReplicaEpoch ignored)
- ReplicaDirectoryId / HighWatermark request-tag semantics (parse-ignore)
- Real incremental fetch sessions / DivergingEpoch / SnapshotId
- True `READ_COMMITTED` / control markers / 2PC
- Multi-lang clients / cargo-fuzz CI

## Wire summary

Apache Kafka documents Fetch **validVersions 4–18** (Volant still advertises
classic **0–3** for older clients), **flexibleVersions 12+**:

| Version | Request delta | Response delta |
|--------:|---------------|----------------|
| **14** | Same as v13 | Same as v13; may return `OffsetMovedToTieredStorage` (Volant never) |
| **15** | Drop top-level `ReplicaId`; add tagged `ReplicaState` (tag 1) | Same as v14 |
| **16** | Same as v15 | Top-level **NodeEndpoints** (tag 0) on KIP-951 leader errors |
| **17** | Partition tagged `ReplicaDirectoryId` (tag 0) | Same as v16 |
| **18** | Partition tagged `HighWatermark` (tag 1) | Same as v16 |

### Request (flexible v12–18)

```
# v12–14 only:
ReplicaId: INT32                          # -1 for consumers

# v12–18 (v15+ starts here — no top-level ReplicaId):
MaxWaitMs: INT32
MinBytes: INT32
MaxBytes: INT32
IsolationLevel: INT8
SessionId: INT32
SessionEpoch: INT32
Topics: COMPACT_ARRAY[{
  TopicId: UUID,                          # v13+
  Partitions: COMPACT_ARRAY[{
    Partition, CurrentLeaderEpoch, FetchOffset,
    LastFetchedEpoch, LogStartOffset, PartitionMaxBytes,
    TAG_BUFFER                            # v17+ ReplicaDirectoryId; v18+ HighWatermark
  }],
  TAG_BUFFER
}]
ForgottenTopicsData: COMPACT_ARRAY[{ TopicId, Partitions, tags }]
RackId: COMPACT_STRING
TAG_BUFFER                                # ClusterId tag 0; ReplicaState tag 1 (v15+)
```

### Response (header v1 for flexible)

```
ThrottleTimeMs: INT32
ErrorCode: INT16
SessionId: INT32
Responses: COMPACT_ARRAY[{
  TopicId: UUID,                          # v13+
  Partitions: COMPACT_ARRAY[{
    PartitionIndex, ErrorCode, HighWatermark,
    LastStableOffset, LogStartOffset,
    AbortedTransactions (empty), PreferredReadReplica (-1),
    Records (compact bytes),
    TAG_BUFFER                            # tag 1 CurrentLeader on fence (v12+)
  }],
  TAG_BUFFER
}]
TAG_BUFFER                                # v16+: tag 0 NodeEndpoints when any CurrentLeader
```

**NodeEndpoints** (tag 0, v16+), same framing as Produce Phase 78:

```
COMPACT_ARRAY[{
  NodeId: INT32,
  Host: COMPACT_STRING,
  Port: INT32,
  Rack: COMPACT_NULLABLE_STRING (null),
  TAG_BUFFER empty
}]
```

Emitted only when at least one partition included **CurrentLeader**
(NotLeader / FencedLeaderEpoch). Success keeps empty top-level tags.

## Semantics (honest)

| Case | Behavior |
|------|----------|
| v14 success | Same as v13 TopicId path |
| v15+ success | Same response as v14; request omits ReplicaId |
| v15+ ReplicaState tag | Parsed via skip_tag_buffer; ignored |
| v16+ FencedLeaderEpoch | CurrentLeader tag 1 + NodeEndpoints tag 0 |
| v16+ success | Empty top-level tags |
| v17–18 partition tags | Parse-ignore |
| OffsetMovedToTieredStorage | Never emitted |
| Classic 0–11 / flex 12–13 | Unchanged |
| v19+ | Header v1 + UnsupportedVersion (35) |

## Exit criteria

1. ApiVersions Fetch max **18**
2. Fetch **v14** TopicId round-trip (records + empty tags)
3. Fetch **v15** without top-level ReplicaId (optional ReplicaState tag)
4. Fetch **v16** success → empty NodeEndpoints
5. Fetch **v16** FencedLeaderEpoch → CurrentLeader + NodeEndpoints
6. Fetch **v18** Kafka max success round-trip
7. Fetch **v13** still works
8. Fetch **v19** → header v1 + UnsupportedVersion (35)
9. phase54 / phase49 / phase68 / phase51 / phase50 max assertions updated
10. ROADMAP / README / ops / KAFKA_COMPAT / WHITEPAPER / PHASE_HISTORY / INDEX honesty

## Honest limitations

- No tiered storage → never `OffsetMovedToTieredStorage`
- ReplicaState / ReplicaDirectoryId / HighWatermark request tags ignored
- No real fetch sessions; forgotten topics ignored
- No DivergingEpoch / SnapshotId tags
- LSO ≡ HWM; preferred_read_replica always -1
- Single-node: CurrentLeader / NodeEndpoints almost always self
- Deterministic TopicId UUID (not KRaft random)
