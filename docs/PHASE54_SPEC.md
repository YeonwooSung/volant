# Phase 54 — Flexible Fetch v12

## Goals

1. Support **Fetch v12** (first flexible Fetch; KIP-482 compact framing)
2. Use **response header v1** for Fetch v12+
3. Advertise Fetch max **12**; keep classic **0–11** unchanged
4. Tests + docs honesty

## Non-goals

- Fetch v13+ (TopicId UUID; UNKNOWN_TOPIC_ID)
- Real incremental fetch sessions
- DivergingEpoch / CurrentLeader / SnapshotId tagged fields (empty tags)
- ClusterId validation (tagged field ignored)
- LastFetchedEpoch truncation detection

## Wire summary

### Request (flexible header + body)

```
ReplicaId: INT32
MaxWaitMs: INT32
MinBytes: INT32
MaxBytes: INT32
IsolationLevel: INT8
SessionId: INT32
SessionEpoch: INT32
Topics: COMPACT_ARRAY[{
  Topic: COMPACT_STRING
  Partitions: COMPACT_ARRAY[{
    Partition, CurrentLeaderEpoch, FetchOffset,
    LastFetchedEpoch,                    # parsed, ignored
    LogStartOffset, PartitionMaxBytes,
    TAG_BUFFER
  }]
  TAG_BUFFER
}]
ForgottenTopicsData: COMPACT_ARRAY[{ Topic, Partitions: COMPACT_ARRAY[INT32], TAG_BUFFER }]
RackId: COMPACT_STRING
TAG_BUFFER                               # ClusterId tag 0 ignored
```

### Response (header v1 + flexible body)

```
ThrottleTimeMs: INT32
ErrorCode: INT16
SessionId: INT32
Responses: COMPACT_ARRAY[{
  Topic: COMPACT_STRING
  Partitions: COMPACT_ARRAY[{
    PartitionIndex, ErrorCode, HighWatermark, LSO, LogStartOffset,
    AbortedTransactions: COMPACT_ARRAY[]   # always empty
    PreferredReadReplica: -1
    Records: COMPACT_RECORDS
    TAG_BUFFER                             # no DivergingEpoch/CurrentLeader
  }]
  TAG_BUFFER
}]
TAG_BUFFER
```

Classic Fetch **0–11** unchanged.

## Exit criteria

1. ApiVersions advertises Fetch max **12**
2. Fetch v12 round-trip: compact topics/records + response header tags
3. Produced records returned in compact records field
4. Fetch v11 still classic (header v0)
5. Fetch v13 → UnsupportedVersion (response header v1 for version ≥12)
6. phase54 tests green

## Honest limitations

- No TopicId (v13+)
- No real fetch sessions; forgotten topics ignored
- No epoch-divergence / leader tagged fields
- LSO ≡ HWM; preferred_read_replica always -1
