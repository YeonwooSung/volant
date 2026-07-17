# Phase 68 — Fetch TopicId (v13)

## Goals

1. **Fetch** max **0–13** (flexible from v12; TopicId from v13)
2. Request/response topics use **TopicId UUID** on v13 (no topic name)
3. ForgottenTopicsData uses TopicId on v13
4. Unknown / non-Volant UUID → **UnknownTopicId (100)** per partition
5. Deterministic UUID mapping (same as Metadata Phase 67)
6. Tests + docs honesty

## Non-goals

- Fetch v14+ (NodeEndpoints / CurrentLeader tagged fields, etc.)
- Real incremental fetch sessions
- DivergingEpoch / SnapshotId tagged fields
- Produce / DeleteTopics / CreateTopics TopicId versions
- Persistent random KRaft UUIDs

## Wire summary

### TopicId mapping

Same as Phase 67:

```
bytes 0–5:  "volant"
bytes 6–11: 0
bytes 12–15: big-endian u32 Volant TopicId
```

Zero UUID and unrecognized layouts → UnknownTopicId.

### Request topics

| Version | Per-topic identity |
|---------|--------------------|
| ≤v11 classic | STRING name |
| v12 flexible | COMPACT_STRING name |
| **v13** | **UUID TopicId** |

Partition fields unchanged from v12 (including LastFetchedEpoch + TAG_BUFFER).

ForgottenTopicsData (v7+): name ≤v12; **TopicId UUID on v13**.

### Response topics

| Version | Per-topic identity |
|---------|--------------------|
| ≤v11 classic | STRING name |
| v12 flexible | COMPACT_STRING name |
| **v13** | **UUID TopicId** (echo request) |

Partition response fields unchanged (HWM, LSO≡HWM, empty aborted, preferred=-1).

## Exit criteria

1. ApiVersions Fetch max **13**
2. Fetch v13 by known TopicId returns records + echo UUID
3. Fetch v13 unknown UUID → partition error 100
4. Fetch v12 still name-based (header v1 + compact name)
5. Fetch v14 → header v1 + UnsupportedVersion
6. phase68 + phase54 + phase49 green

## Honest limitations

- Deterministic UUID from numeric id (not KRaft random storage)
- No real fetch sessions; forgotten topics ignored
- No epoch-divergence / leader tagged fields
- LSO ≡ HWM; preferred_read_replica always -1
- No v14+ CurrentLeader / NodeEndpoints
