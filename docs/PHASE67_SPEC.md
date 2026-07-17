# Phase 67 — Metadata TopicId (v10–12)

## Goals

1. **Metadata** max **0–12** (flexible from v9)
2. Response **TopicId** (Kafka UUID) on v10+
3. Request parse TopicId + nullable Name on v10+
4. **v11**: drop ClusterAuthorizedOperations (request + response)
5. **v12**: resolve topics by TopicId when Name is null
6. Deterministic Volant UUID mapping; tests + docs honesty

## Non-goals

- Metadata v13 top-level ErrorCode
- Fetch / Produce / DeleteTopics TopicId versions
- Persistent random UUIDs (Volant keeps numeric TopicId)
- Auto-create on Metadata

## Wire summary

### TopicId mapping

```
bytes 0–5:  "volant"
bytes 6–11: 0
bytes 12–15: big-endian u32 Volant TopicId
```

Zero UUID = unset. Unknown non-Volant UUIDs → `UnknownTopicId` (100) on v12
id-only queries.

### Request topics (flexible)

| Version | Per-topic fields |
|---------|------------------|
| v9 | compact Name, tags |
| v10–11 | uuid TopicId, compact nullable Name, tags (lookup by name only) |
| v12 | same; null Name → lookup by TopicId |

Flags: `AllowAutoTopicCreation` v4+; `IncludeClusterAuthorizedOperations`
**v8–10 only**; `IncludeTopicAuthorizedOperations` v8+.

### Response topics (flexible)

Order: error, name, **TopicId (v10+)**, is_internal, partitions[],
topic_authorized_ops, tags.

`ClusterAuthorizedOperations` only on **v8–10**.

## Exit criteria

1. ApiVersions Metadata max **12**
2. Metadata v10 returns TopicId for named topics
3. Metadata v11 omits cluster authorized ops
4. Metadata v12 resolves by TopicId; unknown id → UnknownTopicId
5. Metadata v9 still works; classic unchanged
6. v13 → header v1 + UnsupportedVersion
7. phase67 + phase52 + phase38 green

## Honest limitations

- Deterministic UUID from numeric id (not KRaft random UUID storage)
- Unknown topic-by-name still omitted (no UNKNOWN_TOPIC_OR_PARTITION row)
- leader_epoch still -1; empty tags only
- No v13 top-level error
