# Phase 80 — CreatePartitions v3 (version ratchet)

## Goals

1. Raise **CreatePartitions** max from **0–2** to **0–3**
2. Accept flexible **v3** with the same wire framing as **v2** (compact + tags)
3. Response header **v1** for v2–3 (already true for v ≥ 2)
4. v0–2 paths unchanged
5. v4 → UnsupportedVersion with response header v1
6. Tests + docs honesty

## Non-goals

- Real controller/broker **quota** enforcement (KIP-599)
- Emitting **THROTTLING_QUOTA_EXCEEDED** (Volant has no quotas → throttle always 0)
- CreatePartitions by TopicId
- Replica assignment enforcement / multi-broker assignment waits
- CreateTopics / DeleteTopics further bumps
- READ_COMMITTED / 2PC / multi-lang clients / cargo-fuzz CI

## Wire summary

Apache Kafka documents CreatePartitions **v3** as:

> Version 3 is identical to version 2 but may return a THROTTLING_QUOTA_EXCEEDED
> error in the response if the partitions creation is throttled (KIP-599).

### Request (flexible v2+)

```
Topics: COMPACT_ARRAY[{
  Name: COMPACT_STRING,
  Count: INT32,
  Assignments: COMPACT_NULLABLE_ARRAY[{ BrokerIds: COMPACT_ARRAY[INT32], tags }],
  TAG_BUFFER
}],
TimeoutMs: INT32,
ValidateOnly: BOOL,
TAG_BUFFER
```

### Response (flexible v2+)

```
ThrottleTimeMs: INT32,          # always 0 on Volant
Results: COMPACT_ARRAY[{
  Name: COMPACT_STRING,
  ErrorCode: INT16,
  ErrorMessage: COMPACT_NULLABLE_STRING,  # present since classic v0
  TAG_BUFFER
}],
TAG_BUFFER
```

**v3 delta vs v2:** none on the wire. ErrorMessage already exists on all
versions. Volant never returns error code `THROTTLING_QUOTA_EXCEEDED` (89).

## Semantics (honest)

| Case | Behavior |
|------|----------|
| v3 grow | Same as v2 → `Broker::create_partitions` |
| v3 validate_only | Dry-run; no partition change |
| v3 shrink / unknown topic | ErrorCode + ErrorMessage (same as v2) |
| Quota / throttle | Not implemented; ThrottleTimeMs = 0; no code 89 |

## Exit criteria

1. ApiVersions: CreatePartitions **0–3**
2. CreatePartitions **v3** grows partition count; ErrorMessage null on success
3. CreatePartitions **v3** ErrorMessage non-null on InvalidPartitions / UnknownTopic
4. CreatePartitions **v2** still works
5. CreatePartitions **v4** → header v1 + UnsupportedVersion (35)
6. phase80 + phase60 / phase45 green after max-version updates
7. ROADMAP / README / ops / KAFKA_COMPAT honesty

## Honest limitations

- No quota system; KIP-599 THROTTLING_QUOTA_EXCEEDED never emitted
- ThrottleTimeMs always 0
- Replica assignment arrays ignored
- No TopicId on CreatePartitions
- Empty tag buffers only
