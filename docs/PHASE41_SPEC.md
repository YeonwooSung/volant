# Phase 41 — Kafka OffsetFetch classic v0–5

## Goals

1. Raise **OffsetFetch** (API key 9) from classic **v0–1** to classic **v0–5**
2. Correct null-topics semantics (v2+: null = all, empty = none)
3. Top-level error (v2+), throttle (v3+), committed leader epoch (v5+)
4. Advertise max version **5**; tests + docs honesty

## Non-goals

- Flexible OffsetFetch v6+
- Multi-group OffsetFetch v8+
- `require_stable` (v7+) / KIP-848 member fields (v9+)
- Storing real committed leader epochs with offsets (emit `-1`)

## Wire (classic)

### Request

```
group_id: STRING
topics: NULLABLE ARRAY of {
  name: STRING
  partition_indexes: []INT32
}
# v0–1: empty array treated as "all" (legacy shim)
# v2+: null (-1) = all topics; empty (0) = no topics
```

### Response

```
throttle_time_ms: INT32            # v3+
topics: [{
  name: STRING
  partitions: [{
    partition_index: INT32
    committed_offset: INT64        # -1 if unknown
    committed_leader_epoch: INT32  # v5+ (always -1 for now)
    metadata: STRING               # non-null (empty if none)
    error_code: INT16
  }]
}]
error_code: INT16                  # v2+ top-level
```

## Behavior

| Case | Result |
|------|--------|
| listed partitions | committed offset or `-1` |
| null topics (v2+) / empty (v0–1) | all committed offsets for group |
| empty topics (v2+) | empty topics array |
| Group Read ACL deny | v0–1 empty topics; v2+ top-level `GROUP_AUTHORIZATION_FAILED` |
| success | top-level error 0 (v2+) |

## Exit criteria

1. ApiVersions advertises OffsetFetch max version **5**
2. v2+ top-level error present
3. v3+ throttle 0
4. v5+ committed_leader_epoch = -1
5. v2 null topics returns all commits
6. phase26 / phase36 still green
7. Tests green

## Honest limitations

- No flexible v6+ / multi-group v8+
- No durable committed leader epoch (always -1)
- No require_stable hold for unstable txn offsets
