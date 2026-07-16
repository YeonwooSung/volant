# Phase 40 — Kafka ListOffsets classic v0–5

## Goals

1. Raise **ListOffsets** (API key 2) from classic **v0–1** to classic **v0–5**
2. Parse `isolation_level` (v2+) and `current_leader_epoch` (v4+) honestly
3. Emit throttle (v2+) and response `leader_epoch` (v4+)
4. Advertise max version **5** in ApiVersions; tests + docs

## Non-goals

- Flexible ListOffsets v6+
- Timestamp-based offset lookup (only `-1` latest / `-2` earliest)
- True READ_COMMITTED LSO ≠ HWM (buffer-until-commit: LSO ≡ HWM)
- Durable epoch history for fencing beyond current partition epoch

## Wire (classic)

### Request

```
replica_id: INT32
isolation_level: INT8              # v2+ (0=READ_UNCOMMITTED, 1=READ_COMMITTED; ignored)
topics: [{
  name: STRING
  partitions: [{
    partition_index: INT32
    current_leader_epoch: INT32    # v4+ (-1 = no fence)
    timestamp: INT64               # -1 latest, -2 earliest
    max_num_offsets: INT32         # v0 only
  }]
}]
```

### Response

```
throttle_time_ms: INT32            # v2+
topics: [{
  name: STRING
  partitions: [{
    partition_index: INT32
    error_code: INT16
    # v0: old_style_offsets: [{timestamp, offset}] array
    # v1+:
    timestamp: INT64
    offset: INT64
    leader_epoch: INT32            # v4+
  }]
}]
```

## Behavior

| Case | Result |
|------|--------|
| timestamp `-1` | latest (= HWM / LEO as today) |
| timestamp `-2` | earliest |
| other timestamps | `INVALID_TIMESTAMP` |
| isolation_level | accepted 0/1; both return same offsets (LSO ≡ HWM) |
| `current_leader_epoch` ≠ -1 and **>** partition epoch | `UNKNOWN_LEADER_EPOCH` |
| `current_leader_epoch` ≠ -1 and **<** partition epoch | `FENCED_LEADER_EPOCH` |
| success v4+ | `leader_epoch` = current partition epoch (or `-1` if unknown path) |
| ACL deny | `TOPIC_AUTHORIZATION_FAILED` |
| unknown topic/partition | `UNKNOWN_TOPIC_OR_PARTITION` |

## Exit criteria

1. ApiVersions advertises ListOffsets max version **5**
2. v2+ response starts with throttle 0
3. v4+ response includes leader_epoch; fencing works
4. isolation_level parsed without changing offsets
5. v0/v1 still work (phase25)
6. Tests green

## Honest limitations

- No flexible v6+
- No max-timestamp / local-log-start / tiered offset specials (v7+)
- isolation_level does not filter (buffer-until-commit)
- leader_epoch is the live partition epoch, not historical
