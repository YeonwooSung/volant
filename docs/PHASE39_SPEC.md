# Phase 39 — Kafka OffsetForLeaderEpoch

## Goals

1. **OffsetForLeaderEpoch** (API key 23, classic **v0–3**) on the Kafka shim
2. Enough semantics for consumer truncation checks after leader change
3. Advertise in ApiVersions; tests + docs honesty

## Non-goals

- Flexible v4+ (compact strings / tagged fields)
- Durable leader-epoch → offset history (KIP-101 log)
- Inter-broker replica fetch path using this API
- Changing Metadata’s advertised `leader_epoch = -1` (Phase 38 honesty unchanged)

## Wire (classic)

### Request

```
replica_id: INT32                         # v3+ only (ignored)
topics: [{
  name: STRING
  partitions: [{
    partition: INT32
    current_leader_epoch: INT32           # v2+ (fencing; -1 = no check)
    leader_epoch: INT32                   # epoch to look up end offset for
  }]
}]
```

### Response

```
throttle_time_ms: INT32                   # v2+
topics: [{
  name: STRING
  partitions: [{
    error_code: INT16
    partition: INT32
    leader_epoch: INT32                   # v1+
    end_offset: INT64
  }]
}]
```

## Behavior

| Case | Result |
|------|--------|
| Unknown topic/partition | `UNKNOWN_TOPIC_OR_PARTITION`, end_offset `-1` |
| ACL deny (Topic Describe) | `TOPIC_AUTHORIZATION_FAILED` |
| `current_leader_epoch` ≠ -1 and **>** partition epoch | `UNKNOWN_LEADER_EPOCH` |
| `current_leader_epoch` ≠ -1 and **<** partition epoch | `FENCED_LEADER_EPOCH` |
| Requested `leader_epoch` **>** partition epoch (and ≠ -1) | `UNKNOWN_LEADER_EPOCH` |
| Requested `leader_epoch` ≤ partition epoch, or `-1` (latest) | error 0; `leader_epoch` = current; `end_offset` = **HWM** |
| `replica_id` | ignored |

Volant does **not** store historical epoch end offsets. Any past epoch that is
still ≤ the current partition epoch returns the **current HWM** (same as “latest”).
This is honest for single-node / buffer-until-commit where truncation across
epochs is rare; multi-epoch history remains deferred.

## Error codes added

| Code | Name |
|------|------|
| 74 | FencedLeaderEpoch |
| 75 | UnknownLeaderEpoch |

## Exit criteria

1. ApiVersions advertises 23 with max version 3
2. Happy path returns HWM + current leader epoch
3. Fencing via `current_leader_epoch` works
4. Unknown topic / ACL deny covered
5. Tests green

## Honest limitations

- No epoch history (all eligible epochs → current HWM)
- No flexible v4+
- Metadata still reports `leader_epoch = -1` (Phase 38)
