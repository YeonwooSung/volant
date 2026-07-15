# Phase 25 — Kafka admin APIs (Create/DeleteTopics, ListOffsets)

## Goals

1. **CreateTopics** on the Kafka shim so clients can create topics without the Volant protocol
2. **DeleteTopics** for basic lifecycle
3. **ListOffsets** (earliest / latest) for consumer bootstrap
4. Advertise via ApiVersions; ACL-aware (`kafka-anonymous` principal)
5. Tests + docs honesty

## Non-goals

- CreatePartitions / AlterConfigs / DescribeConfigs on Kafka wire
- Kafka consumer groups / FindCoordinator / offset commit
- Flexible versions / tagged fields
- Full replica-assignment / validate_only semantics
- Timestamp-based ListOffsets (only earliest=-2 / latest=-1)

## API versions (advertised)

| API | Key | Min | Max | Notes |
|-----|----:|----:|----:|-------|
| ListOffsets | 2 | 0 | 1 | timestamp -1 latest, -2 earliest |
| CreateTopics | 19 | 0 | 1 | num_partitions; RF/assignment ignored |
| DeleteTopics | 20 | 0 | 1 | by name |
| Produce | 0 | 0 | 3 | unchanged (Phase 24) |
| Fetch | 1 | 0 | 4 | unchanged |
| Metadata | 3 | 0 | 1 | unchanged |
| ApiVersions | 18 | 0 | 0 | unchanged |

## CreateTopics

Request (v0 body; v1 adds `timeout_ms` at end):

```
[topic name, num_partitions:i32, replication_factor:i16,
  [partition → [broker_id]], [config_key → config_value]]
[timeout_ms:i32]   # v1 only
```

- `replication_factor` and replica assignments are **ignored** (single-node / Volant assignment)
- Configs: best-effort via `create_topic_with_configs` when non-empty; unknown keys may fail
- Empty / zero partitions → Kafka `INVALID_PARTITIONS` (37)
- Already exists → `TOPIC_ALREADY_EXISTS` (36)
- ACL: Cluster `Create` (or topic Create if we only have topic-level — use Cluster Create for create-all)

Response:

- v0: `[topic, error_code]`
- v1: same + trailing `throttle_time_ms=0`

## DeleteTopics

Request:

```
[topic names]
timeout_ms:i32
```

- ACL: topic `Delete` per topic
- Missing → `UNKNOWN_TOPIC_OR_PARTITION` (3)

Response:

- v0: `[topic, error_code]`
- v1: + `throttle_time_ms=0`

## ListOffsets

Request v0:

```
replica_id:i32
[topic [partition, timestamp:i64, max_num_offsets:i32]]
```

Request v1: same without `max_num_offsets` (always one offset).

Timestamps:

| Value | Meaning |
|------:|---------|
| -1 | latest (LEO / next write offset) |
| -2 | earliest (log start) |
| other | `INVALID_REQUEST` / empty — return error `INVALID_TIMESTAMP` (32) or map to latest for MVP |

Response v0: `[topic [partition, error, [timestamp, offset]]]` (array of timestamp-offset pairs)

Response v1: `[topic [partition, error, timestamp, offset]]` (single pair)

## Exit criteria

1. Kafka CreateTopics → topic visible via Metadata + Produce
2. DeleteTopics removes topic
3. ListOffsets returns correct earliest/latest after produce
4. Phase 23–24 suites still green
5. `cargo test --workspace` green

## Honest limitations

- No multi-broker replica assignment from Kafka CreateTopics
- No validate_only / CreatePartitions
- ListOffsets ignores wall-clock timestamps
- No DescribeConfigs / AlterConfigs
