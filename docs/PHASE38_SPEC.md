# Phase 38 — Kafka Metadata classic v0–8

## Goals

1. Raise **Metadata** (API key 3) from classic **v0–1** to classic **v0–8**
2. Fix incomplete v1 encoding (**broker rack**) and request semantics (**null topics = all**)
3. Advertise max version **8** in ApiVersions (flexible **v9+** remains out of scope)
4. Tests + docs honesty

## Non-goals

- Flexible Metadata v9+ (compact strings, tagged fields, topic UUID v10+)
- Real leader epochs / offline replica tracking (emit defaults)
- Auto-topic-creation on Metadata (`allow_auto_topic_creation` is ignored)
- DescribeCluster / ListTransactions (flexible-only modern APIs)

## Wire (classic response fields by version)

| Version | Additive fields |
|---------|-----------------|
| v0 | brokers `[id,host,port]`, topics `[error,name,partitions…]` |
| v1 | broker **rack** (nullable string); **controller_id**; topic **is_internal** |
| v2 | **cluster_id** (nullable string) after brokers |
| v3–4 | top-level **throttle_time_ms** (INT32, always 0) |
| v5–6 | per-partition **offline_replicas** `[]INT32` (always empty) |
| v7 | per-partition **leader_epoch** INT32 (always **-1**) |
| v8 | **topic_authorized_operations** INT32 per topic; **cluster_authorized_operations** INT32 |

### Response order (v8)

```
throttle_time_ms: INT32                 # v3+
brokers: [{
  node_id, host, port,
  rack: NULLABLE_STRING                 # v1+
}]
cluster_id: NULLABLE_STRING             # v2+
controller_id: INT32                    # v1+
topics: [{
  error_code, name,
  is_internal: BOOLEAN                  # v1+
  partitions: [{
    error_code, partition_index, leader_id,
    leader_epoch: INT32                 # v7+
    replica_nodes[], isr_nodes[],
    offline_replicas[]                  # v5+
  }]
  topic_authorized_operations: INT32    # v8+
}]
cluster_authorized_operations: INT32    # v8 only (classic)
```

### Request (classic)

```
topics: NULLABLE ARRAY of { name: STRING }   # v0: empty = all; v1+: null = all, empty = none
allow_auto_topic_creation: BOOLEAN           # v4+ (ignored)
include_cluster_authorized_operations: BOOL  # v8
include_topic_authorized_operations: BOOL    # v8
```

## Behavior

| Field | Volant value |
|-------|----------------|
| `cluster_id` | `"volant"` (stable string) |
| `rack` | null |
| `throttle_time_ms` | `0` |
| `leader_epoch` | `-1` (Volant has no epoch fence on Metadata) |
| `offline_replicas` | empty |
| `allow_auto_topic_creation` | ignored (no auto-create) |
| authorized ops omitted | `INT32_MIN` when include flag is false |
| authorized ops included | Kafka bitfield (`1 << kafka_op_code`) for ops the principal may perform; when ACLs disabled, common topic/cluster ops are all set |

## Authorization

Unchanged: Cluster **Describe** for “all topics”; Topic **Describe** per listed topic.

## Exit criteria

1. ApiVersions advertises Metadata max version **8**
2. Metadata v1 response includes nullable rack after port
3. Metadata v2+ includes `cluster_id = "volant"`
4. Metadata v3+ includes `throttle_time_ms = 0`
5. Metadata v5+ includes empty `offline_replicas`
6. Metadata v7+ includes `leader_epoch = -1`
7. Metadata v8 authorized-ops flags honor include bits / `INT32_MIN`
8. v1+ null topics array lists all topics; empty array lists none
9. phase23 metadata test updated for rack + null-topics; phase38 tests green

## Honest limitations

- No flexible Metadata (v9+)
- No real leader epochs or offline replica sets
- No Metadata-driven auto topic creation
- Authorized-ops bitfield is best-effort (config ops collapse to Describe/Alter as elsewhere)
