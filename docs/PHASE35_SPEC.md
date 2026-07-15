# Phase 35 — Kafka DeleteRecords + ACL admin APIs

## Goals

1. **DeleteRecords** (API key 21, v0–1) on the Kafka shim, mapped to Volant
   Phase 14 `Broker::delete_records` (whole sealed segments only)
2. **DescribeAcls** (29), **CreateAcls** (30), **DeleteAcls** (31) classic
   versions 0–1, mapped to Volant Phase 20/21 ACL store
3. Correct Kafka↔Volant type/operation/permission mapping
4. Advertise keys in ApiVersions; tests + docs honesty

## Non-goals

- Flexible (compact) Kafka versions (v2+)
- Resource pattern types other than **LITERAL** (and **ANY** on filters)
- Host-scoped ACLs (Volant has no host dimension; host always `*`)
- Kafka resource types beyond Topic / Group / Cluster
- Prefix ACLs, DelegationToken, TransactionalId, User resource types
- Incremental delete-by-offset within a live segment

## API summary

| API | Key | Versions | Volant backend |
|-----|-----|----------|----------------|
| DeleteRecords | 21 | 0–1 | `delete_records` (segment truncate) |
| DescribeAcls | 29 | 0–1 | `acls().list` + filter |
| CreateAcls | 30 | 0–1 | `acls().create` |
| DeleteAcls | 31 | 0–1 | filter match → `acls().delete` |

## DeleteRecords wire (classic)

**Request:** `[topics: [name, [partition_index, offset]]] timeout_ms`

**Response:** `throttle_time_ms` + `[name, [partition_index, low_watermark, error_code]]`

- Per-partition ACL: Topic **Delete**
- Unknown topic/partition → `UNKNOWN_TOPIC_OR_PARTITION` (3)
- Not leader → `NOT_LEADER_FOR_PARTITION` (6)
- Offset is the exclusive upper bound for deletion (same as native)

## Kafka ↔ Volant ACL mapping

### Resource type (Kafka int8 → Volant)

| Kafka | Name | Volant |
|------:|------|--------|
| 1 | Any | filter: any |
| 2 | Topic | Topic |
| 3 | Group | Group |
| 4 | Cluster | Cluster |
| other | — | reject |

Cluster resource name: Kafka `kafka-cluster` (and Volant `volant`) both map to
internal `CLUSTER_RESOURCE` (`"volant"`). Describe/Delete responses emit
`kafka-cluster` for Cluster resources.

### Operation (Kafka int8 → Volant)

| Kafka | Name | Volant |
|------:|------|--------|
| 1 | Any | filter: any |
| 2 | All | All |
| 3 | Read | Read |
| 4 | Write | Write |
| 5 | Create | Create |
| 6 | Delete | Delete |
| 7 | Alter | Alter |
| 8 | Describe | Describe |
| 9 | ClusterAction | ClusterAction |
| 10 | DescribeConfigs | Describe (best-effort) |
| 11 | AlterConfigs | Alter (best-effort) |
| 12 | IdempotentWrite | Write (best-effort) |

### Permission type

| Kafka | Name | Volant |
|------:|------|--------|
| 1 | Any | filter: any |
| 2 | Deny | Deny |
| 3 | Allow | Allow |

### Principal

Kafka often uses `User:name`. On create we strip a leading `User:` for storage;
on describe/delete responses we re-prefix `User:`. Bare names are accepted.

### Host & pattern

- Host is **ignored** for matching; always returned as `*`.
- Pattern type: only **LITERAL (3)** and filter **ANY (1)**; PREFIX rejected.

## Authorization for admin APIs

When ACLs are enabled:

| API | Required |
|-----|----------|
| DeleteRecords | Topic Delete on each topic |
| DescribeAcls | Cluster Describe |
| CreateAcls | Cluster Alter |
| DeleteAcls | Cluster Alter |

## Exit criteria

1. ApiVersions advertises 21, 29, 30, 31
2. DeleteRecords truncates sealed segments and returns low watermark
3. CreateAcls → DescribeAcls round-trip with type mapping
4. DeleteAcls filter removes matching entries
5. Unauthorized principal gets Cluster/Topic authorization errors
6. `cargo test --workspace` green

## Honest limitations

- Segment-granularity delete only (same as native Phase 14)
- No host / prefix ACL semantics
- DescribeConfigs/AlterConfigs/IdempotentWrite ops collapse into Describe/Alter/Write
- Flexible Kafka versions not supported
- Cluster name normalized to/from `kafka-cluster`
