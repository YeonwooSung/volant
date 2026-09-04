# v0.228 — Kafka ListPartitionReassignments key 46 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ListPartitionReassignments** (API key
**46**, version **0** only, always flexible). Unfreezes `SUPPORTED_APIS`
from 39 → **40**.

Volant reassignment apply is **instant** (native opcode 114 / key 45).
There is **no** in-progress reassignment log. Honest list: current
replica set as `replicas`, **empty** `addingReplicas` /
`removingReplicas`.

This is residual **v0.228**. It is **not** live reassignment progress.
Do **not** invent a pending log. Do **not** grow homemade 154. Do **not**
change key 45 semantics. Do **not** park Join. Do **not** change txn
schemas.

## Goals

1. Advertise `(ApiKey::ListPartitionReassignments, 0, 0)` in
   `SUPPORTED_APIS`.
2. Dispatch key 46 v0 (flexible request header + compact body).
3. `topics = null` → every known topic/partition.
4. `topics = []` → empty response topics.
5. Named topic with empty `PartitionIndexes` → all partitions of that
   topic.
6. Current assignment as `replicas`; adding/removing empty; error 0.
7. Top-level **41** `NOT_CONTROLLER` when not controller.
8. Unknown topic / bad partition → that partition (or skip topic)
   **3** `UNKNOWN_TOPIC_OR_PARTITION`. Do not fail the whole request.
9. ACL: Cluster **DESCRIBE** when `topics=null`. Topic **DESCRIBE**
   when specific topics. Disabled ACLs allow. Fail → top-level or
   per-topic **29**.

## Non-goals

| Deferred | Why |
|----------|-----|
| Live adding/removing progress | Apply is instant; no pending log |
| Versions 1+ | Kafka key 46 is v0 only |
| Pending-reassignment store | Do not invent one |
| Homemade 154 growth | Read-only list of current assignment |
| Key 45 semantic change | Alter stays native opcode 114 wrap |
| Parked Join / txn schemas | Sibling leftovers |
| Crate 0.3.0 | Stays 0.2.0 |

## Semantics

```
ListPartitionReassignments v0
  │
  ├─ not controller → top-level 41, empty topics
  ├─ TimeoutMs ignored
  │
  ├─ topics = null
  │     ├─ Cluster DESCRIBE fail → top-level 29
  │     └─ else → every known partition, adding/removing empty
  │
  ├─ topics = [] → empty topics
  │
  └─ named topic
          ├─ Topic DESCRIBE fail → requested (or all known) partitions 29
          ├─ empty PartitionIndexes → all partitions of that topic
          ├─ unknown topic / bad partition → that partition (or skip) 3
          └─ else → current replicas, adding/removing empty
```

- Compact nullable topics array; compact partition indexes.
- Replica ids from live assignment / metadata (same source key 45 uses
  for existence checks).
- Single-node no-cluster: still list local topics (controller_id =
  node).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v225_alter_partition_reassignments -- --test-threads=1
cargo test -p volant-broker --test v228_list_partition_reassignments -- --test-threads=1
cargo test -p volant-client --test v206_sync_group -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **46** min=0 max=0; `SUPPORTED_APIS.len()==40`; key 45 still there |
| Create topic + list that partition | replicas = current assignment; adding/removing empty; error 0 |
| topics=null | every local/assigned partition, adding/removing empty |
| unknown topic | per-partition or per-topic **3** |
| v1 | **35** UnsupportedVersion |
| non-controller in cluster | top-level **41** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 46 + `SUPPORTED_APIS` 40 |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch + flexible header |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode + lib tests |
| `crates/volant-broker/tests/v228_list_partition_reassignments.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 46 v0 |
| `docs/V228_SPEC.md` | This spec |

## Honesty leftovers

- **Not** live reassignment progress. There is no adding/removing set
  because native apply (opcode 114 / key 45) is instant.
- **TimeoutMs** is parsed and ignored.
- No pending-reassignment log. Do not invent one.
- Key 45 semantics unchanged. Join parked work and txn schemas
  untouched.

## Related

- [V225_SPEC.md](./V225_SPEC.md) — Kafka AlterPartitionReassignments
  key 45
- [V18_SPEC.md](./V18_SPEC.md) — native reassign (opcode 114)
- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
