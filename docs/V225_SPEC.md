# v0.225 — Kafka AlterPartitionReassignments key 45 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **AlterPartitionReassignments** (API key
**45**, version **0** only, always flexible) and wrap native
`Broker::reassign_partitions` (opcode **114**) +
`complete_assignment_mutation`. Unfreezes `SUPPORTED_APIS` from 38 →
**39**.

This is residual **v0.225**. It is **not** Kafka live reassignment, **not**
a `TimeoutMs` wait, and **not** ListPartitionReassignments (key **46**).
Do **not** invent a cancel log. Do **not** grow homemade 154.

## Goals

1. Advertise `(ApiKey::AlterPartitionReassignments, 0, 0)` in
   `SUPPORTED_APIS`.
2. Dispatch key 45 v0 (flexible request header + compact body).
3. Non-null `replicas` → native `reassign_partitions` for that
   partition (same apply as opcode 114) + assignment wait/rollback.
4. `replicas = null` (cancel): no pending reassignment → per-partition
   **83** `NO_REASSIGNMENT_IN_PROGRESS`.
5. Top-level **41** `NOT_CONTROLLER` when not controller.
6. Per-partition unknown topic / bad replicas.
7. ACL: Topic **ALTER** (same as Kafka CreatePartitions).

## Non-goals

| Deferred | Why |
|----------|-----|
| Key **46** ListPartitionReassignments | Out of scope |
| Versions 1+ | Kafka key 45 is v0 only |
| Live log copy / throttled wait | Native apply is instant; new replicas start empty |
| Pending-reassignment cancel log | Do not invent one |
| Homemade 154 growth | Wrap existing `complete_assignment_mutation` only |
| Crate 0.3.0 | Stays 0.2.0 |

## Semantics

```
AlterPartitionReassignments v0
  │
  ├─ not controller → top-level 41, empty responses
  ├─ TimeoutMs ignored
  │
  └─ per partition
          ├─ ACL Topic ALTER fail → 29
          ├─ replicas = null
          │     ├─ unknown topic/partition → 3
          │     └─ else → 83 (no pending cancel)
          └─ replicas = [...]
                └─ native reassign + complete_assignment_mutation
                      ├─ ok → 0
                      ├─ unknown topic/partition → 3
                      ├─ bad replica ids → 39
                      └─ majority miss → 19
```

- Compact topics / partitions; **nullable** replica list.
- Empty (non-null) replica list is native auto-place (opcode 114).
- Apply is instant: there is no in-progress reassignment to cancel.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v225_alter_partition_reassignments -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **45** min=0 max=0; `SUPPORTED_APIS.len()==39` |
| v0 reassign non-null replicas | native path; assignment generation bumps |
| `replicas = null` | per-partition **83** |
| unknown topic / replica not in membership | **3** / **39** |
| v1 | **35** UnsupportedVersion |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 45 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode + lib tests |
| `crates/volant-broker/tests/v225_alter_partition_reassignments.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 45 v0 |
| `docs/V225_SPEC.md` | This spec |

## Honesty leftovers

- **Not** Kafka live reassignment. New replicas start empty (LEO=0);
  catch-up is ReplicaFetch / ISR expand, same as native 114.
- **TimeoutMs** is parsed and ignored (apply is instant).
- Key **46** is not advertised.
- Cancel does **not** write a cancel log; **83** means nothing was
  pending.
- Native opcode 114 ACL remains Cluster ALTER; this Kafka key uses
  Topic ALTER (CreatePartitions).

## Related

- [V18_SPEC.md](./V18_SPEC.md) — native reassign (opcode 114)
- [V40_SPEC.md](./V40_SPEC.md) — `complete_assignment_mutation`
- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
