# v0.236 — Kafka ElectLeaders key 43 v0–1

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ElectLeaders** (API key **43**, versions
**0–1**, v1 flexible) and wrap existing `elect_leader(replicas, isr,
live)` from `cluster/assignment.rs`. Persist via the same assignment
mutation path as reassign (`complete_assignment_mutation`).

This is residual **v0.236**. It is **not** Kafka preferred-leader
election with `preferred.leader`. It is **not** a live replica copy.
Do **not** touch DescribeLogDirs, DescribeTopicPartitions, SCRAM,
native ListOffsets, or `group.rs`.

## Goals

1. Advertise `(ApiKey::ElectLeaders, 0, 1)` in `SUPPORTED_APIS`.
2. Dispatch key 43 v0 (classic) and v1 (flexible request header +
   compact body).
3. ElectionType **0** (preferred; v0 implied): new leader = first
   replica in ISR ∩ live. Already leader → per-partition **0**. A
   different live ISR replica → write assignment + wait/rollback.
4. ElectionType **1** (unclean): **do not** elect outside ISR.
   Per-partition **87** `ELIGIBLE_LEADERS_NOT_AVAILABLE`.
5. Not controller → top-level **41**.
6. Single-node / no cluster: per-partition **0** if local leader else
   **3**.
7. TimeoutMs parsed, ignored.
8. ACL: Topic **ALTER**.
9. v2+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Unclean election outside ISR | Honest refuse **87** |
| Kafka `preferred.leader` / replica order admin | Wrap `elect_leader` only |
| Live replica copy | Same leftover as opcode 114 / key 45 |
| DescribeLogDirs / DescribeTopicPartitions | Sibling leftovers |
| SCRAM / native ListOffsets / `group.rs` | Sibling leftovers |
| Crate 0.3.0 | Stays 0.2.0 |

## Semantics

```
ElectLeaders v0–1
  │
  ├─ not controller → top-level 41, empty results
  ├─ TimeoutMs ignored
  ├─ ElectionType v1+ (v0 implied 0)
  │
  └─ per partition
          ├─ ACL Topic ALTER fail → 29
          ├─ unknown topic/partition → 3
          ├─ ElectionType 1 (unclean) → 87, leader unchanged
          └─ ElectionType 0 (preferred)
                └─ elect_leader(ISR∩live) + complete_assignment_mutation
                      ├─ already leader → 0 (no write)
                      ├─ new live ISR replica → write + wait; ok → 0
                      ├─ no eligible → 87
                      └─ majority miss → 19
```

- v0 classic arrays; v1 compact + tags. Top-level error on both.
- `topics = null` → all assigned partitions (cluster) / all local
  (single-node).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v236_elect_leaders -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **43** min=0 max=1; `SUPPORTED_APIS.len() >= 43` |
| Single-node elect current | per-partition **0** |
| Cluster preferred already-leader | **0** |
| unclean type 1 | **87**, leader unchanged |
| not controller | **41** |
| v2 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 43 + error 87 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0–1 |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode + lib tests |
| `crates/volant-broker/src/broker/topics.rs` | `Broker::elect_preferred_leader` |
| `crates/volant-broker/tests/v236_elect_leaders.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 43 v0–1 |
| `docs/V236_SPEC.md` | This spec |

## Honesty leftovers

- **Not** Kafka preferred-leader election (`preferred.leader` config).
  Preferred here is `elect_leader`: first replica that is in
  ISR ∩ live.
- **Not** unclean election. Type **1** is refused with **87**.
- **Not** a live replica copy. Leader change is an assignment write
  (same path as key 45 / opcode 114).
- **TimeoutMs** is parsed and ignored.
- `group.rs` `stays_42` assertion is intentionally untouched.

## Related

- [V225_SPEC.md](./V225_SPEC.md) — Kafka AlterPartitionReassignments
  key 45
- [V18_SPEC.md](./V18_SPEC.md) — native reassign (opcode 114)
- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
