# v0.235 — Kafka DescribeLogDirs key 35 v0–1

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **DescribeLogDirs** (API key **35**),
versions **0–1** (v1 flexible). Wrap **local** partition logs: path =
broker `data_dir`, size = `Log::total_size()`, offsetLag = LEO−HWM
(0 if unknown), isFuture = false.

This is residual **v0.235**. It is **not** remote replica dirs, **not**
`log.dir` vs `log.dirs` multi-path. Single-node and cluster: only
partitions this process has open. Do **not** touch ElectLeaders,
DescribeTopicPartitions, SCRAM, native ListOffsets. Do **not** edit
`group.rs`.

## Goals

1. Advertise `(ApiKey::DescribeLogDirs, 0, 1)` in `SUPPORTED_APIS`.
2. Dispatch key 35 v0 (classic header) / v1 (flexible compact + tags).
3. `topics = null` → every local open partition.
4. Named topic with empty `partitions` → all local partitions of that
   topic.
5. Path = broker `data_dir`; one dir; isFuture always false.
6. ACL: Cluster **DESCRIBE**, or Topic **DESCRIBE** per named topic.
   Disabled ACLs allow.
7. Unknown topic → skip or empty (do not crash). Optional per-topic
   **3**.
8. v2+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Remote replica dirs | Only partitions this process has open |
| Multi `log.dirs` | Single `data_dir` |
| AlterReplicaLogDirs (34) | Orthogonal |
| ElectLeaders / DescribeTopicPartitions | Sibling leftovers |
| SCRAM / native ListOffsets / `group.rs` | Sibling leftovers |
| Versions 2+ (official Kafka flex) | Volant treats v1 as first flexible |
| Crate 0.3.0 | Stays 0.2.0 |

## Semantics

```
DescribeLogDirs v0–1
  │
  ├─ topics = null
  │     ├─ Cluster DESCRIBE fail → one dir, error 31, empty topics
  │     └─ else → one dir, every local open partition
  │
  ├─ topics = [] → one dir, empty topics
  │
  └─ named topic
          ├─ Topic DESCRIBE fail (and no Cluster DESCRIBE) → skip
          ├─ empty partitions → all local partitions of that topic
          ├─ unknown topic / not open here → skip or empty
          └─ else → size, offsetLag = LEO−HWM, isFuture false
```

- v0 classic arrays; v1 compact + tags.
- Response throttle is always 0.
- Not multi-log.dirs; path is `data_dir.display()`.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v235_describe_log_dirs -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **35** min=0 max=1 |
| Create topic, produce, DescribeLogDirs null topics | one dir, size > 0, isFuture false |
| Unknown topic | no crash; 3 or empty |
| v2 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 35 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch + v>=1 flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode |
| `crates/volant-broker/src/broker/topics.rs` | `local_log_dir_rows` |
| `crates/volant-broker/tests/v235_describe_log_dirs.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 35 v0–1 |
| `docs/V235_SPEC.md` | This spec |

## Honesty leftovers

- **Not** remote replica log dirs.
- **Not** Kafka `log.dirs` multi-path (one `data_dir` only).
- Official Apache Kafka first flexible version is **2**; Volant
  advertises **0–1** with v1 flexible (this residual).
- v3 top-level `ErrorCode` is not implemented.
- `group.rs` `stays_42` assertion is intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V233_SPEC.md](./V233_SPEC.md) — previous Kafka admin residual
