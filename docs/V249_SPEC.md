# v0.249 — Kafka AlterReplicaLogDirs key 34 v0–1

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **AlterReplicaLogDirs** (API key **34**,
versions **0–1**; v1 flexible). Honest reject: parse the request and
return per-partition **42** `INVALID_REQUEST`
(`single data_dir; replica move not supported`). Do **not** move
files. Do **not** invent multi-log.dirs. DescribeLogDirs (35)
unchanged.

This is residual **v0.249**. Volant has a single `data_dir`. Official
Apache Kafka first flexible version is **2**; Volant advertises **0–1**
with v1 flexible (same residual as DescribeLogDirs). Do **not** touch
DescribeQuorum, AllocateProducerIds, ACLs, SyncGroup, or `group.rs`.

## Goals

1. Advertise `(ApiKey::AlterReplicaLogDirs, 0, 1)` in `SUPPORTED_APIS`.
   Soft length assert `>= 50`.
2. Dispatch key 34 v0 (classic header) / v1 (flexible compact + tags).
3. Parse `dirs[] { path, topics[] { name, partitions[] } }`.
4. Every partition → **42** `INVALID_REQUEST` (or **57**
   `LOG_DIR_NOT_FOUND` if added). Nothing moved.
5. Controller is **not** required (local dirs).
6. ACL: Cluster **ALTER**, or Topic **ALTER** per named topic.
   Disabled ACLs allow.
7. v2+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Multi `log.dirs` / file move | Single `data_dir`; honest reject |
| Official Kafka v2 flexible | Residual advertises 0–1; v1 is flex |
| DescribeLogDirs changes | Sibling leftover; already shipped |
| DescribeQuorum / AllocateProducerIds | Sibling leftovers |
| ACLs / SyncGroup / `group.rs` | Sibling leftovers |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 classic / v1 flexible)

Request:

```
dirs[] {
  path string            // compact string in v1
  topics[] {
    name string          // compact string in v1
    partitions[] i32     // compact array in v1
    tagged               // v1
  }
  tagged                 // v1
}
tagged                   // v1
```

Response (official v0/v1: topics, not dirs; no error message field):

```
throttle i32
topics[] {
  name string            // compact string in v1
  partitions[] {
    partition i32
    error i16
    tagged               // v1
  }
  tagged                 // v1
}
tagged                   // v1
```

## Semantics

```
AlterReplicaLogDirs v0–1
  │
  ├─ Cluster ALTER fail + Topic ALTER fail → per-partition 29
  ├─ Controller not required
  │
  └─ per partition
          └─ 42 INVALID_REQUEST
             ("single data_dir; replica move not supported")
             files unmoved; destination dir not created
```

- Response throttle is always 0.
- Official response has no per-partition error message field; the
  message is documentation / honesty only.
- Official Apache Kafka first flexible version is **2**; Volant
  advertises **0–1** with v1 flexible.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v249_alter_replica_log_dirs -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **34** min=0 max=1; `SUPPORTED_APIS.len() >= 50` |
| Alter any path | per-partition **42** (or **57**); files unmoved |
| v2 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 34 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0–1 + v>=1 flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject; no file move |
| `crates/volant-broker/tests/v249_alter_replica_log_dirs.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 34 v0–1 |
| `docs/V249_SPEC.md` | This spec |

## Honesty leftovers

- **Not** multi-log.dirs. One `data_dir` only.
- Files are never moved, even when `path` equals the current
  `data_dir`.
- Official Kafka first flexible version is **2**; Volant v1 is
  flexible (same residual as DescribeLogDirs).
- Official v0/v1 response has no error message field.
- `group.rs` `SUPPORTED_APIS.len()==49` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V235_SPEC.md](./V235_SPEC.md) — DescribeLogDirs (sibling read path)
- [V244_SPEC.md](./V244_SPEC.md) — previous Kafka admin reject
