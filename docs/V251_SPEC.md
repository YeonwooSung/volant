# v0.251 — Kafka AssignReplicasToDirs key 73 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **AssignReplicasToDirs** (API key **73**,
version **0** only, always flexible). Honest reject: parse the request
and return per-partition **42** `INVALID_REQUEST`
(`single data_dir; directory assignment not supported`). Do **not**
move files. Do **not** invent DirectoryId storage. Not KRaft.

This is residual **v0.251**. Volant has a single `data_dir` (same
honesty as AlterReplicaLogDirs **34** / v0.249). Official Kafka
`flexibleVersions` is **0+**. Do **not** touch WriteTxnMarkers,
ListClientMetricsResources, GetTelemetrySubscriptions,
TxnOffsetCommit, AlterReplicaLogDirs behavior, or `group.rs`.

## Goals

1. Advertise `(ApiKey::AssignReplicasToDirs, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 53`.
2. Dispatch key 73 v0 (always flexible request header + compact body).
3. Parse `brokerId`, `brokerEpoch`, `directories[] { id, topics[] {
   topicId, partitions[] } }`.
4. Every partition → **42** `INVALID_REQUEST`. Nothing moved. TopicId
   is resolved via Volant deterministic helpers when present; unknown
   ids are still echoed with **42** (do not fail the whole request).
5. Controller is **not** required (local dirs). BrokerId / BrokerEpoch
   parsed and ignored (not KRaft fencing).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → top-level
   **31** `CLUSTER_AUTHORIZATION_FAILED`, empty directories.
7. v1+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Multi `log.dirs` / file move | Single `data_dir`; honest reject |
| DirectoryId storage | Not KRaft |
| KRaft broker-epoch fencing | BrokerEpoch ignored |
| Versions 1+ | Kafka key 73 is v0 only |
| AlterReplicaLogDirs changes | Sibling leftover; already shipped |
| WriteTxnMarkers / telemetry leftovers | Sibling leftovers |
| `group.rs` hard `== 52` asserts | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Request:

```
brokerId i32
brokerEpoch i64
directories[] {
  id uuid
  topics[] {
    topicId uuid
    partitions[] i32
    tagged
  }
  tagged
}
tagged
```

Response:

```
throttleTimeMs i32
errorCode i16
directories[] {
  id uuid
  topics[] {
    topicId uuid
    partitions[] {
      partitionIndex i32
      errorCode i16
      tagged
    }
    tagged
  }
  tagged
}
tagged
```

## Semantics

```
AssignReplicasToDirs v0
  │
  ├─ Cluster ALTER fail → top-level 31, empty directories
  ├─ Controller not required
  ├─ BrokerId / BrokerEpoch parsed, ignored
  │
  └─ per partition
          └─ 42 INVALID_REQUEST
             ("single data_dir; directory assignment not supported")
             files unmoved; no DirectoryId stored
```

- Response throttle is always 0.
- Top-level `errorCode` is **0** on the reject path; per-partition
  **42**. ACL deny uses top-level **31** and does not echo directories.
- Official Apache Kafka first flexible version is **0**.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v251_assign_replicas_to_dirs -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **73** min=0 max=0; `SUPPORTED_APIS.len() >= 53` |
| Assign any DirectoryId | per-partition **42**; files unmoved |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 73 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject; no file move |
| `crates/volant-broker/tests/v251_assign_replicas_to_dirs.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 73 v0 |
| `docs/V251_SPEC.md` | This spec |

## Honesty leftovers

- **Not** multi-log.dirs. One `data_dir` only.
- Files are never moved. DirectoryId is echoed, not stored.
- **Not** KRaft. BrokerEpoch is parsed and ignored. No incarnation.
- TopicId uses Volant's deterministic `volant` + u32 mapping, not
  KRaft random UUIDs.
- ACL is Cluster **ALTER**, not Kafka `CLUSTER_ACTION`.
- `group.rs` / v206 / v225 / v228 / v233 `SUPPORTED_APIS.len()==52`
  assertions are intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V249_SPEC.md](./V249_SPEC.md) — AlterReplicaLogDirs (sibling reject)
