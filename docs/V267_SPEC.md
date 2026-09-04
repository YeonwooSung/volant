# v0.267 — Kafka FetchSnapshot key 59 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **FetchSnapshot** (API key **59**,
version **0** only, always flexible). Volant is **not** a KRaft
controller and does **not** serve KRaft metadata snapshots. Parse
the official v0 body and reject with top-level **42**
`INVALID_REQUEST` (`not KRaft snapshot`). Empty `topics[]`. Do
**not** wrap native openraft InstallSnapshot opcodes 112/113 —
that is a different protocol.

This is residual **v0.267**, not Phase 155. Official Apache Kafka
`FetchSnapshotRequest.json` uses apiKey **59**. Envelope is official
**58** (sibling leftover — not advertised). DescribeCluster stays
**60**. Official field layout is used.

## Goals

1. Advertise `(ApiKey::FetchSnapshot, 0, 0)` in `SUPPORTED_APIS`
   (numeric order after UpdateFeatures 57, before DescribeCluster
   60). Soft length assert `>= 65`. Do **not** change hard `== 64`
   asserts in `group.rs`, `v206_*`, `v225_*`, `v228_*`, `v233_*`.
2. Always flexible (Kafka `flexibleVersions: 0+`). Dispatch v0 only.
   v1+ → **35**.
3. Official FetchSnapshotRequest v0 (confirmed
   `FetchSnapshotRequest.json`): `clusterId` is a request-level
   tagged compact nullable string (tag 0). Untagged body:
   `replicaId` i32, `maxBytes` i32, `topics[]` compact with inline
   `SnapshotId { EndOffset i64, Epoch i32 }` then `Position` i64.
   Parse enough to consume the body without panicking. Do **not**
   persist any field.
4. Response matches official `FetchSnapshotResponse.json` v0:
   throttle **0**, top-level error **42**, empty `topics[]`. Do not
   echo request topics. No snapshot bytes. Size / Position /
   UnalignedRecords are not written because topics is empty.
5. Controller is **not** required (local reject **42** so
   single-node tests stay simple).
6. ACL: Cluster **DESCRIBE** (this is a fetch). Disabled ACLs
   allow. Denied → top-level **31**, empty topics.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft metadata snapshot / `__cluster_metadata` bytes | Not a KRaft controller |
| Wrap native InstallSnapshot 112/113 | Different protocol (openraft) |
| Envelope 58, BrokerHeartbeat 63, ControllerRegistration 70, ConsumerGroupHeartbeat 68 | Sibling leftovers |
| Official v1 ReplicaDirectoryId / NodeEndpoints | Advertised max is 0 |
| join-set wait, unclean election, live reassignment, txn-topic default-on, Kafka Fetch group tags | Sibling leftovers |
| `group.rs` hard `== 64` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`FetchSnapshotRequest.json`;
`flexibleVersions: 0+`):

```
ReplicaId i32                  // default -1
MaxBytes i32
Topics[] {
  Name compact string
  Partitions[] {
    Partition i32
    CurrentLeaderEpoch i32
    SnapshotId { EndOffset i64, Epoch i32 }   // inline nested struct
    Position i64
    tagged                                    // ReplicaDirectoryId is v1+ tag 0
  }
  tagged
}
tagged                         // ClusterId tag 0 lives here
```

ClusterId is a tagged compact nullable string (tag 0) in the
request-level tag buffer, not inline. ReplicaDirectoryId /
NodeEndpoints are v1+ — out of advertised range.

Parse loosely. If a field is missing, stop parsing that level and
still return 42. Never panic.

Official response (`FetchSnapshotResponse.json` v0; no
`errorMessage`; CurrentLeader is a per-partition tagged field,
unused because topics is empty):

```
throttleTimeMs i32 = 0
errorCode i16 = 42            // or 31 if Cluster DESCRIBE denied
topics[] compact = empty
tagged
```

## Semantics

```
FetchSnapshot v0
  │
  ├─ Cluster DESCRIBE fail → 31, empty topics
  ├─ Controller not required
  │
  └─ else → throttle 0, error 42 INVALID_REQUEST
            (not KRaft snapshot)
            empty topics[]
            nothing persisted
            no snapshot files written
            openraft state unchanged
            InstallSnapshot 112/113 is not called
```

- Response throttle is always 0.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official).
- Official `validVersions` is 0–1; Volant advertises 0 only.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v267_fetch_snapshot -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **59** min=0 max=0; key **60** still listed; `SUPPORTED_APIS.len() >= 65` |
| v0 fetch snapshot (ReplicaId=-1, MaxBytes=1024, one topic/partition, SnapshotId {0,0}, Position 0) | throttle **0**, top-level **42**, empty topics; no snapshot files; openraft state unchanged |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 59 + `SUPPORTED_APIS` + soft test |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v267_fetch_snapshot.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 59 v0 reject |
| `docs/V267_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a KRaft controller. No metadata snapshot bytes.
- Does **not** wrap native InstallSnapshot 112/113 (different
  protocol).
- Official apiKey is **59**. Official first flex is **0+**; Volant
  v0 is flexible (matches official).
- Official validVersions 0–1; Volant advertises 0 only.
- `group.rs` hard `== 64` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V263_SPEC.md](./V263_SPEC.md) — BrokerRegistration reject
- [V245_SPEC.md](./V245_SPEC.md) — DescribeQuorum wrap of openraft
