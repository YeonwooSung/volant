# v0.250 — Kafka WriteTxnMarkers key 27 v0–1

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **WriteTxnMarkers** (API key **27**,
versions **0–1**; v1 first flexible). Wrap the existing replica-side
control-batch + `__txn_markers` path. This is **not** EndTxn, **not**
2PC, **not** a coordinator. CoordinatorEpoch is parsed and **ignored**.

This is residual **v0.250**. EndTxn already calls
`append_txn_control_markers` + persist. WriteTxnMarkers writes
COMMIT/ABORT control batches for the partitions listed in the request
(same `txn_control_message` / `produce_one` / `flush` as Phase 89)
and persists a matching soft marker. Do **not** touch EndTxn /
InitProducerId / AddPartitions / TxnOffsetCommit semantics, or
`group.rs`.

## Goals

1. Advertise `(ApiKey::WriteTxnMarkers, 0, 1)` in `SUPPORTED_APIS`.
   Soft length assert `>= 53`. Do **not** change hard `== 52` asserts
   in `group.rs` / v206 / v225 / v228 / v233.
2. Dispatch key 27 v0 (classic header) / v1 (flex header + compact).
3. Parse `markers[] { producerId, producerEpoch, transactionResult,
   topics[] { name, partitions[] }, coordinatorEpoch }`.
   v1 compact + tagged buffers after each nested struct + top-level.
4. Response echoes the request shape: `throttleTimeMs=0`,
   `markers[] { producerId, topics[] { name, partitions[] {
   partitionIndex, errorCode } } }`. v1 compact + tags.
5. Per listed partition:
   - Unknown topic → **3** `UNKNOWN_TOPIC_OR_PARTITION`.
   - Else write one control batch via existing
     `txn_control_message` + `produce_one` + `flush`. Persist a soft
     aborted/committed marker the same way EndTxn does
     (`push_aborted_marker` / persist `__txn_markers`). Do **not**
     call `end_txn`.
   - Success → **0**.
6. Controller is **not** required (replica-local apply). Single-node
   allowed.
7. ACL: Topic **WRITE** per named topic, or Cluster **ALTER**.
   Disabled ACLs allow. Denied → per-partition **29**
   `TOPIC_AUTHORIZATION_FAILED`.
8. v2+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| EndTxn / InitProducerId / AddPartitions / TxnOffsetCommit | Sibling txn coordinator APIs |
| 2PC / coordinator finalize | WriteTxnMarkers is replica-local apply |
| AssignReplicasToDirs / ListClientMetricsResources / GetTelemetrySubscriptions | Sibling leftovers |
| `group.rs` `SUPPORTED_APIS.len()==52` | Orchestrator bumps after merge |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 classic / v1 flexible)

Request:

```
markers[] {
  producerId i64
  producerEpoch i16
  transactionResult bool   // true=commit, false=abort
  topics[] {
    name string            // compact in v1
    partitions[] i32       // compact array in v1
    tagged                 // v1
  }
  coordinatorEpoch i32     // parse, ignore
  tagged                   // v1
}
tagged                     // v1
```

Response:

```
throttleTimeMs i32 = 0
markers[] {
  producerId i64
  topics[] {
    name
    partitions[] {
      partitionIndex i32
      errorCode i16
      tagged             // v1
    }
    tagged               // v1
  }
  tagged                 // v1
}
tagged                   // v1
```

## Semantics

```
WriteTxnMarkers v0–1
  │
  ├─ Cluster ALTER fail + Topic WRITE fail → per-partition 29
  ├─ Controller not required
  │
  └─ per listed partition
          ├─ unknown topic → 3
          └─ else
                txn_control_message + produce_one + flush
                abort → push_aborted_marker (open written ranges)
                persist __txn_markers
                → 0
```

- Response throttle is always 0.
- CoordinatorEpoch is parsed and ignored.
- Does **not** call `end_txn` (open / prepared coordinator state
  is left alone).
- Official Kafka first flexible version is **1**.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v250_write_txn_markers -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **27** min=0 max=1; `SUPPORTED_APIS.len() >= 53` |
| Existing topic/partition | per-partition **0**; control batch on log (or `__txn_markers` updated); v1 works |
| Unknown topic | **3** |
| v2 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 27 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0–1 + v>=1 flex header |
| `crates/volant-broker/src/kafka/txn.rs` | parse + encode; ACL Topic WRITE / Cluster ALTER |
| `crates/volant-broker/src/broker/txn.rs` | `Broker::write_txn_markers` |
| `crates/volant-broker/tests/v250_write_txn_markers.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 27 v0–1 |
| `docs/V250_SPEC.md` | This spec |

## Honesty leftovers

- **Not** EndTxn. Open / prepared coordinator state is not finalized.
- **Not** 2PC. CoordinatorEpoch is ignored.
- Soft abort markers are pushed only when this producer already has
  open write-through ranges on the listed partition; otherwise the
  control batch + `__txn_markers` rewrite is the apply.
- `group.rs` `SUPPORTED_APIS.len()==52` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [PHASE89_SPEC.md](./PHASE89_SPEC.md) — control batches
- [V249_SPEC.md](./V249_SPEC.md) — previous Kafka leftover
