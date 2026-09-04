# v0.246 — Kafka AllocateProducerIds key 67 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **AllocateProducerIds** (API key **67**,
version **0** only, always flexible). Wrap `next_producer_id`
(`AtomicU64` on Broker, used by `init_producer_id`).

This is residual **v0.246**. It is **not** KRaft broker-epoch fencing.
BrokerEpoch is parsed and **ignored**. Do **not** touch DescribeQuorum,
ACLs TransactionalId, SyncGroup, AlterReplicaLogDirs, or `group.rs`.

## Goals

1. Advertise `(ApiKey::AllocateProducerIds, 0, 0)` in `SUPPORTED_APIS`.
   Soft length assert `>= 50`.
2. Dispatch key 67 v0 (flexible request header + compact body).
3. Controller only in cluster → else **41** `NOT_CONTROLLER`.
   Single-node: allow (this process is the allocator).
4. Allocate a block: default **1000** ids via `fetch_add`. Return
   `producerIdStart` + `producerIdLen`. Persist `next_id` the same way
   InitProducerId does (`__producer_state`).
5. BrokerEpoch parsed and ignored.
6. ACL: Cluster **ALTER** (same as similar cluster admin:
   UpdateFeatures / UnregisterBroker). Disabled ACLs allow.
7. v1+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft broker-epoch fencing | BrokerEpoch ignored |
| Versions 1+ | Kafka key 67 is v0 only |
| DescribeQuorum / ACLs TransactionalId | Sibling leftovers |
| SyncGroup / AlterReplicaLogDirs / `group.rs` | Sibling leftovers |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Request:

```
brokerId i32
brokerEpoch i64
tagged
```

Response:

```
throttle i32
error i16
producerIdStart i64
producerIdLen i32
tagged
```

## Semantics

```
AllocateProducerIds v0
  │
  ├─ not controller (cluster) → 41, start=0, len=0
  ├─ Cluster ALTER fail → 31, start=0, len=0
  ├─ BrokerId / BrokerEpoch parsed, ignored
  │
  └─ fetch_add(1000) + persist next_id
        └─ 0; producerIdStart + producerIdLen=1000
```

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v246_allocate_producer_ids -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **67** min=0 max=0; `SUPPORTED_APIS.len() >= 50` |
| Single-node allocate | start≥0, len=1000; second call start = first+1000 |
| not controller | **41** |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 67 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode + lib tests |
| `crates/volant-broker/src/broker/txn.rs` | `Broker::allocate_producer_ids` |
| `crates/volant-broker/tests/v246_allocate_producer_ids.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 67 v0 |
| `docs/V246_SPEC.md` | This spec |

## Honesty leftovers

- **Not** KRaft broker-epoch fencing. BrokerEpoch is parsed and
  ignored. No incarnation / DirectoryId.
- Block size is a fixed **1000** (not a request field).
- Persist follows InitProducerId: `next_id` is written under
  `{data_dir}/__producer_state`. The allocated ids are **not**
  inserted into the per-pid producer map (InitProducerId still owns
  that).
- ACL is Cluster **ALTER**, not Kafka `CLUSTER_ACTION`.
- `group.rs` `SUPPORTED_APIS.len()==49` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V244_SPEC.md](./V244_SPEC.md) — previous Kafka admin wrap
