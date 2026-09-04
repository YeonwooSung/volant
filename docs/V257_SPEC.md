# v0.257 — Kafka AlterPartition key 56 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **AlterPartition** (API key **56**, version
**0** only, always flexible). Wrap `Broker::apply_leader_isr_update`
(same path as native ISR opcodes 94/95).

This is residual **v0.257**. It is **not** KRaft NewIsrEpoch / ELR /
DirectoryId. BrokerEpoch is parsed and **ignored**. Controller only in
cluster (**41**). Single-node is a no-op **0**. Do **not** touch
PushTelemetry, OffsetFetch, DelegationToken, ElectLeaders, live
reassignment, unclean election, or `group.rs`.

## Goals

1. Advertise `(ApiKey::AlterPartition, 0, 0)` in `SUPPORTED_APIS`.
   Soft length assert `>= 57`. Do **not** change hard `== 56` asserts.
2. Always flexible (`flexibleVersions: 0+`). Dispatch v0 only.
   v1+ → **35**.
3. Official Kafka AlterPartition v0 (3.7 schema): TopicName compact
   string + NewIsr `[]int32`. TopicId is v2, LeaderRecoveryState is v1,
   NewIsrWithEpochs is v3 — not parsed.
4. For each partition call
   `apply_leader_isr_update(topic, partition, leader_id=brokerId,
   leader_epoch, isr=newIsr, generation_hint=partitionEpoch)`.
5. Map native errors: 0→0, NotController→**41**, NotFound→**3**,
   InvalidArg→**42**, NotLeaderForPartition→**6**,
   InvalidProducerEpoch→**74** `FENCED_LEADER_EPOCH`.
6. Response (flex, official v0): throttle=0, top-level error,
   topics/partitions with `errorCode`, current
   `leaderId`/`leaderEpoch`/`isr[]`/`partitionEpoch` after apply (zeros
   on error). No ELR.
7. Cluster + not controller: per-partition **41** (native already does
   this). Official response also has a top-level error (used for ACL
   **31**).
8. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft NewIsrWithEpochs / ELR / DirectoryId | Broker / ISR epochs ignored |
| Versions 1+ | LeaderRecoveryState / TopicId / NewIsrEpochs |
| ElectLeaders / live reassignment / unclean | Sibling leftovers |
| PushTelemetry / OffsetFetch / DelegationToken | Sibling leftovers |
| `group.rs` hard asserts | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official Kafka 3.7 `AlterPartitionRequest.json` v0:

```
brokerId i32
brokerEpoch i64            // parsed, ignored
topics[] {
  topicName compact string // v0–1; TopicId is v2+
  partitions[] {
    partitionIndex i32
    leaderEpoch i32
    newIsr[] i32           // v0–2; NewIsrWithEpochs is v3+
    partitionEpoch i32     // generation hint
    tagged
  }
  tagged
}
tagged
```

Official Kafka 3.7 `AlterPartitionResponse.json` v0:

```
throttleTimeMs i32 = 0
errorCode i16              // top-level (ACL 31; else 0)
topics[] {
  topicName compact string
  partitions[] {
    partitionIndex i32
    errorCode i16
    leaderId i32
    leaderEpoch i32
    isr[] i32
    partitionEpoch i32
    tagged
  }
  tagged
}
tagged
```

## Semantics

```
AlterPartition v0
  │
  ├─ Cluster ALTER fail → top + per-partition 31, zeros
  ├─ brokerId / partition / ISR ids < 0 → per-partition 42
  │
  └─ apply_leader_isr_update(...)
        ├─ 0 → 0 + current assignment (metadata on single-node)
        ├─ NotController → 41
        ├─ NotFound → 3
        ├─ InvalidArg → 42
        ├─ NotLeaderForPartition → 6
        └─ InvalidProducerEpoch → 74
```

- Response throttle is always 0.
- Single-node `apply_leader_isr_update` is already a no-op **0**.
- PartitionEpoch is passed through as `generation_hint` (native ignores it).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v257_alter_partition -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **56** min=0 max=0; `SUPPORTED_APIS.len() >= 57` |
| Single-node local leader + ISR `[local]` | per-partition **0** |
| Cluster non-controller | **41** |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 56 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse official v0; wrap apply |
| `crates/volant-broker/tests/v257_alter_partition.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 56 v0 |
| `docs/V257_SPEC.md` | This spec |

## Honesty leftovers

- **Not** KRaft. BrokerEpoch and NewIsr broker epochs are ignored.
  No ELR, no DirectoryId, no unclean recovery.
- Official Kafka 4.0 removed v0–1 (`validVersions` 2–3, TopicId only).
  Volant advertises and implements historical **v0** (TopicName).
- `group.rs` `SUPPORTED_APIS.len()==56` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V246_SPEC.md](./V246_SPEC.md) — previous always-flex wrap
