# v0.237 — Kafka DescribeTopicPartitions key 75 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **DescribeTopicPartitions** (API key **75**,
version **0** only, always flexible). Wrap existing
`Broker::metadata` (same leaders / ISR / epochs / TopicId as Metadata).

This is residual **v0.237**. It is **not** Metadata v13+. No full
cursor pagination beyond a simple `responsePartitionLimit` truncate.
Do **not** touch keys 35/43, SCRAM, native ListOffsets, or `group.rs`.

## Goals

1. Advertise `(ApiKey::DescribeTopicPartitions, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 43`.
2. Dispatch key 75 v0 (flexible request header + compact body).
3. Same leaders / replicas / ISR / leader_epoch / TopicId as Metadata.
4. Unknown topic → that topic error **3**, empty partitions.
5. ACL: Topic **DESCRIBE**. Disabled ACLs allow.
6. v1+ → **35** `UNSUPPORTED_VERSION`.
7. `responsePartitionLimit <= 0` → unlimited.

## Non-goals

| Deferred | Why |
|----------|-----|
| Metadata v13+ fields (brokers / cluster id) | This API is topic/partition only |
| Full cursor pagination | Simple truncate only |
| EligibleLeaderReplicas / LastKnownElr | Metadata has no ELR; partition body reused |
| Keys 35 / 43 | Sibling leftovers |
| SCRAM / native ListOffsets / `group.rs` | Sibling leftovers |
| Versions 1+ | Kafka key 75 is v0 only |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Request:

```
topics compact array of { name compact string, tagged }
responsePartitionLimit i32
cursor compact nullable { topicName, partitionIndex, tagged }
tagged
```

Response:

```
throttle i32
topics[] {
  error i16
  name compact nullable string
  topic_id uuid
  is_internal bool
  partitions[] {
    error, index, leader_id, leader_epoch,
    replica_nodes[], isr_nodes[], offline_replicas[], tagged
  }
  authorized_operations i32   // AUTH_OPS_OMITTED (i32::MIN)
  tagged
}
next_cursor nullable { topicName, partitionIndex, tagged }
tagged
```

Empty / null `topics` = all known topics. Partition encoding reuses
Metadata flexible helpers in `kafka/meta_api.rs`. TopicId is the
existing deterministic Volant UUID (`volant` + zeros + u32).

## Semantics

```
DescribeTopicPartitions v0
  │
  ├─ empty / null topics → every known topic (Topic DESCRIBE filter)
  ├─ responsePartitionLimit <= 0 → unlimited
  ├─ cursor topic in the result set → start at that topic/partition
  ├─ cursor topic missing → ignore cursor
  │
  └─ named topic
          ├─ Topic DESCRIBE fail → that topic 29, empty partitions
          ├─ unknown → that topic 3, empty partitions
          └─ else → Metadata leaders / ISR / epochs / TopicId
                    (truncate at responsePartitionLimit; next_cursor if cut)
```

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v237_describe_topic_partitions -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **75** min=0 max=0; `SUPPORTED_APIS.len() >= 43` |
| Create topic + describe | leader / replicas / ISR / epoch / TopicId match Metadata |
| unknown topic | **3**, empty partitions |
| v1 | **35** UnsupportedVersion |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 75 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch + flexible header |
| `crates/volant-broker/src/kafka/meta_api.rs` | encode + Metadata partition helper |
| `crates/volant-broker/tests/v237_describe_topic_partitions.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 75 v0 |
| `docs/V237_SPEC.md` | This spec |

## Honesty leftovers

- **Not** Metadata v13+ (no broker list / cluster id / top-level
  ErrorCode). Topic/partition rows only.
- Cursor: honored when the cursor topic is in the result set; otherwise
  **ignored**. `next_cursor` is set only when `responsePartitionLimit`
  truncates.
- EligibleLeaderReplicas / LastKnownElr are **not** on the wire
  (Metadata partition body reused; Volant has no ELR).
- `authorized_operations` is always omitted (`i32::MIN`), same as
  Metadata when the include-ops flag is off.
- Deterministic TopicId unchanged (`volant` + zeros + u32).
- `group.rs` `SUPPORTED_APIS.len()==42` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V233_SPEC.md](./V233_SPEC.md) — previous Kafka admin wrap
