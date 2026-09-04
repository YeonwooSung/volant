# v0.281 — Kafka WriteShareGroupState key 85 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **WriteShareGroupState** (API key **85**,
version **0** only, always flexible). This is **not** KIP-932
share-partition state. Parse the official v0 body and reject
per-partition **42** `INVALID_REQUEST` (`not KIP-932 share state`).
Do **not** persist share state. Do **not** wrap OffsetCommit or
InitializeShareGroupState.

This is residual **v0.281**, not Phase 155. Official Apache Kafka
`WriteShareGroupStateRequest.json` uses apiKey **85**. Official
`validVersions` is **0–1** (v1 adds `DeliveryCompleteCount` /
KIP-1226). Volant advertises **v0 only**. InitializeShareGroupState
is already **83**. UnregisterController is already **94**. Official
field layout is used.

## Goals

1. Advertise `(ApiKey::WriteShareGroupState, 0, 0)` in
   `SUPPORTED_APIS` (numeric order after InitializeShareGroupState
   **83**, before UnregisterController **94**). Soft length assert
   `>= 80`. Do **not** change hard `== 79` asserts.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch v0
   only. v1+ → **35**.
3. Official `WriteShareGroupStateRequest.json` v0. Parse enough to
   echo TopicId + Partition. Discard StateEpoch / LeaderEpoch /
   StartOffset / StateBatches. Do **not** parse DeliveryCompleteCount
   as an advertised v0 field. Do **not** persist.
4. Official response has **no throttleTimeMs** and **no top-level
   error**. Echo requested TopicId + Partition with ErrorCode **42**
   and errorMessage `"not KIP-932 share state"`. If the body cannot
   be parsed, write empty `Results[]` (still not success — there is
   no top-level 0). Prefer echoing parsed partitions.
5. Controller is **not** required (group-local reject).
6. ACL: Group **ALTER** on the parsed `groupId`. Denied → still echo
   partitions with **30**, errorMessage **null**. Disabled ACLs allow
   the **42** path.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-932 share-partition state store | Reject only |
| Wrap OffsetCommit / InitializeShareGroupState | Not share state |
| Read/Delete/ReadSummary/DescribeShareGroupOffsets | Sibling leftovers |
| Advertise v1 (DeliveryCompleteCount / KIP-1226) | Out of range |
| join-set, unclean, live reassignment, txn default-on, Kafka Fetch group tags | Out of slice |
| `group.rs` / v206 / v225 / v228 / v233 hard `== 79` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`WriteShareGroupStateRequest.json`;
`flexibleVersions: 0+`):

```
GroupId compact string
Topics[] {
  TopicId uuid
  Partitions[] {
    Partition i32
    StateEpoch i32
    LeaderEpoch i32
    StartOffset i64
    StateBatches[] {
      FirstOffset i64
      LastOffset i64
      DeliveryState i8
      DeliveryCount i16
      tagged
    }
    tagged
  }
  tagged
}
tagged
```

`DeliveryCompleteCount` is **v1+** (KIP-1226) and is not parsed as
an advertised v0 field.

Official response (`WriteShareGroupStateResponse.json`; **no**
`throttleTimeMs`, **no** top-level error):

```
Results[] {
  TopicId uuid
  Partitions[] {
    Partition i32
    ErrorCode i16 = 42            // or 30 if Group ALTER denied
    ErrorMessage compact nullable string = "not KIP-932 share state"  // null on ACL deny
    tagged
  }
  tagged
}
tagged
```

Official `validVersions` is **0–1**. Volant advertises **v0 only**.

## Semantics

```
WriteShareGroupState v0
  │
  ├─ Group ALTER fail → echo partitions, per-partition 30, errorMessage null
  ├─ Unparseable body → empty Results[] (no top-level 0)
  ├─ Controller not required
  │
  └─ else → echo TopicId + Partition, per-partition 42 INVALID_REQUEST
            (not KIP-932 share state)
            nothing persisted
            OffsetCommit / InitializeShareGroupState are not called
```

- Official response has no throttle and no top-level error.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official).
- Official `listeners` is `["broker"]` (not controller).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v281_write_share_group_state -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **85** min=0 max=0; key **83** still listed; `SUPPORTED_APIS.len() >= 80` |
| v0 write one topic/partition | **no throttle**; one result topic; partition error **42**; nothing persisted |
| v1 | **35** |
| ACL deny | per-partition **30**, errorMessage **null** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 85 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | parse + per-partition reject 42; ACL deny 30; no persist |
| `crates/volant-broker/tests/v281_write_share_group_state.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 85 v0 reject |
| `docs/V281_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** No share-partition state.
- Official apiKey is **85**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official validVersions is **0–1**; Volant advertises **v0 only**
  (v1 DeliveryCompleteCount / KIP-1226 is out of range).
- Official response has no throttle and no top-level error; reject is
  per-partition **42**.
- `group.rs` / v206 / v225 / v228 / v233 hard `== 79` untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V279_SPEC.md](./V279_SPEC.md) — InitializeShareGroupState 83 reject
- OffsetCommit key **8** — classic consumer offsets; not share state
