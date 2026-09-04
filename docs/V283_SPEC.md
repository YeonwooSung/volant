# v0.283 — Kafka ReadShareGroupStateSummary key 87 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ReadShareGroupStateSummary** (API key **87**,
version **0** only, always flexible). This is **not** KIP-932
share-partition state. Parse the official v0 body and reject
per-partition **42** `INVALID_REQUEST` (`not KIP-932 share state`).
Do **not** persist share state. Do **not** wrap OffsetFetch /
OffsetCommit / InitializeShareGroupState / ReadShareGroupState.

This is residual **v0.283**, not Phase 155. Official Apache Kafka
`ReadShareGroupStateSummaryRequest.json` uses apiKey **87**.
InitializeShareGroupState is already **83**. UnregisterController is
already **94**. Official field layout is used.

## Goals

1. Advertise `(ApiKey::ReadShareGroupStateSummary, 0, 0)` in
   `SUPPORTED_APIS` (numeric order after InitializeShareGroupState
   **83**, before UnregisterController **94**). Soft length assert
   `>= 80`. Do **not** change hard `== 79` asserts.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch v0
   only. v1+ → **35**.
3. Official `ReadShareGroupStateSummaryRequest.json` v0. Same shape
   as ReadShareGroupState **84**. Parse enough to echo TopicId +
   Partition. Discard LeaderEpoch. Do **not** persist.
4. Official response has **no throttleTimeMs** and **no top-level
   error**. Echo requested TopicId + Partition with ErrorCode **42**,
   errorMessage `"not KIP-932 share state"`, StateEpoch **0**,
   LeaderEpoch **-1**, StartOffset **-1**. Do **not** write
   DeliveryCompleteCount (v1+ / KIP-1226). If the body cannot be
   parsed, write empty `Results[]` (still not success — there is no
   top-level 0). Prefer echoing parsed partitions.
5. Controller is **not** required (group-local reject).
6. ACL: Group **READ** on the parsed `groupId`. Denied → still echo
   partitions with **30**, errorMessage **null**, StateEpoch **0**,
   LeaderEpoch **-1**, StartOffset **-1**. Disabled ACLs allow the
   **42** path.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-932 share-partition state store | Reject only |
| Wrap OffsetFetch 9 / OffsetCommit 8 / InitializeShareGroupState 83 / ReadShareGroupState 84 | Not share state |
| Write/Delete/DescribeShareGroupOffsets | Sibling leftovers |
| Read/Write/DeleteShareGroupState 84/85/86 | Sibling leftovers |
| Advertise v1 (DeliveryCompleteCount) | Out of range (KIP-1226) |
| join-set, unclean, live reassignment, txn default-on, Kafka Fetch group tags | Out of slice |
| `group.rs` / v206 / v225 / v228 / v233 hard `== 79` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`ReadShareGroupStateSummaryRequest.json`;
`flexibleVersions: 0+`; same shape as ReadShareGroupState **84**):

```
GroupId compact string
Topics[] {
  TopicId uuid
  Partitions[] {
    Partition i32
    LeaderEpoch i32
    tagged
  }
  tagged
}
tagged
```

Official response (`ReadShareGroupStateSummaryResponse.json`; **no**
`throttleTimeMs`, **no** top-level error):

```
Results[] {
  TopicId uuid
  Partitions[] {
    Partition i32
    ErrorCode i16 = 42            // or 30 if Group READ denied
    ErrorMessage compact nullable = "not KIP-932 share state"  // null on ACL deny
    StateEpoch i32 = 0
    LeaderEpoch i32 = -1
    StartOffset i64 = -1
    tagged
  }
  tagged
}
tagged
```

Official `validVersions` is **0–1**. Version 1 introduces
`DeliveryCompleteCount` (KIP-1226). Volant advertises **v0 only**
and does not write that field.

## Semantics

```
ReadShareGroupStateSummary v0
  │
  ├─ Group READ fail → echo partitions, per-partition 30, errorMessage null
  │                    StateEpoch 0, LeaderEpoch -1, StartOffset -1
  ├─ Unparseable body → empty Results[] (no top-level 0)
  ├─ Controller not required
  │
  └─ else → echo TopicId + Partition, per-partition 42 INVALID_REQUEST
            (not KIP-932 share state)
            StateEpoch 0, LeaderEpoch -1, StartOffset -1
            nothing persisted
            OffsetFetch / OffsetCommit / InitializeShareGroupState /
            ReadShareGroupState are not called
```

- Official response has no throttle and no top-level error.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v283_read_share_group_state_summary -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **87** min=0 max=0; key **83** still listed; `SUPPORTED_APIS.len() >= 80` |
| v0 one topic/partition | **no throttle**; one result topic; partition error **42**; StateEpoch 0 / LeaderEpoch -1 / StartOffset -1; nothing persisted |
| v1 | **35** |
| ACL deny | per-partition **30**, errorMessage **null**, StateEpoch 0 / LeaderEpoch -1 / StartOffset -1 |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 87 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | parse + per-partition reject 42; ACL deny 30; no persist |
| `crates/volant-broker/tests/v283_read_share_group_state_summary.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 87 v0 reject |
| `docs/V283_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** No share-partition state.
- Official apiKey is **87**. Official `validVersions` is **0–1**;
  Volant advertises v0 only (v1 DeliveryCompleteCount / KIP-1226 is
  out of range). Official first flex is **0+**; Volant v0 is flexible
  (matches official).
- Official response has no throttle and no top-level error; reject is
  per-partition **42**.
- `group.rs` / v206 / v225 / v228 / v233 hard `== 79` untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V279_SPEC.md](./V279_SPEC.md) — InitializeShareGroupState 83 reject
- OffsetFetch key **9** / OffsetCommit key **8** — classic consumer
  offsets; not share state
