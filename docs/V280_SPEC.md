# v0.280 — Kafka ReadShareGroupState key 84 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ReadShareGroupState** (API key **84**,
version **0** only, always flexible). This is **not** KIP-932
share-partition state. Parse the official v0 body and reject
per-partition **42** `INVALID_REQUEST` (`not KIP-932 share state`).
Do **not** persist share state. Do **not** wrap OffsetFetch /
OffsetCommit / InitializeShareGroupState.

This is residual **v0.280**, not Phase 155. Official Apache Kafka
`ReadShareGroupStateRequest.json` uses apiKey **84**.
InitializeShareGroupState is already **83**. UnregisterController is
already **94**. Official field layout is used.

## Goals

1. Advertise `(ApiKey::ReadShareGroupState, 0, 0)` in
   `SUPPORTED_APIS` (numeric order after InitializeShareGroupState
   **83**, before UnregisterController **94**). Soft length assert
   `>= 80`. Do **not** change hard `== 79` asserts.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch v0
   only. v1+ → **35**.
3. Official `ReadShareGroupStateRequest.json` v0. Parse enough to
   echo TopicId + Partition. Discard LeaderEpoch. Do **not** persist.
4. Official response has **no throttleTimeMs** and **no top-level
   error**. Echo requested TopicId + Partition with ErrorCode **42**,
   errorMessage `"not KIP-932 share state"`, StateEpoch **0**,
   StartOffset **-1**, empty `StateBatches[]`. If the body cannot be
   parsed, write empty `Results[]` (still not success — there is no
   top-level 0). Prefer echoing parsed partitions.
5. Controller is **not** required (group-local reject).
6. ACL: Group **READ** on the parsed `groupId`. Denied → still echo
   partitions with **30**, errorMessage **null**, StateEpoch 0,
   StartOffset -1, empty batches. Disabled ACLs allow the **42**
   path.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-932 share-partition state store | Reject only |
| Wrap OffsetFetch / OffsetCommit / InitializeShareGroupState | Not share state |
| Write/Delete/ReadSummary/DescribeShareGroupOffsets | Sibling leftovers |
| join-set, unclean, live reassignment, txn default-on, Kafka Fetch group tags | Out of slice |
| Versions 1+ | Official / advertised max is 0 |
| `group.rs` hard `== 79` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`ReadShareGroupStateRequest.json`;
`flexibleVersions: 0+`):

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

Official response (`ReadShareGroupStateResponse.json`; **no**
`throttleTimeMs`, **no** top-level error):

```
Results[] {
  TopicId uuid
  Partitions[] {
    Partition i32
    ErrorCode i16 = 42            // or 30 if Group READ denied
    ErrorMessage compact nullable string = "not KIP-932 share state"  // null on ACL deny
    StateEpoch i32 = 0
    StartOffset i64 = -1
    StateBatches[] empty
    tagged
  }
  tagged
}
tagged
```

Official `validVersions` is **0** only (matches advertisement).

## Semantics

```
ReadShareGroupState v0
  │
  ├─ Group READ fail → echo partitions, per-partition 30, errorMessage null
  ├─ Unparseable body → empty Results[] (no top-level 0)
  ├─ Controller not required
  │
  └─ else → echo TopicId + Partition, per-partition 42 INVALID_REQUEST
            (not KIP-932 share state)
            StateEpoch 0, StartOffset -1, empty StateBatches
            nothing persisted
            OffsetFetch / OffsetCommit / InitializeShareGroupState
            are not called
```

- Official response has no throttle and no top-level error.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v280_read_share_group_state -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **84** min=0 max=0; key **83** still listed; `SUPPORTED_APIS.len() >= 80` |
| v0 read one topic/partition | **no throttle**; one result topic; partition error **42**; StateEpoch 0; StartOffset -1; empty batches; nothing persisted |
| v1 | **35** |
| ACL deny | per-partition **30**, errorMessage null |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 84 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | parse + per-partition reject 42; ACL deny 30; no persist |
| `crates/volant-broker/tests/v280_read_share_group_state.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 84 v0 reject |
| `docs/V280_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** No share-partition state.
- Official apiKey is **84**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official response has no throttle and no top-level error; reject is
  per-partition **42**.
- Official request discards LeaderEpoch. Official response writes
  StateEpoch **0**, StartOffset **-1**, empty `StateBatches[]`.
- `group.rs` hard `== 79` untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V279_SPEC.md](./V279_SPEC.md) — InitializeShareGroupState 83 reject
- OffsetFetch key **9** / OffsetCommit key **8** — classic consumer
  offsets; not share state
