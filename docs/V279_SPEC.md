# v0.279 — Kafka InitializeShareGroupState key 83 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **InitializeShareGroupState** (API key **83**,
version **0** only, always flexible). This is **not** KIP-932
share-partition state. Parse the official v0 body and reject
per-partition **42** `INVALID_REQUEST` (`not KIP-932 share state`).
Do **not** persist share state. Do **not** wrap OffsetCommit.

This is residual **v0.279**, not Phase 155. Official Apache Kafka
`InitializeShareGroupStateRequest.json` uses apiKey **83**.
UpdateRaftVoter is already **82**. UnregisterController is already
**94**. Official field layout is used.

## Goals

1. Advertise `(ApiKey::InitializeShareGroupState, 0, 0)` in
   `SUPPORTED_APIS` (numeric order after UpdateRaftVoter **82**,
   before UnregisterController **94**). Soft length assert `>= 75`.
   Do **not** change hard `== 74` asserts.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch v0
   only. v1+ → **35**.
3. Official `InitializeShareGroupStateRequest.json` v0. Parse enough
   to echo TopicId + Partition. Discard StateEpoch / StartOffset. Do
   **not** persist.
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
| Wrap OffsetCommit | Not share state |
| ShareGroupHeartbeat 76 / ShareGroupDescribe 77 / ShareFetch 78 / ShareAcknowledge 79 | Sibling leftovers |
| join-set, unclean, live reassignment, txn default-on, Kafka Fetch group tags | Out of slice |
| Versions 1+ | Official / advertised max is 0 |
| `group.rs` hard `== 74` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`InitializeShareGroupStateRequest.json`;
`flexibleVersions: 0+`):

```
GroupId compact string
Topics[] {
  TopicId uuid
  Partitions[] {
    Partition i32
    StateEpoch i32
    StartOffset i64
    tagged
  }
  tagged
}
tagged
```

Official response (`InitializeShareGroupStateResponse.json`; **no**
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

Official `validVersions` is **0** only (matches advertisement).

## Semantics

```
InitializeShareGroupState v0
  │
  ├─ Group ALTER fail → echo partitions, per-partition 30, errorMessage null
  ├─ Unparseable body → empty Results[] (no top-level 0)
  ├─ Controller not required
  │
  └─ else → echo TopicId + Partition, per-partition 42 INVALID_REQUEST
            (not KIP-932 share state)
            nothing persisted
            OffsetCommit is not called
```

- Official response has no throttle and no top-level error.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v279_initialize_share_group_state -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **83** min=0 max=0; keys **82** and **94** still listed; `SUPPORTED_APIS.len() >= 75` |
| v0 initialize one topic/partition | **no throttle**; one result topic; partition error **42**; nothing persisted |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 83 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | parse + per-partition reject 42; ACL deny 30; no persist |
| `crates/volant-broker/tests/v279_initialize_share_group_state.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 83 v0 reject |
| `docs/V279_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** No share-partition state.
- Official apiKey is **83**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official response has no throttle and no top-level error; reject is
  per-partition **42**.
- `group.rs` hard `== 74` untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V269_SPEC.md](./V269_SPEC.md) — ConsumerGroupHeartbeat 68 reject
- OffsetCommit key **8** — classic consumer offsets; not share state
