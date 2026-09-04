# v0.282 — Kafka DeleteShareGroupState key 86 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **DeleteShareGroupState** (API key **86**,
version **0** only, always flexible). This is **not** KIP-932
share-partition state. Parse the official v0 body and reject
per-partition **42** `INVALID_REQUEST` (`not KIP-932 share state`).
Do **not** persist share state. Do **not** wrap OffsetCommit /
DeleteGroups / InitializeShareGroupState.

This is residual **v0.282**, not Phase 155. Official Apache Kafka
`DeleteShareGroupStateRequest.json` uses apiKey **86**.
InitializeShareGroupState is already **83**. UnregisterController is
already **94**. Official field layout is used.

## Goals

1. Advertise `(ApiKey::DeleteShareGroupState, 0, 0)` in
   `SUPPORTED_APIS` (numeric order after InitializeShareGroupState
   **83**, before UnregisterController **94**). Soft length assert
   `>= 80`. Do **not** change hard `== 79` asserts.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch v0
   only. v1+ → **35**.
3. Official `DeleteShareGroupStateRequest.json` v0. Parse enough
   to echo TopicId + Partition. Do **not** persist.
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
| Wrap OffsetCommit / DeleteGroups / InitializeShareGroupState | Not share state |
| Read/Write/ReadSummary/DescribeShareGroupOffsets | Sibling leftovers |
| join-set, unclean, live reassignment, txn default-on, Kafka Fetch group tags | Out of slice |
| Versions 1+ | Official / advertised max is 0 |
| `group.rs` hard `== 79` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`DeleteShareGroupStateRequest.json`;
`flexibleVersions: 0+`):

```
GroupId compact string
Topics[] {
  TopicId uuid
  Partitions[] {
    Partition i32
    tagged
  }
  tagged
}
tagged
```

Official response (`DeleteShareGroupStateResponse.json`; **no**
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
DeleteShareGroupState v0
  │
  ├─ Group ALTER fail → echo partitions, per-partition 30, errorMessage null
  ├─ Unparseable body → empty Results[] (no top-level 0)
  ├─ Controller not required
  │
  └─ else → echo TopicId + Partition, per-partition 42 INVALID_REQUEST
            (not KIP-932 share state)
            nothing persisted
            OffsetCommit / DeleteGroups / InitializeShareGroupState
            are not called
```

- Official response has no throttle and no top-level error.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official).
- Official request has **no** StateEpoch / StartOffset (unlike
  InitializeShareGroupState **83**).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v282_delete_share_group_state -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **86** min=0 max=0; key **83** still listed; `SUPPORTED_APIS.len() >= 80` |
| v0 delete one topic/partition | **no throttle**; one result topic; partition error **42**; nothing persisted |
| v1 | **35** |
| ACL deny | echo partitions, per-partition **30**, errorMessage **null** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 86 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | parse + per-partition reject 42; ACL deny 30; no persist |
| `crates/volant-broker/tests/v282_delete_share_group_state.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 86 v0 reject |
| `docs/V282_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** No share-partition state.
- Official apiKey is **86**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official response has no throttle and no top-level error; reject is
  per-partition **42**.
- Official request listeners are `["broker"]` only.
- Official response comments list coordinator / fenced-epoch errors;
  Volant still honest-rejects **42** (not a share-state coordinator).
- `group.rs` hard `== 79` untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V279_SPEC.md](./V279_SPEC.md) — InitializeShareGroupState 83 reject
- OffsetCommit key **8** / DeleteGroups key **42** — classic consumer
  groups; not share state
