# v0.264 — Kafka ConsumerGroupDescribe key 69 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ConsumerGroupDescribe** (API key **69**,
version **0** only) as an honest wrap of
`GroupCoordinator::describe_group` — the same snapshot DescribeGroups
(**15**) already uses.

This is **not** KIP-848 consumer protocol. `memberEpoch` is **-1**,
GroupType is classic only (official v0 has no GroupType field), no regex
subscribe, no assignor streams. Unknown group → per-group **69**
`GROUP_ID_NOT_FOUND` (same code DescribeGroups already emits; official
ConsumerGroupDescribe also lists **69**).

This is residual **v0.264**. Do **not** implement ConsumerGroupHeartbeat
(68) / KIP-848 join. Do **not** change DescribeGroups key 15 behavior.
Do **not** touch Expire/Renew tokens, OffsetCommit epoch,
BrokerRegistration, or `group.rs` hard asserts.

## Goals

1. Advertise `(ApiKey::ConsumerGroupDescribe, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 61`. Do **not** change
   hard `== 60` asserts.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch v0
   only. v1+ → **35**.
3. Official `ConsumerGroupDescribeRequest.json` v0:
   ```
   groupIds[] compact string
   includeAuthorizedOperations bool
   tagged
   ```
   `includeAuthorizedOperations`: if true, write **0** (no bitset) —
   do not invent ACL bits. If false, write INT32_MIN (official omit
   default).
4. Official `ConsumerGroupDescribeResponse.json` v0 field order for
   each requested group. Found: error **0**, `groupState` from
   `listed_state` (Empty / Stable / CompletingRebalance /
   PreparingRebalance), `groupEpoch` = generation as i32,
   `assignmentEpoch` = generation (same; we do not track a separate
   synced epoch on the describe snapshot), `assignorName` = `range`
   when members exist else empty, members from `describe_group`
   (`memberId`, `instanceId` if `static:` prefix else null, `rackId`
   null, `memberEpoch` **-1**, assignment from stored partitions as
   topic + partition list). Missing: error **69**.
5. Controller is **not** required (group-local).
6. ACL: Group **DESCRIBE** per group id. Disabled ACLs allow. Denied →
   per-group **30** `GROUP_AUTHORIZATION_FAILED`.

## Non-goals

| Deferred | Why |
|----------|-----|
| ConsumerGroupHeartbeat (68) / KIP-848 join | Sibling leftover |
| memberEpoch / targetMemberEpoch real values | Classic groups only; always **-1** |
| Regex subscribe / assignor streams | Not KIP-848 |
| Official v1 MemberType | Advertise v0 only |
| DescribeGroups key 15 behavior | Reuse snapshot only |
| `group.rs` hard `== 60` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official `flexibleVersions` is `0+`. Request
(`ConsumerGroupDescribeRequest.json`):

```
groupIds compact array of compact string
includeAuthorizedOperations bool
tagged
```

Response (`ConsumerGroupDescribeResponse.json`; official v0–1, Volant
v0 only — v1 MemberType is not written):

```
throttleTimeMs i32
groups[] {
  errorCode i16
  errorMessage compact nullable string
  groupId compact string
  groupState compact string
  groupEpoch i32
  assignmentEpoch i32
  assignorName compact string
  members[] {
    memberId compact string
    instanceId compact nullable string
    rackId compact nullable string
    memberEpoch i32          // always -1
    clientId compact string
    clientHost compact string
    subscribedTopicNames[] compact string
    subscribedTopicRegex compact nullable string  // always null
    assignment {
      topicPartitions[] { topicId uuid, topicName, partitions[] i32, tagged }
      tagged
    }
    targetAssignment { same shape; echoed current assignment }
    tagged                   // MemberType is v1+
  }
  authorizedOperations i32
  tagged
}
tagged
```

Official v0 has **no** GroupType field. ListGroups v5 still reports
`classic`.

## Semantics

```
ConsumerGroupDescribe v0
  │
  ├─ Group DESCRIBE fail → per-group 30, empty members
  ├─ Controller not required
  │
  ├─ describe_group Some → error 0, listed_state, generation, members
  ├─ offset-only known id → error 0, Empty (same as DescribeGroups)
  └─ else → per-group 69 GROUP_ID_NOT_FOUND
```

- Response throttle is always 0.
- `assignmentEpoch` is the group generation (documented; no separate
  synced-epoch field on the snapshot).
- Unknown handling matches DescribeGroups (**69**, not 25/16). Official
  ConsumerGroupDescribe lists `GROUP_ID_NOT_FOUND` as a supported
  error, so **69** is the Kafka code for this API.
- Official Apache Kafka advertises 0–1 today (v1 = MemberType /
  KIP-1099). Volant advertises **0** only.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v264_consumer_group_describe -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **69** min=max=0; key **15** still listed; `SUPPORTED_APIS.len() >= 61` |
| Join (rebalance 150ms) + SyncGroup + describe | error **0**, member present, `memberEpoch` **-1** |
| Unknown group id | **69** |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 69 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (classic snapshot wrap) |
| `crates/volant-broker/tests/v264_consumer_group_describe.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 69 v0 |
| `docs/V264_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-848.** Classic groups only. `memberEpoch` is always **-1**.
  `SubscribedTopicRegex` is always null. Target assignment echoes the
  current stored assignment (no separate target).
- Official Kafka ConsumerGroupDescribe is the new consumer-group API;
  admin clients that get **69** on a classic group fall back to
  DescribeGroups **15**. Volant fills 69 from the classic snapshot so
  a 69-only client still sees members.
- Official first flex is **0**; Volant v0 is flexible (matches official).
- Official response v1 adds `MemberType`; not advertised.
- Official v0 has no GroupType field.
- ConsumerGroupHeartbeat (**68**) is not advertised.
- `group.rs` `SUPPORTED_APIS.len()==60` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V259_SPEC.md](./V259_SPEC.md) — always-flex residual pattern
- DescribeGroups key **15** — snapshot source
