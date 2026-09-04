# v0.276 — Kafka ShareGroupDescribe key 77 v1 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ShareGroupDescribe** (API key **77**,
version **1** only, always flexible). Volant has **no** KIP-932 share
groups. Parse the official v1 body and reject each requested group
with **42** `INVALID_REQUEST` (`not KIP-932 share group`). Do **not**
call `describe_group`. Do **not** wrap ConsumerGroupDescribe **69**
or DescribeGroups **15**. Members are empty. Do **not** invent
share-group state.

This is residual **v0.276**, not Phase 155. Official Apache Kafka
`ShareGroupDescribeRequest.json` uses apiKey **77**. Official
`validVersions` is **1** only — version 0 was early-access in Kafka
4.0 and **removed** in 4.1. ConsumerGroupDescribe is already **69**.

## Goals

1. Advertise `(ApiKey::ShareGroupDescribe, 1, 1)` in
   `SUPPORTED_APIS` (numeric order after DescribeTopicPartitions **75**,
   before AddRaftVoter **80**). Soft length assert `>= 75`. Do **not**
   change hard `== 74` asserts in `group.rs`.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch
   **v1** only. v0 and v2+ → **35**.
3. Official `ShareGroupDescribeRequest.json` v1. Parse loosely
   (`GroupIds` compact array of compact string,
   `IncludeAuthorizedOperations` bool, tagged). Discard every field
   except the echoed group ids. Do **not** call `describe_group()`.
4. Official response has **no top-level error**. Echo each requested
   groupId with per-group error **42**, empty members, empty
   `groupState`, `groupEpoch` **-1**, `assignmentEpoch` **-1**,
   `assignorName` empty, `authorizedOperations` INT32_MIN (or **0**
   if `includeAuthorizedOperations` is true — do not invent ACL bits).
   `errorMessage` `"not KIP-932 share group"`.
5. Controller is **not** required (group-local reject).
6. ACL: Group **DESCRIBE** per group id. Disabled ACLs allow the
   **42** path. Denied → per-group **30**, empty members,
   errorMessage **null**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-932 share groups | Classic groups only |
| Wrap `describe_group` / ConsumerGroupDescribe 69 / DescribeGroups 15 | Clients keep using 15 / 69 |
| Invent share-group state / members | Reject only; members empty |
| ShareGroupHeartbeat 76 / ShareFetch 78 / ShareAcknowledge 79 / InitializeShareGroupState 83 | Sibling leftovers |
| Advertise v0 | Official validVersions is 1 only |
| `group.rs` hard `== 74` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v1 always flexible)

Official `flexibleVersions` is `0+`. Request
(`ShareGroupDescribeRequest.json` v1):

```
GroupIds compact array of compact string
IncludeAuthorizedOperations bool
tagged
```

Response (`ShareGroupDescribeResponse.json` v1; official has **no**
top-level error). Member struct is not written because `members` is
empty:

```
throttleTimeMs i32 = 0
groups[] {
  errorCode i16                    // 42, or 30 if Group DESCRIBE denied
  errorMessage compact nullable string
                                   // "not KIP-932 share group";
                                   // null on ACL deny
  groupId compact string
  groupState compact string        // empty
  groupEpoch i32                   // -1
  assignmentEpoch i32              // -1
  assignorName compact string      // empty
  members[] compact empty
  authorizedOperations i32         // INT32_MIN (omit default);
                                   // 0 if includeAuthorizedOperations
  tagged
}
tagged
```

Official supported errors include `INVALID_REQUEST` (**42**) and
`GROUP_AUTHORIZATION_FAILED` (**30**). Do **not** invent share-group
members or assignment.

## Semantics

```
ShareGroupDescribe v1
  │
  ├─ Group DESCRIBE fail → per-group 30, errorMessage null,
  │                         empty members; no group mutation
  ├─ Controller not required
  │
  └─ else → per-group 42 "not KIP-932 share group"
             empty members, empty groupState,
             groupEpoch / assignmentEpoch -1, assignorName empty
             classic membership unchanged
```

- Response throttle is always 0. No top-level error.
- Does **not** wrap classic DescribeGroups **15** or
  ConsumerGroupDescribe **69**. Clients must keep using those.
- Official Apache Kafka `validVersions` is **1** only (v0 EA removed
  in 4.1). Volant advertises **1** only.
- Official first flex is **0+**; Volant v1 is flexible (matches
  official). v0 is not advertised and returns **35**.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v276_share_group_describe -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **77** min=max=1; keys **15** and **69** still listed; `SUPPORTED_APIS.len() >= 75` |
| v1 describe one group | throttle **0**, one group, error **42**, empty members |
| v0 | **35** |
| v2 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 77 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v1 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (parse + per-group reject) |
| `crates/volant-broker/tests/v276_share_group_describe.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 77 v1 reject |
| `docs/V276_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** Official validVersions is **1** only. Volant
  advertises 1 only.
- Does **not** wrap classic describe snapshot.
- Official response has no top-level error; reject is per-group
  **42**.
- `group.rs` hard `== 74` untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V264_SPEC.md](./V264_SPEC.md) — ConsumerGroupDescribe 69 wrap
- [V269_SPEC.md](./V269_SPEC.md) — ConsumerGroupHeartbeat 68 reject
- DescribeGroups key **15** — classic describe clients must use
