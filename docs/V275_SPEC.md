# v0.275 — Kafka ShareGroupHeartbeat key 76 v1 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ShareGroupHeartbeat** (API key **76**,
version **1** only, always flexible). Volant groups are classic
Join/Sync/Heartbeat **11/14/12** only — **not** KIP-932 share groups.
Parse the official v1 body and reject **42** `INVALID_REQUEST`
(`not KIP-932 share group`). Do **not** call
`GroupCoordinator::heartbeat`. Do **not** wrap classic Heartbeat key
**12** or ConsumerGroupHeartbeat **68**. Do **not** join / leave /
assign / acquire records.

This is residual **v0.275**, not Phase 155.

## Goals

1. Advertise `(ApiKey::ShareGroupHeartbeat, 1, 1)` in
   `SUPPORTED_APIS` (numeric order after DescribeTopicPartitions **75**,
   before AddRaftVoter **80**). Soft length assert `>= 75`.
   Do **not** change hard `== 74` asserts in `group.rs`, `v206_*`,
   `v225_*`, `v228_*`, `v233_*`.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch v1
   only. v0 and v2+ → **35**.
3. Official `ShareGroupHeartbeatRequest.json` v1. Parse enough to
   consume the body without panicking. Discard every field. Do **not**
   call `heartbeat()`, `join()`, `sync()`, or mutate group state.
4. Response matches official `ShareGroupHeartbeatResponse.json` v1:
   throttle **0**, error **42**, errorMessage
   `"not KIP-932 share group"`, memberId **null**, memberEpoch
   **-1**, heartbeatIntervalMs **0**, assignment **null** (uvarint 0).
5. Controller is **not** required (group-local reject).
6. ACL: Group **READ** on the parsed `groupId`. If `groupId` cannot be
   parsed, treat as empty and still reject **42** (or **30** if ACLs on
   and empty-id denied). Disabled ACLs allow the **42** path. Denied →
   **30**, errorMessage **null**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-932 share groups / share sessions / acquired records | Classic groups only |
| Wrap classic Heartbeat 12 / ConsumerGroupHeartbeat 68 | Clients keep using 12; 68 stays KIP-848 reject |
| ShareGroupDescribe 77 / ShareFetch 78 / ShareAcknowledge 79 / InitializeShareGroupState 83 | Sibling leftovers |
| Join / leave / assign | Reject only |
| Official v0 | Removed in Kafka 4.1; do not advertise |
| `group.rs` hard `== 74` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v1 always flexible)

Official `flexibleVersions` is `0+`. Official `validVersions` is **"1"**
only (v0 was early-access in Kafka 4.0 and removed in 4.1). Request
(`ShareGroupHeartbeatRequest.json` v1; fields versions 0+ so v1 body
is):

```
GroupId compact string
MemberId compact string
MemberEpoch i32
RackId compact nullable string
SubscribedTopicNames compact nullable array of compact string
tagged
```

Response (`ShareGroupHeartbeatResponse.json` v1):

```
ThrottleTimeMs i32                  // 0
ErrorCode i16                       // 42, or 30 if Group READ denied
ErrorMessage compact nullable string
                                    // "not KIP-932 share group";
                                    // null on ACL deny
MemberId compact nullable string    // null
MemberEpoch i32                     // -1
HeartbeatIntervalMs i32             // 0
Assignment                          // nullable struct, default null:
                                    // unsigned varint 0
tagged
```

Official supported errors include `INVALID_REQUEST` (**42**) and
`GROUP_AUTHORIZATION_FAILED` (**30**). Do **not** invent assignment
topic-partitions.

## Semantics

```
ShareGroupHeartbeat v1
  │
  ├─ Group READ fail → 30, errorMessage null, no group mutation
  ├─ Controller not required
  │
  └─ else → 42 "not KIP-932 share group"
       memberId null, memberEpoch -1, heartbeatIntervalMs 0,
       assignment null; classic membership unchanged
```

- Response throttle is always 0.
- Does **not** wrap classic Heartbeat **12** or ConsumerGroupHeartbeat
  **68**. Clients must keep using **12**.
- Official Apache Kafka `validVersions` is **1** only. Volant advertises
  **1** only.
- Official first flex is **0+**; Volant v1 is flexible.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v275_share_group_heartbeat -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **76** min=max=1; keys **12** and **68** still listed; `SUPPORTED_APIS.len() >= 75` |
| v1 heartbeat (any group/member/epoch) | throttle **0**, error **42**, memberId null, memberEpoch **-1**, heartbeatIntervalMs **0**, assignment null; group membership unchanged |
| v0 | **35** |
| v2 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 76 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v1 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (parse + reject) |
| `crates/volant-broker/tests/v275_share_group_heartbeat.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 76 v1 reject |
| `docs/V275_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** No share group, no acquired records.
- Official apiKey is **76**. Official validVersions is **1 only** (v0
  removed in Kafka 4.1). Volant advertises 1 only. Official first flex
  is **0+**; Volant v1 is flexible.
- Does **not** wrap Heartbeat 12 or ConsumerGroupHeartbeat 68.
- `group.rs` hard `== 74` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V269_SPEC.md](./V269_SPEC.md) — ConsumerGroupHeartbeat 68 reject
- Classic Heartbeat key **12** — keep-alive clients must use
