# v0.269 — Kafka ConsumerGroupHeartbeat key 68 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ConsumerGroupHeartbeat** (API key **68**,
version **0** only, always flexible). Volant groups are classic
Join/Sync/Heartbeat **11/14/12** only — **not** KIP-848 consumer
protocol. Parse the official v0 body and reject **42**
`INVALID_REQUEST` (`not KIP-848 consumer protocol`). Do **not** call
`GroupCoordinator::heartbeat`. Do **not** wrap classic Heartbeat key
**12**. Do **not** join / leave / assign.

This is residual **v0.269**, not Phase 155. v0.264 explicitly deferred
this key. ConsumerGroupDescribe **69** stays the classic-snapshot wrap.

## Goals

1. Advertise `(ApiKey::ConsumerGroupHeartbeat, 0, 0)` in
   `SUPPORTED_APIS` (numeric order after AllocateProducerIds **67**,
   before ConsumerGroupDescribe **69**). Soft length assert `>= 65`.
   Do **not** change hard `== 64` asserts in `group.rs`, `v206_*`,
   `v225_*`, `v228_*`, `v233_*`.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch v0
   only. v1+ → **35**.
3. Official `ConsumerGroupHeartbeatRequest.json` v0. Parse enough to
   consume the body without panicking. Discard every field. Do **not**
   call `heartbeat()`, `join()`, `sync()`, or mutate group state.
4. Response matches official `ConsumerGroupHeartbeatResponse.json` v0:
   throttle **0**, error **42**, errorMessage
   `"not KIP-848 consumer protocol"`, memberId **null**, memberEpoch
   **-1**, heartbeatIntervalMs **0**, assignment **null** (uvarint 0).
5. Controller is **not** required (group-local reject).
6. ACL: Group **READ** on the parsed `groupId` (classic Heartbeat is a
   member keep-alive). If `groupId` cannot be parsed, treat as empty
   and still reject **42** (or **30** if ACLs on and empty-id denied).
   Disabled ACLs allow the **42** path. Denied → **30**, errorMessage
   **null**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-848 consumer protocol | Classic groups only |
| memberEpoch / server assignor / regex subscribe | Official v1+ / KIP-848 |
| Wrap classic Heartbeat 12 / `GroupCoordinator::heartbeat` | Clients keep using 12 |
| Join / leave / assign | Reject only |
| ConsumerGroupDescribe 69 / DescribeGroups 15 | Already shipped |
| Envelope 58 / FetchSnapshot 59 / BrokerHeartbeat 63 / ControllerRegistration 70 | Sibling leftovers |
| Official v1 SubscribedTopicRegex | Advertise v0 only |
| `group.rs` hard `== 64` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official `flexibleVersions` is `0+`. Request
(`ConsumerGroupHeartbeatRequest.json` v0):

```
GroupId compact string
MemberId compact string
MemberEpoch i32
InstanceId compact nullable string
RackId compact nullable string
RebalanceTimeoutMs i32
SubscribedTopicNames compact nullable array of compact string
ServerAssignor compact nullable string
TopicPartitions compact nullable array of {
  TopicId uuid
  Partitions compact array of i32
  tagged
}
tagged
```

SubscribedTopicRegex is v1+ — do not parse as v0.

Response (`ConsumerGroupHeartbeatResponse.json` v0; do not write v1
fields):

```
ThrottleTimeMs i32                  // 0
ErrorCode i16                       // 42, or 30 if Group READ denied
ErrorMessage compact nullable string
                                    // "not KIP-848 consumer protocol";
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
ConsumerGroupHeartbeat v0
  │
  ├─ Group READ fail → 30, errorMessage null, no group mutation
  ├─ Controller not required
  │
  └─ else → 42 "not KIP-848 consumer protocol"
       memberId null, memberEpoch -1, heartbeatIntervalMs 0,
       assignment null; classic membership unchanged
```

- Response throttle is always 0.
- Does **not** wrap classic Heartbeat **12**. Clients must keep using
  **12**.
- Official Apache Kafka advertises 0–1 today (v1 = SubscribedTopicRegex).
  Volant advertises **0** only.
- Official first flex is **0+**; Volant v0 is flexible (matches official).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v269_consumer_group_heartbeat -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **68** min=max=0; keys **12** and **69** still listed; `SUPPORTED_APIS.len() >= 65` |
| v0 heartbeat (any group/member/epoch) | throttle **0**, error **42**, memberId null, memberEpoch **-1**, heartbeatIntervalMs **0**, assignment null; group membership unchanged |
| Join (rebalance 150ms) + Sync + key 68 + classic Heartbeat 12 | key **68** still **42**; classic **12** still **0** |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 68 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (parse + reject) |
| `crates/volant-broker/tests/v269_consumer_group_heartbeat.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 68 v0 reject |
| `docs/V269_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-848.** Classic groups only. No memberEpoch, no server
  assignor, no regex.
- Does **not** wrap classic Heartbeat 12. Clients must keep using 12.
- Official apiKey is **68**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official validVersions 0–1; Volant advertises 0 only.
- ConsumerGroupDescribe **69** remains the classic-snapshot wrap.
- `group.rs` hard `== 64` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V264_SPEC.md](./V264_SPEC.md) — ConsumerGroupDescribe 69 wrap
- Classic Heartbeat key **12** — keep-alive clients must use
