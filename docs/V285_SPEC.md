# v0.285 — Kafka StreamsGroupHeartbeat key 88 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **StreamsGroupHeartbeat** (API key **88**,
version **0** only, always flexible). Volant groups are classic
Join/Sync/Heartbeat **11/14/12** only — **not** KIP-1071 streams
groups. Parse the official v0 body and reject **42** `INVALID_REQUEST`
(`not KIP-1071 streams group`). Do **not** call
`GroupCoordinator::heartbeat`. Do **not** wrap classic Heartbeat key
**12**, ConsumerGroupHeartbeat **68**, or ShareGroupHeartbeat **76**.
Do **not** join / leave / assign / persist topology.

This is residual **v0.285**, not Phase 155. Official Apache Kafka
`StreamsGroupHeartbeatRequest.json` uses apiKey **88**. Official
`validVersions` is **0–1** (v1 = TopologyDescriptionRequired /
KIP-1331). Volant advertises **v0 only**.

## Goals

1. Advertise `(ApiKey::StreamsGroupHeartbeat, 0, 0)` in
   `SUPPORTED_APIS` (numeric order after ReadShareGroupStateSummary
   **87**, before DescribeShareGroupOffsets **90**). Soft length
   assert `>= 85`. Do **not** change hard `== 84` asserts in
   `group.rs` / v206 / v225 / v228 / v233.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch
   **v0** only. v1+ → **35**.
3. Official `StreamsGroupHeartbeatRequest.json` v0. Parse enough to
   get `GroupId` for ACL. Discard MemberId / MemberEpoch / Topology /
   tasks / tags. Do **not** persist. Do **not** wrap Heartbeat **12**
   / ConsumerGroupHeartbeat **68** / ShareGroupHeartbeat **76** /
   `GroupCoordinator::heartbeat`.
4. Response matches official `StreamsGroupHeartbeatResponse.json` v0
   field order: throttle **0**, error **42**, errorMessage
   `"not KIP-1071 streams group"`, memberId **null**, memberEpoch
   **-1**, heartbeatIntervalMs **0**, and every subsequent official
   v0 field empty/null/0. Do **not** write v1-only
   TopologyDescriptionRequired or AcceptableRecoveryLag.
5. Controller is **not** required (group-local reject).
6. ACL: Group **READ** on the parsed `groupId` (same as
   ConsumerGroupHeartbeat **68**). If `groupId` cannot be parsed,
   treat as empty and still reject **42** (or **30** if ACLs on and
   empty-id denied). Disabled ACLs allow the **42** path. Denied →
   **30**, errorMessage **null**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-1071 streams group coordinator | Classic groups only |
| Wrap Heartbeat 12 / ConsumerGroupHeartbeat 68 / ShareGroupHeartbeat 76 | Clients keep using 12; 68/76 stay rejects |
| StreamsGroupDescribe 89 / topology-update siblings | Out of slice |
| Advertise v1 (TopologyDescriptionRequired / KIP-1331) | Out of range |
| Join / leave / assign / persist topology | Reject only |
| join-set, unclean, live reassignment, txn default-on, Kafka Fetch group tags | Out of slice |
| `group.rs` hard `== 84` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official `flexibleVersions` is `0+`. Official `validVersions` is
**0–1**. Request (`StreamsGroupHeartbeatRequest.json` v0):

```
GroupId compact string
MemberId compact string
MemberEpoch i32
EndpointInformationEpoch i32
InstanceId compact nullable string
RackId compact nullable string
RebalanceTimeoutMs i32
Topology nullable struct            // discarded
ActiveTasks compact nullable []     // discarded
StandbyTasks compact nullable []    // discarded
WarmupTasks compact nullable []     // discarded
ProcessId compact nullable string
UserEndpoint nullable struct
ClientTags compact nullable []
TaskOffsets compact nullable []
TaskEndOffsets compact nullable []
ShutdownApplication bool
tagged
```

Volant parses `GroupId` for ACL and discards the rest.

Response (`StreamsGroupHeartbeatResponse.json` v0; do **not** write
v1 fields). Official v0 field order (verified from trunk JSON):

```
ThrottleTimeMs i32                  // 0
ErrorCode i16                       // 42, or 30 if Group READ denied
ErrorMessage compact nullable string
                                    // "not KIP-1071 streams group";
                                    // null on ACL deny
MemberId compact nullable string    // null (reject contract;
                                    // official JSON has no nullableVersions)
MemberEpoch i32                     // -1
HeartbeatIntervalMs i32             // 0
AcceptableRecoveryLagLegacy i32     // 0  (versions "0" only)
TaskOffsetIntervalMs i32            // 0
Status compact nullable []          // null
ActiveTasks compact nullable []     // null
StandbyTasks compact nullable []    // null
WarmupTasks compact nullable []     // null
EndpointInformationEpoch i32        // 0
PartitionsByUserEndpoint compact nullable []  // null
tagged
```

Do **not** write `AcceptableRecoveryLag` (int64, versions `1+`) or
`TopologyDescriptionRequired` (bool, versions `1+` / KIP-1331).
Unlike ConsumerGroupHeartbeat **68** / ShareGroupHeartbeat **76**,
there is **no** `Assignment` struct.

Official supported errors include `INVALID_REQUEST` (**42**) and
`GROUP_AUTHORIZATION_FAILED` (**30**). Do **not** invent tasks or
IQ endpoint maps.

## Semantics

```
StreamsGroupHeartbeat v0
  │
  ├─ Group READ fail → 30, errorMessage null, no group mutation
  ├─ Controller not required
  │
  └─ else → 42 "not KIP-1071 streams group"
       memberId null, memberEpoch -1, heartbeatIntervalMs 0,
       remaining official v0 fields empty/null/0;
       classic membership unchanged
```

- Response throttle is always 0.
- Does **not** wrap classic Heartbeat **12**, ConsumerGroupHeartbeat
  **68**, or ShareGroupHeartbeat **76**. Clients must keep using
  **12**.
- Official Apache Kafka advertises 0–1 today (v1 =
  TopologyDescriptionRequired / KIP-1331). Volant advertises **0**
  only.
- Official first flex is **0+**; Volant v0 is flexible (matches
  official).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v285_streams_group_heartbeat -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **88** min=max=0; keys **12** / **68** / **76** still listed; `SUPPORTED_APIS.len() >= 85` |
| v0 heartbeat (any group/member/epoch) | throttle **0**, error **42**, memberId null, memberEpoch **-1**, heartbeatIntervalMs **0**, remaining v0 fields empty/null/0; group membership unchanged |
| Join + Sync + key 88 + classic Heartbeat 12 | key **88** still **42**; classic **12** still **0** |
| v1 | **35** |
| ACL deny | **30**, errorMessage **null** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 88 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (parse + reject) |
| `crates/volant-broker/tests/v285_streams_group_heartbeat.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 88 v0 reject |
| `docs/V285_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-1071.** No streams group coordinator.
- Official apiKey is **88**. Official validVersions 0–1; Volant
  advertises 0 only (v1 TopologyDescriptionRequired / KIP-1331 is
  out of range). Official first flex is **0+**; Volant v0 is
  flexible (matches official).
- Does **not** wrap Heartbeat 12 / ConsumerGroupHeartbeat 68 /
  ShareGroupHeartbeat 76.
- Official response has no `Assignment` (unlike 68/76). v0 includes
  `AcceptableRecoveryLagLegacy` (versions `"0"` only) before
  `TaskOffsetIntervalMs`.
- `group.rs` hard `== 84` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V269_SPEC.md](./V269_SPEC.md) — ConsumerGroupHeartbeat 68 reject
- [V275_SPEC.md](./V275_SPEC.md) — ShareGroupHeartbeat 76 reject
- Classic Heartbeat key **12** — keep-alive clients must use
