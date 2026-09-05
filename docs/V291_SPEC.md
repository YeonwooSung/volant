# v0.291 — Kafka StreamsGroupTopologyDescriptionUpdate key 93 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **StreamsGroupTopologyDescriptionUpdate**
(API key **93**, version **0** only, always flexible). Volant groups
are classic Join/Sync/Heartbeat **11/14/12** only — **not** KIP-1071
streams groups. Parse the official v0 body and reject **42**
`INVALID_REQUEST` (`not KIP-1071 streams group`). Do **not** persist
topology. Do **not** wrap StreamsGroupHeartbeat **88**,
StreamsGroupDescribe **89**, classic Heartbeat **12**,
ConsumerGroupHeartbeat **68**, or ShareGroupHeartbeat **76**. Do
**not** join / leave / assign.

This is residual **v0.291**, not Phase 155. Official Apache Kafka
`StreamsGroupTopologyDescriptionUpdateRequest.json` uses apiKey
**93**. Official `validVersions` is **0** only. Official
`flexibleVersions` is **0+**. UnregisterController stays **94**.
StreamsGroupHeartbeat stays **88**. StreamsGroupDescribe stays **89**.

## Goals

1. Advertise `(ApiKey::StreamsGroupTopologyDescriptionUpdate, 0, 0)`
   in `SUPPORTED_APIS` (numeric order after DeleteShareGroupOffsets
   **92**, before UnregisterController **94**). Soft length assert
   `>= 90`. Do **not** change hard `== 89`.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch
   **v0** only. v1+ → **35**.
3. Official `StreamsGroupTopologyDescriptionUpdateRequest.json` v0.
   Parse enough to get `GroupId` for ACL. Discard MemberId /
   TopologyEpoch / TopologyDescription / tags. Do **not** persist.
   Do **not** wrap StreamsGroupHeartbeat **88** /
   StreamsGroupDescribe **89** / Heartbeat **12** /
   ConsumerGroupHeartbeat **68** / ShareGroupHeartbeat **76**.
4. Official response is throttle **0**, error **42**, errorMessage
   `"not KIP-1071 streams group"`, empty tag buffer. No members, no
   topology echo, no assignment.
5. Controller is **not** required (group-local reject).
6. ACL: Group **ALTER** on the parsed `groupId` (this is an
   update/mutate; Describe **89** is DESCRIBE; Heartbeat **88** is
   READ). If `groupId` cannot be parsed, treat as empty and still
   reject **42** (or **30** if ACLs on and empty-id denied). Disabled
   ACLs allow the **42** path. Denied → **30**, errorMessage **null**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-1071 streams group coordinator | Classic groups only |
| Persist topology / invent streams-group state | Reject only |
| Wrap StreamsGroupHeartbeat 88 / StreamsGroupDescribe 89 / Heartbeat 12 / ConsumerGroupHeartbeat 68 / ShareGroupHeartbeat 76 | Clients keep using 12; 88/89 stay rejects |
| Join / leave / assign | Reject only |
| Advertise v1 | Official validVersions is 0 only |
| Invent UNKNOWN_MEMBER_ID / STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED | Those imply we validated membership |
| join-set, unclean, live reassignment, txn default-on, Kafka Fetch group tags | Out of slice |
| `group.rs` hard `== 89` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official `flexibleVersions` is `0+`. Official `validVersions` is
**0** only. Request
(`StreamsGroupTopologyDescriptionUpdateRequest.json` v0):

```
GroupId compact string
MemberId compact string
TopologyEpoch i32
TopologyDescription {
  Subtopologies[] compact { SubtopologyId compact string, Nodes[] { ... }, tagged }
  GlobalStores[] compact { Source node, Processor node, tagged }
  tagged
}
tagged
```

Volant parses `GroupId` for ACL and discards MemberId /
TopologyEpoch / TopologyDescription. Nested TopologyDescription is
not fully decoded. Parse loosely; never panic.

Response (`StreamsGroupTopologyDescriptionUpdateResponse.json` v0):

```
ThrottleTimeMs i32                  // 0
ErrorCode i16                       // 42, or 30 if Group ALTER denied
ErrorMessage compact nullable string
                                    // "not KIP-1071 streams group";
                                    // null on ACL deny
tagged
```

That is the entire response — no members, no topology echo, no
assignment.

Official supported errors include `INVALID_REQUEST` (**42**) and
`GROUP_AUTHORIZATION_FAILED` (**30**). Do **not** invent
`UNKNOWN_MEMBER_ID` or `STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED`.

## Semantics

```
StreamsGroupTopologyDescriptionUpdate v0
  │
  ├─ Group ALTER fail → 30, errorMessage null, no persist
  ├─ Unparseable body → throttle 0 + 42 (empty groupId)
  ├─ Controller not required
  │
  └─ else → 42 "not KIP-1071 streams group"
       classic membership unchanged
       topology not persisted
```

- Response throttle is always 0.
- Does **not** wrap StreamsGroupHeartbeat **88**,
  StreamsGroupDescribe **89**, classic Heartbeat **12**,
  ConsumerGroupHeartbeat **68**, or ShareGroupHeartbeat **76**.
  Clients must keep using **12**; 88 is also a reject.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official). v1 is not
  advertised and returns **35**.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v291_streams_group_topology_description_update -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **93** min=max=0; keys **88**, **89**, **94** still listed; `SUPPORTED_APIS.len() >= 90` |
| v0 update (any group/member/epoch + empty-ish topology) | throttle **0**, error **42**, errorMessage `"not KIP-1071 streams group"`; group membership unchanged |
| v1 | **35** |
| ACL deny | **30**, errorMessage **null** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 93 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (parse + reject) |
| `crates/volant-broker/tests/v291_streams_group_topology_description_update.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 93 v0 reject |
| `docs/V291_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-1071.** No streams group coordinator. Topology is not
  persisted.
- Official apiKey is **93**. Official validVersions is **0** only.
  Official first flex is **0+**; Volant v0 is flexible (matches
  official).
- Does **not** wrap StreamsGroupHeartbeat 88 / StreamsGroupDescribe
  89 / Heartbeat 12 / ConsumerGroupHeartbeat 68 /
  ShareGroupHeartbeat 76.
- Official response is throttle + error + errorMessage only.
- `group.rs` hard `== 89` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V285_SPEC.md](./V285_SPEC.md) — StreamsGroupHeartbeat 88 reject
- [V286_SPEC.md](./V286_SPEC.md) — StreamsGroupDescribe 89 reject
- Classic Heartbeat key **12** — keep-alive clients must use
