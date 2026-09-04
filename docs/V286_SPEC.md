# v0.286 — Kafka StreamsGroupDescribe key 89 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **StreamsGroupDescribe** (API key **89**,
version **0** only, always flexible). Volant has **no** KIP-1071
streams groups. Parse the official v0 body and reject each requested
group with **42** `INVALID_REQUEST` (`not KIP-1071 streams group`).
Do **not** call `describe_group`. Do **not** wrap
ConsumerGroupDescribe **69**, DescribeGroups **15**, or
ShareGroupDescribe **77**. Members are empty. Topology is **null**.
Do **not** invent streams-group state.

This is residual **v0.286**, not Phase 155. Official Apache Kafka
`StreamsGroupDescribeRequest.json` uses apiKey **89**. Official
`validVersions` is **0–1** (v1 adds IncludeTopologyDescription /
KIP-1331). Volant advertises **v0 only**. Official response has
`throttleTimeMs` and **no** top-level error besides throttle. Closer
to ShareGroupDescribe **77** / ConsumerGroupDescribe **69** than
InitializeShareGroupState **83**.

## Goals

1. Advertise `(ApiKey::StreamsGroupDescribe, 0, 0)` in
   `SUPPORTED_APIS` (numeric order after ReadShareGroupStateSummary
   **87**, before DescribeShareGroupOffsets **90**). Soft length
   assert `>= 85`. Do **not** change hard `== 84` asserts.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch
   **v0** only. v1+ → **35**.
3. Official `StreamsGroupDescribeRequest.json` v0. Parse loosely
   (`GroupIds` compact array of compact string,
   `IncludeAuthorizedOperations` bool, tagged). Do **not** parse
   `IncludeTopologyDescription` (v1+). Discard every field except
   the echoed group ids. Do **not** call `describe_group()`.
4. Official response has **no top-level error** besides throttle.
   Echo each requested groupId with per-group error **42**, empty
   members, **null** Topology (official: null on describe error),
   empty `groupState`, `groupEpoch` **-1**, `assignmentEpoch` **-1**,
   `authorizedOperations` INT32_MIN (or **0** if
   `includeAuthorizedOperations` is true — do not invent ACL bits).
   `errorMessage` `"not KIP-1071 streams group"`. Unparseable body →
   throttle **0** + empty `Groups[]`.
5. Controller is **not** required (group-local reject).
6. ACL: Group **DESCRIBE** per group id. Disabled ACLs allow the
   **42** path. Denied → per-group **30**, empty members, Topology
   null, errorMessage **null**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-1071 streams groups | Classic groups only |
| Wrap `describe_group` / ConsumerGroupDescribe 69 / DescribeGroups 15 / ShareGroupDescribe 77 | Clients keep using 15 / 69 / 77 |
| Invent streams-group state / members / topology | Reject only; members empty; Topology null |
| StreamsGroupHeartbeat 88 / topology-update | Sibling leftovers |
| Advertise v1 (IncludeTopologyDescription / KIP-1331) | Out of range |
| join-set, unclean, live reassignment, txn default-on, Kafka Fetch group tags | Out of slice |
| `group.rs` hard `== 84` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`StreamsGroupDescribeRequest.json`;
`flexibleVersions: 0+`):

```
GroupIds compact array of compact string
IncludeAuthorizedOperations bool
tagged
```

`IncludeTopologyDescription` is official versions `1+` and is **not**
parsed.

Official response v0 (`StreamsGroupDescribeResponse.json`; official
has **no** top-level error besides throttle). Topology is a nullable
struct (unsigned varint **0** = null). Member struct is not written
because `members` is empty. AssignorName / TopologyDescription /
TopologyDescriptionStatus are official v1+ and are **not** written:

```
throttleTimeMs i32 = 0
groups[] {
  errorCode i16                    // 42, or 30 if Group DESCRIBE denied
  errorMessage compact nullable string
                                   // "not KIP-1071 streams group";
                                   // null on ACL deny
  groupId compact string
  groupState compact string        // empty
  groupEpoch i32                   // -1
  assignmentEpoch i32              // -1
  topology nullable struct         // null (describe error)
  members[] compact empty
  authorizedOperations i32         // INT32_MIN (omit default);
                                   // 0 if includeAuthorizedOperations
  tagged
}
tagged
```

Official supported errors include `INVALID_REQUEST` (**42**) and
`GROUP_AUTHORIZATION_FAILED` (**30**). Do **not** invent streams
members, assignment, or topology.

## Semantics

```
StreamsGroupDescribe v0
  │
  ├─ Group DESCRIBE fail → per-group 30, errorMessage null,
  │                         empty members, Topology null;
  │                         no group mutation
  ├─ Unparseable body → throttle 0 + empty Groups[] (no top-level 0)
  ├─ Controller not required
  │
  └─ else → per-group 42 "not KIP-1071 streams group"
             empty members, Topology null, empty groupState,
             groupEpoch / assignmentEpoch -1
             classic membership unchanged
```

- Response throttle is always 0. No top-level error besides throttle.
- Does **not** wrap classic DescribeGroups **15**,
  ConsumerGroupDescribe **69**, or ShareGroupDescribe **77**.
  Clients must keep using those.
- Official Apache Kafka `validVersions` is **0–1**. Volant
  advertises **0** only (v1 IncludeTopologyDescription / KIP-1331
  is out of range).
- Official first flex is **0+**; Volant v0 is flexible (matches
  official). v1 is not advertised and returns **35**.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v286_streams_group_describe -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **89** min=max=0; keys **15**, **69**, and **77** still listed; `SUPPORTED_APIS.len() >= 85` |
| v0 describe one group | throttle **0**, one group, error **42**, empty members, Topology null; does not wrap classic describe |
| v1 | **35** |
| ACL deny | that group **30**, errorMessage **null**, empty members |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 89 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (parse + per-group reject) |
| `crates/volant-broker/tests/v286_streams_group_describe.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 89 v0 reject |
| `docs/V286_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-1071.** Official validVersions is **0–1**. Volant
  advertises **0** only (v1 IncludeTopologyDescription / KIP-1331
  is out of range).
- Does **not** wrap classic describe snapshot / 69 / 15 / 77.
- Official response has throttle and no top-level error; reject is
  per-group **42**. Topology is null on describe error.
- `group.rs` hard `== 84` untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V276_SPEC.md](./V276_SPEC.md) — ShareGroupDescribe 77 reject
- [V264_SPEC.md](./V264_SPEC.md) — ConsumerGroupDescribe 69 wrap
- DescribeGroups key **15** — classic describe clients must use
