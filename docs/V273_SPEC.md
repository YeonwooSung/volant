# v0.273 — Kafka UpdateRaftVoter key 82 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **UpdateRaftVoter** (API key **82**,
version **0** only, always flexible). Volant is **not** a KRaft
voter set. Parse the official v0 body and reject **42**
`INVALID_REQUEST` (`not KRaft raft voter`). Do **not** persist
listeners / `KRaftVersionFeature`. Overlay membership is unchanged.

This is residual **v0.273**, not Phase 155. Official Apache Kafka
`UpdateRaftVoterRequest.json` uses apiKey **82**. AddRaftVoter is
official **80**; RemoveRaftVoter is **81** (siblings — not
implemented). Official field layout is used.

## Goals

1. Advertise `(ApiKey::UpdateRaftVoter, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 70`. Do **not** change
   hard `== 69` asserts.
2. Always flexible (Kafka `flexibleVersions: 0+`). Dispatch v0 only.
   v1+ → **35**.
3. Official UpdateRaftVoterRequest v0 (confirmed
   `UpdateRaftVoterRequest.json`): `clusterId` compact nullable
   string, `currentLeaderEpoch` i32, `voterId` i32,
   `voterDirectoryId` uuid, `listeners[]`, inline
   `KRaftVersionFeature` (`min` i16 + `max` i16 + tags), tagged.
   Parse enough to consume the body without panicking. Do **not**
   persist any field.
4. Response matches official `UpdateRaftVoterResponse.json`:
   throttle **0**, error **42**. Official response has **no**
   `errorMessage`. CurrentLeader (official tag 0) is omitted (empty
   tag buffer). Nothing added to membership overlay.
5. Controller is **not** required (local reject **42** so single-node
   tests stay simple).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**,
   empty tags.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft voter set / DirectoryId / KRaft version feature store | Not a KRaft controller |
| Writing CurrentLeader tag 0 | Official tag omitted (empty tags) |
| Vote 52 / AddRaftVoter 80 / RemoveRaftVoter 81 / UnregisterController 94 | Sibling leftovers |
| Versions 1+ | Official / advertised max is 0 |
| `group.rs` hard `== 69` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`UpdateRaftVoterRequest.json`;
`flexibleVersions: 0+`):

```
ClusterId compact nullable string
CurrentLeaderEpoch i32
VoterId i32
VoterDirectoryId uuid
Listeners[] {
  Name compact string
  Host compact string
  Port uint16
  tagged
}
KRaftVersionFeature {
  MinSupportedVersion i16
  MaxSupportedVersion i16
  tagged
}
tagged
```

`KRaftVersionFeature` is an **inline untagged struct** (not
nullable): min i16 + max i16 + tags.

Official response (`UpdateRaftVoterResponse.json`; no
`errorMessage`; CurrentLeader is taggedVersions 0+ tag 0 — omit):

```
throttleTimeMs i32 = 0
errorCode i16 = 42            // or 31 if Cluster ALTER denied
tagged
```

Official `validVersions` is **0** only (matches advertisement).

## Semantics

```
UpdateRaftVoter v0
  │
  ├─ Cluster ALTER fail → 31, empty tags
  ├─ Controller not required
  │
  └─ else → throttle 0, error 42 INVALID_REQUEST
            (not KRaft raft voter)
            empty tags (CurrentLeader omitted)
            nothing persisted
            membership.json / broker list unchanged
            listeners / KRaftVersionFeature discarded
```

- Response throttle is always 0.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v273_update_raft_voter -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **82** min=0 max=0; `SUPPORTED_APIS.len() >= 70` |
| v0 update voter | header v1 tags, throttle **0**, error **42**, empty tags; no new overlay file if none existed; existing brokers unchanged |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 82 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v273_update_raft_voter.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 82 v0 |
| `docs/V273_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a KRaft controller. No voter set, no KRaft version feature
  store.
- Official apiKey is **82**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official validVersions is 0 only (matches advertisement).
- Official response has no `errorMessage`. CurrentLeader tag is
  omitted.
- `group.rs` hard `== 69` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V263_SPEC.md](./V263_SPEC.md) — BrokerRegistration reject
- [V268_SPEC.md](./V268_SPEC.md) — ControllerRegistration reject
