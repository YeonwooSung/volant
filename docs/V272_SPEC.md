# v0.272 — Kafka RemoveRaftVoter key 81 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **RemoveRaftVoter** (API key **81**,
version **0** only, always flexible). Volant membership is overlay
`membership.json` + native `remove_broker` — **not** KRaft voter
membership. Parse the official v0 body and reject **42**
`INVALID_REQUEST` (`not KRaft raft voter`). Do **not** call
`remove_broker`. Do **not** invent a voter set / DirectoryId.
Overlay membership is unchanged.

This is residual **v0.272**, not Phase 155. Official Apache Kafka
`RemoveRaftVoterRequest.json` uses apiKey **81**. AddRaftVoter is
official **80**; UpdateRaftVoter is official **82** (siblings — not
advertised). UnregisterBroker **64** stays the invert of native
remove.

## Goals

1. Advertise `(ApiKey::RemoveRaftVoter, 0, 0)` in `SUPPORTED_APIS`
   (numeric order after DescribeTopicPartitions **75**; 80 is absent).
   Soft length assert `>= 70`. Do **not** change hard `== 69` asserts
   in `group.rs`, `v206_*`, `v225_*`, `v228_*`, `v233_*`.
2. Always flexible (Kafka `flexibleVersions: 0+`). Dispatch v0 only.
   v1+ → **35**.
3. Official RemoveRaftVoterRequest v0 (confirmed
   `RemoveRaftVoterRequest.json`): `clusterId` compact nullable string,
   `voterId` i32, `voterDirectoryId` uuid, tagged. Parse enough to
   consume the body without panicking. Do **not** persist any field.
   Do **not** call `remove_broker`.
4. Response matches official `RemoveRaftVoterResponse.json`:
   throttle **0**, error **42**, errorMessage
   `"not KRaft raft voter"`. Existing overlay brokers stay put.
5. Controller is **not** required (local reject **42** so single-node
   tests stay simple).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**,
   errorMessage null.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft voter set / DirectoryId | Not a KRaft controller |
| Wrap native `remove_broker` | UnregisterBroker **64** is already that invert |
| Changing membership overlay | Unchanged |
| Vote 52 / AddRaftVoter 80 / UpdateRaftVoter 82 / UnregisterController 94 | Sibling leftovers |
| Versions 1+ | Official / advertised max is 0 |
| `group.rs` hard `== 69` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`RemoveRaftVoterRequest.json`;
`flexibleVersions: 0+`):

```
ClusterId compact nullable string
VoterId i32
VoterDirectoryId uuid
tagged
```

Official response (`RemoveRaftVoterResponse.json`):

```
throttleTimeMs i32 = 0
errorCode i16 = 42            // or 31 if Cluster ALTER denied
errorMessage compact nullable string = "not KRaft raft voter"  // null if ACL deny
tagged
```

Official `validVersions` is **0** only (matches advertisement).

## Semantics

```
RemoveRaftVoter v0
  │
  ├─ Cluster ALTER fail → 31, errorMessage null
  ├─ Controller not required
  │
  └─ else → throttle 0, error 42 INVALID_REQUEST
            (not KRaft raft voter)
            nothing persisted
            membership.json / broker list unchanged
            remove_broker is not called
```

- Response throttle is always 0.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v272_remove_raft_voter -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **81** min=0 max=0; key **64** still listed; `SUPPORTED_APIS.len() >= 70` |
| v0 remove voter | throttle **0**, error **42**, errorMessage present; existing overlay brokers unchanged; `remove_broker` not called |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 81 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v272_remove_raft_voter.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 81 v0 |
| `docs/V272_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a KRaft controller. No voter set, no DirectoryId.
- Does **not** wrap native `remove_broker`. UnregisterBroker **64**
  remains the invert of native remove.
- Official apiKey is **81**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official validVersions is 0 only (matches advertisement).
- `group.rs` hard `== 69` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V268_SPEC.md](./V268_SPEC.md) — ControllerRegistration reject
- [V242_SPEC.md](./V242_SPEC.md) — UnregisterBroker wrap of remove
- [V10_SPEC.md](./V10_SPEC.md) — dynamic membership overlay
