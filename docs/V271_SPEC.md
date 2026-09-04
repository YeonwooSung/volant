# v0.271 — Kafka AddRaftVoter key 80 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **AddRaftVoter** (API key **80**,
version **0** only, always flexible). Volant membership is overlay
`membership.json` + native AddBroker — **not** KRaft voter
membership. Parse the official v0 body and reject **42**
`INVALID_REQUEST` (`not KRaft raft voter`). Do **not** call
`add_broker`. Do **not** invent a voter set / DirectoryId store.
Overlay membership is unchanged.

This is residual **v0.271**, not Phase 155. Official Apache Kafka
`AddRaftVoterRequest.json` uses apiKey **80**. DescribeTopicPartitions
is already **75**. RemoveRaftVoter (**81**) and UpdateRaftVoter (**82**)
are siblings and are not advertised. Official field layout is used.

## Goals

1. Advertise `(ApiKey::AddRaftVoter, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 70`. Do **not** change
   hard `== 69` asserts.
2. Always flexible (Kafka `flexibleVersions: 0+`). Dispatch v0 only.
   v1+ → **35**.
3. Official AddRaftVoterRequest v0 (confirmed
   `AddRaftVoterRequest.json`): `clusterId` compact nullable string,
   `timeoutMs` i32, `voterId` i32, `voterDirectoryId` uuid,
   `listeners[]`, tagged. Parse enough to consume the body without
   panicking. Do **not** persist any field. AckWhenCommitted is v1+
   and is not parsed.
4. Response matches official `AddRaftVoterResponse.json`:
   throttle **0**, error **42**, errorMessage
   `"not KRaft raft voter"`. Nothing added to membership overlay.
5. Controller is **not** required (local reject **42** so single-node
   tests stay simple).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**,
   errorMessage null.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft voter set / DirectoryId store / AckWhenCommitted | Not a KRaft controller |
| Wrap native AddBroker | BrokerRegistration 62 already refuses this |
| Changing membership overlay | Unchanged |
| Vote 52 / RemoveRaftVoter 81 / UpdateRaftVoter 82 / UnregisterController 94 | Sibling leftovers |
| Official versions 1+ | Advertised max is 0 |
| `group.rs` hard `== 69` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`AddRaftVoterRequest.json`;
`flexibleVersions: 0+`):

```
ClusterId compact nullable string
TimeoutMs i32
VoterId i32
VoterDirectoryId uuid
Listeners[] {
  Name compact string
  Host compact string
  Port uint16
  tagged
}
tagged
```

AckWhenCommitted is v1+ — do not parse as v0.

Official response (`AddRaftVoterResponse.json`):

```
throttleTimeMs i32 = 0
errorCode i16 = 42            // or 31 if Cluster ALTER denied
errorMessage compact nullable string = "not KRaft raft voter"  // null if ACL deny
tagged
```

Official `validVersions` is **0–1**. Volant advertises **v0 only**.

## Semantics

```
AddRaftVoter v0
  │
  ├─ Cluster ALTER fail → 31, errorMessage null
  ├─ Controller not required
  │
  └─ else → throttle 0, error 42 INVALID_REQUEST
            (not KRaft raft voter)
            nothing persisted
            membership.json / broker list unchanged
            add_broker is not called
```

- Response throttle is always 0.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v271_add_raft_voter -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **80** min=0 max=0; key **75** still listed; `SUPPORTED_APIS.len() >= 70` |
| v0 add voter | throttle **0**, error **42**, errorMessage present; no new overlay file if none existed; existing brokers unchanged; `add_broker` not called |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 80 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v271_add_raft_voter.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 80 v0 |
| `docs/V271_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a KRaft controller. No voter set, no DirectoryId store.
- Does **not** wrap native AddBroker.
- Official apiKey is **80**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official validVersions 0–1; Volant advertises 0 only.
- `group.rs` hard `== 69` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V268_SPEC.md](./V268_SPEC.md) — ControllerRegistration reject
- [V263_SPEC.md](./V263_SPEC.md) — BrokerRegistration reject
- [V242_SPEC.md](./V242_SPEC.md) — UnregisterBroker wrap of remove
- [V10_SPEC.md](./V10_SPEC.md) — dynamic membership overlay
