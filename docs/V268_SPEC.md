# v0.268 — Kafka ControllerRegistration key 70 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ControllerRegistration** (API key **70**,
version **0** only, always flexible). Volant is **not** a KRaft
controller quorum. Parse the official v0 body and reject **42**
`INVALID_REQUEST` (`not KRaft controller registration`). Do **not**
call `add_broker`. Do **not** invent incarnation / ZK migration /
controller listener persistence. Overlay membership is unchanged.

This is residual **v0.268**, not Phase 155. Official Apache Kafka
`ControllerRegistrationRequest.json` uses apiKey **70**.
ConsumerGroupDescribe is already **69**; GetTelemetrySubscriptions is
already **71**. ConsumerGroupHeartbeat (**68**) is a sibling leftover
and is not advertised. Official field layout is used.

## Goals

1. Advertise `(ApiKey::ControllerRegistration, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 65`. Do **not** change
   hard `== 64` asserts.
2. Always flexible (Kafka `flexibleVersions: 0+`). Dispatch v0 only.
   v1+ → **35**.
3. Official ControllerRegistrationRequest v0 (confirmed
   `ControllerRegistrationRequest.json`): `controllerId` i32,
   `incarnationId` uuid, `zkMigrationReady` bool, `listeners[]`,
   `features[]`, tagged. Parse enough to consume the body without
   panicking. Do **not** persist any field.
4. Response matches official `ControllerRegistrationResponse.json`:
   throttle **0**, error **42**, errorMessage
   `"not KRaft controller registration"`. Nothing added to membership
   overlay.
5. Controller is **not** required (local reject **42** so single-node
   tests stay simple).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**,
   errorMessage null.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft controller quorum / incarnation / ZK migration / RegisterControllerRecord | Not a KRaft controller |
| Wrap native AddBroker | BrokerRegistration 62 already refuses this |
| Changing membership overlay | Unchanged |
| Envelope 58 / FetchSnapshot 59 / BrokerHeartbeat 63 / ConsumerGroupHeartbeat 68 | Sibling leftovers |
| Versions 1+ | Official / advertised max is 0 |
| `group.rs` hard `== 64` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`ControllerRegistrationRequest.json`;
`flexibleVersions: 0+`):

```
ControllerId i32
IncarnationId uuid
ZkMigrationReady bool
Listeners[] {
  Name compact string
  Host compact string
  Port uint16
  SecurityProtocol i16
  tagged
}
Features[] {
  Name compact string
  MinSupportedVersion i16
  MaxSupportedVersion i16
  tagged
}
tagged
```

This is very close to BrokerRegistration v0 (minus clusterId, plus
ZkMigrationReady).

Official response (`ControllerRegistrationResponse.json`):

```
throttleTimeMs i32 = 0
errorCode i16 = 42            // or 31 if Cluster ALTER denied
errorMessage compact nullable string = "not KRaft controller registration"  // null if ACL deny
tagged
```

Official `validVersions` is **0** only (matches advertisement).

## Semantics

```
ControllerRegistration v0
  │
  ├─ Cluster ALTER fail → 31, errorMessage null
  ├─ Controller not required
  │
  └─ else → throttle 0, error 42 INVALID_REQUEST
            (not KRaft controller registration)
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
cargo test -p volant-broker --test v268_controller_registration -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **70** min=0 max=0; keys **69** and **71** still listed; `SUPPORTED_APIS.len() >= 65` |
| v0 register | throttle **0**, error **42**, errorMessage present; no new overlay file if none existed; existing brokers unchanged; `add_broker` not called |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 70 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v268_controller_registration.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 70 v0 |
| `docs/V268_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a KRaft controller. No incarnation, no ZK migration, no
  controller listener store.
- Does **not** wrap native AddBroker.
- Official apiKey is **70**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official validVersions is 0 only (matches advertisement).
- `group.rs` hard `== 64` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V263_SPEC.md](./V263_SPEC.md) — BrokerRegistration reject
- [V242_SPEC.md](./V242_SPEC.md) — UnregisterBroker wrap of remove
- [V10_SPEC.md](./V10_SPEC.md) — dynamic membership overlay
