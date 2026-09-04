# v0.263 — Kafka BrokerRegistration key 62 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **BrokerRegistration** (API key **62**,
version **0** only, always flexible). Volant membership is overlay
`membership.json` + native AddBroker (102–107), **not** KRaft
BrokerRegistration (no incarnation, no DirectoryId, no features).
Parse the request; return error **42** `INVALID_REQUEST`
(`not KRaft broker registration`). Do **not** call `add_broker`.
Do **not** invent incarnation / DirectoryId. UnregisterBroker stays
the invert of native remove.

This is residual **v0.263**. Official Apache Kafka
`BrokerRegistrationRequest.json` uses apiKey **62** (BrokerHeartbeat
is **63**). BrokerHeartbeat is not advertised. Official field layout
is used. Do **not** touch Expire/Renew tokens, OffsetCommit epoch,
ConsumerGroupDescribe, UnregisterBroker / native AddBroker behavior,
or `group.rs`.

## Goals

1. Advertise `(ApiKey::BrokerRegistration, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 61`. Do **not** change
   hard `== 60` asserts.
2. Always flexible (Kafka `flexibleVersions: 0+`). Dispatch v0 only.
   v1+ → **35**.
3. Official BrokerRegistrationRequest v0 (confirmed
   `BrokerRegistrationRequest.json`): `brokerId` i32, `clusterId`
   compact string, `incarnationId` uuid, `listeners[]`, `features[]`,
   `rack` compact nullable string, tagged. Parse enough to consume
   the body without panicking. Do **not** persist any field.
4. Response matches official `BrokerRegistrationResponse.json`:
   throttle **0**, error **42**, brokerEpoch **-1** (official default).
   Nothing added to membership overlay.
5. Controller is **not** required (local reject **42** so single-node
   tests stay simple).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**.

## Non-goals

| Deferred | Why |
|----------|-----|
| Wrap native AddBroker | Opposite of UnregisterBroker; not KRaft |
| Incarnation / DirectoryId / features | Not a KRaft controller |
| Official apiKey 62 / BrokerHeartbeat 63 | Residual advertises 63 |
| Versions 1+ (ZK migration / LogDirs / previous epoch) | Advertised max is 0 |
| Expire/Renew tokens, OffsetCommit epoch, ConsumerGroupDescribe | Sibling leftovers |
| UnregisterBroker / native AddBroker behavior | Already shipped |
| `group.rs` hard `== 60` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`BrokerRegistrationRequest.json`;
`flexibleVersions: 0+`):

```
BrokerId i32
ClusterId compact string
IncarnationId uuid
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
Rack compact nullable string
tagged
```

IsMigratingZkBroker is v1+, LogDirs v2+, PreviousBrokerEpoch v3+ —
out of advertised range.

Official response (`BrokerRegistrationResponse.json`; no
`errorMessage`; `brokerEpoch` default **-1**):

```
throttleTimeMs i32 = 0
errorCode i16 = 42
brokerEpoch i64 = -1
tagged
```

## Semantics

```
BrokerRegistration v0
  │
  ├─ Cluster ALTER fail → 31, brokerEpoch -1
  ├─ Controller not required
  │
  └─ else → throttle 0, error 42 INVALID_REQUEST
            (not KRaft broker registration)
            brokerEpoch -1
            nothing persisted
            membership.json / broker list unchanged
            add_broker is not called
```

- Response throttle is always 0.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v263_broker_registration -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **62** min=0 max=0; key **64** still listed; `SUPPORTED_APIS.len() >= 61` |
| v0 register | **42**; `brokerEpoch` **-1**; no new overlay file if none existed; existing brokers unchanged |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 63 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v263_broker_registration.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 62 v0 |
| `docs/V263_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a KRaft controller. No incarnation epoch, no DirectoryId,
  no feature negotiation, no assigned broker epoch.
- Does **not** wrap native AddBroker (102–107). Membership overlay
  is unchanged.
- Official Apache Kafka apiKey is **62**; this residual advertises
  **63**. Official request/response field layout is used.
- Official response has no `errorMessage`.
- UnregisterBroker (**64**) remains the invert of native remove.
- `group.rs` `SUPPORTED_APIS.len()==60` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V242_SPEC.md](./V242_SPEC.md) — UnregisterBroker wrap of remove
- [V10_SPEC.md](./V10_SPEC.md) — dynamic membership overlay
