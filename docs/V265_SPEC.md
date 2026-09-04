# v0.265 — Kafka BrokerHeartbeat key 63 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **BrokerHeartbeat** (API key **63**,
version **0** only, always flexible). Volant is **not** a KRaft
controller (no fencing, no metadata offset catch-up, no assigned
epoch). Parse the request; return error **42** `INVALID_REQUEST`
(`not KRaft broker heartbeat`). Do **not** wrap native Heartbeat
(key 12) or `GroupCoordinator::heartbeat`. Do **not** invent
incarnation / fencing / metadata offset catch-up. Overlay membership
/ broker list / `brokerEpoch` are unchanged.

This is residual **v0.265**. Official Apache Kafka
`BrokerHeartbeatRequest.json` uses apiKey **63** (BrokerRegistration
is **62**; UnregisterBroker is **64**). Official field layout is used.
Do **not** touch BrokerRegistration, UnregisterBroker, native
Heartbeat 12, or `group.rs`.

## Goals

1. Advertise `(ApiKey::BrokerHeartbeat, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 65`. Do **not** change
   hard `== 64` asserts.
2. Always flexible (Kafka `flexibleVersions: 0+`). Dispatch v0 only.
   v1+ → **35**.
3. Official BrokerHeartbeatRequest v0 (confirmed
   `BrokerHeartbeatRequest.json`): `brokerId` i32, `brokerEpoch` i64,
   `currentMetadataOffset` i64, `wantFence` bool, `wantShutDown`
   bool, tagged. Parse enough to consume the body without panicking.
   Do **not** persist any field.
4. Response matches official `BrokerHeartbeatResponse.json`:
   throttle **0**, error **42**, `isCaughtUp` **false**, `isFenced`
   **true**, `shouldShutDown` **false** (official defaults). Nothing
   added to membership overlay.
5. Controller is **not** required (local reject **42** so single-node
   tests stay simple).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft heartbeat manager / fencing / metadata offset / assigned epoch | Not a KRaft controller |
| Wrap native Heartbeat 12 / `GroupCoordinator::heartbeat` | Different API (consumer group) |
| Wrap native AddBroker / change membership | That is BrokerRegistration 62, already reject |
| UnregisterBroker / BrokerRegistration behavior | Already shipped |
| Versions 1–2 (OfflineLogDirs / CordonedLogDirs) | Advertised max is 0 |
| Envelope / FetchSnapshot / ControllerRegistration / ConsumerGroupHeartbeat | Sibling leftovers |
| `group.rs` hard `== 64` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`BrokerHeartbeatRequest.json`;
`flexibleVersions: 0+`):

```
BrokerId i32
BrokerEpoch i64
CurrentMetadataOffset i64
WantFence bool
WantShutDown bool
tagged
```

OfflineLogDirs is v1+ tagged; CordonedLogDirs is v2+ tagged —
out of advertised range. Parse v0 then skip tags.

Official response (`BrokerHeartbeatResponse.json`; no
`errorMessage`; official defaults `isCaughtUp=false`,
`isFenced=true`, `shouldShutDown=false`):

```
throttleTimeMs i32 = 0
errorCode i16 = 42
isCaughtUp bool = false
isFenced bool = true
shouldShutDown bool = false
tagged
```

## Semantics

```
BrokerHeartbeat v0
  │
  ├─ Cluster ALTER fail → 31, isCaughtUp false, isFenced true,
  │                        shouldShutDown false
  ├─ Controller not required
  │
  └─ else → throttle 0, error 42 INVALID_REQUEST
            (not KRaft broker heartbeat)
            isCaughtUp false, isFenced true, shouldShutDown false
            nothing persisted
            membership.json / broker list unchanged
            add_broker is not called
            native Heartbeat 12 is not invoked
```

- Response throttle is always 0.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v265_broker_heartbeat -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **63** min=0 max=0; keys **62** and **64** still listed; `SUPPORTED_APIS.len() >= 65` |
| v0 heartbeat | **42**; `isCaughtUp` false, `isFenced` true, `shouldShutDown` false; no new overlay file if none existed; existing brokers unchanged |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 63 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v265_broker_heartbeat.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 63 v0 |
| `docs/V265_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a KRaft controller. No fencing, no metadata offset, no
  assigned epoch.
- Does **not** wrap native Heartbeat 12.
- Official apiKey is **63**; BrokerRegistration stays **62**.
- Official first flex is **0+**; Volant v0 is flexible (matches
  official).
- Official validVersions 0–2; Volant advertises 0 only.
- `group.rs` hard `== 64` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V263_SPEC.md](./V263_SPEC.md) — BrokerRegistration reject
- [V242_SPEC.md](./V242_SPEC.md) — UnregisterBroker wrap of remove
