# v0.274 — Kafka UnregisterController key 94 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **UnregisterController** (API key **94**,
version **0** only, always flexible). Volant is **not** a KRaft
controller quorum. Parse the official v0 body and reject **42**
`INVALID_REQUEST` (`not KRaft controller unregister`). Do **not**
call `remove_broker`. Do **not** invent a KRaft unregister record.
Overlay membership is unchanged.

This is residual **v0.274**, not Phase 155. Official Apache Kafka
`UnregisterControllerRequest.json` uses apiKey **94**.
ControllerRegistration is already **70**; UnregisterBroker is already
**64**. Official field layout is used.

## Goals

1. Advertise `(ApiKey::UnregisterController, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 70`. Do **not** change
   hard `== 69` asserts.
2. Always flexible (Kafka `flexibleVersions: 0+`). Dispatch v0 only.
   v1+ → **35**.
3. Official UnregisterControllerRequest v0 (confirmed
   `UnregisterControllerRequest.json`): `controllerId` i32, tagged.
   Parse enough to consume the body without panicking. Do **not**
   persist any field.
4. Response matches official `UnregisterControllerResponse.json`:
   throttle **0**, error **42**, errorMessage
   `"not KRaft controller unregister"`. Nothing removed from
   membership overlay.
5. Controller is **not** required (local reject **42** so single-node
   tests stay simple).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**,
   errorMessage null.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft controller quorum / unregister record | Not a KRaft controller |
| Wrap native `remove_broker` | That is UnregisterBroker **64** |
| Changing ControllerRegistration 70 | Reject sibling; leave alone |
| Vote 52 / AddRaftVoter 80 / RemoveRaftVoter 81 / UpdateRaftVoter 82 | Sibling leftovers |
| Versions 1+ | Official / advertised max is 0 |
| `group.rs` hard `== 69` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`UnregisterControllerRequest.json`;
`flexibleVersions: 0+`):

```
ControllerId i32
tagged
```

Official response (`UnregisterControllerResponse.json`):

```
throttleTimeMs i32 = 0
errorCode i16 = 42            // or 31 if Cluster ALTER denied
errorMessage compact nullable string = "not KRaft controller unregister"  // null if ACL deny
tagged
```

Official `validVersions` is **0** only (matches advertisement).

## Semantics

```
UnregisterController v0
  │
  ├─ Cluster ALTER fail → 31, errorMessage null
  ├─ Controller not required
  │
  └─ else → throttle 0, error 42 INVALID_REQUEST
            (not KRaft controller unregister)
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
cargo test -p volant-broker --test v274_unregister_controller -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **94** min=0 max=0; keys **64** and **70** still listed; `SUPPORTED_APIS.len() >= 70` |
| v0 unregister | throttle **0**, error **42**, errorMessage present; no new overlay file if none existed; existing brokers unchanged; `remove_broker` not called |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 94 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v274_unregister_controller.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 94 v0 |
| `docs/V274_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a KRaft controller. No unregister record.
- Does **not** wrap native `remove_broker`.
- Official apiKey is **94**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official validVersions is 0 only (matches advertisement).
- ControllerRegistration **70** remains the reject sibling.
- `group.rs` hard `== 69` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V268_SPEC.md](./V268_SPEC.md) — ControllerRegistration reject
- [V242_SPEC.md](./V242_SPEC.md) — UnregisterBroker wrap of remove
- [V10_SPEC.md](./V10_SPEC.md) — dynamic membership overlay
