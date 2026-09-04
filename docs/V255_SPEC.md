# v0.255 — Kafka PushTelemetry key 72 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **PushTelemetry** (API key **72**, version
**0** only, always flexible). Volant has **no** client telemetry
(KIP-714). Parse the request; return throttle=0, error **42**
`INVALID_REQUEST`. Do **not** persist metrics bytes.
GetTelemetrySubscriptions stays empty / `pushIntervalMs = -1`.

This is residual **v0.255**. Do **not** touch OffsetFetch
RequireStable, AlterPartition, DelegationToken APIs, GetTelemetry
behavior, `group.rs` hard asserts, crate bump, or join-set / unclean /
live reassignment / txn default-on.

## Goals

1. Advertise `(ApiKey::PushTelemetry, 0, 0)` in `SUPPORTED_APIS`. Soft
   length assert `>= 57`. Do **not** change hard `== 56` asserts
   (group.rs / v206 / v225 / v228 / v233).
2. Always flexible. Dispatch v0 only. v1+ → **35**.
3. Official Kafka PushTelemetryRequest v0 (flex) from
   `PushTelemetryRequest.json`: `clientInstanceId` uuid, `subscriptionId`
   i32, `terminating` bool, `compressionType` i8, `metrics` compact
   bytes, tagged. Parse and discard.
4. Response matches official `PushTelemetryResponse.json` (no
   `errorMessage`): throttleTimeMs i32 = 0, errorCode i16 = **42**,
   tagged. Contract: **42**, nothing persisted.
5. Controller is **not** required.
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31** (still
   nothing persisted).

## Non-goals

| Deferred | Why |
|----------|-----|
| Persist metrics / client telemetry pipeline | Not KIP-714 |
| GetTelemetrySubscriptions behavior | Already shipped empty / do not push |
| errorMessage on response | Official schema has none |
| OffsetFetch RequireStable / AlterPartition / DelegationToken | Orthogonal leftovers |
| `group.rs` hard `== 56` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Request (official Kafka field order):

```
clientInstanceId uuid
subscriptionId i32
terminating bool
compressionType i8
metrics compact bytes
tagged
```

Response (official field order; no errorMessage):

```
throttleTimeMs i32 = 0
errorCode i16 = 42
tagged
```

## Semantics

```
PushTelemetry v0
  │
  ├─ Cluster ALTER fail → 31, nothing persisted
  ├─ Controller not required
  │
  └─ else → throttle 0, error 42 INVALID_REQUEST
            metrics discarded
            nothing persisted
```

- Response throttle is always 0.
- Official Apache Kafka response has no `errorMessage`.
- GetTelemetrySubscriptions remains empty with `pushIntervalMs = -1`.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v255_push_telemetry -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **72** min=0 max=0; key **71** still listed; `SUPPORTED_APIS.len() >= 57` |
| v0 random UUID + metrics bytes | error **42**; no telemetry files under data_dir |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 72 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v255_push_telemetry.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 72 v0 |
| `docs/V255_SPEC.md` | This spec |

## Honesty leftovers

- **Not** KIP-714. No metrics store, no subscription, no push pipeline.
- GetTelemetrySubscriptions still returns empty / `pushIntervalMs = -1`.
- Official response has no errorMessage field.
- `group.rs` / v206 / v225 / v228 / v233 `SUPPORTED_APIS.len()==56`
  assertions are intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V253_SPEC.md](./V253_SPEC.md) — GetTelemetrySubscriptions empty
