# v0.253 — Kafka GetTelemetrySubscriptions key 71 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **GetTelemetrySubscriptions** (API key **71**,
version **0** only, always flexible). Volant has **no** client telemetry
(KIP-714). Parse the request; return error **0**, echo
`clientInstanceId`, `subscriptionId = 0`, empty `requestedMetrics`,
`pushIntervalMs = -1` (do not push), `telemetryMaxBytes = 0`,
`deltaTemporality = false`, empty accepted compression. Nothing
persisted. Not a metrics pipeline.

This is residual **v0.253**. Do **not** add PushTelemetry (key **72**).
Do **not** touch WriteTxnMarkers, AssignReplicasToDirs,
ListClientMetricsResources, TxnOffsetCommit, or `group.rs`.

## Goals

1. Advertise `(ApiKey::GetTelemetrySubscriptions, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 53`. Do **not** change hard
   `== 52` asserts.
2. Always flexible. Dispatch v0 only. v1+ → **35**.
3. Official Kafka GetTelemetrySubscriptionsRequest v0 (flex):
   `clientInstanceId` uuid (16 bytes), leftover `subscriptionId` i32
   if present, tagged. Official Apache Kafka request is uuid + tagged
   only (`subscriptionId` is a response field).
4. Response matches official `GetTelemetrySubscriptionsResponse.json`.
5. Controller is **not** required.
6. ACL: Cluster **DESCRIBE**. Disabled ACLs allow. Denied → error **31**;
   still echo `clientInstanceId`; empty metrics.

## Non-goals

| Deferred | Why |
|----------|-----|
| PushTelemetry (key 72) | Sibling leftover; do not advertise |
| Persist subscriptions / metrics | Not a telemetry pipeline |
| KIP-714 client metrics | Honest empty / do not push |
| Assign a clientInstanceId | Echo request; zeros stay zeros |
| WriteTxnMarkers / AssignReplicasToDirs / ListClientMetricsResources | Sibling leftovers |
| TxnOffsetCommit / `group.rs` | Orthogonal |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Request (official Kafka is uuid + tagged; leftover `subscriptionId` accepted):

```
clientInstanceId uuid
subscriptionId i32            // optional leftover; ignored
tagged
```

Response (official field order):

```
throttleTimeMs i32 = 0
errorCode i16 = 0
clientInstanceId uuid         // echo request
subscriptionId i32 = 0        // no subscription
acceptedCompressionTypes[]    // empty compact []int8
pushIntervalMs i32 = -1
telemetryMaxBytes i32 = 0
deltaTemporality bool = false
requestedMetrics[]            // empty compact strings
tagged
```

## Semantics

```
GetTelemetrySubscriptions v0
  │
  ├─ Cluster DESCRIBE fail → 31, echo clientInstanceId, empty metrics
  ├─ Controller not required
  │
  └─ else → error 0, echo clientInstanceId
            subscriptionId 0
            acceptedCompressionTypes empty
            pushIntervalMs -1 (do not push)
            telemetryMaxBytes 0
            deltaTemporality false
            requestedMetrics empty
            nothing persisted
```

- Response throttle is always 0.
- Official Apache Kafka request has no `subscriptionId`; Volant consumes
  it when present so residual clients that send it still decode.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v253_get_telemetry_subscriptions -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **71** min=0 max=0; `SUPPORTED_APIS.len() >= 53` |
| v0 random UUID | error **0**, echoed id, subscriptionId **0**, empty metrics, pushIntervalMs **-1**; no telemetry files |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 71 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode empty; ACL deny 31; no persist |
| `crates/volant-broker/tests/v253_get_telemetry_subscriptions.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 71 v0 |
| `docs/V253_SPEC.md` | This spec |

## Honesty leftovers

- **Not** KIP-714. No subscription store, no metrics push, no
  assigned `clientInstanceId`.
- `pushIntervalMs = -1` means do not push. PushTelemetry (72) is
  **not** advertised.
- `group.rs` `SUPPORTED_APIS.len()==52` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V249_SPEC.md](./V249_SPEC.md) — previous Kafka admin reject
