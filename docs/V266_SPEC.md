# v0.266 — Kafka Envelope key 58 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **Envelope** (API key **58**, version **0**
only, always flexible). Volant has **no request forwarding**
(KIP-590). Parse the official v0 body and reject **42**
`INVALID_REQUEST` (`forwarding not supported`). Do **not** unwrap or
forward the embedded request. Do **not** invent a forwarding path.

This is residual **v0.266**, not Phase 155. Official Apache Kafka
`EnvelopeRequest.json` uses apiKey **58**. FetchSnapshot is official
**59** (sibling leftover — not implemented). DescribeCluster stays
**60**. Official field layout is used.

## Goals

1. Advertise `(ApiKey::Envelope, 0, 0)` in `SUPPORTED_APIS` (numeric
   order after UpdateFeatures **57**, before DescribeCluster **60**).
   Soft length assert `>= 65`. Do **not** change hard `== 64` asserts
   in `group.rs`, `v206_*`, `v225_*`, `v228_*`, `v233_*`.
2. Always flexible (Kafka `flexibleVersions: 0+`). Dispatch v0 only.
   v1+ → **35**.
3. Official EnvelopeRequest v0 (confirmed `EnvelopeRequest.json`):
   `RequestData` compact bytes, `RequestPrincipal` compact nullable
   bytes, `ClientHostAddress` compact bytes, tagged. Parse enough to
   consume the body without panicking. Discard every field. Do **not**
   interpret `RequestData` as an inner Kafka request. Do **not**
   persist.
4. Response matches official `EnvelopeResponse.json`. **There is no
   `throttleTimeMs`**: `ResponseData` compact nullable bytes = **null**,
   `ErrorCode` i16 = **42** (or **31** if Cluster ALTER denied), tagged.
5. Controller is **not** required (local reject).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-590 forwarding / unwrap inner request | Not implemented |
| Principal impersonation | No forwarding path |
| FetchSnapshot 59, BrokerHeartbeat 63, ControllerRegistration 70, ConsumerGroupHeartbeat 68 | Sibling leftovers |
| Versions 1+ | Advertised max is 0 |
| `group.rs` hard `== 64` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`EnvelopeRequest.json`; `bytes` fields are
compact under flex 0+):

```
RequestData compact bytes          // embedded request; discard
RequestPrincipal compact nullable bytes
ClientHostAddress compact bytes
tagged
```

`uvarint 0` = null (`get_compact_bytes`).

Official response (`EnvelopeResponse.json`; **no throttleTimeMs**):

```
ResponseData compact nullable bytes   // always null — we do not forward
ErrorCode i16
tagged
```

## Semantics

```
Envelope v0
  │
  ├─ Cluster ALTER fail → 31, ResponseData null
  ├─ Controller not required
  │
  └─ else → ResponseData null, error 42 INVALID_REQUEST
            (forwarding not supported)
            RequestData discarded (not unwrapped / not executed)
            membership / topics unchanged
```

- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official).
- Official response has no `throttleTimeMs`.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v266_envelope -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **58** min=0 max=0; keys **57** and **60** still listed; `SUPPORTED_APIS.len() >= 65` |
| v0 envelope with dummy compact bytes | header v1 tags, ResponseData null (uvarint 0), error **42**; no inner request executed (membership / topics unchanged) |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 58 + `from_i16` + `SUPPORTED_APIS` + soft test + crate-doc |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no unwrap |
| `crates/volant-broker/tests/v266_envelope.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 58 v0 reject |
| `docs/V266_SPEC.md` | This spec |

## Honesty leftovers

- No request forwarding. Embedded RequestData is discarded.
- Official apiKey is **58**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official response has no throttleTimeMs.
- `group.rs` hard `== 64` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V263_SPEC.md](./V263_SPEC.md) — BrokerRegistration reject (same class)
- [V255_SPEC.md](./V255_SPEC.md) — PushTelemetry reject
