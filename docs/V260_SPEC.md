# v0.260 — Kafka ExpireDelegationToken key 40 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ExpireDelegationToken** (API key **40**,
version **0** only, always flexible). Volant has **no**
delegation-token store. Parse the request; return error **42**
`INVALID_REQUEST` (`delegation tokens not supported`). Nothing
persisted.

This is residual **v0.260**. Official Kafka first flexible version is
**2**; this residual advertises **v0** as flexible (same class as
Create/Describe DelegationToken **38**/**41**). Do **not** implement
RenewDelegationToken (sibling). Do **not** change Create/Describe
behavior. Do **not** touch `group.rs` hard asserts, crate bump, or
push.

## Goals

1. Advertise `(ApiKey::ExpireDelegationToken, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 61`. Do **not** change
   hard `== 60` asserts (group.rs / v206 / v225 / v228 / v233).
2. Always flexible (treat v0 as flex). Official Apache Kafka first
   flexible version is **2**. Dispatch v0 only. v1+ → **35**.
3. Official Kafka ExpireDelegationTokenRequest (confirmed
   `ExpireDelegationTokenRequest.json`):
   ```
   hmac compact bytes
   expiryTimePeriodMs i64
   tagged
   ```
   Parse and discard.
4. Response matches official `ExpireDelegationTokenResponse.json`
   field order (no errorMessage):
   ```
   errorCode i16 = 42
   expiryTimestampMs i64 = -1
   throttleTimeMs i32 = 0
   tagged
   ```
5. Controller is **not** required (reject is local).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**.

## Non-goals

| Deferred | Why |
|----------|-----|
| RenewDelegationToken | Sibling leftover; do not advertise |
| Persist tokens / HMAC | No token store |
| Official flex v2+ | Advertised max is 0 |
| DelegationToken ACL resource type | Still unsupported |
| Create/Describe behavior | Already shipped |
| `group.rs` hard `== 60` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 is classic (`hmac` bytes + `expiryTimePeriodMs`).
Residual treats advertised v0 as flex (compact + tagged):

```
hmac compact bytes
expiryTimePeriodMs i64
tagged
```

Official response field order (`ExpireDelegationTokenResponse.json`;
no errorMessage; throttle last):

```
errorCode i16 = 42
expiryTimestampMs i64 = -1
throttleTimeMs i32 = 0
tagged
```

## Semantics

```
ExpireDelegationToken v0
  │
  ├─ Cluster ALTER fail → 31, expiryTimestampMs -1
  ├─ Controller not required
  │
  └─ else → 42 INVALID_REQUEST
            (delegation tokens not supported)
            expiryTimestampMs = -1
            throttle = 0
            nothing persisted
```

- Response throttle is always 0.
- Official Apache Kafka first flexible version is **2**. Volant
  advertises v0 only and encodes it as flexible.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v260_expire_delegation_token -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **40** min=max=0; keys **38** and **41** still listed; `SUPPORTED_APIS.len() >= 61` |
| v0 expire | **42**; `expiryTimestampMs` **-1**; no token files under data_dir |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 40 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v260_expire_delegation_token.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 40 v0 |
| `docs/V260_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a delegation-token broker. No store, no HMAC, no expire.
- Official Kafka `flexibleVersions` is **2+**; residual v0 is flexible.
- Official response has no errorMessage; throttle is last.
- RenewDelegationToken (**39**) is **not** advertised.
- DelegationToken remains an unsupported ACL resource type.
- `group.rs` / v206 / v225 / v228 / v233 `SUPPORTED_APIS.len()==60`
  assertions are intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V258_SPEC.md](./V258_SPEC.md) — CreateDelegationToken reject
- [V259_SPEC.md](./V259_SPEC.md) — DescribeDelegationToken empty
