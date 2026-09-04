# v0.261 — Kafka RenewDelegationToken key 39 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **RenewDelegationToken** (API key **39**,
version **0** only, always flexible). Volant has **no**
delegation-token store. Parse the request; return error **42**
`INVALID_REQUEST` (`delegation tokens not supported`). Nothing
persisted.

This is residual **v0.261**. Official Kafka first flexible version is
**2**; this residual advertises **v0** as flexible (same class as
CreateDelegationToken v0 / quotas v0-only). Do **not** add
ExpireDelegationToken (sibling). Do **not** change Create/Describe
behavior. Do **not** touch OffsetCommit epoch, BrokerRegistration,
ConsumerGroupDescribe, or `group.rs`.

## Goals

1. Advertise `(ApiKey::RenewDelegationToken, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 61`. Do **not** change
   hard `== 60` asserts.
2. Always flexible. Dispatch v0 only. v1+ → **35**.
3. Official Kafka RenewDelegationTokenRequest (confirmed
   `RenewDelegationTokenRequest.json`):
   `hmac` compact bytes, `renewPeriodMs` i64, tagged in flex. Parse
   and discard.
4. Response matches official `RenewDelegationTokenResponse.json` field
   order (error first; throttle last; no errorMessage): error **42**,
   `expiryTimestampMs` **-1**, throttle **0**, tagged.
5. Controller is **not** required (reject is local).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**.

## Non-goals

| Deferred | Why |
|----------|-----|
| ExpireDelegationToken | Sibling leftover |
| Persist tokens / HMAC / tokenId | No token store |
| Official flex v2+ | Advertised max is 0 |
| DelegationToken ACL resource type | Still unsupported |
| Create/Describe behavior | Already shipped (v0.258 / v0.259) |
| OffsetCommit epoch / BrokerRegistration / ConsumerGroupDescribe | Sibling leftovers |
| `group.rs` hard `== 60` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 is classic (`hmac bytes` + `renewPeriodMs`). Residual
treats advertised v0 as flex (compact + tagged):

```
hmac compact bytes
renewPeriodMs i64
tagged
```

Official response field order (`RenewDelegationTokenResponse.json`):

```
errorCode i16 = 42
expiryTimestampMs i64 = -1
throttleTimeMs i32 = 0
tagged
```

## Semantics

```
RenewDelegationToken v0
  │
  ├─ Cluster ALTER fail → 31, expiry = -1
  ├─ Controller not required
  │
  └─ else → 42 INVALID_REQUEST
            (delegation tokens not supported)
            expiryTimestampMs = -1
            nothing persisted
```

- Response throttle is always 0.
- Official Apache Kafka first flexible version is **2**. Volant
  advertises v0 only and encodes it as flexible.
- Official Kafka 4.0 removed v0 (`validVersions` 1–2). Residual still
  advertises **0**.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v261_renew_delegation_token -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **39** min=0 max=0; keys **38** and **41** still listed; `SUPPORTED_APIS.len() >= 61` |
| v0 renew | **42**; expiry **-1**; throttle **0**; no delegation-token files |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 39 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v261_renew_delegation_token.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 39 v0 |
| `docs/V261_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a delegation-token broker. No store, no HMAC, no tokenId.
- Official Kafka `flexibleVersions` is **2+**; residual v0 is flexible.
- ExpireDelegationToken is **not** advertised.
- DelegationToken remains an unsupported ACL resource type.
- `group.rs` `SUPPORTED_APIS.len()==60` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V258_SPEC.md](./V258_SPEC.md) — CreateDelegationToken reject
- [V259_SPEC.md](./V259_SPEC.md) — DescribeDelegationToken empty
