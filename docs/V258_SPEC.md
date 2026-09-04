# v0.258 — Kafka CreateDelegationToken key 38 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **CreateDelegationToken** (API key **38**,
version **0** only, always flexible). Volant has **no**
delegation-token store. Parse the request; return error **42**
`INVALID_REQUEST` (`delegation tokens not supported`). Nothing
persisted.

This is residual **v0.258**. Official Kafka first flexible version is
**2**; this residual advertises **v0** as flexible (same class as
DescribeLogDirs v1 flex / quotas v0-only). Do **not** add
DescribeDelegationToken / ExpireDelegationToken / RenewDelegationToken
(siblings). Do **not** touch PushTelemetry, OffsetFetch,
AlterPartition, or `group.rs`.

## Goals

1. Advertise `(ApiKey::CreateDelegationToken, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 57`. Do **not** change
   hard `== 56` asserts.
2. Always flexible. Dispatch v0 only. v1+ → **35**.
3. Official Kafka CreateDelegationTokenRequest v0 (confirmed
   `CreateDelegationTokenRequest.json`):
   `renewers[] { principalType, principalName }`, `maxLifeTimeMs` i64,
   tagged in flex. Owner/requester fields are v3+ and out of advertised
   range. Parse and discard.
4. Response matches official `CreateDelegationTokenResponse.json` field
   order (error first; throttle last): error **42**, empty owner
   principal, empty `tokenId`, empty `hmac`, issue/expiry/max
   timestamps **-1**, throttle **0**.
5. Controller is **not** required (reject is local).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → **31**.

## Non-goals

| Deferred | Why |
|----------|-----|
| Describe / Expire / Renew DelegationToken | Sibling leftovers |
| Persist tokens / HMAC / tokenId | No token store |
| Official flex v2+ / owner principal v3 | Advertised max is 0 |
| DelegationToken ACL resource type | Still unsupported |
| PushTelemetry / OffsetFetch / AlterPartition | Sibling leftovers |
| `group.rs` hard `== 56` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 is classic (`[renewers] maxLifetimeMs`). Residual
treats advertised v0 as flex (compact + tagged):

```
renewers[] {
  principalType compact string
  principalName compact string
  tagged
}
maxLifeTimeMs i64
tagged
```

Official response field order (`CreateDelegationTokenResponse.json`;
v0–2; requester fields are v3+):

```
errorCode i16 = 42
principalType compact string = ""
principalName compact string = ""
issueTimestampMs i64 = -1
expiryTimestampMs i64 = -1
maxTimestampMs i64 = -1
tokenId compact string = ""
hmac compact bytes = empty
throttleTimeMs i32 = 0
tagged
```

## Semantics

```
CreateDelegationToken v0
  │
  ├─ Cluster ALTER fail → 31, empty/zero token fields
  ├─ Controller not required
  │
  └─ else → 42 INVALID_REQUEST
            (delegation tokens not supported)
            empty tokenId / hmac
            issue/expiry/max = -1
            nothing persisted
```

- Response throttle is always 0.
- Official Apache Kafka first flexible version is **2**. Volant
  advertises v0 only and encodes it as flexible.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v258_create_delegation_token -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **38** min=0 max=0; `SUPPORTED_APIS.len() >= 57` |
| v0 create | **42**; empty token fields; timestamps **-1**; no delegation-token files |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 38 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v258_create_delegation_token.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 38 v0 |
| `docs/V258_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a delegation-token broker. No store, no HMAC, no tokenId.
- Official Kafka `flexibleVersions` is **2+**; residual v0 is flexible.
- DescribeDelegationToken / ExpireDelegationToken /
  RenewDelegationToken are **not** advertised.
- DelegationToken remains an unsupported ACL resource type.
- `group.rs` `SUPPORTED_APIS.len()==56` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V253_SPEC.md](./V253_SPEC.md) — previous Kafka admin residual
