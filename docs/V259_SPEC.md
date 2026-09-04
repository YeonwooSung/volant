# v0.259 — Kafka DescribeDelegationToken key 41 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **DescribeDelegationToken** (API key **41**,
version **0** only). Volant has **no** delegation-token store (same
honesty as DescribeClientQuotas **48** / v0.241). Parse the owners
filter; return throttle=0, error **0**, **empty** tokens. Nothing
persisted.

This is residual **v0.259**. Do **not** implement CreateDelegationToken,
ExpireDelegationToken, or RenewDelegationToken. Do **not** touch
PushTelemetry, OffsetFetch, AlterPartition, or `group.rs`.

## Goals

1. Advertise `(ApiKey::DescribeDelegationToken, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 57`. Do **not** change
   hard `== 56` asserts.
2. Always flexible (treat v0 as flex). Official Apache Kafka first
   flexible version is **2** — residual. Dispatch v0 only. v1+ → **35**.
3. Official `DescribeDelegationTokenRequest.json` (`nullableVersions`
   0+):
   ```
   owners[] { principalType, principalName }   // null = all; compact in flex
   tagged
   ```
   Parse owners (null or list) and ignore — always empty result.
4. Official `DescribeDelegationTokenResponse.json` field order (no
   errorMessage):
   ```
   errorCode i16 = 0
   tokens[]   // empty compact
   throttleTimeMs i32 = 0
   tagged     // residual flex trailer
   ```
5. Controller is **not** required.
6. ACL: Cluster **DESCRIBE**. Disabled ACLs allow. Denied → **31**,
   empty tokens.

## Non-goals

| Deferred | Why |
|----------|-----|
| CreateDelegationToken / Expire / Renew | Sibling leftovers; do not advertise |
| Delegation-token store / HMAC | Volant has no token store |
| Official Kafka v1–3 / flex-from-v2 | Residual advertises v0 only, treated as flex |
| PushTelemetry / OffsetFetch / AlterPartition | Sibling leftovers |
| `group.rs` hard `== 56` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible residual)

Official first flexible version is **2**. Volant treats advertised v0
as compact + tagged.

Request (`DescribeDelegationTokenRequest.json`):

```
owners compact nullable array of {
  principalType compact string
  principalName compact string
  tagged
}
tagged
```

Response (`DescribeDelegationTokenResponse.json`; throttle is last;
no errorMessage):

```
errorCode i16
tokens[] {
  principalType compact string
  principalName compact string
  issueTimestamp i64
  expiryTimestamp i64
  maxTimestamp i64
  tokenId compact string
  hmac compact bytes
  renewers[] { principalType, principalName, tagged }
  tagged
}
throttleTimeMs i32
tagged
```

Volant always writes empty `tokens`. Token struct fields are never
emitted.

## Semantics

```
DescribeDelegationToken v0
  │
  ├─ Cluster DESCRIBE fail → top-level 31, empty tokens
  ├─ Controller not required
  │
  └─ else → error 0, empty tokens, throttle 0
            (any owners filter; nothing persisted)
```

- Response throttle is always 0.
- Official Apache Kafka advertises 1–3 today (v0 removed in 4.0;
  flexible from 2). Volant advertises **0** only and treats it as flex.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v259_describe_delegation_token -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **41** min=max=0; `SUPPORTED_APIS.len() >= 57` |
| v0 empty/null owners | error **0**, empty tokens; no new files under data_dir |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 41 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode (empty tokens) |
| `crates/volant-broker/tests/v259_describe_delegation_token.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 41 v0 |
| `docs/V259_SPEC.md` | This spec |

## Honesty leftovers

- **No delegation-token store.** The list is always empty.
- Official first flex is **2**; Volant v0 is flexible residual.
- Official response has no errorMessage; throttle is last.
- CreateDelegationToken (**38**), RenewDelegationToken (**39**),
  ExpireDelegationToken (**40**) are not advertised.
- `group.rs` `SUPPORTED_APIS.len()==56` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V241_SPEC.md](./V241_SPEC.md) — DescribeClientQuotas empty-store
  pattern
- [V252_SPEC.md](./V252_SPEC.md) — ListClientMetricsResources empty
