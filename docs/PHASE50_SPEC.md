# Phase 50 — Kafka ApiVersions classic v0–2

## Goals

1. Raise **ApiVersions** (API key 18) from classic **v0** to classic **v0–2**
   (last non-flexible; flexible **v3+** out of scope)
2. Emit trailing `throttle_time_ms` on v1–2
3. Advertise max version **2** in ApiVersions (self-describing); tests + docs

## Non-goals

- Flexible ApiVersions v3+ (compact arrays, client software name/version, tagged features)
- SupportedFeatures / FinalizedFeatures (v3+ tagged only)
- Real quota throttling (always 0)

## Wire summary

### Request

Classic v0–2: **empty body** (v3+ adds ClientSoftwareName/Version under flexible encoding).

### Response

```
error_code: INT16
api_keys: [{ api_key: INT16, min_version: INT16, max_version: INT16 }]
throttle_time_ms: INT32   # v1+ trailing; always 0
```

| Ver | Notes |
|-----|--------|
| v0 | error + api_keys only |
| v1 | + trailing throttle |
| v2 | wire-identical to v1 (quota-timing semantics only on real Kafka) |

## Exit criteria

1. ApiVersions advertises itself as max **2**
2. ApiVersions v0 still parses (no trailing throttle) — existing tests green
3. ApiVersions v1/v2 include trailing throttle_time_ms = 0 after api_keys
4. ApiVersions v3 → UnsupportedVersion
5. phase50 tests green

## Honest limitations

- No flexible ApiVersions; no KIP-511 client software fields
- No feature flags / finalized features
- throttle always 0
