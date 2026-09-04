# v0.244 — Kafka UpdateFeatures key 57 v0–1

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **UpdateFeatures** (API key **57**, versions
**0–1**, always flexible). Honest reject: parse the request, return
per-feature **92** `FEATURE_UPDATE_FAILED` (`empty / not supported`).
Do **not** persist features. Do **not** claim KIP-584. ApiVersions
SupportedFeatures / FinalizedFeatures stay empty.

This is residual **v0.244**. UpdateFeatures is the write path;
clients describe via ApiVersions (already empty). Do **not** touch
ListOffsets, quotas, UnregisterBroker, `__metadata_raft`, or
`group.rs`.

## Goals

1. Advertise `(ApiKey::UpdateFeatures, 0, 1)` in `SUPPORTED_APIS`.
   Soft length assert `>= 46`.
2. Dispatch key 57 v0–1 (always flexible request header + compact
   body). v0 `allowDowngrade` bool; v1 `upgradeType` + `validateOnly`.
3. Every feature update → that result **92** `FEATURE_UPDATE_FAILED`
   (else **42** if 92 were absent). Nothing persisted.
4. `--cluster-config` and `!is_controller()` → top-level **41**.
   Single-node: allow and still reject each feature.
5. ACL: Cluster **ALTER**. Disabled ACLs allow.
6. v2+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Persist finalized features | Honest empty / not supported |
| KIP-584 feature versioning | ApiVersions features stay empty |
| Separate DescribeFeatures API | Describe is ApiVersions tags |
| v2 (no per-feature results) | Advertised max is 1 |
| ListOffsets / quotas / UnregisterBroker | Sibling leftovers |
| `__metadata_raft` / `group.rs` | Sibling leftovers |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0–1 always flexible)

Request:

```
timeoutMs i32
featureUpdates[] {
  feature compact string
  maxVersionLevel i16
  allowDowngrade bool          // v0
  upgradeType i8               // v1
  tagged
}
validateOnly bool              // v1
tagged
```

Response:

```
throttle i32
error i16
errorMessage compact nullable string
results[] {
  feature compact string
  error i16
  errorMessage compact nullable string
  tagged
}
tagged
```

## Semantics

```
UpdateFeatures v0–1
  │
  ├─ not controller (cluster) → top-level 41, empty results
  ├─ Cluster ALTER fail → top-level 31, empty results
  ├─ TimeoutMs / validateOnly parsed, ignored
  │
  └─ per feature
          └─ 92 FEATURE_UPDATE_FAILED ("empty / not supported")
             nothing written; ApiVersions features stay empty
```

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v244_update_features -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **57** min=0 max=1; `SUPPORTED_APIS.len() >= 46` |
| Update any feature | that feature **92** (or **42**); nothing persisted |
| not controller | **41** |
| v2 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 57 + error 92 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0–1 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode + reject; lib persist check |
| `crates/volant-broker/tests/v244_update_features.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 57 v0–1 |
| `docs/V244_SPEC.md` | This spec |

## Honesty leftovers

- **Not** KIP-584. Features are not stored, upgraded, or advertised.
- ApiVersions SupportedFeatures / FinalizedFeatures / ZkMigrationReady
  tags stay **empty**.
- Per-feature error is **92** with message `empty / not supported`.
- **TimeoutMs** and v1 **validateOnly** are parsed and ignored.
- Kafka v2 (no per-feature results) is refused with **35**.
- `group.rs` `SUPPORTED_APIS.len()==45` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V237_SPEC.md](./V237_SPEC.md) — previous Kafka admin wrap
