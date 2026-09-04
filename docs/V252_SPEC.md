# v0.252 — Kafka ListClientMetricsResources key 74 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ListClientMetricsResources** (API key
**74**, version **0** only, always flexible). Volant has **no**
client-metrics resource store (same honesty as DescribeClientQuotas
**48** / v0.241).

This is residual **v0.252**. Parse the empty request; return
throttle=0, error=0, **empty** resources. Nothing persisted. Do **not**
invent a KIP-714 store. Do **not** touch WriteTxnMarkers,
AssignReplicasToDirs, GetTelemetrySubscriptions, TxnOffsetCommit, or
`group.rs`.

## Goals

1. Advertise `(ApiKey::ListClientMetricsResources, 0, 0)` in
   `SUPPORTED_APIS`. Soft length assert `>= 53`. Do **not** change
   hard `== 52` asserts.
2. Dispatch key 74 v0 (flexible request header + tagged body).
3. Request is a tagged buffer only (no body fields).
4. Response: throttle=0, error=0, **empty** `clientMetricsResources`.
   Official Kafka body has **no** `errorMessage`.
5. Controller is **not** required.
6. ACL: Cluster **DESCRIBE**. Disabled ACLs allow. Denied → error
   **31**, empty resources.
7. v1+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Client-metrics resource store / KIP-714 push | Volant has no store |
| Official Kafka later `ListConfigResources` v1 | Residual advertises v0 only |
| WriteTxnMarkers / AssignReplicasToDirs / GetTelemetrySubscriptions | Sibling leftovers |
| TxnOffsetCommit / quota store | Orthogonal |
| `group.rs` hard `== 52` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official `ListClientMetricsResourcesRequest.json` (`validVersions` 0,
`flexibleVersions` 0+, empty `fields`):

```
tagged
```

Official `ListClientMetricsResourcesResponse.json` (no errorMessage):

```
throttleTimeMs i32
errorCode i16
clientMetricsResources[] {
  name compact string
  tagged
}
tagged
```

## Semantics

```
ListClientMetricsResources v0
  │
  ├─ Cluster DESCRIBE fail → top-level 31, empty resources
  ├─ Controller not required
  │
  └─ else → throttle 0, error 0, empty resources
            (nothing persisted)
```

- Response throttle is always 0.
- Official Apache Kafka later renamed key 74 to ListConfigResources
  (v1); Volant advertises **ListClientMetricsResources v0** only.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v252_list_client_metrics -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **74** min=max=0; `SUPPORTED_APIS.len() >= 53` |
| Call v0 | error **0**, empty resources; no new files under data_dir |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 74 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode (empty list) |
| `crates/volant-broker/tests/v252_list_client_metrics.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 74 v0 |
| `docs/V252_SPEC.md` | This spec |

## Honesty leftovers

- **No client-metrics resource store.** The list is always empty.
- Official Kafka key 74 later became ListConfigResources v0–1; Volant
  stays ListClientMetricsResources v0.
- Official response has no errorMessage field.
- `group.rs` `SUPPORTED_APIS.len()==52` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V241_SPEC.md](./V241_SPEC.md) — DescribeClientQuotas empty-store
  pattern
