# v0.241 — Kafka Describe/AlterClientQuotas keys 48/49 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **DescribeClientQuotas** (API key **48**) and
**AlterClientQuotas** (API key **49**), version **0** only (always
flexible). Volant has **no quotas**.

This is residual **v0.241**. Describe returns empty entries. Alter is
rejected per entry. Do **not** invent a quota store. Do **not** touch
ListOffsets, UnregisterBroker, UpdateFeatures, `__metadata_raft`, or
`group.rs`.

## Goals

1. Advertise `(ApiKey::DescribeClientQuotas, 0, 0)` and
   `(ApiKey::AlterClientQuotas, 0, 0)` in `SUPPORTED_APIS`. Soft length
   assert `>= 46`.
2. Dispatch keys 48 / 49 v0 (flexible request header + compact body).
3. Describe: parse the filter, return throttle=0, error=0, **empty
   entries** (no matching entities).
4. Alter: parse the request, per-entry **42** `INVALID_REQUEST` with
   message `quotas not supported`. Do **not** silently succeed. Do
   **not** persist.
5. ACL: Cluster **DESCRIBE** (48) / Cluster **ALTER** (49). Disabled
   ACLs allow.
6. v1+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Quota store / produce-fetch throttle | Volant has no client quotas |
| Silent Alter success | Would invent stored quotas |
| Keys 48/49 v1+ | Residual advertises v0 only |
| ListOffsets / UnregisterBroker / UpdateFeatures | Sibling leftovers |
| `__metadata_raft` / `group.rs` | Orthogonal |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

DescribeClientQuotas request:

```
components compact array of {
  entityType compact string
  matchType i8
  match compact nullable string
  tagged
}
strict bool
tagged
```

DescribeClientQuotas response:

```
throttle i32
error i16
errorMessage compact nullable string
entries[] {
  entity[] { entityType, entityName nullable, tagged }
  values[] { key, value f64, tagged }
  tagged
}
tagged
```

AlterClientQuotas request:

```
entries[] {
  entity[] { entityType, entityName nullable, tagged }
  ops[] { key, value f64, remove bool, tagged }
  tagged
}
validateOnly bool
tagged
```

AlterClientQuotas response:

```
throttle i32
entries[] {
  error i16
  errorMessage compact nullable string
  entity[] { entityType, entityName nullable, tagged }
  tagged
}
tagged
```

## Semantics

```
DescribeClientQuotas v0
  │
  ├─ Cluster DESCRIBE fail → top-level 31, empty entries
  └─ else → throttle 0, error 0, empty entries
            (any filter; no quota entities exist)

AlterClientQuotas v0
  │
  ├─ Cluster ALTER fail → 31 on each parsed entry
  └─ per parsed entry → 42 INVALID_REQUEST
                        message "quotas not supported"
                        (validateOnly ignored; nothing persisted)
```

- Response throttle is always 0.
- Official Apache Kafka advertises 0–1; Volant advertises **0** only.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v241_client_quotas -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | keys **48** and **49** min=max=0; `SUPPORTED_APIS.len() >= 46` |
| Describe any filter | error **0**, empty entries |
| Alter any entry | that entry error **42**, no persist |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 48/49 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch + flexible header |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode (empty / reject) |
| `crates/volant-broker/tests/v241_client_quotas.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | keys 48/49 v0 |
| `docs/V241_SPEC.md` | This spec |

## Honesty leftovers

- **No quota store.** Describe is always empty. Alter never applies.
- `validateOnly` does not make Alter succeed.
- Official Kafka keys 48/49 go to v1; Volant stays v0.
- `group.rs` `SUPPORTED_APIS.len()==45` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V233_SPEC.md](./V233_SPEC.md) — previous Kafka admin wrap
