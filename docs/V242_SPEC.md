# v0.242 — Kafka UnregisterBroker key 64 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **UnregisterBroker** (API key **64**, version
**0** only, always flexible). Wrap native `Broker::remove_broker` (same
invert as AddBroker / v0.217).

This is residual **v0.242**. It is **not** Kafka KRaft UnregisterBroker
(no broker incarnation, no DirectoryId). Do **not** touch quotas,
ListOffsets, UpdateFeatures, `__metadata_raft`, or `group.rs`.

## Goals

1. Advertise `(ApiKey::UnregisterBroker, 0, 0)` in `SUPPORTED_APIS`.
   Soft length assert `>= 46`.
2. Dispatch key 64 v0 (flexible request header + compact body).
3. Controller only → else **41** `NOT_CONTROLLER`.
4. Single-node / no cluster → **42** `INVALID_REQUEST` “unregister
   requires cluster”.
5. Self / last broker → same errors as native `remove_broker` mapped
   to Kafka codes.
6. TimeoutMs parsed when present before tags, ignored.
7. ACL: Cluster **ALTER**. Disabled ACLs allow.
8. v1+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft incarnation / DirectoryId | Honest wrap of overlay remove |
| Versions 1+ | Kafka key 64 is v0 only |
| Quotas / ListOffsets / UpdateFeatures | Sibling leftovers |
| `__metadata_raft` / `group.rs` | Orthogonal |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Request:

```
BrokerId i32
TimeoutMs i32   // optional; some versions; ignored
tagged
```

Response:

```
throttle i32          // always 0
errorCode i16
errorMessage compact nullable string
tagged
```

## Semantics

```
UnregisterBroker v0
  │
  ├─ Cluster ALTER fail → 31
  ├─ no cluster → 42 “unregister requires cluster”
  ├─ not controller → 41
  ├─ TimeoutMs ignored
  │
  └─ native remove_broker (v0.217 invert)
        ├─ ok → 0; overlay loses that id
        ├─ last / self / unknown id → 42 (native InvalidArgument)
        └─ joint fail → 19
```

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v242_unregister_broker -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **64** min=0 max=0; `SUPPORTED_APIS.len() >= 46` |
| Unregister extra broker on controller | overlay loses that id (same as native RemoveBroker) |
| not controller | **41** |
| v1 | **35** UnsupportedVersion |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 64 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch + flexible header |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode + lib tests |
| `crates/volant-broker/tests/v242_unregister_broker.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 64 v0 |
| `docs/V242_SPEC.md` | This spec |

## Honesty leftovers

- **Not** Kafka KRaft UnregisterBroker. No incarnation epoch, no
  DirectoryId, no controller quorum voter unregistration.
- **TimeoutMs** is parsed when present before tags and ignored
  (apply is overlay remove, same as native 104/105).
- Native opcode ACL remains Cluster ALTER; this Kafka key uses the
  same Cluster ALTER.
- `group.rs` `SUPPORTED_APIS.len()==45` assertion is intentionally
  untouched.

## Related

- [V10_SPEC.md](./V10_SPEC.md) — dynamic membership overlay
- [V217_SPEC.md](./V217_SPEC.md) — in-process add/remove invert
- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
