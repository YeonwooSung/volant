# v0.168 — Rust reassign_partitions_all

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V59_SPEC.md](./V59_SPEC.md):
language clients gained a no-partition overload/default
(`partition=None` / `null` → all partitions). Rust already treats
`reassign_partitions(topic, None, replicas)` as all partitions
(`REASSIGN_ALL_PARTITIONS` = `u32::MAX`). There is no named
all-partition helper. Go `ReassignAllPartitions` is sibling **v0.167**.

Add `Client::reassign_partitions_all`. Reuse `reassign_partitions`
(do not reimplement the RPC). `reassign_partitions` stays unchanged.
This is **not** Kafka AlterPartitionReassignments.

This is residual **v0.168** (Rust reassign_partitions_all). It is
**not** Phase 168 work. It does **not** open Phase 155, add Kafka API
keys, add native opcodes, or change the broker, protocol, or
Python/Go/Java.

## Goals

1. Add public `Client::reassign_partitions_all(topic, replicas)` that
   calls `reassign_partitions(topic, None, replicas)` (wire partition
   `REASSIGN_ALL_PARTITIONS` = `u32::MAX`).
2. Return `u32` generation (same as `reassign_partitions`).
3. Inherit retry / error **14** from `reassign_partitions`
   (`admin_round_trip`: v0.104 transient retry + v0.79 error 14).
   No new retry policy.
4. Do **not** change `reassign_partitions`.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `reassign_partitions` | Frozen; `None` already means all |
| Kafka AlterPartitionReassignments (API key 45) | Native opcode 114/115 only |
| Overlay / assignment wait-rollback | Broker-side (v0.18 / v0.39) |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Python / Java | Already have no-partition default |
| Go `ReassignAllPartitions` | Sibling **v0.167** |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// Reassign replicas for every partition of `topic`.
///
/// Same as `reassign_partitions(topic, None, replicas)`. Empty
/// `replicas` still asks the controller to auto-place.
pub async fn reassign_partitions_all(
    &self,
    topic: &str,
    replicas: &[u32],
) -> Result<u32> {
    self.reassign_partitions(topic, None, replicas).await
}
```

```rust
let _ = client.reassign_partitions_all("t", &[1, 2]).await?; // all parts
let _ = client.reassign_partitions("t", None, &[1, 2]).await?; // unchanged
let _ = client.reassign_partitions_all("t", &[]).await?;      // auto-place
```

## Semantics

- `partition = None` / `REASSIGN_ALL_PARTITIONS` (`u32::MAX`) applies
  to every partition of the topic (same as today).
- `reassign_partitions_all` is a named wrapper; it does not re-encode.
- Empty `replicas` still means auto-place with current membership
  (same as CreateTopic).
- `reassign_partitions(topic, partition, replicas)` is unchanged
  (`None` still means all).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `reassign_partitions` / `admin_round_trip` (v0.104; default
  `max_retries=0`).
- Error 14 follows `max_redirects` (v0.79).
- Not Kafka AlterPartitionReassignments.

## Tests

Fake TCP stub that records decoded ReassignPartitions topic /
partition / replicas.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `reassign_partitions_all("t", &[1, 2])` | partition `REASSIGN_ALL_PARTITIONS` (`u32::MAX`), replicas `[1, 2]` |
| `reassign_partitions_all("t", &[])` | partition `REASSIGN_ALL_PARTITIONS`, replica count 0 (auto-place) |

Existing admin-14 / admin-retry tests must still pass
(`reassign_partitions` unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `reassign_partitions_all` wraps `reassign_partitions` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v168_reassign_partitions_all.rs` | fake TCP all-partition wire check |
| `docs/V168_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** AlterPartitionReassignments.
- `None` / `u32::MAX` still reassigns **all** partitions of the topic.
- Empty `replicas` still auto-places.
- `reassign_partitions` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the ReassignPartitions named helper:

- **Keep the named wrapper only.** Do not change `reassign_partitions`.
- Do not change the ReassignPartitions send loop (v0.104 retry +
  v0.79 14).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `reassign_partitions_all` after `reassign_partitions`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V59_SPEC.md](./V59_SPEC.md) — language ReassignPartitions
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin error 14
- [V104_SPEC.md](./V104_SPEC.md) — Rust admin_round_trip transient retry
- [V72_SPEC.md](./V72_SPEC.md) — language admin 14
- [V18_SPEC.md](./V18_SPEC.md) — native ReassignPartitions 114/115
