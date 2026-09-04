# v0.172 — Rust add_broker_no_rack

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V58_SPEC.md](./V58_SPEC.md):
language clients gained a no-rack default/overload (`rack=None` /
`addBroker(id, host, port)`). Rust already treats
`add_broker(id, host, port, None)` as no rack (wire flag **0**). There
is no named no-rack helper. Go `AddBrokerNoRack` is sibling **v0.171**.

Add `Client::add_broker_no_rack`. Reuse `add_broker` (do not
reimplement the RPC). `add_broker` stays unchanged. This is **not**
Kafka broker catalog.

This is residual **v0.172** (Rust add_broker_no_rack). It is **not**
Phase 172 work. It does **not** open Phase 155, add Kafka API keys,
add native opcodes, or change the broker, protocol, or Python/Go/Java.

## Goals

1. Add public `Client::add_broker_no_rack(id, host, port)` that calls
   `add_broker(id, host, port, None)` (wire rack flag **0**).
2. Return `u64` generation (same as `add_broker`).
3. Inherit retry / error **14** from `add_broker`
   (`admin_round_trip`: v0.104 transient retry + v0.91 error 14).
   No new retry policy.
4. Do **not** change `add_broker`.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `add_broker` | Frozen; `None` already means no rack |
| Kafka broker catalog / AlterPartitionReassignments | Native opcode 102/103 only |
| Overlay / membership SoT | Overlay remains SoT (v0.10) |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Java | Already have no-rack default/overload |
| Go `AddBrokerNoRack` | Sibling **v0.171** |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// Add a broker endpoint with no rack (v0.172).
///
/// Same as `add_broker(id, host, port, None)`. Wire rack flag is 0.
pub async fn add_broker_no_rack(
    &self,
    id: u32,
    host: &str,
    port: u16,
) -> Result<u64> {
    self.add_broker(id, host, port, None).await
}
```

```rust
let _ = client.add_broker_no_rack(2, "10.0.0.2", 9092).await?; // no rack
let _ = client.add_broker(2, "10.0.0.2", 9092, None).await?;   // unchanged
let _ = client.add_broker(2, "10.0.0.2", 9092, Some("r1")).await?;
```

## Semantics

- `rack = None` / wire flag **0** means no rack (same as today).
- `add_broker_no_rack` is a named wrapper; it does not re-encode.
- `add_broker(id, host, port, rack)` is unchanged (`None` still means
  no rack; `Some` still writes flag **1** + rack string).
- Transient 6 / 7 / 15 / 16 and transport retry via `add_broker` /
  `admin_round_trip` (v0.104; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.91).
- Overlay is still SoT. Not Kafka broker catalog.

## Tests

Fake TCP stub that records decoded AddBroker id / host / port / rack.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `add_broker_no_rack(2, "10.0.0.2", 9092)` | AddBroker id=2 host=`10.0.0.2` port=9092 rack `None` (flag **0**) |

Existing `v91_add_remove_broker_14.rs` must still pass
(`add_broker` unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `add_broker_no_rack` wraps `add_broker` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v172_add_broker_no_rack.rs` | fake TCP no-rack wire check |
| `docs/V172_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** broker catalog / AlterPartitionReassignments.
- `None` / flag **0** still means no rack.
- `add_broker` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the AddBroker named helper:

- **Keep the named wrapper only.** Do not change `add_broker`.
- Do not change the AddBroker send loop (v0.104 retry + v0.91 14).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `add_broker_no_rack` after `add_broker`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V58_SPEC.md](./V58_SPEC.md) — language AddBroker / RemoveBroker / ListMembers
- [V10_SPEC.md](./V10_SPEC.md) — native AddBroker 102/103
- [V91_SPEC.md](./V91_SPEC.md) — Rust AddBroker error 14
- [V104_SPEC.md](./V104_SPEC.md) — Rust admin_round_trip transient retry
- [V89_SPEC.md](./V89_SPEC.md) — language AddBroker 14
