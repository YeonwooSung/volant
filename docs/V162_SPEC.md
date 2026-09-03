# v0.162 — Rust ListAcls unfiltered helper

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V56_SPEC.md](./V56_SPEC.md):
Java `listAcls()` and Python `list_acls()` default to empty filters
(principal `""`, resource_type `255`, resource `""`). Rust only has
`list_acls(principal, resource_type, resource)`.

Add `Client::list_acls_all`. Reuse `list_acls` (do not reimplement
the RPC). `list_acls` stays unchanged. This is **not** Kafka
DescribeAcls.

This is residual **v0.162** (Rust ListAcls unfiltered named helper).
It is **not** Phase 162 work. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, or
Python/Go/Java.

## Goals

1. Add public `Client::list_acls_all()` that calls
   `list_acls("", 255, "")` (empty filters = any principal / type /
   resource).
2. Return `Vec<volant_protocol::AclBinding>`.
3. Inherit retry / error **14** from `list_acls` (`admin_round_trip`:
   v0.104 transient retry + v0.88 error 14). No new retry policy.
4. Do **not** change `list_acls`.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `list_acls` | Frozen; empty filters already mean any |
| Kafka DescribeAcls (API key 29) | Native opcode 58/59 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Python / Java | Already default to empty filters (v0.56) |
| Go `ListAcls` named helper | Out of scope; Go still takes explicit filters |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// List every ACL binding (empty filters: any principal / type / resource).
/// Same as `list_acls("", 255, "")`.
pub async fn list_acls_all(&self) -> Result<Vec<volant_protocol::AclBinding>> {
    self.list_acls("", 255, "").await
}
```

```rust
let all = client.list_acls_all().await?;            // any/any/any
let same = client.list_acls("", 255, "").await?;    // unchanged: same rows
```

## Semantics

- Empty principal / resource = any. `resource_type = 255` = any type
  (same as `list_acls("", 255, "")`).
- Returned rows are public `AclBinding` (principal, resource_type,
  resource, operation, permission).
- `list_acls` is unchanged (empty filters still mean any).
- Transient 6 / 7 / 15 / 16 and transport retry via `list_acls` /
  `admin_round_trip` (v0.104; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.88).
- Not Kafka DescribeAcls / CreateAcls / DeleteAcls.

## Tests

Fake TCP stub that records decoded ListAcls filters.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `list_acls_all()` | wire principal `""`, resource_type `255`, resource `""` |

Existing `phase20_acls.rs` and `v88_scram_listacls_14.rs` must still
pass (`list_acls` unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `list_acls_all` wraps `list_acls` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v162_list_acls_all.rs` | fake TCP empty-filter wire |
| `docs/V162_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** DescribeAcls / CreateAcls / DeleteAcls.
- Unfiltered list is still empty principal / type 255 / empty resource
  (same as today).
- `list_acls` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the ListAcls unfiltered helper:

- **Keep the named wrapper only.** Do not change `list_acls`.
- Do not change the ListAcls send loop (v0.104 retry + v0.88 14).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `list_acls_all` after `list_acls`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V56_SPEC.md](./V56_SPEC.md) — language Create/Delete/ListAcls
- [V88_SPEC.md](./V88_SPEC.md) — Rust ListAcls error 14
- [V104_SPEC.md](./V104_SPEC.md) — Rust admin_round_trip transient retry
- [V85_SPEC.md](./V85_SPEC.md) — language ACL / SCRAM-admin 14
- [PHASE20_SPEC.md](./PHASE20_SPEC.md) — native ACL opcodes 54–59
