# v0.170 — Rust create_acl / delete_acl

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V56_SPEC.md](./V56_SPEC.md) /
[V162_SPEC.md](./V162_SPEC.md): language clients are adding singular
`create_acl` / `delete_acl` wrappers (v0.169). Rust only has batch
`Client::create_acls(entries)` / `Client::delete_acls(entries)`
(exact-match; delete returns removed count). There is no one-binding
helper.

Add `Client::create_acl` and `Client::delete_acl`. Reuse
`create_acls` / `delete_acls` (do not reimplement the RPC). Batch
APIs stay unchanged. `list_acls` / `list_acls_all` stay unchanged.
Exact-match delete only. This is **not** Kafka CreateAcls /
DeleteAcls.

This is residual **v0.170** (Rust create_acl / delete_acl). It is
**not** Phase 170 work. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, or
Python/Go/Java.

## Goals

1. Add public `Client::create_acl(entry)` that calls
   `create_acls(vec![entry])` (one `AclBinding` on the wire).
2. Add public `Client::delete_acl(entry)` that calls
   `delete_acls(vec![entry])` and returns the removed count.
3. Inherit retry / error **14** from `create_acls` / `delete_acls`
   (`admin_round_trip`: v0.104 transient retry + v0.79 error 14).
   No new retry policy.
4. Do **not** change `create_acls` / `delete_acls`.
5. Do **not** change `list_acls` / `list_acls_all`.
6. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `create_acls` / `delete_acls` | Frozen; batch stays public |
| Filter-delete | Exact-match delete only (same as Phase 20) |
| Kafka CreateAcls / DeleteAcls / DescribeAcls (API keys 30/31/29) | Native opcodes 54–59 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Go / Java | Sibling v0.169; do not wait or edit |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
pub async fn create_acl(&self, entry: volant_protocol::AclBinding) -> Result<()> {
    self.create_acls(vec![entry]).await
}

pub async fn delete_acl(&self, entry: volant_protocol::AclBinding) -> Result<u32> {
    self.delete_acls(vec![entry]).await
}
```

```rust
let entry = AclBinding {
    principal: "alice".into(),
    resource_type: 0, // Topic
    resource: "t".into(),
    operation: 0,
    permission: 1, // Allow
};
client.create_acl(entry.clone()).await?;
let n = client.delete_acl(entry).await?; // exact-match; removed count
let _ = client.create_acls(vec![a, b]).await?; // unchanged batch
```

`AclBinding` fields: `principal`, `resource_type` (0=Topic),
`resource`, `operation`, `permission` (0=Deny, 1=Allow). See
`crates/volant-protocol/src/request.rs`.

## Semantics

- `create_acl` sends CreateAcls (opcode 54) with **one** binding.
- `delete_acl` sends DeleteAcls (opcode 56) with **one** binding.
  Delete is exact-match only; the return is the removed count.
- `create_acls` / `delete_acls` are unchanged (batch still accepted).
- `list_acls` / `list_acls_all` are unchanged.
- Transient 6 / 7 / 15 / 16 and transport retry via `create_acls` /
  `delete_acls` / `admin_round_trip` (v0.104; default
  `max_retries=0`).
- Error 14 follows `max_redirects` (v0.79).
- Not Kafka CreateAcls / DeleteAcls / DescribeAcls.

## Tests

Fake TCP stub that records decoded CreateAcls / DeleteAcls entries.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `create_acl(entry)` | CreateAcls with **one** binding (the entry) |
| `delete_acl(entry)` | DeleteAcls with **one** binding |
| Existing batch | `create_acls` / `delete_acls` unchanged |

Existing `phase20_acls.rs`, `v79_admin_not_controller.rs`, and
`v162_list_acls_all.rs` must still pass (batch + list unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `create_acl` / `delete_acl` wrap batch APIs |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v170_create_delete_acl.rs` | fake TCP one-binding wire check |
| `docs/V170_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** CreateAcls / DeleteAcls / DescribeAcls.
- Delete is still exact-match only (no filter-delete).
- `create_acls` / `delete_acls` are unchanged.
- `list_acls` / `list_acls_all` are unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the singular ACL wrappers:

- **Keep the named wrappers only.** Do not change `create_acls` /
  `delete_acls`.
- Do not change the ACL send loop (v0.104 retry + v0.79 14).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `create_acl` / `delete_acl` next to `create_acls` / `delete_acls`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V56_SPEC.md](./V56_SPEC.md) — language Create/Delete/ListAcls
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin error 14
- [V88_SPEC.md](./V88_SPEC.md) — Rust ListAcls error 14
- [V104_SPEC.md](./V104_SPEC.md) — Rust admin_round_trip transient retry
- [V162_SPEC.md](./V162_SPEC.md) — Rust `list_acls_all`
- [PHASE20_SPEC.md](./PHASE20_SPEC.md) — native ACL opcodes 54–59
