# v0.161 — Go ListAclsAll

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V56_SPEC.md](./V56_SPEC.md): Java
already has `listAcls()` (empty filters: principal `""`, type 255,
resource `""`). Python `list_acls()` defaults to the same. Go only
has `ListAcls(principal, resourceType, resource)`.

Add `Client.ListAclsAll`. Reuse `ListAcls` (do not reimplement the
RPC). `ListAcls(principal, resourceType, resource)` stays unchanged.
This is **not** Kafka DescribeAcls.

This is residual **v0.161** (Go ListAclsAll). It is **not**
Phase 155. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, or change the broker, protocol, Rust, Python, or Java.

## Goals

1. Add public `func (c *Client) ListAclsAll() ([]codec.AclBinding, error)`
   that calls `ListAcls("", 255, "")`.
2. Inherit retry / error **14** from `ListAcls` (v0.85 error 14 via
   `adminRoundTrip`). No new retry policy.
3. Do **not** change `ListAcls(principal, resourceType, resource)`.
4. Do **not** change broker / protocol / Rust / Python / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `ListAcls(principal, resourceType, resource)` | Frozen; empty filters already mean all |
| Kafka DescribeAcls (API key 29) | Native opcode 58 only |
| Filter-delete / new ACL opcodes | Exact-match delete only (v0.56) |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Python / Java | Already have no-arg / default-filter overloads (v0.56) |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// ListAclsAll lists every ACL binding (empty filters).
// Same as ListAcls("", 255, "").
func (c *Client) ListAclsAll() ([]codec.AclBinding, error) {
    return c.ListAcls("", 255, "")
}
```

```go
listed, _ := c.ListAclsAll()                 // every binding
listed, _ = c.ListAcls("", 255, "")          // unchanged: same wire
listed, _ = c.ListAcls("User:alice", 0, "events")
```

## Semantics

- Empty principal/resource = any. `resourceType` 255 = any type
  (same as today).
- `ListAclsAll` is a named wrapper; it does not re-encode.
- `ListAcls(principal, resourceType, resource)` is unchanged (empty
  filters still mean all).
- Transient 6 / 7 / 15 / 16 and transport retry via `ListAcls` /
  `adminRoundTrip` (default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.85).
- Not Kafka DescribeAcls / CreateAcls / DeleteAcls.

## Tests

Fake TCP stub that records the decoded ListAcls request (same helper
as existing `acls_test.go`).

```bash
(cd clients/go && go test ./...)
```

| Case | Expect |
|------|--------|
| `ListAclsAll()` | empty principal, resourceType 255, empty resource; same as `ListAcls("", 255, "")` |
| Existing `ListAcls` empty / explicit / error cases | still pass |

Existing ListAcls retry / 14 tests must still pass
(`ListAcls` unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `ListAclsAll` wraps `ListAcls("", 255, "")` |
| `clients/go/acls_test.go` | empty-filter wire check |
| `clients/go/README.md` | usage line + one prose sentence |
| `docs/V161_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** DescribeAcls / CreateAcls / DeleteAcls.
- Empty filters still list **every** ACL binding.
- `ListAcls(principal, resourceType, resource)` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust / Python / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.go` should keep this hunk local
to the ListAclsAll wrapper:

- **Keep the wrapper only.** Do not change `ListAcls`.
- Do not change the ListAcls send loop (`adminRoundTrip` + v0.85 14).
- Do not change Python, Java, broker, or protocol.

Expect conflicts on:

- `clients/go/client.go` — hunk is local to `ListAclsAll` after
  `ListAcls`
- `clients/go/acls_test.go`
- `clients/go/README.md`

## Related

- [V56_SPEC.md](./V56_SPEC.md) — language Create/Delete/ListAcls
- [V85_SPEC.md](./V85_SPEC.md) — language SCRAM-admin / ListAcls 14
- [V88_SPEC.md](./V88_SPEC.md) — Rust SCRAM-admin / ListAcls 14
- [PHASE20_SPEC.md](./PHASE20_SPEC.md) — native 54–59
