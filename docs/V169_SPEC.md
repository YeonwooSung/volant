# v0.169 — language single-entry CreateAcl / DeleteAcl

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V56_SPEC.md](./V56_SPEC.md) /
[V161_SPEC.md](./V161_SPEC.md): `CreateAcls` / `create_acls` /
`createAcls` and `DeleteAcls` / `delete_acls` / `deleteAcls` already
take a list of bindings. `ListAclsAll` is the named all-filter helper.
There is no one-binding helper matching `DeleteOffset` / `delete_offset`.
Rust is unchanged (already has batch-only ACLs).

Add `CreateAcl` / `createAcl` / `create_acl` and `DeleteAcl` /
`deleteAcl` / `delete_acl`. Reuse the existing batch method (do not
reimplement the RPC). `CreateAcls` / `DeleteAcls` / `ListAcls` /
`ListAclsAll` stay unchanged. This is **not** Kafka CreateAcls /
DeleteAcls. Exact-match delete only (no filter-delete).

This is residual **v0.169** (language single-entry CreateAcl /
DeleteAcl). It is **not** Phase 155. It does **not** open Phase 155,
add Kafka API keys, add native opcodes, or change the broker,
protocol, or Rust.

## Goals

1. **Go:** public `func (c *Client) CreateAcl(entry codec.AclBinding) error`
   that calls `CreateAcls([]codec.AclBinding{entry})`, and
   `func (c *Client) DeleteAcl(entry codec.AclBinding) (uint32, error)`
   that calls `DeleteAcls([]codec.AclBinding{entry})`.
2. **Java:** named `void createAcl(AclBinding entry)` that calls
   `createAcls(Collections.singletonList(entry))`, and
   `int deleteAcl(AclBinding entry)` that calls
   `deleteAcls(Collections.singletonList(entry))`. Do not add an
   overload that could collide with `createAcls` / `deleteAcls`.
3. **Python:** public `def create_acl(self, entry: codec.AclBinding) -> None`
   that calls `self.create_acls([entry])`, and
   `def delete_acl(self, entry: codec.AclBinding) -> int` that calls
   `self.delete_acls([entry])`.
4. Inherit retry / error **14** from the existing batch method
   (`adminRoundTrip` / `_admin_round_trip`; v0.72 error 14). No new
   retry policy.
5. Do **not** change `CreateAcls` / `create_acls` / `createAcls` /
   `DeleteAcls` / `delete_acls` / `deleteAcls` / `ListAcls` /
   `ListAclsAll` / `list_acls`.
6. Do **not** change broker / protocol / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `CreateAcls` / `create_acls` / `createAcls` | Frozen; list still accepts one or many |
| Change `DeleteAcls` / `delete_acls` / `deleteAcls` | Frozen; exact-match list delete |
| Change `ListAcls` / `ListAclsAll` / `list_acls` | Frozen; empty filters already mean all |
| Filter-delete | Exact-match delete only (v0.56) |
| Kafka CreateAcls / DeleteAcls / DescribeAcls (API keys 30/31/29) | Native opcodes 54–59 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Rust `create_acl` / `delete_acl` | Unchanged this slice |
| Phase 155 / homemade Raft | Frozen |

## API

```go
func (c *Client) CreateAcl(entry codec.AclBinding) error {
    return c.CreateAcls([]codec.AclBinding{entry})
}
func (c *Client) DeleteAcl(entry codec.AclBinding) (uint32, error) {
    return c.DeleteAcls([]codec.AclBinding{entry})
}
```

```java
public void createAcl(AclBinding entry) {
    createAcls(Collections.singletonList(entry));
}
public int deleteAcl(AclBinding entry) {
    return deleteAcls(Collections.singletonList(entry));
}
```

```python
def create_acl(self, entry: codec.AclBinding) -> None:
    return self.create_acls([entry])

def delete_acl(self, entry: codec.AclBinding) -> int:
    return self.delete_acls([entry])
```

```go
_ = c.CreateAcl(e)                                          // one binding
_ = c.CreateAcls([]codec.AclBinding{e})                     // unchanged
n, _ := c.DeleteAcl(e)                                      // one binding
n, _ = c.DeleteAcls([]codec.AclBinding{e})                  // unchanged
listed, _ := c.ListAclsAll()                                // unchanged
```

```java
c.createAcl(e);                                             // one binding
c.createAcls(Collections.singletonList(e));                 // unchanged
int n = c.deleteAcl(e);                                     // one binding
n = c.deleteAcls(Collections.singletonList(e));             // unchanged
List<AclBinding> listed = c.listAcls();                     // unchanged
```

```python
c.create_acl(e)                                             # one binding
c.create_acls([e])                                          # unchanged
n = c.delete_acl(e)                                         # one binding
n = c.delete_acls([e])                                      # unchanged
listed = c.list_acls()                                      # unchanged
```

## Semantics

- One-binding helpers send wire count **1** with that `AclBinding`.
- They do not re-encode; they wrap the existing batch method.
- `CreateAcls` / `create_acls` / `createAcls` are unchanged.
- `DeleteAcls` / `delete_acls` / `deleteAcls` are unchanged
  (exact-match only; no filter-delete).
- `ListAcls` / `ListAclsAll` / `list_acls` are unchanged.
- Transient 6 / 7 / 15 / 16 and transport retry via the batch method
  (`adminRoundTrip`; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.72).
- Not Kafka CreateAcls / DeleteAcls / DescribeAcls.

## Tests

Fake TCP stub that records decoded CreateAcls / DeleteAcls entries
(same helpers as existing `acls_test.go` / `AclsTest.java` /
`test_acls.py`). Existing batch tests stay green.

```bash
(cd clients/go && go test ./...)
(cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_acls -q)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| `CreateAcl` / `createAcl` / `create_acl` (`sample` binding) | wire entries count 1; same binding |
| `DeleteAcl` / `deleteAcl` / `delete_acl` (`sample` binding) | wire entries count 1; same binding; removed returned |
| Existing `CreateAcls` / `DeleteAcls` / `ListAcls` cases | still pass |

Existing CreateAcls / DeleteAcls retry / 14 tests must still pass
(batch methods unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `CreateAcl` / `DeleteAcl` wrap batch with one entry |
| `clients/go/acls_test.go` | one-entry wire checks |
| `clients/go/README.md` | usage line + one prose sentence |
| `clients/java/src/main/java/io/volant/Client.java` | named `createAcl` / `deleteAcl` wrap batch |
| `clients/java/src/test/java/io/volant/AclsTest.java` | one-entry wire checks |
| `clients/java/README.md` | usage line + one prose sentence |
| `clients/python/src/volant/client.py` | `create_acl` / `delete_acl` wrap batch |
| `clients/python/tests/test_acls.py` | one-entry wire checks |
| `clients/python/README.md` | usage line + one prose sentence |
| `docs/V169_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** CreateAcls / DeleteAcls / DescribeAcls.
- Delete is exact-match only (no filter-delete).
- `CreateAcls` / `DeleteAcls` / `ListAcls` / `ListAclsAll` are unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust are unchanged.

## Merge notes

Sibling slices that also edit language `Client` should keep this hunk
local to the one-binding wrappers:

- **Keep the wrappers only.** Do not change `CreateAcls` /
  `create_acls` / `createAcls` / `DeleteAcls` / `delete_acls` /
  `deleteAcls` / `ListAcls` / `ListAclsAll` / `list_acls`.
- Do not change the ACL send loop (`adminRoundTrip` / v0.72 error 14).
- Do not change Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` — hunk is local to `CreateAcl` after
  `CreateAcls` and `DeleteAcl` after `DeleteAcls`
- Java `clients/java/src/main/java/io/volant/Client.java` —
  named `createAcl` / `deleteAcl` after the batch methods
- Python `clients/python/src/volant/client.py` — `create_acl` /
  `delete_acl` after the batch methods
- `clients/*/README.md` and the existing ACL test files

## Related

- [V56_SPEC.md](./V56_SPEC.md) — language Create/Delete/ListAcls
- [V72_SPEC.md](./V72_SPEC.md) — language CreateAcls / DeleteAcls error 14
- [V161_SPEC.md](./V161_SPEC.md) — Go ListAclsAll
- [V164_SPEC.md](./V164_SPEC.md) — language single-entry DeleteOffset
- [PHASE20_SPEC.md](./PHASE20_SPEC.md) — native 54–59
