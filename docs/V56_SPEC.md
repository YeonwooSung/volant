# v0.56 — Create/Delete/ListAcls on Python / Go / Java clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “language clients still lack ACLs.”
Rust `volant-client` already has `create_acls` / `delete_acls` /
`list_acls`. This slice ports native opcodes **54–59** (Phase 20) to
the Python, Go, and Java clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker ACL store or enforcement.

## Goals

1. **Codec** encode/decode for CreateAcls (54/55), DeleteAcls (56/57),
   and ListAcls (58/59) in Python, Go, and Java. Match
   `crates/volant-protocol/src/payload.rs` `Request::CreateAcls` /
   `DeleteAcls` / `ListAcls` and `AclBinding`.
2. **Public RPC** on each language client, matching the Rust shape.
3. Non-zero `error_code` raises like every other RPC (`BrokerError` /
   `BrokerException`) with `op="create_acls"` / `"delete_acls"` /
   `"list_acls"`.
4. Unit tests without a broker: codec round-trip of one Allow Topic
   binding plus a fake TCP server (create ok; delete `removed=1`; list
   returns bindings; error 23 raises). Existing tests still pass.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka CreateAcls / DeleteAcls / DescribeAcls (API keys 30/31/29) | Native 54–59 only |
| Filter-delete | Exact-match delete only (same as Rust / Phase 20) |
| New native opcodes | Reuse 54–59 |
| Broker / protocol / Rust client changes | Already shipped (Phase 20) |
| Kafka API keys | Frozen at 38 |
| Phase 155 / homemade metadata Raft | Out of scope |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`. Payload integers are
little-endian. Strings are `u16` length + UTF-8 (`put_string`).

### AclBinding (one entry)

```
principal: string
resource_type: u8    # 0=Topic, 1=Group, 2=Cluster
resource: string     # or *
operation: u8        # 0=All … 7=ClusterAction
permission: u8       # 0=Deny, 1=Allow
```

### Request opcode 54 `CreateAcls`

```
count: u32
  entries: AclBinding × count
```

### Response opcode 55 `CreateAcls`

```
error_code: u16    # 0=ok; 3=invalid; 23=unauthorized
```

### Request opcode 56 `DeleteAcls`

Same body as CreateAcls. Delete removes **exact** matching entries.

### Response opcode 57 `DeleteAcls`

```
error_code: u16
removed: u32
```

### Request opcode 58 `ListAcls`

```
principal: string      # empty = any
resource_type: u8      # 255 = any type
resource: string       # empty = any
```

### Response opcode 59 `ListAcls`

```
error_code: u16
count: u32
  entries: AclBinding × count
```

This is **not** Kafka CreateAcls / DeleteAcls / DescribeAcls.

## API

```python
c.create_acls(entries: list[AclBinding]) -> None
c.delete_acls(entries: list[AclBinding]) -> int   # removed
c.list_acls(principal="", resource_type=255, resource="") -> list[AclBinding]
```

```go
c.CreateAcls(entries []codec.AclBinding) error
c.DeleteAcls(entries []codec.AclBinding) (uint32, error)
c.ListAcls(principal string, resourceType uint8, resource string) ([]codec.AclBinding, error)
```

```java
c.createAcls(List<AclBinding> entries)
c.deleteAcls(List<AclBinding> entries)  // returns int removed
c.listAcls()  // any/any/any
c.listAcls(String principal, int resourceType, String resource)
```

Non-zero `error_code` raises `BrokerError(..., op="create_acls")`
(Python), `BrokerError{Code, Op: "create_acls"}` (Go), or
`BrokerException(code, "", "create_acls")` (Java) — same `op`
strings for delete / list.

## Tests

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode one Allow Topic binding (`User:alice`, type 0, `events`, op 3, perm 1) | request count 1; response 0 |
| List request empty filters (`""`, 255, `""`) | empty principal/resource; type 255 |
| Fake server create ok | no raise; wire fields match |
| Fake server delete | returns `removed=1` |
| Fake server list | returns bindings |
| Fake server `error_code=23` | raises with `op="create_acls"` |
| Existing tests | still pass |

## Merge notes

Sibling slices **v0.57–v0.59** also edit the same codec / Client /
README files. When merging:

- **Keep all opcodes.** Do not drop 30–49, 54–59, 60–69, or any
  opcode another slice added. `decode_response` / `DecodeResponse` /
  `decodeResponse` is a switch — union every case.
- ACL path is **additive**. Do not reuse 54–59 for a new API.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- **Not Kafka CreateAcls / DeleteAcls / DescribeAcls.** Native 54–59
  only. Kafka API keys 30/31/29 stay on the shim.
- Exact-match delete only. No filter-delete.
- Does not change broker ACL store or enforcement.
- No Kafka API keys / new opcodes / Phase 155.

See [PHASE20_SPEC.md](./PHASE20_SPEC.md) (native 54–59) and
[PHASE35_SPEC.md](./PHASE35_SPEC.md) (Kafka ACL admin on the shim).
