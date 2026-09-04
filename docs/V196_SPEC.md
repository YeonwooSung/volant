# v0.196 — Python list_acls_all helper

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V56_SPEC.md](./V56_SPEC.md) /
[V161_SPEC.md](./V161_SPEC.md) / [V162_SPEC.md](./V162_SPEC.md):
Go already has `ListAclsAll()` (v0.161) wrapping
`ListAcls("", 255, "")`. Rust has `list_acls_all` (v0.162). Java
has no-arg `listAcls()`. Python `list_acls(principal="",
resource_type=255, resource="")` already defaults to empty filters,
but there is no named `list_acls_all` helper matching Go/Rust.

Add `Client.list_acls_all`. Reuse `list_acls` (do not reimplement
the RPC). `list_acls` stays unchanged. This is **not** Kafka
DescribeAcls.

This is residual **v0.196** (Python ListAcls unfiltered named helper).
It is **not** Phase 155. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, Go,
Java, or Rust.

## Goals

1. Add public `def list_acls_all(self) -> list[codec.AclBinding]`
   that calls `self.list_acls()` (same as
   `list_acls("", 255, "")`; empty filters = any principal / type /
   resource).
2. Inherit retry / error **14** from `list_acls` /
   `_admin_round_trip`. No new retry policy.
3. Do **not** change `list_acls` signature or defaults.
4. Do **not** change broker / protocol / Go / Java / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `list_acls` | Frozen; empty filters already mean any |
| Kafka DescribeAcls (API key 29) | Native opcode 58/59 only |
| Filter-delete / new ACL opcodes | Exact-match delete only (v0.56) |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Go / Java / Rust | Already have no-arg / named-all helpers |
| Phase 155 / homemade Raft | Frozen |

## API

```python
def list_acls_all(self) -> list[codec.AclBinding]:
    """List every ACL binding (empty filters).

    Same as ``list_acls()`` / ``list_acls("", 255, "")``.
    Error 14 / transient retry inherit from ``list_acls``.
    """
    return self.list_acls()
```

```python
listed = c.list_acls_all()                 # every binding
listed = c.list_acls()                     # unchanged: same wire
listed = c.list_acls("", 255, "")          # unchanged: same wire
listed = c.list_acls("User:alice", 0, "events")
```

## Semantics

- Empty principal / resource = any. `resource_type=255` = any type
  (same as `list_acls()` / `list_acls("", 255, "")`).
- `list_acls_all` is a named wrapper; it does not re-encode.
- `list_acls(...)` is unchanged (empty filters still mean all).
- Transient 6 / 7 / 15 / 16 and transport retry via `list_acls` /
  `_admin_round_trip` (default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.85).
- Not Kafka DescribeAcls / CreateAcls / DeleteAcls.

## Tests

Fake TCP stub that records the decoded ListAcls request (same
`_AclServer` helper as existing `test_acls.py`).

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_acls -q
```

Do **not** run full unittest discover (hangs ~5 min). Do **not**
append codec tests.

| Case | Expect |
|------|--------|
| `list_acls_all()` | empty principal, resource_type 255, empty resource; same as `list_acls()` |
| Existing `list_acls` empty / explicit / error cases | still pass |

Existing ListAcls retry / 14 tests must still pass
(`list_acls` unchanged).

| File | What |
|------|------|
| `clients/python/src/volant/client.py` | `list_acls_all` wraps `list_acls()` |
| `clients/python/tests/test_acls.py` | empty-filter wire check |
| `clients/python/README.md` | usage line + one prose sentence |
| `docs/V196_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** DescribeAcls.
- Empty filters still list **every** ACL binding.
- `list_acls(...)` is unchanged.
- Go / Java / Rust / broker / protocol are unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**

## Merge notes

Sibling v0.197 / v0.198 also edit
`clients/python/src/volant/client.py` and possibly the Python README.
Keep this hunk local to `list_acls_all` after `list_acls`. Do not
change `list_acls`.

- **Keep the wrapper only.** Do not change `list_acls`.
- Do not change the ListAcls send loop (`_admin_round_trip` + v0.85 14).
- Do not change Go, Java, Rust, broker, or protocol.

Expect conflicts on:

- `clients/python/src/volant/client.py` — hunk is local to
  `list_acls_all` after `list_acls`
- `clients/python/tests/test_acls.py`
- `clients/python/README.md`

## Related

- [V56_SPEC.md](./V56_SPEC.md) — language Create/Delete/ListAcls
- [V85_SPEC.md](./V85_SPEC.md) — language SCRAM-admin / ListAcls 14
- [V88_SPEC.md](./V88_SPEC.md) — Rust SCRAM-admin / ListAcls 14
- [V161_SPEC.md](./V161_SPEC.md) — Go ListAclsAll
- [V162_SPEC.md](./V162_SPEC.md) — Rust list_acls_all
- [PHASE20_SPEC.md](./PHASE20_SPEC.md) — native 54–59
