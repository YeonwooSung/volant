# v0.121 — language ListMembers NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V95_SPEC.md](./V95_SPEC.md) /
[V120_SPEC.md](./V120_SPEC.md): language `list_members` retries
transient 6/7/15/16 but treats **14** as not retried. Rust
`list_members` now redirects on 14 (v0.120).

Wrap **only** `list_members` / `ListMembers` / `listMembers`. Do
**not** wrap DescribeGroup / ListGroups (sibling v0.124), OffsetFetch
(v0.122), or GroupConsumer (v0.123). Do not change Rust, broker, or
protocol.

Reuse existing `_redirect_to_controller` / `redirectToController` +
`max_redirects`. Keep existing transient retry (`max_retries`, default
**0**). 14 uses an independent `redirect_attempt` counter.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / Rust client.

## Goals

1. On ListMembers typed `error_code == 14` or BrokerError / Error
   opcode 14: if `redirect_attempt + 1 < 1 + max_redirects` and the
   helper returns true, resend ListMembers.
2. Parse `controller_id=N` from an Error message via
   `_controller_id_hint` / `parseControllerID` / `parseControllerId`.
   Typed 14 has no hint (`None`).
3. 14 uses **redirect budget** (`max_redirects`), not `max_retries`.
   14 does **not** increment `retry_attempt`. Transient 6 / 7 / 15 /
   16 stay on `max_retries` (existing loop).
4. Budget is the same as produce/fetch / v0.72 admin:
   `1 + max_redirects` (default `max_redirects=1`).
   `max_redirects=0` surfaces 14 with no Metadata.
5. **Not redirected:** 13, 2, 9 / 10 / 11, 17 / 18, 21, 22, Protocol.
6. No new public methods. Existing `list_members` signatures stay.
7. Hunt recursion: `_redirect_to_controller` currently calls public
   `list_members()` on a Metadata id miss. After this wrap that would
   recurse on 14. Extract a **private** `list_members_rpc` /
   `listMembersRpc` that does the existing retry **without** 14
   redirect, and call that from the hunt (same as Rust
   `list_members_rpc`). Public `list_members` uses the 14 wrap around
   that helper.
8. Do **not** change broker / protocol / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (existing helper as-is) |
| Rust `list_members` 14 | Already v0.120 |
| DescribeGroup / ListGroups 14 | Sibling v0.124 |
| OffsetFetch 14 | Sibling v0.122 |
| GroupConsumer | Sibling v0.123 |
| Adding 14 to Metadata retry | Metadata is not controller-gated |
| Kafka `FindCoordinator` / API keys | Native ListMembers only |
| Broker / protocol | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

Same redirect budget as Produce/Fetch / v0.72 admin; independent of
the v0.95 transient retry budget:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- ListMembers may arrive as Error opcode
  `{ code: 14, message: "not controller; controller_id=N" }` or a
  typed ListMembers `{ error_code: 14, .. }` with no id.
- Transient 6 / 7 / 15 / 16 and TCP/IO still retry on `max_retries`
  (default 0) as in v0.95. A 7-then-0 ListMembers still succeeds in
  two RPCs with `max_retries >= 1` and no Metadata.
- Error **13** / **2** / **9** / **10** / **11** / **17** / **18** /
  **21** / **22** and protocol are not retried and not redirected
  here.

Hunt is unchanged (existing helper). Message hint wins; otherwise
Metadata.controller_id when non-zero; otherwise the first other
advertised broker. Hunt calls the private no-14 path so a hinted-id
Metadata miss does not re-enter the 14 wrap.

## API

No new public methods. Existing:

```python
c.list_members()
```

```go
c.ListMembers()
```

```java
c.listMembers();
```

Error 14 now follows Produce/Fetch redirect budget. Transient 7 still
uses `max_retries`. Not Kafka FindCoordinator.

## Tests

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest discover -s tests -q
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

Fake TCP (no live broker), next to existing ListMembers retry tests:

| Case | Expect |
|------|--------|
| First ListMembers 14 + `controller_id=2`; Metadata names node 2; second ok | success; 14 is redirect not retry |
| Typed 14 (no hint); Metadata has another advertised broker; second ok | success |
| `max_redirects=0` + first 14 | 14; no Metadata |
| `max_retries=2`, first 7 then 0 | two ListMembers, no Metadata |

| File | What |
|------|------|
| `clients/python/src/volant/client.py` | 14 wrap + `_list_members_rpc` hunt |
| `clients/go/client.go` | 14 wrap + `listMembersRpc` hunt |
| `clients/java/src/main/java/io/volant/Client.java` | 14 wrap + `listMembersRpc` hunt |
| `clients/python/tests/test_client.py` | queued-code stub |
| `clients/go/client_test.go` | queued-code stub |
| `clients/java/src/test/java/io/volant/ClientTest.java` | queued-code stub |
| `docs/V121_SPEC.md` | This spec |

## Honesty leftovers

- Redirect still uses the existing `_redirect_to_controller` /
  `redirectToController` helper (Metadata brokers or ListMembers on a
  hinted id miss). Hunt is unchanged. The hunt calls a private no-14
  ListMembers path so `list_members` and the helper are not mutually
  recursive. Hinted-id Metadata miss therefore does not re-enter the
  14 wrap.
- Not Kafka `FindCoordinator`.
- Default `max_retries` stays **0**.
- `metadata()` is not wrapped.
- Rust / broker / protocol still do not change whether ListMembers
  returns 14 today.
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling slices that also edit the three Client files should keep this
wrap local to `list_members` + the hunt helper call:

- **Keep the 14 arm around `list_members_rpc`**. Do not drop the v0.95
  transient retry.
- Do **not** add 14 to Metadata retry.
- Do not wrap DescribeGroup / ListGroups (v0.124), OffsetFetch
  (v0.122), or GroupConsumer (v0.123).
- Do not change Rust, broker, or protocol.

Expect conflicts on the three Client files — hunk is local to
`list_members` + hunt helper call.

## Related

- [V120_SPEC.md](./V120_SPEC.md) — Rust ListMembers 14 this matches
- [V95_SPEC.md](./V95_SPEC.md) — language Metadata / ListMembers retry
- [V72_SPEC.md](./V72_SPEC.md) — language admin 14
- [V96_SPEC.md](./V96_SPEC.md) — Rust Metadata / ListMembers retry
