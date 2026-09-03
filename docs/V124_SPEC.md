# v0.124 — DescribeGroup / ListGroups NotController redirect on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V90_SPEC.md](./V90_SPEC.md) /
[V105_SPEC.md](./V105_SPEC.md): language `describe_group` /
`list_groups` already retry transient 6 / 7 / 15 / 16 (v0.90) but
treat **14** (`NotController`) as not retried. OffsetCommit already
redirects on 14 (v0.105). Same honesty: the broker may not return 14
on these RPCs today; this is client-side wrap only.

Reuse `_redirect_to_controller` / `redirectToController` (v0.81 hunt).
Keep existing `max_retries` for 6 / 7 / 15 / 16. 14 is **not** a
transient retry.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. On DescribeGroup / ListGroups, if `error_code == 14` or
   `BrokerError` 14: if redirect attempts remain (`1 + max_redirects`),
   call the existing controller redirect helper and retry the **same**
   RPC.
2. Parse `controller_id=` from any 14 message when present (existing
   helper).
3. Budget is the same as produce/fetch: `1 + max_redirects` (default
   `max_redirects=1`). `max_redirects=0` raises on the first 14
   (no Metadata).
4. Transient 6 / 7 / 15 / 16 still use `max_retries` (already shipped).
   14 is **not** a transient retry — it uses the redirect budget only.
5. **Not redirected:** 13, 2, 9 / 10 / 11, 17 / 18, 21, 22, Protocol.
   Error **2** on DescribeGroup (no live members) still raises
   immediately.
6. No new public methods. Wrap `describe_group` / `DescribeGroup` /
   `describeGroup` and `list_groups` / `ListGroups` / `listGroups`.
7. Do **not** wrap ListMembers (v0.121) or OffsetFetch (v0.122).
8. Do **not** change `_redirect_to_controller` / `redirectToController`
   or `_admin_round_trip`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (v0.81 already shipped) |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| ListMembers 14 | Sibling v0.121 |
| OffsetFetch 14 | Sibling v0.122 (already v0.105) |
| Broker / protocol / Rust client | Frozen (whether the broker returns 14 today) |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.72 / v0.105 OffsetCommit:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- DescribeGroup / ListGroups may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`
  or a typed response with `error_code=14` and no id.
- Transient 6 / 7 / 15 / 16 (and transport) still sleep
  `retry_backoff_ms` and retry up to `max_retries` extra times.
  Independent of `max_redirects`.
- Error **2** (DescribeGroup, no live members) still raises
  immediately; not a retry and not a redirect.

Hunt is unchanged (existing helper + v0.81 Metadata.controller_id).

## API

No new public methods. Existing:

```python
c.describe_group("g")
c.list_groups()
```

```go
c.DescribeGroup("g")
c.ListGroups()
```

```java
c.describeGroup("g");
c.listGroups();
```

Error 14 now follows Produce/Fetch redirect budget. Transient 6 / 7 /
15 / 16 still follow `max_retries`. Not Kafka FindCoordinator.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Next to existing Describe/ListGroups retry tests:

| Case | Expect |
|------|--------|
| DescribeGroup first 14 + `controller_id=2`; Metadata names node 2; second ok | success |
| ListGroups typed 14 (no hint); Metadata has another broker; second ok | success |
| DescribeGroup `max_redirects=0` + first 14 | raise 14; no Metadata |
| Existing: `max_retries=2`, first DescribeGroup 7 then 0 | two RPCs, no Metadata |
| DescribeGroup first **2** with `max_retries=2` | immediately 2 |

## Merge notes

Sibling slices **v0.121** (ListMembers 14) / **v0.122** (OffsetFetch)
also edit `Client`. When merging:

- **Keep the DescribeGroup / ListGroups wrap** (14 redirect + existing
  transient retry). Do not drop the v0.90 transient retry.
- Do not change `_redirect_to_controller` / `redirectToController`
  hunt logic (that is v0.81).
- Do not wrap ListMembers or OffsetFetch in this merge.
- Do not change `_admin_round_trip` or InitProducerId.
- Do not change the broker, Kafka shim, or Rust client in this merge.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`describe_group` /
  `list_groups`)
- Go `clients/go/client.go` (`DescribeGroup` / `ListGroups`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`describeGroup` / `listGroups`)
- Scripted brokers in `test_client.py` / `client_test.go` /
  `ClientTest.java`

The hunk is local to `describe_group` / `list_groups`.

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is v0.81.
- Not Kafka `FindCoordinator`.
- DescribeGroup / ListGroups are often group-local; the broker may not
  return 14 today. This slice is client-side wrap only (same honesty
  as OffsetCommit 14).
- ListMembers and OffsetFetch are not wrapped here.
- No Kafka API keys / opcodes / Phase 155.

See [V90_SPEC.md](./V90_SPEC.md) (DescribeGroup / ListGroups transient
retry), [V105_SPEC.md](./V105_SPEC.md) (OffsetCommit / OffsetFetch
error 14), [V72_SPEC.md](./V72_SPEC.md) (admin 14 redirect), and
[V81_SPEC.md](./V81_SPEC.md) (Metadata.controller_id hunt).
