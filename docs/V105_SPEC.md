# v0.105 — OffsetCommit / OffsetFetch NotController redirect on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V97_SPEC.md](./V97_SPEC.md) /
[V98_SPEC.md](./V98_SPEC.md) / [V78_SPEC.md](./V78_SPEC.md): language
DeleteOffsets already redirects on error **14** (`NotController`).
OffsetCommit / OffsetFetch still only have transient retry (v0.78).
Rust v0.98 already put 14 on `offset_admin_round_trip`, so
OffsetCommit / OffsetFetch inherit there.

Reuse `_redirect_to_controller` / `redirectToController` (v0.81 hunt).
Keep existing `max_retries` for 6 / 7 / 15 / 16. 14 is **not** a
transient retry.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. On OffsetCommit / OffsetFetch, if `error_code == 14` or
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
5. Other errors (2, 9 / 10 / 11, etc.) still raise immediately.
6. No new public methods. Wrap `offset_commit` / `OffsetCommit` /
   `commitOffsets` and `offset_fetch` / `OffsetFetch` / `fetchOffsets`
   (all overloads that send the RPC).
7. GroupConsumer `commit` inherits via offset_commit.
8. Do **not** change `_admin_round_trip` or InitProducerId.

The three languages do not share an offset-admin helper the way Rust
does. Each OffsetCommit / OffsetFetch send loop is wrapped the same
way DeleteOffsets was in v0.97.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (v0.81 already shipped) |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| `_admin_round_trip` / InitProducerId | Frozen |
| Broker / protocol / Rust client | Frozen (Rust already inherits via v0.98) |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.72 / v0.97 admin:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- OffsetCommit / OffsetFetch may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`
  or a typed response with `error_code=14` and no id.
- Transient 6 / 7 / 15 / 16 (and transport) still sleep
  `retry_backoff_ms` and retry up to `max_retries` extra times.
  Independent of `max_redirects`.

Hunt is unchanged (existing helper + v0.81 Metadata.controller_id).

## API

No new public methods. Existing:

```python
c.offset_commit("g", "t", 0, 5)
c.offset_fetch("g", "t")
```

```go
c.OffsetCommit(group, topic, 0, 5)
c.OffsetFetch(group, topic)
```

```java
c.offsetCommit(group, topic, 0, 5);
c.offsetFetch(group, topic);
```

Error 14 now follows Produce/Fetch redirect budget. Transient 6 / 7 /
15 / 16 still follow `max_retries`. Not Kafka FindCoordinator.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| OffsetCommit first 14 + `controller_id=2`; Metadata names node 2; second ok | success |
| OffsetFetch typed 14 (no hint); Metadata has another broker; second ok | success |
| OffsetCommit `max_redirects=0` + 14 | raise 14; no Metadata |
| Existing OffsetCommit 7 then 0 (`max_retries=2`) | still two RPCs, success |

## Merge notes

Sibling slices **v0.101** / **v0.103** also edit `Client`. When merging:

- **Keep the OffsetCommit / OffsetFetch wrap** (14 redirect + existing
  transient retry). Do not drop the v0.78 transient retry.
- Do not change `_redirect_to_controller` / `redirectToController`
  hunt logic (that is v0.81).
- Do not change `_admin_round_trip` or InitProducerId.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops or the v0.72 /
  v0.85 / v0.89 / v0.93 / v0.97 admin wraps.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is v0.81.
- Not Kafka `FindCoordinator`.
- OffsetCommit / OffsetFetch are often group-local; the broker may not
  return 14 today. This slice is client-side wrap only.
- `metadata`, `begin_transaction`, and InitProducerId are not wrapped.
- No Kafka API keys / opcodes / Phase 155.

See [V78_SPEC.md](./V78_SPEC.md) (OffsetCommit / OffsetFetch transient
retry), [V97_SPEC.md](./V97_SPEC.md) (language DeleteOffsets 14),
[V98_SPEC.md](./V98_SPEC.md) (Rust offset-admin 14),
[V72_SPEC.md](./V72_SPEC.md) (admin 14 redirect), and
[V81_SPEC.md](./V81_SPEC.md) (Metadata.controller_id hunt).
