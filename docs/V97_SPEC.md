# v0.97 — DeleteOffsets NotController redirect on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V72_SPEC.md](./V72_SPEC.md) /
[V93_SPEC.md](./V93_SPEC.md) / TODO: “DeleteOffsets still does not
redirect.” v0.78 already retries transient 6 / 7 / 15 / 16 on
DeleteOffsets. DeleteOffsets is often group-local, so the broker may
not return native **14** (`NotController`) today. When it does,
language clients should follow the same controller-gated admin budget
as CreateTopic / CreateAcls / DescribeConfigs.

Reuse the existing `_redirect_to_controller` / `redirectToController`
and `max_redirects` budget (and the v0.81 Metadata.controller_id hunt).
Do **not** rewrite the helper. Keep the existing transient retry.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. On DeleteOffsets, if `error_code == 14` or `BrokerError` 14: if
   redirect attempts remain (`1 + max_redirects`), call the existing
   controller redirect helper and retry the **same** DeleteOffsets
   (same entries / wait).
2. Parse `controller_id=` from any 14 message when present (existing
   helper).
3. Budget is the same as produce/fetch: `1 + max_redirects` (default
   `max_redirects=1`). `max_redirects=0` raises on the first 14
   (no Metadata).
4. Transient 6 / 7 / 15 / 16 still use `max_retries` (already shipped).
   14 is **not** a transient retry — it uses the redirect budget only.
5. Other errors (2, etc.) still raise immediately.
6. No new public methods. Wrap `delete_offsets` / `DeleteOffsets`
   only.
7. Do **not** wrap OffsetCommit / OffsetFetch 14 (loops are separate).
8. Do **not** wrap begin_transaction / metadata.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (v0.81 already shipped) |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| OffsetCommit / OffsetFetch 14 | Separate loops; not this slice |
| begin_transaction / metadata wrap | Out of scope |
| Broker / protocol / Rust client | Frozen (whether the broker returns 14 today) |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.72 admin:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- DeleteOffsets may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`
  or a typed response with `error_code=14` and no id.
- Transient 6 / 7 / 15 / 16 (and transport) still sleep
  `retry_backoff_ms` and retry up to `max_retries` extra times.
  Independent of `max_redirects`.

Hunt is unchanged (existing helper + v0.81 Metadata.controller_id).

## API

No new public methods. Existing:

```python
c.delete_offsets("g")
c.delete_offsets("g", [("events", 0)])
```

```go
c.DeleteOffsets(group, nil)
c.DeleteOffsets(group, entries)
```

```java
c.deleteOffsets(group);
c.deleteOffsets(group, entries);
```

Error 14 now follows Produce/Fetch redirect budget. Transient 6 / 7 /
15 / 16 still follow `max_retries`. Not Kafka FindCoordinator / OffsetDelete.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| DeleteOffsets first 14 + `controller_id=2`; Metadata names node 2; second ok | success; deleted_count parsed |
| DeleteOffsets typed 14 (no hint); Metadata has another broker; second ok | success |
| `max_redirects=0` + 14 | raise 14; no Metadata |
| Existing DeleteOffsets transient retry (7 then 0, max_retries=2) | still two RPCs, success |

## Merge notes

Sibling slice **v0.99** also edits `Client`. When merging:

- **Keep the DeleteOffsets wrap** (14 redirect + existing transient
  retry).
- Do not change `_redirect_to_controller` / `redirectToController`
  hunt logic (that is v0.81).
- Do not wrap OffsetCommit / OffsetFetch / begin_transaction /
  metadata.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops or the v0.72 /
  v0.85 / v0.89 / v0.93 admin wraps.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is v0.81.
- Not Kafka `FindCoordinator`. Not Kafka OffsetDelete.
- DeleteOffsets is often group-local; the broker may not return 14
  today. This slice is client-side wrap only.
- OffsetCommit / OffsetFetch still do not redirect on 14.
- No Kafka API keys / opcodes / Phase 155.

See [V72_SPEC.md](./V72_SPEC.md) (admin 14 redirect),
[V78_SPEC.md](./V78_SPEC.md) (DeleteOffsets transient retry),
[V81_SPEC.md](./V81_SPEC.md) (Metadata.controller_id hunt),
[V85_SPEC.md](./V85_SPEC.md) (SCRAM-admin / ListAcls 14), and
[V93_SPEC.md](./V93_SPEC.md) (Describe/AlterConfigs 14).
