# v0.93 — DescribeConfigs / AlterConfigs NotController redirect on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V72_SPEC.md](./V72_SPEC.md) /
TODO: “Describe/AlterConfigs still do not redirect.” Topic configs are
often local-readable, so the broker may not return native **14**
(`NotController`) today. When it does, language clients should follow
the same controller-gated admin budget as CreateTopic / CreateAcls.

Reuse the existing `_redirect_to_controller` / `redirectToController`
and `max_redirects` budget (and the v0.81 Metadata.controller_id hunt).
Do **not** rewrite the helper.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. Same loop as `create_topic` / `create_acls`: on error **14**
   (`BrokerError` / typed `error_code` / `ErrorResponse`), if attempts
   remain, call the existing controller redirect helper and retry the
   **same** DescribeConfigs / AlterConfigs.
2. Parse `controller_id=` from any 14 message when present (existing
   helper).
3. Budget is the same as produce/fetch: `1 + max_redirects` (default
   `max_redirects=1`). `max_redirects=0` raises on the first 14.
4. Other errors (2 not found, etc.) still raise immediately.
5. Topic-only configs unchanged. Not Kafka DescribeConfigs BROKER.
6. No new public methods. Wrap `describe_configs` / `alter_configs`
   only.
7. Do **not** wrap delete_offsets, add_broker, metadata, list_members.

Python uses existing `_admin_round_trip`. Go / Java match the
CreateAcls 14 loop (`adminRoundTrip` on Java).

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (v0.81 already shipped) |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| Kafka DescribeConfigs BROKER | Native topic-only 40–43 |
| DeleteOffsets / metadata / list_members wrap | Out of scope |
| Broker / protocol / Rust client | Frozen (whether the broker returns 14 today) |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.72 admin:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- DescribeConfigs may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`.
  AlterConfigs may return a typed response with `error_code=14` and no
  id.

Hunt is unchanged (existing helper + v0.81 Metadata.controller_id).

## API

No new public methods. Existing:

```python
c.describe_configs("t")
c.alter_configs("t", [("retention.ms", "86400000")])
```

```go
c.DescribeConfigs(...)
c.AlterConfigs(...)
```

```java
c.describeConfigs(...)
c.alterConfigs(...)
```

Error 14 now follows Produce/Fetch redirect budget. Not Kafka
FindCoordinator. Topic configs only.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| DescribeConfigs first 14 + `controller_id=2`; Metadata names node 2; second ok | success; configs parsed |
| AlterConfigs typed 14 (no hint); Metadata has another broker; second ok | success |
| DescribeConfigs `max_redirects=0` + 14 | raise 14; no Metadata |

## Merge notes

Sibling slice **v0.95** also edits `Client`. When merging:

- **Keep the two method wraps** (DescribeConfigs / AlterConfigs).
- Do not change `_redirect_to_controller` / `redirectToController`
  hunt logic (that is v0.81).
- Do not wrap delete_offsets, add_broker, metadata, list_members.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops or the v0.72 /
  v0.85 / v0.89 admin wraps.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is v0.81.
- Not Kafka `FindCoordinator`. Not Kafka DescribeConfigs BROKER.
- Topic configs are often local-readable; the broker may not return 14
  today. This slice is client-side wrap only.
- DeleteOffsets still does not redirect on 14.
- No Kafka API keys / opcodes / Phase 155.

See [V72_SPEC.md](./V72_SPEC.md) (admin 14 redirect),
[V81_SPEC.md](./V81_SPEC.md) (Metadata.controller_id hunt),
[V85_SPEC.md](./V85_SPEC.md) (SCRAM-admin / ListAcls 14), and
[V53_SPEC.md](./V53_SPEC.md) (Describe/AlterConfigs).
