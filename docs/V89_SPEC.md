# v0.89 — AddBroker / RemoveBroker NotController redirect on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V72_SPEC.md](./V72_SPEC.md) /
TODO: “Add/RemoveBroker still do not redirect.” v0.72 skipped them
because the broker already forwards to the openraft leader (v0.38).
When forward is unavailable the client still sees native **14**
(`NotController`).

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
   **same** AddBroker / RemoveBroker.
2. Parse `controller_id=` from any 14 message when present (existing
   helper).
3. Budget is the same as produce/fetch: `1 + max_redirects` (default
   `max_redirects=1`). `max_redirects=0` raises on the first 14.
4. Other errors still raise immediately.
5. No new public methods. Wrap `add_broker` / `AddBroker` and
   `remove_broker` / `RemoveBroker` only.
6. Overlay is still SoT. This is not Kafka broker catalog.
7. Do **not** wrap Describe/AlterConfigs, leave_group, describe_group.

Python uses existing `_admin_round_trip`. Go / Java match the
CreateAcls 14 loop (`adminRoundTrip` on Java).

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (v0.81 already shipped) |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| Describe/AlterConfigs, leave_group, describe_group | Out of scope |
| Broker / protocol / Rust client | Frozen (broker already forwards; 14 when forward fails) |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.72 admin:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- AddBroker may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`.
  RemoveBroker may return a typed response with `error_code=14` and no
  id. When the broker cannot forward (v0.38), 14 is also typed.

Hunt is unchanged (existing helper + v0.81 Metadata.controller_id).

## API

No new public methods. Existing:

```python
c.add_broker(3, "10.0.0.3", 9092)
c.remove_broker(3)
```

```go
c.AddBroker(...)
c.RemoveBroker(...)
```

```java
c.addBroker(...)
c.removeBroker(...)
```

Error 14 now follows Produce/Fetch redirect budget when the broker
cannot forward. Not Kafka FindCoordinator. Overlay is still SoT.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| AddBroker first 14 + `controller_id=2`; Metadata names node 2; second ok | success; generation parsed |
| RemoveBroker typed 14 (no hint); Metadata has another broker; second ok | success |
| AddBroker `max_redirects=0` + 14 | raise 14; no Metadata |

## Merge notes

Sibling slices **v0.86** / **v0.90** also edit `Client`. When merging:

- **Keep the two method wraps** (AddBroker / RemoveBroker).
- Do not change `_redirect_to_controller` / `redirectToController`
  hunt logic (that is v0.81).
- Do not wrap Describe/AlterConfigs, leave_group, describe_group.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops or the v0.72 /
  v0.85 admin wraps.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is v0.81.
- Not Kafka `FindCoordinator`.
- Describe/AlterConfigs, leave_group, and describe_group still do not
  redirect on 14.
- Overlay is still SoT; this is not Kafka broker catalog.
- No Kafka API keys / opcodes / Phase 155.

See [V72_SPEC.md](./V72_SPEC.md) (admin 14 redirect),
[V81_SPEC.md](./V81_SPEC.md) (Metadata.controller_id hunt), and
[V38_SPEC.md](./V38_SPEC.md) (AddBroker forward).
