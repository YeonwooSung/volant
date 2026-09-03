# v0.134 — Heartbeat NotController redirect on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V74_SPEC.md](./V74_SPEC.md) /
[V124_SPEC.md](./V124_SPEC.md): language `heartbeat` already retries
transient 6 / 7 / 15 / 16 (v0.74) but treats **14** (`NotController`)
as not retried. DescribeGroup already redirects on 14 (v0.124). Same
honesty: the broker may not return 14 on Heartbeat today; this is
client-side wrap only.

Reuse `_redirect_to_controller` / `redirectToController` (v0.81 hunt).
Keep existing `max_retries` for 6 / 7 / 15 / 16. 14 is **not** a
transient retry.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. On Heartbeat, if `error_code == 14` or `BrokerError` 14: if
   redirect attempts remain (`1 + max_redirects`), call the existing
   controller redirect helper and retry the **same** Heartbeat.
2. Parse `controller_id=` from any 14 message when present (existing
   helper).
3. Budget is the same as produce/fetch: `1 + max_redirects` (default
   `max_redirects=1`). `max_redirects=0` raises on the first 14
   (no Metadata).
4. Transient 6 / 7 / 15 / 16 still use `max_retries` (already shipped).
   14 is **not** a transient retry — it uses the redirect budget only.
5. **Not redirected / not retried:** 13, 2, 9 / 10 / 11, 17 / 18,
   21, 22, Protocol. Rebalance 9 / 10 / 11 still surface immediately
   so GroupConsumer can rejoin.
6. No new public methods. Wrap `heartbeat` / `Heartbeat` /
   `heartbeat`. Python still returns `error_code`.
7. Do **not** wrap JoinGroup (siblings v0.131–v0.133) or Produce.
8. Do **not** change `_redirect_to_controller` / `redirectToController`
   or `_admin_round_trip`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (v0.81 already shipped) |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| JoinGroup / Produce wrap | Siblings v0.131–v0.133 / other residuals |
| Broker / protocol / Rust client | Frozen (whether the broker returns 14 today) |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.72 / v0.124 DescribeGroup:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- Heartbeat may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`
  or a typed response with `error_code=14` and no id.
- Transient 6 / 7 / 15 / 16 (and transport) still sleep
  `retry_backoff_ms` and retry up to `max_retries` extra times.
  Independent of `max_redirects`.
- Error **9** / **10** / **11** still raise immediately; not a retry
  and not a redirect.

Hunt is unchanged (existing helper + v0.81 Metadata.controller_id).

## API

No new public methods. Existing:

```python
c.heartbeat("g", "m1", 1)  # still returns error_code
```

```go
c.Heartbeat("g", "m1", 1)
```

```java
c.heartbeat("g", "m1", 1);
```

Error 14 now follows Produce/Fetch redirect budget. Transient 6 / 7 /
15 / 16 still follow `max_retries`. Not Kafka FindCoordinator.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Next to existing Heartbeat retry tests:

| Case | Expect |
|------|--------|
| First Heartbeat 14 + `controller_id=2`; Metadata names node 2; second ok | success |
| Typed 14 (no hint); Metadata has another broker; second ok | success |
| `max_redirects=0` + first 14 | raise 14; no Metadata |
| Existing: `max_retries=2`, first **7** then 0 | two Heartbeats, no Metadata |
| Rebalance **9** with `max_retries=2` | immediately 9 |

## Merge notes

Sibling slices **v0.131–v0.133** (JoinGroup / Produce) also edit
`Client`. When merging:

- **Keep the Heartbeat wrap** (14 redirect + existing transient
  retry). Do not drop the v0.74 transient retry.
- Do not change `_redirect_to_controller` / `redirectToController`
  hunt logic (that is v0.81).
- Do not wrap JoinGroup or Produce in this merge.
- Do not change `_admin_round_trip` or InitProducerId.
- Do not change the broker, Kafka shim, or Rust client in this merge.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`heartbeat`)
- Go `clients/go/client.go` (`Heartbeat`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`heartbeat`)
- Scripted brokers in `test_client.py` / `client_test.go` /
  `ClientTest.java`

The hunk is local to `heartbeat` / `Heartbeat`.

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is v0.81.
- Not Kafka `FindCoordinator`.
- Heartbeat is often group-local; the broker may not return 14
  today. This slice is client-side wrap only (same honesty as
  DescribeGroup 14).
- JoinGroup and Produce are not wrapped here.
- No Kafka API keys / opcodes / Phase 155.

See [V74_SPEC.md](./V74_SPEC.md) (Heartbeat transient retry),
[V124_SPEC.md](./V124_SPEC.md) (DescribeGroup / ListGroups error 14),
[V72_SPEC.md](./V72_SPEC.md) (admin 14 redirect), and
[V81_SPEC.md](./V81_SPEC.md) (Metadata.controller_id hunt).
