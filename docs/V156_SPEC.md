# v0.156 — language Metadata NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V95_SPEC.md](./V95_SPEC.md) /
[V134_SPEC.md](./V134_SPEC.md): language `metadata` / `Metadata`
retries transient 6 / 7 / 15 / 16 (v0.95) but treats **14**
(`NotController`) as not redirected. Native Metadata has no
top-level `error_code`; 14 arrives as Error opcode / BrokerError.
Heartbeat already redirects on 14 (v0.134). Same honesty: the
broker may not return 14 on Metadata today; this is client-side
wrap only. Rust is sibling **v0.157**.

Reuse `_redirect_to_controller` / `redirectToController` (v0.81
hunt). Keep existing `max_retries` for 6 / 7 / 15 / 16. 14 is
**not** a transient retry.

Hunt currently calls public `metadata()` / `Metadata()`. Wrapping
that public path on 14 without a no-14 helper would recurse. Extract
a private helper (same honesty as ListMembers v0.121) and point hunt
at it.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. Extract the current Metadata send loop into a **private no-14
   helper**:
   - Python `_metadata_rpc(topics)`
   - Go `metadataRpc(topics)`
   - Java `metadataRpc(List<String> topics)`
   The helper keeps transient retry (v0.95). It does **not** wrap 14.
2. Public `metadata` / `Metadata` / `MetadataTopics` /
   `metadata(topics)` wrap 14: if `error_code == 14` or `BrokerError`
   14 and redirect attempts remain (`1 + max_redirects`), call the
   existing controller redirect helper and retry the **same**
   Metadata.
3. Parse `controller_id=` from any 14 message when present (existing
   helper).
4. Budget is the same as produce/fetch: `1 + max_redirects` (default
   `max_redirects=1`). `max_redirects=0` raises on the first 14
   (hunt does not send a second Metadata).
5. Transient 6 / 7 / 15 / 16 still use `max_retries` (already shipped).
   14 is **not** a transient retry — it uses the redirect budget only.
6. **Critical:** `_redirect_to_controller` / `redirectToController`
   must call the **no-14 helper**, not public `metadata()`. Hunt
   already uses `_list_members_rpc` on id miss (v0.81 / v0.121).
7. **Not redirected / not retried:** 13, 2, 9 / 10 / 11, 17 / 18,
   21, 22, Protocol.
8. No new public methods. Existing Metadata signatures stay.
9. Do **not** wrap JoinGroup.
10. Do **not** change hunt algorithm beyond pointing it at the no-14
    helper.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (v0.81 already shipped); only the Metadata call site |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| JoinGroup wrap | Frozen (siblings v0.131–v0.133) |
| Broker / protocol / Rust client | Frozen (whether the broker returns 14 today); Rust is v0.157 |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.72 / v0.134 Heartbeat:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; hunt does not send a
  second Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- Metadata 14 arrives as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`
  (native Metadata has no top-level `error_code`).
- Transient 6 / 7 / 15 / 16 (and transport) still sleep
  `retry_backoff_ms` and retry up to `max_retries` extra times.
  Independent of `max_redirects`.

Hunt is unchanged except the Metadata call: existing helper +
v0.81 Metadata.controller_id, using `_metadata_rpc` /
`metadataRpc` so redirect and metadata are not recursive.

## API

No new public methods. Existing:

```python
c.metadata()
c.metadata(["events"])
```

```go
c.Metadata()
c.MetadataTopics([]string{"events"})
```

```java
c.metadata();
c.metadata(List.of("events"));
```

Error 14 now follows Produce/Fetch redirect budget. Transient 6 / 7 /
15 / 16 still follow `max_retries`. Not Kafka FindCoordinator.

## Tests

```bash
# from clients/python (full discover hangs)
PYTHONPATH=src python3 -m unittest tests.test_client -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Next to existing Metadata retry tests:

| Case | Expect |
|------|--------|
| First Metadata 14 + `controller_id=2`; Metadata names node 2; second ok | success |
| `max_redirects=0` + first 14 | raise 14; hunt does not send a second Metadata |
| Existing: `max_retries=2`, first **7** then 0 | two Metadatas, no 14 path |

Do **not** append codec tests. Metadata 14 arrives as Error opcode
(`ErrorResponse`).

| File | What |
|------|------|
| `clients/python/src/volant/client.py` | 14 wrap + `_metadata_rpc` hunt |
| `clients/go/client.go` | 14 wrap + `metadataRpc` hunt |
| `clients/java/src/main/java/io/volant/Client.java` | 14 wrap + `metadataRpc` hunt |
| `clients/python/tests/test_client.py` | queued-code stub |
| `clients/go/client_test.go` | queued-code stub |
| `clients/java/src/test/java/io/volant/ClientTest.java` | queued-code stub |
| `docs/V156_SPEC.md` | This spec |

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is v0.81. The hunt calls a
  private no-14 Metadata path so `metadata` and the helper are not
  mutually recursive.
- Not Kafka `FindCoordinator`.
- Native Metadata has no top-level `error_code`. 14 is Error opcode
  / BrokerError only. The broker may not return 14 on Metadata today;
  this slice is client-side wrap only (same honesty as Heartbeat 14).
- JoinGroup is not wrapped here.
- Rust Metadata 14 is sibling **v0.157**.
- No Kafka API keys / opcodes / Phase 155.

See [V95_SPEC.md](./V95_SPEC.md) (Metadata transient retry),
[V134_SPEC.md](./V134_SPEC.md) (Heartbeat error 14),
[V121_SPEC.md](./V121_SPEC.md) (ListMembers 14 + no-14 hunt helper),
[V72_SPEC.md](./V72_SPEC.md) (admin NotController redirect), and
[V81_SPEC.md](./V81_SPEC.md) (Metadata.controller_id hunt).

## Merge notes

Sibling slices that also edit `Client` should keep this hunk local to
Metadata + the hunt helper call:

- **Keep the 14 wrap around `_metadata_rpc`**. Do not drop the v0.95
  transient retry.
- Do **not** change hunt algorithm beyond pointing
  `_redirect_to_controller` / `redirectToController` at the no-14
  helper.
- Do not wrap JoinGroup.
- Do not change the broker, Kafka shim, or Rust client in this merge.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`metadata` /
  `_redirect_to_controller`)
- Go `clients/go/client.go` (`Metadata` / `MetadataTopics` /
  `redirectToController`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`metadata` / `redirectToController`)
- Scripted brokers in `test_client.py` / `client_test.go` /
  `ClientTest.java`
