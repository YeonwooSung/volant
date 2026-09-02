# v0.81 — language admin-14 prefers Metadata.controller_id

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V77_SPEC.md](./V77_SPEC.md) /
[V72_SPEC.md](./V72_SPEC.md): language `_redirect_to_controller` /
`redirectToController` still said “Native Metadata has no
controller_id” and hunted the first other advertised broker when the
14 message had no `controller_id=N` hint. v0.77 already added the
trailer; Python `MetadataResponse.controller_id`, Go
`codec.MetadataResponse.ControllerID`, Java `Metadata.controllerId`
exist (`0` = unknown). Rust already prefers the trailer after the
v0.77∩v0.79 splice.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. After `metadata()`, if the caller hint is None/null **and**
   `controller_id != 0`, use that id as the hint (same lookup: Metadata
   brokers, then `list_members` on miss).
2. Hint `0` / missing trailer still means unknown → first other
   advertised broker (unchanged).
3. Explicit message `controller_id=N` still wins over Metadata.
4. Update stale comments (“Native Metadata has no controller_id”).
5. No new public methods. Do **not** wrap new RPCs (that is v0.85). Do
   **not** add ListOffsets retry (v0.82).

Match Rust `redirect_to_controller` in
`crates/volant-client/src/client.rs`:

```
let controller_id = controller_id.or_else(|| {
    if meta.controller_id != 0 { Some(meta.controller_id) } else { None }
});
```

## Non-goals

| Deferred | Why |
|----------|-----|
| ListOffsets retry | Sibling v0.82; keep hunks local |
| Wrap new admin RPCs | Sibling v0.85 |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| AddBroker / RemoveBroker redirect | Broker already forwards (v0.38) |
| Describe/AlterConfigs, offsets | Do not return 14 today |
| Broker / protocol / Rust client | Already prefer the trailer |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.72:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.

Hunt order:

1. `controller_id=N` in the 14 Error message (CreateTopic
   `ErrorResponse`), when present.
2. Else Metadata trailer `controller_id` when non-zero.
3. Else first advertised broker whose host:port is not this connection.

`0` on the trailer (or a legacy payload with no trailer) is unknown,
not “node 0 is the controller.”

## API

No new public methods. Existing:

```python
c.create_topic("t", partitions=1)
c.delete_topic("t")
c.create_partitions("t", 2)
c.reassign_partitions("t", [1, 2])
c.create_acls([e])
c.delete_acls([e])
```

```go
c.CreateTopic(...)
c.DeleteTopic(...)
c.CreatePartitions(...)
c.ReassignPartitions(...)
c.CreateAcls(...)
c.DeleteAcls(...)
```

```java
c.createTopic(...)
c.deleteTopic(...)
c.createPartitions(...)
c.reassignPartitions(...)
c.createAcls(...)
c.deleteAcls(...)
```

Error 14 now prefers Metadata.controller_id when the message has no
hint. Not Kafka FindCoordinator.

## Tests

```bash
(cd clients/python && PYTHONPATH=src python3 -m unittest discover -s tests -q)
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| CreatePartitions typed 14 (no message hint); Metadata names controller_id=2 and node 2; second RPC ok | reconnects to node 2 (not merely “first other broker”) |
| Metadata controller_id=0 + 14 no hint | still first other advertised broker |
| Message `controller_id=3` + Metadata controller_id=2 | uses **3** |

The existing “no hint picks other broker” test (two brokers,
controller_id=0 on the fake Metadata) still passes.

## Files

| Path | Role |
|------|------|
| `clients/python/src/volant/client.py` | `_redirect_to_controller` prefers trailer |
| `clients/go/client.go` | `redirectToController` prefers trailer |
| `clients/java/src/main/java/io/volant/Client.java` | `redirectToController` prefers trailer |
| `clients/python/tests/test_admin_redirect.py` | three new fake-TCP cases |
| `clients/go/client_test.go` | same |
| `clients/java/src/test/java/io/volant/ClientTest.java` | same |
| `clients/python/README.md` | one-line admin-14 note |
| `clients/go/README.md` | one-line admin-14 note |
| `clients/java/README.md` | one-line admin-14 note |
| `docs/V81_SPEC.md` | This spec |

## Honesty leftovers

- Redirect still uses Metadata brokers (or ListMembers on a hinted id
  miss). Trailer `0` is unknown, not “never node 0.” Single-node
  `Broker::new` uses `node_id=0`, so 0 is also the real id.
- Not Kafka `FindCoordinator`. Not the Kafka Metadata `controller_id`
  tagged field.
- AddBroker/RemoveBroker, Describe/AlterConfigs, and offset admin RPCs
  still do not redirect.
- ListOffsets retry is v0.82. Wrapping new RPCs is v0.85.
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Siblings **v0.82** (ListOffsets retry) and **v0.85** (new RPC wraps)
also edit Client files. Keep this hunk local to the redirect helper +
its tests. Do not wrap ListOffsets or add public methods.

## Related

- [V72_SPEC.md](./V72_SPEC.md) — language admin NotController redirect (hunt)
- [V77_SPEC.md](./V77_SPEC.md) — Metadata controller_id trailer
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin 14 (already prefers trailer)
