# v0.72 — admin NotController redirect on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V65_SPEC.md](./V65_SPEC.md):
“Other admin RPCs still do not redirect.” Produce/Fetch/DeleteRecords
already redirect on error **13** (`NotLeaderForPartition`) via Metadata
partition leader. Controller-gated admin returns native **14**
(`NotController`).

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client. Native Metadata has **no**
`controller_id` field; this slice does not add one.

## Goals

1. Add a shared helper `_redirect_to_controller` /
   `redirectToController` next to the existing leader redirect:
   - If a `controller_id` hint is known (parsed from
     `BrokerError.message` via `controller_id=(\d+)`), look that node
     up in existing `metadata()` brokers (or `list_members()` if
     Metadata has no matching id).
   - Else pick the first advertised broker whose host:port is not the
     current connection.
   - Reconnect (re-run Auth/SCRAM like today’s leader redirect).
   - Return false if no other broker / lookup miss / empty host /
     reconnect fail.
2. Budget is the same as produce/fetch: `1 + max_redirects` (default
   `max_redirects=1`). `max_redirects=0` raises on the first 14.
3. Apply the loop to these existing methods (no new public APIs):
   - `create_topic` / `delete_topic` (14 often arrives as
     `ErrorResponse` / `BrokerError` from `_round_trip`; message may
     include `controller_id=`)
   - `create_partitions`
   - `reassign_partitions`
   - `create_acls` / `delete_acls`
4. Other errors (2 not found, 15 NotEnoughReplicas, etc.) still raise
   immediately.
5. Do **not** wrap AddBroker/RemoveBroker (broker already forwards,
   v0.38).
6. Do **not** wrap Describe/AlterConfigs, DeleteOffsets, ListOffsets,
   OffsetCommit — those do not return 14 today.
7. Parse `controller_id=` from **any** 14 `BrokerError.message` when
   present; ignore junk.

## Non-goals

| Deferred | Why |
|----------|-----|
| Metadata `controller_id` trailer | Frozen (broker / protocol / Rust client) |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| AddBroker / RemoveBroker redirect | Broker already forwards (v0.38) |
| Describe/AlterConfigs, offsets | Do not return 14 today |
| Heartbeat / JoinGroup wrap | Sibling v0.74; keep hunks local |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- CreateTopic on a follower returns
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`.
  CreatePartitions / ReassignPartitions / CreateAcls / DeleteAcls
  return typed responses with `error_code=14` and no id.

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

Error 14 now follows Produce/Fetch redirect budget. Not Kafka
FindCoordinator. Native Metadata has no `controller_id`.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| CreateTopic first reply 14 + `controller_id=2`; Metadata names node 2; second CreateTopic ok | success; one Metadata; two CreateTopic |
| CreatePartitions first 14 (typed, no hint); Metadata has another broker; second ok | success |
| `max_redirects=0` + 14 | raise 14; no Metadata |
| Helper cannot find other broker / empty host | raise original 14 |
| CreateAcls 14 then ok after redirect | success |
| ReassignPartitions 14 then ok | success |

## Merge notes

Sibling slice **v0.74** also edits `Client` (heartbeat retry). When
merging:

- **Keep the new helper + the six admin method wraps.**
- Do not wrap `heartbeat` / `join_group`.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- Redirect still uses Metadata brokers (or ListMembers on a hinted id
  miss). Native Metadata has no `controller_id`.
- Not Kafka `FindCoordinator`.
- AddBroker/RemoveBroker, Describe/AlterConfigs, and offset admin RPCs
  still do not redirect.
- No Kafka API keys / opcodes / Phase 155.

See [V43_SPEC.md](./V43_SPEC.md) (Produce/Fetch redirect),
[V65_SPEC.md](./V65_SPEC.md) (DeleteRecords redirect), and
[V38_SPEC.md](./V38_SPEC.md) (AddBroker forward).
