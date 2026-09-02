# v0.85 — SCRAM-admin / ListAcls NotController redirect on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V72_SPEC.md](./V72_SPEC.md):
admin error-**14** redirect wraps CreateTopic / DeleteTopic /
CreatePartitions / Reassign / CreateAcls / DeleteAcls only.
CreateScramUser / DeleteScramUser / ListScramUsers / ListAcls still
stay on the original connection.

Reuse the existing `_redirect_to_controller` / `redirectToController`
and `max_redirects` budget. Do **not** change the helper’s hunt
algorithm (that is v0.81).

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. Same loop as create_topic / create_acls: on error **14**
   (`BrokerError` / typed `error_code` / `ErrorResponse`), if attempts
   remain, call the existing controller redirect helper and retry the
   **same** RPC.
2. Parse `controller_id=` from any 14 message when present (existing
   helper).
3. Budget is the same as produce/fetch: `1 + max_redirects` (default
   `max_redirects=1`). `max_redirects=0` raises on the first 14.
4. Other errors (2 not found, etc.) still raise immediately.
5. No new public methods. Wrap:
   - `create_scram_user` / `delete_scram_user` / `list_scram_users`
   - `list_acls`
6. Do **not** wrap AddBroker/RemoveBroker, Describe/AlterConfigs,
   DeleteOffsets, ListOffsets, heartbeat.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (v0.81 sibling) |
| Metadata `controller_id` trailer use | Frozen (v0.81) |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| AddBroker / RemoveBroker redirect | Broker already forwards (v0.38) |
| Describe/AlterConfigs, offsets | Do not return 14 today |
| Heartbeat wrap | Already has its own retry (v0.74) |
| Broker / protocol / Rust client | Frozen (whether the broker returns 14 today) |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.72 admin:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- CreateScramUser may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`.
  ListAcls / DeleteScramUser / ListScramUsers may return typed
  responses with `error_code=14` and no id.

Hunt is unchanged (existing helper). Not a Metadata `controller_id`
field.

## API

No new public methods. Existing:

```python
c.create_scram_user("alice", "s3cret")
c.delete_scram_user("alice")
c.list_scram_users()
c.list_acls()
```

```go
c.CreateScramUser(...)
c.DeleteScramUser(...)
c.ListScramUsers()
c.ListAcls(...)
```

```java
c.createScramUser(...)
c.deleteScramUser(...)
c.listScramUsers()
c.listAcls(...)
```

Error 14 now follows Produce/Fetch redirect budget. Not Kafka
FindCoordinator. Hunt stays the v0.72 helper (v0.81 may change it).

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| CreateScramUser first 14 + `controller_id=2`; Metadata names node 2; second ok | success; one Metadata; two CreateScramUser |
| ListAcls typed 14 (no hint); Metadata has another broker; second ok | success |
| DeleteScramUser `max_redirects=0` + 14 | raise 14; no Metadata |
| ListScramUsers 14 then ok | success |

## Merge notes

Sibling slices **v0.81** / **v0.82** also edit `Client`. When merging:

- **Keep the four method wraps** (CreateScramUser / DeleteScramUser /
  ListScramUsers / ListAcls).
- Do not change `_redirect_to_controller` / `redirectToController`
  hunt logic (that is v0.81).
- Do not wrap AddBroker/RemoveBroker, Describe/AlterConfigs,
  DeleteOffsets, ListOffsets, heartbeat.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops or the v0.72
  six admin wraps.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is v0.81.
- Not Kafka `FindCoordinator`.
- AddBroker/RemoveBroker, Describe/AlterConfigs, and offset admin RPCs
  still do not redirect.
- No Kafka API keys / opcodes / Phase 155.

See [V72_SPEC.md](./V72_SPEC.md) (admin 14 redirect),
[V55_SPEC.md](./V55_SPEC.md) (SCRAM admin), and
[V56_SPEC.md](./V56_SPEC.md) (ACLs).
