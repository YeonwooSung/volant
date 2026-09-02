# v0.79 — Rust admin NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V72_SPEC.md](./V72_SPEC.md):
Python / Go / Java already redirect controller-gated admin on error
**14** (`NotController`). Rust `volant-client` already redirects
Produce/Fetch (and DeleteRecords if wired) on error **13**, but
CreateTopic on a follower is still
`Response::Error { code: 14, message: "not controller; controller_id=N" }`
with no reconnect. CreatePartitions / ReassignPartitions / CreateAcls /
DeleteAcls return typed `error_code=14`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / language clients. Native Metadata has
**no** `controller_id` field; this slice does not add one (that is
v0.77, parallel).

## Goals

1. Add a helper `redirect_to_controller` next to `redirect_to_leader`:
   - Parse `controller_id=(\d+)` from any 14 error message when present.
   - Look that node up in `metadata().brokers` (`BrokerInfo.node_id` /
     host / port). If miss, `list_members()` overlay brokers.
   - Else pick the first advertised broker whose `host:port` is not the
     current connection (`current_addr()`).
   - `reconnect` (must re-run Auth/SCRAM like leader redirect).
   - Return false if no other broker / empty host / reconnect fail.
2. Budget `1 + config.max_redirects`. `max_redirects=0` does not
   redirect (no Metadata).
3. Wrap (no new public APIs):
   - `create_topic` / `create_topic_with_configs` / `delete_topic`
   - `create_partitions`
   - `reassign_partitions`
   - `create_acls` / `delete_acls`
4. Other errors (2 not found, 15 NotEnoughReplicas, etc.) still fail
   immediately.
5. Do **not** wrap AddBroker/RemoveBroker (broker already forwards,
   v0.38).
6. Do **not** wrap Describe/AlterConfigs, DeleteOffsets, OffsetCommit,
   heartbeat.

## Non-goals

| Deferred | Why |
|----------|-----|
| Metadata `controller_id` trailer | Frozen (v0.77, parallel) |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| AddBroker / RemoveBroker redirect | Broker already forwards (v0.38) |
| Describe/AlterConfigs, offsets | Do not return 14 today |
| Heartbeat wrap | Sibling v0.80; keep hunks local |
| Language clients | Already have this (v0.72) |
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

Hunt is the message hint or the first other advertised broker. Not a
Metadata `controller_id` field.

## API

No new public methods. Existing:

```rust
c.create_topic("t", 1).await?;
c.create_topic_with_configs("t", 1, configs).await?;
c.delete_topic("t").await?;
c.create_partitions("t", 2).await?;
c.reassign_partitions("t", None, &[1, 2]).await?;
c.create_acls(entries).await?;
c.delete_acls(entries).await?;
```

Error 14 now follows Produce/Fetch redirect budget. Not Kafka
FindCoordinator. Native Metadata has no `controller_id`.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Tokio TCP stub (no live broker):

| Case | Expect |
|------|--------|
| CreateTopic first 14 + `controller_id=2`; Metadata names node 2; second ok | success; one Metadata; two CreateTopic |
| CreatePartitions 14 (typed, no hint); Metadata has another broker; second ok | success |
| `max_redirects=0` + 14 | error 14; no Metadata |
| Helper cannot find other broker | raise original 14 |
| CreateAcls 14 then ok after redirect | success |
| Other error (2) | fail immediately; no Metadata |

## Merge notes

Sibling slices also edit `client.rs`:

- **v0.77** Metadata `controller_id` trailer
- **v0.80** heartbeat retry

When merging:

- **Keep the new helper + the six admin method wraps.**
- Do not wrap `heartbeat` / `join_group`.
- Do not add a Metadata `controller_id` field on this slice.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops.
- Do not change the broker, Kafka shim, or language clients.

## Honesty leftovers

- Redirect still uses Metadata brokers (or ListMembers on a hinted id
  miss). Native Metadata has no `controller_id` on this slice.
- Not Kafka `FindCoordinator`.
- AddBroker/RemoveBroker, Describe/AlterConfigs, and offset admin RPCs
  still do not redirect.
- No Kafka API keys / opcodes / Phase 155.

See [V72_SPEC.md](./V72_SPEC.md) (language admin 14),
[V43_SPEC.md](./V43_SPEC.md) (Produce/Fetch redirect),
[V65_SPEC.md](./V65_SPEC.md) (DeleteRecords redirect), and
[V38_SPEC.md](./V38_SPEC.md) (AddBroker forward).
