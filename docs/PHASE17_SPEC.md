# Phase 17 — Cooperative rebalance (binding)

## Goals

1. **Incremental handoff** on rebalance: keep fetch positions for partitions the
   member still owns; only OffsetFetch/reset for newly assigned partitions
2. **Revoked list** on JoinGroup response: partitions this member lost
   (`old − new`) when the join path can observe the prior assignment
3. Sticky assignor remains the default (Phase 11); cooperative builds on it
4. Docs honesty

## Non-goals

- Full Kafka cooperative-sticky two-phase protocol (`ConsumerPartitionAssignor`
  incremental revoke / assign epochs)
- Kafka sticky assignor wire bytes
- Stopping consumption mid-poll for a two-phase revoke barrier
- Transactions / Kafka shim / SCRAM / mTLS

## Semantics

### Eager (pre-Phase 17)

On any re-JoinGroup, `GroupConsumer` cleared **all** positions and OffsetFetched
every assigned partition — even partitions sticky retained.

### Cooperative (Phase 17)

On JoinGroup result with assignment `new` and prior local assignment `old`:

| Set | Action |
|-----|--------|
| `retained = old ∩ new` | Keep in-memory fetch positions |
| `added = new − old` | OffsetFetch (or 0 if unknown); start positions |
| `revoked = old − new` | Drop positions; stop fetching |

Broker sticky reassignment is unchanged. Generation bump / heartbeat error 9
still triggers re-JoinGroup.

### Broker `revoked` field

Each member tracks **`delivered`**: the assignment last returned on JoinGroup.
Current `assignment` updates on every rebalance; `delivered` updates only when
that member receives a JoinGroup response.

```
revoked = delivered − new_assignment
delivered := new_assignment
```

This covers both topic-subscription changes and re-sync after a peer join.
New members have empty `delivered` → empty `revoked`. Clients still compute
`old − new` locally for cooperative handoff (GroupConsumer does this).

## Protocol

JoinGroup **response** gains a trailing optional field (backward compatible):

```
… existing JoinGroup fields (error, generation, member_id, assignment) …
revoked_count: u32          # omitted in legacy payloads → 0
  for each:
    topic: string
    partition: u32
```

Encoders always write the trailer. Decoders treat missing trailer as empty
`revoked`.

## Client

- `JoinGroupResult.revoked: Vec<Assignment>`
- `GroupConsumer::do_join` applies cooperative position handoff
- Optional: expose last revoked list for debugging (`last_revoked()`)

## CLI

On group consume join, print `revoked=[...]` when non-empty (if exposed).

## Exit criteria

1. Solo member → second member joins → first re-joins: sticky-retained
   partitions keep prior fetch positions (no full clear)
2. JoinGroup response round-trips `revoked`; legacy response without trailer
   decodes as empty revoked
3. Broker unit: member that triggers reassign with prior partitions gets
   non-empty revoked for lost partitions when observable
4. `cargo test --workspace` green

## Honest limitations

- Not Kafka cooperative-sticky (no separate revoke/assign generations)
- No pause/resume barrier: revoke is applied at re-join, not mid-batch
- Broker revoked list is best-effort for the joining call; client local diff
  is authoritative for GroupConsumer
- Multi-topic sticky still Volant-local
