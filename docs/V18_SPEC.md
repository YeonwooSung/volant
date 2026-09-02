# v0.18 — Partition reassignment after add-broker

**Status:** Shipped (bounded MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the v0.10 leftover that **existing topic replicas are not
reassigned** when a broker is added. Native admin first; auto-on-add is
opt-in.

**Honesty:** this is **not** Kafka `AlterPartitionReassignments`, **not**
a live log copy, and **not** Raft joint consensus. New replicas start
**empty** (LEO=0) and catch up via existing ReplicaFetch / ISR expand
(Phase 118). Isolated controllers can still diverge (same as v0.10).

## Goals

1. **Native admin** `ReassignPartitions` — opcodes **114 / 115**
   (112/113 reserved for the openraft snapshot sibling). Not a Kafka
   API key (`SUPPORTED_APIS` stays 38).
2. **Apply** updates `assignment.json` generation + topic
   replica / ISR / leader. Brokers that newly gained a replica open a
   local partition via `apply_local_assignment`.
3. **Opt-in auto-reassign on AddBroker** via
   `VOLANT_REASSIGN_ON_ADD=1` (default **off**). Controller
   best-effort expands under-replicated partitions onto the new id.
4. **CLI:** `volant topic reassign --topic T [--partition P] [--replicas 1,2,3]`.

## Protocol

| Opcode | Direction | Name | Body |
|--------|-----------|------|------|
| **114** / **115** | client/admin | `ReassignPartitions` | `topic`, `partition:u32` (`u32::MAX` = all), `replicas[]` → `error_code`, `generation` (assignment) |

- Empty `replicas` → **auto**: recompute with the current effective
  broker list using the same `assign_replicas` as CreateTopic
  (`rf = min(default_replication_factor, N)`).
- Non-empty `replicas` → explicit set (dedup, preserve order). Applied
  to the named partition or to **every** partition when `partition ==
  u32::MAX`.
- Reject unknown topic (`NotFound`); reject replica ids not in
  membership (`InvalidArg`); reject an empty computed set
  (`InvalidArg`). Controller-only (`NotController`).

## Apply

For each updated partition:

- **Replicas** become the new list.
- **Leader** stays if still in the set; otherwise the first replica.
  Leader change bumps `leader_epoch`.
- **ISR** is the intersection of the old ISR with the new replica set
  (leader always first). Newly added replicas are **not** in ISR.
- Assignment generation is incremented and persisted to
  `{data_dir}/cluster/assignment.json`.
- `apply_local_assignment` opens an empty log on brokers that just
  gained a replica.

There is **no** live-copy of log segments. A new replica's LEO is 0
until ReplicaFetch catches up; ISR expand (Phase 118) adds it once
`LEO ≥ HWM` and lag is within the configured max.

## Auto-reassign on AddBroker

Default **off**. When `VOLANT_REASSIGN_ON_ADD` is `1` / `true` / `yes`
/ `on`, after a successful add the **controller** walks every
partition and, if:

```
new_id ∉ replicas  AND  unique(replicas) < min(default_rf, N)
```

it **appends** `new_id` (does not reshuffle existing replicas). Leader
and ISR are unchanged. Failures are logged; they do **not** roll back
the overlay add.

This only expands **under-replicated** topics. A topic created at
RF=`min(default_rf, N_old)` that already has that many unique replicas
is left alone (same as flag-off / v0.10). Example: N=2, default RF=3
→ create yields `{1,2}`; add id=3 with the flag on → `{1,2,3}`.
N=2, default RF=2 → create yields `{1,2}`; add does **not** expand
because `len == min(2, 3)`.

Use the explicit admin RPC to force a set such as `[1,2,3]`.

## CLI

```bash
volant topic reassign --topic events --replicas 1,2,3 --broker 127.0.0.1:9092
volant topic reassign --topic events --partition 0 --broker 127.0.0.1:9092
```

Omit `--replicas` for auto-place. Omit `--partition` to update every
partition of the topic.

## Non-goals

| Deferred | Why |
|----------|-----|
| Live segment copy / replica rebuild | Catch-up is ReplicaFetch only |
| Kafka AlterPartitionReassignments | Shim frozen at 38 API keys |
| Throttled / cancelable reassignment | MVP is one-shot assignment write |
| Auto-reassign on RemoveBroker | Would shrink RF / move leaders; later slice |
| Majority wait on the overlay add itself | Unchanged v0.10 best-effort `MembershipPut` |
| openraft / homemade Raft / new Kafka keys | Sibling slices; 112/113 reserved |

## Tests

`crates/volant-broker/tests/v18_reassign.rs`:

1. N=2, create topic RF=2; AddBroker id=3 **without** auto env →
   replicas stay `{1,2}`. Explicit `ReassignPartitions` `[1,2,3]`
   (or auto empty list) includes 3; generation bumped.
2. New replica broker opens a local partition; produce `acks=1` to
   the original leader still works.
3. Unknown topic → error; replica id not in membership → error.
4. Flag-off add-broker does not rewrite existing replica sets
   (v0.10 regression).

## Honesty leftovers

- New replicas start empty; `acks=all` does not wait for them until
  they join ISR.
- Auto-on-add is expand-only (append new id). It does **not**
  rebalance leaders or move replicas off old brokers.
- Any node may still accept AddBroker (v0.10); auto-reassign runs
  only on the controller that handled the add.
- No cancel / progress RPC. Concurrent reassigns last-write-wins
  (generation bump).
- Isolated controllers can each rewrite assignment (same split-brain
  as v0.10 overlay).
