# v0.16 — openraft SetAssignment log apply (MVP)

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** When `VOLANT_OPENRAFT_METADATA=1`, the openraft leader replicates
assignment mutations (`SetAssignment`) so followers install topics without
homemade 150/154. v0.11 remains election-only when the write path is off.

**Honesty:** this is **not** full KRaft, **not** InstallSnapshot (sibling
v0.17), and **not** a delete of homemade `metadata_raft.rs` / Phase 150
notes. The openraft log store stays **in-memory**. Applied assignment is
also written to `{data_dir}/cluster/assignment.json` (existing helper).
No new native opcodes; reuse 108/109 (`client_write` / AppendEntries).

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **off** | on: `1` / `true` / `yes` / `on`; off: unset / `0` / `false` / `no` / `off` |

When **off**, CreateTopic / DeleteTopic / CreatePartitions are unchanged
(lowest-id controller; 150 notes or 154 if that flag is on). Existing
tests must pass without the env.

When **on** and this node is the openraft leader (`controller_id()`), a
successful local assignment mutation also `raft.client_write(SetAssignment)`
and waits for local apply (5s timeout). Followers apply via AppendEntries
(108/109) and install the snapshot.

## What is / is not replaced

| Replaced when flag on | Still present |
|-----------------------|---------------|
| Assignment **apply** after CreateTopic / DeleteTopic / CreatePartitions | Homemade 154 AppendEntries 98/99 (code stays) |
| Follower install of topics via openraft log | Phase 150 `AssignmentConsensusNote` (used when openraft flag off) |
| `controller_id()` / `is_controller()` already from v0.11 | In-memory openraft log / vote (no `__metadata_raft` persist) |
| | InstallSnapshot (v0.17) |
| | Per-partition data Raft |
| | Kafka API keys / Phase 155 |

When **both** `VOLANT_METADATA_RAFT` and `VOLANT_OPENRAFT_METADATA` are on,
**prefer openraft** for assignment apply (skip 154 fan-out for that
mutation). Typical tests set only the openraft flag.

## Mutation + honesty

Same sites that write `assignment.json` (native + Kafka CreateTopics /
DeleteTopics / CreatePartitions) call `complete_assignment_mutation`.

| Wait / committed-only | Write/apply fail |
|----------------------|------------------|
| **on** | Reuse existing rollback (`restore_live_assignment`) and client **15** / Kafka **19** `NotEnoughReplicas`. No new error family. |
| **off** (default) | Best-effort: still `client_write` + wait-for-apply with timeout, but the client succeeds from the local `assignment.json` write. |

CreateTopic / CreatePartitions on a non-controller still fail with
`NotController`. The inner gate uses `Broker::is_controller()` (openraft
leader when the flag is on; lowest-id otherwise).

## Apply

`MetaRequest::SetAssignment { generation, topics }` (full snapshot, same
shape as 154). `apply()`:

1. Updates last_applied / membership (as v0.11).
2. For `SetAssignment`, `Broker::apply_cluster_state` → `save_assignment`
   + `apply_local_assignment` via a `Weak<Broker>` registered at raft start.

Followers that never handled the admin RPC still get the topic (or lose it
on DeleteTopic) in **live assignment**.

Leader abort after a committed CreateTopic: survivors who applied the log
keep the topic in memory (and `assignment.json`). A new leader among those
survivors still has it. A node that never applied, or a full process
restart, has **no** InstallSnapshot catch-up (v0.17).

## Protocol

No new opcodes. `SetAssignment` rides inside existing openraft
AppendEntries JSON (108/109). Vote remains 110/111. InstallSnapshot is
still unimplemented on the network.

## Tests

`crates/volant-broker/tests/v16_openraft_apply.rs`:

1. Default off — lowest-id controller; CreateTopic does not need openraft.
2. Flag on, 3-node — CreateTopic to the openraft leader; all three have the
   topic in live assignment (timeout loop).
3. Flag on — DeleteTopic on the leader removes the topic from a follower.
4. Leader abort after committed CreateTopic — new leader still has the topic.

Also keep `v11_openraft_election` green.

## Non-goals

- RequestVote / InstallSnapshot on homemade `metadata_raft.rs`
- Persisting the openraft log next to `__metadata_raft` (optional later)
- Removing 150/154 code paths
- Kafka API keys
- Phase 155
