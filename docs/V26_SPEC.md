# v0.26 — openraft joint membership on add/remove broker

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** When `VOLANT_OPENRAFT_METADATA=1`, overlay `AddBroker` /
`RemoveBroker` also proposes an **openraft joint** membership change so
the voter set tracks the configured broker list.

**Honesty:** this is **not** KRaft voter reconfig, **not** automatic
replica move, and **not** a rollback of the v0.10 overlay. Overlay
`{data_dir}/cluster/membership.json` remains SoT. `change_membership` is
**best-effort**: wait up to 5s; on fail, log and keep the overlay (client
still succeeds). Homemade `metadata_raft.rs` is unchanged. No new native
opcodes (reuse 108–113). No Kafka API keys.

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **off** | on: `1` / `true` / `yes` / `on`; off: unset / `0` / `false` / `no` / `off` |

When **off**, add/remove is unchanged v0.10 (overlay + best-effort
`MembershipPut` 100). Existing v0.10 / v0.11 tests must pass without the
env.

When **on** and this node is the openraft leader (`controller_id()`):

1. After a successful overlay `add_broker`, `change_membership` to the
   **new** voter set (all configured broker ids).
2. After a successful overlay `remove_broker`, `change_membership`
   without that id.
3. Reject removing the last remaining overlay broker (v0.10) and the
   last openraft voter.

A new process that boots with an overlay that includes itself still
starts openraft (v0.11). The existing leader adds it as a voter
(learner-then-voter: `AddNodes` then `ReplaceAllVoters`; openraft 0.9
requires a Node record before `ReplaceAllVoters`).

## Path

```text
AddBroker / RemoveBroker (any node):
  overlay persist {data_dir}/cluster/membership.json   # v0.10 SoT
  note last change_membership target (flag on)
  best-effort MembershipPut 100
  if flag on AND this node is openraft leader:
    AddNodes(configured)          # learner records
    change_membership(configured) # joint → uniform, 5s wait
    fail → log; do not roll back overlay; client error_code=0
```

`MembershipPut` apply on a leader also attempts the same sync so a
follower-accepted add can still update voters when the put arrives.

## What is / is not replaced

| Replaced when flag on | Still present |
|-----------------------|---------------|
| openraft voter set after add/remove (joint) | v0.10 overlay file + opcodes 102–107 |
| | Best-effort MembershipPut 100 (no majority wait) |
| | Homemade 154 / lowest-id when flag off |
| | Kafka `SUPPORTED_APIS` (38) |
| | Automatic replica reassignment (v0.18 opt-in only) |

## Split-brain / last voter

Any node may still accept overlay add/remove (v0.10). Only the **openraft
leader** proposes `change_membership`. A follower-accepted add updates
the overlay immediately; raft voters catch up if the leader applies
`MembershipPut` (or an admin retries on the leader).

Remove of self and of the last remaining overlay broker stay rejected.
When the flag is on, remove of the sole current openraft voter is also
rejected (`cannot remove the last voter`).

## Tests

`crates/volant-broker/tests/v26_openraft_joint.rs`:

1. Flag off — AddBroker writes overlay; no `change_membership` target.
2. Flag on, 3-node — AddBroker id=4 (endpoint only). After timeout,
   leader `openraft_voter_ids` includes 4 **or** the test hook
   `test_last_openraft_membership_target` contains 4.
3. Flag on — add 4 then remove 4 shrinks the voter set / hook.

Also keep `v10_dynamic_membership` and `v11_openraft_election` green.

## Non-goals

| Deferred | Why |
|----------|-----|
| Roll back overlay on `change_membership` fail | Overlay is SoT; no wait-on for membership |
| Learner-only staging with catch-up wait | Unreachable new id would block; voter-direct after `AddNodes` |
| Kafka DescribeCluster / new API keys | Shim frozen at 38 |
| Homemade 154 RequestVote / InstallSnapshot | Frozen |
| Phase 155 | Out of band |
