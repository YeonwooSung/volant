# v0.39 — restore assignment if add-broker joint rolls back

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the v0.34 leftover “`VOLANT_REASSIGN_ON_ADD` still runs
inside `add_broker` before joint; a rolled-back add may leave replica
sets mentioning the dropped id.” When openraft metadata is on, this node
is the openraft leader, overlay rollback is on, **and** reassign-on-add
expanded (or would have written) assignment, a failed joint membership
change restores the pre-add `assignment.json` together with the overlay.

**Honesty:** this is **not** Kafka `AlterPartitionReassignments`, **not**
a live log copy, and **not** a majority wait on `MembershipPut`. No new
native opcodes. No Kafka API keys. Homemade `metadata_raft.rs` is
unchanged. In-process `Broker::add_broker` still persists assignment
first (same as v0.34 overlay).

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **off** | on: `1` / `true` / `yes` / `on`; off: unset / `0` / `false` / `no` / `off` |
| `VOLANT_OPENRAFT_JOINT_ROLLBACK` | **on** (when consulted) | off: `0` / `false` / `no` / `off` restores v0.26 overlay best-effort |
| `VOLANT_REASSIGN_ON_ADD` | **off** | on: `1` / `true` / `yes` / `on` expands under-replicated topics (v0.18) |
| `VOLANT_REASSIGN_ON_ADD_ROLLBACK` | **on** (when consulted) | off: `0` / `false` / `no` / `off` keeps the v0.18 assignment write even if overlay rolls back |

Assignment rollback is consulted only when **all** of the following hold:

1. `VOLANT_REASSIGN_ON_ADD` is on (otherwise no extra work).
2. `VOLANT_REASSIGN_ON_ADD_ROLLBACK` is not explicitly off.
3. v0.34 overlay rollback actually runs (openraft on, this node is the
   openraft leader, joint rollback armed).

When reassign-on-add is **off**, AddBroker is unchanged v0.34 (overlay
only). Existing v0.18 / v0.34 tests must pass without the new env.

## Path

```text
AddBroker (dispatch / after_overlay_mutation):
  snapshot overlay (v0.34)
  if reassign-on-add AND assignment-rollback on:
    snapshot live AssignmentSnapshot          # before add_broker
  persist membership.json + optional auto_reassign_after_add
  if flag on AND this node is openraft leader AND overlay rollback on:
    AddNodes(configured) + change_membership
    ok  → MembershipPut 100
    fail:
         restore previous overlay (v0.34)
         restore assignment snapshot (file + live apply)   # v0.39
         do not fan out the aborted overlay
         client error_code = 15
  else:
    MembershipPut 100
    change_membership best-effort
```

`restore_live_assignment` writes `{data_dir}/cluster/assignment.json`
and the in-memory snapshot, then `apply_local_assignment`. Restore is
skipped if another admin already advanced assignment generation (same
contract as the wait-path majority miss). RemoveBroker does not
reassign-on-add, so it passes no assignment snapshot.

In-process `Broker::add_broker` still expands replicas when the flag is
on (v0.18) and does **not** rewind them. Rollback is owned by the
**dispatch / client opcode** path so `v18_reassign` and
`v26_openraft_joint` stay as they are.

## Who restores assignment

| Caller | Behavior |
|--------|----------|
| Leader, openraft on, both rollbacks on, reassign on | Persist + expand → joint fail → overlay **and** assignment restored + **15** |
| Leader, `VOLANT_REASSIGN_ON_ADD_ROLLBACK=0` | Overlay still rolls back (v0.34); assignment keeps the new id |
| Reassign-on-add **off** | No assignment snapshot / restore (v0.34 overlay only) |
| Follower / openraft flag off / overlay rollback off | Unchanged v0.10 / v0.18 / v0.26 |
| Direct `add_broker` (no dispatch) | v0.18 expand stays; no assignment rewind |

## Tests

`crates/volant-broker/tests/v39_reassign_rollback.rs`:

1. Flag on openraft + `VOLANT_REASSIGN_ON_ADD=1`, create an
   under-replicated topic (N=3, default RF=4 → create RF=3), AddBroker
   id=4 via **TCP/dispatch**, `fail_next_change_membership` — overlay
   generation / broker list **and** assignment replicas do **not**
   include the new id. Client `error_code` is **15**.
2. Happy path add still expands replicas (existing v0.18 in-process
   behavior).
3. `v34_joint_rollback` still passes (overlay-only path unchanged).

```bash
cargo test -p volant-broker --test v39_reassign_rollback -- --test-threads=1
cargo test -p volant-broker --test v34_joint_rollback -- --test-threads=1
cargo test -p volant-broker --test v18_reassign -- --test-threads=1
```

## Non-goals

| Deferred | Why |
|----------|-----|
| Roll back a follower-accepted overlay / assignment | Peers already have a higher generation; ignore-if-stale cannot rewind |
| Auto-reassign on RemoveBroker | Unchanged v0.18 leftover |
| Live segment copy / replica rebuild | New replicas still start empty (v0.18) |
| Kafka AlterPartitionReassignments / new API keys | Shim frozen at 38 |
| Homemade 154 RequestVote / InstallSnapshot | Frozen |
| Phase 155 | Out of band |

## Honesty leftovers

- Follower add/remove still persists overlay immediately (v0.10 split-brain).
- Leader `MembershipPut` apply does not roll back overlay or assignment.
- `VOLANT_REASSIGN_ON_ADD_ROLLBACK=0` can still leave replica sets
  mentioning a dropped id after overlay rollback.
- Direct `add_broker` + `change_openraft_membership` (no dispatch) stays
  v0.18 / v0.26 best-effort (assignment and overlay both persist first).
- Restore skips rewind if another admin advanced assignment generation
  between the add write and the joint fail.
- Isolated controllers can each rewrite assignment (same split-brain as
  v0.10 / v0.18).
- New replicas start empty; `acks=all` does not wait for them until they
  join ISR.
