# v0.34 — roll back overlay if openraft joint fails

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the v0.26 honesty leftover “overlay is SoT;
`change_membership` fail does **not** roll back `membership.json` (client
still succeeds).” When openraft metadata is on **and** this node is the
openraft leader, a failed joint membership change restores the previous
overlay so configured N does not increase (or shrink) without a matching
voter set.

**Honesty:** this is **not** KRaft voter reconfig and **not** a majority
wait on `MembershipPut`. Homemade `metadata_raft.rs` is unchanged. No new
native opcodes (reuse 102–107 + 108–113). No Kafka API keys.

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **off** | on: `1` / `true` / `yes` / `on`; off: unset / `0` / `false` / `no` / `off` |
| `VOLANT_OPENRAFT_JOINT_ROLLBACK` | **on** (when consulted) | off: `0` / `false` / `no` / `off` restores v0.26 best-effort; unset / any other value keeps rollback |

Rollback is consulted only when `VOLANT_OPENRAFT_METADATA` is on **and**
this node is the openraft leader (`controller_id()`). When the openraft
flag is **off**, add/remove is unchanged v0.10 (overlay + best-effort
`MembershipPut` 100). Existing v0.10 / v0.26 tests must pass without the
new env.

Runtime setter: `Broker::set_openraft_joint_rollback` (tests / live).
Test hook: `Broker::fail_next_change_membership` forces the next leader
`change_membership` to fail without waiting 5s.

## Client error

Joint fail on the leader returns native **`NotEnoughReplicas` (15)** on
the typed `AddBroker` / `RemoveBroker` response (`error_code != 0`,
`generation` is the restored overlay generation). Same family as
assignment wait-path majority miss. Not `InvalidArg` (3): that stays
validation (duplicate id, remove self, last broker / last voter).

## Path

```text
AddBroker / RemoveBroker:
  snapshot overlay (generation + brokers + file + voter hook)
  persist {data_dir}/cluster/membership.json     # v0.10 write
  if flag on AND this node is openraft leader AND rollback on:
    AddNodes(configured) + change_membership     # joint → uniform, 5s
    ok  → MembershipPut 100 (after commit)
    fail (timeout / error / not-leader from raft / test hook):
         restore previous overlay file
         restore in-memory config + membership_generation
         restore last voter-set hook
         apply_configured_ids (re-added id is not live)
         do not fan out the aborted overlay
         client error_code = 15
  else:
    MembershipPut 100                            # v0.10 / v0.26
    change_membership best-effort                # no rollback
```

In-process `Broker::add_broker` / `remove_broker` still persist first
(v0.26). Rollback is owned by the **dispatch / client opcode** path so
`v26_openraft_joint` (add then `change_openraft_membership`) stays
best-effort.

## Who rolls back

| Caller | Behavior |
|--------|----------|
| Leader, openraft on, rollback on | Persist → joint → restore + **15** on fail |
| Leader, `VOLANT_OPENRAFT_JOINT_ROLLBACK=0` | v0.26: persist + fan-out; joint fail is log-only; client **0** |
| Follower (not openraft leader) | Today’s overlay + `MembershipPut`. Follower cannot `change_membership`. |
| Openraft flag off | Unchanged v0.10 |

`MembershipPut` apply on a leader still attempts the same voter sync
(v0.26) and does **not** roll back an overlay that a follower already
accepted (higher generation would not rewind peers).

## Tests

`crates/volant-broker/tests/v34_joint_rollback.rs`:

1. Flag off — AddBroker writes overlay (v0.10).
2. Flag on, happy path — in-process add still writes overlay (v0.26).
3. Flag on, `fail_next_change_membership` — overlay generation / broker
   list unchanged; client `error_code` is **15**.

Also keep `v26_openraft_joint` and `v10_dynamic_membership` green.

```bash
cargo test -p volant-broker --test v34_joint_rollback -- --test-threads=1
cargo test -p volant-broker --test v26_openraft_joint -- --test-threads=1
cargo test -p volant-broker --test v10_dynamic_membership -- --test-threads=1
```

## Non-goals

| Deferred | Why |
|----------|-----|
| Roll back a follower-accepted overlay on leader `MembershipPut` | Peers already have a higher generation; ignore-if-stale cannot rewind |
| Restore live/heartbeat for a re-added id after remove rollback | Spec restores file + config + generation |
| Learner-only staging with catch-up wait | Unchanged v0.26 leftover |
| Kafka DescribeCluster / new API keys | Shim frozen at 38 |
| Homemade 154 RequestVote / InstallSnapshot | Frozen |
| Phase 155 | Out of band |

## Honesty leftovers

- Follower add/remove still persists overlay immediately (v0.10 split-brain).
- Leader `MembershipPut` apply does not roll back.
- `VOLANT_REASSIGN_ON_ADD` still runs inside `add_broker` before joint;
  a rolled-back add may leave replica sets mentioning the dropped id.
- AddNodes may install a learner before `ReplaceAllVoters` fails; overlay
  rolls back, the learner record may remain until a later membership change.
- Direct `add_broker` + `change_openraft_membership` (no dispatch) stays
  v0.26 best-effort.
- `change_membership` wait is still 5s on the real (non-hook) fail path.
