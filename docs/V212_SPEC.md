# v0.212 — persist membership overlay after openraft joint commit

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “overlay is SoT; `change_membership` runs
against an already-written `membership.json`.” When cluster +
`VOLANT_OPENRAFT_METADATA` is on, the **leader** dispatch path for
AddBroker / RemoveBroker must **not** write `{data_dir}/cluster/membership.json`
before `change_membership` commits. Persist overlay **after** a
successful joint. Fail → native **15**, disk unchanged.

This is the first half of “overlay is no longer SoT.” Sibling **v0.216**
will write overlay from StateMachine apply on followers. This slice is
**leader persist-after-commit** only.

**Honesty:** this is **not** KRaft voter reconfig and **not** a majority
wait on `MembershipPut`. Homemade `metadata_raft.rs` is unchanged. No new
native opcodes (reuse 102–107 + 108–113). No Kafka API keys. No
RequestVote on homemade 154.

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **off** (tests pin `0` unless set) | on: `1` / `true` / `yes` / `on`; off: unset / `0` / `false` / `no` / `off` |
| `VOLANT_OPENRAFT_JOINT_ROLLBACK` | **on** (when consulted) | off: `0` / `false` / `no` / `off` restores persist-first + v0.26 best-effort |

The invert is consulted only when `VOLANT_OPENRAFT_METADATA` is on,
this node is the openraft leader (`controller_id()`), rollback is
armed, **and** configured N≥2. When the openraft flag is **off**,
N<2, or there is no cluster, add/remove is unchanged v0.10
(persist-first + best-effort `MembershipPut` 100). Existing
`v10_dynamic_membership` must pass without the new env.

## Client error

Joint fail on the leader returns native **`NotEnoughReplicas` (15)** on
the typed `AddBroker` / `RemoveBroker` response (`error_code != 0`,
`generation` is the **unchanged** overlay generation). Validation
(duplicate id, remove self, last broker / last voter) stays
`InvalidArg` (3).

## Path

```text
AddBroker / RemoveBroker (openraft-on leader, N>=2, rollback armed):
  validate only                    # no membership.json, no gen bump, no reassign
  pending = current ± id
  change_membership(pending)       # AddNodes + ReplaceAllVoters, 5s
  ok  → persist overlay + bump gen
        optional reassign-on-add
        MembershipPut 100
        client error_code = 0
  fail → no overlay write
         no generation bump
         no reassign-on-add
         client error_code = 15
         v0.34 restore is a no-op (nothing was written)

Flag off / N<2 / no cluster / rollback escape:
  persist-first                    # v0.10
  after_overlay_mutation           # v0.26 / v0.34
```

In-process `Broker::add_broker` / `remove_broker` stay persist-first
so `v10_dynamic_membership` and in-process v0.26 / v0.34 happy paths
keep working. Invert is owned by the **dispatch / client opcode**
path.

## Who writes overlay

| Caller | Behavior |
|--------|----------|
| Leader, openraft on, rollback on, N≥2 | Validate → joint pending → persist **after** commit; **15** + disk unchanged on fail |
| Leader, `VOLANT_OPENRAFT_JOINT_ROLLBACK=0` | v0.26: persist-first + fan-out; joint fail is log-only; client **0** |
| Follower (forward on) | No local persist (v0.38); leader path above |
| Openraft flag off / N<2 / no cluster | Unchanged v0.10 persist-first |

`MembershipPut` apply on a leader still attempts voter sync against
already-written `config.brokers` (v0.26) and does **not** roll back.

## Tests

`crates/volant-broker/tests/v34_joint_rollback.rs`:

1. Flag off — AddBroker writes overlay (v0.10).
2. Flag on, happy path — in-process add still writes overlay (honesty hole).
3. Flag on, `fail_next_change_membership` — overlay file / generation /
   broker list unchanged; client `error_code` is **15**. Failed joint
   must not create `membership.json`.

`crates/volant-broker/tests/v26_openraft_joint.rs`:

1. Flag off — overlay only; no raft target.
2. Flag on, in-process add — overlay + voter/hook (persist-first hole).
3. Flag on, **dispatch** AddBroker — overlay appears **after** a
   successful joint; fail → 15 and disk unchanged.

`crates/volant-broker/tests/v39_reassign_rollback.rs`:

1. Flag on + `VOLANT_REASSIGN_ON_ADD=1`, dispatch AddBroker +
   `fail_next_change_membership` — new id is **not** in live or
   on-disk assignment. Client **15**.

`v10_dynamic_membership` must still pass (flag off).

```bash
cargo test -p volant-broker --test v34_joint_rollback --test v26_openraft_joint --test v39_reassign_rollback --test v10_dynamic_membership -- --test-threads=1
```

## Non-goals

| Deferred | Why |
|----------|-----|
| Overlay write from StateMachine apply on followers | Sibling **v0.216** |
| Invert in-process `add_broker` / `remove_broker` | Would break v0.10 / v0.26 in-process tests |
| Majority wait on `MembershipPut` | Unchanged v0.10 best-effort |
| Kafka DescribeCluster / new API keys | Shim frozen at 38 |
| Homemade 154 RequestVote / InstallSnapshot | Frozen |
| Phase 155 | Out of band |

## Honesty leftovers

- In-process `add_broker` / `remove_broker` still persist first
  (library API; client opcodes go through dispatch).
- `VOLANT_OPENRAFT_JOINT_ROLLBACK=0` restores persist-first + v0.26
  best-effort (overlay may exist without a matching voter set).
- Follower `VOLANT_OPENRAFT_FORWARD_MEMBERSHIP=0` still writes local
  overlay (v0.10 split-brain).
- Leader `MembershipPut` apply does not roll back.
- AddNodes may install a learner before `ReplaceAllVoters` fails;
  overlay is not written, the learner record may remain until a later
  membership change.
- `change_membership` wait is still 5s on the real (non-hook) fail path.

## Merge notes

v0.216 also edits `admin.rs` / `openraft_meta.rs` / `dispatch.rs`.
Keep this hunk to **leader persist-after-commit**. Keep both on
conflicts.

## Related

- [V10_SPEC.md](./V10_SPEC.md) — overlay add/remove
- [V26_SPEC.md](./V26_SPEC.md) — openraft joint on add/remove
- [V34_SPEC.md](./V34_SPEC.md) — roll back overlay if joint fails
- [V38_SPEC.md](./V38_SPEC.md) — forward Add/RemoveBroker to leader
- [V39_SPEC.md](./V39_SPEC.md) — restore assignment if add-broker joint fails
