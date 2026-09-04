# v0.217 — in-process add/remove_broker persist after openraft joint

**Status:** Shipped  
**Crate:** 0.2.0  
**Theme:** Close the leftover honesty hole: in-process
`Broker::add_broker` / `remove_broker` still wrote
`{data_dir}/cluster/membership.json` before `change_membership` when
openraft was on. Client opcodes already persist **after** joint
(v0.212). This slice inverts the library API the same way.

This is residual **v0.217**. It is **not** KRaft voter reconfig and
**not** a majority wait on `MembershipPut`. Homemade `metadata_raft.rs`
is unchanged. No new native opcodes. No Kafka API keys.

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **on** (Phase 155) | off: `0` / `false` / `no` / `off` restores v0.10 persist-first |

The invert is consulted only when the flag is on **and** raft is
started (`openraft_meta` is Some, which implies N≥2). Flag **off**,
N&lt;2, or no cluster keep v0.10 persist-first. Existing
`v10_dynamic_membership` must pass (raft is not started; overlay
write stays persist-first).

## Path

```text
add_broker / remove_broker (openraft on, raft started, N>=2):
  validate only                    # no membership.json, no gen bump
  pending = current ± id
  change_membership(pending)       # AddNodes + ReplaceAllVoters
  ok  → persist overlay + bump gen
  fail → Error (native 15)
         no overlay write
         no generation bump

add_broker / remove_broker (openraft on, raft started, no tokio runtime):
  InvalidArgument                  # use the native opcode path
  no overlay write                 # do not silently persist-first

Flag off / N<2 / no cluster:
  persist-first                    # v0.10
```

`add_broker` / `remove_broker` stay **sync**. The joint wait uses
`tokio::runtime::Handle::try_current()` then `block_on` (parked with
`block_in_place` so `#[tokio::test]` workers do not panic). No
crate-wide async break.

## Errors

| Case | In-process | Client opcode (v0.212) |
|------|------------|------------------------|
| Joint fail | `Error` message includes native **15** / not enough replicas; disk unchanged | `AddBroker` / `RemoveBroker` `error_code` **15** |
| No tokio runtime | `InvalidArgument` (use native opcode) | n/a (dispatch is already async) |
| Validation (duplicate, self, last) | `InvalidArgument` (3) | `Error` / InvalidArg (3) |

## Who writes overlay

| Caller | Behavior |
|--------|----------|
| In-process, openraft on, raft started | Validate → joint pending → persist **after** commit; **15** + disk unchanged on fail |
| In-process, openraft on, no runtime | `InvalidArgument`; **no** disk write |
| Leader dispatch, rollback on, N≥2 | Unchanged v0.212 invert |
| Leader, `VOLANT_OPENRAFT_JOINT_ROLLBACK=0` | Dispatch stays v0.26 persist-first + fan-out (does not call inverted `add_broker`) |
| Openraft flag off / N&lt;2 / no cluster | Unchanged v0.10 persist-first |

## Tests

`crates/volant-broker/tests/v10_dynamic_membership.rs`:

1. Flag off / raft not started — AddBroker / RemoveBroker still write overlay (v0.10).

`crates/volant-broker/tests/v26_openraft_joint.rs`:

1. Flag off — overlay only; no raft target.
2. Flag on, in-process add — overlay after a successful joint.
3. Flag on, in-process add + `fail_next_change_membership` — overlay **not** written.

`crates/volant-broker/tests/v34_joint_rollback.rs`:

1. Flag off — AddBroker writes overlay (v0.10).
2. Flag on, happy path — in-process add writes overlay after joint.
3. Flag on, in-process `fail_next_change_membership` — overlay file / generation / broker list unchanged.
4. Flag on, dispatch `fail_next` — client **15**, disk unchanged (v0.212).

```bash
cargo test -p volant-broker --test v10_dynamic_membership --test v26_openraft_joint --test v34_joint_rollback -- --test-threads=1
```

## Non-goals

| Deferred | Why |
|----------|-----|
| Make `add_broker` / `remove_broker` async | Crate-wide break; block_on is enough |
| Majority wait on `MembershipPut` | Unchanged v0.10 best-effort |
| Kafka DescribeCluster / new API keys | Shim frozen at 38 |
| Homemade 154 RequestVote / InstallSnapshot | Frozen |
| Delete homemade 154 | Tighten-only leftovers are v0.213/v0.214 |
| Phase 155 | Out of band |

## Honesty leftovers

- `VOLANT_OPENRAFT_JOINT_ROLLBACK=0` on the **dispatch** path still
  persist-first + v0.26 best-effort (overlay may exist without a
  matching voter set). In-process invert does not consult that escape.
- Follower `VOLANT_OPENRAFT_FORWARD_MEMBERSHIP=0` still writes local
  overlay (v0.10 split-brain).
- AddNodes may install a learner before `ReplaceAllVoters` fails;
  overlay is not written, the learner record may remain until a later
  membership change.
- `change_membership` wait is still 5s on the real (non-hook) fail path.

## Related

- [V10_SPEC.md](./V10_SPEC.md) — overlay add/remove
- [V26_SPEC.md](./V26_SPEC.md) — openraft joint on add/remove
- [V34_SPEC.md](./V34_SPEC.md) — roll back overlay if joint fails
- [V212_SPEC.md](./V212_SPEC.md) — leader dispatch persist-after-commit
- [V216_SPEC.md](./V216_SPEC.md) — overlay write from Membership apply
