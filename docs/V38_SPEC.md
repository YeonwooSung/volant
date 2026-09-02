# v0.38 — forward Add/RemoveBroker to the openraft leader

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the v0.34 leftover “Follower add/remove still persists
overlay immediately (v0.10 split-brain).” When openraft metadata is on
and this node is **not** the openraft leader, AddBroker / RemoveBroker
do **not** write `{data_dir}/cluster/membership.json`. The same request
body is forwarded to `controller_id()` over existing `inter_broker_rpc`
and the leader’s response is returned to the client.

**Honesty:** this is **not** KRaft voter reconfig and **not** a majority
wait on `MembershipPut`. Homemade `metadata_raft.rs` is unchanged. No
new native opcodes (reuse 102–107). No Kafka API keys.

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **off** | on: `1` / `true` / `yes` / `on`; off: unset / `0` / `false` / `no` / `off` |
| `VOLANT_OPENRAFT_FORWARD_MEMBERSHIP` | **on** (when consulted) | off: `0` / `false` / `no` / `off` restores follower-local v0.10 write; unset / any other value keeps forward |

Forward is consulted only when `VOLANT_OPENRAFT_METADATA` is on **and**
this node is not the openraft leader (`controller_id()`). When the
openraft flag is **off**, add/remove is unchanged v0.10 (any node writes
overlay). Existing v0.10 / v0.34 tests must pass without the new env.

Runtime setter: `Broker::set_openraft_forward_membership` (tests / live).

## Client error

No leader (`controller_id()==0`), missing leader address, in-flight
re-entry, or `inter_broker_rpc` failure returns native
**`NotController` (14)** on the typed `AddBroker` / `RemoveBroker`
response (`error_code != 0`, `generation` is the **unchanged** local
overlay generation). Overlay file is not written.

**14** is used (not **15**) so “could not reach a controller” stays
distinct from v0.34 leader joint-fail **`NotEnoughReplicas` (15)**.

## Path

```text
AddBroker / RemoveBroker:
  if VOLANT_OPENRAFT_METADATA on
     AND VOLANT_OPENRAFT_FORWARD_MEMBERSHIP on
     AND this node is not controller_id():
       if controller_id()==0 or no addr or RPC fail:
            do not persist overlay
            client error_code = 14
       else:
            inter_broker_rpc(leader, same request body)
            return leader response   # 0 / 15 / InvalidArg / …
  else:
       snapshot + persist overlay    # v0.10 / leader / escape
       v0.34 after_overlay_mutation  # joint + rollback or MembershipPut
```

In-process `Broker::add_broker` / `remove_broker` still persist first
(v0.10 / v0.26). Forward is owned by the **dispatch / client opcode**
path so existing in-process tests stay valid.

The leader receiving a forwarded opcode is `controller_id()`, so it
does not re-forward. A second inbound mutate on a follower that already
has a forward in flight returns **14** (A↔B leadership-split loop
guard).

## Who writes overlay

| Caller | Behavior |
|--------|----------|
| Leader, openraft on | Persist → v0.34 joint / rollback |
| Follower, openraft on, forward on | **No local persist.** RPC to leader |
| Follower, `VOLANT_OPENRAFT_FORWARD_MEMBERSHIP=0` | v0.10: persist + `MembershipPut` |
| Openraft flag off | Unchanged v0.10 |

`MembershipPut` (opcode 100) after a successful leader persist is
unchanged. Followers apply if `incoming.generation > local`.

## Tests

`crates/volant-broker/tests/v38_membership_forward.rs`:

1. Flag off — any node AddBroker still writes local overlay (v0.10).
2. Flag on, 3-node — AddBroker to a **non-leader**; overlay generation
   bumps on **all** (leader persist + `MembershipPut`); contacted
   follower overlay matches the leader.
3. Flag on, no leader (solo process of a 3-voter group) — AddBroker
   returns `error_code != 0` (**14**) and does **not** write a higher
   local generation.

Also keep `v10_dynamic_membership` and `v34_joint_rollback` green.

```bash
cargo test -p volant-broker --test v38_membership_forward -- --test-threads=1
cargo test -p volant-broker --test v10_dynamic_membership -- --test-threads=1
cargo test -p volant-broker --test v34_joint_rollback -- --test-threads=1
```

## Non-goals

| Deferred | Why |
|----------|-----|
| In-process `add_broker` refuse-on-follower | Dispatch owns forward; v0.10/v0.26/v0.34 call the method directly |
| Majority wait on `MembershipPut` | Unchanged v0.10 best-effort |
| Kafka DescribeCluster / new API keys | Shim frozen at 38 |
| Homemade 154 RequestVote / InstallSnapshot | Frozen |
| Phase 155 | Out of band |

## Honesty leftovers

- In-process `add_broker` / `remove_broker` on a follower still persist
  (library API; client opcodes go through dispatch).
- `VOLANT_OPENRAFT_FORWARD_MEMBERSHIP=0` restores v0.10 follower-local
  write (split-brain).
- Concurrent Add/RemoveBroker to the same follower while a forward is
  in flight may return **14**; client can retry.
- Leader `MembershipPut` apply still does not roll back (v0.34 leftover).
- Offline new voter still trips v0.34 joint wait / rollback on the
  leader path (forward does not change that).
