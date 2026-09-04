# v0.216 — write membership overlay from openraft Membership apply

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** When cluster + openraft is on, `StateMachine` apply of
`EntryPayload::Membership` writes `{data_dir}/cluster/membership.json`.
Overlay becomes the apply artifact on **followers** and after snapshot
install. Sibling **v0.212** is leader persist-after-commit; this slice
is apply-side.

This is residual **v0.216**. It is **not** KRaft voter reconfig and
**not** a majority wait on `MembershipPut`. Homemade `metadata_raft.rs`
is unchanged. No new native opcodes. No Kafka API keys.

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **on** (Phase 155) | off: `0` / `false` / `no` / `off` restores v0.10 overlay SoT |

Flag off / N&lt;2: no change (v0.10 overlay SoT). Apply only runs when
openraft has booted.

## Apply

In `StateMachine::apply` (and `install_snapshot` when membership is in
the snapshot / `SnapshotMeta`):

1. On `EntryPayload::Membership`, derive broker endpoints from **voter
   ids** + `BasicNode.addr` (`host:port`, last colon).
2. If derived endpoints already match live toml/config **and** no overlay
   file exists, skip the write (initialize Membership is a no-op; v0.10
   absent-file SoT). A later add/remove or a membership that differs
   still writes.
3. Otherwise `save_membership_overlay`, update `config.brokers`, bump
   generation (`max(log_index, 1)`). An older membership log does **not**
   rewind a higher overlay generation (`MembershipPut` / leader persist).
   Then `apply_configured_ids`.
4. **Rack:** `rack: None` for newly derived ids. Known ids keep the
   previous rack when it is already on the local config/overlay. Rack
   is **not** on the raft membership log.
5. Empty / unparseable voter sets are skipped (do not wipe toml/overlay).

Boot: after `boot_openraft_metadata`, if restored `last_membership`
disagrees with the overlay file (file present but id/host/port differs,
or file missing **and** last_membership differs from toml), apply
`last_membership` over the file (best-effort). Missing file + same
endpoints as toml stays v0.10 absent-file SoT.

## `MembershipPut`

`MembershipPut` (opcode 100) stays **best-effort catch-up**, not SoT.
This slice does **not** make Put the write path when openraft is on
and apply already replicated the membership. Leader persist-after-commit
is v0.212.

## Tests

```bash
cargo test -p volant-broker --test v38_membership_forward --test v26_openraft_joint --test v11_openraft_election -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Flag off any-node AddBroker | v0.10 local overlay (unchanged) |
| Flag on, follower AddBroker | overlays still converge |
| Flag on, no leader | AddBroker does not write a higher overlay |
| After election, follower SM apply extra voter | overlay includes the new id; no Put |
| SM apply Membership with extra voter | overlay includes the new id; no Put |
| SM install_snapshot with last_membership | overlay written from snapshot membership |

## Honesty leftovers

- Rack is not replicated on the membership log; new ids are `rack: None`.
- Learners (`AddNodes` before `ReplaceAllVoters`) are not overlay brokers
  until they become voters.
- `MembershipPut` ignore-if-stale can still overwrite a lower generation.
- In-process `add_broker` / `remove_broker` still persist first (v0.10 /
  v0.26); dispatch owns forward / joint rollback.
- Not KRaft. Not a majority wait on Put. Homemade 154 unchanged.

## Merge notes

v0.212 also edits `openraft_meta.rs` / `admin.rs`. This hunk is SM
`apply` / `install_snapshot` / boot reconcile plus overlay comment and
v0.38 tests. Keep both.

## Related

- [V10_SPEC.md](./V10_SPEC.md) — overlay SoT
- [V26_SPEC.md](./V26_SPEC.md) — joint membership
- [V38_SPEC.md](./V38_SPEC.md) — follower forward
- [V22_SPEC.md](./V22_SPEC.md) — snapshot assignment apply
