# v0.22 — InstallSnapshot applies assignment.json

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the v0.17 leftover that snapshot payload already includes
`assignment` but **Install does not rewrite live assignment**.

**Honesty:** this is **not** homemade Phase 154 snapshots, **not** full
KRaft, and **not** on-disk snapshot restore. The openraft snapshot store
stays **in-memory**. Default `VOLANT_OPENRAFT_METADATA` remains **off**.
Sibling v0.21 (durable raft log) is out of scope.

v0.17 `build_snapshot` already serializes
`{ last_applied, membership, assignment }`. Install stored the bytes and
set last_applied / membership only. A lagging empty node could vote after
InstallSnapshot without ever seeing the topic in live assignment. This
slice applies a **non-empty** snapshot assignment through the same
`Broker::apply_cluster_state` path as v0.16 `SetAssignment`.

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **off** | on: `1` / `true` / `yes` / `on`; off: unset / `0` / `false` / `no` / `off` |
| `VOLANT_OPENRAFT_SNAPSHOT_LOGS` | **1000** | same as [V17_SPEC.md](./V17_SPEC.md); tests use `1` so a late node is sent a snapshot |

When the flag is **off**, election / snapshot / apply are not started.
Lowest-id controller and Phase 6 / 150 / 154 apply are unchanged.

## Apply

In `StateMachine::install_snapshot`, after parsing `MetaSnapshotPayload`:

1. If `assignment.topics` is **non-empty** and `Weak<Broker>` upgrades,
   call `apply_cluster_state(generation, controller_id(), topics)`
   (writes `{data_dir}/cluster/assignment.json` + `apply_local_assignment`).
2. If `assignment.topics` is **empty**, skip apply (keep today's
   last_applied / membership-only behavior). This **must not** wipe an
   existing live assignment.
3. If apply fails, **log a warning** and still install raft meta
   (`last_applied`, membership, stored bytes). Do **not** abort raft.
4. If the `Weak<Broker>` does not upgrade, log a warning and still
   install raft meta.

A lagging / empty node that receives InstallSnapshot therefore ends up
with the topic in **live assignment** (and `assignment.json`), not only
raft last_applied.

## What is / is not replaced

| This slice | Still frozen / elsewhere |
|------------|--------------------------|
| openraft `install_snapshot` → live `assignment.json` | Homemade 154 `metadata_raft.rs` (no InstallSnapshot) |
| Late-node catch-up of topics via snapshot | On-disk snapshot files / restart restore of snapshot store |
| | Durable openraft log (sibling v0.21) |
| | Kafka API keys / Phase 155 |
| | Per-partition data Raft |

v0.16 log apply (`SetAssignment` via AppendEntries) is unchanged. A late
node that can still catch up from the log prefix may install the topic
via apply instead of snapshot; both paths write the same live state.

## Tests

`crates/volant-broker/tests/v22_snapshot_apply.rs`:

1. Snapshot payload with a topic; `install_snapshot` on a fresh broker
   with empty assignment → topic appears in live assignment and
   `assignment.json`.
2. Flag on, 3-node, `VOLANT_OPENRAFT_SNAPSHOT_LOGS=1`, CreateTopic,
   start the third node late; after InstallSnapshot (or append catch-up)
   the late node **has the topic**.
3. Empty snapshot assignment does **not** wipe an existing live
   assignment.

Keep `v17_openraft_snapshot` and `v16_openraft_apply` green.

```bash
cargo test -p volant-broker --test v22_snapshot_apply -- --test-threads=1
cargo test -p volant-broker --test v17_openraft_snapshot -- --test-threads=1
cargo test -p volant-broker --test v16_openraft_apply -- --test-threads=1
```

## Non-goals

- RequestVote / InstallSnapshot on homemade `metadata_raft.rs`
- Persisting the openraft snapshot store to disk
- Using an empty snapshot assignment to delete topics (use v0.16
  `SetAssignment` / DeleteTopic)
- Kafka API keys
- Phase 155
- Touching homemade 154

## Honesty leftovers

- Apply failure still advances raft last_applied; the node may vote
  without the topic until a later `SetAssignment` or a retry. There is
  no automatic re-apply.
- Empty snapshot assignment is intentionally a no-op (does not mean
  “zero topics”).
- Snapshot bytes stay in-memory; a process restart still depends on
  `assignment.json` plus (if merged) a durable raft log — not on
  replaying this snapshot file.
- `Weak<Broker>` gone → assignment is not applied (raft meta still
  installs).
