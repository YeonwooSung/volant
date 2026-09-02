# v0.17 — openraft InstallSnapshot (MVP)

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Wire **InstallSnapshot** on the **openraft** metadata path
(opcodes **112/113**), emit a real snapshot payload, and purge the
in-memory log prefix after a snapshot.

**Honesty:** this is **not** homemade Phase 154 snapshots, **not** full
KRaft, and **not** a replace of `assignment.json` apply. Snapshot store
is **in-memory**. Log apply is still `Noop` (election + snapshot only).

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **off** | on: `1` / `true` / `yes` / `on`; off: unset / `0` / `false` / `no` / `off` |
| `VOLANT_OPENRAFT_SNAPSHOT_LOGS` | **1000** | unset → snapshot every **1000** applied logs; `N` ≥ 1 → every N (tests use `1` or `5`); `0` / `never` / `off` → never (manual trigger only) |

When the env is set to a number, tests also get `max_in_snapshot_log_to_keep=0`
and a matching `replication_lag_threshold` so a lagging node is sent a
snapshot and the log prefix is purged. Production (env unset) keeps
`max_in_snapshot_log_to_keep=1000` and `replication_lag_threshold=5000`.

When **`VOLANT_OPENRAFT_METADATA` is off**, election and snapshot are
not started. Default controller stays lowest live id (v0.11).

## Protocol

New **native** inter-broker opcodes (do **not** reuse 96–107 or 108–111):

| Opcode | Direction | Name | Body |
|--------|-----------|------|------|
| **112** / **113** | inter-broker | `OpenraftInstallSnapshot` / ack | `u32` length + JSON `InstallSnapshotRequest` / `InstallSnapshotResponse` |

JSON is `serde` of openraft 0.9 types, same style as 108–111.

## Snapshot payload

`build_snapshot` serializes:

```json
{ "last_applied": ..., "membership": ..., "assignment": <AssignmentSnapshot or empty> }
```

`assignment` is a copy of the live `{data_dir}/cluster/assignment.json`
view when the broker is clustered; otherwise empty. **Install does not
rewrite live assignment** — apply stays Phase 6 / 154. A lagging node
sets `last_applied` + membership from `SnapshotMeta` and can then vote /
append.

After a successful snapshot, openraft calls `LogStore::purge` (in-memory
`BTreeMap` split). `Broker::test_openraft_last_purged_index()` exposes
the advanced prefix.

## What is / is not replaced

| This slice | Still frozen / elsewhere |
|------------|--------------------------|
| openraft `RaftNetwork::install_snapshot` | Homemade 154 `metadata_raft.rs` (no InstallSnapshot) |
| Snapshot policy + log purge on openraft | `assignment.json` apply / SetAssignment through openraft |
| Inbound opcode 112 dispatch | Kafka `SUPPORTED_APIS` (38) |
| | Per-partition data Raft |
| | Durable snapshot files |

## Tests

`crates/volant-broker/tests/v17_openraft_snapshot.rs`:

1. Flag off — lowest-id controller; no snapshot hook.
2. Flag on, 3-node — after Noop writes, a snapshot exists (`v17-*` id,
   JSON has `last_applied` / `membership` / `assignment`).
3. After snapshot, `last_purged` advances.
4. Lagging node — start 2-of-3, snapshot, start the third; it applies
   and agrees on leader; term does not go backwards.
5. One 112/113 RPC roundtrip.

```bash
cargo test -p volant-broker --test v17_openraft_snapshot -- --test-threads=1
cargo test -p volant-broker --test v11_openraft_election -- --test-threads=1
cargo test -p volant-protocol --lib
```

## Non-goals

- RequestVote / InstallSnapshot on homemade `metadata_raft.rs`
- Replicating `SetAssignment` through openraft (log apply stays 154)
- On-disk snapshot store / restart restore of the openraft log
- Kafka API keys
- Phase 155
