# v0.21 — durable openraft log and hard state (MVP)

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Persist the opt-in openraft vote, log, and last snapshot so a
process restart can re-elect and keep applied metadata. Homemade 154
`__metadata_raft/` is unchanged. Sibling v0.22 owns snapshot → assignment
apply.

**Honesty:** this is **not** Rocks / openraft-rocks, **not** joint
consensus, and **not** a delete of `assignment.json`. Files are JSON with
atomic replace (same pattern as other Volant stores). `install_snapshot`
still does **not** rewrite live assignment.

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **off** | on: `1` / `true` / `yes` / `on`; off: unset / `0` / `false` / `no` / `off` |

When **off**, CreateTopic / election stay lowest-id; **`__openraft/` is
not created**. Existing tests must pass without the env.

When **on**, `boot_openraft_metadata` **loads** `{data_dir}/__openraft/`
if present, then constructs `LogStore` / `StateMachine` from disk.
Missing files start empty (today).

## On-disk layout

`{data_dir}/__openraft/` (created on first persist; never when the flag
is off):

| File | Contents |
|------|----------|
| `hard_state.json` | vote, committed log id, last_purged (serde of openraft types) |
| `log.json` | JSON object `{ "entries": [ ... Entry ... ] }` |
| `snapshot.json` | last_applied + last_membership + optional last snapshot meta + `MetaSnapshotPayload` (raw JSON) |

Writes are temp file + `fsync` + rename, same as homemade 154 /
partition-raft JSON stores.

## Persist points

`append` / `truncate` / `purge` / `save_vote` / `save_committed` /
`install_snapshot` / `build_snapshot` / `apply` persist. `apply` writes
the last_applied checkpoint so a restart without a formal snapshot still
has `last_applied` matching disk.

On load: restore vote + log + last snapshot (if any). `last_applied`
comes from `snapshot.json`. Openraft then re-applies any committed prefix
not yet in the SM (`last_applied < committed`). **Do not** apply snapshot
assignment into live `assignment.json` here — that is v0.22. Live topics
after restart come from existing `{data_dir}/cluster/assignment.json`
plus any log replay of `SetAssignment`.

If vote or log already exist, skip `initialize()` (already initialized).

## What is / is not replaced

| This slice | Still frozen / elsewhere |
|------------|--------------------------|
| Durable openraft vote + log + snapshot files | Homemade 154 `__metadata_raft/` (do not extend) |
| Restart re-election with flag on | Snapshot → assignment apply (v0.22) |
| Flag-off: no `__openraft/` | Rocks / openraft-rocks |
| | Joint consensus / dynamic voters |
| | Kafka API keys / Phase 155 |

## Tests

`crates/volant-broker/tests/v21_openraft_durable.rs`:

1. Flag off — CreateTopic does not create `__openraft/`.
2. Flag on — after CreateTopic, `__openraft/` exists with log or snapshot
   files on the leader (or all nodes).
3. Restart — 3-node, create topic, drop brokers, `Broker::with_cluster`
   again on the same data_dirs + start raft/bg, wait for leader, topic
   still present on all, `controller_id()` is a live openraft leader.

Also keep `v11_openraft_election` and `v16_openraft_apply` green.

```bash
cargo test -p volant-broker --test v21_openraft_durable -- --test-threads=1
cargo test -p volant-broker --test v11_openraft_election -- --test-threads=1
cargo test -p volant-broker --test v16_openraft_apply -- --test-threads=1
```

## Non-goals

- RequestVote / InstallSnapshot on homemade `metadata_raft.rs`
- Applying snapshot payload into live assignment (v0.22)
- Rocks / openraft-rocks
- Joint consensus
- Kafka API keys
- Phase 155
