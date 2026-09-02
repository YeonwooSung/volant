# v0.35 — openraft log store on redb

**Status:** Shipped (MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Persist the opt-in openraft vote / committed / last_purged / log
entries in a **redb** file instead of rewriting `{data_dir}/__openraft/log.json`
on every append (v0.21 honesty leftover). Snapshot meta stays a JSON side
file. Homemade 154 `__metadata_raft/` is unchanged.

**Honesty:** this is **not** RocksDB, **not** openraft-rocksstore, and
**not** a multi-process file DB. `raft.redb` is one-process (redb exclusive
lock), same durability class as stream `DurableStore`. Reads still use the
in-memory `BTreeMap` cache loaded on open. Open retries briefly on
`DatabaseAlreadyOpen` so a test/process restart can acquire the lock after
the previous `LogStore` drops.

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **off** | on: `1` / `true` / `yes` / `on`; off: unset / `0` / `false` / `no` / `off` |

When **off**, CreateTopic / election stay lowest-id; **`__openraft/` is
not created**. Existing tests must pass without the env.

When **on**, `boot_openraft_metadata` **loads** `{data_dir}/__openraft/raft.redb`
if present (legacy `log.json` + `hard_state.json` are imported once when
the redb file is missing), then constructs `LogStore` / `StateMachine`.

## On-disk layout

`{data_dir}/__openraft/` (created on first persist; never when the flag
is off):

| File | Contents |
|------|----------|
| `raft.redb` | **v0.35 SoT** — table `entries` (`u64` index → JSON `Entry`); table `meta` keys `vote` / `committed` / `last_purged` |
| `snapshot.json` | last_applied + last_membership + optional last snapshot meta + `MetaSnapshotPayload` (unchanged v0.21 side file) |
| `hard_state.json` / `log.json` | **legacy v0.21**. Read only when `raft.redb` is absent; imported into redb on first open. Not rewritten. |

Writes to `raft.redb` use a redb write transaction with
[`Durability::Immediate`](https://docs.rs/redb) (fsync on commit), same as
stream `DurableStore`. `append` inserts only new keys; `truncate` /
`purge` delete a key range; `save_vote` / `save_committed` update `meta`.
`purge` is one transaction (prefix delete + `last_purged`).

## Persist points

`append` / `truncate` / `purge` / `save_vote` / `save_committed` write
redb. `install_snapshot` / `build_snapshot` / `apply` still rewrite
`snapshot.json` (atomic replace).

On load: restore vote + log + last snapshot (if any) from redb +
`snapshot.json`. Openraft then re-applies any committed prefix not yet
in the SM. Live topics after restart still come from
`{data_dir}/cluster/assignment.json` plus log replay of `SetAssignment`.

If vote or log already exist, skip `initialize()` (already initialized).

## Legacy JSON import

If `raft.redb` is missing and `log.json` / `hard_state.json` exist, the
first open **imports** them into redb (one Immediate txn) and then
prefers redb. Stale JSON is left on disk and ignored while `raft.redb`
exists. Operators may delete the JSON files after a successful import;
they are not required for a v0.35 restart.

## What is / is not replaced

| This slice | Still frozen / elsewhere |
|------------|--------------------------|
| Incremental redb log + vote + committed + last_purged | Homemade 154 `__metadata_raft/` (do not extend) |
| One-shot import of v0.21 JSON | `snapshot.json` full rewrite |
| Flag-off: no `__openraft/` | Rocks / openraft-rocks |
| | Joint consensus / dynamic voters (v0.26 already) |
| | Kafka API keys / Phase 155 |

## Tests

`crates/volant-broker/tests/v35_openraft_redb.rs`:

1. Flag off — CreateTopic does not create `__openraft/` or `raft.redb`.
2. Flag on — after CreateTopic, leader has `raft.redb` (and does **not**
   rewrite `log.json`).
3. Restart — 3-node, create topic, drop brokers, `Broker::with_cluster`
   again on the same data_dirs + start raft/bg, wait for leader, topic
   still present on all, `controller_id()` is a live openraft leader.
4. Many Noop appends + snapshot + purge — `last_purged` advances;
   `raft.redb` still exists.

Unit: `import_legacy_json_creates_redb_and_reopen_prefers_it`,
`append_truncate_purge_roundtrip_on_redb` in `openraft_meta.rs`.

Also keep `v21_openraft_durable`, `v16_openraft_apply`, and
`v11_openraft_election` green.

```bash
cargo test -p volant-broker --test v35_openraft_redb -- --test-threads=1
cargo test -p volant-broker --test v21_openraft_durable -- --test-threads=1
cargo test -p volant-broker --test v16_openraft_apply -- --test-threads=1
cargo test -p volant-broker --test v11_openraft_election -- --test-threads=1
```

## Non-goals

- RocksDB / openraft-rocksstore / C++ toolchain
- Multi-process shared `raft.redb`
- Moving `snapshot.json` into redb
- RequestVote / InstallSnapshot on homemade `metadata_raft.rs`
- Kafka API keys
- Phase 155
