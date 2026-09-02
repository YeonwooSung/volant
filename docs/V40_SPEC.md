# v0.40 — homemade metadata-raft wait-commit

**Status:** Shipped (MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** When homemade 154 (`VOLANT_METADATA_RAFT=1`) is on, CreateTopic /
DeleteTopic / CreatePartitions must **not** return client success until
`commit_index` covers the new `SetAssignment` entry (majority of configured
N). Timeout / majority miss rolls back live `assignment.json` like v0.3
and returns native **15** / Kafka **19**.

**Honesty:** this is **not** RequestVote, **not** InstallSnapshot on
homemade 154, and **not** a Kafka API-key change. Openraft
(`VOLANT_OPENRAFT_METADATA=1`) already `client_write`s and waits for
apply; this slice does not change that path. Homemade 154 still has no
election (lowest live id). Uncommitted log entries may still sit on disk
after a rolled-back client fail (same 154 retain-log honesty).

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_METADATA_RAFT_WAIT_COMMIT` | **on** | off: `0` / `false` / `no` / `off`; on: unset / `1` / `true` / `yes` / `on` |

The flag is **inert** unless homemade 154 is on **and** openraft is off
(when both raft flags are on, prefer openraft apply — v0.16).

| Homemade raft | Wait-commit | Client on majority miss |
|---------------|-------------|-------------------------|
| **on** | **on** (default) | Native **15** / Kafka **19**; `restore_live_assignment` |
| **on** | **off** | 154 mutate-first: local `assignment.json` is SoT; uncommitted entry retained |
| **off** | any | Unchanged Phase 150/152 (wait / committed-only still gate notes) |

## Mutation

Same sites as v0.3 / v0.16: native + Kafka CreateTopics / DeleteTopics /
CreatePartitions call `snapshot_if_must_wait` then
`complete_assignment_mutation`.

When homemade 154 wait-commit is on:

1. Snapshot live assignment.
2. Mutate locally (`assignment.json` + in-memory).
3. `fanout_metadata_raft_append` (opcodes **98/99**).
4. Client ok only if `commit_index` advanced past the pre-append value
   (majority match_index). Else restore the snapshot and return **15**.

Inter-broker RPC timeout is the existing `VOLANT_INTER_BROKER_RPC_TIMEOUT_MS`
budget (a dead peer is a miss, not a hang). No new error family.

## What is / is not replaced

| This slice | Still frozen / elsewhere |
|------------|--------------------------|
| Homemade 154 client ok waits for `commit_index` | Homemade RequestVote / InstallSnapshot (do not extend 154) |
| Default-on wait-commit + rollback | Openraft `client_write` wait (v0.16 already) |
| Escape hatch `WAIT_COMMIT=0` (154 mutate-first) | Phase 152 committed-only Metadata (separate hide) |
| | Kafka API keys / Phase 155 |

## Tests

`crates/volant-broker/tests/v40_raft_wait_commit.rs`:

1. Default on — wait-commit true; inert until homemade raft is enabled.
2. Homemade raft on, wait-commit on, N=2 one dead — CreateTopic **15**;
   topic **not** left on disk; `commit_index` unchanged.
3. Homemade raft on, wait-commit **off** — CreateTopic succeeds locally
   (154 mutate-first); uncommitted entry retained.
4. 3 live nodes, wait-commit on — CreateTopic ok and `commit_index`
   advanced.

`phase154_metadata_raft` stays green: those tests set wait-commit **off**
so they keep documenting uncommitted-lead on a direct AppendEntries miss.

```bash
cargo test -p volant-broker --test v40_raft_wait_commit -- --test-threads=1
cargo test -p volant-broker --test phase154_metadata_raft -- --test-threads=1
```

## Non-goals

- RequestVote / InstallSnapshot on homemade `metadata_raft.rs`
- Changing openraft wait / apply
- Flipping `VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY` default
- Kafka API keys
- Phase 155
