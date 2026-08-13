# Phase 154 — KRaft-style metadata Raft log (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Ordered assignment mutations as a **replicated log** with
`(term, index)`, majority AppendEntries, and apply only when `commit_index`
advances — closing the Phase 150/152 residual of “majority note of full
snapshots without a true log.”

**Honesty:** this is **not** full openraft / Kafka KRaft. No true Raft
election (controller remains **lowest live id**), no InstallSnapshot, no
dynamic membership, no data-plane / partition Raft. Interface is intentionally
small so a later openraft wrapper can replace the storage engine.

## Goals

1. **Durable metadata log** under `{data_dir}/__metadata_raft/`:
   - `log.json` — ordered `MetadataLogEntry { term, index, payload }`
   - `hard_state.json` — `{ current_term, commit_index, last_applied }`
2. **Protocol:** opcodes **98/99** `MetadataRaftAppend` — simplified
   AppendEntries (`leader_id`, `term`, `prev_log_index`, `prev_log_term`,
   `entries[]`, `leader_commit` → `term`, `success`, `match_index`).
3. **Payload:** `MetadataCommand::SetAssignment { generation, topics }`
   (full snapshot OK for MVP) + optional `Noop`.
4. **Leader path (controller):** after CreateTopic / DeleteTopic /
   CreatePartitions (and IsrUpdate best-effort):
   1. Local assignment already mutated (same as Phase 150)
   2. Append `SetAssignment` at next index
   3. Fan out AppendEntries to live peers
   4. Majority of **configured N** match_index → advance `commit_index`,
      apply, update Phase 152 committed snapshot
   5. On miss: do **not** advance commit; uncommitted entry retained;
      wait / committed-only → native **15**
5. **Apply path:** `apply_committed_metadata_entries` walks
   `last_applied+1 ..= commit_index` and applies `SetAssignment` via
   `apply_cluster_state` + `assignment_consensus.note_committed_snapshot`.
6. **Compatibility:** when `VOLANT_METADATA_RAFT=0`, keep Phase 150/152
   `AssignmentConsensusNote` path only. When on, prefer Raft over notes for
   admin mutations; still update assignment_consensus gens for metrics /
   Metadata.

## Non-goals

| Deferred | Why |
|----------|-----|
| Full openraft crate embed | rustc / API surface risk; later wrapper |
| True Raft election / log leadership | lowest-id controller remains leader |
| InstallSnapshot / log compaction | MVP retains full JSON log |
| Dynamic membership reconfiguration | Static N only |
| Raft for data plane / partitions | Metadata assignment only |
| Stream EOS / volant-stream | Sibling Phase 153 |

## Protocol

| Step | Mechanism |
|------|-----------|
| Append | Leader appends `SetAssignment` at `last_index+1` |
| Replicate | Parallel `MetadataRaftAppend` (98) with prev term/index + entries |
| Reject | Peer rejects if prev does not match |
| Commit | acks with `match_index >= entry.index` ≥ majority(N) → `commit_index` |
| Catch-up commit | Empty AppendEntries heartbeat with updated `leader_commit` |
| Apply | Both leader and followers apply committed entries to assignment |

## Env knobs

| Env | Default | Meaning |
|-----|---------|---------|
| `VOLANT_METADATA_RAFT` | **on** in cluster mode; **off** single-node (`1`/`true`/`yes`; `0`/`false`/`no`) | Prefer metadata Raft log over AssignmentConsensusNote |
| `VOLANT_ASSIGNMENT_CONSENSUS` | **on** (Phase 150) | Used when metadata Raft is **off** |
| `VOLANT_ASSIGNMENT_CONSENSUS_WAIT` | **off** | Client wait for majority (also forced by committed-only) |
| `VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY` | **on** (Phase 152) | Metadata serves committed snapshot; wait-like admin |

Runtime: `Broker::metadata_raft_enabled()` /
`set_metadata_raft_enabled(bool)`.

## File layout

```
{data_dir}/__metadata_raft/
  log.json          # [{ term, index, payload }, ...]
  hard_state.json   # { version, current_term, commit_index, last_applied }
```

Module: `crates/volant-broker/src/cluster/metadata_raft.rs`.

## Metrics

| Metric | Type |
|--------|------|
| `volant_metadata_raft_term` | gauge |
| `volant_metadata_raft_commit_index` | gauge |
| `volant_metadata_raft_last_applied` | gauge |
| `volant_metadata_raft_append_success_total` | counter |
| `volant_metadata_raft_append_fail_total` | counter |

Phase 150/152 metrics remain for dual-path / compatibility.

## Honest limitations

- **Not full Raft:** no election, no snapshot install, no joint consensus
- Controller remains **lowest live id** (static membership election residual)
- Full-snapshot `SetAssignment` payloads (not incremental CreateTopic ops)
- Local assignment may still exist before commit (same honesty as Phase 150/152;
  Metadata committed-only hides uncommitted)
- Majority uses **static configured N** (N=2 trap unchanged)
- JSON log is not segment-compacted; long-lived clusters may need later compaction
- Interface designed to later wrap openraft — not embedded today

## Exit criteria

1. Opcode 98/99 roundtrip  
2. Single-node append+commit; create_topic + fanout ok  
3. 3-node create_topic + raft fanout → all nodes same gen; commit advances  
4. N=2 one dead → fail metric; commit_index not advanced  
5. prev_log mismatch unit reject  
6. phase150 / phase152 green with raft off or dual-path  
