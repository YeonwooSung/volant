# v0.12 — `__cluster_metadata` topic + per-partition Raft log MVP

**Status:** Shipped (bounded MVP)  
**Crate:** 0.2.0 (unchanged)  
**Does not open Phase 155.** Does not add `openraft`, Kafka API keys, or
KRaft record batch schemas. Homemade `metadata_raft.rs` election is
untouched (no RequestVote / InstallSnapshot).

## Theme

Two small, **opt-in (default off)** pieces toward KRaft-shaped metadata
and per-partition Raft — honest dual-write, not a replacement of the
ISR + HWM data plane.

## A. Internal `__cluster_metadata` topic

When `VOLANT_CLUSTER_METADATA_TOPIC=1` (or `true`/`yes`):

1. The controller (lowest live id) ensures an internal topic
   `__cluster_metadata` exists: **1 partition**, **RF = min(3, N)**.
   Replicas are the lowest broker ids so the controller is the leader
   and can produce.
2. Each successful assignment mutation that already writes
   `assignment.json` (CreateTopic / DeleteTopic / CreatePartitions, plus
   wait-path restore) **also** appends one record to `__cluster_metadata-0`:
   - **key** = generation as a decimal string
   - **value** = JSON [`AssignmentSnapshot`]
   - **header** `volant-cluster-metadata=1`
3. On broker start with the flag on, if `assignment.json` is
   missing/empty but the topic log has records, **rebuild** assignment
   from the last record (last-write-wins) and persist `assignment.json`.

**Honesty:** this is a **local + ISR-replicated topic**, not a Raft
metadata log and not Kafka KRaft record schemas. Client fetch is still
capped at ISR HWM; rebuild reads the partition log LEO.

## B. Per-partition Raft log

Module: `crates/volant-broker/src/replica/partition_raft.rs`.

- Tiny Raft log per selected partition: term, index, vote, commit_index.
- **No second election** — leader is the current partition / ISR leader.
- Followers accept AppendEntries-shaped in-memory/disk entries.
- Persist: `{data_dir}/__partition_raft/{topic}/{partition}/log.json`
  (+ `hard_state.json`), same style as `__metadata_raft`.
- `VOLANT_PARTITION_RAFT=1` enables the log for **new** topics only.
  Tests may call `Broker::enable_partition_raft(topic, partition)`.
- When enabled, produce dual-writes `{offset, crc}`. `commit_index`
  advances only after a **majority** of ISR-sized replicas have a
  match_index. ISR size 1 commits immediately. Produce does **not**
  fail if Raft majority misses (ISR HWM remains the data-plane SoT).
- In-process `PartitionRaftGroup` covers 3-replica majority / minority
  without extra OS processes or new opcodes.

**Honesty:** dual-write / extra commit gate only. Does **not** replace
mmap `PartitionLog` or ISR HWM.

## Defaults

| Knob | Default | Notes |
|------|---------|-------|
| `VOLANT_CLUSTER_METADATA_TOPIC` | **off** | `1`/`true`/`yes` enables |
| `VOLANT_PARTITION_RAFT` | **off** | `1`/`true`/`yes` enables for new topics |

No new native opcodes (108–111 reserved for the openraft sibling; 112+
unused). No Kafka API keys.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka `__cluster_metadata` record schemas | ISR topic + JSON snapshot only |
| Replace `assignment.json` as SoT | Dual-write; json remains primary when present |
| openraft / RequestVote / InstallSnapshot | Sibling v0.11 / frozen homemade Raft |
| Per-partition election | Reuse ISR leader |
| Block `acks=all` on Raft majority | Would be a large produce-path refactor |
| New inter-broker opcodes | Stay in-process |

## Tests

`crates/volant-broker/tests/v12_partition_raft.rs`:

1. Flag off: no `__cluster_metadata` auto-create; produce unchanged
2. Flag on: CreateTopic → last record value contains the topic; generation matches
3. Wipe `assignment.json`, reopen same `data_dir` + flag → topic restored
4. 3 in-process replicas: majority commits + follower apply; minority (1 of 3) does not
5. 3-node `acks=all` still works with both flags on

Regression: `v05_ops_confidence`.

## Related

- [PHASE154_SPEC.md](./PHASE154_SPEC.md) — homemade metadata Raft (frozen)
- [V10_SPEC.md](./V10_SPEC.md) — membership overlay
- [ops.md](./ops.md) — operator knobs
