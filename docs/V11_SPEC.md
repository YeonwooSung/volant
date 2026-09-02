# v0.11 — openraft metadata leader election (MVP)

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Replace lowest-id controller with a real **openraft** election,
**opt-in**, default **off**.

**Honesty:** this is **not** full KRaft, **not** a complete replace of
Phase 150/152/154, and **not** per-partition data Raft. Homemade
`metadata_raft.rs` is unchanged (no RequestVote). Assignment apply still
writes `{data_dir}/cluster/assignment.json` (and optional 154 log when
`VOLANT_METADATA_RAFT=1`). InstallSnapshot is **not** in this slice.

## Flag

| Env | Default | Values |
|-----|---------|--------|
| `VOLANT_OPENRAFT_METADATA` | **off** | on: `1` / `true` / `yes` / `on`; off: unset / `0` / `false` / `no` / `off` |

When **off**, `Broker::controller_id()` is still `Membership::controller_id()`
(lowest live id). All existing tests must pass without the env.

When **on**, nodes form an openraft group whose membership is the
**effective broker list** (toml or v0.10 overlay). After start, exactly
one leader. `controller_id()` returns that leader id (`0` if none yet).

## Protocol

New **native** inter-broker opcodes (do **not** reuse 96–107):

| Opcode | Direction | Name | Body |
|--------|-----------|------|------|
| **108** / **109** | inter-broker | `OpenraftAppend` / ack | `u32` length + JSON `AppendEntriesRequest` / `AppendEntriesResponse` |
| **110** / **111** | inter-broker | `OpenraftVote` / ack | `u32` length + JSON `VoteRequest` / `VoteResponse` |

JSON is `serde` of openraft 0.9 types (`openraft` crate **0.9.21** with
`serde` + `storage-v2`; Cargo resolves **0.9.25**, MSRV 1.75).
InstallSnapshot is not on the wire.

## What is / is not replaced

| Replaced in this slice | Still 154 / lowest-id |
|------------------------|------------------------|
| Metadata **leader identity** (`controller_id` / `is_controller`) when flag on | Homemade AppendEntries 98/99 |
| Term contests / RequestVote via openraft | `assignment.json` apply path |
| Metrics `volant_openraft_leader_id`, `volant_openraft_term` | Kafka `SUPPORTED_APIS` (38) |
| | Per-partition data Raft (sibling slice) |
| | Dynamic voter reconfig / joint consensus |

Next slice (not this one): full openraft replace of 150/152/154 log apply.

## Isolation

openraft is started from `start_background_tasks` (so `serve_listener`
abort drops the raft node). Outbound RPCs reuse `inter_broker_rpc`.
Aborting the leader plus `test_set_inter_broker_blocked` isolates it;
survivors elect a new leader and the term does not go backwards.

## Tests

`crates/volant-broker/tests/v11_openraft_election.rs`:

1. Default off — 3-node, `controller_id() == 1` (lowest live).
2. Flag on — agreed leader on all live nodes (timeout loop).
3. Leader abort — new leader, term ≥ previous, produce `acks=1` on an
   existing topic.

## Non-goals

- RequestVote / InstallSnapshot on homemade `metadata_raft.rs`
- Replicating `SetAssignment` through openraft (log apply stays 154)
- Kafka API keys
- Phase 155
