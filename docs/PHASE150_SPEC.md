# Phase 150 — Cluster assignment majority consensus (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Raft-style **majority commit** for assignment generations (topics /
leaders / ISR snapshots) without embedding full openraft / KRaft. Same pattern
as truncate-journal majority (Phase 130).

## Goals

1. **Durable committed generation** under
   `{data_dir}/__assignment_consensus/state.json`:
   `{ "committed_generation", "pending_generation" }`.
2. **Protocol:** opcodes **96/97** `AssignmentConsensusNote` — request carries
   `generation` + `controller_id` + full wire topics (ClusterState encoding);
   peers `apply_cluster_state` when `generation >= local` and ack.
3. **Majority:** `floor(N/2)+1` of **configured** brokers (static membership).
   Self counts as 1 ack.
4. **Admin path:** after controller CreateTopic / DeleteTopic / CreatePartitions
   (and successful IsrUpdate), fan out when
   `VOLANT_ASSIGNMENT_CONSENSUS` is on (default **on**).
5. **Best-effort default:** client success independent of majority miss;
   optional wait via `VOLANT_ASSIGNMENT_CONSENSUS_WAIT` / 
   `Broker::set_assignment_consensus_wait(true)` → `NotEnoughReplicas` (15) on
   fail (local assignment still retained — honesty residual).
6. Metrics + tests + living docs.

## Non-goals

| Deferred | Why |
|----------|-----|
| Full openraft / KRaft / `__cluster_metadata` | Larger product bet |
| Dynamic membership reconfiguration | Static N only |
| Metadata always gated on `committed_generation` | MVP: Metadata may lead committed_gen; metrics expose lag |
| Per-partition Raft log | Assignment gen only |
| Stream durable state | Phase 149 sibling; out of scope |

## Protocol

| Step | Mechanism |
|------|-----------|
| Propose | Local assignment already mutated; `pending_generation = gen` |
| Fan-out | Parallel `AssignmentConsensusNote` (96) to live peers |
| Apply | Peer `apply_cluster_state(gen, controller_id, topics)` if gen ≥ local |
| Commit | acks ≥ majority(N) → `committed_generation = gen` durable |

## Env knobs

| Env | Default | Meaning |
|-----|---------|---------|
| `VOLANT_ASSIGNMENT_CONSENSUS` | **on** (`1`/`true`/`yes`; `0`/`false`/`no` off) | Fan out notes after assignment mutates |
| `VOLANT_ASSIGNMENT_CONSENSUS_WAIT` | **off** | When on, admin RPCs fail client with 15 if majority miss |

## Metrics

| Metric | Type |
|--------|------|
| `volant_assignment_consensus_success_total` | counter |
| `volant_assignment_consensus_fail_total` | counter |
| `volant_assignment_committed_generation` | gauge |

## Honest limitations

- **Not full Raft:** no term/leader election for metadata, no linearizable multi-key log
- Majority uses **static configured N** (N=2 trap: one peer down → permanent fail)
- Metadata / local assignment may **lead** `committed_generation` until majority
- Wait-mode fail does **not** roll back local create/delete (like journal default)
- Death-path / background ISR generation bumps still rely on ClusterState pull;
  IsrUpdate path fans out best-effort when enabled
- Controller remains **lowest live id** (unchanged)

## Exit criteria

1. Opcode 96/97 roundtrip  
2. 3-node create_topic + fanout → peers have topic; success metric > 0  
3. N=2 one dead → fail metric; local assignment retained  
4. Single-node / N=1 trivial majority success  
5. phase6/118 green (best-effort default)  
