# Phase 152 — Assignment consensus depth (Metadata serves committed)

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Close the Phase 150 residual where **Metadata may lead**
`committed_generation` — Metadata (cluster path) serves the majority-committed
assignment snapshot when gating is on.

## Goals

1. **Durable committed assignment snapshot** under
   `{data_dir}/__assignment_consensus/committed_snapshot.json`, loaded on open.
   API on `AssignmentConsensus`:
   - `note_committed_snapshot(gen, snap)`
   - `committed_snapshot() -> Option<AssignmentSnapshot>`
   - `committed_generation()` (Phase 150)
2. **Fan-out commit path:** after majority success,
   `commit(gen)` **and** store controller live `cluster.assignment` as committed
   snapshot for the same generation.
3. **Metadata gating:** when assignment consensus is **enabled** and
   `metadata_committed_only()` is **true** (default):
   - Build topic metadata from **committed snapshot** if available and
     `committed_generation > 0`
   - If `committed_generation == 0` and live gen is also 0: live (true bootstrap)
   - If `committed_generation == 0` but live gen > 0: serve **empty** Metadata
     (hide uncommitted creates until first majority)
   - When consensus disabled **or** committed_only false: live assignment
     (Phase 150 lead-Metadata behavior)
4. **Admin create visibility:** when `committed_only` is on, admin mutations that
   fan out consensus (CreateTopic / DeleteTopic / CreatePartitions) force
   wait-like behavior — majority fail → native **15** `NotEnoughReplicas`.
   Local assignment is **not** rolled back (honesty residual).
5. Metrics + tests + living docs.

## Non-goals

| Deferred | Why |
|----------|-----|
| Full openraft / KRaft | Larger product bet |
| Dynamic membership | Static N only |
| Stream EOS / volant-stream | Sibling Phase 151 |
| Rollback local assignment files on consensus fail | Hard; document residual |
| Separate commit RPC after majority | Peers install snapshot on note apply |

## Env knobs

| Env | Default | Meaning |
|-----|---------|---------|
| `VOLANT_ASSIGNMENT_CONSENSUS` | **on** (Phase 150) | Fan out notes after assignment mutates |
| `VOLANT_ASSIGNMENT_CONSENSUS_WAIT` | **off** (Phase 150) | Explicit client wait for majority |
| `VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY` | **on** (`1`/`true`; `0`/`false`/`no` off) | Metadata serves committed snapshot; also forces wait-like admin visibility when consensus enabled |

Runtime: `Broker::assignment_metadata_committed_only()` /
`set_assignment_metadata_committed_only(bool)`.

## Metadata behavior matrix

| consensus enabled | committed_only | Metadata source |
|-------------------|----------------|-----------------|
| no | * | Live assignment (Phase 150) |
| yes | **false** | Live assignment (may lead committed gen) |
| yes | **true**, committed_gen == 0, live gen == 0 | Live (true bootstrap, empty) |
| yes | **true**, committed_gen == 0, live gen > 0 | **Empty** (hide uncommitted) |
| yes | **true**, committed_gen > 0 + snap | **Committed snapshot** |

## Metrics

| Metric | Type |
|--------|------|
| `volant_assignment_committed_generation` | gauge (Phase 150) |
| `volant_assignment_metadata_committed_only` | gauge 0/1 |
| `volant_assignment_generation_lag` | gauge `max(0, live_gen - committed_gen)` |
| `volant_assignment_consensus_{success,fail}_total` | counters (Phase 150) |

## Honest limitations

- Local disk may retain an uncommitted assignment after majority fail (no rollback)
- Peers install committed snapshot on successful note **apply** (may briefly
  advertise a gen the controller later fails to majority-commit when other peers
  are unreachable — static-N residual)
- Not full KRaft / linearizable multi-key metadata log
- Single-node (no cluster) Metadata still uses local topics path

## Exit criteria

1. 3-node create + majority → Metadata on all shows topic; committed_gen == live
2. N=2 one dead + committed_only → Metadata does not advertise new topic
3. committed_only=false → Metadata can show live while committed lags
4. Single-node / N=1 Metadata works
5. phase150 tests still green
