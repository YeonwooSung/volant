# Phase 142 — Metadata ISR freshness when leader ≠ controller

**Status:** ✅ Shipped (MVP)  
**Theme:** Clients that hit the partition **leader** always see live ISR in
Metadata; non-controller leaders **report** ISR changes to the controller so
assignment becomes cluster SoT and peers refresh via ClusterState.

## Goals

1. **Metadata overlay:** In cluster-mode `Broker::metadata()`, when this node is
   the partition leader, prefer **local** partition ISR (and local
   `leader_epoch` / HWM) over `assignment.isr`.
2. **Leader → controller IsrUpdate:** New inter-broker opcodes **94/95**. After
   leader-local ISR change (ReplicaFetch reconcile, death shrink), non-controller
   leaders enqueue a best-effort report; controller applies with leadership +
   epoch fence, bumps generation, persists assignment.
3. **Generation alignment:** On successful report, leader aligns local assignment
   generation to controller response (avoids permanent gen divergence that
   rejected ClusterState pulls). Non-controller leaders no longer bump local
   generation on ISR-only updates.
4. **Tests** `phase142_metadata_isr` + living docs.

## Non-goals

| Deferred | Why |
|----------|-----|
| Full Raft / KRaft metadata log | Larger product |
| Change produce/HWM semantics | Already leader-local |
| Preferred replica / session mirror | Orthogonal (140/138) |
| N=2 majority ops tooling | Sibling Phase 141 candidate |
| Guarantee report delivery | Best-effort; overlay covers leader hits |

## Wire

| Opcode | Name | Direction |
|-------:|------|-----------|
| 94 | `IsrUpdate` request | Non-controller leader → controller |
| 95 | `IsrUpdate` response | Controller → leader |

**Request:** `topic`, `partition u32`, `leader_id u32`, `leader_epoch u32`,
`isr: Vec<u32>`, `generation_hint u32` (`0` = none).

**Response:** `error_code u16`, `generation u32` (controller assignment gen
after apply; unchanged on reject).

**Errors:** `0` ok · `2` NotFound · `3` InvalidArg · `13` NotLeader ·
`14` NotController · `19` InvalidProducerEpoch (stale epoch).

## Design

```text
  Client Metadata → any broker
       │
       ├─ this node is leader for TP?
       │     yes → overlay local ISR / epoch / HWM   (immediate honesty)
       │     no  → assignment ISR (may lag until report + ClusterState)

  Leader ISR change (ReplicaFetch lag/death, on_broker_death shrink)
       │
       ├─ always: local partition ISR + HWM recompute
       ├─ controller: assignment.isr + generation++ + save
       └─ non-controller: assignment.isr (no gen bump) + enqueue IsrUpdate
              → async RPC 94 → controller apply_leader_isr_update
              → on success: leader align_assignment_generation
```

### Controller accept rules

1. Must be controller (`NotController` otherwise).
2. Topic/partition in assignment.
3. `leader_id == assignment.leader`.
4. `leader_epoch >= assignment.leader_epoch` (strict `<` fences).
5. ISR non-empty and contains leader; filtered to replica set.

## Honest limitations

- Report is **best-effort**; RPC fail leaves non-leader Metadata stale until
  retry/death path re-enqueues or ClusterState catches up another way.
- Overlay only helps clients that query the **leader** (or any node after
  controller SoT refresh + ClusterState pull).
- Not a consensus metadata log; dual-controller races still possible during
  failover windows.
- Kafka Metadata path reuses `Broker::metadata()` so overlay applies there too.

## Exit criteria

1. [x] Leader Metadata shows shrunk local ISR before controller sync  
2. [x] `apply_leader_isr_update` refreshes controller Metadata  
3. [x] Stale epoch / wrong leader rejected; ISR unchanged  
4. [x] Phase 118 rejoin tests still green  
5. [x] Docs honesty (consistency / TODO / history)

## Files

| Path | Change |
|------|--------|
| `volant-protocol` request/response/payload | Opcodes 94/95 |
| `volant-broker/src/broker.rs` | Overlay, apply, enqueue, align |
| `volant-broker/src/net.rs` | Handler + schedule fan-out |
| `tests/phase142_metadata_isr.rs` | Overlay + report + fence |
