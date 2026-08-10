# Phase 134 — Peer-to-peer heartbeat mesh (MVP)

**Status:** ✅ Shipped  
**Theme:** Close Phase 131/132 honesty gap that heartbeats are
**controller-centric**, so a non-controller holding a truncate-journal watermark
cannot drive rejoin catch-up for peers that only talk to the controller.

## Goals

1. **Mesh heartbeats:** Each broker sends `HeartbeatBroker` to **all other
   configured cluster peers** (not only the controller), on the same period as
   today (`session_timeout/3`).
2. **Safe membership:** Only apply `apply_controller_alive_set` + ClusterState
   pull when the peer contacted is the **current controller**. Peer-to-peer
   responses only `note_peer_live` (do not trust non-controller alive-sets as SoT).
3. **Journal catch-up:** Receive path already schedules Phase 132 catch-up on
   lag; mesh makes non-controller → non-controller lag detection work.
4. Controller still self-touches membership locally (existing path).
5. Tests: peer with journal watermark reaches lagging peer without controller
   holding that watermark; phase131 rejoin still green.
6. Living docs honesty when shipping.

## Non-goals

| Deferred | Why |
|----------|-----|
| Full Raft / openraft | Orthogonal |
| Replacing controller election | Still lowest live id |
| Admin ACL/BROKER catch-up from non-controller | Still controller SoT (Phase 117) |
| Fully connected always-on mesh with adaptive rates | MVP fixed period |

## Design

```text
  each tick:
    if self == controller: local handle_heartbeat_broker(self)
    for peer in configured − self:
      HeartbeatBroker(applied config/acl/journal gens) → peer
      on ok:
        note_peer_live(peer)
        if peer == controller:
          apply_controller_alive_set + optional ClusterState pull

  on receive HeartbeatBroker (any sender):
    handle_heartbeat_broker (mark sender live)
    if controller: admin catch-up when lag (Phase 117)
    journal schedule catch-up when lag (Phase 131/132)  // any node
```

Touch points:
- `crates/volant-broker/src/net.rs` — replace/extend `heartbeat_to_controller`
- Prefer **not** changing opcode wire format
- Tests: `crates/volant-broker/tests/phase134_heartbeat_mesh.rs`

## Critical correctness

Do **not** call `apply_controller_alive_set` on responses from non-controllers —
partial alive lists could shrink ISR incorrectly.

## Exit criteria

1. Non-controller A with journal watermark can catch up lagging peer B via mesh  
2. Controller-only membership SoT preserved (alive-set apply only vs controller)  
3. phase131 offline rejoin + phase132 hardening still green  
4. Docs 0–134 when shipped  

## Honest limitations

- O(N) heartbeats per node per period (static small clusters only)  
- Best-effort sequential peer RPCs; slow peer does not block others if
  implemented with per-peer timeout (reuse `inter_broker_rpc` timeout)  
- Not a full gossip membership protocol  
