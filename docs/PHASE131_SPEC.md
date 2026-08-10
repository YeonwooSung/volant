# Phase 131 — Truncate journal rejoin catch-up (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Cluster correctness — peers that miss Phase 129/130 journal note/push
(offline, flaky RPC, restart) **converge** after rejoin via heartbeat lag
detection + best-effort `TruncateJournalPush`, instead of permanent watermark
drift until the next DeleteRecords.

## Goals

1. **Heartbeat piggyback:** senders report `applied_journal_generation` on
   `HeartbeatBroker` (backward-compatible trailer after Phase 117 config/ACL gens).
2. **Lag-driven re-push:** any node receiving a heartbeat whose local journal
   generation is ahead of the peer’s applied gen re-pushes a full max-merge
   snapshot (opcode **88** `TruncateJournalPush`).
3. **Multi-controller:** catch-up is **not** controller-gated (any broker with
   newer journal may push). Heartbeats still target the controller today, so
   the common path is controller → rejoining peer.
4. Metrics: `volant_journal_catchup_success_total` /
   `volant_journal_catchup_errors_total`.
5. Integration tests: direct catch-up API + offline peer rejoin.
6. Living docs honesty (0–131).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Full Raft / openraft journal log | Larger; max-merge remains SoT |
| Peer-to-peer heartbeats (mesh) | **closed by Phase 134** |
| Sync wait on client DeleteRecords | Latency; keep best-effort client |
| Catch-up await stall / per-peer throttle / single-flight | **closed by Phase 132** |
| Shared fetch session store / full preferred selector | Orthogonal deferred |
| Full KIP-890/939 / `__transaction_state` | Orthogonal |

## Protocol

| Field | Wire |
|-------|------|
| Existing | `broker_id`, `controller_id_known`, `generation` |
| Phase 117 | `applied_config_generation`, `applied_acl_generation` (optional trailer) |
| **Phase 131** | `applied_journal_generation` (optional trailing `u64` after ACL gen) |

Decode compatibility:

- `< 16` trailing bytes → all applied gens `0` (legacy)
- `≥ 16` and `< 24` → config+ACL, journal `0` (Phase 117 peers)
- `≥ 24` → config+ACL+journal

## Design

```text
  peer (rejoin) ──HeartbeatBroker(applied_journal_gen=0)──▶ controller
                                                              │
                                              local_gen > peer_applied?
                                                              │
                                                              ▼
                                              TruncateJournalPush (88)
                                                              │
                                                              ▼
                                                         peer max-merge
```

- Lag predicate: `local_gen > peer_applied && (local_gen > 0 || entry_count > 0)`.
- No extra throttle; failed apply retries on the next heartbeat.
- Max-merge never shrinks watermarks (Phase 129/130).

## Honest limitations

- Not Raft; brief lag windows until the next successful heartbeat catch-up.
- ~~Controller-centric heartbeats: a non-controller that alone holds a watermark
  the controller never saw still relies on the DeleteRecords fan-out path /
  later multi-controller notes to reach the controller.~~ **closed by Phase 134**
  (mesh heartbeats; non-controller → non-controller lag catch-up).
- Generation is a weak process-local counter (not a commit index).

## Exit criteria

1. Heartbeat encodes/decodes `applied_journal_generation` with legacy defaults  
2. Direct `catch_up_peer_truncate_journal` restores peer watermark + metric  
3. Offline peer rejoins and receives journal via heartbeat path  
4. phase129/130 still green  
5. Docs 0–131 honest  

## Tests

- `crates/volant-broker/tests/phase131_journal_catchup.rs`
- Protocol trailer cases in `volant-protocol` HeartbeatBroker round-trip
