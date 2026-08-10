# Phase 136 — Non-blocking admin catch-up (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Ops quality / stall resistance for Phase 117 ACL + BROKER config
rejoin catch-up. Mirrors Phase 132 (journal catch-up hardening). Does **not**
change controller SoT, does **not** add Raft, does **not** change opcodes.

## Goals

1. **Non-blocking catch-up:** Do not await config/ACL re-push RPCs inside the
   `HeartbeatBroker` request path in a way that stalls membership heartbeats
   under a slow or black-holed peer. Prefer spawn + bounded timeout +
   single-flight per peer so the HeartbeatBroker response returns promptly.
2. **Catch-up throttle / coalescing:** Avoid re-pushing full admin state on
   every lagging heartbeat while a push is in-flight or within a min interval
   per peer.
3. **Metrics honesty:** Keep `volant_cluster_admin_catchup_success_total` /
   `_errors_total` meaningful under an async/throttled path; add
   `volant_admin_catchup_skipped_total` for schedule skips.
4. Keep direct `catch_up_peer_admin_state` public for tests and explicit callers.
5. Keep `phase117_admin_catchup` green (direct API + background rejoin paths).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Raft / multi-master ACL/config | Out of scope; controller SoT remains |
| Peer-to-peer admin catch-up mesh | Admin catch-up stays controller-gated |
| New opcodes or Heartbeat trailer fields | Reuse Phase 113 opcodes 72–75 |
| Shared fetch session store / preferred selector | Orthogonal |
| Editing ROADMAP/TODO/README/INDEX/PHASE_HISTORY | Parent orchestration |

## Implementation (landed)

Hot path (`crates/volant-broker/src/net.rs` HeartbeatBroker arm):

- On lag (`peer_admin_gens_lag` while controller), call
  [`schedule_catch_up_peer_admin_state`](../crates/volant-broker/src/net.rs)
  — **non-blocking** schedule, not `await catch_up_peer_admin_state`.
- Scheduler claims per-peer **single-flight** + **min-interval** via
  `Broker::try_begin_admin_catchup` / `finish_admin_catchup`.
- Spawned task runs `catch_up_peer_admin_state` (opcodes **72–75**) with an
  outer timeout (`inter_broker_rpc_timeout * 2 + 1s`; admin may do config + ACL
  RPCs); always releases single-flight.
- Direct/sync `catch_up_peer_admin_state` remains for tests and explicit
  callers.

### Knobs

| Knob | Default | Meaning |
|------|---------|---------|
| `VOLANT_ADMIN_CATCHUP_MIN_INTERVAL_MS` | **500** | Min time between admin catch-up **starts** for the same peer; `0` = no time throttle (single-flight still applies) |

Constant: `DEFAULT_ADMIN_CATCHUP_MIN_INTERVAL_MS` / `admin_catchup_min_interval_ms()`.

Independent of `VOLANT_JOURNAL_CATCHUP_MIN_INTERVAL_MS` (Phase 132) for clarity.

### Metrics

| Metric | Meaning |
|--------|---------|
| `volant_cluster_admin_catchup_success_total` | Successful catch-up RPC applies (unchanged; Phase 117) |
| `volant_cluster_admin_catchup_errors_total` | Failed / timed-out catch-up RPCs |
| `volant_admin_catchup_skipped_total` | Schedule skipped (in-flight or min-interval) |

## Design

```text
  Before (Phase 117):
    HeartbeatBroker handler
      → lag? → await catch_up_peer_admin_state (up to 2 RPCs)
      → return HeartbeatBroker response

  After (Phase 136):
    HeartbeatBroker handler
      → lag? → schedule catch-up (spawn / single-flight / min-interval)
      → return HeartbeatBroker response promptly
      ⋮
      async catch-up task
        → ClusterBrokerConfig / ClusterAclSnapshot with bounded timeout
        → peer apply; success/error metrics
        → finish single-flight
```

- Lag still detected via `applied_config_generation` /
  `applied_acl_generation` (Phase 117 trailer).
- Failure still retries on later heartbeats (when throttle/single-flight
  allows another attempt).
- Controller remains SoT; non-controller schedules no-op.
- Per-peer single-flight: at most one in-flight admin catch-up per peer;
  min-interval coalesces chattiness under repeated lagging heartbeats.

## Honest limitations

- Still not Raft; controller-centric heartbeats remain for admin SoT.
- Throttle may add brief lag vs Phase 117’s chatty inline path.
- Admin catch-up is still controller-only (unlike multi-controller journal
  catch-up Phase 131/134).
- Generation remains a weak process-local counter (durable under
  `__cluster_admin`, not a commit index).

## Exit criteria

1. ✅ HeartbeatBroker handler does not block membership on slow admin catch-up RPC  
2. ✅ Per-peer throttle / single-flight prevents duplicate concurrent full pushes  
3. ✅ New ITs for single-flight / black-hole heartbeat / schedule restore green  
4. ✅ `phase117_admin_catchup` + `phase132_journal_catchup_hardening` still green  
5. ✅ Docs updated when shipped  

## Tests

**Formal Phase 136 (this ship):**

- `crates/volant-broker/tests/phase136_admin_catchup_hardening.rs`
  - single-flight + min-interval (`try_begin_admin_catchup`)
  - HeartbeatBroker not blocked by black-hole peer
  - schedule restores BROKER config
  - direct `catch_up_peer_admin_state` still works

**Regression:**

- `phase117_admin_catchup` — offline rejoin + durable gens (direct API fallback
  still present)
- `phase132_journal_catchup_hardening` — journal path unchanged

## Protocol

No new opcodes or HeartbeatBroker trailer fields. Catch-up still uses Phase 113
opcodes driven by Phase 117 applied-generation lag detection. Dispatch paths
take `&Arc<Broker>` so catch-up can be spawned from the request handler.
