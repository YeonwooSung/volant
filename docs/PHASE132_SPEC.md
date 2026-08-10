# Phase 132 — Truncate journal catch-up hardening (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Ops quality / stall resistance / test depth for Phase 131 journal
rejoin catch-up. Does **not** change max-merge journal SoT, does **not** add
Raft, does **not** add a peer-to-peer heartbeat mesh.

## Goals

1. **Non-blocking catch-up:** Do not await a full journal push inside the
   `HeartbeatBroker` request path in a way that stalls membership heartbeats
   under a large snapshot or slow peer. Prefer spawn + bounded timeout,
   single-flight per peer, or equivalent so the HeartbeatBroker response
   returns promptly.
2. **Catch-up throttle / coalescing:** Avoid re-pushing a full snapshot on
   every lagging heartbeat while a push is in-flight or within a min interval
   per peer (document knobs if any; defaults must be safe).
3. **Wire IT depth for journal opcodes beyond note fence:** Integration
   coverage for opcodes **87–89** and especially the **push (88)** path
   (majority note round, push apply, catch-up path), beyond residual note-fence
   and auth ITs (see Tests / Naming).
4. **Metrics honesty:** Keep `volant_journal_catchup_success_total` /
   `_errors_total` meaningful under an async/throttled path; optional
   `throttled` / `skipped` metric only if cheap and useful (not required for
   MVP).
5. **Living docs honesty** when shipping (0–132).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Full openraft / KRaft truncate log | Larger; max-merge remains SoT |
| Peer-to-peer heartbeat mesh | Heartbeats remain controller-centric |
| Sync client wait on DeleteRecords majority | Latency; keep best-effort client |
| Preferred selector full Kafka parity | Separate candidate Phase 133 |
| Shared fetch session store | Orthogonal deferred |
| Full KIP-890/939 / `__transaction_state` | Orthogonal |
| Closing honest residual current-epoch forge under auth off | Document only (Phase 130 follow-up residual) |

## Implementation (landed)

Hot path (`crates/volant-broker/src/net.rs` HeartbeatBroker arm):

- On lag (`peer_journal_gen_lags`), call
  [`schedule_catch_up_peer_truncate_journal`](../crates/volant-broker/src/net.rs)
  — **non-blocking** schedule, not `await catch_up_peer_truncate_journal`.
- Scheduler claims per-peer **single-flight** + **min-interval** via
  `Broker::try_begin_journal_catchup` / `finish_journal_catchup`.
- Spawned task runs `catch_up_peer_truncate_journal` (opcode **88**) with an
  outer timeout (`inter_broker_rpc_timeout + 1s`); always releases single-flight.
- Direct/sync `catch_up_peer_truncate_journal` remains for tests and explicit
  callers.

### Knobs

| Knob | Default | Meaning |
|------|---------|---------|
| `VOLANT_JOURNAL_CATCHUP_MIN_INTERVAL_MS` | **500** | Min time between catch-up **starts** for the same peer; `0` = no time throttle (single-flight still applies) |

Constant: `DEFAULT_JOURNAL_CATCHUP_MIN_INTERVAL_MS` / `journal_catchup_min_interval_ms()`.

### Metrics

| Metric | Meaning |
|--------|---------|
| `volant_journal_catchup_success_total` | Successful catch-up pushes (unchanged) |
| `volant_journal_catchup_errors_total` | Failed / timed-out catch-up pushes |
| `volant_journal_catchup_skipped_total` | Schedule skipped (in-flight or min-interval) |

## Design

```text
  Before (Phase 131):
    HeartbeatBroker handler
      → lag? → await catch_up_peer_truncate_journal (full push RPC)
      → return HeartbeatBroker response

  After (Phase 132):
    HeartbeatBroker handler
      → lag? → schedule catch-up (spawn / single-flight / min-interval)
      → return HeartbeatBroker response promptly
      ⋮
      async catch-up task
        → TruncateJournalPush (88) with bounded timeout
        → peer max-merge; success/error metrics
        → finish single-flight
```

- Lag still detected via `applied_journal_generation` (Phase 131 trailer).
- Failure still retries on later heartbeats (when throttle/single-flight
  allows another attempt).
- Max-merge never shrinks watermarks (Phase 129/130).
- Per-peer single-flight: at most one in-flight full push per peer; min-interval
  coalesces chattiness under repeated lagging heartbeats.

## Honest limitations

- Still not Raft; controller-centric heartbeats remain.
- Throttle may add brief lag vs Phase 131’s chatty inline path.
- Auth/ACL gate on 86/88 remains a production requirement (locked by residual
  `phase133_journal_auth`).
- Generation remains a weak process-local counter (not a commit index).

## Exit criteria

1. ✅ HeartbeatBroker handler does not block membership on slow catch-up RPC  
2. ✅ Per-peer throttle / single-flight prevents duplicate concurrent full pushes  
3. ✅ New ITs for push / majority / catch-up wire paths green  
4. ✅ phase129–131 + residual `phase132_journal_note_fence` /
   `phase133_journal_auth` still green  
5. ✅ Docs updated when shipped  

## Tests

**Formal Phase 132 (this ship):**

- `crates/volant-broker/tests/phase132_journal_catchup_hardening.rs`
  - single-flight + min-interval
  - HeartbeatBroker not blocked by black-hole peer
  - schedule restores watermark
  - push wire apply
  - majority note + push depth

**Naming collision (do not confuse):** residual post-131 fix suites already
exist under phase-numbered filenames. They are **not** formal Phase 132/133
ship records:

| File | What it is |
|------|------------|
| `phase132_journal_note_fence` | Residual epoch/existence fence on note (86); Phase 130 follow-up |
| `phase133_journal_auth` | Residual ACL/auth gates on 86/88; Phase 130 follow-up |

## Protocol

No new opcodes or HeartbeatBroker trailer fields. Catch-up still uses opcode
**88** `TruncateJournalPush` driven by Phase 131
`applied_journal_generation` lag detection. Dispatch paths take
`&Arc<Broker>` so catch-up can be spawned from the request handler.
