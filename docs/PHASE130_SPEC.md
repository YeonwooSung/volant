# Phase 130 — Multi-controller majority consensus for truncate journal (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** any-broker durable note (drop controller-only gate) — **landed**  
- **PR2** majority quorum on configured N + metrics — **landed**  
- **PR3** best-effort snapshot push after note (always) — **landed**  
- **PR4** max-merge push apply + tests/docs — **landed**  
**Theme:** Raft-style **majority commit** for DeleteRecords truncate watermarks
without embedding a full Raft log. Multi-controller: every broker is a voting
replica for the journal. Best-effort note/push retained for availability.

## Goals

1. **Multi-controller:** Any live broker durable-merges `TruncateJournalNote`
   (no `NotController` reject).
2. **Majority consensus:** Proposer counts local note + peer note acks; success
   when `acks ≥ floor(N/2)+1` for configured cluster size `N`.
3. **Best-effort push:** After the note round, always snapshot-push to live
   peers so lagging nodes catch up (max-merge apply; never shrink watermarks).
4. **Client path:** DeleteRecords still never fails on journal consensus miss;
   local + partial acks retained; `consensus_fail` metric increments.
5. Metrics: `volant_truncate_journal_consensus_success_total` /
   `_fail_total`.
6. Tests + living-docs honesty.

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Full Raft log / openraft / leader election for metadata | Larger; static membership remains |
| Dynamic membership reconfiguration | Orthogonal to Phase 6 static ISR |
| Sync wait on client DeleteRecords for majority | Latency; keep best-effort client |
| KRaft `__cluster_metadata` | Orthogonal |

## Protocol (unchanged opcodes)

| Step | Mechanism |
|------|-----------|
| Propose | Local `note` + RPC `TruncateJournalNote` (86) to live peers |
| Commit rule | `acks ≥ majority(configured brokers)` |
| Catch-up | Best-effort `TruncateJournalPush` (88) max-merge snapshot |

## Honest limitations

- **Not full Raft:** no replicated log, no term/leader election for journal,
  no linearizable multi-key batching
- Majority uses **static configured N**, not live-only (offline members make
  quorum harder — honest Raft-like)
- Client DeleteRecords does not block on majority
- Controller role for other admin (ACL/BROKER config) unchanged
- Journal is bounded: `MAX_TRUNCATE_JOURNAL_ENTRIES` (100_000) refuses new keys
  at cap; `MAX_TRUNCATE_JOURNAL_SNAPSHOT_BYTES` (4 MiB) rejects oversized push
  apply. Topic delete prunes local journal entries (no generation bump).

## Follow-ups landed (post-MVP residual)

- **Ingress epoch/existence fence** on `handle_truncate_journal_note` (not a new
  phase): empty topic → InvalidArg; `before_offset == 0` → no-op success;
  unknown topic/partition → NotFound; `leader_epoch < 0` (non-zero offset) →
  InvalidArg (journal requires a stamped epoch; fanout never stamps `-1` —
  skips note if not leading); local epoch **strictly greater** than note →
  InvalidProducerEpoch (19); future epochs (`req >= local`) accepted for
  multi-controller lag. Receiver need **not** be leader. Proposer still uses
  `local_note_truncate_journal`; ACL/auth for 86/88 unchanged (ib principal
  **or** Cluster Alter when on).
  **ITs:** `phase132_journal_note_fence` (epoch fence); `phase133_journal_auth`
  (86/88 ACL/auth gates).
- **Still open:** **current-epoch** forge + huge `before_offset` under auth/ACL
  off (or any Cluster Alter / ib principal); push 88 max-merge intentionally
  unfenced for catch-up. Enable cluster auth for 86/88 in production.

## Exit criteria

1. Non-controller accepts note  
2. 3/3 live → consensus success + all nodes have watermark  
3. 1 down of 3 → still can reach majority with 2  
4. Push max-merge never shrinks  
5. phase129 still green; phase130 tests pass  
6. Docs 0–130 honest  
