# Phase 129 — Controller SoT DeleteRecords truncate journal (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** durable `__truncate_journal` + max-merge watermarks — **landed**  
- **PR2** opcodes 86/87 note + 88/89 push + controller fan-out — **landed**  
- **PR3** reconcile uses `max(local log_start, journal watermark)` — **landed**  
- **PR4** tests + living docs — **landed**  
**Theme:** Close the Phase 123 honesty gap — when a **new leader never applied**
the local truncate (was offline during DeleteRecords), it can still drive peer
catch-up using a **controller-owned truncate watermark journal**.

## Goals

1. **Controller SoT journal:** Durable per `(topic, partition)` desired
   `before_offset` (max-merge) under `{data_dir}/__truncate_journal/state.json`.
2. **Note after DeleteRecords:** Leader best-effort notes controller (local if
   self is controller, else `TruncateJournalNote` RPC); controller bumps
   generation and pushes snapshot to live peers (`TruncateJournalPush`).
3. **Peer apply:** Install snapshot when `generation >= applied`.
4. **Reconcile integration:** New/existing leader outbox rebuild target =
   `max(local log_start, journal watermark)`.
5. Client DeleteRecords success still local-leader only (unchanged).
6. Tests + living-docs honesty.

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Raft / multi-controller merge | Static membership; controller failovers may re-seed from peer cache only if re-noted |
| Sync 2PC truncate (client waits) | Rejected since 113 |
| Heartbeat lag re-push of journal | Stretch; peers that miss push need re-note / re-DeleteRecords / manual |
| Multi-lang / chaos-mesh | Orthogonal |

## Problem (post Phase 123)

```text
  DeleteRecords on leader A → outbox for offline C
  A dies; B elected but B was also offline during truncate
  B local log_start stale → reconcile invents nothing
  C never catches up
```

## Design

```text
DeleteRecords OK on leader
  → fanout_truncate_journal_note (controller merge + push)
  → ReplicaDeleteRecords peers + outbox (116)

reconcile_delete_records_outbox:
  target = max(local log_start, journal.watermark)
  enqueue peers at target
```

## Wire

| Opcode | Direction | Payload |
|-------:|-----------|---------|
| 86 / 87 | Leader → controller | topic, partition, before_offset, leader_epoch → generation |
| 88 / 89 | Controller → peer | generation + JSON snapshot |

## Honest limitations

- Not Raft; controller data_dir loss loses SoT until re-note
- Best-effort note/push (client path never waits)
- No dedicated heartbeat journal catch-up (missed push → wait for next note/reconcile path)
- Single-node unchanged (no cluster)

## Exit criteria

1. Journal durable + max-merge  
2. Controller note bumps gen; push installs on peers  
3. Reconcile enqueues at journal watermark when log_start stale  
4. `phase129_truncate_journal` + unit tests pass  
5. Living docs 0–129 honest  
