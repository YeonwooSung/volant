# Phase 148 — Defer local DeleteRecords truncate until journal majority (MVP)

**Status:** ✅ Shipped  
**Theme:** When effective **wait_majority** is on, do not destroy local log
data until truncate-journal majority commits. Closes the Phase 135 residual
where majority fail returned `NotEnoughReplicas` but local data was already gone.

## Goals

1. **Wait mode ordering:** journal majority note **first**, then local
   `delete_records`, then replica/outbox fan-out.
2. **Majority fail:** return native `NotEnoughReplicas` (**15**) / Kafka **19**;
   `low_watermark` = **current** `log_start` (unchanged). **No local truncate.**
3. **Wait off (default):** keep existing **local-first** best-effort behavior
   (client success independent of majority; no client fail on majority miss).
4. Metrics for majority-first path + living docs/tests.

## Non-goals

| Deferred | Why |
|----------|-----|
| Rollback after wait-off local truncate | Segment files already deleted; irreversible |
| Full 2PC truncate / Raft log | Larger; journal majority MVP remains |
| Wait for every replica **log** truncate | Journal SoT; outbox still best-effort |
| Phases 145–147 | Out of scope for this slice |

## Design

```text
  effective wait_majority?
       │
       ├─ OFF (default / trailer 2):
       │     local delete_records → fanout_delete_records (journal + replicas)
       │     client error = local only (majority miss ignored)
       │
       └─ ON (env or trailer 1):
             preflight leader + log_start (no mutate)
                  │
                  ▼
             fanout_truncate_journal_note_provisional(note_offset)
               note_offset = min(before_offset, LEO)
                  │
                  ├─ majority fail → roll back provisional local journal entry
                  │                  return 15 + log_start unchanged
                  │                  metrics: wait_fail + majority_first_fail
                  │
                  └─ majority ok  → local delete_records
                                    fanout_delete_records_replicas_only (no re-note)
                                    metrics: wait_success + majority_first_success
```

### Clamp honesty

Journal may note client `before_offset` (or `min(before, LEO)`) while local
whole-segment clamp achieves a **lower** low. Max-merge keeps journal ≥ local;
reconcile targets journal watermark and re-clamps.

### Provisional note rollback

On wait-mode majority fail the local provisional journal watermark is restored
to its prior value (or removed) so `reconcile_delete_records_outbox` does not
auto-apply a non-majority note. Peers that acked a partial note may still hold
a provisional max-merge entry (honest residual under weak partial connectivity).

## Behavior matrix

| Effective wait | Journal majority | Local truncate | Client error | `low_watermark` |
|----------------|------------------|----------------|--------------|-----------------|
| OFF | ok or fail | **Yes** (first) | 0 if local ok | achieved local low |
| ON | ok | **Yes** (after majority) | 0 | achieved local low |
| ON | fail | **No** | 15 / Kafka 19 | current log_start |

## Metrics

| Metric | When |
|--------|------|
| `volant_delete_records_majority_wait_success_total` | Effective wait + majority ok (Phase 135/148) |
| `volant_delete_records_majority_wait_fail_total` | Effective wait + majority fail (**no truncate** as of 148) |
| `volant_delete_records_majority_first_success_total` | Phase 148 wait-mode majority-first success |
| `volant_delete_records_majority_first_fail_total` | Phase 148 wait-mode majority-first fail |

## Honest limitations

- Wait-**off** path still local-first (no undo on later majority miss).
- Journal majority still over **configured N** (N=2 one-down trap; Phase 141 gauges).
- Partial peer acks without majority may leave peer journal entries (max-merge).
- Replica log truncate + outbox remain best-effort after local success.
- Kafka still env/broker knob only (no per-request wire field; Phase 137).

## Exit criteria

1. [x] Wait on + 3/3 live → success, log truncated, majority-first success++  
2. [x] Wait on + majority impossible → 15, log_start unchanged after reconcile tick  
3. [x] Wait off + majority would fail → local truncate still succeeds  
4. [x] phase135 / phase137 adapted to no-truncate-on-wait-fail  
5. [x] Living docs (ops / consistency / features / TODO / ROADMAP)

## Tests

- `crates/volant-broker/tests/phase148_defer_truncate_majority.rs`
- Regression: `phase135_delete_records_majority_wait`, `phase137_delete_records_request_wait_flag`

## Implementation notes (shipped)

- `fanout_truncate_journal_note_provisional` — majority note with local rollback on fail  
- `fanout_delete_records_replicas_only` — skip re-note after majority-first local truncate  
- `Broker::delete_records_leader_log_start` / `delete_records_note_offset`  
- Native + Kafka wait paths reordered; wait-off path unchanged local-first  
- Metrics: `majority_first_{success,fail}_total` + existing wait counters  
