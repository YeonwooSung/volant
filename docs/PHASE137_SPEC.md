# Phase 137 — DeleteRecords request wait flag + journal GC hygiene

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Close two Phase 135 / journal residuals without Raft or KIP-890:
(1) native **per-request** majority-wait override, (2) cluster-correct
truncate-journal prune so deleted topics do not linger or resurrect via push.

## Goals

1. **Native request-level wait flag (optional trailer):**  
   `Request::DeleteRecords` gains `wait_majority: u8` after `before_offset`.
   - `0` (absent trailer / default) → broker knob
     (`VOLANT_DELETE_RECORDS_WAIT_MAJORITY` / `delete_records_wait_majority()`)
   - `1` → force wait on for this request
   - `2` → force wait off for this request  
   Encode always writes the `u8`. Decode: if `src.remaining() >= 1` read it,
   else `0` (legacy clients).
2. **Effective wait helper:**  
   `Broker::effective_delete_records_wait_majority(flag: u8) -> bool`  
   used by native DeleteRecords handler. Metrics still only when effective
   wait is on (same counters as Phase 135).
3. **Kafka path unchanged:** no invented Kafka wire field; broker knob only.
4. **Client + CLI:**  
   - `Client::delete_records` defaults flag `0`  
   - `Client::delete_records_with_wait_flag(…, wait_majority: u8)` or
     `delete_records_with_options`  
   - CLI: `--wait-majority` / `--no-wait-majority` on `topic delete-records`
5. **Journal peer prune on assignment apply:**  
   When `apply_cluster_state` removes topics from the assignment, call
   `TruncateJournal::remove_topic` for each removed name so peers that never
   ran local `delete_topic` still drop watermarks.
6. **Anti-resurrection on push:**  
   `apply_push` (via filtered path used by `handle_truncate_journal_push`)
   skips entries whose topic is **not** in the known set:
   - cluster: current assignment topic names  
   - single-node: local topics map  
   Existing unit tests that call `TruncateJournal::apply_push` directly keep
   accepting all keys (`known_topics = None`).
7. Tests + living docs 0–137.

## Non-goals

| Deferred | Why |
|----------|-----|
| Rollback local truncate on majority fail | Storage deletes segment files; no undo |
| Defer local truncate until majority (provisional note) | Larger redesign of note timing |
| Shared fetch session store / full preferred selector | Orthogonal |
| Full openraft / KRaft / KIP-890 | Out of scope |
| Kafka per-request wait field | Not in Kafka wire; env/broker only |
| Tombstone generation for pruned journal | Max-merge + known-topic filter is enough |
| Remove orphan local topic objects on assignment | Broader lifecycle; journal-only this phase |

## Design

### Wait flag

```text
  native DeleteRecords (opcode 44)
       topic | partition | before_offset | [wait_majority u8]
                                              │
                    0 → broker AtomicBool (env default)
                    1 → force true
                    2 → force false
                                              ▼
                    effective_wait?
                       │
           ┌───────────┴───────────┐
           no                      yes
           client err = local      await fanout; majority fail → 15
           (low still achieved; no rollback — same as Phase 135)
```

### Journal hygiene

```text
  apply_cluster_state(generation, topics)
       old = assignment.topics keys
       apply_wire(...)
       removed = old \ new
       for t in removed: truncate_journal.remove_topic(t)
       apply_local_assignment()

  handle_truncate_journal_push(snapshot)
       known = assignment topics (cluster) | local topics (solo)
       apply_push_filtered(..., Some(known))
         → skip entries with topic ∉ known  (anti-resurrection)
```

## Honest limitations

- Local truncate still irreversible when wait fails (Phase 135 residual).
- Journal majority still over **configured N** (N=2 one-down trap).
- Known-topic filter can briefly skip watermarks for a brand-new topic until
  assignment/local catalog includes it; later catch-up push applies them.
- Prune does not bump journal generation (same as `remove_topic` today).
- Kafka clients cannot pass per-request wait.

## Exit criteria

1. [x] Legacy DeleteRecords payload (no trailer) decodes `wait_majority=0`  
2. [x] Flag `1` forces wait when env off; majority fail → native **15** + fail metric  
3. [x] Flag `2` forces no-wait when env on; solo majority fail → error **0**  
4. [x] Kafka path still broker-knob only  
5. [x] `apply_cluster_state` removing a topic prunes peer journal entries  
6. [x] Push cannot reintroduce watermarks for unknown/deleted topics  
7. [x] `phase135_*` green; new `phase137_*` green  
8. [x] Living docs + TODO residual updated  

## Tests

**Formal Phase 137:**

- `crates/volant-protocol` unit: legacy decode + trailer roundtrip (0/1/2)
- `crates/volant-broker/tests/phase137_delete_records_request_wait_flag.rs`
  - flag 1 forces wait (env off) → NotEnoughReplicas on solo N=3
  - flag 0 uses broker default off → success
  - flag 2 forces no-wait (env on) → success; wait metrics unchanged
  - flag 1 + 3 live → majority ok + success metric
- `crates/volant-broker/tests/phase137_journal_topic_gc.rs`
  - assignment remove → journal prune
  - apply_push with known filter skips unknown topic
  - delete_topic local prune still works (regression)

**Regression:** `phase135_*`, `phase129–134` journal suites.

## Protocol

| Field | Wire |
|-------|------|
| Opcode | 44 (unchanged) |
| Body | `string topic` + `u32 partition` + `u64 before_offset` + optional `u8 wait_majority` |
| Response | Unchanged (opcode 45) |

No new opcodes.

## Implementation notes (shipped)

- Protocol: native opcode **44** body `topic | partition | before_offset | [wait_majority u8]`;
  encode always writes the `u8`; decode missing trailer → `0` (legacy).
- Broker: `Broker::effective_delete_records_wait_majority(flag)` —
  `0` = env/`delete_records_wait_majority()`, `1` force on, `2` force off;
  native DeleteRecords uses effective wait; metrics only when effective on
  (same `volant_delete_records_majority_wait_{success,fail}_total` as Phase 135).
- Kafka path: unchanged — `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` broker knob only
  (no invented Kafka wire field).
- Client: `delete_records` → flag `0`; `delete_records_with_wait_flag(…, wait_majority)`.
- CLI: `volant topic delete-records … --wait-majority` / `--no-wait-majority`.
- Journal: `apply_cluster_state` calls `TruncateJournal::remove_topic` for topics
  dropped from assignment; `handle_truncate_journal_push` uses
  `apply_push_filtered(..., Some(known_topics))` so unknown/deleted topics cannot
  resurrect watermarks (`apply_push` direct callers keep `known_topics = None`).
- Tests: `phase137_delete_records_request_wait_flag`, `phase137_journal_topic_gc`
  (+ protocol unit roundtrip / legacy decode).
- Residual: no local rollback on majority fail; Kafka still env-only.
