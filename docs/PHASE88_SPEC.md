# Phase 88 — Fetch DivergingEpoch + real fetch sessions (MVP)

## Goals

1. **Fetch DivergingEpoch (flexible Fetch v12+)**: When the client’s
   `last_fetched_epoch` and `fetch_offset` indicate the client is past the end
   of that epoch in local durable leader-epoch history, return partition error
   **OFFSET_OUT_OF_RANGE** and populate **tag 0 DivergingEpoch**
   (`epoch` + `end_offset`) from Phase 87 history when possible.
2. **Real fetch sessions (MVP)**: Process-local in-memory sessions keyed by
   `session_id`:
   - Create on full fetch (`session_id == 0` or `session_epoch == INITIAL (0)`)
   - Track topic partitions; honor `forgotten_topics_data` removals
   - Incremental: non-zero `session_id` + positive `session_epoch`; empty topics
     array re-fetches all partitions currently in the session (full record data
     always — no omit-unchanged cache)
   - Session errors: unknown id → **FETCH_SESSION_ID_NOT_FOUND (70)**; wrong
     epoch → **INVALID_FETCH_SESSION_EPOCH (71)**
   - Close on **FINAL** epoch (`-1`); response `session_id = 0`
3. Wire versions already advertised (**Fetch 0–18**); no new max versions.
4. Tests + docs honesty.

## Non-goals

- Byte-identical Kafka incremental omit-unchanged / cached record-set responses
- Multi-broker session stickiness / KRaft session replication
- SnapshotId tagged fields
- Kafka control batches on the data log, real 2PC, multi-lang clients, fuzz CI
- Inventing API max versions beyond `SUPPORTED_APIS`

## Wire

### DivergingEpoch (FetchResponse partition TAG_BUFFER, v12+)

```
tag 0: DivergingEpoch = EpochEndOffset {
  Epoch: INT32
  EndOffset: INT64
}
tag 1: CurrentLeader = LeaderIdAndEpoch { … }   # unchanged (Phase 78)
```

Tags may co-exist (sorted by tag id). Empty TAG_BUFFER when neither applies.

### Session fields (v7+, unchanged framing)

```
Request:  SessionId INT32, SessionEpoch INT32, … ForgottenTopicsData …
Response: ErrorCode INT16, SessionId INT32, Responses …
```

| Request | Behavior |
|---------|----------|
| `session_epoch == -1` (FINAL) | Close `session_id` if present; full fetch from request topics; response session **0** |
| `session_id == 0` or `session_epoch == 0` (INITIAL) | Full fetch; **create** session from request partitions; response = new id |
| `session_id != 0` and `session_epoch > 0` | Incremental: validate id+epoch; merge request partitions; apply forgotten; if topics empty, use session cache |
| Unknown session id | Top-level error **70**, empty responses, echo request session id |
| Epoch mismatch | Top-level error **71**, empty responses, echo request session id |

Session epoch protocol (Kafka-compatible subset):

- On create: store `expected_epoch = 1`
- On successful incremental with request epoch `E`: require `E == expected`; then
  `expected = E+1` (wrap `MAX→1`; never 0)
- Client advances its epoch after each successful response

### DivergingEpoch semantics

For Fetch **v12+** when `last_fetched_epoch != -1`:

1. Resolve end offset for `last_fetched_epoch` via Phase 87 history
   (`offset_for_leader_epoch` / largest epoch ≤ requested)
2. If resolution succeeds and `fetch_offset > end_offset`:
   - Partition **error = OFFSET_OUT_OF_RANGE (1)**
   - Empty records
   - HWM / LSO / log_start still filled when partition known
   - **DivergingEpoch** tag: found `(epoch, end_offset)`
3. Otherwise: normal fetch path (fencing via `current_leader_epoch` first)

Prefer correctness + honesty over full Kafka parity (e.g. no ReplicaManager
throttling, no tiered-storage divergence).

## Exit criteria

1. After epoch bump mid-log, Fetch v12+ with `last_fetched_epoch` = prior and
   `fetch_offset` past prior end → OFFSET_OUT_OF_RANGE + DivergingEpoch tag
2. Session create (`id=0,epoch=0`) returns non-zero session id
3. Incremental empty-topics fetch returns data for session partitions
4. `forgotten_topics_data` drops partitions from session
5. Invalid session id / epoch → top-level 70 / 71
6. FINAL epoch → response session id 0 (Kafka-correct; prior “echo” tests updated)
7. `cargo test` green for broker phase tests + workspace

## Honest limitations

- Sessions are **process-local** (lost on restart; not sticky across brokers)
- Always return full record data for included partitions (no omit-unchanged)
- No max session count / eviction policy beyond simple HashMap
- DivergingEpoch only from durable history + fetch_offset comparison (no
  replica log-truncation RPC path)
- SnapshotId / PreferredReadReplica still unused (-1)
- Single-node session manager; no inter-broker share

## Test plan

`crates/volant-broker/tests/phase88_fetch_sessions_diverging.rs`:

1. DivergingEpoch path after epoch bump
2. Create session + incremental empty topics
3. Forgotten topics removes partition
4. Invalid session id → 70; wrong epoch → 71

## Deferred (Phase 89+)

- Omit-unchanged incremental responses / response size caps
- Session TTL / max concurrent sessions / metrics
- Multi-broker session affinity
- SnapshotId; Kafka control batches; real 2PC; multi-lang; fuzz CI
