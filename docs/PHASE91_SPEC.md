# Phase 91 — Omit-unchanged incremental fetch session responses (MVP)

## Goals

1. Track per-session, per-partition **last returned high watermark** and
   **last stable offset** so incremental Fetch can **omit partitions with no
   new data** when the session is valid and the client sends an empty topics
   array (Kafka incremental pattern).
2. When HWM/LSO advanced or new records are available at the session fetch
   offset, **include** that partition (with records, or empty records + updated
   HWM/LSO).
3. Preserve Phase 88 behaviors: create (`session_id=0` / INITIAL epoch),
   forgotten topics, invalid id/epoch errors **70/71**, FINAL closes session
   (`response session_id = 0`).
4. DivergingEpoch, isolation (`READ_COMMITTED` LSO/aborted), control batches,
   and prepared 2PC must not regress.
5. Tests (`phase91_*.rs`) + living docs honesty.

## Non-goals

- Byte-identical Kafka response caching of compressed record sets
- Multi-broker session affinity / durable / replicated sessions
- Session TTL, max concurrent sessions, eviction metrics
- Multi-lang clients, cargo-fuzz corpus CI
- Full multi-broker 2PC / prepared timeout (unless already shipped)
- SnapshotId / PreferredReadReplica changes

## Design (honest MVP)

### Session partition cache fields (process-local)

Extend Phase 88 `SessionPartition` with:

| Field | Meaning |
|-------|---------|
| `last_hwm: Option<i64>` | High watermark last **included** in a response for this partition |
| `last_lso: Option<i64>` | Last stable offset last included (matters under READ_COMMITTED) |

`None` means “never successfully returned in a session response” → always
include on the next opportunity.

### When omit applies

| Request kind | Omit-unchanged? |
|--------------|-----------------|
| Fetch v0–6 (no sessions) | No — full response |
| FINAL epoch (`-1`) | No — full fetch of request topics; session closed |
| Create / INITIAL (`session_id=0` or `epoch=0`) | No — full data; seed `last_hwm`/`last_lso` |
| Incremental + **non-empty** topics | No force-omit — partitions in the request are always included (param update / partial fetch); still refresh `last_*` |
| Incremental + **empty** topics | **Yes** — re-fetch session set; omit unchanged partitions |

### Omit decision (empty-topics incremental only)

For each session partition after the normal fetch path:

**Omit** when all of:

1. Top-level session valid (not error 70/71)
2. Partition error code is **None (0)**
3. Encoded record set is **empty**
4. `Some(current_hwm) == last_hwm` **and** `Some(current_lso) == last_lso`

**Include** when any of:

- Records non-empty (new or re-fetchable data at `fetch_offset`)
- HWM advanced vs last returned
- LSO advanced vs last returned (e.g. txn commit under READ_COMMITTED with no HWM move — rare with write-through, but tracked for honesty)
- Partition error ≠ 0 (fencing, unknown topic, DivergingEpoch / OFFSET_OUT_OF_RANGE, …)
- `last_hwm` / `last_lso` is `None` (first return)

### Include payload choice (MVP)

| Condition | Response content |
|-----------|------------------|
| New records at fetch offset | Records + current HWM/LSO (normal path) |
| No records, but HWM or LSO advanced | **Empty records** + updated HWM/LSO (Kafka-appropriate; client learns log end moved) |
| No records, HWM+LSO unchanged | **Omitted** from response topics/partitions arrays |
| Error / DivergingEpoch | Always included (unchanged Phase 88 wire) |

After a partition is **included**, store `last_hwm` / `last_lso` from that
response (error paths with `hwm < 0` still update only when `error == 0` or
DivergingEpoch keeps real offsets — MVP: update on `error == 0` only; error
partitions do not poison the cache with `-1`).

### Topic-level encoding

- If every partition under a topic is omitted → omit the whole topic entry.
- If every topic is omitted → empty topics array (still success, same
  `session_id`, epoch advanced).

### Unchanged Phase 88 session protocol

| Request | Behavior |
|---------|----------|
| `session_epoch == -1` (FINAL) | Close; full fetch; response session **0** |
| `session_id == 0` or `epoch == 0` | Full fetch; create session; seed cache from response |
| Incremental valid | Validate id+epoch; merge; forget; empty topics → session snapshot + omit |
| Unknown id | Top-level **70**, empty responses |
| Epoch mismatch | Top-level **71**, empty responses |

Epoch advance still happens on successful incremental **before** fetch (Phase 88),
including when the response body omits all partitions.

## Exit criteria

1. Empty-topics incremental with no produce since last include → **0 topics**
   in response (omit)
2. After new produce (HWM advance / records available) → partition **included**
3. Session create / forgotten / 70 / 71 / FINAL still pass (Phase 88 tests)
4. DivergingEpoch + isolation + control batches + 2PC tests green
5. `cargo test --workspace` green
6. Docs: PHASE91_SPEC + KAFKA_COMPAT / ROADMAP / PHASE_HISTORY / WHITEPAPER /
   INDEX / ops / features / README nits

## Honest limitations

- Process-local sessions only (lost on restart; not multi-broker sticky)
- Not byte-identical to Kafka’s cached compressed response reuse
- Omit uses HWM+LSO+empty-records, not a full response fingerprint
- No max session count / TTL / metrics
- Partial-topic incremental always returns those partitions (no omit on that path)
- PreferredReadReplica still -1; SnapshotId unused

## Test plan

`crates/volant-broker/tests/phase91_omit_unchanged_sessions.rs`:

1. Create session at log end (empty records) → include once; empty-topics
   incremental → omit (0 topics)
2. Produce; empty-topics incremental → include partition with new data
3. Sanity: invalid session still 70; create still assigns id

## Deferred (Phase 92+)

- Session TTL / max sessions / metrics
- Multi-broker session affinity
- Byte-level response cache / compressed batch reuse
- Prepared timeout / multi-broker 2PC / TRANSACTION_ABORTABLE
- Multi-lang clients; cargo-fuzz corpus CI
