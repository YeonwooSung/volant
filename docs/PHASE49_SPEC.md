# Phase 49 — Kafka Fetch classic v0–11

## Goals

1. Raise **Fetch** (API key 1) from classic **v0–4** to classic **v0–11**
   (last non-flexible; flexible **v12+** out of scope)
2. Parse additive request fields; emit response fields modern clients expect
3. Advertise max version **11** in ApiVersions; tests + docs honesty

## Non-goals

- Flexible Fetch v12+ (compact strings, topic UUID, LastFetchedEpoch, tagged fields)
- Real incremental fetch sessions (session_id/epoch accepted; always full response)
- Rack-aware preferred read replica routing (always **-1**)
- True READ_COMMITTED LSO ≠ HWM / control markers (Phase 36 honesty unchanged)
- Follower replication Fetch semantics beyond ignoring `log_start_offset`

## Wire summary

### Request

| Ver | Additive |
|-----|----------|
| v0–2 | replica_id, max_wait, min_bytes, topics[partitions[partition, fetch_offset, max_bytes]] |
| v3 | max_bytes (request-level) |
| v4 | isolation_level |
| v5–6 | per-partition log_start_offset (follower; ignored) |
| v7–8 | session_id, session_epoch; forgotten_topics_data after topics |
| v9–10 | current_leader_epoch per partition (fencing) |
| v11 | rack_id (ignored) |

### Response

| Ver | Additive |
|-----|----------|
| v1+ | leading throttle_time_ms |
| v4+ | last_stable_offset, aborted_transactions[] (always empty; LSO≡HWM) |
| v5+ | log_start_offset |
| v7+ | top-level error_code + session_id (before topics) |
| v11+ | preferred_read_replica (always -1) before records |

Partition field order (classic):

```
index, error, hwm,
lso (v4+),
log_start (v5+),
aborted[] (v4+),
preferred_read_replica (v11+),
records
```

## Behavior notes

- **Session**: no incremental state; response `session_id` echoes request (or 0)
- **Forgotten topics**: parsed and discarded
- **Leader epoch fence** (v9+): same rules as ListOffsets / OffsetForLeaderEpoch
  (`-1` = no fence; too high → UNKNOWN_LEADER_EPOCH; stale → FENCED_LEADER_EPOCH)
- **Record encoding**: v0–3 MessageSet; v4–11 RecordBatch (compression unchanged)
- **ZStd**: allowed in batches from v10 on Kafka; Volant already supports at batch level

## Exit criteria

1. ApiVersions: Fetch max **11**; Produce stays max **8**
2. Fetch v5 includes log_start_offset after LSO
3. Fetch v7 response has top-level error + session_id before topics
4. Fetch v11 preferred_read_replica = -1; rack_id parsed
5. Fetch v9 fences on current_leader_epoch when applicable
6. Fetch v12 → UnsupportedVersion
7. phase24/32/36 still green; phase49 tests green

## Honest limitations

- No flexible Fetch; no topic IDs
- No real fetch sessions / partial responses
- preferred_read_replica always -1
- LSO always equals HWM; aborted_transactions always empty
