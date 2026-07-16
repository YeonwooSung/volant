# Phase 48 — Kafka Produce classic v0–8

## Goals

1. Raise **Produce** (API key 0) from classic **v0–3** to classic **v0–8**
   (last non-flexible; flexible **v9+** out of scope)
2. Emit response fields required by modern clients:
   - **log_start_offset** (v5+)
   - **record_errors[]** + **error_message** (v8+)
3. Advertise max version **8** in ApiVersions; tests + docs honesty

## Non-goals

- Flexible Produce v9+ (compact records, tagged fields, topic UUID)
- Per-record `record_errors` population (always empty array; partition error_code only)
- `log_append_time` semantics (always **-1**; CreateTime topics)
- CurrentLeader / NodeEndpoints (v10+ tagged, flexible-only path)
- Fetch classic bumps (still 0–4; separate phase)

## Wire summary

### Request (stable v3–8)

```
transactional_id: NULLABLE_STRING   # v3+
acks: INT16
timeout_ms: INT32
topics: [{ name, partitions: [{ index, records }] }]
```

| Ver | Notes |
|-----|--------|
| v0–2 | no transactional_id; MessageSet common |
| v3+ | transactional_id + RecordBatch (MessageSet still accepted) |
| v4 | wire-identical to v3 (KAFKA_STORAGE_ERROR readiness) |
| v5–6 | request same as v3 |
| v7 | ZStd allowed in batches (Volant already supports; no new fields) |
| v8 | request same; richer response |

### Response

```
topics: [{
  name
  partitions: [{
    index, error_code, base_offset,
    log_append_time_ms: INT64     # v2+ (always -1)
    log_start_offset: INT64       # v5+ (partition earliest, or -1)
    record_errors: [{             # v8+ (always empty)
      batch_index, batch_index_error_message
    }]
    error_message: NULLABLE_STRING # v8+ (always null)
  }]
}]
throttle_time_ms: INT32           # v1+ (trailing; always 0)
```

## Exit criteria

1. ApiVersions: Produce max **8**
2. Produce v5 response includes log_start_offset after log_append_time
3. Produce v8 response includes empty record_errors + null error_message + trailing throttle
4. Produce v0–3 still work (phase23/24/28/29/31)
5. Produce v9 → UnsupportedVersion
6. phase24 ApiVersions assert updated; phase48 tests green

## Honest limitations

- No flexible Produce; clients needing v9+ must stay on classic max 8
- record_errors never populated (batch-level error_code only)
- log_append_time always -1
- ZStd is batch-level (phase28); not gated on Produce version
