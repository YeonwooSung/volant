# Phase 71 — Produce TopicId (v10–13)

## Goals

1. **Produce** max **0–13** (flexible from v9; **TopicId from v13**)
2. v10–12: same name-based flexible framing as v9 (KIP-951 tags empty)
3. v13: request/response topics use **TopicId UUID** (no name)
4. Unknown / non-Volant UUID → **UnknownTopicId (100)** per partition
5. Deterministic UUID mapping (same as Metadata/Fetch/admin)
6. Tests + docs honesty

## Non-goals

- Produce v14+
- Populating KIP-951 CurrentLeader / NodeEndpoints tagged fields
- Full TRANSACTION_ABORTABLE semantics (KIP-890)
- Auto-create on produce by TopicId

## Wire summary

### Topic identity

| Version | Request / response topic field |
|---------|--------------------------------|
| ≤v8 classic | STRING name |
| v9–12 flexible | COMPACT_STRING name |
| **v13** | **UUID TopicId** |

Partition records framing unchanged from v9 (compact records + empty tags).

### TopicId mapping

```
bytes 0–5:  "volant"
bytes 6–11: 0
bytes 12–15: big-endian u32 Volant TopicId
```

Zero UUID and unrecognized layouts → UnknownTopicId.

### KIP-951 (v10+)

CurrentLeader / NodeEndpoints remain **empty tag buffers** (honest: no leader
redirect hints on NOT_LEADER).

## Exit criteria

1. ApiVersions Produce max **13**
2. Produce v13 by known TopicId appends records + echoes UUID
3. Produce v13 unknown UUID → partition error 100
4. Produce v10 name path works; Produce v9 still name-based
5. Produce v14 → header v1 + UnsupportedVersion
6. phase71 + phase53 + phase48 green

## Honest limitations

- Deterministic UUID only
- No CurrentLeader / NodeEndpoints on error
- record_errors always empty; throttle always 0
- No v14+
