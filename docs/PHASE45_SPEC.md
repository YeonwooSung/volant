# Phase 45 — Kafka topic admin classic version bumps

## Goals

1. Raise classic topic-admin APIs:
   - **CreateTopics** 0–1 → **0–4** (flexible 5+)
   - **DeleteTopics** 0–1 → **0–3** (flexible 4+)
   - **CreatePartitions** 0 → **0–1** (flexible 2+)
2. Align response framing with Kafka:
   - CreateTopics: error_message v1+; throttle **first** on v2+
   - DeleteTopics: throttle **first** on v1+ (was trailing)
   - CreatePartitions: throttle on **all** versions (was missing)
3. Support validate_only and CreateTopics v4 default partitions (`-1` → 1)
4. Advertise in ApiVersions; tests + docs honesty

## Non-goals

- Flexible CreateTopics v5+ / DeleteTopics v4+ / CreatePartitions v2+
- Topic IDs (CreateTopics v7 / DeleteTopics v6)
- Returning topic configs in CreateTopics response (v5+)
- Replica assignment / RF enforcement

## Wire summary

### CreateTopics

| Ver | Additive |
|-----|----------|
| v1 | request validate_only; response error_message per topic |
| v2–3 | response throttle_time_ms (leading) |
| v4 | num_partitions / rf may be -1 (default partitions = 1; RF ignored) |

### DeleteTopics

| Ver | Additive |
|-----|----------|
| v1–3 | response throttle_time_ms (leading) |

### CreatePartitions

| Ver | Additive |
|-----|----------|
| v0–1 | throttle on all versions; validate_only dry-run |

## Exit criteria

1. ApiVersions: CreateTopics max 4; DeleteTopics max 3; CreatePartitions max 1
2. CreateTopics v4 with partitions=-1 creates 1 partition
3. CreateTopics v1 validate_only does not create
4. CreateTopics v2 response starts with throttle; includes error_message
5. DeleteTopics v3 response starts with throttle
6. CreatePartitions v1 throttle + validate_only
7. phase25 / phase27 updated for corrected framing; tests green

## Honest limitations

- No flexible topic-admin versions
- No topic UUID
- Default partitions hardcoded to 1
- RF / replica assignments ignored
- Configs accepted on create but limited to Volant-known keys
