# Phase 69 — Admin TopicId (CreateTopics v7 / DeleteTopics v5–6)

## Goals

1. **CreateTopics** max **0–7** (flexible v5+; **TopicId in response v7**)
2. **DeleteTopics** max **0–6** (flexible v4+; **ErrorMessage v5**; **TopicId v6**)
3. Delete by TopicId UUID (v6); unknown → UnknownTopicId (100)
4. Same deterministic UUID mapping as Metadata/Fetch (Phases 67–68)
5. Tests + docs honesty

## Non-goals

- CreateTopics v8+ / DeleteTopics v7+
- Real CreateTopics configs array (still null)
- CreatePartitions TopicId
- Produce TopicId
- Quota throttle enforcement (v6 CreateTopics accepts, never returns THROTTLING_QUOTA_EXCEEDED)

## Wire summary

### CreateTopics

| Version | Notes |
|---------|-------|
| 0–4 | Classic (unchanged) |
| 5–6 | Flexible; v6 same fields as v5 |
| **7** | Response: Name, **TopicId UUID**, ErrorCode, ErrorMessage, NumPartitions, RF, Configs=null, tags |

TopicId on success = `volant_topic_uuid(created_id)`; on error / validate_only = zero UUID.

### DeleteTopics

| Version | Request | Response |
|---------|---------|----------|
| 0–3 | Classic names[] | name + error |
| 4 | Compact names[] | name + error + tags |
| **5** | Compact names[] | name + error + **ErrorMessage** + tags |
| **6** | topics[{Name nullable, TopicId, tags}] | Name nullable, **TopicId**, error, ErrorMessage, tags |

v6 lookup: non-null Name → by name; else parse Volant UUID → by id; else UnknownTopicId.

## Exit criteria

1. ApiVersions: CreateTopics max **7**, DeleteTopics max **6**
2. CreateTopics v7 success returns non-zero TopicId matching Metadata
3. DeleteTopics v5 returns ErrorMessage on unknown topic
4. DeleteTopics v6 by TopicId deletes topic; unknown id → 100
5. CreateTopics v5 / DeleteTopics v4 still work
6. CreateTopics v8 / DeleteTopics v7 → header v1 + UnsupportedVersion
7. phase69 + phase60 + phase45 green

## Honest limitations

- Deterministic UUID only
- CreateTopics Configs always null; RF reported as 1
- validate_only returns zero TopicId
- No quota throttling errors
