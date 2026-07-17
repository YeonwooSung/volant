# Phase 73 — Metadata v13 (top-level ErrorCode)

## Goals

1. **Metadata** max **0–13**
2. v13 request wire identical to v12 (TopicId lookup, no cluster ops flag)
3. v13 response adds **top-level ErrorCode** (INT16) after Topics, before tags
4. Success path always emits ErrorCode = 0 (None)
5. Tests + docs honesty

## Non-goals

- Metadata v14+
- Non-zero top-level errors (auth/cluster failures still use empty body / per-topic errors)
- Changing TopicId mapping or v10–12 field layout

## Wire summary

### Request (v13 = v12)

Same as v12: Topics[{TopicId, Name nullable, tags}], AllowAutoTopicCreation,
IncludeTopicAuthorizedOperations, tags. No IncludeClusterAuthorizedOperations.

### Response (v13)

```
ThrottleTimeMs
Brokers[]
ClusterId
ControllerId
Topics[]   (same layout as v12: Error, Name, TopicId, …)
ErrorCode  (INT16)   ← new at top level
TAG_BUFFER
```

Volant always writes `ErrorCode = 0` on successful framing. Per-topic errors
(UnknownTopicId, etc.) remain on the topic row.

## Exit criteria

1. ApiVersions Metadata max **13**
2. Metadata v13 named / by-id / all-topics returns ErrorCode 0
3. Metadata v12 still omits top-level ErrorCode
4. Metadata v14 → header v1 + UnsupportedVersion
5. phase73 + phase67 + phase52 green

## Honest limitations

- Top-level ErrorCode is always 0 (no cluster-level failure path yet)
- No v14+
