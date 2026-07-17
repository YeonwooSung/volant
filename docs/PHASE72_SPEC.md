# Phase 72 — OffsetCommit/OffsetFetch TopicId (v9–10)

## Goals

1. **OffsetCommit** max **0–10** (flexible from v8; **TopicId from v10**)
2. **OffsetFetch** max **0–10** (multi-group from v8; **MemberId/Epoch v9**; **TopicId v10**)
3. OffsetCommit v9 wire-identical to v8 (name-based flexible)
4. OffsetFetch v9 parses MemberId + MemberEpoch per group (ignored — no KIP-848)
5. v10 request/response topics use **TopicId UUID** (no name)
6. Unknown / non-Volant UUID → **UnknownTopicId (100)** per partition
7. Deterministic UUID mapping (same as Metadata/Fetch/admin/Produce)
8. Tests + docs honesty

## Non-goals

- OffsetCommit / OffsetFetch v11+
- Real KIP-848 consumer group protocol (STALE_MEMBER_EPOCH, MemberEpoch checks)
- TxnOffsetCommit TopicId
- ListOffsets TopicId
- RequireStable enforcement (still ignored)
- committed_leader_epoch storage (still -1 on fetch)

## Wire summary

### OffsetCommit

| Version | Framing | Topic identity |
|---------|---------|----------------|
| ≤v7 classic | STRING name | name |
| v8–9 flexible | COMPACT_STRING name + tags | name |
| **v10** | UUID TopicId + tags | **TopicId** |

v9 is wire-identical to v8 for Volant (no extra fields). Response echoes
name (v8–9) or UUID (v10). Per-partition error is group-level kerr, or
**UnknownTopicId** when the UUID does not resolve.

### OffsetFetch

| Version | Shape | Topic identity / extras |
|---------|-------|-------------------------|
| ≤v5 classic | single group | name |
| v6–7 flexible | single group | name |
| v8 multi-group | Groups[] | name |
| **v9** | Groups[] | name + **MemberId (nullable) + MemberEpoch** (ignored) |
| **v10** | Groups[] | **TopicId UUID** (+ MemberId/Epoch still present) |

Response v8+: throttle, Groups[{ GroupId, Topics, ErrorCode, tags }], tags.
Topics use name (v8–9) or UUID (v10). list_all (null topics) emits UUID via
metadata lookup on v10.

### TopicId mapping

```
bytes 0–5:  "volant"
bytes 6–11: 0
bytes 12–15: big-endian u32 Volant TopicId
```

Zero UUID and unrecognized layouts → UnknownTopicId.

## Exit criteria

1. ApiVersions: OffsetCommit **0–10**, OffsetFetch **0–10**
2. OffsetCommit v10 by known TopicId commits; response echoes UUID
3. OffsetCommit v10 unknown UUID → partition error 100
4. OffsetCommit v9 name path works (same as v8)
5. OffsetFetch v9 with MemberId/Epoch still returns offsets
6. OffsetFetch v10 by TopicId returns committed offset; unknown → 100
7. OffsetCommit/Fetch v11 → header v1 + UnsupportedVersion
8. phase72 + phase57 + phase58 + phase44 green

## Honest limitations

- Deterministic UUID only
- MemberId / MemberEpoch ignored (no KIP-848 membership)
- RequireStable ignored; leader_epoch always -1 on fetch
- No v11+
