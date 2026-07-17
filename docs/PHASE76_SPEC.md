# Phase 76 — TxnOffsetCommit TopicId (v6)

## Goals

1. **TxnOffsetCommit** max **0–6** (flexible from v3; **TopicId from v6**)
2. v4–5 wire-identical to v3 (name-based flexible; empty tags)
3. v6 request/response topics use **TopicId UUID** instead of name
4. Unknown / non-Volant UUID → **UnknownTopicId (100)** per partition (do not buffer)
5. Deterministic UUID mapping (same as Metadata/Fetch/admin/OffsetCommit)
6. v0–5 name path unchanged
7. Tests + docs honesty

## Non-goals

- Raising InitProducerId / EndTxn / AddPartitionsToTxn / AddOffsetsToTxn max
  (Phase 75 owns those)
- TxnOffsetCommit v7+
- Real TRANSACTION_ABORTABLE semantics beyond existing buffer-until-commit
- Member/generation enforcement (still ignored)
- committed_leader_epoch storage

## Wire summary

| Version | Framing | Topic identity |
|---------|---------|----------------|
| ≤v2 classic | STRING name | name |
| v3–5 flexible | COMPACT_STRING name + tags | name |
| **v6** | UUID TopicId + tags | **TopicId** |

Request (v3+ flexible):

```
TransactionalId, GroupId, ProducerId, ProducerEpoch,
GenerationId, MemberId, GroupInstanceId,
Topics[{ Name | TopicId (v6), Partitions[{
  PartitionIndex, CommittedOffset, CommittedLeaderEpoch,
  CommittedMetadata, tags
}], tags }],
tags
```

Response:

```
ThrottleTimeMs,
Topics[{ Name | TopicId (v6), Partitions[{
  PartitionIndex, ErrorCode, tags
}], tags }],
tags
```

### TopicId mapping

```
bytes 0–5:  "volant"
bytes 6–11: 0
bytes 12–15: big-endian u32 Volant TopicId
```

Zero UUID and unrecognized layouts → UnknownTopicId. Resolve via
`parse_volant_topic_uuid` + `broker.topic_name_by_id`.

## Exit criteria

1. ApiVersions: TxnOffsetCommit **0–6**
2. Full path: InitProducerId → AddPartitionsToTxn → TxnOffsetCommit **v6 by TopicId**
   → EndTxn commit → offsets applied
3. Unknown TopicId → partition error **100** (no buffer)
4. TxnOffsetCommit v3 name path still works
5. v7 → header v1 + UnsupportedVersion
6. phase76 + phase62 + phase47 green

## Honest limitations

- Deterministic UUID only
- v4–5 advertised but same wire as v3 (no extra fields)
- Member/generation ignored; leader_epoch ignored
- Buffer-until-EndTxn semantics unchanged
- No v7+
