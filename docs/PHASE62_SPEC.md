# Phase 62 — Flexible transaction APIs

## Goals

1. First flexible versions of transaction APIs:
   - **InitProducerId** 0–2 (flexible **v2**)
   - **AddPartitionsToTxn** 0–3 (flexible **v3**)
   - **AddOffsetsToTxn** 0–3 (flexible **v3**)
   - **EndTxn** 0–3 (flexible **v3**)
   - **TxnOffsetCommit** 0–3 (flexible **v3**; member/generation fields ignored)
2. Response header **v1** for those flexible versions (and unsupported higher flex versions)
3. Compact strings/arrays + empty TAG_BUFFER
4. Classic paths unchanged (Init 0–1, others 0–2)
5. Tests + docs honesty

## Non-goals

- InitProducerId v3+ (ProducerId/Epoch resume, PRODUCER_FENCED, KIP-890, 2PC)
- AddPartitionsToTxn v4+ broker-batch `Transactions[]` / VerifyOnly
- EndTxn v5 ProducerId/Epoch response fields
- TxnOffsetCommit v4+ TRANSACTION_ABORTABLE / v6 TopicId
- Control markers / READ_COMMITTED fetch filter

## Wire summary

### InitProducerId v2

**Request:** compact nullable transactional_id, transaction_timeout_ms, tags.

**Response** (header v1): throttle, error, producer_id, producer_epoch, tags.

### AddPartitionsToTxn v3

**Request:** compact transactional_id, producer_id, producer_epoch, compact topics[{name, compact partitions[]int32, tags}], tags.

**Response** (header v1): throttle, compact results[{name, compact results[{partition, error, tags}], tags}], tags.

### AddOffsetsToTxn v3

**Request:** compact transactional_id, producer_id, producer_epoch, compact group_id, tags.

**Response:** throttle, error, tags.

### EndTxn v3

**Request:** compact transactional_id, producer_id, producer_epoch, committed, tags.

**Response:** throttle, error, tags.

### TxnOffsetCommit v3

**Request:** compact transactional_id, compact group_id, producer_id, producer_epoch, generation_id, compact member_id, compact nullable group_instance_id, compact topics[{name, compact partitions[{partition, offset, leader_epoch, metadata, tags}], tags}], tags.

**Response:** throttle, compact topics[{name, compact partitions[{partition, error, tags}], tags}], tags.

Member/generation/instance and leader_epoch are parsed and ignored (no group membership check on the txn path).

## Exit criteria

1. ApiVersions maxes: Init **2**, AddPartitions/AddOffsets/EndTxn/TxnOffsetCommit **3**
2. Flexible full path: Init → AddPartitions → Produce → AddOffsets → TxnOffsetCommit → EndTxn commit → data visible
3. Classic InitProducerId v0 still works
4. Unsupported higher versions → header v1 + UnsupportedVersion
5. phase62 + phase47 + phase31 + phase29 green

## Honest limitations

- Same buffer-until-commit semantics as classic (crash ≡ abort)
- Empty tag buffers only
- No KIP-890 / batch AddPartitions / TopicId TxnOffsetCommit
- transaction_timeout_ms ignored
