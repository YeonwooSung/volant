# Phase 60 — Flexible topic admin (Create/DeleteTopics, CreatePartitions)

## Goals

1. First flexible versions of topic-admin APIs:
   - **CreateTopics** 0–5 (flexible **v5**)
   - **DeleteTopics** 0–4 (flexible **v4**)
   - **CreatePartitions** 0–2 (flexible **v2**)
2. Response header **v1** for those flexible versions
3. Compact strings/arrays + empty TAG_BUFFER
4. Classic paths unchanged
5. Tests + docs honesty

## Non-goals

- CreateTopics v6 quota / v7 TopicId
- Returning real topic configs in CreateTopics v5 (null Configs array)
- DeleteTopics v5 ErrorMessage / v6 TopicId
- CreatePartitions v3 quota throttling
- Replica assignment enforcement

## Wire summary

### CreateTopics v5

**Request:** compact topics[{name, num_partitions, rf, assignments[], configs[]}], timeout, validate_only, tags.

**Response** (header v1): throttle, compact topics[{name, error, error_message, num_partitions, rf, configs=null, tags}], tags.

### DeleteTopics v4

**Request:** compact topic_names[], timeout, tags.

**Response** (header v1): throttle, compact responses[{name, error, tags}], tags.

### CreatePartitions v2

**Request:** compact topics[{name, count, assignments|null}], timeout, validate_only, tags.

**Response** (header v1): throttle, compact results[{name, error, error_message, tags}], tags.

## Exit criteria

1. ApiVersions: CreateTopics max **5**, DeleteTopics max **4**, CreatePartitions max **2**
2. Flexible create → create partitions → delete roundtrip
3. validate_only + default partitions (-1) on v5
4. Classic CreateTopics v4 still works
5. Unsupported (Create 6 / Delete 5 / CreatePartitions 3) → header v1 + UnsupportedVersion
6. phase60 + phase45 green

## Honest limitations

- CreateTopics v5 Configs always null; RF reported as 1 on success
- No TopicId
- No ErrorMessage on DeleteTopics
- Empty tag buffers only
