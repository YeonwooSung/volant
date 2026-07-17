# Phase 64 — Flexible DeleteRecords + ACL admin

## Goals

1. First flexible versions of:
   - **DeleteRecords** 0–2 (flexible **v2**)
   - **DescribeAcls** 0–2 (flexible **v2**)
   - **CreateAcls** 0–2 (flexible **v2**)
   - **DeleteAcls** 0–2 (flexible **v2**)
2. Response header **v1** for those flexible versions
3. Compact strings/arrays + empty TAG_BUFFER
4. Classic paths unchanged
5. Tests + docs honesty

## Non-goals

- ACL v3 USER resource type
- Host-dimension ACL matching
- Non-LITERAL pattern types (PREFIXED, etc.)
- DeleteRecords beyond whole sealed segments (existing honesty)

## Wire summary

### DeleteRecords v2

**Request:** compact topics[{name, compact partitions[{partition, offset, tags}], tags}], timeout_ms, tags.

**Response** (header v1): throttle, compact topics[{name, compact partitions[{partition, low_watermark, error, tags}], tags}], tags.

### DescribeAcls v2

**Request:** resource_type, compact nullable name, pattern, compact nullable principal/host, op, perm, tags.

**Response:** throttle, error, compact nullable error_message, compact resources[{type, name, pattern, compact acls[{principal, host, op, perm, tags}], tags}], tags.

### CreateAcls v2

**Request:** compact creations[{type, name, pattern, principal, host, op, perm, tags}], tags.

**Response:** throttle, compact results[{error, error_message, tags}], tags.

### DeleteAcls v2

**Request:** compact filters[{…same as Describe filter…, tags}], tags.

**Response:** throttle, compact filter_results[{error, msg, compact matching[{error, msg, type, name, pattern, principal, host, op, perm, tags}], tags}], tags.

## Exit criteria

1. ApiVersions maxes: DeleteRecords **2**, Describe/Create/DeleteAcls **2**
2. DeleteRecords v2 returns low watermark
3. Create → Describe → Delete ACL flexible roundtrip
4. Classic CreateAcls v1 still works
5. Unsupported higher versions → header v1 + UnsupportedVersion
6. phase64 + phase35 green

## Honest limitations

- Empty tag buffers only
- No USER resource / PREFIXED patterns
- Host filter ignored
- DeleteRecords still sealed-segment only
