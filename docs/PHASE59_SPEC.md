# Phase 59 — Flexible group admin (Describe/List/DeleteGroups)

## Goals

1. First flexible versions of group-admin APIs:
   - **DescribeGroups** 0–5 (flexible **v5**)
   - **ListGroups** 0–3 (flexible **v3**)
   - **DeleteGroups** 0–2 (flexible **v2**)
2. Response header **v1** (correlation + TAG_BUFFER) for those flexible versions
3. Compact strings/arrays/bytes + empty TAG_BUFFER on flexible bodies
4. Classic paths (Describe ≤4, List ≤2, Delete ≤1) unchanged
5. Tests + docs honesty

## Non-goals

- DescribeGroups v6 ErrorMessage
- ListGroups v4 StatesFilter / GroupState response field
- ListGroups v5 TypesFilter / GroupType response field
- DeleteGroups v3 ErrorMessage
- Full ACL bitfield parity beyond existing Read/Delete/Describe

## Wire summary

### DescribeGroups v5

**Request** (flexible header + body):

```
Groups: COMPACT_ARRAY[COMPACT_STRING]
IncludeAuthorizedOperations: BOOL
TAG_BUFFER
```

**Response** (header v1):

```
ThrottleTimeMs: INT32
Groups: COMPACT_ARRAY[{
  ErrorCode: INT16
  GroupId, GroupState, ProtocolType, ProtocolData: COMPACT_STRING
  Members: COMPACT_ARRAY[{
    MemberId: COMPACT_STRING
    GroupInstanceId: COMPACT_NULLABLE_STRING
    ClientId, ClientHost: COMPACT_STRING
    MemberMetadata, MemberAssignment: COMPACT_BYTES
    TAG_BUFFER
  }]
  AuthorizedOperations: INT32
  TAG_BUFFER
}]
TAG_BUFFER
```

Static members: `static:<instance>` → GroupInstanceId = `<instance>` (same as classic v4).

### ListGroups v3

**Request:** body is empty fields + `TAG_BUFFER` only.

**Response** (header v1):

```
ThrottleTimeMs: INT32
ErrorCode: INT16
Groups: COMPACT_ARRAY[{
  GroupId, ProtocolType: COMPACT_STRING
  TAG_BUFFER
}]
TAG_BUFFER
```

### DeleteGroups v2

**Request:**

```
GroupsNames: COMPACT_ARRAY[COMPACT_STRING]
TAG_BUFFER
```

**Response** (header v1):

```
ThrottleTimeMs: INT32
Results: COMPACT_ARRAY[{
  GroupId: COMPACT_STRING
  ErrorCode: INT16
  TAG_BUFFER
}]
TAG_BUFFER
```

## Exit criteria

1. ApiVersions: DescribeGroups max **5**, ListGroups max **3**, DeleteGroups max **2**
2. List/Describe/Delete flexible roundtrip with live group + static instance id
3. Classic v2/v4/v1 still work (no header tags)
4. Unsupported higher versions (Describe 6, List 4, Delete 3) → UnsupportedVersion with header v1
5. Unknown group Describe v5 → GROUP_ID_NOT_FOUND / Dead
6. phase59 + phase43 + phase27 green

## Honest limitations

- No ErrorMessage on Describe/Delete
- No StatesFilter / TypesFilter / GroupState / GroupType on List
- Member client_id/host are placeholders (`volant-kafka`, `/`)
- Empty tag buffers only
