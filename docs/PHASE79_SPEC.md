# Phase 79 — Group admin version bumps (List/Describe/Delete)

## Goals

1. **ListGroups** max **0–5**
   - **v4** StatesFilter request + **GroupState** on each listed group
   - **v5** TypesFilter request + **GroupType** on each listed group
2. **DescribeGroups** max **0–6** — **ErrorMessage** (compact nullable) per group
3. **DeleteGroups** max **0–3** — **ErrorMessage** (compact nullable) per result
4. v0–previous max paths unchanged; response header v1 for all flexible versions
5. Tests + docs honesty

## Non-goals

- Real KIP-848 consumer protocol / share groups (GroupType always `"classic"`)
- Full Kafka group state machine (only Stable / Empty from membership)
- CreatePartitions v3 quota errors
- JoinGroup / SyncGroup further bumps
- READ_COMMITTED / 2PC

## Wire summary

### ListGroups

| Version | Request | Response ListedGroup |
|--------:|---------|----------------------|
| 0–2 | (empty) classic | GroupId, ProtocolType |
| 3 | TAG_BUFFER only | compact GroupId, ProtocolType, tags |
| **4** | StatesFilter[] + tags | + **GroupState** |
| **5** | StatesFilter[] + TypesFilter[] + tags | + GroupState + **GroupType** |

```
# Request v5 (flexible)
StatesFilter: COMPACT_ARRAY[COMPACT_STRING]   # empty = all states
TypesFilter:  COMPACT_ARRAY[COMPACT_STRING]   # empty = all types
TAG_BUFFER

# Response ListedGroup v5
GroupId: COMPACT_STRING
ProtocolType: COMPACT_STRING   # always "consumer" for Volant groups
GroupState: COMPACT_STRING     # v4+  "Stable" | "Empty"
GroupType: COMPACT_STRING      # v5+  always "classic"
TAG_BUFFER
```

**Filter semantics:** empty array = no filter. Non-empty: keep groups whose
state/type matches any entry (case-insensitive). Unknown filter tokens match
nothing extra (no error).

**State derivation:** members present → `"Stable"`; offset-only / empty →
`"Empty"` (same as DescribeGroups today).

**GroupType:** always `"classic"` (no KIP-848 / share groups).

### DescribeGroups v6

Same flexible body as v5, plus **ErrorMessage** after AuthorizedOperations:

```
# DescribedGroup v6
ErrorCode, GroupId, GroupState, ProtocolType, ProtocolData,
Members[…], AuthorizedOperations,
ErrorMessage: COMPACT_NULLABLE_STRING,   # null on success; short text on error
TAG_BUFFER
```

### DeleteGroups v3

Same flexible body as v2, plus **ErrorMessage** after ErrorCode:

```
# DeletableGroupResult v3
GroupId: COMPACT_STRING
ErrorCode: INT16
ErrorMessage: COMPACT_NULLABLE_STRING
TAG_BUFFER
```

## Exit criteria

1. ApiVersions: ListGroups **0–5**, DescribeGroups **0–6**, DeleteGroups **0–3**
2. ListGroups v4 returns GroupState; StatesFilter filters
3. ListGroups v5 returns GroupType `"classic"`; TypesFilter filters
4. DescribeGroups v6 ErrorMessage null on success; non-null on GroupIdNotFound
5. DeleteGroups v3 ErrorMessage on NonEmptyGroup / GroupIdNotFound
6. ListGroups v3 / Describe v5 / Delete v2 still work
7. ListGroups v6 / Describe v7 / Delete v4 → header v1 + UnsupportedVersion
8. phase79 + phase59 green

## Honest limitations

- Only Stable/Empty states; no PreparingRebalance / CompletingRebalance / Dead
  in ListGroups (Dead only on Describe for unknown)
- GroupType always classic; TypesFilter for `consumer`/`share` returns empty
- ErrorMessage is short static English, not full Kafka exception text
- No group type other than classic consumer

