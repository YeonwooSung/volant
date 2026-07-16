# Phase 43 — Kafka group admin classic version bumps

## Goals

1. Raise classic group-admin APIs beyond v0:
   - **DescribeGroups** 0 → **0–4** (flexible 5+)
   - **ListGroups** 0 → **0–2** (flexible 3+)
   - **DeleteGroups** 0 → **0–1** (flexible 2+)
2. Align DeleteGroups response with Kafka: `throttle_time_ms` is present on **all** versions (including v0)
3. Surface static membership in DescribeGroups v4 via `group_instance_id` (derived from `static:` prefix)
4. Advertise in ApiVersions; tests + docs honesty

## Non-goals

- Flexible group-admin APIs (DescribeGroups v5+, ListGroups v3+, DeleteGroups v2+)
- ListGroups v4+ StatesFilter / v5 TypesFilter
- DescribeGroups v6 ErrorMessage
- DeleteGroups v3 ErrorMessage
- True ACL bitfield coverage beyond Read/Delete/Describe for groups

## Wire summary

### DescribeGroups

| Ver | Additive |
|-----|----------|
| v1–2 | response throttle_time_ms |
| v3 | request include_authorized_operations; response authorized_operations per group |
| v4 | response members include group_instance_id (nullable) |

Authorized operations: when include=false → `Integer.MIN_VALUE` (omitted); when true and ACLs off → Read|Delete|Describe bits.

### ListGroups

| Ver | Additive |
|-----|----------|
| v1–2 | response throttle_time_ms |

Request remains empty for classic 0–2.

### DeleteGroups

| Ver | Additive |
|-----|----------|
| v0–1 | response throttle_time_ms (Kafka has this on all versions; Phase 43 fixes the missing field) |

Request: `[group_id]` unchanged.

## Static membership mapping (DescribeGroups v4)

| Member id | group_instance_id |
|-----------|-------------------|
| `static:inst-1` | `"inst-1"` |
| dynamic (no prefix) | null |

## Exit criteria

1. ApiVersions: DescribeGroups max 4; ListGroups max 2; DeleteGroups max 1
2. ListGroups v2 returns throttle 0 then groups
3. DescribeGroups v4 returns throttle, static member instance id, authorized_ops when requested
4. DeleteGroups v0/v1 response starts with throttle
5. phase27 updated for DeleteGroups throttle; still green
6. Tests green

## Honest limitations

- No flexible group-admin versions
- No StatesFilter / GroupState on ListGroups (v4+)
- group_instance_id only for static-prefixed members
- Authorized ops are a coarse best-effort bitfield (not full Kafka ACL table)
