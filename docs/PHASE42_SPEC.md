# Phase 42 — Kafka consumer group classic versions + static membership

## Goals

1. Raise classic group APIs for static membership (`group.instance.id`):
   - **JoinGroup** 0–1 → **0–5** (flexible 6+)
   - **Heartbeat** 0 → **0–3** (flexible 4+)
   - **SyncGroup** 0 → **0–3** (flexible 4+)
   - **LeaveGroup** 0 → **0–3** (flexible 4+)
2. Wire `group_instance_id` into Volant Phase 12 static membership (`static:{id}`)
3. Add throttle fields where Kafka classic versions require them
4. Advertise in ApiVersions; tests + docs honesty

## Non-goals

- Flexible group APIs (JoinGroup v6+, Heartbeat/Sync/Leave v4+)
- JoinGroup two-step MEMBER_ID_REQUIRED (v4 consumer flow) — still assign id on first join
- LeaveGroup v5 reason / SyncGroup v5 protocol type-name echo
- DescribeGroups / ListGroups version bumps (separate)

## Wire summary

### JoinGroup

| Ver | Additive |
|-----|----------|
| v1 | request rebalance_timeout (ignored) |
| v2–3 | response throttle_time_ms |
| v5 | request group_instance_id; response members include group_instance_id |

### Heartbeat / SyncGroup

| Ver | Additive |
|-----|----------|
| v1–2 | response throttle_time_ms |
| v3 | request group_instance_id (nullable; ignored when member_id set) |

### LeaveGroup

| Ver | Additive |
|-----|----------|
| v1–2 | response throttle_time_ms |
| v3 | request members[] of {member_id, group_instance_id}; response members[] errors |

## Static membership mapping

| Kafka field | Volant |
|-------------|--------|
| `group_instance_id = "i1"`, empty member_id on Join | member_id = `static:i1` |
| Heartbeat/Sync with member_id set | use member_id; instance id ignored |
| Leave v3 with empty member_id + instance id | leave `static:{instance}` |
| Leave v3 with member_id | leave that member_id |

## Exit criteria

1. ApiVersions: JoinGroup max 5; Heartbeat/Sync/Leave max 3
2. Static join (empty member_id + instance) returns `static:…` member_id
3. Heartbeat v3 + throttle works for static member
4. LeaveGroup v3 batch leaves static member; response members[] present
5. SyncGroup v3 parses instance id + returns assignment
6. phase26 still green
7. Tests green

## Honest limitations

- No flexible group versions
- No MEMBER_ID_REQUIRED double-join dance
- group_instance_id not stored separately on Member (derived via static: prefix only)
- Leave by instance id only works for static-prefixed members
