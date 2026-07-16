# Phase 44 — Kafka OffsetCommit classic + FindCoordinator v2

## Goals

1. Raise **OffsetCommit** classic versions 0–2 → **0–7** (flexible 8+)
2. Raise **FindCoordinator** 0–1 → **0–2** (flexible 3+; v2 wire-identical to v1)
3. Wire OffsetCommit v7 `group_instance_id` to Phase 12 static membership
4. Advertise in ApiVersions; tests + docs honesty

## Non-goals

- Flexible OffsetCommit v8+ / FindCoordinator v3+
- FindCoordinator v4+ multi-key batch (CoordinatorKeys)
- Durable storage of `committed_leader_epoch` on commit
- Retention-time enforcement (broker config only; field ignored)

## Wire summary

### OffsetCommit

| Ver | Additive |
|-----|----------|
| v1 | generation + member_id; partition commit_timestamp |
| v2–4 | retention_time_ms (no timestamp); same body as v2 |
| v3+ | response throttle_time_ms |
| v5 | no retention_time in request |
| v6 | committed_leader_epoch per partition (parsed, ignored) |
| v7 | group_instance_id (nullable) |

### FindCoordinator

| Ver | Additive |
|-----|----------|
| v1–2 | key_type; response throttle + error_message |

v2 differs from v1 only in Kafka quota-timing behavior; framing is identical.

## Static membership (OffsetCommit v7)

| Fields | Member used for commit |
|--------|------------------------|
| member_id set | use member_id (instance ignored) |
| empty member_id + instance id | `static:{instance}` |
| empty both | empty member_id (generation 0 path still works) |

## Exit criteria

1. ApiVersions: OffsetCommit max 7; FindCoordinator max 2
2. OffsetCommit v7 with static instance commits and is OffsetFetch-visible
3. OffsetCommit v5 (no retention) + v3+ throttle works
4. OffsetCommit v6 parses leader_epoch without error
5. FindCoordinator v2 returns throttle + coordinator
6. phase26 OffsetCommit v2 still green; phase31 ApiVersions updated
7. Tests green

## Honest limitations

- No flexible OffsetCommit / FindCoordinator
- Leader epoch on commit is discarded (OffsetFetch still returns -1)
- Retention time ignored
- No multi-key FindCoordinator batch
