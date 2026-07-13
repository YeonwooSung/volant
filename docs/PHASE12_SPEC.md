# Phase 12 — Group admin: ListGroups, DeleteOffsets, static membership (binding)

## Goals

1. **ListGroups** — protocol + CLI to enumerate known consumer groups
2. **DeleteOffsets** — protocol + CLI to reset/delete committed offsets
3. **Static membership** — optional `group_instance_id` on JoinGroup for stable member ids
4. Docs honesty in ROADMAP / ops

## Non-goals

- Full cooperative rebalance (incremental revoke protocol)
- Multi-partition transactions
- SCRAM / mTLS / Kafka shim
- Kafka static membership wire parity

## ListGroups

| Direction | Opcode | Name |
|-----------|--------|------|
| Req | 36 | `ListGroups` |
| Resp | 37 | `ListGroups` |

Request: empty body.

Response:

```
error_code: u16
group_count: u32
  for each:
    group_id: string
    state: u8          # 0 = Empty (offsets only), 1 = Stable (live members)
    member_count: u32
    generation: u32    # 0 if empty
```

CLI: `volant group list`

## DeleteOffsets

| Direction | Opcode | Name |
|-----------|--------|------|
| Req | 38 | `DeleteOffsets` |
| Resp | 39 | `DeleteOffsets` |

Request:

```
group_id: string
entry_count: u32   # 0 = delete all offsets for group
  for each: topic string, partition u32
```

Response:

```
error_code: u16
deleted_count: u32
```

CLI: `volant group delete-offsets --group G [--topic T --partition P]`

## Static membership

JoinGroup request gains a trailing optional field (backward compatible):

```
… existing JoinGroup fields …
group_instance_id: string   # empty = dynamic membership
```

- When `group_instance_id` is non-empty and `member_id` is empty, broker assigns
  `member_id = "static:{instance_id}"`.
- Re-join with the same instance id reuses the member without generation bump
  when topics are unchanged (existing re-join path).
- Sticky assignor continues to minimize partition moves.

Client: `Client::join_group_with_instance` / `GroupConsumer::join_static`.

## Exit criteria

1. `volant group list` shows live and offset-only groups
2. DeleteOffsets removes durable commits; lag/fetch reflects reset
3. Static member rejoin keeps member id and avoids unnecessary rebalance
4. `cargo test --workspace` green
