# Phase 11 — Sticky assignor, durable producer state, group describe (binding)

## Goals

1. **Sticky partition assignor** — minimize ownership churn on rebalance while staying balanced
2. **Durable idempotent producer state** — survive broker restart under `data_dir`
3. **DescribeGroup** — protocol + CLI for membership / assignment inspection
4. Docs honesty in ROADMAP / ops

## Non-goals

- Full cooperative rebalance (incremental revoke)
- Kafka sticky assignor wire protocol parity
- Multi-partition transactions
- SCRAM / mTLS / Kafka shim

## Sticky assignor

Default group assignor becomes **sticky** (range remains available as fallback).

Per topic:

1. Keep previous `(member → partitions)` when the member is still in the group and still subscribed
2. Collect free partitions (new, or owned by departed members)
3. Assign free partitions to members with the fewest partitions (stable member-id order for ties)
4. If a member holds more than `ceil(n/m)`, strip extras into the free pool and rebalance

Multi-topic: run per topic independently (same as range multi).

## Durable producer state

Layout:

```
{data_dir}/__producer_state/
  state.json
```

```json
{
  "next_id": 3,
  "producers": {
    "1": {
      "epoch": 0,
      "partitions": {
        "events:0": { "base_sequence": 2, "count": 1, "base_offset": 10 }
      }
    }
  }
}
```

- Load on broker start
- Persist after `InitProducerId` and successful idempotent produce
- Atomic write via temp + rename

## DescribeGroup

| Direction | Opcode | Name |
|-----------|--------|------|
| Req | 34 | `DescribeGroup` |
| Resp | 35 | `DescribeGroup` |

Request: `group_id` string.

Response:

```
error_code: u16
generation: u32
member_count: u32
  for each:
    member_id: string
    topic_count: u32
      topics: string…
    assignment_count: u32
      for each: topic string, partition u32
```

CLI: `volant group describe --group G`

## Exit criteria

1. Sticky rebalance keeps partitions for surviving members when possible
2. Broker restart reloads PID state; duplicate produce still de-dupes
3. DescribeGroup + CLI show members and assignments
4. `cargo test --workspace` green
