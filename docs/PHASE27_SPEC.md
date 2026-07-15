# Phase 27 — Kafka ops surface (groups + configs + partitions)

## Goals

1. **ListGroups / DescribeGroups / DeleteGroups** on the Kafka shim
2. **CreatePartitions** to grow topic partition counts
3. **DescribeConfigs / AlterConfigs** for Volant topic config keys
4. Advertise via ApiVersions; ACL-aware (`kafka-anonymous`)
5. Tests + docs honesty

## Non-goals

- Broker/cluster resource configs on DescribeConfigs
- Full Kafka config synonym / dynamic broker configs
- IncrementalAlterConfigs / flexible versions
- Kafka SASL
- Member metadata/assignment bytes matching Kafka consumer protocol exactly
  in DescribeGroups (we emit empty metadata; assignment as consumer bytes)

## API versions (advertised)

| API | Key | Min | Max | Notes |
|-----|----:|----:|----:|-------|
| DescribeGroups | 15 | 0 | 0 | state + members |
| ListGroups | 16 | 0 | 0 | group_id + protocol_type |
| CreatePartitions | 37 | 0 | 0 | total partition count |
| DescribeConfigs | 32 | 0 | 0 | TOPIC resources only |
| AlterConfigs | 33 | 0 | 0 | TOPIC resources only |
| DeleteGroups | 42 | 0 | 0 | empty groups only |

## Group state strings

| Condition | State |
|-----------|-------|
| No live members | `Empty` |
| ≥1 live member | `Stable` |

(Volant has no PreparingRebalance wire state.)

## DeleteGroups

- Live members present → Kafka `NON_EMPTY_GROUP` (68)
- Unknown / empty → remove membership (if any), delete durable offsets, success
- Unknown with no offsets → still success (idempotent) or `GROUP_ID_NOT_FOUND` (69)
  when neither membership nor offsets exist

## CreatePartitions

Request topics carry **total** partition count (Kafka `count` field). Maps to
`Broker::create_partitions`. Assignments ignored.

## DescribeConfigs / AlterConfigs

Supported keys (Volant Phase 13):

- `retention.ms`
- `retention.bytes`
- `segment.bytes`
- `cleanup.policy` (`delete` \| `compact`)

Resource type **2 (TOPIC)** only. Other types → `INVALID_REQUEST`.

## Exit criteria

1. Join a group → ListGroups sees it; DescribeGroups returns members
2. Leave all members → DeleteGroups succeeds; ListGroups drops it
3. CreatePartitions grows Metadata partition count
4. AlterConfigs then DescribeConfigs round-trips values
5. Phases 23–26 green; `cargo test --workspace` green

## Honest limitations

- DescribeGroups assignment/metadata are best-effort (not full Kafka client parity)
- No broker configs; no config documentation/synonyms
- CreatePartitions ignores replica assignment arrays
- No IncrementalAlterConfigs
