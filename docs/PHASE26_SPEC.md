# Phase 26 — Kafka consumer groups on the shim

## Goals

1. **FindCoordinator** so clients discover the group coordinator
2. **JoinGroup / SyncGroup / Heartbeat / LeaveGroup** mapped to Volant `GroupCoordinator`
3. **OffsetCommit / OffsetFetch** on durable `__consumer_offsets`
4. Advertise via ApiVersions; ACL checks as principal `kafka-anonymous`
5. Tests + docs honesty

## Non-goals

- True Kafka two-phase rebalance (leader computes assignment from member metadata)
- Cooperative sticky / incremental protocol on Kafka wire
- DescribeGroups / ListGroups / DeleteGroups on Kafka wire
- Flexible versions / tagged fields
- Kafka SASL on the shim

## Design

Volant assigns partitions **eagerly on JoinGroup**. The Kafka two-phase flow is
emulated:

1. **JoinGroup** — parse consumer subscription topics from protocol metadata;
   call `GroupCoordinator::join`; return generation + member id. Leader is the
   lexicographically smallest live member; only the leader receives the member list.
2. **SyncGroup** — **ignore** leader-provided assignments; return this member's
   coordinator assignment encoded as Kafka consumer `MemberAssignment` bytes.
3. **Heartbeat / LeaveGroup** — pass through to the coordinator.
4. **OffsetCommit / OffsetFetch** — durable offsets via existing store.

This is enough for simple single-member (and multi-member sticky) clients that
tolerate coordinator-driven assignment.

## API versions (advertised)

| API | Key | Min | Max |
|-----|----:|----:|----:|
| OffsetCommit | 8 | 0 | 2 |
| OffsetFetch | 9 | 0 | 1 |
| FindCoordinator | 10 | 0 | 0 |
| JoinGroup | 11 | 0 | 1 |
| Heartbeat | 12 | 0 | 0 |
| LeaveGroup | 13 | 0 | 0 |
| SyncGroup | 14 | 0 | 0 |

Plus prior Produce/Fetch/Metadata/ListOffsets/Create/DeleteTopics/ApiVersions.

## Consumer protocol bytes

**Subscription** (JoinGroup protocol metadata):

```
version: i16
topics: [string]
user_data: bytes
```

**MemberAssignment** (SyncGroup response):

```
version: i16
assigned: [topic [partition:i32]]
user_data: bytes
```

## FindCoordinator

Returns this broker's advertised `(node_id, host, port)` from `Broker::metadata`.
**Honest:** host/port are the Volant advertised address (usually `--listen`), not
necessarily `--kafka-listen`. Clients already on the kafka port may ignore the
returned port if it matches the connected broker id; dual-port setups should
advertise carefully.

## Error mapping (Volant → Kafka)

| Volant | Kafka |
|-------:|------:|
| 0 | 0 None |
| 9 RebalanceInProgress | 27 |
| 10 UnknownMemberId | 25 |
| 11 IllegalGeneration | 22 |

## Exit criteria

1. Join → Sync → assignment partitions match sticky coordinator output
2. OffsetCommit then OffsetFetch round-trips
3. Heartbeat ok; LeaveGroup removes member
4. FindCoordinator returns node id + host/port
5. Phase 23–25 suites still green; `cargo test --workspace` green

## Honest limitations

- Coordinator-driven assignment (not Kafka leader assignor)
- No Describe/List/DeleteGroups on Kafka wire
- FindCoordinator port may be native listen port
- No static membership / group.instance.id on Kafka JoinGroup v1+ fields beyond
  what Volant already supports via empty member_id rejoin
- OffsetCommit generation checks use Volant semantics
