# v0.41 — client-side range assignor

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Multi-language clients — not kafka-python /
custom assignor” by porting the broker **range** algorithm to the Python,
Go, and Java clients and offering an optional local fetch-set override.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, or change the
broker JoinGroup wire.

## Goals

1. **Public range helper** on all three language clients, bit-for-bit with
   `volant_broker::{range_assign, range_assign_multi}`
   (`crates/volant-broker/src/assignor.rs`).
2. **Optional GroupConsumer assignor** (`"broker"` default, `"range"`).
   Membership (join / heartbeat / leave) stays broker-coordinated.
   Only the **fetch set** may come from a local range computation.
3. Unknown assignor values fail (`ValueError` / error / `IllegalArgumentException`).
   Empty / `"broker"` is today’s behavior.
4. Unit tests port the Rust cases (uneven 5/2, even 4/2, solo 3/1, three
   members / 7 partitions unsorted ids, multi-topic disjoint cover).
5. GroupConsumer fake-client test: `assignor="range"` plus metadata with
   3 partitions fetches `[0,1,2]` even if JoinGroup assigned only `0`.

## Algorithm

Members are sorted by `member_id`. For `n` partitions and `m` members:

```
base  = n / m
extra = n % m
sorted member i gets base + (i < extra ? 1 : 0) consecutive partitions
```

Results are written back in the **original** member order. Empty members
or `n == 0` → `m` empty lists (`vec![Vec::new(); m]`).

`range_assign_multi` range-assigns **each topic independently** to the
members subscribed to that topic. The topic union is sorted + deduped.
A topic missing from `partition_counts` is skipped. Each member’s
`(topic, partition)` list is sorted by topic then partition.

This is **not** Kafka cooperative-sticky and **not** the broker sticky
default. Sticky stays broker-only.

## API

### Helpers

| Language | Functions |
|----------|-----------|
| Python | `volant.range_assign(num_partitions, member_ids) -> list[list[int]]` |
|        | `volant.range_assign_multi(member_ids, member_topics, partition_counts) -> list[list[tuple[str, int]]]` |
| Go | `RangeAssign(numPartitions uint32, memberIDs []string) [][]uint32` |
|    | `RangeAssignMulti(memberIDs []string, memberTopics [][]string, partitionCounts map[string]uint32) [][]Assignment` |
| Java | `RangeAssignor.rangeAssign(int, List<String>)` → `List<List<Integer>>` |
|      | `RangeAssignor.rangeAssignMulti(...)` → `List<List<Codec.Assignment>>` |

Impl files: `clients/python/src/volant/assignor.py` (re-exported from
`volant`), `clients/go/assignor.go`, `clients/java/src/main/java/io/volant/RangeAssignor.java`.

### GroupConsumer

| Language | How to opt in |
|----------|----------------|
| Python | `GroupConsumer.join(..., assignor="broker"\|"range")` |
| Go | `JoinGroupConsumer(c, group, topics, timeout, WithAssignor("range"))` — existing 4-arg call is unchanged |
| Java | `GroupConsumer.join(c, group, topics, timeout, "range")` overload — existing 4-arg `join` is unchanged |

When `assignor="range"`: after a successful JoinGroup, call `metadata()`,
collect partition counts for subscribed topics, and replace the poll
assignment with

```
range_assign_multi([self.member_id], [self.topics], counts)
```

This member is the **only** subscriber in the local computation, so the
solo range result is “I own every subscribed partition.” Topics missing
from metadata are skipped (same as Rust `range_assign_multi`).

When `assignor="broker"` (default), do **not** fetch partitions the
broker did not assign and do **not** call metadata for assignment.

## Honesty

- **Not Kafka cooperative-sticky.** No sticky / cooperative client
  assignor in this slice.
- **Not SyncGroup.** Native JoinGroup does not return the full member
  list and there is no native SyncGroup. Do not invent one.
- **Local range cannot see other live members.** Without a member list
  on the wire, `assignor="range"` cannot split partitions across the
  group. The public helper exists so apps can call `range_assign` /
  `range_assign_multi` themselves with a known member set.
- **Broker assignment remains the default source of truth.** Language
  GroupConsumers still honor JoinGroup unless the caller opts into
  `"range"`.
- **Not kafka-python / kafka-clients / kafka-go.** Native protocol only.
- **No new Kafka API keys** (`SUPPORTED_APIS` stays 38) and **no new
  native opcodes** (next free is 116+).
- Broker `assignor.rs` and the JoinGroup wire are unchanged.

## Non-goals

| Deferred | Why |
|----------|-----|
| Sticky / cooperative client assignor | Broker already has sticky; this slice is range only |
| SyncGroup / member list on JoinGroup | Would need a new opcode; frozen |
| RequestVote / InstallSnapshot / homemade metadata Raft | Frozen; do not extend `cluster/metadata_raft.rs` |
| New Kafka API keys / native opcodes | Frozen |
| Changing the broker assignor | Client-only slice |
| Auto-split across live members | Impossible without a member list on the wire |

## Tests

```
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

- Python: `clients/python/tests/test_assignor.py`
- Go: `clients/go/assignor_test.go` (plus fake-TCP metadata on the
  existing `group_test.go` harness)
- Java: `clients/java/src/test/java/io/volant/RangeAssignorTest.java`

## Files

| Path | Role |
|------|------|
| `clients/python/src/volant/assignor.py` | Range helper |
| `clients/python/src/volant/group.py` | Optional `assignor=` |
| `clients/go/assignor.go` | Range helper |
| `clients/go/group.go` | `WithAssignor` |
| `clients/java/src/main/java/io/volant/RangeAssignor.java` | Range helper |
| `clients/java/src/main/java/io/volant/GroupConsumer.java` | `join(..., assignor)` |
| `docs/V41_SPEC.md` | This spec |
