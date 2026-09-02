# v0.49 — ListGroups and DescribeGroup on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Python / Go / Java clients cannot list or
describe consumer groups.” Native opcodes already exist (Phase 11/12);
this slice teaches the language clients to speak them, matching Rust
`Client::list_groups` / `Client::describe_group`.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, or change the
broker.

## Goals

1. **Codec** encode/decode for DescribeGroup (34/35) and ListGroups
   (36/37) in Python, Go, and Java. Reuse each language’s existing
   `put_string` / `get_string`.
2. **Client API** matching the Rust shape: `list_groups` returns
   listings; `describe_group` returns members + assignments.
3. **Error 2** (`NotFound`, no live members) on DescribeGroup raises
   like any other non-zero broker `error_code`.
4. **Unit tests** without a broker: payload fixtures plus a fake TCP
   server (list two groups; describe members; error 2 raises).

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka DescribeGroups / ListGroups API keys | Native opcodes only |
| Authorized operations / StatesFilter / TypesFilter | Kafka shim only (Phase 43/59/79) |
| DeleteOffsets / DeleteGroups on these clients | Sibling leftovers |
| Broker group coordinator changes | Already shipped (Phase 11/12) |
| New native opcodes | Do not add |
| Phase 155 | Frozen |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`:

### DescribeGroup

| Direction | Opcode | Body |
|-----------|--------|------|
| Request | **34** | one `put_string` `group_id` |
| Response | **35** | `u16 error_code`, `string group_id`, `u32 generation`, `u32 member_count`, then per member: `string member_id`, `u32 topic_count` + strings, `u32 assignment_count` + `{string topic, u32 partition}` |

Error **2** = `NotFound` (unknown group or no live members). Clients
raise `BrokerError` / `BrokerException` with `op="describe_group"`.

### ListGroups

| Direction | Opcode | Body |
|-----------|--------|------|
| Request | **36** | empty payload |
| Response | **37** | `u16 error_code`, `u32 group_count`, then per group: `string group_id`, `u8 state`, `u32 member_count`, `u32 generation` |

`state`: **0** Empty (offsets only), **1** Stable (live members).
Unknown state bytes decode as Empty (same as Rust `GroupState::from_u8`).

## API

```python
groups = c.list_groups()            # list[GroupListing]
desc = c.describe_group("cg-1")     # DescribeGroupResult
# GroupListing: group_id, state (GroupState.EMPTY/STABLE), member_count, generation
# DescribeGroupResult: group_id, generation, members[GroupMemberInfo]
# GroupMemberInfo: member_id, topics, assignment[Assignment]
```

```go
groups, err := c.ListGroups()                 // []GroupListing
desc, err := c.DescribeGroup("cg-1")          // DescribeGroupResult
```

```java
List<Codec.GroupListing> groups = c.listGroups();
DescribeGroupResult desc = c.describeGroup("cg-1");
```

Existing methods are unchanged. Empty / unknown groups are a broker
error 2 on describe, not an empty result.

## Tests

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| DescribeGroup request `"cg-1"` | bytes `04 00 63 67 2d 31` |
| DescribeGroup response (1 member, 2 assignments) | Phase 11 rust fixture shape; `decode_response(35, …)` |
| DescribeGroup error 2 | bytes start `02 00`; client raises code 2 |
| ListGroups request | empty payload |
| ListGroups response (g1 Stable + g2 Empty) | Phase 12 rust fixture; `decode_response(37, …)` |
| Fake TCP list | two groups, empty + stable |
| Fake TCP describe | members + assignment |
| Fake TCP describe error 2 | raises |

## Honesty leftovers

- This is **not Kafka** `DescribeGroups` / `ListGroups`. No
  `include_authorized_operations`, no `StatesFilter` / `TypesFilter`,
  no `protocol_type`, no `group_instance_id` on native DescribeGroup
  members, no PreparingRebalance / CompletingRebalance / Dead states.
  Native list state is only Empty (0) or Stable (1).
- DescribeGroup reflects **live membership only**. A group that only
  has committed offsets is listed as Empty; `describe_group` on it
  (or on an unknown id) is error **2**, not an empty member list.
- DeleteOffsets / DeleteGroups are still Rust/CLI-only.
- Language clients still do not speak Kafka API keys 15/16.

See [PHASE11_SPEC.md](./PHASE11_SPEC.md) (DescribeGroup) and
[PHASE12_SPEC.md](./PHASE12_SPEC.md) (ListGroups).
