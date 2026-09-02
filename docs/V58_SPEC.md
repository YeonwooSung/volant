# v0.58 — AddBroker / RemoveBroker / ListMembers on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “language clients cannot speak membership
overlay admin.” Rust `volant-client` already has `add_broker` /
`remove_broker` / `list_members`. This slice ports native opcodes
**102–107** (v0.10) to the Python, Go, and Java clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes,
extend homemade metadata Raft, or change the broker membership
handler. Overlay remains the source of truth. Follower forward
(v0.38) is already on the broker; clients just send 102 / 104 / 106.

## Goals

1. **Codec** encode/decode for AddBroker (102/103), RemoveBroker
   (104/105), and ListMembers (106/107) in Python, Go, and Java.
   Match `crates/volant-protocol/src/payload.rs`. Shared
   `put_membership_broker` / `get_membership_broker`.
2. **Public RPC** on each language client, matching the Rust shape.
3. Non-zero `error_code` raises like every other RPC (`BrokerError` /
   `BrokerException`) with `op="add_broker"` / `"remove_broker"` /
   `"list_members"`.
4. Unit tests without a broker: codec round-trip plus a fake TCP
   server (add / remove return generation; list parses brokers +
   live; error raises). Existing tests still pass.

## Non-goals

| Deferred | Why |
|----------|-----|
| Homemade metadata Raft / Phase 155 | Frozen; overlay is still SoT |
| Openraft membership as SoT | Overlay file remains SoT |
| Kafka AlterPartitionReassignments / broker catalog | Native 102–107 only |
| Follower forward | Already broker-side (v0.38) |
| New native opcodes | Reuse 102–107 |
| Broker / protocol / Rust client changes | Already shipped (v0.10) |
| Kafka API keys | Frozen |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`. Payload integers are
little-endian. Strings are `u16` length + UTF-8 (`put_string`).

### `MembershipBroker`

```
id: u32
host: string
port: u16
rack_present: u8     # 1 = next is string, 0 = absent
  if 1: rack: string
```

### Request opcode 102 `AddBroker`

One `MembershipBroker`.

### Response opcode 103 `AddBroker`

```
error_code: u16
generation: u64      # 0 on error
```

### Request opcode 104 `RemoveBroker`

```
id: u32
```

### Response opcode 105 `RemoveBroker`

```
error_code: u16
generation: u64
```

### Request opcode 106 `ListMembers`

Empty payload.

### Response opcode 107 `ListMembers`

```
error_code: u16
generation: u64
broker_count: u32
  for each: MembershipBroker
live_count: u32
  for each: id u32
```

This is **not** Kafka AlterPartitionReassignments and **not** a Kafka
broker catalog. Overlay is still SoT.

## API

`MembershipList` has `generation`, `brokers`, `live`.
`MembershipBroker` has `id`, `host`, `port`, `rack` (`None` / `nil` /
`null` = absent).

```python
c.add_broker(id: int, host: str, port: int, rack: Optional[str] = None) -> int
c.remove_broker(id: int) -> int
c.list_members() -> MembershipList
```

```go
c.AddBroker(id uint32, host string, port uint16, rack *string) (uint64, error)
c.RemoveBroker(id uint32) (uint64, error)
c.ListMembers() (MembershipList, error)
```

```java
c.addBroker(int id, String host, int port)                 // no rack
c.addBroker(int id, String host, int port, String rack)    // null rack = absent
c.removeBroker(int id)   // returns long generation
c.listMembers()          // MembershipList
```

Non-zero `error_code` raises `BrokerError(..., op="add_broker")`
(Python), `BrokerError{Code, Op: "add_broker"}` (Go), or
`BrokerException(code, "", "add_broker")` (Java) — same `op` strings
for remove / list.

## Tests

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode AddBroker id=2 host=`10.0.0.2` port=9092 rack=`r1` | request + response generation |
| AddBroker with no rack | wire flag **0** |
| Encode/decode RemoveBroker id=2 | request + response generation |
| ListMembers two brokers + live `[1, 2]` | parsed brokers + live ids |
| Fake server add / remove | returns generation |
| Fake server list | parses brokers + live |
| Fake server `error_code != 0` | raises with matching `op` |

## Merge notes

Sibling slices **v0.56 / v0.57 / v0.59** also edit the language
**codec** / Client / README files. When merging:

- **Keep all opcodes.** Do not drop 102–107 or any opcode another
  slice added (`decode_response` / `DecodeResponse` / `decodeResponse`
  is a switch — union every case).
- Membership path is **additive**. Do not reuse 102–107 for a new API.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- Overlay is still SoT (not KRaft / openraft membership as source of
  truth).
- Follower forward is broker-side (v0.38); clients just send 102 /
  104 / 106.
- Not Kafka AlterPartitionReassignments / broker catalog.
- No Kafka API keys / new opcodes / Phase 155.
- Split-brain on concurrent add/remove is unchanged (v0.10).

See [V10_SPEC.md](./V10_SPEC.md) (overlay + native 102–107) and
[V38_SPEC.md](./V38_SPEC.md) (follower forward).
