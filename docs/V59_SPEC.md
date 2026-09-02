# v0.59 — ReassignPartitions on Python / Go / Java clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “language clients cannot reassign replicas.”
Rust `volant-client` already has `Client::reassign_partitions`. This
slice ports native opcode **114 / 115** (v0.18) to the Python, Go, and
Java clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker ReassignPartitions handler.

## Goals

1. **Codec** encode/decode for ReassignPartitions request (opcode **114**)
   and response (opcode **115**) in Python, Go, and Java. Match
   `crates/volant-protocol/src/payload.rs` `Request::ReassignPartitions` /
   `Response::ReassignPartitions`.
2. **Public RPC** on each language client. `partition=None` / `nil` /
   `null` encodes as `u32::MAX` (all partitions of the topic). Empty
   `replicas` encodes as count **0** (broker auto-place, same as
   CreateTopic).
3. Non-zero `error_code` raises like every other RPC (`BrokerError` /
   `BrokerError` / `BrokerException`) with `op="reassign_partitions"`.
4. Unit tests without a broker: payload fixture topic `"events"`,
   partition `0`, replicas `[1, 2]`; all-partitions sentinel
   `0xFFFFFFFF` + empty replicas; plus a fake server that returns a
   generation on success and raises on `error_code != 0`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka AlterPartitionReassignments (API key 45) | Native 114/115 only |
| Overlay / assignment wait-rollback | Broker-side (v0.18 / v0.39) |
| Client-picked racks | Empty replicas = auto-place; no extra rack API |
| New native opcodes | Reuse 114 / 115 |
| Broker / protocol / Rust client changes | Already shipped (v0.18) |
| AddBroker / ListMembers / ACLs / BeginTxn | Separate leftovers |
| Kafka API keys | Frozen |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`. Payload integers are
little-endian. Strings are `u16` length + UTF-8 (`put_string`).

### Request opcode 114 `ReassignPartitions`

```
topic: string
partition: u32     # u32::MAX (4294967295) = all partitions of the topic
replica_count: u32
  for each: replica_id u32
```

Empty `replicas` = auto-place with current membership (same as
CreateTopic). Sentinel: `REASSIGN_ALL_PARTITIONS = 0xFFFFFFFF`
(`u32::MAX`).

### Response opcode 115 `ReassignPartitions`

```
error_code: u16    # 0=ok; 2=not found; 3=invalid; 14=not controller
generation: u32    # assignment generation after apply (`0` on error)
```

This is **not** Kafka AlterPartitionReassignments. There is no
throttle, no per-partition error array, and no TopicId on this opcode.

## API

Public RPC matches Rust `reassign_partitions` → `u32` generation.

```python
c.reassign_partitions(topic: str, replicas: list[int], partition: Optional[int] = None) -> int
# partition=None → wire u32::MAX (all)
# replicas=[] → auto-place
c.reassign_partitions("events", [1, 2], partition=0)
c.reassign_partitions("events", [])
```

```go
c.ReassignPartitions(topic string, replicas []uint32, partition *uint32) (uint32, error)
// nil partition = all; nil/empty replicas = auto
c.ReassignPartitions("events", []uint32{1, 2}, &part)
c.ReassignPartitions("events", nil, nil)
```

```java
c.reassignPartitions(String topic, int... replicas)           // all partitions
c.reassignPartitions(String topic, Integer partition, int... replicas)  // null partition = all
c.reassignPartitions("events", new int[] {1, 2});
c.reassignPartitions("events", Integer.valueOf(0), new int[] {1, 2});
c.reassignPartitions("events");
```

Non-zero `error_code` raises `BrokerError(..., op="reassign_partitions")`
(Python), `BrokerError{Code, Op: "reassign_partitions"}` (Go), or
`BrokerException(code, "", "reassign_partitions")` (Java).

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode topic `"events"`, partition `0`, replicas `[1,2]` | request + response `{0, generation}` |
| All-partitions sentinel `0xFFFFFFFF` + empty replicas | request count 0 |
| `decode_response(115, …)` | `ReassignPartitionsResponse` |
| Fake server success | returns generation |
| Fake server `error_code != 0` | raises with `op="reassign_partitions"` |

## Merge notes

Sibling slices **v0.56–v0.58** also edit the same language **codec** /
**Client** / **README** files. When merging:

- **Keep all opcodes.** Do not drop 114/115 or any opcode another slice
  added (`decode_response` / `DecodeResponse` / `decodeResponse` is a
  switch — union every case). Especially keep 102–107 if a sibling
  added AddBroker / RemoveBroker / ListMembers.
- ReassignPartitions is **additive**: request **114**, response **115**.
  Do not reuse those numbers for a new API.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- **Not Kafka AlterPartitionReassignments** (API key 45). Native
  114/115 only.
- Overlay / assignment wait-rollback stay broker-side (v0.18 / v0.39).
- Empty replicas = auto-place; does not let the client pick racks
  beyond what the broker already does.
- Language clients still lack AddBroker / RemoveBroker / ListMembers
  (102–107) and the rest of the admin opcodes Rust has.
- No Kafka API keys / new opcodes / broker changes / Phase 155.

See [V18_SPEC.md](./V18_SPEC.md) (native 114/115) and
[V39_SPEC.md](./V39_SPEC.md) (reassign-on-add rollback).
