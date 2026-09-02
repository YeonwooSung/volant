# v0.51 — CreatePartitions on Python / Go / Java clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “language clients still lack CreatePartitions.”
Rust `volant-client` already has `Client::create_partitions`. This slice
ports native opcode **46 / 47** (Phase 15) to the Python, Go, and Java
clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker CreatePartitions handler.

## Goals

1. **Codec** encode/decode for CreatePartitions request (opcode **46**) and
   response (opcode **47**) in Python, Go, and Java. Match
   `crates/volant-protocol/src/payload.rs` `Request::CreatePartitions` /
   `Response::CreatePartitions`.
2. **Public RPC** on each language client. Returns the new total
   partition count (`u32` / `int`).
3. Non-zero `error_code` raises like every other RPC (`BrokerError` /
   `BrokerError` / `BrokerException`) with `op="create_partitions"`.
4. Unit tests without a broker: payload fixture topic `"events"`,
   `total_count` **4**, response `{0, "events", 4}` plus a fake server
   that returns 4 on success and raises on `error_code != 0`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka CreatePartitions (API key 37) | Native 46/47 only |
| Wait for all brokers to apply | Same as Rust / Phase 15 |
| Shrink partitions | Broker rejects `total_count <= current` (error 3) |
| New native opcodes | Reuse 46 / 47 |
| Broker / protocol / Rust client changes | Already shipped (Phase 15) |
| DeleteRecords / Describe-AlterConfigs / DeleteOffsets / SCRAM admin | Separate leftovers |
| Kafka API keys | Frozen |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`. Payload integers are
little-endian. Strings are `u16` length + UTF-8 (`put_string`).

### Request opcode 46 `CreatePartitions`

```
topic: string
total_count: u32   # desired total partition count (must exceed current)
```

### Response opcode 47 `CreatePartitions`

```
error_code: u16    # 0=ok; 2=not found; 3=invalid; 14=not controller
topic: string
partitions: u32    # new total (`0` on error)
```

This is **not** Kafka CreatePartitions. There is no `validate_only`,
no replica assignment array, and no TopicId on this opcode.

## API

Public RPC matches Rust `create_partitions` → `u32` new count.

```python
c.create_partitions(topic: str, total_count: int) -> int
c.create_partitions("events", 4)
```

```go
c.CreatePartitions(topic string, totalCount uint32) (uint32, error)
c.CreatePartitions("events", 4)
```

```java
c.createPartitions(String topic, int totalCount)  // returns int new count
c.createPartitions("events", 4);
```

Non-zero `error_code` raises `BrokerError(..., op="create_partitions")`
(Python), `BrokerError{Code, Op: "create_partitions"}` (Go), or
`BrokerException(code, "", "create_partitions")` (Java).

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode topic `"events"`, `total_count` `4` | request + response `{0, "events", 4}` |
| `decode_response(47, …)` | `CreatePartitionsResponse` |
| Fake server success | returns **4** |
| Fake server `error_code != 0` | raises with `op="create_partitions"` |

## Merge notes

Sibling slices **v0.52–v0.55** also edit the same language **codec** /
**Client** / **README** files. When merging:

- **Keep all opcodes.** Do not drop 46/47 or any opcode another slice
  added (`decode_response` / `DecodeResponse` / `decodeResponse` is a
  switch — union every case).
- CreatePartitions is **additive**: request **46**, response **47**. Do
  not reuse those numbers for a new API.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- **Not Kafka CreatePartitions** (API key 37). Native 46/47 only.
- **Does not wait for all brokers** (same as Rust / Phase 15).
- **Cannot shrink partitions.** `total_count` must exceed the current
  count; the broker returns error 3 otherwise.
- Language clients still lack DeleteRecords / Describe-AlterConfigs /
  DeleteOffsets / SCRAM admin.
- No Kafka API keys / new opcodes / broker changes / Phase 155.

See [PHASE15_SPEC.md](./PHASE15_SPEC.md) (native 46/47) and
[PHASE27_SPEC.md](./PHASE27_SPEC.md) (Kafka CreatePartitions on the shim).
