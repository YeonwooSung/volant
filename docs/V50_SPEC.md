# v0.50 — ListOffsets on Python / Go / Java clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “language clients can produce / fetch /
offset-commit but cannot ask the broker for earliest/latest offsets.”
Rust `volant-client` already has `Client::list_offsets`. This slice
ports native opcode **48 / 49** (Phase 15) to the Python, Go, and Java
clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker ListOffsets handler.

## Goals

1. **Codec** encode/decode for ListOffsets request (opcode **48**) and
   response (opcode **49**) in Python, Go, and Java. Match
   `crates/volant-protocol/src/payload.rs` `Request::ListOffsets` /
   `Response::ListOffsets` / `OffsetListing`.
2. **Public RPC** on each language client. Empty / omitted partitions
   encode as `u32` count **0** (broker: all partitions of the topic).
3. Non-zero `error_code` raises like every other RPC (`BrokerError` /
   `BrokerError` / `BrokerException`).
4. Unit tests without a broker: payload fixture matching
   `payload.rs` `phase15_create_partitions_list_offsets_roundtrip`
   (topic `"events"`, partitions `[0, 1]`, entries `{0,0,10}` and
   `{1,2,5}`) plus a fake server that records an empty partition list
   and raises on `error_code != 0`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka ListOffsets (API key 2) isolation / timestamp | Native 48/49 returns **both** earliest and latest; no timestamp selector |
| Kafka specials (max-timestamp, earliest-local, tiered) | Kafka shim only (Phases 40 / 63 / 74) |
| Isolation `READ_COMMITTED` / LSO | Kafka shim Phase 86; native latest is LEO |
| New native opcodes | Reuse 48 / 49 |
| Broker / protocol / Rust client changes | Already shipped (Phase 15) |
| CreatePartitions (46/47) on these clients | Separate leftover |
| GroupConsumer auto-bootstrap from ListOffsets | Still OffsetFetch or 0 |
| Kafka API keys | Frozen |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`. Payload integers are
little-endian. Strings are `u16` length + UTF-8 (`put_string`).

### Request opcode 48 `ListOffsets`

```
topic: string
partition_count: u32   # 0 = all partitions
  for each: partition u32
```

### Response opcode 49 `ListOffsets`

```
error_code: u16        # 0 = ok; 2 = not found
topic: string
entry_count: u32
  for each:
    partition: u32
    earliest: u64      # log start
    latest: u64        # log end (next write / LEO)
```

This is **not** Kafka ListOffsets. There is no isolation field, no
timestamp, no replica-id, no leader-epoch fence on this opcode.

## API

`OffsetListing` has `partition`, `earliest`, `latest`.

```python
c.list_offsets(topic, partitions=None) -> list[OffsetListing]
# None or [] = all partitions (wire count 0)
c.list_offsets("events")
c.list_offsets("events", [0, 1])
```

```go
c.ListOffsets(topic string, partitions []uint32) ([]OffsetListing, error)
// nil or empty = all partitions (wire count 0)
c.ListOffsets("events", nil)
c.ListOffsets("events", []uint32{0, 1})
```

```java
c.listOffsets(String topic)
c.listOffsets(String topic, int... partitions)
// no / empty partitions = all (wire count 0)
c.listOffsets("events");
c.listOffsets("events", 0, 1);
```

Non-zero `error_code` raises `BrokerError(..., op="list_offsets")`
(Python), `BrokerError{Code, Op: "list_offsets"}` (Go), or
`BrokerException(code, "", "list_offsets")` (Java).

## Tests

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode topic `"events"`, partitions `[0,1]` | request count 2; response entries `{0,0,10}` and `{1,2,5}` |
| Empty partitions | request `u32` count **0** |
| Fake server success | parsed `OffsetListing` rows |
| Fake server `error_code != 0` | raises with `op="list_offsets"` |
| `decode_response(49, …)` | `ListOffsetsResponse` |

## Merge notes

v0.46–v0.49 are parallel residual slices that also edit the language
**codec** files (other existing native opcodes). When merging:

- **Keep all opcodes.** Do not drop 48/49 or any opcode another slice
  added (`decode_response` / `DecodeResponse` / `decodeResponse` is a
  switch — union every case).
- ListOffsets is **additive**: request **48**, response **49**. Do not
  reuse those numbers for a new API.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- **Not Kafka ListOffsets.** Native 48/49 only. No isolation, timestamp,
  max-timestamp, earliest-local, or tiered specials. Those stay on the
  Kafka shim.
- **`latest` is LEO**, not the client high-watermark (same as Rust /
  Phase 15). On a single-node leader they usually match.
- Language clients still lack CreatePartitions (46/47), DeleteRecords,
  Describe/AlterConfigs, and the rest of the admin opcodes Rust has.
- GroupConsumer does not call ListOffsets to seed a fetch position.
- Does not change broker ListOffsets, ACLs, or Kafka API key 2.

See [PHASE15_SPEC.md](./PHASE15_SPEC.md) (native 48/49) and
[PHASE25_SPEC.md](./PHASE25_SPEC.md) (Kafka ListOffsets on the shim).
