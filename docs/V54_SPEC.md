# v0.54 — DeleteOffsets on Python / Go / Java clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “DeleteOffsets is still Rust/CLI-only.”
Rust `volant-client` already has `Client::delete_offsets`. This slice
ports native opcode **38 / 39** (Phase 12) to the Python, Go, and Java
clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker DeleteOffsets handler.

There is **no** native DeleteGroups opcode. This slice is DeleteOffsets
only.

## Goals

1. **Codec** encode/decode for DeleteOffsets request (opcode **38**) and
   response (opcode **39**) in Python, Go, and Java. Match
   `crates/volant-protocol/src/payload.rs` `Request::DeleteOffsets` /
   `Response::DeleteOffsets`. Reuse each language’s existing
   `{topic, partition}` `OffsetEntry` (same shape as OffsetFetch).
2. **Public RPC** on each language client. Empty / omitted entries
   encode as `u32` count **0** (broker: all committed offsets for the
   group).
3. Non-zero `error_code` raises like every other RPC (`BrokerError` /
   `BrokerError` / `BrokerException`) with `op="delete_offsets"`.
4. Unit tests without a broker: payload fixture (group `"g"`, one
   entry `("events", 0)`, response `{0, 1}`) plus a fake server that
   records an empty entry list and raises on `error_code != 0`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka OffsetDelete (API key 47) | Native 38/39 only |
| Kafka DeleteGroups (API key 42) | No native DeleteGroups opcode exists; do not invent one |
| New native opcodes | Reuse 38 / 39 |
| Broker / protocol / Rust client changes | Already shipped (Phase 12) |
| CreatePartitions / DeleteRecords / Describe-AlterConfigs / SCRAM admin | Sibling leftovers |
| Kafka API keys | Frozen (`SUPPORTED_APIS` stays 38) |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`. Payload integers are
little-endian. Strings are `u16` length + UTF-8 (`put_string`).

### Request opcode 38 `DeleteOffsets`

```
group_id: string
entry_count: u32     # 0 = all offsets for the group
  for each:
    topic: string
    partition: u32
```

`OffsetEntry` is the same shape as OffsetFetch selectors (topic +
partition).

### Response opcode 39 `DeleteOffsets`

```
error_code: u16
deleted_count: u32   # number of offset files removed
```

This is **not** Kafka OffsetDelete. There is no per-partition error
array and no DeleteGroups opcode.

## API

Empty entries delete **all** committed offsets for the group (same as
Rust).

```python
c.delete_offsets(group: str, entries: Optional[list[tuple[str,int]]] = None) -> int
# returns deleted_count. None or [] = all offsets for the group (wire count 0)
c.delete_offsets("g")
c.delete_offsets("g", [("events", 0)])
```

```go
c.DeleteOffsets(group string, entries []codec.OffsetEntry) (uint32, error)
// nil or empty = all
c.DeleteOffsets("g", nil)
c.DeleteOffsets("g", []codec.OffsetEntry{{Topic: "events", Partition: 0}})
```

```java
c.deleteOffsets(String group)                         // all
c.deleteOffsets(String group, List<OffsetEntry> entries)  // empty/null = all
// returns int deletedCount
c.deleteOffsets("g");
c.deleteOffsets("g", List.of(new Codec.OffsetEntry("events", 0)));
```

Non-zero `error_code` raises `BrokerError(..., op="delete_offsets")`
(Python), `BrokerError{Code, Op: "delete_offsets"}` (Go), or
`BrokerException(code, "", "delete_offsets")` (Java).

## Tests

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode group `"g"`, one entry `("events", 0)` | request count 1; response `{error_code=0, deleted_count=1}` |
| Empty entries | request `u32` count **0** |
| Fake server success | parsed `deleted_count` |
| Fake server `error_code != 0` | raises with `op="delete_offsets"` |
| `decode_response(39, …)` | `DeleteOffsetsResponse` |

## Merge notes

v0.51–v0.53 / v0.55 are parallel residual slices that also edit the
language **codec** / Client / README files (other existing native
opcodes). When merging:

- **Keep all opcodes.** Do not drop 38/39 or any opcode another slice
  added (`decode_response` / `DecodeResponse` / `decodeResponse` is a
  switch — union every case).
- DeleteOffsets is **additive**: request **38**, response **39**. Do
  not reuse those numbers for a new API.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- **Not Kafka OffsetDelete (API key 47)** and **not DeleteGroups
  (API key 42)**. Native 38/39 only.
- Empty entries delete **all** committed offsets for the group (same
  as Rust).
- No native DeleteGroups opcode exists; this slice does not add one.
- Language clients still lack CreatePartitions / DeleteRecords /
  Describe-AlterConfigs / SCRAM admin.
- Does not change broker DeleteOffsets, ACLs, or Kafka API keys.

See [PHASE12_SPEC.md](./PHASE12_SPEC.md) (native 38/39).
