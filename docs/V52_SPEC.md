# v0.52 — DeleteRecords on Python / Go / Java clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “language clients still lack DeleteRecords.”
Rust `volant-client` already has `Client::delete_records` and
`delete_records_with_wait_flag`. This slice ports native opcode **44 /
45** (Phase 14 + Phase 137 wait trailer) to the Python, Go, and Java
clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker DeleteRecords handler.

## Goals

1. **Codec** encode/decode for DeleteRecords request (opcode **44**) and
   response (opcode **45**) in Python, Go, and Java. Match
   `crates/volant-protocol/src/payload.rs` `Request::DeleteRecords` /
   `Response::DeleteRecords`.
2. **Public RPC** on each language client matching Rust:
   `delete_records` / `DeleteRecords` sends `wait_majority=0`;
   Go/Java also expose the Phase 137 wait-flag overload.
3. Non-zero `error_code` raises like every other RPC (`BrokerError` /
   `BrokerError` / `BrokerException`) with `op="delete_records"`.
   Error **13** is **not** auto-redirected (v0.43 is Produce/Fetch only).
4. Unit tests without a broker: payload fixture matching
   `payload.rs` `phase14_delete_records_roundtrip` and
   `phase137_delete_records_wait_majority_trailer` plus a fake server
   that returns `low_watermark` and raises on `error_code == 13`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka DeleteRecords (API key 21) | Native 44/45 only; Kafka shim already has 21 |
| New native opcodes | Reuse 44 / 45 |
| Broker / protocol / Rust client changes | Already shipped (Phase 14 / 137) |
| v0.43-style leader redirect on error 13 | Produce/Fetch only |
| Change wait-off dual-ACK (v0.45) | Broker env; this slice does not touch it |
| CreatePartitions / Describe-AlterConfigs / DeleteOffsets | Sibling leftovers |
| Kafka API keys | Frozen |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`. Payload integers are
little-endian. Strings are `u16` length + UTF-8 (`put_string`).

### Request opcode 44 `DeleteRecords`

```
topic: string
partition: u32
before_offset: u64
wait_majority: u8   # always write. Decode: if trailer absent → 0
                    # 0 = broker default, 1 = force wait, 2 = force no-wait
```

### Response opcode 45 `DeleteRecords`

```
error_code: u16     # 0=ok; 2=not found; 13=not leader
topic: string
partition: u32
low_watermark: u64  # new log start
```

This is **not** Kafka DeleteRecords. There is no topics-array, no
timeout-ms, and no Kafka API key 21 on these clients.

## API

`DeleteRecordsResult` has `topic`, `partition`, `low_watermark`.

```python
c.delete_records(topic, partition, before_offset, wait_majority=0) -> DeleteRecordsResult
# result has topic, partition, low_watermark
```

```go
c.DeleteRecords(topic string, partition uint32, beforeOffset uint64) (DeleteRecordsResult, error)
c.DeleteRecordsWithWaitFlag(topic string, partition uint32, beforeOffset uint64, waitMajority uint8) (DeleteRecordsResult, error)
```

```java
c.deleteRecords(String topic, int partition, long beforeOffset)  // wait_majority=0
c.deleteRecords(String topic, int partition, long beforeOffset, int waitMajority)
// returns DeleteRecordsResult
```

Non-zero `error_code` raises `BrokerError(..., op="delete_records")`
(Python), `BrokerError{Code, Op: "delete_records"}` (Go), or
`BrokerException(code, "", "delete_records")` (Java).

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode topic `"events"`, partition `2`, before `100`, wait `0` and `1` | request trailer written; response `low_watermark=96` |
| Legacy request body **without** the u8 trailer | `wait_majority` decodes as **0** |
| Fake server success | parsed `DeleteRecordsResult` with `low_watermark` |
| Fake server `error_code=13` | raises with `op="delete_records"`; no Metadata redirect |
| `decode_response(45, …)` | `DeleteRecordsResponse` |

## Merge notes

Sibling slices **v0.51 / v0.53–v0.55** also edit the language
**codec** / Client / README files. When merging:

- **Keep all opcodes.** Do not drop 44/45 or any opcode another slice
  added (`decode_response` / `DecodeResponse` / `decodeResponse` is a
  switch — union every case).
- DeleteRecords is **additive**: request **44**, response **45**. Do not
  reuse those numbers for a new API.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- **Not Kafka DeleteRecords.** Native 44/45 only. Kafka API key 21 stays
  on the shim.
- **Error 13 is not auto-redirected.** v0.43 leader redirect is
  Produce/Fetch only. Raise 13 like any other broker error.
- Cluster wait-off still requires **both**
  `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE` **and**
  `VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK` (v0.45). This slice does
  not change that.
- `wait_majority=2` is force no-wait; irreversible on the broker when
  allowed.
- Language clients still lack CreatePartitions (46/47),
  Describe/AlterConfigs, DeleteOffsets, and the rest of the admin
  opcodes Rust has.
- Does not change broker DeleteRecords, ACLs, or Kafka API key 21.
- No Kafka API keys / new opcodes / broker changes / Phase 155.

See [PHASE14_SPEC.md](./PHASE14_SPEC.md) (native 44/45),
[PHASE137_SPEC.md](./PHASE137_SPEC.md) (wait trailer), and
[V45_SPEC.md](./V45_SPEC.md) (wait-off dual ACK).
