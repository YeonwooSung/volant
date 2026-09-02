# v0.57 — BeginTxn / EndTxn on Python / Go / Java clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “language clients have idempotent produce
(v0.47) but no transactions.” Rust `volant-client` already has
`begin_transaction` / `commit_transaction` / `abort_transaction`. This
slice ports native opcodes **50–53** (Phase 18) to the Python, Go, and
Java clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / `volant-protocol` / Rust client.

This is **not** Kafka transactions (API keys 22/24/25/26/28). Native
opcodes only.

## Goals

1. **Codec** encode/decode for BeginTxn (50/51) and EndTxn (52/53) in
   Python, Go, and Java. Match `crates/volant-protocol/src/payload.rs`
   `Request::BeginTxn` / `Request::EndTxn` / `Response::BeginTxn` /
   `Response::EndTxn` / `TxnOffsetCommit` / `TxnProduceResult`.
2. **`transactional_id`** on the Client (optional string). If set,
   `InitProducerId` (opcode 32) sends that string instead of empty.
   Existing `enable_idempotence` still works with empty id.
3. **Public RPC** `begin_transaction` / `commit_transaction` /
   `abort_transaction` matching Rust `client.rs`.
4. Produce during an open txn uses the same v0.47 idempotent trailer
   (pid/epoch/seq). No new produce path.
5. After EndTxn success, `in_transaction = false`. Abort rewinds
   per-partition sequences to the BeginTxn snapshot.
6. Reconnect / redirect (v0.43) does **not** clear pid/epoch/txn id.
7. Unit tests without a broker: codec round-trip plus a fake TCP
   server. Existing v0.47 idempotent tests still pass (empty
   transactional_id).

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka InitProducerId / AddPartitionsToTxn / EndTxn (API keys 22/24/25/26/28) | Native 50–53 only |
| `TxnProducer` helper class | Client methods are enough (Rust helper is optional) |
| Broker-side produce buffering changes | Already shipped (Phase 18) |
| New native opcodes | Reuse 50–53 |
| Broker / protocol / Rust client changes | Already shipped (Phase 18) |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`. Payload integers are
little-endian. Strings are `u16` length + UTF-8 (`put_string`).

### Request opcode 50 `BeginTxn`

```
producer_id: u64
producer_epoch: u16
```

### Response opcode 51 `BeginTxn`

```
error_code: u16   # 0=ok; 19=bad epoch; 21=unknown PID; 22=invalid txn state
```

### Request opcode 52 `EndTxn`

```
producer_id: u64
producer_epoch: u16
committed: u8     # 1=commit, 0=abort
offset_count: u32
  for each TxnOffsetCommit:
    group_id: string
    topic: string
    partition: u32
    offset: u64
    metadata: string
```

### Response opcode 53 `EndTxn`

```
error_code: u16
result_count: u32
  for each TxnProduceResult:
    topic: string
    partition: u32
    base_offset: u64
    count: u32
```

Commit returns per-batch results; abort returns empty results.

## Client behavior (match Rust `client.rs`)

1. **`transactional_id`** on the Client (optional string). If set,
   `InitProducerId` (opcode 32) sends that string instead of empty.
   `transactional_id` without `enable_idempotence` still Inits on first
   begin (`ensure_producer_id`). A set id implies pid/seq trailers on
   produce.
2. `begin_transaction` / `beginTxn`:
   - error if `transactional_id` is unset/empty (`ValueError` /
     `fmt.Errorf` / `IllegalStateException`) **before send**;
   - ensure producer id (Init if needed, **with** transactional_id);
   - send opcode 50 with stored pid/epoch;
   - mark in-transaction; snapshot current per-partition sequences.
3. `commit_transaction(offsets=)` / `CommitTransaction`: EndTxn
   committed=1. Returns list of `TxnProduceResult`.
4. `abort_transaction` / `AbortTransaction`: EndTxn committed=0, empty
   offsets. Rewind sequences to the snapshot (broker discarded pending).
5. Produce during an open txn uses the same idempotent trailer as v0.47.
6. After EndTxn success, `in_transaction = false`.
7. Reconnect / redirect (v0.43) does **not** clear pid/epoch/txn id.
   Do not re-Init unless error 21.

Non-zero `error_code` raises with `op="begin_txn"` / `"end_txn"`.

## API

```python
c = Client("127.0.0.1:9092", transactional_id="txn-1")  # implies Init with that id
c.begin_transaction()
c.produce(...)
c.commit_transaction()  # or commit_transaction(offsets=[TxnOffsetCommit(...)])
c.abort_transaction()
```

```go
c.SetTransactionalID("txn-1")
c.BeginTransaction()
c.CommitTransaction(nil) // or []codec.TxnOffsetCommit
c.AbortTransaction()
```

```java
c.setTransactionalId("txn-1");
c.beginTransaction();
c.commitTransaction(); // or List<TxnOffsetCommit>
c.abortTransaction();
```

Existing constructors stay. Default remains non-transactional.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode BeginTxn pid=1 epoch=0 | request `u64` 1 + `u16` 0 |
| EndTxn commit with one offset | request committed=1, one `TxnOffsetCommit` |
| EndTxn abort empty | committed=0, offset_count 0 |
| Fake server begin → produce → commit | Init with txn id, produce seq=0, EndTxn committed=1 |
| Abort rewinds seq | second produce after abort uses seq=0 again |
| Missing transactional_id | raises before send |
| `error_code` 22 | raises with `op="begin_txn"` |
| Existing v0.47 idempotent tests | still pass (empty transactional_id) |

## Merge notes

Siblings **v0.56 / v0.58 / v0.59** also edit codecs and Client. When
merging:

- **Keep all opcodes.** Do not drop 50–53 or any opcode another slice
  added (`decode_response` / `DecodeResponse` / `decodeResponse` is a
  switch — union every case).
- BeginTxn / EndTxn is **additive**: request **50/52**, response
  **51/53**. Do not reuse those numbers.
- Preserve `enable_idempotence` + `max_redirects` + SCRAM/Auth fields.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- **Not Kafka transactions.** Native 50–53 only. No InitProducerId /
  AddPartitionsToTxn / EndTxn API keys. librdkafka will not speak this.
- **No `TxnProducer` helper** required. Client methods are enough.
- Produce is still one TCP connection; no extra broker-side produce
  buffering beyond what the native broker already does.
- No Kafka API keys / new opcodes / Phase 155.
- Language clients still lack ACLs / AddBroker / ListMembers /
  ReassignPartitions.

See [PHASE18_SPEC.md](./PHASE18_SPEC.md) (native 50–53) and
[V47_SPEC.md](./V47_SPEC.md) (idempotent produce trailer).
