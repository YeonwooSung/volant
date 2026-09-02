# v0.63 — TransactionalProducer helper on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V57_SPEC.md](./V57_SPEC.md): language
clients have BeginTxn / EndTxn on `Client` but no
`TransactionalProducer` helper. Rust `volant-client` already has
`crates/volant-client/src/txn.rs`. This slice ports that thin wrapper
to the Python, Go, and Java clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / `volant-protocol` / Rust client.

This is **not** Kafka transactions (API keys 22/24/25/26/28). Native
opcodes 50–53 only. Codecs are unchanged (v0.57).

## Goals

1. **`TransactionalProducer`** on Python, Go, and Java: a thin wrapper
   around the existing v0.57 `Client` APIs (`begin_transaction` /
   `commit_transaction` / `abort_transaction` / `produce`).
2. Constructor / `from` / `NewTransactionalProducer` fails if
   `transactional_id` is unset (same check as `begin_transaction`).
3. `add_offsets` queues locally; nothing is sent until `commit`.
4. `abort` clears the queue then `abort_transaction`.
5. Double `begin` while open: raise. `commit` / `abort` while not
   open: raise.
6. Reuse existing `TxnOffsetCommit` / `TxnProduceResult` types. Do
   **not** change BeginTxn / EndTxn codecs.
7. Unit tests without a broker: existing v0.57 scripted TCP server.
   Existing v0.57 txn tests still pass.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka InitProducerId / AddPartitionsToTxn / EndTxn (API keys 22/24/25/26/28) | Native 50–53 only |
| New native opcodes | Reuse 50–53 |
| Broker / protocol / Rust client changes | Already shipped (Phase 18 / v0.57) |
| Produce buffering / LSO on the client | Write-through; LSO/commit is broker-side |
| Extra fencing beyond InitProducerId transactional_id | Same as Rust helper |
| Kafka API keys / Phase 155 / homemade Raft | Frozen |

## API

```python
from volant import TransactionalProducer
p = TransactionalProducer(c)          # c must have transactional_id
p.begin()
p.produce("t", 0, value=b"x")         # delegates to Client.produce
p.add_offsets("g", [("t", 0, 1)])     # queues TxnOffsetCommit
results = p.commit()                  # Client.commit_transaction(queued)
p.abort()                             # clears queue + abort_transaction
p.is_open()
```

```go
p := volant.NewTransactionalProducer(c)
p.Begin()
p.Produce("t", 0, nil, []byte("x"))
p.AddOffsets("g", []volant.TxnOffset{{Topic: "t", Partition: 0, Offset: 1}})
// or p.AddOffset("g", "t", 0, 1)
results, err := p.Commit()
p.Abort()
p.IsOpen()
```

```java
TransactionalProducer p = TransactionalProducer.from(c);
p.begin();
p.produce("t", 0, null, "x".getBytes());
p.addOffsets("g", topic, partition, offset); // or List<TxnOffsetCommit>
List<TxnProduceResult> r = p.commit();
p.abort();
p.isOpen();
```

Existing `Client` BeginTxn / EndTxn methods stay. Default remains
non-transactional.

## Behavior (match Rust `txn.rs`)

1. Wrap an existing `Client` that already has `transactional_id`.
   Constructor fails with the same “transactional_id not configured”
   error as `begin_transaction` if the id is unset or empty.
2. `begin`: error if already open; then `Client.begin_transaction`;
   clear the local offset queue; mark open.
3. `produce`: delegates to `Client.produce` (write-through; LSO holds
   until broker commit).
4. `add_offsets`: append `TxnOffsetCommit` rows locally (empty
   metadata). No RPC.
5. `commit`: error if not open; take the queue; `commit_transaction`
   with those offsets; mark closed. Returns `TxnProduceResult` list.
6. `abort`: error if not open; clear the queue; `abort_transaction`
   (EndTxn committed=0); mark closed.
7. `is_open`: local flag only.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| begin → produce → add_offsets → commit | EndTxn committed=1 with queued offsets |
| abort after add_offsets | EndTxn committed=0, empty offsets; later commit does not replay |
| Missing transactional_id | constructor / `from` / `New` fails before send |
| commit / abort while not open | error; no opcode |
| Double begin while open | error; one BeginTxn only |
| Existing v0.57 txn tests | still pass |

## Merge notes

Prefer **new files** (`txn.py`, `txn.go`, `TransactionalProducer.java`).
Touch `Client` only if a constructor must be exported. Do not rewrite
produce / fetch. Do not change BeginTxn / EndTxn codecs.

Siblings **v0.56 / v0.58 / v0.59** also edit codecs and Client. When
merging, keep all opcodes. This slice is additive helpers only.

## Honesty leftovers

- **Still native 50–53, not Kafka transactions.** No InitProducerId /
  AddPartitionsToTxn / EndTxn API keys. librdkafka will not speak this.
- **Produce is write-through** (same as the Rust helper); LSO / commit
  is broker-side. The helper does not buffer produces.
- **No extra fencing** beyond InitProducerId `transactional_id`.
- **No Kafka API keys / opcodes / Phase 155.**

See [V57_SPEC.md](./V57_SPEC.md) (BeginTxn / EndTxn on language
clients) and [PHASE18_SPEC.md](./PHASE18_SPEC.md) (native 50–53).
