# v0.47 — idempotent produce on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “language clients send produce trailer
`(producer_id=0, producer_epoch=0, base_sequence=-1)`” by matching Rust
`volant-client`: when `enable_idempotence` is on, send native
**InitProducerId** (opcode 32) with an empty transactional_id, store the
Volant-local pid/epoch, and increment a per-(topic, partition) sequence
on each successful produce.

This is **not** Kafka idempotent produce v2. The pid is allocated by
the native broker via opcode 32/33, not Kafka API key 22. Transactions
(BeginTxn / EndTxn) are **not** implemented here.

## Goals

1. **Codec** encode/decode for InitProducerId request (32) and response
   (33) in Python, Go, and Java. Always write the transactional_id
   string (empty = non-transactional PID). Legacy empty request body
   still decodes as `""`.
2. **When idempotence is on:** first produce (or first after connect)
   sends InitProducerId; later produces attach the stored pid/epoch and
   the next per-partition sequence (starts at 0; increments by the
   batch message count after a successful produce).
3. **When off (default):** trailer stays `(0, 0, -1)`. No InitProducerId.
4. **After v0.43 redirect/reconnect:** keep the same pid/epoch/sequences.
   Do not re-Init unless the broker returns **UnknownProducerId** (21).
5. Do **not** implement transactions. Empty transactional_id only.

## Non-goals

| Deferred | Why |
|----------|-----|
| BeginTxn / EndTxn / transactional_id | Explicitly out of this slice |
| Kafka InitProducerId (API key 22) / idempotent produce v2 | Native opcodes only; pid is Volant-local |
| Persist pid across Client process restart | In-memory per Client instance, same as Rust |
| Produce retry loop (`max_retries`) | Language clients still only retry redirect + unknown-pid re-Init |
| New native opcodes | Reuse 32 / 33 |
| Phase 155 / homemade Raft | Frozen |

## Wire (unchanged)

Matches `crates/volant-protocol/src/payload.rs`:

| Direction | Opcode | Body |
|-----------|--------|------|
| Request `InitProducerId` | **32** | `put_string` transactional_id (u16 LE length + UTF-8). Empty string = non-transactional PID. Always written; legacy empty body still decodes as `""`. |
| Response `InitProducerId` | **33** | `producer_id: u64` LE, `epoch: u16` LE, `error_code: u16` LE |

Produce request already has trailer `producer_id: u64`, `producer_epoch:
u16`, `base_sequence: i32`. Default remains `(0, 0, -1)`.

Unknown producer: native `ErrorCode::UnknownProducerId = 21`.

## API

Existing produce signatures are unchanged.

```python
c = Client("127.0.0.1:9092", enable_idempotence=False)  # default
c = Client("127.0.0.1:9092", enable_idempotence=True)
```

```go
c, _ := volant.Dial("127.0.0.1:9092")
c.EnableIdempotence()
```

```java
try (Client c = Client.connect("127.0.0.1", 9092)) {
  c.setEnableIdempotence(true);
}
```

Default is **off**. Go `Dial` / `DialTLS` / `DialAuth` and Java
`connect` / `connectTls` stay as they are.

## Sequence rules

1. First produce with idempotence on: send opcode 32 (empty
   transactional_id), store pid+epoch, send produce with `base_sequence=0`.
2. After a successful produce, next sequence for that `(topic, partition)`
   is `base_sequence + batch_message_count`.
3. Failed produce (including error 13 before a successful retry) does
   **not** increment the sequence. The redirect retry reuses the same
   trailer.
4. Redirect / reconnect does **not** re-Init. pid/epoch/sequences live
   on the Client object, not the socket.
5. If a produce returns **21** (`UnknownProducerId`) after a hard
   reconnect (broker no longer knows the pid), the client re-Inits once,
   **resets sequences to 0**, and retries that produce. Documented; not
   a Kafka producer-epoch fence.

## Tests

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

1. Codec round-trip InitProducerId empty id (and legacy empty body).
2. Fake server, enable on: first produce sends opcode 32 then produce
   with pid/epoch/seq=0; second produce to the same partition uses
   seq=1 (or +batch size).
3. Enable off: no opcode 32; trailer `(0, 0, -1)`.
4. Redirect: sequences survive reconnect (Init only on the first
   connection; leader sees seq=0 then seq=1, no second Init).

## Honesty leftovers

- **Not Kafka.** Native 32/33, not API key 22. librdkafka will not
  speak this. pid is Volant-local and dies with the Client process.
- **No transactions.** Empty transactional_id only. No BeginTxn /
  EndTxn / txn fencing.
- **No produce retry beyond redirect + one unknown-pid re-Init.** Rust
  `max_retries` / backoff is not ported.
- Go/Java convenience `Produce` still sends one message per RPC.
  Python `messages=` batches increment the sequence by batch size.
- Still one TCP connection at a time. Leader redirect remains
  Produce/Fetch only (v0.43).
- Turning idempotence off after Init does not revoke the pid; later
  produces with the flag off go back to trailer `(0, 0, -1)`.

## Merge notes

v0.43 added `max_redirects` and a Produce/Fetch redirect loop on
`Client` in all three languages. This slice adds `enable_idempotence`
state next to those fields and wraps the produce path (Init + trailer
+ sequence increment) around that loop. Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`__init__`, `produce`)
- Go `clients/go/client.go` (`Client` struct, `Produce`)
- Java `clients/java/src/main/java/io/volant/Client.java` (fields, `produce`)
- Codec opcode tables / `decode_response` in all three
- Scripted brokers in `test_client.py` / `client_test.go` / `ClientTest.java`

If v0.46 also touches Client / produce (sibling residual), rebase the
produce loop so redirect (13) and unknown-pid (21) stay separate
budgets, and so reconnect does not clear pid/epoch/sequences.

## Related

- [V43_SPEC.md](./V43_SPEC.md) — leader redirect (keep pid across reconnect)
- [V42_SPEC.md](./V42_SPEC.md) — shared-token Auth
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — native InitProducerId + trailer
- [PHASE18_SPEC.md](./PHASE18_SPEC.md) — transactional_id on InitProducerId
