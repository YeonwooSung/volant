# v0.132 — Go/Java convenience Produce timestamp

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover that Go `Produce` / `ProduceAcks` /
`ProduceHeaders` and Java 4-arg / headers `produce` always send
`TimestampMs: -1`. `ProduceBatch` / `produce(..., messages)` already
carry `ProduceMessage.timestampMs`. Python already has
`produce(..., timestamp_ms=-1)`; Rust already has `Message.timestamp`.
Port a convenience onto **Go and Java only**.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / Python / Rust client.

## Goals

1. One-message Produce with a caller-supplied native timestamp,
   without breaking existing signatures.
2. Go: `ProduceTimestamp(topic, partition, key, value, timestampMs)`
   with client default acks and empty headers. `Produce` /
   `ProduceAcks` / `ProduceHeaders` still send `TimestampMs: -1`.
3. Java: named `produceTimestamp(topic, partition, key, value,
   timestampMs)` — a 5-arg `produce(..., long)` would collide with
   existing `produce(..., int acks)` and
   `produce(..., List<Header>)`. 4-arg / headers / acks overloads
   stay `-1L`.
4. Reuse existing produce retry / error **13** / error **21**. `-1`
   still means broker timestamp (current behavior).
5. Do **not** wrap JoinGroup, Heartbeat, or Produce headers/acks
   combo (siblings). Call existing `ProduceBatch` / `produce(messages)`.
6. Do **not** change broker / protocol. `ProduceMessage` already has
   `TimestampMs`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka Produce / RecordBatch timestamp types | Native opcode 1 only |
| Python / Rust API changes | Python already has `timestamp_ms=`; Rust already has `Message.timestamp` |
| ProduceAcks + timestamp / headers + timestamp | Use `ProduceBatch` / `produce(messages)` |
| JoinGroup / Heartbeat / Produce headers+acks combo | Sibling residuals |
| Broker / protocol / codec | Wire already has timestamp |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Keep existing `Produce` / 4-arg `produce` and `ProduceAcks` / 5-arg
`produce(..., acks)` signatures. Additive convenience only.

```go
c.Produce(topic, partition, key, value) // still one message, TimestampMs=-1, default acks
c.ProduceAcks(topic, partition, key, value, acks) // still TimestampMs=-1
c.ProduceHeaders(topic, partition, key, value, headers) // still TimestampMs=-1
c.ProduceTimestamp(topic, partition, key, value, timestampMs) // default acks, empty headers
```

```java
c.produce(topic, partition, key, value) // still one message, timestampMs=-1, default acks
c.produce(topic, partition, key, value, acks) // still timestampMs=-1
c.produce(topic, partition, key, value, List<Record.Header> headers) // still timestampMs=-1
c.produceTimestamp(topic, partition, key, value, timestampMs) // default acks, empty headers
```

Both new methods build a 1-element `ProduceMessage` with the given
timestamp and call the existing batch encode + retry path. Return
value is the same as today: first offset of the batch (`int64` /
`long`).

`-1` is still broker now. Acks and headers stay on their own
overloads / the batch path.

## Honesty leftovers

- Not Kafka Produce / RecordBatch. Native opcode **1** only.
- Python `timestamp_ms=` and Rust `Message.timestamp` are unchanged
  (already shipped).
- Convenience Produce is still **one message**. Batch timestamps stay
  `ProduceBatch` / `produce(..., messages)`.
- No Kafka API keys / opcodes / Phase 155.

## Tests

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Fake TCP stub that decodes Produce request message timestamp.

| Case | Expect |
|------|--------|
| Existing `Produce` / 4-arg `produce` | one message, timestamp **-1** |
| `ProduceTimestamp` / `produceTimestamp` | given timestamp on the wire |
| Existing produce retry / error 13 / error 21 | still pass |

## Merge notes

Sibling slices that also edit Go/Java `Client` Produce should keep
this hunk local to the convenience Produce path:

- **Keep `Produce` / `ProduceAcks` / `ProduceHeaders` timestamp -1.**
  Do not drop `ProduceBatch` / batch `produce(messages)`.
- Do not wrap JoinGroup, Heartbeat, or Produce headers/acks combo.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`Produce` / `ProduceAcks` /
  `ProduceHeaders`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`produce` 4-arg / headers / acks)
- Scripted-broker produce tests in `client_test.go` /
  `ClientTest.java`

The hunk is local to the convenience Produce path.

## Related

- [V130_SPEC.md](./V130_SPEC.md) — Go/Java Produce headers
- [V129_SPEC.md](./V129_SPEC.md) — language produce default acks
- [V68_SPEC.md](./V68_SPEC.md) — Go/Java ProduceBatch (timestamp
  already on `ProduceMessage`)
- [V61_SPEC.md](./V61_SPEC.md) — produce retry / error 13 / error 21
