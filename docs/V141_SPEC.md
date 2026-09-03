# v0.141 — Go/Java Produce timestamp + acks

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V132_SPEC.md](./V132_SPEC.md) /
[V64_SPEC.md](./V64_SPEC.md): Go `ProduceTimestamp` and Java
`produceTimestamp` send a caller timestamp with the client default
acks and empty headers, while `ProduceAcks` / 5-arg
`produce(..., acks)` send explicit acks with `TimestampMs: -1`. There
is no one-message convenience with **both** caller timestamp and
explicit acks. Python already has `produce(..., timestamp_ms=, acks=)`
with empty headers; `ProduceBatch` / `produce(..., messages)` already
carry both.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / Python / Rust client.

## Goals

1. One-message Produce with a caller-supplied native timestamp **and**
   an explicit acks byte, empty headers, without breaking existing
   signatures.
2. Go: `ProduceTimestampAcks(topic, partition, key, value,
   timestampMs, acks)` via `ProduceBatch` with one
   `ProduceMessage{{Key, Value, TimestampMs: timestampMs}}`.
   `ProduceTimestamp` still uses `c.acks`. `ProduceAcks` still sends
   `TimestampMs: -1`.
3. Java: named `produceTimestampAcks(...)` via the batch path. Do
   **not** add `produce(..., long, int)` (signature collision with
   existing overloads). `produceTimestamp` still uses `this.acks`.
   Acks `produce` still sends `-1L`.
4. Reuse existing produce retry / error **13** / error **21**. 255 =
   acks=all. `-1` is still broker timestamp. Not Kafka RecordBatch
   versions.
5. Do **not** wrap JoinGroup, Heartbeat, or Produce headers+timestamp
   (already shipped). Call existing `ProduceBatch` / `produce(messages)`.
6. Do **not** change broker / protocol / Python / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka Produce / RecordBatch versions | Native opcode 1 only |
| Python / Rust API changes | Python already has `timestamp_ms=` + `acks=`; Rust already has `Message.timestamp` + `produce_with_acks` |
| Extra Java `produce(..., long, int)` overload | Named method avoids collision |
| Timestamp + headers + acks combo | Use `ProduceBatch` / `produce(messages)` |
| JoinGroup / Heartbeat wrap | Sibling residuals |
| Broker / protocol / codec | Wire already has timestamp + acks |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Keep existing `Produce` / 4-arg `produce`, `ProduceTimestamp` /
`produceTimestamp`, `ProduceAcks` / 5-arg `produce(..., acks)`,
`ProduceHeaders` / headers `produce`, `ProduceHeadersAcks` /
`produceHeadersAcks`, and `ProduceTimestampHeaders` /
`produceTimestampHeaders` signatures. Additive named convenience
only.

```go
c.ProduceTimestamp(topic, partition, key, value, timestampMs) // still client default acks, empty headers
c.ProduceAcks(topic, partition, key, value, acks)             // still TimestampMs=-1
c.ProduceTimestampAcks(topic, partition, key, value, timestampMs, acks)
```

```java
c.produceTimestamp(topic, partition, key, value, timestampMs); // still this.acks, empty headers
c.produce(topic, partition, key, value, acks);                 // still timestampMs=-1
c.produceTimestampAcks(topic, partition, key, value, timestampMs, acks);
```

Both new methods build a 1-element `ProduceMessage` with the given
timestamp and empty headers and call the existing batch encode +
retry path with the caller acks. Return value is the same as today:
first offset of the batch (`int64` / `long`).

`acks=255` is still `acks=all`. `-1` is still broker now. Batch
timestamp + acks stay on `ProduceBatch` / `produce(..., messages)`.

## Honesty leftovers

- Not Kafka Produce / RecordBatch. Native opcode **1** only.
- Python `timestamp_ms=` + `acks=` and Rust `Message.timestamp` +
  `produce_with_acks` are unchanged (already shipped).
- Convenience Produce is still **one message**. Batch stays
  `ProduceBatch` / `produce(..., messages)`.
- No Kafka API keys / opcodes / Phase 155.

## Tests

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Fake TCP stub that decodes Produce request message timestamp **and**
acks.

| Case | Expect |
|------|--------|
| Existing `ProduceTimestamp` / `produceTimestamp` | given timestamp, empty headers, default acks |
| Existing `ProduceAcks` / 5-arg `produce` | acks, timestamp **-1**, empty headers |
| `ProduceTimestampAcks` / `produceTimestampAcks` | timestamp `1_700_000_000_000` **and** acks=255, empty headers |
| Existing produce retry / error 13 / error 21 | still pass |

## Merge notes

Sibling slices that also edit Go/Java `Client` Produce should keep
this hunk local to the new named method:

- **Keep `ProduceTimestamp` on default acks.** Keep `ProduceAcks`
  timestamp **-1**. Do not drop `ProduceBatch` / batch
  `produce(messages)`.
- Do not add a Java `produce(..., long, int)` overload.
- Do not wrap JoinGroup or Heartbeat.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`ProduceTimestamp` / `ProduceAcks`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`produceTimestamp` / acks `produce`)
- Scripted-broker produce tests in `client_test.go` /
  `ClientTest.java`

The hunk is local to the new named method.

## Related

- [V138_SPEC.md](./V138_SPEC.md) — Go/Java Produce timestamp + headers
- [V133_SPEC.md](./V133_SPEC.md) — Go/Java Produce headers + acks
- [V132_SPEC.md](./V132_SPEC.md) — Go/Java Produce timestamp
- [V130_SPEC.md](./V130_SPEC.md) — Go/Java Produce headers
- [V129_SPEC.md](./V129_SPEC.md) — language produce default acks
- [V68_SPEC.md](./V68_SPEC.md) — Go/Java ProduceBatch
- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Produce acks
- [V61_SPEC.md](./V61_SPEC.md) — produce retry / error 13 / error 21
