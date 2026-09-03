# v0.142 — Go/Java Produce timestamp + headers + acks

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V138_SPEC.md](./V138_SPEC.md) /
[V133_SPEC.md](./V133_SPEC.md) / [V132_SPEC.md](./V132_SPEC.md): Go
`ProduceTimestampHeaders` and Java `produceTimestampHeaders` send a
caller timestamp and headers with the client default acks, while
`ProduceHeadersAcks` / `produceHeadersAcks` send headers with
explicit acks and `TimestampMs: -1`. There is no one-message
convenience with **timestamp, headers, and explicit acks** together.
Python already has `produce(..., timestamp_ms=, headers=, acks=)`;
`ProduceBatch` / `produce(..., messages)` already carry all three.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / Python / Rust client.

## Goals

1. One-message Produce with a caller-supplied native timestamp,
   native record headers, **and** an explicit acks byte, without
   breaking existing signatures.
2. Go: `ProduceTimestampHeadersAcks(topic, partition, key, value,
   timestampMs, headers, acks)` via `ProduceBatch` with one
   `ProduceMessage`. `ProduceTimestampHeaders` still uses the client
   default acks. `ProduceHeadersAcks` still sends `TimestampMs: -1`.
3. Java: named `produceTimestampHeadersAcks(...)` via the batch path.
   Do **not** add `produce(..., long, List, int)` (signature collision
   risk with existing overloads). `produceTimestampHeaders` still uses
   `this.acks`. `produceHeadersAcks` still sends `-1L`.
4. Reuse existing produce retry / error **13** / error **21**. 255 =
   acks=all. Not Kafka RecordBatch versions.
5. Do **not** wrap JoinGroup, Heartbeat, or other Produce combos
   (already shipped). Call existing `ProduceBatch` / `produce(messages)`.
6. Do **not** change broker / protocol / Python / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka Produce / RecordBatch versions | Native opcode 1 only |
| Python / Rust API changes | Python already has `timestamp_ms=` + `headers=` + `acks=`; Rust already has `Message.timestamp` + `Message.headers` + `produce_with_acks` |
| Extra Java `produce(..., long, List, int)` overload | Named method avoids collision |
| Extra timestamp + acks (no headers) combo | Use `ProduceBatch` / `produce(messages)` |
| JoinGroup / Heartbeat wrap | Sibling residuals |
| Broker / protocol / codec | Wire already has timestamp + headers + acks |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Keep existing `ProduceTimestampHeaders` / `produceTimestampHeaders`
and `ProduceHeadersAcks` / `produceHeadersAcks` signatures. Additive
named convenience only.

```go
c.ProduceTimestampHeaders(topic, partition, key, value, timestampMs, headers) // still client default acks
c.ProduceHeadersAcks(topic, partition, key, value, headers, acks)             // still TimestampMs=-1
c.ProduceTimestampHeadersAcks(topic, partition, key, value, timestampMs, headers, acks)
```

```java
c.produceTimestampHeaders(topic, partition, key, value, timestampMs, headers); // still this.acks
c.produceHeadersAcks(topic, partition, key, value, headers, acks);             // still timestampMs=-1
c.produceTimestampHeadersAcks(topic, partition, key, value, timestampMs, headers, acks);
```

Both new methods build a 1-element `ProduceMessage` with the given
timestamp and headers and call the existing batch encode + retry path
with the caller acks. Return value is the same as today: first offset
of the batch (`int64` / `long`).

`-1` is still broker now. `acks=255` is still `acks=all`.
`ProduceTimestampHeaders` / `produceTimestampHeaders` stay on the
client default acks.

## Honesty leftovers

- Not Kafka Produce / RecordBatch. Native opcode **1** only.
- Python `timestamp_ms=` + `headers=` + `acks=` and Rust
  `Message.timestamp` + `Message.headers` + `produce_with_acks` are
  unchanged (already shipped).
- Convenience Produce is still **one message**. Batch timestamp +
  headers + acks stay `ProduceBatch` / `produce(..., messages)`.
- No Kafka API keys / opcodes / Phase 155.

## Tests

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Fake TCP stub that decodes Produce request message timestamp,
headers, **and** acks.

| Case | Expect |
|------|--------|
| Existing `ProduceTimestampHeaders` / `produceTimestampHeaders` | given timestamp **and** headers, default acks (1) |
| Existing `ProduceHeadersAcks` / `produceHeadersAcks` | headers, timestamp **-1**, caller acks |
| `ProduceTimestampHeadersAcks` / `produceTimestampHeadersAcks` | given timestamp, header `h=hv`, **and** acks=255 |
| Existing produce retry / error 13 / error 21 | still pass |

## Merge notes

Sibling slices that also edit Go/Java `Client` Produce should keep
this hunk local to the new named method:

- **Keep `ProduceTimestampHeaders` on default acks.** Keep
  `ProduceHeadersAcks` timestamp **-1**. Do not drop `ProduceBatch` /
  batch `produce(messages)`.
- Do not add a Java `produce(..., long, List, int)` overload.
- Do not wrap JoinGroup or Heartbeat.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`ProduceTimestampHeaders` /
  `ProduceHeadersAcks`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`produceTimestampHeaders` / `produceHeadersAcks`)
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
- [V61_SPEC.md](./V61_SPEC.md) — produce retry / error 13 / error 21
