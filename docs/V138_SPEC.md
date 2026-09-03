# v0.138 — Go/Java Produce timestamp + headers

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V132_SPEC.md](./V132_SPEC.md) /
[V130_SPEC.md](./V130_SPEC.md): Go `ProduceTimestamp` and Java
`produceTimestamp` send a caller timestamp with empty headers, while
`ProduceHeaders` / headers `produce` and `ProduceHeadersAcks` /
`produceHeadersAcks` send headers with `TimestampMs: -1`. There is no
one-message convenience with **both** timestamp and headers. Python
already has `produce(..., timestamp_ms=, headers=)`;
`ProduceBatch` / `produce(..., messages)` already carry both.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / Python / Rust client.

## Goals

1. One-message Produce with a caller-supplied native timestamp **and**
   native record headers, using the client default acks, without
   breaking existing signatures.
2. Go: `ProduceTimestampHeaders(topic, partition, key, value,
   timestampMs, headers)` via `ProduceBatch` with one
   `ProduceMessage`. `ProduceTimestamp` still sends empty headers.
   `ProduceHeaders` / `ProduceHeadersAcks` still send
   `TimestampMs: -1`.
3. Java: named `produceTimestampHeaders(...)` via the batch path. Do
   **not** add `produce(..., long, List)` (signature collision risk
   with existing overloads). `produceTimestamp` still sends empty
   headers. Headers `produce` / `produceHeadersAcks` still send `-1L`.
4. Reuse existing produce retry / error **13** / error **21**. Client
   default acks (`c.acks` / `this.acks`; 1 unless `SetAcks` /
   `setAcks`). Not Kafka RecordBatch versions.
5. Do **not** wrap JoinGroup, Heartbeat, or Produce headers+acks
   (already shipped). Call existing `ProduceBatch` / `produce(messages)`.
6. Do **not** change broker / protocol / Python / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka Produce / RecordBatch versions | Native opcode 1 only |
| Python / Rust API changes | Python already has `timestamp_ms=` + `headers=`; Rust already has `Message.timestamp` + `Message.headers` |
| Extra Java `produce(..., long, List)` overload | Named method avoids collision |
| Timestamp + explicit acks combo | Use `ProduceBatch` / `produce(messages)` |
| JoinGroup / Heartbeat wrap | Sibling residuals |
| Broker / protocol / codec | Wire already has timestamp + headers |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Keep existing `Produce` / 4-arg `produce`, `ProduceTimestamp` /
`produceTimestamp`, `ProduceHeaders` / headers `produce`, and
`ProduceHeadersAcks` / `produceHeadersAcks` signatures. Additive
named convenience only.

```go
c.ProduceTimestamp(topic, partition, key, value, timestampMs) // still empty headers
c.ProduceHeaders(topic, partition, key, value, headers)       // still TimestampMs=-1
c.ProduceHeadersAcks(topic, partition, key, value, headers, acks) // still TimestampMs=-1
c.ProduceTimestampHeaders(topic, partition, key, value, timestampMs, headers)
```

```java
c.produceTimestamp(topic, partition, key, value, timestampMs); // still empty headers
c.produce(topic, partition, key, value, headers);              // still timestampMs=-1
c.produceHeadersAcks(topic, partition, key, value, headers, acks); // still timestampMs=-1
c.produceTimestampHeaders(topic, partition, key, value, timestampMs, headers);
```

Both new methods build a 1-element `ProduceMessage` with the given
timestamp and headers and call the existing batch encode + retry path
with the client default acks. Return value is the same as today:
first offset of the batch (`int64` / `long`).

`-1` is still broker now. Explicit acks stay on `ProduceAcks` /
`ProduceHeadersAcks` / the batch path.

## Honesty leftovers

- Not Kafka Produce / RecordBatch. Native opcode **1** only.
- Python `timestamp_ms=` + `headers=` and Rust `Message.timestamp` +
  `Message.headers` are unchanged (already shipped).
- Convenience Produce is still **one message**. Batch timestamp +
  headers stay `ProduceBatch` / `produce(..., messages)`.
- No Kafka API keys / opcodes / Phase 155.

## Tests

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Fake TCP stub that decodes Produce request message timestamp **and**
headers.

| Case | Expect |
|------|--------|
| Existing `ProduceTimestamp` / `produceTimestamp` | given timestamp, empty headers, default acks |
| Existing `ProduceHeaders` / headers `produce` | headers, timestamp **-1**, default acks |
| Existing `ProduceHeadersAcks` / `produceHeadersAcks` | headers, timestamp **-1**, caller acks |
| `ProduceTimestampHeaders` / `produceTimestampHeaders` | given timestamp **and** headers, default acks (1) |
| Existing produce retry / error 13 / error 21 | still pass |

## Merge notes

Sibling slices that also edit Go/Java `Client` Produce should keep
this hunk local to the new named method:

- **Keep `ProduceTimestamp` empty headers.** Keep `ProduceHeaders` /
  `ProduceHeadersAcks` timestamp **-1**. Do not drop `ProduceBatch` /
  batch `produce(messages)`.
- Do not add a Java `produce(..., long, List)` overload.
- Do not wrap JoinGroup or Heartbeat.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`ProduceTimestamp` / `ProduceHeaders` /
  `ProduceHeadersAcks`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`produceTimestamp` / headers `produce` / `produceHeadersAcks`)
- Scripted-broker produce tests in `client_test.go` /
  `ClientTest.java`

The hunk is local to the new named method.

## Related

- [V133_SPEC.md](./V133_SPEC.md) — Go/Java Produce headers + acks
- [V132_SPEC.md](./V132_SPEC.md) — Go/Java Produce timestamp
- [V130_SPEC.md](./V130_SPEC.md) — Go/Java Produce headers
- [V129_SPEC.md](./V129_SPEC.md) — language produce default acks
- [V68_SPEC.md](./V68_SPEC.md) — Go/Java ProduceBatch
- [V61_SPEC.md](./V61_SPEC.md) — produce retry / error 13 / error 21
