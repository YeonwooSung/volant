# v0.130 — Go/Java convenience Produce headers

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover that Go `Produce` / `ProduceAcks` and
Java 4-arg `produce` always send a one-message batch with empty
headers. `ProduceBatch` / `produce(..., messages)` already carry
`ProduceMessage.headers`. Python already has `produce(..., headers=)`;
Rust already has `Message.headers`. Port a convenience onto **Go and
Java only**.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / Python / Rust client.

## Goals

1. One-message Produce with caller-supplied native record headers,
   without breaking existing signatures.
2. Go: `ProduceHeaders(topic, partition, key, value, headers)` with
   `acks=1`. `Produce` / `ProduceAcks` still send empty headers.
3. Java: `produce(topic, partition, key, value, List<Record.Header>)`
   calling the batch path. Existing 4-arg `produce` calls it with
   empty headers. 5-arg `produce(..., acks)` stays empty headers.
4. Reuse existing produce retry / error **13** / error **21**. Not
   Kafka RecordBatch header versions.
5. Do **not** wrap CreateTopic, JoinGroup, OffsetCommit, or default
   acks (siblings). Call existing `ProduceBatch` / `produce(messages)`.
6. Do **not** change broker / protocol. `ProduceMessage` already has
   `Headers`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka Produce / RecordBatch header versions | Native opcode 1 only |
| Python / Rust API changes | Python already has `headers=`; Rust already has `Message.headers` |
| ProduceAcks + headers / 6-arg Java | Use `ProduceBatch` / `produce(messages)` |
| CreateTopic / JoinGroup / OffsetCommit / default acks | Sibling residuals |
| Broker / protocol / codec | Wire already has headers |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Keep existing `Produce` / 4-arg `produce` and `ProduceAcks` / 5-arg
`produce(..., acks)` signatures. Additive convenience only.

```go
c.Produce(topic, partition, key, value) // still one message, empty headers, acks=1
c.ProduceAcks(topic, partition, key, value, acks) // still empty headers
c.ProduceHeaders(topic, partition, key, value, headers []codec.Header) // acks=1
```

```java
c.produce(topic, partition, key, value) // still one message, empty headers, acks=1
c.produce(topic, partition, key, value, acks) // still empty headers
c.produce(topic, partition, key, value, List<Record.Header> headers) // acks=1
```

Both new methods build a 1-element `ProduceMessage` with the given
headers and call the existing batch encode + retry path. Return value
is the same as today: first offset of the batch (`int64` / `long`).

`acks=255` is still `acks=all` on `ProduceAcks` / 5-arg `produce`.
Headers on acks≠1 stay on the batch path.

## Honesty leftovers

- Not Kafka Produce / RecordBatch. Native opcode **1** only.
- Python `headers=` and Rust `Message.headers` are unchanged
  (already shipped).
- Convenience Produce is still **one message**. Batch headers stay
  `ProduceBatch` / `produce(..., messages)`.
- No Kafka API keys / opcodes / Phase 155.

## Tests

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Fake TCP stub that decodes Produce request message headers.

| Case | Expect |
|------|--------|
| Existing `Produce` / 4-arg `produce` | one message, **zero** headers |
| `ProduceHeaders` / 5-arg `produce(..., headers)` | header key/value on the wire |
| Existing produce retry / error 13 / error 21 | still pass |

## Merge notes

Sibling slices that also edit Go/Java `Client` Produce should keep
this hunk local to the convenience Produce path:

- **Keep `Produce` / `ProduceAcks` empty-headers.** Do not drop
  `ProduceBatch` / batch `produce(messages)`.
- Do not wrap CreateTopic, JoinGroup, OffsetCommit, or default acks.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`Produce` / `ProduceAcks`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`produce` 4-arg / acks)
- Scripted-broker produce tests in `client_test.go` /
  `ClientTest.java`

The hunk is local to the convenience Produce path.

## Related

- [V68_SPEC.md](./V68_SPEC.md) — Go/Java ProduceBatch (headers already
  on `ProduceMessage`)
- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Produce acks
- [V61_SPEC.md](./V61_SPEC.md) — produce retry / error 13 / error 21
