# v0.133 — Go/Java Produce headers + acks

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V130_SPEC.md](./V130_SPEC.md): Go
`ProduceHeaders` and Java headers `produce` use the client default
acks, while `ProduceAcks` / 5-arg `produce(..., acks)` still send
empty headers. There is no one-message convenience with **both**
headers and explicit acks. `ProduceBatch` / `produce(..., messages)`
already carry both.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / Python / Rust client.

## Goals

1. One-message Produce with caller-supplied native record headers
   **and** an explicit acks byte, without breaking existing signatures.
2. Go: `ProduceHeadersAcks(topic, partition, key, value, headers, acks)`
   via `ProduceBatch` with one `ProduceMessage`. `ProduceHeaders`
   stays default acks.
3. Java: named `produceHeadersAcks(...)` via the batch path. Do
   **not** add another `produce(..., int)` overload (headers vs acks
   already collide on a 5th `int`/`List` argument). Headers
   `produce` still uses `this.acks`.
4. Reuse existing produce retry / error **13** / error **21**. 255 =
   acks=all. Not Kafka RecordBatch versions.
5. Do **not** wrap JoinGroup, Heartbeat, or Produce timestamp
   (siblings). Call existing `ProduceBatch` / `produce(messages)`.
6. Do **not** change broker / protocol / Python / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka Produce / RecordBatch versions | Native opcode 1 only |
| Python / Rust API changes | Python already has `produce(..., headers=, acks=)`; Rust already has `produce` + `produce_with_acks` with `Message.headers` |
| Extra Java `produce(..., int)` overload | Named method avoids collision |
| JoinGroup / Heartbeat / Produce timestamp | Sibling residuals |
| Broker / protocol / codec | Wire already has headers + acks |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Keep existing `ProduceHeaders` / headers `produce` and `ProduceAcks`
/ 5-arg `produce(..., acks)` signatures. Additive named convenience
only.

```go
c.ProduceHeaders(topic, partition, key, value, headers) // still client default acks
c.ProduceAcks(topic, partition, key, value, acks)       // still empty headers
c.ProduceHeadersAcks(topic, partition, key, value, headers, acks)
```

```java
c.produce(topic, partition, key, value, headers); // still this.acks
c.produce(topic, partition, key, value, acks);    // still empty headers
c.produceHeadersAcks(topic, partition, key, value, headers, acks);
```

Both new methods build a 1-element `ProduceMessage` with the given
headers and call the existing batch encode + retry path with the
caller acks. Return value is the same as today: first offset of the
batch (`int64` / `long`).

`acks=255` is still `acks=all`. Batch headers + acks stay on
`ProduceBatch` / `produce(..., messages)`.

## Honesty leftovers

- Not Kafka Produce / RecordBatch. Native opcode **1** only.
- Python `headers=` + `acks=` and Rust `Message.headers` +
  `produce_with_acks` are unchanged (already shipped).
- Convenience Produce is still **one message**. Batch stays
  `ProduceBatch` / `produce(..., messages)`.
- No Kafka API keys / opcodes / Phase 155.

## Tests

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Fake TCP stub that decodes Produce acks **and** headers.

| Case | Expect |
|------|--------|
| Existing `ProduceHeaders` / headers `produce` | default acks (1 unless set) + headers |
| `ProduceHeadersAcks` / `produceHeadersAcks` | headers **and** acks=255 |
| Existing produce retry / error 13 / error 21 | still pass |

## Merge notes

Sibling slices that also edit Go/Java `Client` Produce should keep
this hunk local to the new named method:

- **Keep `ProduceHeaders` / headers `produce` on default acks.** Do
  not drop `ProduceBatch` / batch `produce(messages)`.
- Do not wrap JoinGroup, Heartbeat, or Produce timestamp.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`ProduceHeaders` / `ProduceAcks`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (headers `produce` / acks overload)
- Scripted-broker produce tests in `client_test.go` /
  `ClientTest.java`

The hunk is local to the new named method.

## Related

- [V130_SPEC.md](./V130_SPEC.md) — Go/Java Produce headers
- [V129_SPEC.md](./V129_SPEC.md) — language produce default acks
- [V68_SPEC.md](./V68_SPEC.md) — Go/Java ProduceBatch
- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Produce acks
- [V61_SPEC.md](./V61_SPEC.md) — produce retry / error 13 / error 21
