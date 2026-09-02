# v0.64 — Go/Java Fetch knobs and Produce acks

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close two leftovers: Go `Fetch` / Java `fetch` hid
`max_messages` / `max_bytes` / `max_wait_ms`, and convenience Produce
hardcoded `acks=1`. Python already exposes both.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / `volant-protocol` / Rust client.

## Goals

1. **Public Fetch knobs** on Go and Java, matching Python
   `fetch(..., max_messages=, max_bytes=, max_wait_ms=)`. Existing
   3-arg `Fetch` / `fetch` keep defaults **128 / 4MiB / 0**.
2. **Public Produce acks** on Go and Java, matching Python
   `produce(..., acks=)`. Existing 4-arg `Produce` / `produce` stay
   `acks=1`. `acks=255` is `acks=all` (same as Rust / existing
   Python). Do not invent other acks values.
3. Existing produce/fetch **redirect loops** stay. Overloads call
   into the same loop with extra parameters.
4. Unit tests without a broker: scripted TCP broker sees the knobs
   and acks on the wire. Existing 3-arg Fetch / 4-arg Produce
   defaults still hold. Redirect still works on both paths.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka Fetch / Produce versions (API keys 1 / 0) | Native opcodes 1/2 only |
| ProduceBatch on Go/Java | Convenience Produce is still one message per RPC (batch stays Python `messages=`) |
| New native opcodes | Reuse Fetch (2) / Produce (1) |
| Broker / protocol / Rust client changes | FetchRequest already has the fields; Rust already has `produce_with_acks` |
| Codec changes | Public types already have `max_messages` / `max_bytes` / `max_wait_ms` / `acks` |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Keep existing 3-arg Fetch / 4-arg Produce signatures. Additive
overloads only.

```go
c.Fetch(topic, partition, offset) // unchanged defaults 128 / 4MiB / 0
c.FetchOpts(topic, partition, offset, maxMessages, maxBytes, maxWaitMs)
c.Produce(topic, partition, key, value) // still acks=1
c.ProduceAcks(topic, partition, key, value, acks uint8)
```

```java
c.fetch(topic, partition, offset) // unchanged
c.fetch(topic, partition, offset, maxMessages, maxBytes, maxWaitMs) // public
// existing package-private fetch(..., maxMessages, maxWaitMs) stays
c.produce(topic, partition, key, value) // still acks=1
c.produce(topic, partition, key, value, int acks)
```

Python is already complete (`fetch(..., max_messages=, max_bytes=,
max_wait_ms=)` and `produce(..., acks=)`). Do not break
`produce(..., acks=)`.

`acks=255` is `acks=all` (leader waits for ISR / HWM, same as Rust
`produce_with_acks` and existing Python).

## Honesty leftovers

- Not Kafka Fetch/Produce versions. Native opcodes **1/2** only.
- Go/Java convenience Produce is still **one message per RPC**
  (batch stays Python `messages=` / this slice does not add
  ProduceBatch).
- No Kafka API keys / opcodes / Phase 155.

## Tests

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
```

| Case | Expect |
|------|--------|
| Go `FetchOpts` / Java public 6-arg `fetch` | scripted broker sees `max_messages=10`, `max_bytes=4096`, `max_wait_ms=100` |
| Existing 3-arg Fetch | still sends 128 / 4MiB / 0 |
| Go `ProduceAcks` / Java 5-arg `produce` | wire `acks=255` |
| Existing Produce | still `acks=1` |
| Redirect | still works on both default and knob/acks paths |

## Merge notes

Siblings **v0.61** (produce retry) and **v0.65** (DeleteRecords
redirect) also edit `Client`. Keep existing produce/fetch redirect
loops. Add overloads that call into the same loop with extra
parameters.
