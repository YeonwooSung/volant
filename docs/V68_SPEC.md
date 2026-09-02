# v0.68 — Go/Java ProduceBatch

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V64_SPEC.md](./V64_SPEC.md):
“Convenience Produce is still one message per RPC.” The wire already
encodes `messages: []ProduceMessage`. Python `Client.produce` already
accepts `messages=`. Port a batch produce API onto **Go and Java
only**.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / `volant-protocol` / Rust client.

## Goals

1. One Produce RPC with N messages (N >= 1). Same topic / partition /
   acks / idempotent trailer as today’s single produce.
2. Convenience `Produce` / 4-arg `produce` **stay one message**
   (`acks=1`). Do not change their signatures.
3. Reuse existing retry (v0.61 `max_retries`), redirect (error 13 /
   `max_redirects`), and unknown-pid re-Init (error 21). Failed batch
   does **not** increment the idempotent sequence. Success increments
   sequence by **batch length** (`noteProduceSuccess(..., count)`).
4. Empty batch → error (do not send opcode 1).
5. Idempotent trailer `base_sequence` is the first message’s seq; the
   broker treats the batch as consecutive seqs. Match Python.
6. Do **not** change Python except a one-line README note that Go/Java
   now match `messages=`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka Produce versions (API key 0) | Native opcode 1 only |
| Python / Rust API changes | Python already has `messages=`; Rust already batches |
| New native opcodes | Reuse Produce (1) |
| Broker / protocol / Rust client changes | Wire already has `messages[]` |
| Codec changes | `ProduceMessage` / `EncodeProduceRequest` already exist |
| Fetch retry / wrap fetch | Sibling v0.66; this slice is produce-only |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Keep existing 4-arg Produce / 5-arg ProduceAcks signatures. Additive
batch only.

```go
c.Produce(topic, partition, key, value) // still one message, acks=1
c.ProduceAcks(topic, partition, key, value, acks) // still one message
c.ProduceBatch(topic, partition, msgs []codec.ProduceMessage, acks uint8)
```

```java
c.produce(topic, partition, key, value) // still one message, acks=1
c.produce(topic, partition, key, value, acks) // still one message
c.produce(topic, partition, List<Codec.ProduceMessage> messages, acks)
```

`Produce` / 4-arg `produce` and `ProduceAcks` / 5-arg `produce` become
a 1-element batch on the shared encode + retry path. Return value is
the same as today: first offset of the batch (`int64` / `long`).

Empty / nil batch is a client error (`fmt.Errorf` /
`IllegalArgumentException`) and does not send opcode 1.

`acks=255` is `acks=all` (same as v0.64).

## Honesty leftovers

- Not Kafka Produce. Native opcode **1** only.
- Python `messages=` is unchanged (already shipped).
- No Kafka API keys / opcodes / Phase 155.

## Tests

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| ProduceBatch 3 messages, acks=1 | one Produce RPC; request has 3 messages; returns offset |
| Empty batch | error; zero Produce RPCs |
| Produce / 4-arg produce unchanged | still 1 message |
| max_retries=2, first batch 7 then ok | success; two Produce RPCs; same 3 messages both times |
| error 13 then Metadata leader then ok | redirect still works |
| Two idempotent batches of 3 then 2 | `base_sequence` 0 then 3 |

## Merge notes

Sibling **v0.66** (fetch retry) also edits `Client`. Keep hunks local
to produce encode + the new batch API. Do not wrap fetch.

When merging:

- **Keep produce/fetch redirect loops.** `ProduceAcks` / 5-arg
  `produce` call into the same loop as `ProduceBatch`.
- Do not change Python client code.
- Do not change the broker, Kafka shim, or Rust client in this merge.

See [V64_SPEC.md](./V64_SPEC.md) (Go/Java Fetch knobs and Produce
acks), [V61_SPEC.md](./V61_SPEC.md) (produce retry), and
[V47_SPEC.md](./V47_SPEC.md) (idempotent produce).
