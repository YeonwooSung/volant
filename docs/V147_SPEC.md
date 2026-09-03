# v0.147 — Go/Java ProduceBatch default acks

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V129_SPEC.md](./V129_SPEC.md) /
[V68_SPEC.md](./V68_SPEC.md): one-message `Produce` / 4-arg `produce`
use the client default acks (`c.acks` / `this.acks`). `ProduceBatch` /
`produce(topic, partition, messages, acks)` still **require** explicit
acks. Python `produce(..., messages=)` already defaults `acks` to
`self.acks`.

Add a batch-default path that uses the client acks without changing
the explicit-acks signatures. Default remains **1** (leader only).
**255** = acks=all (ISR), same as today.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, Python, or Rust client.

## Goals

1. Go: `ProduceBatchDefault(topic, partition, msgs)` calls
   `ProduceBatch(..., c.acks)`. `ProduceBatch` still requires explicit
   acks.
2. Java: 3-arg `produce(topic, partition, List<ProduceMessage>)` calls
   `produce(topic, partition, messages, this.acks)`. The 4-arg list
   overload still requires explicit acks. No named `produceBatch`
   (the 3-arg list slot was free).
3. No new retry / redirect. Existing produce retry (v0.61) and error
   13 redirect stay as-is.
4. Do **not** change `ProduceBatch` / 4-arg list
   `produce(..., messages, acks)` signatures.
5. Do **not** change Python (`messages=` already uses `self.acks`)
   or Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Python `messages=` default acks | Already uses `self.acks` |
| Change `ProduceBatch` / 4-arg list signatures | Frozen; still explicit acks |
| Extra Java `produceBatch` name | 3-arg list `produce` was free |
| Kafka Produce versions (API key 0) | Native opcode 1 only |
| New retry / redirect | Existing loops unchanged |
| Broker / protocol / Python / Rust | Frozen |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

- Default remains **1** (leader only). Unchanged call sites still send
  acks=1.
- After set (`SetAcks(255)` / `setAcks(255)`), batch-default encode
  acks **255** and N messages.
- Explicit `ProduceBatch(..., 1)` / 4-arg list `produce(..., 1)` still
  wins over a 255 default.
- `acks=255` is acks=all (ISR), same as v0.64 / v0.129.
- `Produce` / 4-arg `produce` stay one message.

## API

```go
c.SetAcks(255)
c.ProduceBatchDefault(topic, partition, msgs) // uses c.acks
c.ProduceBatch(topic, partition, msgs, 1)     // explicit; ignores SetAcks
```

```java
c.setAcks(255);
c.produce(topic, partition, messages);        // uses this.acks
c.produce(topic, partition, messages, 1);     // explicit; ignores setAcks
```

`ProduceBatchDefault` / 3-arg list `produce` call the existing batch
encode + retry path with the client default acks. Return value is the
same as today: first offset of the batch (`int64` / `long`).

Empty / nil batch is still a client error and does not send opcode 1.

## Tests

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Fake TCP stub that records decoded Produce `acks` and message count.

| Case | Expect |
|------|--------|
| After `SetAcks(255)` / `setAcks(255)`, batch-default | wire acks=255 and N messages |
| Explicit `ProduceBatch(..., 1)` / 4-arg list produce over a 255 default | wire acks=1 |
| Existing produce retry / 13 / 21 tests | still pass |

Do **not** append codec tests.

## Honesty leftovers

- Not Kafka Produce. Native opcode **1** only.
- Default stays **1**. No new retry / redirect.
- `ProduceBatch` / 4-arg list `produce` still require explicit acks.
- Python `messages=` already defaults acks (unchanged).
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling slices that also edit Go/Java `Client` Produce should keep
this hunk local to the batch-default wrapper:

- **Keep `ProduceBatch` / 4-arg list `produce` explicit.** Do not
  change their signatures.
- Do not add a colliding Java `produce(String, int, List)` if one
  appears later; use named `produceBatch` only if the 3-arg slot is
  taken.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`ProduceBatch` / `SetAcks`)
- Java `clients/java/src/main/java/io/volant/Client.java` (list
  `produce`)
- Scripted-broker produce tests in `client_test.go` /
  `ClientTest.java`

The hunk is local to the default-acks batch wrapper.

## Related

- [V129_SPEC.md](./V129_SPEC.md) — language produce default acks
- [V68_SPEC.md](./V68_SPEC.md) — Go/Java ProduceBatch (explicit acks)
- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Produce acks overloads
- [V61_SPEC.md](./V61_SPEC.md) — produce retry / error 13 / error 21
