# v0.129 — language produce default acks

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V64_SPEC.md](./V64_SPEC.md): Rust
`ClientConfig.acks` (default 1) is the produce default, but language
clients hardcoded `acks=1` on the convenience Produce path (Python
`produce(..., acks=1)`, Go `Produce` → `ProduceAcks(..., 1)`, Java
`produce(topic, partition, key, value)`).

Add a client-level default without breaking explicit acks. Default
remains **1** (leader only). **255** = acks=all (ISR), same as today.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. Python: constructor `acks: int = 1`, store `self.acks`. Change
   `produce(..., acks=None)` so `None` uses `self.acks`. Existing
   `produce(..., acks=255)` still wins. Default call
   `produce(topic, partition, value=...)` stays acks=1 unless `c.acks`
   was changed.
2. Go: field `acks uint8` default 1; `SetAcks` / `Acks()`. `Produce`
   uses `c.acks` instead of hardcoded 1. `ProduceAcks` / `ProduceBatch`
   stay explicit.
3. Java: field `acks` default 1; `setAcks` / `acks()`. No-acks
   `produce(...)` uses `this.acks`. Overload with explicit acks
   unchanged.
4. No new retry / redirect. Existing produce retry (v0.61) and error
   13 redirect stay as-is.
5. Do **not** wrap CreateTopic, JoinGroup, OffsetCommit, or Produce
   headers (sibling v0.130).
6. Do **not** change Rust (`ClientConfig.acks` already exists;
   `produce()` uses it, `produce_with_acks` overrides).

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust `ClientConfig.acks` | Already shipped |
| Produce headers on the wire | Sibling v0.130 |
| Kafka Produce versions (API key 0) | Native opcode 1 only |
| New retry / redirect | Existing loops unchanged |
| Broker / protocol / Rust client | Frozen |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

- Default remains **1** (leader only). Unchanged call sites still send
  acks=1.
- After set (`c.acks = 255` / `SetAcks(255)` / `setAcks(255)`),
  convenience produce sends 255.
- Explicit `produce(..., acks=1)` / `ProduceAcks(..., 1)` / 5-arg
  `produce(..., 1)` still wins over a 255 default.
- `acks=255` is acks=all (ISR), same as v0.64 / Rust
  `produce_with_acks`.
- Go `ProduceBatch` and Java list `produce(..., messages, acks)` stay
  explicit (no implicit default).
- Python `messages=` batch uses `self.acks` when `acks=` is omitted
  (same method).

## API

```python
c = Client("127.0.0.1:9092")          # acks=1
c = Client("127.0.0.1:9092", acks=255)
c.acks = 255
c.produce("t", 0, value=b"hello")      # uses c.acks
c.produce("t", 0, value=b"hello", acks=1)  # explicit wins
```

```go
c.Produce(topic, partition, key, value)            // uses c.Acks()
c.SetAcks(255)
c.ProduceAcks(topic, partition, key, value, 1)     // explicit
c.ProduceBatch(topic, partition, msgs, 1)          // explicit
```

```java
c.produce(topic, partition, key, value);           // uses c.acks()
c.setAcks(255);
c.produce(topic, partition, key, value, 1);        // explicit
c.produce(topic, partition, messages, 1);          // explicit
```

## Tests

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest discover -s tests -q
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

Fake TCP stub that records decoded Produce `acks`:

| Case | Expect |
|------|--------|
| Default produce | wire acks=1 |
| After set acks=255, convenience produce | wire acks=255 |
| Explicit ProduceAcks / `produce(..., acks=1)` over a 255 default | wire acks=1 |
| Existing produce retry / 13 tests | still pass |

## Honesty leftovers

- Not Kafka Produce. Native opcode **1** only.
- Default stays **1**. No new retry / redirect.
- Go `ProduceBatch` / Java list `produce` still require explicit acks.
- Produce headers are a sibling residual (v0.130).
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling slices that also edit Client constructors / Produce should
keep this hunk local to the acks default:

- **Keep `produce(..., acks=None)` / `Produce` → `c.acks` /
  4-arg `produce` → `this.acks`**. Do not hardcode 1 again.
- Do **not** wrap CreateTopic, JoinGroup, OffsetCommit, or Produce
  headers (v0.130).
- Do not change Rust, broker, or protocol.

Expect conflicts on:

- Client constructors (Python kwargs / Go `Client{}` / Java fields)
- Convenience Produce
- hunk is otherwise local to acks default

## Related

- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Produce acks overloads
- [V68_SPEC.md](./V68_SPEC.md) — Go/Java ProduceBatch (explicit acks)
