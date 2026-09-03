# v0.150 — language public InitProducerId

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V47_SPEC.md](./V47_SPEC.md) /
[V57_SPEC.md](./V57_SPEC.md) / [V101_SPEC.md](./V101_SPEC.md):
InitProducerId (native opcode 32/33) already exists and is retried
(v0.101). It is only called implicitly from produce / BeginTxn via
`_ensure_producer_id` / `ensureProducerID` / `ensureProducerId`.
Callers cannot pre-allocate a pid.

Expose the existing helper as a public no-arg method. If already
initialized, it is a no-op (same as the helper). Returns the
pid/epoch. Do **not** reimplement the retry loop; wrap the existing
private helper. Do **not** change implicit produce / BeginTxn Init.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, or Rust `volant-client`.

## Goals

1. **Python:** public `init_producer_id(self) -> tuple[int, int]`.
   Call `_ensure_producer_id()` then return
   `(self._producer_id, self._producer_epoch)`.
2. **Go:** public
   `InitProducerID() (producerID uint64, epoch uint16, err error)`.
   Call `c.ensureProducerID()` then return
   `c.producerID, c.producerEpoch`.
3. **Java:** public `long initProducerId()` returns the stored
   producer id. Add `producerId()` / `producerEpoch()` getters
   (fields were private with no getter). Do not invent a new public
   class.
4. Second call is a no-op (already ready), same as the helper.
5. Produce / BeginTxn still init implicitly. No extra Init from this
   slice.
6. Do **not** reimplement the retry loop. Wrap the existing helper
   so v0.101 transient retries still apply.
7. Do **not** change Rust (sibling leftover if any).

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust `ensure_producer_id` public | Frozen; language clients only |
| Change implicit produce / BeginTxn Init | Frozen |
| Reimplement Init retry | Wrap the existing helper (v0.101) |
| Kafka InitProducerId (API key 22) | Native opcode 32 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Codec encode/decode tests | Already shipped (v0.47) |

## API

```python
pid, epoch = c.init_producer_id()  # (producer_id, epoch)
pid, epoch = c.init_producer_id()  # no-op; same values
```

```go
pid, epoch, err := c.InitProducerID()
pid, epoch, err = c.InitProducerID() // no-op; same values
```

```java
long pid = c.initProducerId();
int epoch = c.producerEpoch();
pid = c.initProducerId(); // no-op; same values
```

Existing produce / BeginTxn signatures are unchanged. They still
call the private helper on first use.

## Semantics

- First public call sends native opcode **32** and stores pid/epoch.
- Second call does **not** send another Init (already ready).
- Produce with `enable_idempotence` / `EnableIdempotence` /
  `setEnableIdempotence` still inits once (no extra Init).
- BeginTxn still inits implicitly when the pid is not ready.
- Transient 6 / 7 / 15 / 16 and transport retry via the existing
  helper (v0.101; default `max_retries=0`).
- Error 21 on Init itself is still raised immediately.
- Not Kafka InitProducerId versions.

## Tests

Fake TCP stub that records Init opcode / count (same scripted
brokers as v0.47 / v0.101):

| Case | Expect |
|------|--------|
| First `init_producer_id` / `InitProducerID` / `initProducerId` | opcode 32; stored pid/epoch (42 / 1) |
| Second call | no extra Init; same pid/epoch |
| Produce with idempotence (no public Init) | still one Init (no extra from this slice) |

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_client -q
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

Do **not** append codec tests.

## Honesty leftovers

- **Not Kafka** InitProducerId (API key 22). Native opcode **32**
  only.
- Rust `ensure_producer_id` stays private.
- Implicit produce / BeginTxn Init is unchanged.
- Default `max_retries=0` (v0.101) is unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit language `Client` should keep this
hunk local to the public wrapper:

- **Keep the public method as a wrap of the existing helper.** Do
  not copy the retry loop.
- Do not change produce / BeginTxn Init.
- Do not change Rust, broker, or protocol.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`_ensure_producer_id`)
- Go `clients/go/client.go` (`ensureProducerID`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`ensureProducerId`)
- Scripted brokers in `test_client.py` / `client_test.go` /
  `ClientTest.java`

The hunk is local to the public wrapper + three fake-TCP tests.

## Related

- [V47_SPEC.md](./V47_SPEC.md) — idempotent produce / InitProducerId
- [V57_SPEC.md](./V57_SPEC.md) — BeginTxn / EndTxn on language clients
- [V101_SPEC.md](./V101_SPEC.md) — InitProducerId retry
- [V102_SPEC.md](./V102_SPEC.md) — Rust InitProducerId retry
