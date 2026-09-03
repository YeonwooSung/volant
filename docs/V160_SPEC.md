# v0.160 — Go/Python/Rust producer id getters

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V150_SPEC.md](./V150_SPEC.md) /
[V151_SPEC.md](./V151_SPEC.md): Java already has `producerId()` /
`producerEpoch()` getters that do **not** force Init. Go
`InitProducerID()` returns values but has no getters. Python
`init_producer_id()` returns a tuple but has no properties. Rust
`init_producer_id().await` returns a tuple; no getters that skip Init.

Expose stored pid/epoch without calling Init. Uninitialized is 0 / 0.
Do **not** change Init / produce implicit init.

This is residual **v0.160**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, or Java
(already has getters).

## Goals

1. **Go:** public `ProducerID() uint64` and `ProducerEpoch() uint16`.
   Return stored fields (`c.producerID` / `c.producerEpoch`). 0 until
   init. Do **not** call `ensureProducerID`.
2. **Python:** public `@property producer_id` / `producer_epoch`.
   Return `self._producer_id` / `self._producer_epoch`. 0 until init.
   Do **not** call `_ensure_producer_id`.
3. **Rust:** public `async fn producer_id(&self) -> u64` and
   `async fn producer_epoch(&self) -> u16`. Read the `idempotent`
   lock. Do **not** call `ensure_producer_id`. 0 until initialized.
4. Java already covered (v0.150). Do not change Java.
5. Do **not** change Init / produce implicit init.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change Init / produce implicit init | Frozen |
| Force Init from getters | Frozen; uninitialized is 0 / 0 |
| Java getters | Already shipped (v0.150) |
| Kafka InitProducerId (API key 22) | Native opcode 32 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

```go
func (c *Client) ProducerID() uint64
func (c *Client) ProducerEpoch() uint16
```

```python
@property
def producer_id(self) -> int: ...
@property
def producer_epoch(self) -> int: ...
```

```rust
pub async fn producer_id(&self) -> u64
pub async fn producer_epoch(&self) -> u16
```

```go
_ = c.ProducerID()   // 0 until Init
_ = c.ProducerEpoch()
pid, epoch, err := c.InitProducerID()
_ = c.ProducerID()   // matches pid
_ = c.ProducerEpoch()
```

```python
_ = c.producer_id    # 0 until Init
_ = c.producer_epoch
pid, epoch = c.init_producer_id()
_ = c.producer_id    # matches pid
_ = c.producer_epoch
```

```rust
assert_eq!(c.producer_id().await, 0);     // no opcode 32
assert_eq!(c.producer_epoch().await, 0);
let (pid, epoch) = c.init_producer_id().await?;
assert_eq!(c.producer_id().await, pid);
assert_eq!(c.producer_epoch().await, epoch);
```

Existing Init / produce / BeginTxn signatures are unchanged.

## Semantics

- Getters read stored fields only. They do **not** send native
  opcode **32**.
- Uninitialized (no public Init, no implicit produce / BeginTxn
  Init) returns **0 / 0**.
- After `InitProducerID` / `init_producer_id` /
  `init_producer_id().await`, getters match the stored pid/epoch.
- Produce / BeginTxn still init implicitly. Getters do not add an
  extra Init.
- Not Kafka InitProducerId versions.

## Tests

Fake TCP stub that records Init opcode / count (same scripted
brokers as v0.150 / v0.151):

| Case | Expect |
|------|--------|
| Getters before any Init | 0 / 0; no opcode 32 |
| After `InitProducerID` / `init_producer_id` / `init_producer_id().await` | getters match stored pid/epoch |

```bash
cd clients/go && go test ./...
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_client -q
cargo test -p volant-client -- --test-threads=1
```

Do **not** change Java. Do **not** append codec tests.

## Honesty leftovers

- **Not Kafka** InitProducerId (API key 22). Native opcode **32**
  only.
- Getters never call Init. Uninitialized is 0 / 0.
- Implicit produce / BeginTxn Init is unchanged.
- Java getters already exist (v0.150).
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit language / Rust `Client` should keep
this hunk local to the getters:

- **Keep getters as a read of stored fields.** Do not call Init.
- Do not change produce / BeginTxn Init.
- Do not change Java, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`InitProducerID`)
- Python `clients/python/src/volant/client.py` (`init_producer_id`)
- Rust `crates/volant-client/src/client.rs` (`init_producer_id`)
- Scripted brokers in `client_test.go` / `test_client.py`

The hunk is local to the getters + fake-TCP tests.

## Related

- [V150_SPEC.md](./V150_SPEC.md) — language public InitProducerId
  (Java getters)
- [V151_SPEC.md](./V151_SPEC.md) — Rust public InitProducerId
- [V47_SPEC.md](./V47_SPEC.md) — idempotent produce / InitProducerId
- [V101_SPEC.md](./V101_SPEC.md) — language InitProducerId retry
- [V102_SPEC.md](./V102_SPEC.md) — Rust InitProducerId retry
