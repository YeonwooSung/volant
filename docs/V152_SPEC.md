# v0.152 — language DeleteRecords default wait flag

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V52_SPEC.md](./V52_SPEC.md) /
[V129_SPEC.md](./V129_SPEC.md): Produce already has client-level
default acks, but language `DeleteRecords` / 3-arg `deleteRecords`
hardcode `wait_majority=0`. `DeleteRecordsWithWaitFlag` / 4-arg
already send an explicit flag. Python `delete_records(...,
wait_majority=0)` already has a kwarg defaulting to 0.

Add a client-level default without breaking an explicit flag.
Default remains **0** (broker default). **1** = force wait, **2** =
force no-wait (Phase 137).

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. Python: constructor `delete_records_wait: int = 0`, store
   `self.delete_records_wait`. Change
   `delete_records(..., wait_majority=None)` so `None` uses
   `self.delete_records_wait`. Existing
   `delete_records(..., wait_majority=2)` still wins. Default call
   `delete_records(topic, partition, before_offset)` stays
   wait_majority=0 unless `c.delete_records_wait` was changed.
2. Go: field `deleteRecordsWait uint8` default 0, set in every
   constructor that sets `acks`; `SetDeleteRecordsWait` /
   `DeleteRecordsWait()`. `DeleteRecords` calls
   `DeleteRecordsWithWaitFlag(..., c.deleteRecordsWait)` instead of
   `0`. `DeleteRecordsWithWaitFlag` stays explicit.
3. Java: field `deleteRecordsWait` default 0;
   `setDeleteRecordsWait` / `deleteRecordsWait()`. 3-arg
   `deleteRecords` uses the field. 4-arg stays explicit.
4. No new retry / redirect. Existing DeleteRecords retry (v0.65)
   and error 13 redirect stay as-is.
5. Do **not** change `DeleteRecordsWithWaitFlag` / explicit
   `wait_majority=` / 4-arg signatures.
6. Do **not** change Rust (sibling leftover if any). Do **not**
   open Phase 155 / add Kafka API keys / add opcodes / change
   broker (sibling **v0.155**).

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust `ClientConfig` wait default | Frozen; language clients only |
| Change `DeleteRecordsWithWaitFlag` / 4-arg / explicit `wait_majority=` | Frozen; explicit still wins |
| Kafka DeleteRecords (API key 21) | Native opcode 44 only |
| New retry / redirect | Existing loops unchanged |
| Broker / protocol / Rust client | Frozen (sibling **v0.155**) |
| Kafka API keys / new opcodes | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Codec encode/decode tests | Already shipped (v0.52) |

## Semantics

- Default remains **0** (broker default). Unchanged call sites still
  send wait_majority=0.
- After set (`c.delete_records_wait = 1` / `SetDeleteRecordsWait(1)`
  / `setDeleteRecordsWait(1)`), 3-arg / default-kwarg DeleteRecords
  encodes 1.
- Explicit `delete_records(..., wait_majority=2)` /
  `DeleteRecordsWithWaitFlag(..., 2)` / 4-arg `deleteRecords(..., 2)`
  still wins over a client default.
- wait_majority: 0 = broker default, 1 = force wait, 2 = force
  no-wait (Phase 137). Always written on the wire.

## API

```python
c = Client("127.0.0.1:9092")                   # delete_records_wait=0
c = Client("127.0.0.1:9092", delete_records_wait=1)
c.delete_records_wait = 1
c.delete_records("t", 0, 100)                  # uses c.delete_records_wait
c.delete_records("t", 0, 100, wait_majority=2) # explicit wins
```

```go
c.DeleteRecords(topic, partition, beforeOffset)            // uses c.DeleteRecordsWait()
c.SetDeleteRecordsWait(1)
c.DeleteRecordsWithWaitFlag(topic, partition, beforeOffset, 2) // explicit
```

```java
c.deleteRecords(topic, partition, beforeOffset);           // uses c.deleteRecordsWait()
c.setDeleteRecordsWait(1);
c.deleteRecords(topic, partition, beforeOffset, 2);        // explicit
```

## Tests

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_delete_records tests.test_client -q
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

Fake TCP stub that records decoded DeleteRecords `wait_majority`:

| Case | Expect |
|------|--------|
| Default DeleteRecords | wire wait_majority=0 |
| After set wait=1, 3-arg / default-kwarg DeleteRecords | wire wait_majority=1 |
| Explicit WithWaitFlag / 4-arg / `wait_majority=2` over a client default | wire wait_majority=2 |
| Existing DeleteRecords retry / 13 tests | still pass |

Do **not** append codec tests.

## Honesty leftovers

- Not Kafka DeleteRecords. Native opcode **44** only.
- Default stays **0**. No new retry / redirect.
- `DeleteRecordsWithWaitFlag` / 4-arg / explicit `wait_majority=`
  still require an explicit flag.
- Rust client wait default is frozen (sibling leftover if any).
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling slices that also edit Client constructors / DeleteRecords
should keep this hunk local to the wait default:

- **Keep `delete_records(..., wait_majority=None)` / `DeleteRecords`
  → `c.deleteRecordsWait` / 3-arg `deleteRecords` →
  `this.deleteRecordsWait`**. Do not hardcode 0 again.
- Do **not** change `DeleteRecordsWithWaitFlag` / 4-arg signatures.
- Do not change Rust, broker, or protocol.

Expect conflicts on:

- Client constructors (Python kwargs / Go `Client{}` / Java fields)
- Convenience DeleteRecords
- hunk is otherwise local to the wait default

## Related

- [V52_SPEC.md](./V52_SPEC.md) — DeleteRecords on Python / Go / Java
- [V65_SPEC.md](./V65_SPEC.md) — language DeleteRecords retry
- [V129_SPEC.md](./V129_SPEC.md) — language produce default acks
- [PHASE137_SPEC.md](./PHASE137_SPEC.md) — wait_majority trailer
