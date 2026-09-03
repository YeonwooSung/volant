# v0.143 — language Fetch client-level default knobs

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V64_SPEC.md](./V64_SPEC.md) /
[V129_SPEC.md](./V129_SPEC.md): Produce already has client-level
default acks (`SetAcks` / `self.acks`), but language `Fetch` / 3-arg
`fetch` still hardcodes `max_messages=128`, `max_bytes=4MiB`,
`max_wait_ms=0`. FetchOpts / 6-arg fetch / Python kwargs already send
explicit knobs.

Add client-level Fetch defaults without breaking explicit knobs.
Constructor defaults stay **128 / 4MiB / 0**. Setter `0` stays `0`
(wire-legal; no clamp) so tests can send 0.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. Python: constructor `fetch_max_messages=128`,
   `fetch_max_bytes=4*1024*1024`, `fetch_max_wait_ms=0`, stored on
   `self`. Change `fetch(..., max_messages=None, max_bytes=None,
   max_wait_ms=None)` so `None` uses `self.fetch_*`. Existing
   `fetch(..., max_messages=10)` still wins. Default call
   `fetch(topic, partition, offset=...)` stays 128 / 4MiB / 0 unless
   `c.fetch_max_*` was changed.
2. Go: fields `fetchMaxMessages` / `fetchMaxBytes` / `fetchMaxWaitMs`
   default 128 / 4MiB / 0; `SetFetchMaxMessages` /
   `SetFetchMaxBytes` / `SetFetchMaxWaitMs` and matching getters.
   `Fetch()` calls `FetchOpts(..., c.fetchMaxMessages,
   c.fetchMaxBytes, c.fetchMaxWaitMs)`. `FetchOpts` stays explicit.
3. Java: same fields; `setFetchMaxMessages` / `setFetchMaxBytes` /
   `setFetchMaxWaitMs` / getters. 3-arg `fetch` uses the fields.
   6-arg `fetch` unchanged.
4. No new retry / redirect. Existing fetch retry (v0.61) and error
   13 redirect stay as-is.
5. Do **not** change GroupConsumer poll knobs (those stay 100 / 4MiB
   historical, v0.75).
6. Do **not** change FetchOpts / 6-arg fetch signatures (explicit
   knobs still win).
7. Do **not** change Rust (sibling **v0.144**).

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust `ClientConfig` fetch knobs | Sibling v0.144 |
| GroupConsumer poll knobs | Frozen at 100 / 4MiB (v0.75) |
| FetchOpts / 6-arg fetch signatures | Explicit knobs still win |
| Kafka Fetch versions (API key 1) | Native opcode 2 only |
| New retry / redirect | Existing loops unchanged |
| Broker / protocol / Rust client | Frozen |
| Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

- Defaults remain **128 / 4MiB / 0**. Unchanged call sites still send
  those knobs.
- After set (`c.fetch_max_messages = 10` / `SetFetchMaxMessages(10)`
  / `setFetchMaxMessages(10)`), 3-arg Fetch encodes those knobs.
- Explicit `fetch(..., max_messages=20)` / `FetchOpts(..., 20, ...)`
  / 6-arg `fetch(..., 20, ...)` still wins over a client default.
- Setter `0` stays `0` (no clamp). Wire-legal; tests can send 0.
- GroupConsumer poll still uses its own 100 / 4MiB knobs.

## API

```python
c = Client("127.0.0.1:9092")                    # 128 / 4MiB / 0
c = Client("127.0.0.1:9092", fetch_max_messages=10)
c.fetch_max_messages = 10
c.fetch_max_bytes = 4096
c.fetch_max_wait_ms = 100
c.fetch("t", 0, offset=0)                       # uses c.fetch_max_*
c.fetch("t", 0, offset=0, max_messages=20)      # explicit wins
```

```go
c.Fetch(topic, partition, offset)                          // uses c.FetchMax*
c.SetFetchMaxMessages(10)
c.SetFetchMaxBytes(4096)
c.SetFetchMaxWaitMs(100)
c.FetchOpts(topic, partition, offset, 20, 8192, 50)        // explicit
```

```java
c.fetch(topic, partition, offset);                         // uses c.fetchMax*
c.setFetchMaxMessages(10);
c.setFetchMaxBytes(4096);
c.setFetchMaxWaitMs(100);
c.fetch(topic, partition, offset, 20, 8192, 50);           // explicit
```

## Tests

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_client -q
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

Fake TCP stub that records decoded Fetch knobs:

| Case | Expect |
|------|--------|
| Default Fetch | wire 128 / 4MiB / 0 |
| After set 10 / 4096 / 100, 3-arg Fetch | wire 10 / 4096 / 100 |
| Explicit FetchOpts / 6-arg / kwargs over a client default | wire explicit values |
| Existing FetchOpts / 6-arg / default Fetch tests | still pass |

Do **not** append codec tests.

## Honesty leftovers

- Not Kafka Fetch. Native opcode **2** only.
- Defaults stay **128 / 4MiB / 0**. No new retry / redirect.
- GroupConsumer poll knobs stay 100 / 4MiB (v0.75).
- FetchOpts / 6-arg / explicit kwargs still require explicit knobs.
- Rust client fetch defaults are a sibling residual (v0.144).
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling slices that also edit Client constructors / Fetch should
keep this hunk local to the Fetch defaults:

- **Keep `fetch(..., max_*=None)` / `Fetch` → `c.fetchMax*` /
  3-arg `fetch` → `this.fetchMax*`**. Do not hardcode 128 / 4MiB / 0
  again.
- Do **not** wrap GroupConsumer poll knobs (v0.75).
- Do not change Rust, broker, or protocol.

Expect conflicts on:

- Client constructors (Python kwargs / Go `Client{}` / Java fields)
- Convenience Fetch
- hunk is otherwise local to Fetch defaults

## Related

- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Fetch knobs and Produce acks
- [V75_SPEC.md](./V75_SPEC.md) — GroupConsumer poll fetch size (100 / 4MiB)
- [V129_SPEC.md](./V129_SPEC.md) — language produce default acks
