# v0.61 — produce retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V47_SPEC.md](./V47_SPEC.md): “No
produce retry beyond redirect + one unknown-pid re-Init. Rust
`max_retries` / backoff is not ported.”

Port Rust `ClientConfig.max_retries` + `retry_backoff_ms` onto
Python / Go / Java `Client.produce` only. Fetch is **not** retried.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client`.

## Goals

1. Extra produce attempts after the first on **transient** errors only.
   Budget is independent of `max_redirects` and of the one unknown-pid
   re-Init.
2. **`max_retries` default 0** — existing tests stay valid (no extra
   produce attempts). Rust default is also 0.
3. **`retry_backoff_ms` default 50**. Tests may set 0. Sleep between
   retry attempts (Python `time.sleep`, Go `time.Sleep`, Java
   `Thread.sleep`).
4. Failed produce (including retries that eventually fail) does **not**
   increment the idempotent sequence. The retry reuses the same trailer.
5. Keep existing constructors. Additive knobs next to `max_redirects` /
   `enable_idempotence`.

## Transient errors

Match `crates/volant-client/src/client.rs`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** socket / IO errors from the TCP layer (not
`ProtocolError` / constructor errors). Java retries `ProtocolException`
only when the cause is `IOException`.

**Not retried here:**

- Error **13** (`NotLeaderForPartition`) stays on the **redirect**
  budget (`max_redirects`).
- Error **21** (`UnknownProducerId`) stays on the **one re-Init**
  budget.
- Fetch is not retried.
- Protocol / constructor errors.

## Non-goals

| Deferred | Why |
|----------|-----|
| Fetch retry | Produce only |
| Kafka `retries` / idempotent produce v2 | Native produce only; not Kafka |
| Changing the broker / protocol / Rust client | Already has this (Phase 10) |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing produce signatures and constructors are unchanged.

```python
Client("127.0.0.1:9092", max_retries=0, retry_backoff_ms=50)  # defaults
c.max_retries = 3
```

```go
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
```

```java
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
```

Default is **0 extra attempts**. Go `Dial` / `DialTLS` / `DialAuth` and
Java `connect` / `connectTls` stay as they are.

## Tests

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

1. Default `max_retries=0`: first transient 7 (Timeout) raises; produce
   sent once.
2. `max_retries=2`, backoff 0: Timeout then Timeout then ok → 3
   produces, success.
3. Exhausted retries: 3 Timeouts with `max_retries=2` → raise, 3 sends.
4. Error 13 still uses redirect budget only (do not consume
   `max_retries`).
5. Existing produce / idempotent / redirect tests still pass.

## Honesty leftovers

- **Fetch is not retried here** (Produce only).
- **Not Kafka** `retries` / idempotent produce v2.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**

## Merge notes

Siblings **v0.64 / v0.65** also edit `Client` produce/fetch/delete_records.
Keep `max_redirects` + idempotence + txn fields. Additive knobs only.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`__init__`, `produce`)
- Go `clients/go/client.go` (`Client` struct, `Produce`)
- Java `clients/java/src/main/java/io/volant/Client.java` (fields, `produce`)
- Scripted brokers in `test_client.py` / `client_test.go` / `ClientTest.java`

## Related

- [V47_SPEC.md](./V47_SPEC.md) — idempotent produce leftover this closes
- [V43_SPEC.md](./V43_SPEC.md) — leader redirect (error 13 stays here)
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
