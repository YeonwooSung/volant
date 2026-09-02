# v0.66 — Fetch retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V61_SPEC.md](./V61_SPEC.md):
“Fetch is not retried.” v0.61 already added `max_retries` (default
**0**) and `retry_backoff_ms` (default **50**) on language-client
produce. Reuse those same knobs on **fetch**. No new constructor args.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client` (Rust fetch already
retries via `ClientConfig.max_retries`).

## Goals

1. Extra fetch attempts after the first on **transient** errors only.
   Budget is independent of `max_redirects`.
2. Same transient set as produce / Rust `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: TCP / socket / IO (Python `OSError`; Go
     `isTransientTransport`; Java `isTransientTransport` /
     `RuntimeException` wrapping IO — match produce)
3. **Not retried here:**
   - Error **13** (`NotLeaderForPartition`) stays on the **redirect**
     budget (`max_redirects`)
   - Protocol / constructor errors
4. Default `max_retries=0` so existing fetch tests stay valid (no extra
   fetch attempts).
5. Sleep between retry attempts using the existing produce backoff
   helper (`_sleep_produce_retry` / `sleepProduceRetry` /
   `Thread.sleep` via the same field).
6. No new public methods. Existing `fetch` / `Fetch` / `FetchOpts`
   signatures stay.

## Transient errors

Match produce and `crates/volant-client` `is_transient_error_code`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** socket / IO errors from the TCP layer (not
`ProtocolError` / constructor errors). Java retries
`RuntimeException` wrapping `IOException` the same way produce does.

**Not retried here:**

- Error **13** (`NotLeaderForPartition`) stays on the **redirect**
  budget (`max_redirects`).
- Protocol / constructor errors.

## Non-goals

| Deferred | Why |
|----------|-----|
| Produce-batch retry changes | Fetch only |
| Group reset / admin redirect | Other residuals |
| Kafka `retries` / Fetch vN | Native fetch only; not Kafka |
| Changing the broker / protocol / Rust client | Rust already retries fetch |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing fetch signatures and constructors are unchanged. Fetch now
shares produce’s knobs:

```python
Client("127.0.0.1:9092", max_retries=0, retry_backoff_ms=50)  # defaults
c.max_retries = 3
c.fetch("t", 0, offset=0)
```

```go
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
c.Fetch(topic, partition, offset)
c.FetchOpts(topic, partition, offset, maxMessages, maxBytes, maxWaitMs)
```

```java
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
c.fetch(topic, partition, offset);
```

Default is **0 extra attempts**. Go `Dial` / `DialTLS` / `DialAuth` and
Java `connect` / `connectTls` stay as they are.

## Semantics

Same budget as produce (independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one Fetch RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two Fetch RPCs,
  success.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` Fetch RPCs.
- Error 13 still uses `max_redirects` only (do not consume
  `max_retries`).
- Transport fail then ok with `max_retries >= 1` → success.

## Tests

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first fetch error 7 | raise immediately; one Fetch RPC |
| `max_retries=2`, `retry_backoff_ms=0`, first fetch 7 then ok | success; two Fetch RPCs |
| `max_retries=2`, first fetch error 13 then Metadata leader then ok | still redirect path (not counted as retry); works as today |
| Transport fail then ok with `max_retries>=1` | success |
| Exhaust retries (always 7) | raise 7 after `1+max_retries` Fetch RPCs |

## Honesty leftovers

- **Not Kafka** `retries` / Fetch versions.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- Admin RPCs other than the produce/fetch/delete_records paths are
  unchanged.

## Merge notes

Siblings that also edit `Client` should keep produce retry +
delete_records redirect. Only wrap `fetch` / `FetchOpts` / `fetchAt`.
Reuse `_is_transient_broker` / `isTransientBroker` /
`isTransientTransport` and the existing backoff helper.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`fetch`)
- Go `clients/go/client.go` (`FetchOpts`)
- Java `clients/java/src/main/java/io/volant/Client.java` (`fetchAt`)
- Scripted brokers in `test_client.py` / `client_test.go` / `ClientTest.java`

## Related

- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this closes
- [V43_SPEC.md](./V43_SPEC.md) — leader redirect (error 13 stays here)
- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Fetch knobs
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
