# v0.101 — InitProducerId retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V61_SPEC.md](./V61_SPEC.md) /
TODO: produce retries transient 6 / 7 / 15 / 16, and error **21**
(`UnknownProducerId`) has a **one re-Init** budget. The InitProducerId
RPC itself (`_ensure_producer_id` / `ensureProducerID`) is still a
single shot. A timeout on opcode **32** fails the first
idempotent/txn produce.

Reuse those same knobs and the same transient set. No new constructor
args. Wrap only the InitProducerId send path. Produce’s error-21
re-Init still calls this helper — after this slice it inherits Init
retries independently of the one re-Init. Do **not** change produce
retry, BeginTxn retry, or `_admin_round_trip`.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client`.

## Goals

1. Extra InitProducerId attempts after the first on **transient**
   errors only. Budget is independent of `max_redirects` and of
   produce’s one unknown-pid re-Init.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   Leave / Describe / Metadata / BeginTxn / EndTxn / Rust
   `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: TCP / socket / IO (Python `OSError`; Go
     `isTransientTransport`; Java `isTransientTransport` /
     `RuntimeException` wrapping IO — match produce)
3. **Not retried here:**
   - Error **13** / **14**
   - Error **9** / **10** / **11**
   - Error **2** (NotFound)
   - Protocol / constructor errors
   - Error **21** (`UnknownProducerId` on Init itself — raise; do not
     confuse with produce’s re-Init budget)
4. Default `max_retries=0` so existing idempotent / txn tests stay
   valid (no extra Init RPCs).
5. Sleep between retry attempts using the existing produce/fetch
   backoff helper (`_sleep_produce_retry` / `sleepProduceRetry` /
   `Thread.sleep` via the same field).
6. No new public methods. Wrap only:
   - `_ensure_producer_id`
   - `ensureProducerID`
   - Java `ensureProducerId`

## Transient errors

Match produce / fetch / heartbeat / offset-admin / Leave / Describe /
Metadata / BeginTxn / EndTxn and `crates/volant-client`
`is_transient_error_code`.

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

- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Error **21** (`UnknownProducerId`) on Init itself. Produce’s one
  re-Init budget is unchanged; that path calls this helper again
  after `resetProducerID`, and that second Init has its own
  `max_retries` budget.
- Protocol / constructor errors.
- Produce retry, BeginTxn retry, `_admin_round_trip` (already
  retried / redirected elsewhere; do not re-wrap).

## Non-goals

| Deferred | Why |
|----------|-----|
| Produce retry / produce’s unknown-pid re-Init | Frozen; independent budget |
| BeginTxn / EndTxn retry | Already shipped (v0.99) |
| Wrapping `_admin_round_trip` | Already redirected (error 14) |
| Kafka `retries` / InitProducerId vN | Native opcode 32 only; not Kafka |
| Changing the broker / protocol / Rust client | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing produce / Init signatures and constructors are unchanged.
InitProducerId now shares produce/fetch/heartbeat knobs:

```python
Client("127.0.0.1:9092", enable_idempotence=True, max_retries=0, retry_backoff_ms=50)
c.max_retries = 3
c.produce("t", 0, value=b"hello")  # first produce sends Init
```

```go
c.EnableIdempotence()
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
c.Produce("t", 0, nil, []byte("hello"))
```

```java
c.setEnableIdempotence(true);
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
c.produce("t", 0, null, "hello".getBytes());
```

Default is **0 extra attempts**. Go `Dial` / `DialTLS` / `DialAuth` and
Java `connect` / `connectTls` stay as they are.

## Semantics

Same budget as produce/fetch/heartbeat (independent of redirect and
of produce’s one re-Init):

- Default `max_retries=0`: first Init transient 7 raises; one Init
  RPC.
- `max_retries=2`, backoff 0: first Init Timeout then ok → two Init
  RPCs, produce/init success.
- First Init **UnknownProducerId (21)** raises immediately; no extra
  Init.
- Exhausted retries: always 7 on Init with `max_retries=2` → raise
  7 after `1 + max_retries` Init RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

Honesty: produce’s error-21 re-Init still has a **one** budget. Each
Init call (first or re-Init) independently applies `max_retries` for
transient 6 / 7 / 15 / 16 / TCP.

## Tests

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Reuse existing InitProducerId scripted brokers (idempotent produce
tests):

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first Init 7 | raise 7; one Init RPC |
| `max_retries=2`, `retry_backoff_ms=0`, first Init 7 then 0 | produce/init success; two Init RPCs |
| first Init 21 | raise 21 immediately; no extra Init |
| Exhaust always-7 | raise 7 after `1+max_retries` Init RPCs |

## Honesty leftovers

- **Not Kafka** `retries` / InitProducerId versions.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- Produce retry and produce’s unknown-pid re-Init (error 21) are
  unchanged (one re-Init still).
- BeginTxn / EndTxn wraps are unchanged (already retried).
- `_admin_round_trip` is unchanged.
- Error 21 on Init itself is raised immediately (not retried).
- Rust `ensure_producer_id` is unchanged (language clients only).

## Merge notes

Siblings **v0.103 / v0.105** also edit `Client`. Keep hunks on the
Init helper + tests. Reuse `_is_transient_broker` /
`isTransientBroker` / `isTransientTransport` and the existing
backoff helper. Do **not** wrap produce, BeginTxn, or
`_admin_round_trip`.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`_ensure_producer_id`)
- Go `clients/go/client.go` (`ensureProducerID`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`ensureProducerId`)
- Scripted brokers in `test_client.py` / `client_test.go` /
  `ClientTest.java`

## Related

- [V99_SPEC.md](./V99_SPEC.md) — BeginTxn / EndTxn retry leftover this
  does not change
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V57_SPEC.md](./V57_SPEC.md) — BeginTxn / EndTxn on language clients
- [V47_SPEC.md](./V47_SPEC.md) — idempotent produce / InitProducerId
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
