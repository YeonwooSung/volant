# v0.74 — Heartbeat retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V61_SPEC.md](./V61_SPEC.md) /
[V66_SPEC.md](./V66_SPEC.md): `max_retries` (default **0**) applies to
produce and fetch only. Group membership **Heartbeat** (native opcode
**9**) is not retried, so a single transient 7 / 6 / 15 / 16 or TCP
blip expires a quiet consumer.

Reuse those same knobs and the same transient set on **Heartbeat**. No
new constructor args. Do **not** retry JoinGroup (not idempotent the
same way). Do **not** retry LeaveGroup.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client`.

## Goals

1. Extra Heartbeat attempts after the first on **transient** errors
   only. Budget is independent of `max_redirects`.
2. Same transient set as produce / fetch / Rust
   `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: TCP / socket / IO (Python `OSError`; Go
     `isTransientTransport`; Java `isTransientTransport` /
     `RuntimeException` wrapping IO — match produce)
3. **Not retried here:**
   - **9** RebalanceInProgress / **10** UnknownMemberId / **11**
     IllegalGeneration — GroupConsumer must still see these to rejoin
   - Error **13** / **14**
   - Protocol / constructor errors
   - JoinGroup / LeaveGroup
4. Default `max_retries=0` so existing heartbeat tests stay valid (no
   extra Heartbeat attempts).
5. Sleep between retry attempts using the existing produce/fetch
   backoff helper (`_sleep_produce_retry` / `sleepProduceRetry` /
   `Thread.sleep` via the same field).
6. No new public methods. Existing `heartbeat` / `Heartbeat`
   signatures stay. GroupConsumer `poll` already calls `heartbeat` and
   inherits the retry.

## Transient errors

Match produce / fetch and `crates/volant-client` `is_transient_error_code`.

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

- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`) — membership must rejoin.
- Error **13** / **14**.
- Protocol / constructor errors.
- JoinGroup / LeaveGroup.

## Non-goals

| Deferred | Why |
|----------|-----|
| JoinGroup / LeaveGroup retry | Not idempotent the same way |
| Produce-batch / fetch retry changes | Heartbeat only |
| Group reset / admin redirect | Other residuals |
| Kafka `retries` / Heartbeat vN | Native heartbeat only; not Kafka |
| Changing the broker / protocol / Rust client | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing heartbeat signatures and constructors are unchanged.
Heartbeat now shares produce/fetch knobs:

```python
Client("127.0.0.1:9092", max_retries=0, retry_backoff_ms=50)  # defaults
c.max_retries = 3
c.heartbeat("g", member_id, generation)
```

```go
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
c.Heartbeat(group, memberID, generation)
```

```java
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
c.heartbeat(group, memberId, generation);
```

Default is **0 extra attempts**. Go `Dial` / `DialTLS` / `DialAuth` and
Java `connect` / `connectTls` stay as they are.

## Semantics

Same budget as produce/fetch (independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one Heartbeat RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two Heartbeat RPCs,
  success.
- First Heartbeat **9** (rebalance) raises/returns 9 immediately; no
  retry. Same for 10 / 11.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` Heartbeat RPCs.
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
| Default `max_retries=0`, first Heartbeat 7 | raise immediately; one Heartbeat RPC |
| `max_retries=2`, `retry_backoff_ms=0`, first Heartbeat 7 then ok | success; two Heartbeat RPCs |
| first Heartbeat 9 (rebalance) | raise/return 9 immediately; no retry |
| Transport fail then ok with `max_retries>=1` | success |
| Exhaust retries (always 7) | raise 7 after `1+max_retries` Heartbeat RPCs |

## Honesty leftovers

- **Not Kafka** `retries` / Heartbeat versions.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- JoinGroup / LeaveGroup and other admin RPCs are unchanged.

## Merge notes

Siblings that also edit `Client` (v0.72 admin 14 redirect) should keep
produce/fetch retry + delete_records redirect. Only wrap `heartbeat` /
`Heartbeat`. Reuse `_is_transient_broker` / `isTransientBroker` /
`isTransientTransport` and the existing backoff helper.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`heartbeat`)
- Go `clients/go/client.go` (`Heartbeat`)
- Java `clients/java/src/main/java/io/volant/Client.java` (`heartbeat`)
- Scripted brokers in `test_client.py` / `client_test.go` / `ClientTest.java`

## Related

- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V66_SPEC.md](./V66_SPEC.md) — fetch retry leftover this closes
- [V44_SPEC.md](./V44_SPEC.md) — group heartbeat
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
