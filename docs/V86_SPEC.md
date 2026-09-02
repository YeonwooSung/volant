# v0.86 — LeaveGroup retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V74_SPEC.md](./V74_SPEC.md):
`max_retries` (default **0**) applies to produce, fetch, heartbeat, and
offset-admin. **JoinGroup / LeaveGroup** are not retried. Join is not
idempotent — do **not** retry JoinGroup. Leave is the safer next: a
timeout after the broker already processed Leave should not fail
`GroupConsumer.leave`.

Reuse those same knobs and the same transient set on **LeaveGroup**.
No new constructor args. Do **not** retry JoinGroup.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client`.

## Goals

1. Extra LeaveGroup attempts after the first on **transient** errors
   only. Budget is independent of `max_redirects`.
2. Same transient set as produce / fetch / heartbeat / Rust
   `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: TCP / socket / IO (Python `OSError`; Go
     `isTransientTransport`; Java `isTransientTransport` /
     `RuntimeException` wrapping IO — match produce)
3. **Error 10 (UnknownMemberId) is success** — already left (first call
   or after a retried timeout). Do not raise.
4. **Not retried here:**
   - **9** RebalanceInProgress / **11** IllegalGeneration
   - Error **13** / **14**
   - Error **2** (NotFound)
   - Protocol / constructor errors
   - JoinGroup / heartbeat / describe_group / add_broker
5. Default `max_retries=0` so existing leave tests stay valid (no extra
   LeaveGroup attempts).
6. Sleep between retry attempts using the existing produce/fetch
   backoff helper (`_sleep_produce_retry` / `sleepProduceRetry` /
   `Thread.sleep` via the same field).
7. No new public methods. Existing `leave_group` / `LeaveGroup` /
   `leaveGroup` signatures stay. `GroupConsumer.leave` already calls
   LeaveGroup and inherits the retry.

## Transient errors

Match produce / fetch / heartbeat and `crates/volant-client`
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

**Success (not an error):**

- Error **10** (`UnknownMemberId`) — already left. Return success
  without retrying.

**Not retried here:**

- Error **9** (`RebalanceInProgress`), **11** (`IllegalGeneration`).
- Error **13** / **14**.
- Error **2** (`NotFound`).
- Protocol / constructor errors.
- JoinGroup.

## Non-goals

| Deferred | Why |
|----------|-----|
| JoinGroup retry | Not idempotent the same way |
| Produce-batch / fetch / heartbeat / offset-admin retry changes | LeaveGroup only |
| Group reset / admin redirect | Other residuals |
| Kafka `retries` / LeaveGroup vN | Native leave only; not Kafka |
| Changing the broker / protocol / Rust client | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing LeaveGroup signatures and constructors are unchanged.
LeaveGroup now shares produce/fetch/heartbeat knobs:

```python
Client("127.0.0.1:9092", max_retries=0, retry_backoff_ms=50)  # defaults
c.max_retries = 3
c.leave_group("g", member_id)
```

```go
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
c.LeaveGroup(group, memberID)
```

```java
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
c.leaveGroup(group, memberId);
```

Default is **0 extra attempts**. Go `Dial` / `DialTLS` / `DialAuth` and
Java `connect` / `connectTls` stay as they are.

## Semantics

Same budget as produce/fetch/heartbeat (independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one LeaveGroup
  RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two LeaveGroup RPCs,
  success.
- First LeaveGroup **10** (already left) succeeds; one RPC. No raise.
- `max_retries=2`: Timeout then 10 → two LeaveGroup RPCs, success.
- First LeaveGroup **9** (rebalance) raises/returns 9 immediately; no
  retry. Same for 11 / 2 / 13 / 14.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` LeaveGroup RPCs.
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
| Default `max_retries=0`, first Leave 7 | raise immediately; one Leave RPC |
| `max_retries=2`, `retry_backoff_ms=0`, first Leave 7 then ok | success; two Leave RPCs |
| first Leave 10 | success; one RPC (already left) |
| `max_retries=2`, first Leave 7 then 10 | success; two RPCs |
| first Leave 9 (rebalance) | raise/return 9 immediately; no retry |

## Honesty leftovers

- **Not Kafka** `retries` / LeaveGroup versions.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- JoinGroup is unchanged (not retried).
- Heartbeat already retried (v0.74); offset-admin already retried
  (v0.78 / v0.82).
- Rust `Client::leave_group` is unchanged (language clients only).

## Merge notes

Siblings that also edit `Client` (v0.89 / v0.90) should keep
produce/fetch/heartbeat/offset-admin retry. Only wrap `leave_group` /
`LeaveGroup` / `leaveGroup`. Reuse `_is_transient_broker` /
`isTransientBroker` / `isTransientTransport` and the existing backoff
helper. Do **not** wrap `join_group`, `heartbeat`, `describe_group`, or
`add_broker`.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`leave_group`)
- Go `clients/go/client.go` (`LeaveGroup`)
- Java `clients/java/src/main/java/io/volant/Client.java` (`leaveGroup`)
- Scripted brokers in `test_client.py` / `client_test.go` /
  `ClientTest.java`

## Related

- [V74_SPEC.md](./V74_SPEC.md) — heartbeat retry leftover this extends
- [V82_SPEC.md](./V82_SPEC.md) — ListOffsets retry
- [V78_SPEC.md](./V78_SPEC.md) — offset-admin retry
- [V66_SPEC.md](./V66_SPEC.md) — fetch retry
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V44_SPEC.md](./V44_SPEC.md) — group heartbeat / leave
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
