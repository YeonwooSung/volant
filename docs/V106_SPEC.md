# v0.106 — Auth (shared-token) retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V42_SPEC.md](./V42_SPEC.md) /
[V101_SPEC.md](./V101_SPEC.md): produce / InitProducerId retry transient
6 / 7 / 15 / 16, but token Auth (`_authenticate` / `authenticate` /
Java `authenticate`, opcode 30/31) is still a single shot. Connect /
reconnect call `_maybe_authenticate` → `_authenticate` when
`auth_token` is set. A timeout (7) on Auth fails the whole constructor.

Reuse those same knobs and the same transient set. No new constructor
args. Wrap only the shared-token Auth send path. Do **not** wrap
SCRAM first/final (`_authenticate_scram` / sibling v0.108) or
DeleteRecords (sibling v0.110).

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client` (sibling v0.107).

## Goals

1. Extra Auth attempts after the first on **transient** errors only.
   Budget is independent of `max_redirects` and of produce / Init.
2. Same transient set as produce / InitProducerId (v0.101):
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: TCP / socket / IO (Python `OSError`; Go
     `isTransientTransport`; Java `isTransientTransport` /
     `RuntimeException` wrapping IO — match produce)
3. **Not retried here:**
   - Error **17** (`AuthenticationFailed`) / **18**
     (`AuthenticationRequired`, if present)
   - Error **13** / **14**
   - Error **9** / **10** / **11**
   - Error **2** (NotFound)
   - Error **21** / **22**
   - Protocol / constructor errors
   - SCRAM first/final (`_authenticate_scram` left alone)
4. Default `max_retries=0` so existing Auth tests stay valid (no extra
   Auth RPCs).
5. Sleep between retry attempts using the existing produce/fetch
   backoff helper (`_sleep_produce_retry` / `sleepProduceRetry` /
   `Thread.sleep` via the same field).
6. Independent retry counter inside `_authenticate` / `authenticate`
   only. Reconnect still re-runs auth (already true); each Auth call
   gets its own `max_retries`.
7. No new public methods. Wrap only:
   - `_authenticate`
   - `authenticate`
   - Java `authenticate`

## Transient errors

Match produce / InitProducerId and `crates/volant-client`
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

- Error **17** (`AuthenticationFailed`) / **18**
  (`AuthenticationRequired`).
- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Error **21** (`UnknownProducerId`) / **22** (`InvalidTxnState`).
- Protocol / constructor errors.
- SCRAM first/final handshake (sibling v0.108).
- Produce / Init / `_admin_round_trip` (already retried elsewhere).

## Non-goals

| Deferred | Why |
|----------|-----|
| SCRAM first/final retry | Sibling v0.108 |
| DeleteRecords | Sibling v0.110 |
| Rust `volant-client` Auth retry | Sibling v0.107 |
| Kafka SASL / `--kafka-listen` | Native opcode 30 only |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing Auth signatures and constructors are unchanged. Auth now
shares produce/fetch/Init knobs:

```python
Client("127.0.0.1:9092", auth_token="s3cret", max_retries=0, retry_backoff_ms=50)
c = Client("127.0.0.1:9092", auth_token="s3cret", max_retries=3, retry_backoff_ms=0)
```

```go
c, err := DialAuth("127.0.0.1:9092", "s3cret") // maxRetries defaults to 0
c.SetMaxRetries(3)                             // applies on reconnect Auth
c.SetRetryBackoff(0)                           // 0 allowed in tests
```

```java
Client.connect("127.0.0.1", 9092, "s3cret"); // maxRetries defaults to 0
c.setMaxRetries(3);                          // applies on reconnect Auth
c.setRetryBackoffMs(0);
```

Default is **0 extra attempts**. Go `Dial` / `DialTLS` / `DialAuth` and
Java `connect` / `connectTls` stay as they are.

## Semantics

Same budget as produce/fetch/Init (independent of redirect):

- Default `max_retries=0`: first Auth transient 7 raises; one Auth
  RPC. Constructor / Dial / connect fails.
- `max_retries=2`, backoff 0: first Auth Timeout then ok → two Auth
  RPCs, connect succeeds.
- First Auth **17** (even with `max_retries=2`) raises immediately;
  one Auth RPC.
- Exhausted retries: always 7 on Auth with `max_retries=2` → raise
  7 after `1 + max_retries` Auth RPCs.
- Transport fail then ok with `max_retries >= 1` → success (same
  socket; retry is `_round_trip`, not reconnect).

Honesty: Go / Java apply `SetMaxRetries` / `setMaxRetries` after
Dial / connect, so the **first** connect Auth uses the default 0.
Reconnect re-runs Auth with whatever knobs are then set. Python
constructor already takes `max_retries`, so first-connect Auth
honors it.

## Tests

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Reuse / extend existing Auth stubs (`_OneShotServer` / `serveAuth` /
`OneShotAuthServer`) with a **queue of Auth codes on the same
connection** (retry is same-socket `_round_trip`, not reconnect).
Keep the connection open after a transient Auth reply.

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first Auth 7 | raise 7; one Auth RPC |
| `max_retries=2`, `retry_backoff_ms=0`, first Auth 7 then 0 | connect success; two Auth RPCs |
| first Auth 17 (even with `max_retries=2`) | raise 17 immediately; one Auth RPC |
| Exhaust always-7 with `max_retries=2` | raise 7 after 3 Auth RPCs |

Keep existing Auth tests (token sent first, empty token skips,
rejected 17).

## Honesty leftovers

- **Not Kafka** SASL / `retries`.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- SCRAM first/final is unchanged (sibling v0.108).
- DeleteRecords is unchanged (sibling v0.110).
- Rust Auth retry is unchanged (sibling v0.107).
- Error 17 / 18 on Auth are raised immediately (not retried).
- Go / Java first-connect Auth still sees default `maxRetries=0`
  unless a test helper applies knobs before `maybeAuthenticate`.

## Merge notes

Siblings **v0.108 (SCRAM)** and **v0.110 (DeleteRecords)** also edit
the three `Client` files. Keep hunks on the Auth helper + tests.
Reuse `_is_transient_broker` / `isTransientBroker` /
`isTransientTransport` and the existing backoff helper. Do **not**
wrap SCRAM or DeleteRecords.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`_authenticate`)
- Go `clients/go/client.go` (`authenticate`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`authenticate`)
- Auth stubs in `test_auth.py` / `auth_test.go` / `AuthTest.java`

## Related

- [V42_SPEC.md](./V42_SPEC.md) — shared-token Auth leftover this
  extends
- [V101_SPEC.md](./V101_SPEC.md) — InitProducerId retry this mirrors
- [V61_SPEC.md](./V61_SPEC.md) — produce retry knobs
- [V46_SPEC.md](./V46_SPEC.md) — SCRAM handshake this does not wrap
