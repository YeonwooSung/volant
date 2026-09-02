# v0.82 — ListOffsets retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V78_SPEC.md](./V78_SPEC.md):
OffsetCommit / OffsetFetch / DeleteOffsets share `max_retries` (default
**0**). **ListOffsets** (native 48/49) is not retried, so a single
transient 7 / 6 / 15 / 16 or TCP blip fails GroupConsumer
`earliest` / `latest` reset (`list_offsets` on join).

This RPC does **not** return error 13 / 14 today (v0.72 explicitly did
not wrap it). This slice is **retry**, not redirect.

Reuse those same knobs and the same transient set. No new constructor
args. Do **not** wrap OffsetCommit (already v0.78), JoinGroup,
LeaveGroup, or CreateTopic. Heartbeat is already retried (v0.74).

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client`.

## Goals

1. Extra ListOffsets attempts after the first on **transient** errors
   only. Budget is independent of `max_redirects`.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   Rust `is_transient_error_code`:
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
   - OffsetCommit / JoinGroup / LeaveGroup / CreateTopic
4. Default `max_retries=0` so existing ListOffsets tests stay valid (no
   extra ListOffsets attempts).
5. Sleep between retry attempts using the existing produce/fetch
   backoff helper (`_sleep_produce_retry` / `sleepProduceRetry` /
   `Thread.sleep` via the same field).
6. No new public methods. Existing `list_offsets` / `ListOffsets` /
   `listOffsets` signatures stay. GroupConsumer `earliest` / `latest`
   reset already calls `list_offsets` and inherits the retry.

## Transient errors

Match produce / fetch / heartbeat / offset-admin and
`crates/volant-client` `is_transient_error_code`.

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
  v0.72 did not wrap this RPC; this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Protocol / constructor errors.
- OffsetCommit / JoinGroup / LeaveGroup / CreateTopic.

## Non-goals

| Deferred | Why |
|----------|-----|
| JoinGroup / LeaveGroup retry | Not this slice; Join/Leave not idempotent the same way |
| Produce-batch / fetch / heartbeat / offset-admin retry changes | ListOffsets only |
| Group reset / admin redirect | Other residuals; 13/14 not returned here today |
| Kafka `retries` / ListOffsets vN | Native 48/49 only; not Kafka |
| Changing the broker / protocol / Rust client | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping `offset_commit` / `_redirect_to_controller` / `create_scram_user` | Already v0.78 / siblings |

## API

Existing ListOffsets signatures and constructors are unchanged.
ListOffsets now shares produce/fetch/heartbeat/offset-admin knobs:

```python
Client("127.0.0.1:9092", max_retries=0, retry_backoff_ms=50)  # defaults
c.max_retries = 3
c.list_offsets("t")
c.list_offsets("t", [0, 1])
```

```go
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
c.ListOffsets(topic, nil)
c.ListOffsets(topic, []uint32{0, 1})
```

```java
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
c.listOffsets(topic);
c.listOffsets(topic, 0, 1);
```

Default is **0 extra attempts**. Go `Dial` / `DialTLS` / `DialAuth` and
Java `connect` / `connectTls` stay as they are.

## Semantics

Same budget as produce/fetch/heartbeat/offset-admin (independent of
redirect):

- Default `max_retries=0`: first transient 7 raises; one ListOffsets
  RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two ListOffsets RPCs,
  success.
- First ListOffsets **2** (not found) raises immediately; no retry.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` ListOffsets RPCs.
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
| Default `max_retries=0`, first ListOffsets 7 | raise immediately; one ListOffsets RPC |
| `max_retries=2`, `retry_backoff_ms=0`, first ListOffsets 7 then ok | success; two ListOffsets RPCs |
| first ListOffsets 2 (not found) | raise immediately; no retry |
| Exhausted retries (always 7) | raise 7 after `1+max_retries` ListOffsets RPCs |

## Honesty leftovers

- **Not Kafka** `retries` / ListOffsets versions.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- JoinGroup / LeaveGroup / CreateTopic are unchanged.
- OffsetCommit / OffsetFetch / DeleteOffsets already retried (v0.78).
- Heartbeat already retried (v0.74).
- Error 13 / 14 are not redirected here (v0.72 leftover).
- Rust `Client::list_offsets` is unchanged (language clients only).

## Merge notes

Siblings that also edit `Client` (v0.81 / v0.85) should keep
produce/fetch/heartbeat/offset-admin retry. Only wrap `list_offsets` /
`ListOffsets` / `listOffsets`. Reuse `_is_transient_broker` /
`isTransientBroker` / `isTransientTransport` and the existing backoff
helper. Do **not** wrap `offset_commit`, `_redirect_to_controller`, or
`create_scram_user`.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`list_offsets`)
- Go `clients/go/client.go` (`ListOffsets`)
- Java `clients/java/src/main/java/io/volant/Client.java` (`listOffsets`)
- Scripted brokers in `test_client.py` / `client_test.go` /
  `ClientTest.java`

## Related

- [V78_SPEC.md](./V78_SPEC.md) — offset-admin retry leftover this extends
- [V74_SPEC.md](./V74_SPEC.md) — heartbeat retry
- [V66_SPEC.md](./V66_SPEC.md) — fetch retry
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V70_SPEC.md](./V70_SPEC.md) — GroupConsumer earliest via ListOffsets
- [V50_SPEC.md](./V50_SPEC.md) — ListOffsets on Python / Go / Java
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
