# v0.78 — OffsetCommit / OffsetFetch / DeleteOffsets retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V61_SPEC.md](./V61_SPEC.md) /
[V74_SPEC.md](./V74_SPEC.md): `max_retries` (default **0**) applies to
produce, fetch, and heartbeat only. Group **OffsetCommit** (and
OffsetFetch / DeleteOffsets) is not retried, so a single transient
7 / 6 / 15 / 16 or TCP blip fails `GroupConsumer.commit`.

These RPCs do **not** return error 13 / 14 today (v0.72 explicitly did
not wrap them). This slice is **retry**, not redirect.

Reuse those same knobs and the same transient set. No new constructor
args. Do **not** retry ListOffsets, JoinGroup, LeaveGroup, or
CreateTopic. Heartbeat is already retried (v0.74).

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client`.

## Goals

1. Extra OffsetCommit / OffsetFetch / DeleteOffsets attempts after the
   first on **transient** errors only. Budget is independent of
   `max_redirects`.
2. Same transient set as produce / fetch / heartbeat / Rust
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
   - ListOffsets / JoinGroup / LeaveGroup / CreateTopic
4. Default `max_retries=0` so existing offset tests stay valid (no
   extra OffsetCommit / OffsetFetch / DeleteOffsets attempts).
5. Sleep between retry attempts using the existing produce/fetch
   backoff helper (`_sleep_produce_retry` / `sleepProduceRetry` /
   `Thread.sleep` via the same field).
6. No new public methods. Existing `offset_commit` / `OffsetCommit`,
   `offset_fetch` / `OffsetFetch`, `delete_offsets` / `DeleteOffsets`
   signatures stay. GroupConsumer `commit` already calls
   `offset_commit` and inherits the retry.

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

**Not retried here:**

- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  v0.72 did not wrap these RPCs; this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Protocol / constructor errors.
- ListOffsets / JoinGroup / LeaveGroup / CreateTopic.

## Non-goals

| Deferred | Why |
|----------|-----|
| ListOffsets / JoinGroup / LeaveGroup retry | Not this slice; Join/Leave not idempotent the same way |
| Produce-batch / fetch / heartbeat retry changes | Offset admin only |
| Group reset / admin redirect | Other residuals; 13/14 not returned here today |
| Kafka `retries` / OffsetCommit vN | Native opcodes only; not Kafka |
| Changing the broker / protocol / Rust client | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing offset signatures and constructors are unchanged.
Offset admin now shares produce/fetch/heartbeat knobs:

```python
Client("127.0.0.1:9092", max_retries=0, retry_backoff_ms=50)  # defaults
c.max_retries = 3
c.offset_commit("g", "t", 0, 5)
c.offset_fetch("g", "t")
c.delete_offsets("g")
```

```go
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
c.OffsetCommit(group, topic, partition, offset)
c.OffsetFetch(group, topic)
c.DeleteOffsets(group, entries)
```

```java
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
c.offsetCommit(group, topic, partition, offset);
c.offsetFetch(group, topic);
c.deleteOffsets(group);
```

Default is **0 extra attempts**. Go `Dial` / `DialTLS` / `DialAuth` and
Java `connect` / `connectTls` stay as they are.

## Semantics

Same budget as produce/fetch/heartbeat (independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one OffsetCommit
  RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two OffsetCommit RPCs,
  success. Same for OffsetFetch and DeleteOffsets.
- First OffsetCommit **2** (not found) raises immediately; no retry.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` OffsetCommit RPCs.
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
| Default `max_retries=0`, first OffsetCommit 7 | raise immediately; one OffsetCommit RPC |
| `max_retries=2`, `retry_backoff_ms=0`, first OffsetCommit 7 then ok | success; two OffsetCommit RPCs |
| OffsetFetch 7 then ok | success; two OffsetFetch RPCs |
| DeleteOffsets 7 then ok | success; two DeleteOffsets RPCs |
| first OffsetCommit 2 (not found) | raise immediately; no retry |
| Exhaust retries (always 7) | raise 7 after `1+max_retries` OffsetCommit RPCs |

## Honesty leftovers

- **Not Kafka** `retries` / OffsetCommit versions.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- ListOffsets / JoinGroup / LeaveGroup / CreateTopic are unchanged.
- Heartbeat already retried (v0.74).
- Error 13 / 14 are not redirected here (v0.72 leftover).

## Merge notes

Siblings that also edit `Client` (v0.72 admin 14 redirect, v0.77
Metadata) should keep produce/fetch/heartbeat retry. Only wrap
`offset_commit` / `OffsetCommit` (and the overloads that send the
RPC), `offset_fetch` / `OffsetFetch`, `delete_offsets` /
`DeleteOffsets`. Reuse `_is_transient_broker` / `isTransientBroker` /
`isTransientTransport` and the existing backoff helper.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (offset methods)
- Go `clients/go/client.go` (`commitOffsets` / `fetchOffsets` /
  `DeleteOffsets`)
- Java `clients/java/src/main/java/io/volant/Client.java` (offset
  methods)
- Scripted brokers in `test_client.py` / `client_test.go` /
  `ClientTest.java`

## Related

- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V66_SPEC.md](./V66_SPEC.md) — fetch retry
- [V74_SPEC.md](./V74_SPEC.md) — heartbeat retry leftover this closes
- [V72_SPEC.md](./V72_SPEC.md) — admin 14 redirect (did not wrap
  OffsetCommit / OffsetFetch / DeleteOffsets)
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
