# v0.90 — DescribeGroup / ListGroups retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V78_SPEC.md](./V78_SPEC.md) /
[V69_SPEC.md](./V69_SPEC.md): `max_retries` covers produce / fetch /
heartbeat / offset-admin / ListOffsets, but **DescribeGroup** (34/35)
and **ListGroups** (36/37) are single-shot. Range assignor
(`assignor="range"`) calls `describe_group` after JoinGroup; a
transient 7 falls back to solo / broker assignment.

Reuse those same knobs and the same transient set. No new constructor
args. Do **not** wrap `leave_group`, `join_group`, `add_broker`, or
`_redirect_to_controller`. Heartbeat is already retried (v0.74).
ListOffsets is already retried (v0.82).

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client`.

## Goals

1. Extra DescribeGroup / ListGroups attempts after the first on
   **transient** errors only. Budget is independent of `max_redirects`.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   ListOffsets / Rust `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: TCP / socket / IO (Python `OSError`; Go
     `isTransientTransport`; Java `isTransientTransport` /
     `RuntimeException` wrapping IO — match produce)
3. **Not retried here:**
   - Error **13** / **14**
   - Error **9** / **10** / **11**
   - Error **2** (NotFound). DescribeGroup **2** (no live members) must
     still raise immediately.
   - Protocol / constructor errors
   - JoinGroup / LeaveGroup / CreateTopic
4. Default `max_retries=0` so existing DescribeGroup / ListGroups tests
   stay valid (no extra RPCs).
5. Sleep between retry attempts using the existing produce/fetch
   backoff helper (`_sleep_produce_retry` / `sleepProduceRetry` /
   `Thread.sleep` via the same field).
6. No new public methods. Existing `describe_group` / `DescribeGroup` /
   `describeGroup` and `list_groups` / `ListGroups` / `listGroups`
   signatures stay. GroupConsumer range path already calls
   `describe_group` and inherits the retry.

## Transient errors

Match produce / fetch / heartbeat / offset-admin / ListOffsets and
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
  this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`). DescribeGroup **2** (no live members) raises
  immediately.
- Protocol / constructor errors.
- JoinGroup / LeaveGroup / CreateTopic.

## Non-goals

| Deferred | Why |
|----------|-----|
| JoinGroup / LeaveGroup retry | Not this slice; Join/Leave not idempotent the same way |
| Produce-batch / fetch / heartbeat / offset-admin / ListOffsets retry changes | DescribeGroup / ListGroups only |
| Group reset / admin redirect | Other residuals; 13/14 not returned here today |
| Kafka `retries` / DescribeGroups vN | Native 34–37 only; not Kafka |
| Changing the broker / protocol / Rust client | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping `leave_group` / `join_group` / `add_broker` / `_redirect_to_controller` | Explicitly out of scope |

## API

Existing DescribeGroup / ListGroups signatures and constructors are
unchanged. These RPCs now share produce/fetch/heartbeat/offset-admin /
ListOffsets knobs:

```python
Client("127.0.0.1:9092", max_retries=0, retry_backoff_ms=50)  # defaults
c.max_retries = 3
c.describe_group("g")
c.list_groups()
```

```go
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
c.DescribeGroup("g")
c.ListGroups()
```

```java
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
c.describeGroup("g");
c.listGroups();
```

Default is **0 extra attempts**. Go `Dial` / `DialTLS` / `DialAuth` and
Java `connect` / `connectTls` stay as they are.

## Semantics

Same budget as produce/fetch/heartbeat/offset-admin/ListOffsets
(independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one DescribeGroup
  RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two DescribeGroup RPCs,
  success. Same for ListGroups.
- First DescribeGroup **2** (no live members) raises immediately; no
  retry.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` DescribeGroup RPCs.
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
| Default `max_retries=0`, first DescribeGroup 7 | raise immediately; one DescribeGroup RPC |
| `max_retries=2`, `retry_backoff_ms=0`, first DescribeGroup 7 then ok | success; two DescribeGroup RPCs |
| first DescribeGroup 2 (no live members) | raise immediately; no retry |
| ListGroups 7 then ok | success; two ListGroups RPCs |
| Exhausted retries (always 7) | raise 7 after `1+max_retries` DescribeGroup RPCs |

## Honesty leftovers

- **Not Kafka** `retries` / DescribeGroups versions.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- JoinGroup / LeaveGroup / CreateTopic are unchanged.
- OffsetCommit / OffsetFetch / DeleteOffsets already retried (v0.78).
- ListOffsets already retried (v0.82).
- Heartbeat already retried (v0.74).
- Error 13 / 14 are not redirected here.
- Rust `Client::describe_group` / `list_groups` are unchanged
  (language clients only).

## Merge notes

Siblings that also edit `Client` (v0.86 / v0.89) should keep
produce/fetch/heartbeat/offset-admin/ListOffsets retry. Only wrap
`describe_group` / `DescribeGroup` / `describeGroup` and
`list_groups` / `ListGroups` / `listGroups`. Reuse
`_is_transient_broker` / `isTransientBroker` / `isTransientTransport`
and the existing backoff helper. Do **not** wrap `leave_group`,
`join_group`, `add_broker`, or `_redirect_to_controller`.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`describe_group` /
  `list_groups`)
- Go `clients/go/client.go` (`DescribeGroup` / `ListGroups`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`describeGroup` / `listGroups`)
- Scripted brokers in `test_client.py` / `client_test.go` /
  `ClientTest.java`

## Related

- [V82_SPEC.md](./V82_SPEC.md) — ListOffsets retry leftover this extends
- [V78_SPEC.md](./V78_SPEC.md) — offset-admin retry leftover this extends
- [V74_SPEC.md](./V74_SPEC.md) — heartbeat retry
- [V69_SPEC.md](./V69_SPEC.md) — GroupConsumer range via DescribeGroup
- [V66_SPEC.md](./V66_SPEC.md) — fetch retry
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V49_SPEC.md](./V49_SPEC.md) — DescribeGroup / ListGroups on Python / Go / Java
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
