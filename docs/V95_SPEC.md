# v0.95 — Metadata / ListMembers retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V72_SPEC.md](./V72_SPEC.md) /
[V81_SPEC.md](./V81_SPEC.md): admin-14 and leader-13 redirect call
**Metadata** (and sometimes **ListMembers**) with no retry. A single
transient 7 on Metadata aborts redirect. `max_retries` already covers
produce / fetch / heartbeat / offset-admin / Leave / DescribeGroup /
ListGroups.

Reuse those same knobs and the same transient set. No new constructor
args. Do **not** wrap `describe_configs`, `add_broker`, `leave_group`,
or `_redirect_to_controller` hunt logic. Redirect helpers inherit
automatically because they call `metadata()` / `list_members()`.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client`.

## Goals

1. Extra Metadata / ListMembers attempts after the first on
   **transient** errors only. Budget is independent of `max_redirects`.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   ListOffsets / DescribeGroup / ListGroups / Rust
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
4. Default `max_retries=0` so existing Metadata / ListMembers / redirect
   tests stay valid (no extra RPCs).
5. Sleep between retry attempts using the existing produce/fetch
   backoff helper (`_sleep_produce_retry` / `sleepProduceRetry` /
   `Thread.sleep` via the same field).
6. No new public methods. Existing `metadata` / `Metadata` /
   `metadata` and `list_members` / `ListMembers` / `listMembers`
   signatures stay. Redirect helpers already call those and inherit
   the retry.

## Transient errors

Match produce / fetch / heartbeat / offset-admin / ListOffsets /
DescribeGroup / ListGroups and `crates/volant-client`
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

Native Metadata has **no** top-level `error_code`. Failures arrive as
`ErrorResponse` / Error opcode or transport. Do not invent a Metadata
error_code the codec does not have. Topic-level `error_code` is
unchanged and is not a retry signal. ListMembers keeps its typed
`error_code`.

**Not retried here:**

- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Protocol / constructor errors.
- DescribeConfigs / AddBroker / LeaveGroup / `_redirect_to_controller`
  hunt logic.

## Non-goals

| Deferred | Why |
|----------|-----|
| DescribeConfigs / AddBroker / LeaveGroup retry changes | LeaveGroup already retried (v0.86); others not this slice |
| Produce-batch / fetch / heartbeat / offset-admin / ListOffsets / DescribeGroup / ListGroups retry changes | Metadata / ListMembers only |
| Redirect hunt algorithm | Frozen (v0.72 / v0.81); helpers inherit via existing calls |
| Kafka `retries` / Metadata vN | Native opcodes only; not Kafka |
| Changing the broker / protocol / Rust client | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping `describe_configs` / `add_broker` / `leave_group` / `_redirect_to_controller` | Explicitly out of scope |

## API

Existing Metadata / ListMembers signatures and constructors are
unchanged. These RPCs now share produce/fetch/heartbeat/offset-admin /
ListOffsets / DescribeGroup / ListGroups knobs:

```python
Client("127.0.0.1:9092", max_retries=0, retry_backoff_ms=50)  # defaults
c.max_retries = 3
c.metadata()
c.list_members()
```

```go
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
c.Metadata()
c.ListMembers()
```

```java
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
c.metadata();
c.listMembers();
```

Default is **0 extra attempts**. Go `Dial` / `DialTLS` / `DialAuth` and
Java `connect` / `connectTls` stay as they are.

## Semantics

Same budget as produce/fetch/heartbeat/offset-admin/ListOffsets
(independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one Metadata RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two Metadata RPCs,
  success. Same for ListMembers.
- First Metadata **2** (Error opcode; native Metadata has no top-level
  error_code) raises immediately; no retry.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` Metadata RPCs.
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
| Default `max_retries=0`, first Metadata 7 | raise immediately; one Metadata RPC |
| `max_retries=2`, `retry_backoff_ms=0`, first Metadata 7 then ok | success; two Metadata RPCs |
| first Metadata 2 | raise immediately; no retry |
| ListMembers 7 then ok | success; two ListMembers RPCs |
| Exhausted retries (always 7) | raise 7 after `1+max_retries` Metadata RPCs |

Metadata 7 / 2 arrive as Error opcode (`ErrorResponse`). ListMembers 7
is the typed response `error_code`.

## Honesty leftovers

- **Not Kafka** `retries` / Metadata versions.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- DescribeConfigs / AddBroker / LeaveGroup wraps are unchanged
  (LeaveGroup already retried in v0.86).
- OffsetCommit / OffsetFetch / DeleteOffsets already retried (v0.78).
- ListOffsets already retried (v0.82).
- DescribeGroup / ListGroups already retried (v0.90).
- Heartbeat already retried (v0.74).
- Error 13 / 14 are not redirected here.
- Native Metadata still has no top-level `error_code`.
- Rust `Client::metadata` / `list_members` are unchanged
  (language clients only).

## Merge notes

Siblings that also edit `Client` (v0.93) should keep
produce/fetch/heartbeat/offset-admin/ListOffsets/DescribeGroup/ListGroups
retry. Only wrap `metadata` / `Metadata` / `metadata` and
`list_members` / `ListMembers` / `listMembers`. Reuse
`_is_transient_broker` / `isTransientBroker` / `isTransientTransport`
and the existing backoff helper. Do **not** wrap `describe_configs`,
`add_broker`, `leave_group`, or `_redirect_to_controller`.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`metadata` /
  `list_members`)
- Go `clients/go/client.go` (`Metadata` / `ListMembers`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`metadata` / `listMembers`)
- Scripted brokers in `test_client.py` / `client_test.go` /
  `ClientTest.java`

## Related

- [V90_SPEC.md](./V90_SPEC.md) — DescribeGroup / ListGroups retry leftover this extends
- [V86_SPEC.md](./V86_SPEC.md) — LeaveGroup retry
- [V82_SPEC.md](./V82_SPEC.md) — ListOffsets retry
- [V81_SPEC.md](./V81_SPEC.md) — admin-14 prefers Metadata.controller_id
- [V78_SPEC.md](./V78_SPEC.md) — offset-admin retry
- [V74_SPEC.md](./V74_SPEC.md) — heartbeat retry
- [V72_SPEC.md](./V72_SPEC.md) — admin NotController redirect (calls Metadata / ListMembers)
- [V66_SPEC.md](./V66_SPEC.md) — fetch retry
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
