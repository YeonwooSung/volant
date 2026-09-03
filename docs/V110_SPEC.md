# v0.110 — DeleteRecords transient retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V65_SPEC.md](./V65_SPEC.md) /
[V61_SPEC.md](./V61_SPEC.md): language DeleteRecords already redirects
on error **13** (`NotLeaderForPartition`) via `_redirect_to_leader` /
`max_redirects` (v0.65). It does **not** retry transient **6 / 7 / 15 /
16** or TCP/IO.

Reuse existing `max_retries` / `retry_backoff_ms` (produce / fetch /
heartbeat / offset-admin / ListOffsets / LeaveGroup / DescribeGroup /
ListGroups / Metadata / ListMembers / BeginTxn / EndTxn / admin) and
`_is_transient_broker` / `isTransientBroker` / `isTransientTransport`.
**13 stays on `max_redirects`.** Do **not** wrap Auth (v0.106) or SCRAM
(v0.108). Do **not** wrap `_admin_round_trip`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. Inside existing DeleteRecords (`delete_records` / `DeleteRecords` /
   `deleteRecords`): extra attempts after the first on transient **6 /
   7 / 15 / 16** and TCP/IO.
2. **13 stays on `max_redirects`** — not counted as a transient retry.
   Independent counters: on 13 redirect without incrementing
   `retry_attempt`; on transient increment `retry_attempt` and sleep
   (and do not consume the redirect budget).
3. **Not retried:** 14, 9 / 10 / 11, 2, 17 / 18, 21, 22, Protocol.
4. Default `max_retries=0` so existing DeleteRecords tests stay valid.
5. Sleep via the existing produce backoff helper
   (`_sleep_produce_retry` / `sleepProduceRetry`).
6. `wait_majority` trailer (0 / 1 / 2) is unchanged. Retry resends the
   same encoded body.
7. No new public methods. Wrap the existing send loops only.

## Transient errors

Match produce / fetch / heartbeat / offset-admin / ListOffsets /
LeaveGroup / DescribeGroup / ListGroups / Metadata / ListMembers /
BeginTxn / EndTxn / admin and `crates/volant-client`
`is_transient_error_code`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** socket / IO errors from the TCP layer (Python
`OSError`; Go `isTransientTransport`; Java `isTransientTransport` /
`RuntimeException` wrapping IO — match produce).

**Not retried here:**

- Error **13** (`NotLeaderForPartition`) — stays on `max_redirects`.
- Error **14** (`NotController`).
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Error **17** / **18**.
- Error **21** (`UnknownProducerId`).
- **InvalidTxnState (22)**.
- Protocol / constructor errors.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt / `_redirect_to_leader` change | Frozen (v0.65 already shipped) |
| Auth (v0.106) / SCRAM (v0.108) wrap | Separate slices |
| `_admin_round_trip` | Frozen (v0.103 already retries) |
| Kafka `retries` / FindCoordinator | Native opcodes only; no Kafka API keys |
| Broker / protocol / Rust client | Frozen |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same transient budget as produce/fetch (independent of redirect):

- Default `max_retries=0`: first DeleteRecords transient 7 raises; one
  DeleteRecords RPC.
- `max_retries=2`, backoff 0: first DeleteRecords Timeout then ok → two
  DeleteRecords RPCs, success (no Metadata).
- First DeleteRecords **13** then Metadata then ok on leader: still the
  v0.65 redirect path; not counted as a retry (`max_retries=0` still
  succeeds).
- First DeleteRecords **2** (not-found) raises immediately; no retry
  (even with `max_retries=2`).
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` DeleteRecords RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

Redirect budget is unchanged (`1 + max_redirects`, default 1).
`max_redirects=0` still raises on the first 13 (no Metadata).

## API

No new public methods. Existing DeleteRecords now shares produce/fetch
`max_retries`:

```python
c = Client("127.0.0.1:9092", max_retries=0, retry_backoff_ms=50)
c.max_retries = 3
c.delete_records("t", 0, 100)
```

```go
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
c.DeleteRecords("t", 0, 100)
```

```java
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
c.deleteRecords("t", 0, 100);
```

Default is **0 extra attempts**. Error 13 still follows
`max_redirects` only.

## Tests

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first DeleteRecords 7 | raise 7; one RPC |
| `max_retries=2`, backoff 0, first DeleteRecords 7 then 0 | success; two DeleteRecords RPCs (no Metadata) |
| first DeleteRecords 13 then Metadata then ok on leader | still v0.65 redirect path; 13 is not a retry |
| first DeleteRecords 2 (even with `max_retries=2`) | raise 2 immediately; one RPC |
| Exhaust always-7 with `max_retries=2` | raise 7 after 3 RPCs |
| Existing `max_redirects=0` + first 13 | still raises 13; no Metadata |

Existing 13 redirect tests must still pass.

## Honesty leftovers

- **Not Kafka** `retries` / FindCoordinator.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- Error **13** is still redirect-only (`max_redirects`).
- `_redirect_to_leader` hunt is unchanged (v0.65).
- Auth / SCRAM / `_admin_round_trip` are unchanged.
- Rust `volant-client` DeleteRecords is unchanged (language clients
  only).

## Merge notes

Sibling slices **v0.106 / v0.108** also edit `Client`. When merging:

- **Keep the DeleteRecords retry wrap** (Python `delete_records`, Go
  `DeleteRecords` / `DeleteRecordsWithWaitFlag`, Java `deleteRecords`)
  and the tests next to the existing 13 redirect cases.
- Do not change `_redirect_to_leader` / `redirectToLeader` hunt logic
  (that is v0.65).
- Do not wrap Auth (v0.106) or SCRAM (v0.108).
- Do not wrap `_admin_round_trip`.
- Do not drop Produce/Fetch error-13 loops or the v0.72 / v0.85 /
  v0.89 / v0.91 / v0.93 / v0.103 admin wraps.
- Do not change the broker, Kafka shim, or Rust client in this merge.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`delete_records`)
- Go `clients/go/client.go` (`DeleteRecords` / `DeleteRecordsWithWaitFlag`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`deleteRecords`)

The hunk is local to `delete_records` / `DeleteRecords`.

## Related

- [V65_SPEC.md](./V65_SPEC.md) — DeleteRecords 13 redirect leftover
  this extends
- [V103_SPEC.md](./V103_SPEC.md) — admin_round_trip transient retry
- [V105_SPEC.md](./V105_SPEC.md) — OffsetCommit / OffsetFetch 14
- [V78_SPEC.md](./V78_SPEC.md) — offset-admin retry
- [V61_SPEC.md](./V61_SPEC.md) — produce retry
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
