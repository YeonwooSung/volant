# v0.103 — admin_round_trip transient retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V72_SPEC.md](./V72_SPEC.md) /
[V85_SPEC.md](./V85_SPEC.md) / [V89_SPEC.md](./V89_SPEC.md) /
[V91_SPEC.md](./V91_SPEC.md) / [V93_SPEC.md](./V93_SPEC.md):
`_admin_round_trip` / `adminRoundTrip` redirects on **14**
(`NotController`) but does **not** retry transient **6 / 7 / 15 / 16**
or TCP/IO. CreateTopic / DeleteTopic / CreatePartitions / Reassign /
ACLs / SCRAM-admin / Add/RemoveBroker / Describe/AlterConfigs all
inherit that helper.

Reuse existing `max_retries` / `retry_backoff_ms` (produce / fetch /
heartbeat / offset-admin / ListOffsets / LeaveGroup / DescribeGroup /
ListGroups / Metadata / ListMembers / BeginTxn / EndTxn) and
`_is_transient_broker` / `isTransientBroker` / `isTransientTransport`.
**14 stays on `max_redirects`.** Do **not** rewrite
`_redirect_to_controller` / `redirectToController`. Do **not** change
`_ensure_producer_id` or OffsetCommit 14.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. Inside the existing admin helper (Python `_admin_round_trip`, Go
   `adminRoundTrip`, Java `adminRoundTrip`): extra attempts after the
   first on transient **6 / 7 / 15 / 16** and TCP/IO.
2. **14 stays on `max_redirects`** — not counted as a transient retry.
   After a successful redirect, the next send is a new attempt for both
   budgets. Independent counters: on 14 redirect without incrementing
   `retry_attempt`; on transient increment `retry_attempt` and sleep
   (and do not consume the redirect budget).
3. **Not retried:** 13, 9 / 10 / 11, Protocol, not-found (2), 21,
   InvalidTxnState (22).
4. Default `max_retries=0` so existing admin tests stay valid.
5. Sleep via the existing produce backoff helper
   (`_sleep_produce_retry` / `sleepProduceRetry`).
6. No new public methods. CreateTopic / DeleteTopic / CreatePartitions /
   Reassign / ACLs / SCRAM-admin / Add/RemoveBroker /
   Describe/AlterConfigs inherit via the helper.
7. Do **not** change `_ensure_producer_id` or OffsetCommit 14.
8. Do **not** rewrite `_redirect_to_controller`.

## Transient errors

Match produce / fetch / heartbeat / offset-admin / ListOffsets /
LeaveGroup / DescribeGroup / ListGroups / Metadata / ListMembers /
BeginTxn / EndTxn and `crates/volant-client` `is_transient_error_code`.

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

- Error **14** (`NotController`) — stays on `max_redirects`.
- Error **13** (`NotLeaderForPartition`).
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Error **21** (`UnknownProducerId`).
- **InvalidTxnState (22)**.
- Protocol / constructor errors.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt / `_redirect_to_controller` change | Frozen (v0.81 already shipped) |
| OffsetCommit / OffsetFetch 14 | Separate loops; not this slice |
| `_ensure_producer_id` / InitProducerId unknown-pid | Frozen |
| DeleteOffsets wrap | Already retries (v0.78) + redirects (v0.97) |
| Kafka `retries` / FindCoordinator | Native opcodes only; no Kafka API keys |
| Broker / protocol / Rust client | Frozen |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same transient budget as produce/fetch (independent of redirect):

- Default `max_retries=0`: first CreateTopic transient 7 raises; one
  CreateTopic RPC.
- `max_retries=2`, backoff 0: first CreateTopic Timeout then ok → two
  CreateTopic RPCs, success.
- First CreateTopic **14** then Metadata then ok: still the redirect
  path; not counted as a retry (`max_retries=0` still succeeds).
- First CreateTopic **2** (not-found) raises immediately; no retry.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` CreateTopic RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

Redirect budget is unchanged (`1 + max_redirects`, default 1).
`max_redirects=0` still raises on the first 14 (no Metadata).

## API

No new public methods. Existing controller-gated admin now shares
produce/fetch `max_retries`:

```python
c = Client("127.0.0.1:9092", max_retries=0, retry_backoff_ms=50)
c.max_retries = 3
c.create_topic("t", partitions=1)
c.create_acls([e])
```

```go
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
c.CreateTopic("t", 1)
c.CreateAcls(entries)
```

```java
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
c.createTopic("t", 1);
c.createAcls(entries);
```

Default is **0 extra attempts**. Error 14 still follows
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
| Default `max_retries=0`, first CreateTopic 7 | raise 7; one RPC |
| `max_retries=2`, backoff 0, first CreateTopic 7 then 0 | success; two CreateTopic RPCs |
| first CreateTopic 14 then Metadata then ok | still redirect path; not counted as retry |
| first CreateTopic 2 | raise immediately |
| Exhaust always-7 | raise 7 after `1+max_retries` RPCs |
| CreateAcls 7 then 0, `max_retries=2` | success; two CreateAcls RPCs |

Existing 14 redirect tests must still pass.

## Honesty leftovers

- **Not Kafka** `retries` / FindCoordinator.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- Error **14** is still redirect-only (`max_redirects`).
- `_redirect_to_controller` hunt is unchanged (v0.81).
- OffsetCommit / OffsetFetch still do not redirect on 14.
- `_ensure_producer_id` is unchanged.
- DeleteOffsets already had its own retry + 14 wrap (v0.78 / v0.97);
  this slice does not re-wrap it.
- Rust `volant-client` admin helpers are unchanged (language clients
  only).

## Merge notes

Sibling slices **v0.101 / v0.105** also edit `Client`. When merging:

- **Keep the admin helper retry** (Python `_admin_round_trip`, Go
  `adminRoundTrip`, Java `adminRoundTrip`) and the tests next to the
  existing 14 redirect cases.
- Do not change `_redirect_to_controller` / `redirectToController`
  hunt logic (that is v0.81).
- Do not change `_ensure_producer_id` or OffsetCommit 14.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops or the v0.72 /
  v0.85 / v0.89 / v0.91 / v0.93 admin wraps.
- Do not change the broker, Kafka shim, or Rust client in this merge.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`_admin_round_trip`)
- Go `clients/go/client.go` (`adminRoundTrip` + admin method wraps)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`adminRoundTrip`)
- Admin redirect tests (`test_admin_redirect.py` / `client_test.go` /
  `ClientTest.java`)

## Related

- [V99_SPEC.md](./V99_SPEC.md) — BeginTxn / EndTxn retry leftover this
  extends
- [V97_SPEC.md](./V97_SPEC.md) — DeleteOffsets 14 (already retries)
- [V93_SPEC.md](./V93_SPEC.md) — Describe/AlterConfigs 14
- [V91_SPEC.md](./V91_SPEC.md) — AddBroker / RemoveBroker 14
- [V89_SPEC.md](./V89_SPEC.md) — SCRAM-admin / ListAcls 14
- [V85_SPEC.md](./V85_SPEC.md) — SCRAM-admin / ListAcls 14 (Rust)
- [V81_SPEC.md](./V81_SPEC.md) — Metadata.controller_id hunt
- [V78_SPEC.md](./V78_SPEC.md) — offset-admin retry
- [V72_SPEC.md](./V72_SPEC.md) — admin 14 redirect leftover this
  extends
- [V61_SPEC.md](./V61_SPEC.md) — produce retry
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
