# v0.99 — BeginTxn / EndTxn retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V61_SPEC.md](./V61_SPEC.md) /
[V57_SPEC.md](./V57_SPEC.md) / [V63_SPEC.md](./V63_SPEC.md):
`max_retries` covers produce / fetch / heartbeat / offsets / Leave /
Describe / Metadata, but **BeginTxn (50/51)** and **EndTxn (52/53)**
are single-shot. `TransactionalProducer.commit` / `abort` call EndTxn.

Reuse those same knobs and the same transient set. No new constructor
args. Do **not** wrap `delete_offsets` or `metadata`. Do **not** change
produce retry or InitProducerId’s unknown-pid re-Init (error 21)
budget.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client`.

## Goals

1. Extra BeginTxn / EndTxn attempts after the first on **transient**
   errors only. Budget is independent of `max_redirects`.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   Leave / Describe / Metadata / Rust `is_transient_error_code`:
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
   - **InvalidTxnState** (native **22**)
   - Existing txn fence / epoch errors (19 / 20 / 21 / 24)
4. Default `max_retries=0` so existing BeginTxn / EndTxn tests stay
   valid (no extra RPCs).
5. Sleep between retry attempts using the existing produce/fetch
   backoff helper (`_sleep_produce_retry` / `sleepProduceRetry` /
   `Thread.sleep` via the same field).
6. No new public methods. Wrap existing:
   - `begin_transaction` / `BeginTransaction` / `beginTransaction`
   - `commit_transaction` / `CommitTransaction` / `commitTransaction`
   - `abort_transaction` / `AbortTransaction` / `abortTransaction`
     (the shared `_end_transaction` / `endTransaction` path)
   `TransactionalProducer.commit` / `abort` inherit via EndTxn.

## Transient errors

Match produce / fetch / heartbeat / offset-admin / Leave / Describe /
Metadata and `crates/volant-client` `is_transient_error_code`.

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
- **InvalidTxnState (22)** — BeginTxn retry after the broker already
  began can surface this; raise immediately.
- Txn fence / epoch: **19** (`InvalidProducerEpoch`), **20**
  (`OutOfOrderSequence`), **21** (`UnknownProducerId`; stays on the
  one re-Init budget, not this loop), **24** (`TransactionAbortable`).
- Protocol / constructor errors.
- `delete_offsets` / `metadata` (already retried elsewhere; do not
  re-wrap).

## Non-goals

| Deferred | Why |
|----------|-----|
| Produce retry / InitProducerId unknown-pid re-Init | Frozen; independent budget |
| Wrapping `delete_offsets` / `metadata` | Already retried (v0.78 / v0.95) |
| Kafka `retries` / EndTxn vN | Native opcodes only; not Kafka |
| Changing the broker / protocol / Rust client | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing BeginTxn / EndTxn signatures and constructors are unchanged.
These RPCs now share produce/fetch/heartbeat/offset-admin knobs:

```python
Client("127.0.0.1:9092", transactional_id="txn-1", max_retries=0, retry_backoff_ms=50)
c.max_retries = 3
c.begin_transaction()
c.commit_transaction()
c.abort_transaction()
```

```go
c.SetTransactionalID("txn-1")
c.SetMaxRetries(3)
c.SetRetryBackoff(50 * time.Millisecond) // 0 allowed in tests
c.BeginTransaction()
c.CommitTransaction(nil)
c.AbortTransaction()
```

```java
c.setTransactionalId("txn-1");
c.setMaxRetries(3);
c.setRetryBackoffMs(50);
c.beginTransaction();
c.commitTransaction();
c.abortTransaction();
```

Default is **0 extra attempts**. Go `Dial` / `DialTLS` / `DialAuth` and
Java `connect` / `connectTls` stay as they are.

## Semantics

Same budget as produce/fetch/heartbeat (independent of redirect):

- Default `max_retries=0`: first BeginTxn transient 7 raises; one
  BeginTxn RPC.
- `max_retries=2`, backoff 0: first EndTxn Timeout then ok → two
  EndTxn RPCs, commit/abort success.
- First BeginTxn **InvalidTxnState (22)** raises immediately; no retry.
- Exhausted retries: always 7 on EndTxn with `max_retries=2` → raise
  7 after `1 + max_retries` EndTxn RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

Honesty: a BeginTxn retry after the broker already began can surface
InvalidTxnState. That must **not** be retried; it raises immediately.

## Tests

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first BeginTxn 7 | raise 7; one BeginTxn RPC |
| `max_retries=2`, `retry_backoff_ms=0`, first EndTxn 7 then 0 | commit/abort success; two EndTxn RPCs |
| first BeginTxn InvalidTxnState (22) | raise immediately; no retry |
| Exhaust always-7 on EndTxn | raise 7 after `1+max_retries` EndTxn RPCs |

If BeginTxn tests need an existing `transactional_id` / InitProducerId
fake, reuse v0.57 / v0.63 scripted-broker patterns.

## Honesty leftovers

- **Not Kafka** `retries` / EndTxn versions.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- Produce retry and InitProducerId unknown-pid re-Init (error 21) are
  unchanged.
- `delete_offsets` / `metadata` wraps are unchanged (already retried).
- InvalidTxnState (22) after a successful-on-broker first BeginTxn is
  raised immediately (not retried).
- Rust `Client::begin_transaction` / `commit_transaction` /
  `abort_transaction` are unchanged (language clients only).

## Merge notes

Siblings that also edit `Client` (v0.97) should keep
produce/fetch/heartbeat/offset-admin/Leave/Describe/Metadata retry.
Only wrap `begin_transaction` / `BeginTransaction` / `beginTransaction`
and the shared `_end_transaction` / `endTransaction` path. Reuse
`_is_transient_broker` / `isTransientBroker` / `isTransientTransport`
and the existing backoff helper. Do **not** wrap `delete_offsets` or
`metadata`. Do **not** change produce retry or InitProducerId’s
unknown-pid re-Init.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`begin_transaction` /
  `_end_transaction`)
- Go `clients/go/client.go` (`BeginTransaction` / `endTransaction`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`beginTransaction` / `endTransaction`)
- Scripted brokers in `test_txn.py` / `txn_test.go` / `TxnTest.java`

## Related

- [V95_SPEC.md](./V95_SPEC.md) — Metadata / ListMembers retry leftover this extends
- [V90_SPEC.md](./V90_SPEC.md) — DescribeGroup / ListGroups retry
- [V86_SPEC.md](./V86_SPEC.md) — LeaveGroup retry
- [V78_SPEC.md](./V78_SPEC.md) — offset-admin retry
- [V74_SPEC.md](./V74_SPEC.md) — heartbeat retry
- [V66_SPEC.md](./V66_SPEC.md) — fetch retry
- [V63_SPEC.md](./V63_SPEC.md) — TransactionalProducer helper (calls EndTxn)
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V57_SPEC.md](./V57_SPEC.md) — BeginTxn / EndTxn on language clients
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
