# v0.100 — Rust BeginTxn / EndTxn retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V57_SPEC.md](./V57_SPEC.md) /
produce retry: language BeginTxn / EndTxn is a sibling residual
(v0.99). Rust `begin_transaction` / `end_transaction`
(`crates/volant-client/src/client.rs`) are a single `round_trip`.
`commit_transaction` / `abort_transaction` and
[`TransactionalProducer`] inherit via `end_transaction` /
`begin_transaction`.

Reuse `ClientConfig.max_retries` / `retry_backoff_ms` (produce /
heartbeat / offset-admin / ListOffsets / LeaveGroup / DescribeGroup /
ListGroups) and `is_transient_error_code` / `is_transient_transport`.
No new public methods. Do **not** wrap `metadata`, `delete_offsets`,
`ensure_producer_id`’s unknown-pid path, or `produce`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients (sibling
v0.99).

## Goals

1. Extra BeginTxn / EndTxn attempts after the first on **transient**
   errors only. Budget is independent of `max_redirects`.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   ListOffsets / LeaveGroup / DescribeGroup / ListGroups /
   `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: IO errors that produce already retries
     (`is_transient_transport` — `Error::Io`)
3. **Not retried here:**
   - Error **13** / **14**
   - Error **9** / **10** / **11**
   - Error **2** (NotFound)
   - Protocol / constructor errors
   - **InvalidTxnState** (`ErrorCode::InvalidTxnState`, 22)
   - Fence / epoch / abortable txn errors if they appear on these
     RPCs (`InvalidProducerEpoch` 19, `OutOfOrderSequence` 20,
     `UnknownProducerId` 21, `TransactionAbortable` 24)
   - `metadata` / `delete_offsets` / `ensure_producer_id` unknown-pid
     / `produce`
4. Default `max_retries=0` so existing txn tests stay valid (no extra
   BeginTxn / EndTxn attempts).
5. Sleep between retry attempts using `retry_backoff_ms` (0 allowed in
   tests).
6. No new public methods. Existing `begin_transaction` /
   `commit_transaction` / `abort_transaction` signatures stay.
   `end_transaction` is wrapped; commit / abort inherit.

If BeginTxn already succeeded on the broker, a retried BeginTxn may
return InvalidTxnState — raise immediately (no extra retries).

## Transient errors

Match produce / fetch / heartbeat / offset-admin / ListOffsets /
LeaveGroup / DescribeGroup / ListGroups and `crates/volant-client`
`is_transient_error_code`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** `Error::Io` from the TCP layer (same as produce /
heartbeat).

**Not retried here:**

- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Protocol / constructor errors.
- **InvalidTxnState** (22). A second BeginTxn after the first already
  opened the txn on the broker is a state error, not a blip.
- Fence / epoch / abortable: **19** / **20** / **21** / **24**.
- `metadata` / `delete_offsets` / `ensure_producer_id` unknown-pid /
  `produce`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Language BeginTxn / EndTxn retry | Sibling residual (v0.99) |
| Metadata / DeleteOffsets / produce / InitProducerId retry changes | Already shipped or not this slice |
| Kafka `retries` / BeginTxn vN | Native opcodes 50–53 only; not Kafka |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping `metadata` / `delete_offsets` / `ensure_producer_id` / `produce` | Explicitly out of scope |

## API

Existing BeginTxn / EndTxn signatures and constructors are unchanged.
These RPCs now share produce / heartbeat knobs:

```rust
Client::connect(ClientConfig {
    transactional_id: Some("txn-1".into()),
    max_retries: 0,        // default
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
client.begin_transaction().await?;
client.commit_transaction(Vec::new()).await?;
client.abort_transaction().await?;
```

Default is **0 extra attempts**. `TransactionalProducer::begin` /
`commit` / `abort` call `Client::begin_transaction` /
`commit_transaction` / `abort_transaction` and inherit.

## Semantics

Same budget as produce / heartbeat (independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one BeginTxn
  RPC.
- `max_retries=2`, backoff 0: EndTxn Timeout then ok → two EndTxn
  RPCs, commit success.
- First BeginTxn **InvalidTxnState** (22) raises immediately; one
  RPC. A retried BeginTxn that hits 22 because the first attempt
  already opened the txn also raises immediately.
- Exhausted retries: always 7 on EndTxn with `max_retries=2` → raise
  7 after `1 + max_retries` EndTxn RPCs.
- Transport fail then ok with `max_retries >= 1` → success.
- `ensure_producer_id` (InitProducerId) is still a single
  `round_trip` (unknown-pid path not wrapped).

## Tests

Tiny protocol stub that answers InitProducerId and queues BeginTxn /
EndTxn error codes:

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first BeginTxn 7 | one RPC; 7 surfaced |
| `max_retries=2`, backoff 0, first EndTxn 7 then 0 | commit success; two EndTxn RPCs |
| first BeginTxn InvalidTxnState | immediately that error; one RPC |
| Exhaust always-7 on EndTxn | 7 after `1+max_retries` RPCs |

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `begin_transaction` / `end_transaction` retry loops |
| `crates/volant-client/src/config.rs` | comments: knobs also apply to BeginTxn / EndTxn |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v100_begin_end_txn_retry.rs` | queued-code stub |
| `docs/V100_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** `retries` / BeginTxn / EndTxn versions.
- **Default 0** (same as produce / heartbeat / language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- `metadata` / `delete_offsets` / `ensure_producer_id` / `produce`
  are unchanged.
- OffsetCommit / OffsetFetch / DeleteOffsets already retried (v0.83).
- ListOffsets already retried (v0.84).
- Heartbeat already retried (v0.80).
- LeaveGroup already retried (v0.87).
- DescribeGroup / ListGroups already retried (v0.92).
- Language BeginTxn / EndTxn retry is a sibling residual (v0.99).
- Error 13 / 14 are not redirected here.
- Not a fully concurrent producer. One TCP connection.

## Merge notes

Siblings **v0.96** / **v0.98** also edit `client.rs`. Keep this hunk
local to `begin_transaction` / `end_transaction`. Do **not** wrap
`metadata`, `delete_offsets`, `ensure_producer_id`’s unknown-pid
path, or `produce`. Reuse `is_transient_error_code` /
`is_transient_transport` and the existing backoff field.

## Related

- [V57_SPEC.md](./V57_SPEC.md) — language BeginTxn / EndTxn leftover
  this extends
- [V92_SPEC.md](./V92_SPEC.md) — Rust DescribeGroup / ListGroups retry
- [V87_SPEC.md](./V87_SPEC.md) — Rust LeaveGroup retry
- [V84_SPEC.md](./V84_SPEC.md) — Rust ListOffsets retry
- [V83_SPEC.md](./V83_SPEC.md) — Rust offset-admin retry
- [V80_SPEC.md](./V80_SPEC.md) — Rust heartbeat retry
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
- [PHASE18_SPEC.md](./PHASE18_SPEC.md) — native BeginTxn / EndTxn
