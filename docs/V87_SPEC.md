# v0.87 — Rust LeaveGroup retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V74_SPEC.md](./V74_SPEC.md) /
TODO: Rust `Client::leave_group` (`crates/volant-client/src/client.rs`)
is a single `round_trip`. Language LeaveGroup retry is a sibling
residual. Match those semantics.

Reuse `ClientConfig.max_retries` / `retry_backoff_ms` (produce /
heartbeat / offset-admin / ListOffsets) and
`is_transient_error_code` / `is_transient_transport`. No new public
methods. Do **not** wrap `join_group`, `heartbeat`,
`create_scram_user`, or `add_broker`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients.

## Goals

1. Extra LeaveGroup attempts after the first on **transient** errors
   only. Budget is independent of `max_redirects`.
2. Same transient set as produce / heartbeat /
   `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: IO errors that produce already retries
     (`is_transient_transport` — `Error::Io`)
3. **Error 10 is success** (`UnknownMemberId` — already left).
   `check_ok` still fails any other non-zero.
4. **Not retried here:**
   - Error **9** (`RebalanceInProgress`) / **11** (`IllegalGeneration`)
   - Error **13** (`NotLeaderForPartition`) / **14** (`NotController`)
   - Error **2** (`NotFound`)
   - Protocol / constructor errors
   - JoinGroup / Heartbeat / CreateScramUser / AddBroker
5. Default `max_retries=0` so existing leave tests stay valid (no
   extra LeaveGroup attempts).
6. Sleep between retry attempts using `retry_backoff_ms` (0 allowed in
   tests).
7. No new public methods. Existing `leave_group` signature stays.
   `GroupConsumer::leave` inherits via `Client::leave_group`.

## Transient errors

Match produce / heartbeat / offset-admin / ListOffsets and
`crates/volant-client` `is_transient_error_code`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** `Error::Io` from the TCP layer (same as produce /
heartbeat).

**Success (not an error):**

- Error **10** (`UnknownMemberId`) — already left.

**Not retried here:**

- Error **9** (`RebalanceInProgress`) / **11** (`IllegalGeneration`).
- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  this slice is retry, not redirect.
- Error **2** (`NotFound`).
- Protocol / constructor errors.
- JoinGroup / Heartbeat / CreateScramUser / AddBroker.

## Non-goals

| Deferred | Why |
|----------|-----|
| Language LeaveGroup retry | Sibling residual |
| JoinGroup retry | Not idempotent the same way |
| Heartbeat / CreateScramUser / AddBroker | Already shipped or not this slice |
| Produce-batch / fetch retry changes | LeaveGroup only |
| Kafka `retries` / LeaveGroup vN | Native opcode 10 only; not Kafka |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing LeaveGroup signature and constructors are unchanged.
LeaveGroup now shares produce / heartbeat knobs:

```rust
Client::connect(ClientConfig {
    max_retries: 0,        // default
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
client.leave_group("g", member_id).await?
```

Non-zero codes still go through `check_ok` except **10**, which
returns `Ok(())`. Transient **7** is retried; after exhaust the
caller sees `error_code=7`. Rebalance **9** returns immediately.

Default is **0 extra attempts**. `GroupConsumer::leave` calls
`Client::leave_group` and inherits.

## Semantics

Same budget as produce / heartbeat (independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one LeaveGroup
  RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two LeaveGroup RPCs,
  success.
- First LeaveGroup **10** (unknown member) succeeds immediately; one
  RPC.
- `max_retries=2`: Timeout then **10** → two LeaveGroup RPCs,
  success.
- First LeaveGroup **9** (rebalance) raises immediately; no retry.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` LeaveGroup RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

## Tests

Tiny protocol stub that queues LeaveGroup error codes:

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first Leave 7 | one RPC; 7 surfaced |
| `max_retries=2`, backoff 0, first 7 then 0 | success; two RPCs |
| first Leave 10 | success; one RPC |
| `max_retries=2`, first 7 then 10 | success; two RPCs |
| first Leave 9 | immediately 9; one RPC |

Existing v44 leave tests must still pass.

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `leave_group` retry loop + 10 as success |
| `crates/volant-client/src/config.rs` | comments: knobs also apply to LeaveGroup |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v87_leave_group_retry.rs` | queued-code stub |
| `docs/V87_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** `retries` / LeaveGroup versions.
- **Default 0** (same as produce / heartbeat / language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- JoinGroup is unchanged (not idempotent the same way).
- Heartbeat already retried (v0.80). Offset admin / ListOffsets
  already retried (v0.83 / v0.84).
- Language LeaveGroup retry is a sibling residual.
- Not a fully concurrent consumer. One TCP connection.

## Merge notes

Sibling **v0.88** also edits `client.rs`. Keep this hunk local to
`leave_group` + the existing `is_transient_error_code` /
`is_transient_transport` helpers. Do **not** wrap `join_group`,
`heartbeat`, `create_scram_user`, or `add_broker`.

## Related

- [V74_SPEC.md](./V74_SPEC.md) — language Heartbeat retry leftover
  this extends (LeaveGroup was deferred there)
- [V80_SPEC.md](./V80_SPEC.md) — Rust heartbeat retry pattern this
  copies
- [V83_SPEC.md](./V83_SPEC.md) — Rust offset-admin retry
- [V84_SPEC.md](./V84_SPEC.md) — Rust ListOffsets retry
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V44_SPEC.md](./V44_SPEC.md) — group heartbeat / leave
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
