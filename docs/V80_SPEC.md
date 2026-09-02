# v0.80 — Rust GroupConsumer heartbeat retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V74_SPEC.md](./V74_SPEC.md): language
`heartbeat` already shares `max_retries` (default **0**). Rust
`Client::heartbeat` (`crates/volant-client/src/client.rs`) does a single
`round_trip`. A transient 7 / 6 / 15 / 16 or IO blip can expire a quiet
`GroupConsumer`.

Reuse `ClientConfig.max_retries` / `retry_backoff_ms` (produce) and
`is_transient_error_code` / `is_transient_transport`. No new public
methods. Do **not** retry JoinGroup / LeaveGroup.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients (they already
did this in v0.74).

## Goals

1. Extra Heartbeat attempts after the first on **transient** errors
   only. Budget is independent of `max_redirects`.
2. Same transient set as produce / `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: IO errors that produce already retries
     (`is_transient_transport` — `Error::Io`)
3. **Not retried here:**
   - **9** RebalanceInProgress / **10** UnknownMemberId / **11**
     IllegalGeneration — `HeartbeatResult` must still surface these so
     GroupConsumer rejoins
   - Error **13** / **14**
   - Protocol errors
   - JoinGroup / LeaveGroup
4. Default `max_retries=0` so existing heartbeat tests stay valid (no
   extra Heartbeat attempts).
5. Sleep between retry attempts using `retry_backoff_ms` (0 allowed in
   tests).
6. No new public methods. Existing `heartbeat` signature stays.
   GroupConsumer `poll` / background heartbeat inherit via
   `Client::heartbeat`.

## Transient errors

Match produce and `crates/volant-client` `is_transient_error_code`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** `Error::Io` from the TCP layer (same as produce).

**Not retried here:**

- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`) — membership must rejoin.
- Error **13** / **14**.
- Protocol / constructor errors.
- JoinGroup / LeaveGroup.

## Non-goals

| Deferred | Why |
|----------|-----|
| JoinGroup / LeaveGroup retry | Not idempotent the same way |
| Produce-batch / fetch retry changes | Heartbeat only |
| Language clients | Already have this (v0.74) |
| Kafka `retries` / Heartbeat vN | Native heartbeat only; not Kafka |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping `create_topic` | Sibling v0.79 admin 14 |

## API

Existing heartbeat signature and constructors are unchanged.
Heartbeat now shares produce knobs:

```rust
Client::connect(ClientConfig {
    max_retries: 0,        // default
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
client.heartbeat("g", member_id, generation).await?
```

`HeartbeatResult { error_code }` is still returned for non-zero codes
(no `check_ok`). Transient **7** is retried; after exhaust the caller
sees `Ok(HeartbeatResult { error_code: 7 })`. Rebalance **9** / **10**
/ **11** return immediately.

Default is **0 extra attempts**. `GroupConsumer` poll and the
background heartbeat task call `Client::heartbeat` and inherit.

## Semantics

Same budget as produce (independent of redirect):

- Default `max_retries=0`: first transient 7 surfaces; one Heartbeat
  RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two Heartbeat RPCs,
  success.
- First Heartbeat **9** (rebalance) returns 9 immediately; no retry.
  Same for 10 / 11.
- Exhausted retries: always 7 with `max_retries=2` → surface 7 after
  `1 + max_retries` Heartbeat RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

## Tests

Tiny protocol stub that queues Heartbeat error codes:

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first Heartbeat 7 | one RPC; 7 surfaced |
| `max_retries=2`, backoff 0, first 7 then 0 | success; two Heartbeat RPCs |
| first Heartbeat 9 | immediately 9; one RPC |
| Exhaust (always 7) | 7 after `1+max_retries` RPCs |

Existing v44 / v60 / v67 / v73 tests must still pass.

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `heartbeat` retry loop |
| `crates/volant-client/src/config.rs` | comments: knobs also apply to heartbeat |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v80_heartbeat_retry.rs` | queued-code stub |
| `docs/V80_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** `retries` / Heartbeat versions.
- **Default 0** (same as produce / language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- JoinGroup / LeaveGroup and other admin RPCs are unchanged.
- Language clients already have this (v0.74); this slice is Rust only.
- Non-zero Heartbeat codes still return `Ok(HeartbeatResult)` (no
  `check_ok`); language clients raise `BrokerError` on those codes.
- Not a fully concurrent consumer. One TCP connection.

## Merge notes

Sibling **v0.79** also edits `client.rs` (admin 14). Keep this hunk
local to `heartbeat` + the existing `is_transient_error_code` /
`is_transient_transport` helpers. Do **not** wrap `create_topic`.

## Related

- [V74_SPEC.md](./V74_SPEC.md) — language Heartbeat retry leftover this closes
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V66_SPEC.md](./V66_SPEC.md) — fetch retry
- [V44_SPEC.md](./V44_SPEC.md) — group heartbeat
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
