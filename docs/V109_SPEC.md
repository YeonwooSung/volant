# v0.109 — Rust SCRAM handshake retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [TODO.md](../TODO.md) / produce
retry: Rust `authenticate_scram` (`crates/volant-client/src/client.rs`)
is a single first+final `round_trip` pair. Token Auth retry is a
sibling residual (v0.107). Language SCRAM handshake retry is a sibling
residual (v0.108).

Reuse `ClientConfig.max_retries` / `retry_backoff_ms` (produce /
heartbeat / offset-admin / ListOffsets / LeaveGroup / DescribeGroup /
ListGroups / Metadata / ListMembers / BeginTxn / EndTxn /
InitProducerId / controller-gated admin) and
`is_transient_error_code` / `is_transient_transport`. No new public
methods. Wrap the **entire handshake as one unit**. Do **not** wrap
`authenticate` (token Auth), `admin_round_trip`,
`ensure_producer_id`, or produce.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients (sibling
v0.108).

## Goals

1. Extra SCRAM handshake attempts after the first on **transient**
   errors only. Budget is independent of `max_redirects`.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   ListOffsets / LeaveGroup / DescribeGroup / ListGroups / Metadata /
   ListMembers / BeginTxn / EndTxn / InitProducerId /
   `admin_round_trip` / `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: IO errors that produce already retries
     (`is_transient_transport` — `Error::Io`)
3. If ScramFirst **or** ScramFinal fails transiently (typed
   `error_code` or `Response::Error`, or `Error::Io`), retry from
   **ScramFirst with a new `generate_client_nonce()`**. Do not reuse
   the old nonce / proof.
4. **Not retried here:**
   - Error **17** (`AuthenticationFailed`) / **18**
     (`AuthenticationRequired`)
   - Error **13** / **14**
   - Error **9** / **10** / **11**
   - Error **2** (NotFound)
   - Error **21** (`UnknownProducerId`) / **22** (`InvalidTxnState`)
   - Protocol (including server signature mismatch)
   - InvalidArgument
   - Token Auth (`authenticate`) / `admin_round_trip` /
     `ensure_producer_id` / produce
5. Default `max_retries=0` so existing Phase 22 SCRAM tests stay
   valid (no extra handshake attempts).
6. Sleep between retry attempts using `retry_backoff_ms` (0 allowed in
   tests).
7. No new public methods. `auth_token` still wins
   (`maybe_authenticate` order unchanged). `connect` / `reconnect`
   inherit via `maybe_authenticate`.

## Transient errors

Match produce / fetch / heartbeat / offset-admin / ListOffsets /
LeaveGroup / DescribeGroup / ListGroups / Metadata / ListMembers /
BeginTxn / EndTxn / InitProducerId / `admin_round_trip` and
`crates/volant-client` `is_transient_error_code`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** `Error::Io` from the TCP layer (same as produce /
heartbeat). Today `authenticate_scram` does `round_trip(...).await?`
on first and final — this slice catches transient transport instead of
`?`.

**Not retried here:**

- Error **17** (`AuthenticationFailed`) / **18**
  (`AuthenticationRequired`) — auth failure is not a transient
  timeout.
- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Error **21** (`UnknownProducerId`) / **22** (`InvalidTxnState`).
- Protocol / constructor errors, including server signature mismatch.
- InvalidArgument (proof construction).
- Token Auth (`authenticate`) / `admin_round_trip` /
  `ensure_producer_id` / produce.

## Non-goals

| Deferred | Why |
|----------|-----|
| Language SCRAM handshake retry | Sibling residual (v0.108) |
| Token Auth (`authenticate`) retry | Sibling residual (v0.107) |
| `admin_round_trip` / `ensure_producer_id` / produce | Already shipped or not this slice |
| Kafka `retries` / SaslHandshake / SaslAuthenticate | Native opcode only; not Kafka |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping `authenticate` / `admin_round_trip` / `ensure_producer_id` / produce | Explicitly out of scope |

## API

Existing connect / reconnect signatures and constructors are
unchanged. The SCRAM handshake now shares produce / heartbeat knobs:

```rust
Client::connect(ClientConfig {
    scram_username: Some("alice".into()),
    scram_password: Some("s3cret".into()),
    max_retries: 0,        // default
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
```

Default is **0 extra attempts**. `auth_token` still wins when both
token and SCRAM are set.

## Semantics

Same budget as produce / heartbeat (independent of redirect):

- Default `max_retries=0`: first transient 7 on ScramFirst raises;
  one first RPC, zero final.
- `max_retries=2`, backoff 0: first ScramFirst Timeout then full
  ok → two first RPCs, connect success.
- First ok, final Timeout, then full ok → handshake restarted (new
  nonce); two first RPCs, connect success.
- First ScramFirst **17** raises immediately; one first, zero final.
- Exhausted retries: always 7 on first with `max_retries=2` → raise
  7 after `1 + max_retries` first RPCs.
- Transport fail then ok with `max_retries >= 1` → success (new
  nonce).
- Token Auth / `admin_round_trip` / `ensure_producer_id` / produce
  retry loops are unchanged.

## Tests

Tiny protocol stub that queues ScramFirst / ScramFinal error codes.
Success replies echo the combined nonce, use a pinned salt / iteration
count, and return the server signature computed the same way as
`crates/volant-client/src/scram.rs` (`alice` / `s3cret`).

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first ScramFirst 7 | connect fails 7; one first, zero final |
| `max_retries=2`, backoff 0, first ScramFirst 7 then full ok | connect ok; two first RPCs |
| First ok, final 7, then full ok | connect ok; handshake restarted (two first RPCs) |
| First 17 | immediately 17; one first, zero final |

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `authenticate_scram` handshake retry loop |
| `crates/volant-client/src/config.rs` | comments: knobs also apply to SCRAM handshake |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v109_scram_handshake_retry.rs` | queued-code stub |
| `docs/V109_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** `retries` / SaslHandshake / SaslAuthenticate.
- **Default 0** (same as produce / heartbeat / language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- Token Auth (`authenticate`) is unchanged (sibling v0.107).
- Language SCRAM handshake retry is a sibling residual (v0.108).
- `admin_round_trip` / `ensure_producer_id` / produce are unchanged.
- Error 17 / 18 are not retried (auth failure).
- Error 13 / 14 are not redirected here.
- Not a fully concurrent client. One TCP connection.

## Merge notes

Sibling **v0.107** also edits `client.rs`. Keep this hunk local to
`authenticate_scram`. Do **not** wrap `authenticate`,
`admin_round_trip`, `ensure_producer_id`, or produce. Reuse
`is_transient_error_code` / `is_transient_transport` and the existing
backoff field. `maybe_authenticate` order is unchanged (`auth_token`
still wins).

## Related

- [V104_SPEC.md](./V104_SPEC.md) — Rust `admin_round_trip` retry
- [V102_SPEC.md](./V102_SPEC.md) — Rust InitProducerId retry
- [V100_SPEC.md](./V100_SPEC.md) — Rust BeginTxn / EndTxn retry
- [PHASE22_SPEC.md](./PHASE22_SPEC.md) — native SCRAM-SHA-256
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
