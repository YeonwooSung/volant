# v0.107 — Rust Auth (shared-token) retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from produce / `ensure_producer_id`
retry (v0.102): Rust `authenticate` (`crates/volant-client/src/client.rs`)
is a single `round_trip(Request::Auth)`. `connect` / `reconnect` call
`maybe_authenticate` → `authenticate` when `auth_token` is set. Auth
itself is not retried. Language Auth retry is a sibling residual
(v0.106). SCRAM (`authenticate_scram`) is a sibling residual (v0.109).

Reuse `ClientConfig.max_retries` / `retry_backoff_ms` (produce /
heartbeat / offset-admin / ListOffsets / LeaveGroup / DescribeGroup /
ListGroups / Metadata / ListMembers / BeginTxn / EndTxn /
InitProducerId / controller-gated admin) and
`is_transient_error_code` / `is_transient_transport`. No new public
methods. Wrap `authenticate` only. Do **not** wrap
`authenticate_scram`, `admin_round_trip`, `ensure_producer_id`,
`produce`, or DeleteRecords.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients (sibling
v0.106).

## Goals

1. Extra Auth attempts after the first on **transient** errors only.
   Budget is independent of `max_redirects`. Each `authenticate` call
   (`connect` + `reconnect`) has its own retry budget.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   ListOffsets / LeaveGroup / DescribeGroup / ListGroups / Metadata /
   ListMembers / BeginTxn / EndTxn / InitProducerId /
   `admin_round_trip` / `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: IO errors that produce already retries
     (`is_transient_transport` — `Error::Io`)
3. **Not retried here:**
   - Error **17** (`AuthenticationFailed`) / **18**
     (`AuthenticationRequired`)
   - Error **13** / **14**
   - Error **9** / **10** / **11**
   - Error **2** (NotFound)
   - Error **21** (`UnknownProducerId`)
   - **InvalidTxnState** (22)
   - Protocol / InvalidArgument
   - `authenticate_scram` / `admin_round_trip` / `ensure_producer_id`
     / produce / DeleteRecords
4. Default `max_retries=0` so existing auth / connect tests stay
   valid (no extra Auth attempts).
5. Sleep between retry attempts using `retry_backoff_ms` (0 allowed in
   tests).
6. No new public methods. Existing `connect` / `reconnect` /
   `connect_with_auth` signatures stay. `maybe_authenticate` is
   unchanged; it still calls `authenticate` when `auth_token` is set.
7. Already-connected clients skip Auth. No token → no Auth RPC.

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
heartbeat). Today `authenticate` does
`self.round_trip(Request::Auth { token }).await?` — this slice
catches transient transport instead of `?`.

**Not retried here:**

- Error **17** (`AuthenticationFailed`) / **18**
  (`AuthenticationRequired`) — permanent auth failure.
- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Error **21** (`UnknownProducerId`).
- **InvalidTxnState** (22).
- Protocol / InvalidArgument.
- `authenticate_scram` / `admin_round_trip` / `ensure_producer_id` /
  produce / DeleteRecords.

## Non-goals

| Deferred | Why |
|----------|-----|
| Language Auth retry | Sibling residual (v0.106) |
| SCRAM (`authenticate_scram`) retry | Sibling residual (v0.109) |
| `admin_round_trip` / `ensure_producer_id` / produce / DeleteRecords | Already shipped or not this slice |
| Kafka `retries` / SaslHandshake / SaslAuthenticate | Native opcode only; not Kafka |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping `maybe_authenticate` | Dispatch only; retry lives in `authenticate` |

## API

Existing connect / reconnect signatures and constructors are
unchanged. Shared-token Auth now shares produce / heartbeat knobs:

```rust
Client::connect(ClientConfig {
    auth_token: Some("tok".into()),
    max_retries: 0,        // default
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
```

Default is **0 extra attempts**. `connect` / `reconnect` /
`connect_with_auth` call `maybe_authenticate` → `authenticate` when
`auth_token` is set and inherit.

## Semantics

Same budget as produce / heartbeat (independent of redirect):

- Default `max_retries=0`: first Auth 7 raises; one Auth RPC.
- `max_retries=2`, backoff 0: Auth Timeout then ok → two Auth RPCs,
  connect success.
- First Auth **17** raises immediately; one RPC.
- Exhausted retries: always 7 on Auth with `max_retries=2` → raise
  7 after `1 + max_retries` Auth RPCs.
- Transport fail then ok with `max_retries >= 1` → success.
- No `auth_token` → no Auth RPC.
- Already-connected clients do not re-Auth until `reconnect`.
- Each `authenticate` call has its own retry budget (connect and
  reconnect do not share a counter).
- `authenticate_scram` / `admin_round_trip` / `ensure_producer_id` /
  produce / DeleteRecords retry loops are unchanged.

## Tests

Tiny protocol stub that queues Auth error codes. Drive Auth via
`auth_token` on `Client::connect` (no Metadata / Produce required).

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first Auth 7 | one RPC; 7 surfaced; connect fails |
| `max_retries=2`, backoff 0, first Auth 7 then 0 | connect ok; two Auth RPCs |
| first Auth 17 | immediately 17; one RPC |
| Exhaust always-7 | 7 after `1+max_retries` RPCs |
| No `auth_token` | no Auth RPC |

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `authenticate` retry loop |
| `crates/volant-client/src/config.rs` | comments: knobs also apply to Auth |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v107_auth_retry.rs` | queued-code stub |
| `docs/V107_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** `retries` / SaslHandshake / SaslAuthenticate.
- **Default 0** (same as produce / heartbeat / language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- `authenticate_scram` is unchanged (sibling v0.109).
- `admin_round_trip` / `ensure_producer_id` / produce / DeleteRecords
  are unchanged.
- Language Auth retry is a sibling residual (v0.106).
- Error 17 / 18 are not retried.
- Error 13 / 14 are not redirected here.
- Not a fully concurrent client. One TCP connection.

## Merge notes

Sibling **v0.109** also edits `client.rs`. Keep this hunk local to
`authenticate`. Do **not** wrap `authenticate_scram`,
`admin_round_trip`, `ensure_producer_id`, produce, or DeleteRecords.
Reuse `is_transient_error_code` / `is_transient_transport` and the
existing backoff field.

## Related

- [V102_SPEC.md](./V102_SPEC.md) — Rust InitProducerId retry leftover
  this extends
- [V104_SPEC.md](./V104_SPEC.md) — Rust `admin_round_trip` retry
- [V100_SPEC.md](./V100_SPEC.md) — Rust BeginTxn / EndTxn retry
- [V96_SPEC.md](./V96_SPEC.md) — Rust Metadata / ListMembers retry
- [V80_SPEC.md](./V80_SPEC.md) — Rust heartbeat retry
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
- [PHASE7_SPEC.md](./PHASE7_SPEC.md) — native shared-token Auth
