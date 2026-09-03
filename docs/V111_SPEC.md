# v0.111 — Rust DeleteRecords 13 redirect + transient retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V65_SPEC.md](./V65_SPEC.md) /
[V110_SPEC.md](./V110_SPEC.md): language DeleteRecords already
redirects on error **13** (`NotLeaderForPartition`) via
`_redirect_to_leader` / `max_redirects` (v0.65) and retries transient
**6 / 7 / 15 / 16** + TCP/IO via `max_retries` (v0.110). Rust
`delete_records_with_wait_flag` was a single `round_trip`.

Reuse existing `ClientConfig.max_retries` / `retry_backoff_ms`
(produce / fetch / heartbeat / offset-admin / ListOffsets /
LeaveGroup / DescribeGroup / ListGroups / Metadata / ListMembers /
BeginTxn / EndTxn / admin / Auth / SCRAM) and
`is_transient_error_code` / `is_transient_transport`. **13 stays on
`max_redirects`** via existing `redirect_to_leader`. Do **not** wrap
`list_offsets` (sibling v0.113) or `metadata` (sibling v0.114).

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or language clients (already shipped as
v0.65 + v0.110).

## Goals

1. Inside existing `delete_records_with_wait_flag` (`delete_records`
   inherits): extra attempts after the first on transient **6 / 7 /
   15 / 16** and `Error::Io`.
2. **13 stays on `max_redirects`** — not counted as a transient retry.
   Independent counters: on 13 redirect without incrementing
   `retry_attempt`; on transient increment `retry_attempt` and sleep
   (and do not consume the redirect budget). Catch typed `error_code`
   and `Response::Error` / `Error` from `round_trip`.
3. If `redirect_to_leader` fails / already on leader /
   `max_redirects` exhausted, surface 13.
4. **Not retried / not redirected here:** 14, 9 / 10 / 11, 2, 17 / 18,
   21, 22, Protocol.
5. Default `max_retries=0` so existing DeleteRecords callers stay
   valid. Default `max_redirects` stays 1 extra.
6. Sleep via `retry_backoff_ms` (0 allowed in tests).
7. `wait_majority` trailer (0 / 1 / 2) is unchanged. Retry / redirect
   resends the same request.
8. No new public methods. Wrap `delete_records_with_wait_flag` only.

## Transient errors

Match produce / fetch / heartbeat / offset-admin / ListOffsets /
LeaveGroup / DescribeGroup / ListGroups / Metadata / ListMembers /
BeginTxn / EndTxn / admin / Auth / SCRAM and
`is_transient_error_code`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** `Error::Io` from the TCP layer (same as produce).
Today `delete_records_with_wait_flag` does `round_trip(...).await?` —
this slice catches transient transport instead of `?`.

**Not retried / not redirected here:**

- Error **13** (`NotLeaderForPartition`) — stays on `max_redirects`
  via `redirect_to_leader`. Independent of `retry_attempt`.
- Error **14** (`NotController`).
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Error **17** / **18**.
- Error **21** (`UnknownProducerId`).
- **InvalidTxnState (22)**.
- Protocol / constructor errors.
- `list_offsets` / `metadata` (sibling residuals).

## Non-goals

| Deferred | Why |
|----------|-----|
| Language DeleteRecords 13 / transient | Already shipped (v0.65 / v0.110) |
| Hunt / `redirect_to_leader` change | Frozen (existing helper) |
| Wrapping `list_offsets` | Sibling residual (v0.113) |
| Wrapping `metadata` | Sibling residual (v0.114) |
| Kafka `retries` / FindCoordinator | Native opcodes only; no Kafka API keys |
| Broker / protocol | Frozen |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same transient budget as produce/fetch (independent of redirect):

- Default `max_retries=0`: first DeleteRecords transient 7 raises; one
  DeleteRecords RPC.
- `max_retries=2`, backoff 0: first DeleteRecords Timeout then ok → two
  DeleteRecords RPCs, success (no Metadata).
- First DeleteRecords **13** then Metadata then ok on leader: redirect
  path; not counted as a retry (`max_retries=0` still succeeds).
- First DeleteRecords **2** (not-found) raises immediately; no retry
  (even with `max_retries=2`).
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` DeleteRecords RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

Redirect budget is unchanged (`1 + max_redirects`, default 1).
`max_redirects=0` still raises on the first 13 (no Metadata).
If the helper fails, the client is already on the leader, or the
budget is exhausted, surface 13.

## API

No new public methods. Existing DeleteRecords now shares produce/fetch
`max_retries` and Produce/Fetch `max_redirects` for error 13:

```rust
let c = Client::connect(ClientConfig {
    brokers: vec!["127.0.0.1:9092".into()],
    max_retries: 3,
    retry_backoff_ms: 50,
    ..ClientConfig::default()
}).await?;
c.delete_records("t", 0, 100).await?;
c.delete_records_with_wait_flag("t", 0, 100, 1).await?;
```

Default is **0 extra retry attempts**. Error 13 still follows
`max_redirects` only.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first DeleteRecords 7 | raise 7; one RPC |
| `max_retries=2`, backoff 0, first DeleteRecords 7 then 0 | success; two DeleteRecords RPCs (no Metadata) |
| first DeleteRecords 13 then Metadata then ok on leader | redirect path; 13 is not a retry (`max_retries=0` still succeeds) |
| first DeleteRecords 2 (even with `max_retries=2`) | raise 2 immediately; one RPC |
| Exhaust always-7 with `max_retries=2` | raise 7 after 3 RPCs |
| Existing `max_redirects=0` + first 13 | still raises 13; no Metadata |

## Honesty leftovers

- **Not Kafka** `retries` / FindCoordinator.
- **Default 0** (same as language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- Error **13** is still redirect-only (`max_redirects`).
- `redirect_to_leader` hunt is unchanged.
- Language clients are unchanged (already v0.65 + v0.110).
- `list_offsets` / `metadata` are unchanged (sibling residuals).

## Merge notes

Sibling slices **v0.113** (`list_offsets`) and **v0.114** (`metadata`)
also edit `client.rs`. When merging:

- **Keep the DeleteRecords wrap** inside
  `delete_records_with_wait_flag` (and the inherit via
  `delete_records`) plus
  `crates/volant-client/tests/v111_delete_records_retry.rs`.
- Do not change `redirect_to_leader` hunt logic.
- Do not wrap `list_offsets` (v0.113) or `metadata` (v0.114).
- Do not drop Produce/Fetch error-13 loops or the v0.72 / v0.85 /
  v0.89 / v0.91 / v0.93 / v0.103 / v0.104 admin wraps.
- Do not change the broker, Kafka shim, or language clients in this
  merge.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `delete_records_with_wait_flag` (siblings wrap `list_offsets` /
  `metadata`)
- `crates/volant-client/src/lib.rs` / `config.rs` comments that list
  which RPCs share `max_retries`

## Related

- [V110_SPEC.md](./V110_SPEC.md) — language DeleteRecords transient
  retry this mirrors
- [V65_SPEC.md](./V65_SPEC.md) — language DeleteRecords 13 redirect
- [V104_SPEC.md](./V104_SPEC.md) — admin_round_trip transient retry
- [V98_SPEC.md](./V98_SPEC.md) — offset-admin 14 redirect
- [V61_SPEC.md](./V61_SPEC.md) — produce retry
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
