# v0.137 — Rust LeaveGroup NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V87_SPEC.md](./V87_SPEC.md) /
[V135_SPEC.md](./V135_SPEC.md): Rust `leave_group` already retries
transient 6/7/15/16 + Io (v0.87) and treats error **10** as success,
but treats **14** as not redirected. `heartbeat` already redirects on
14 (v0.135). Same honesty: the broker may not return 14 on LeaveGroup
today; this is client-side wrap only.

Add 14 redirect **inside** `leave_group` only so [`GroupConsumer::leave`]
inherits. Do **not** wrap `join_group`, `heartbeat`, or
`list_members`.

Reuse existing `redirect_to_controller` + `max_redirects` (v0.79).
Keep existing transient retry (`max_retries`, default **0**). 14 uses
an independent `redirects` counter and does **not** increment
`retry_attempt`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / language clients.

## Goals

1. On LeaveGroup typed `error_code == 14` or
   `Response::Error { code: 14 }`: if `redirects < max_redirects` and
   `redirect_to_controller` returns true, resend the same LeaveGroup.
2. Parse `controller_id=N` from a `Response::Error` message the same
   way other 14 wraps do. Typed 14 has no hint (`None`).
3. 14 uses **redirect budget** (`max_redirects`), not `max_retries`.
   14 does **not** increment `retry_attempt`. Transient 6 / 7 / 15 /
   16 stay on `max_retries` (existing v0.87 loop).
4. Budget is the same as Heartbeat v0.135:
   `redirects < max_redirects` (default `max_redirects=1`).
   `max_redirects=0` surfaces the first 14 with no Metadata.
5. **Error 10 stays success** (`UnknownMemberId` — already left)
   **before** 14 / transient handling.
6. **Not redirected / not retried:** 13, 2, 9, 11 (rebalance /
   illegal gen), 17 / 18, 21, 22, Protocol.
7. No new public methods. Existing `leave_group` signature stays.
   `GroupConsumer::leave` inherits via `Client::leave_group`.
8. Do **not** wrap `join_group`, `heartbeat`, or `list_members`.
9. Do **not** change language clients, broker, or protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (`redirect_to_controller` as-is) |
| Language LeaveGroup 14 | Sibling residual (do not change Python/Go/Java) |
| JoinGroup 14 | Out of scope (do not wrap) |
| Heartbeat 14 | Already v0.135 |
| `list_members` 14 | Already v0.120 |
| Kafka `FindCoordinator` / API keys | Native LeaveGroup only |
| Broker / protocol | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

Same redirect budget as Heartbeat v0.135; independent of the v0.87
transient retry budget:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: surface the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): surface the original 14.
- LeaveGroup may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`
  or a typed `Response::LeaveGroup { error_code: 14 }` with no id.
- Error **10** (`UnknownMemberId`) returns `Ok(())` immediately (one
  RPC). `check_ok` still fails any other non-zero that is not retried
  or redirected.
- Transient 6 / 7 / 15 / 16 and `Error::Io` still retry on
  `max_retries` (default 0) as in v0.87. A 7-then-0 LeaveGroup still
  succeeds in two RPCs with `max_retries >= 1` and no Metadata.
- Error **13** / **2** / **9** / **11** / **17** / **18** / **21** /
  **22** and protocol are not retried and not redirected here.
  Rebalance **9** / **11** still return immediately.

Hunt is unchanged (existing helper). Message hint wins; otherwise
Metadata.controller_id when non-zero; otherwise the first other
advertised broker.

## API

No new public methods. Existing:

```rust
Client::connect(ClientConfig {
    max_retries: 0,        // default; transient 6/7/15/16
    max_redirects: 1,      // default; error 14
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
client.leave_group("g", member_id).await?
```

Error 14 now follows Heartbeat / Produce/Fetch redirect budget.
Transient 7 still uses `max_retries`. Error 10 stays success. Not
Kafka FindCoordinator. GroupConsumer inherits via `Client::leave_group`.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Tokio TCP stub (no live broker):

| Case | Expect |
|------|--------|
| First LeaveGroup 14 + `controller_id=2`; Metadata names node 2; second ok | success; 14 is redirect not retry |
| Typed 14 (no hint); Metadata has another advertised broker; second ok | success |
| `max_redirects=0` + first 14 | 14; no Metadata |
| `max_retries=2`, first LeaveGroup **7** then 0 | two LeaveGroups, no Metadata |
| Error **10** | success, one LeaveGroup |
| Rebalance **9** with `max_retries=2` | immediately 9 |

`v87_leave_group_retry.rs` must still pass.

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | 14 arm inside `leave_group` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/src/config.rs` | `max_redirects` comment: LeaveGroup 14 |
| `crates/volant-client/tests/v137_leave_group_14.rs` | queued-code stub |
| `docs/V137_SPEC.md` | This spec |

## Honesty leftovers

- Redirect still uses the existing `redirect_to_controller` helper
  (Metadata brokers or ListMembers on a hinted id miss). Hunt is
  unchanged.
- Not Kafka `FindCoordinator`.
- Default `max_retries` stays **0**.
- JoinGroup / Heartbeat / `list_members` are not wrapped here.
- Language clients are a sibling residual (do not change Python/Go/Java).
- Broker / protocol still do not change whether LeaveGroup returns 14
  today.
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc / `config.rs`
should keep this wrap local to `leave_group`:

- **Keep the 14 arm inside `leave_group`**. Do not drop the v0.87
  transient retry. Keep error **10** as success **before** 14 /
  transient handling.
- Do **not** wrap `join_group`, `heartbeat`, or `list_members`.
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/lib.rs` (crate-doc)
- `crates/volant-client/src/config.rs` (`max_redirects` comment)
- hunk is otherwise local to `leave_group`

## Related

- [V87_SPEC.md](./V87_SPEC.md) — Rust LeaveGroup retry leftover this
  extends
- [V135_SPEC.md](./V135_SPEC.md) — Rust Heartbeat 14 pattern this copies
- [V125_SPEC.md](./V125_SPEC.md) — Rust DescribeGroup / ListGroups 14
- [V120_SPEC.md](./V120_SPEC.md) — Rust ListMembers 14
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin 14 redirect
- [V44_SPEC.md](./V44_SPEC.md) — group heartbeat / leave
