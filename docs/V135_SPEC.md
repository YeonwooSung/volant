# v0.135 — Rust Heartbeat NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V80_SPEC.md](./V80_SPEC.md): Rust
`heartbeat` retries transient 6/7/15/16 + Io but does **not** redirect
on error **14** (`NotController`). `describe_group` / `list_groups`
already have 14 (v0.125). `list_members` has 14 (v0.120). Language
clients are a sibling residual (v0.134).

Add 14 redirect **inside** `heartbeat` only so [`GroupConsumer`] poll
/ background heartbeat inherit. Do **not** wrap `leave_group`,
`join_group`, or `list_members`.

Reuse existing `redirect_to_controller` + `max_redirects` (v0.79).
Keep existing transient retry (`max_retries`, default **0**). 14 uses
an independent `redirects` counter and does **not** increment
`retry_attempt`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / language clients.

## Goals

1. On Heartbeat typed `error_code == 14` or
   `Response::Error { code: 14 }`: if `redirects < max_redirects` and
   `redirect_to_controller` returns true, resend the same Heartbeat.
2. Parse `controller_id=N` from a `Response::Error` message the same
   way other 14 wraps do. Typed 14 has no hint (`None`).
3. 14 uses **redirect budget** (`max_redirects`), not `max_retries`.
   14 does **not** increment `retry_attempt`. Transient 6 / 7 / 15 /
   16 stay on `max_retries` (existing v0.80 loop).
4. Budget is the same as produce/fetch / v0.79 admin:
   `1 + max_redirects` (default `max_redirects=1`).
   `max_redirects=0` surfaces 14 with no Metadata.
5. **Not redirected:** 13, 2, 9 / 10 / 11, 17 / 18, 21, 22, Protocol.
   Rebalance 9 / 10 / 11 is still not retried.
6. No new public methods. Existing `heartbeat` signature stays.
   `HeartbeatResult` return on typed codes stays (including non-zero
   that are not retried).
7. Do **not** wrap `leave_group`, `join_group`, or `list_members`.
8. Do **not** change language clients, broker, or protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (`redirect_to_controller` as-is) |
| Language Heartbeat 14 | Sibling v0.134 |
| LeaveGroup / JoinGroup 14 | Out of scope (do not wrap) |
| `list_members` 14 | Already v0.120 |
| Kafka `FindCoordinator` / API keys | Native Heartbeat only |
| Broker / protocol | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

Same redirect budget as Produce/Fetch / v0.79 admin; independent of
the v0.80 transient retry budget:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: surface the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): surface the original 14.
- Heartbeat may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`
  or a typed `Response::Heartbeat { error_code: 14 }` with no id.
- Typed non-zero codes still return `Ok(HeartbeatResult { error_code })`
  (no `check_ok`). `Response::Error` still returns `Err`.
- Transient 6 / 7 / 15 / 16 and `Error::Io` still retry on
  `max_retries` (default 0) as in v0.80. A 7-then-0 Heartbeat still
  succeeds in two RPCs with `max_retries >= 1` and no Metadata.
- Error **13** / **2** / **9** / **10** / **11** / **17** / **18** /
  **21** / **22** and protocol are not retried and not redirected
  here. Rebalance **9** / **10** / **11** still return immediately so
  GroupConsumer can rejoin.

Hunt is unchanged (existing helper). Message hint wins; otherwise
Metadata.controller_id when non-zero; otherwise the first other
advertised broker.

## API

No new public methods. Existing:

```rust
client.heartbeat("g", member_id, generation).await?;
```

Error 14 now follows Produce/Fetch redirect budget. Transient 7 still
uses `max_retries`. Not Kafka FindCoordinator. GroupConsumer inherits
via `Client::heartbeat`.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Tokio TCP stub (no live broker):

| Case | Expect |
|------|--------|
| First Heartbeat 14 + `controller_id=2`; Metadata names node 2; second ok | success; 14 is redirect not retry |
| Typed 14 (no hint); Metadata has another advertised broker; second ok | success |
| `max_redirects=0` + first 14 | 14; no Metadata |
| `max_retries=2`, first Heartbeat 7 then 0 | two Heartbeats, no Metadata |
| Rebalance 9 with `max_retries=2` | immediately 9 |

`v80_heartbeat_retry.rs` must still pass.

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | 14 arm inside `heartbeat` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/src/config.rs` | `max_redirects` comment: Heartbeat 14 |
| `crates/volant-client/tests/v135_heartbeat_14.rs` | queued-code stub |
| `docs/V135_SPEC.md` | This spec |

## Honesty leftovers

- Redirect still uses the existing `redirect_to_controller` helper
  (Metadata brokers or ListMembers on a hinted id miss). Hunt is
  unchanged.
- Not Kafka `FindCoordinator`.
- Default `max_retries` stays **0**.
- LeaveGroup / JoinGroup / `list_members` are not wrapped.
- Language clients are a sibling residual (v0.134).
- Broker / protocol still do not change whether Heartbeat returns 14
  today.
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc / `config.rs`
should keep this wrap local to `heartbeat`:

- **Keep the 14 arm inside `heartbeat`**. Do not drop the v0.80
  transient retry.
- Do **not** wrap `leave_group`, `join_group`, or `list_members`.
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/lib.rs` (crate-doc)
- `crates/volant-client/src/config.rs` (`max_redirects` comment)
- hunk is otherwise local to `heartbeat`

## Related

- [V80_SPEC.md](./V80_SPEC.md) — Rust Heartbeat retry leftover this
  extends
- [V125_SPEC.md](./V125_SPEC.md) — Rust DescribeGroup / ListGroups 14
- [V120_SPEC.md](./V120_SPEC.md) — Rust ListMembers 14
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin 14 redirect
- [V74_SPEC.md](./V74_SPEC.md) — language Heartbeat retry
