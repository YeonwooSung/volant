# v0.120 — Rust ListMembers NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V96_SPEC.md](./V96_SPEC.md): Rust
`list_members` uses `metadata_list_members_round_trip`, which retries
transient 6/7/15/16 but does **not** redirect on error **14**
(`NotController`). The crate-doc / method comment claimed “Admin-14
redirect inherits” — that was **not** true. Language `list_members`
14 is a later sibling.

Wrap **only** `list_members`. Do **not** add 14 to
`metadata_list_members_round_trip`, or `metadata()` / `metadata_topics`
would start hunting on 14. Metadata is not controller-gated the same
way.

Reuse existing `redirect_to_controller` + `max_redirects` (v0.79).
Keep existing transient retry (`max_retries`, default **0**) via the
v0.96 helper. 14 uses an independent `redirect_attempt` counter.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / language clients.

## Goals

1. On ListMembers typed `error_code == 14` or
   `Response::Error { code: 14 }`: if
   `redirect_attempt + 1 < 1 + max_redirects` and
   `redirect_to_controller` returns true, resend ListMembers.
2. Parse `controller_id=N` from a `Response::Error` message the same
   way other 14 wraps do. Typed 14 has no hint (`None`).
3. 14 uses **redirect budget** (`max_redirects`), not `max_retries`.
   14 does **not** increment `retry_attempt`. Transient 6 / 7 / 15 /
   16 stay on `max_retries` (existing helper).
4. Budget is the same as produce/fetch / v0.79 admin:
   `1 + max_redirects` (default `max_redirects=1`).
   `max_redirects=0` surfaces 14 with no Metadata.
5. **Not redirected:** 13, 2, 9 / 10 / 11, 17 / 18, 21, 22, Protocol.
6. No new public methods. Existing `list_members` signature stays.
7. Do **not** wrap `metadata()` / `metadata_topics`.
8. Do **not** change DeleteRecords, ListOffsets, Auth, or broker /
   protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (`redirect_to_controller` as-is) |
| Language `list_members` 14 | Later sibling |
| `metadata()` / `metadata_topics` 14 | Metadata is not controller-gated |
| Adding 14 to `metadata_list_members_round_trip` | Would make Metadata hunt |
| DeleteRecords / ListOffsets / Auth | Out of scope |
| Kafka `FindCoordinator` / API keys | Native ListMembers only |
| Broker / protocol | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

Same redirect budget as Produce/Fetch / v0.79 admin; independent of
the v0.96 transient retry budget:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- ListMembers may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`
  or a typed `Response::ListMembers { error_code: 14, .. }` with no
  id.
- Transient 6 / 7 / 15 / 16 and `Error::Io` still retry on
  `max_retries` (default 0) as in v0.96. A 7-then-0 ListMembers
  still succeeds in two RPCs with `max_retries >= 1` and no
  Metadata.
- Error **13** / **2** / **9** / **10** / **11** / **17** / **18** /
  **21** / **22** and protocol are not retried and not redirected
  here.

Hunt is unchanged (existing helper). Message hint wins; otherwise
Metadata.controller_id when non-zero; otherwise the first other
advertised broker.

## API

No new public methods. Existing:

```rust
client.list_members().await?;
```

Error 14 now follows Produce/Fetch redirect budget. Transient 7 still
uses `max_retries`. Not Kafka FindCoordinator.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Tokio TCP stub (no live broker):

| Case | Expect |
|------|--------|
| First ListMembers 14 + `controller_id=2`; Metadata names node 2; second ok | success; 14 is redirect not retry |
| Typed 14 (no hint); Metadata has another advertised broker; second ok | success |
| `max_redirects=0` + first 14 | 14; no Metadata |
| `max_retries=2`, first 7 then 0 | two ListMembers, no Metadata |

`v96_metadata_list_members_retry.rs` must still pass (`metadata()`
must **not** start redirecting on 14).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | 14 arm inside `list_members` |
| `crates/volant-client/src/lib.rs` | crate-doc note; fix “14 inherits” |
| `crates/volant-client/src/config.rs` | `max_redirects` comment: ListMembers 14 |
| `crates/volant-client/tests/v120_list_members_14.rs` | queued-code stub |
| `docs/V120_SPEC.md` | This spec |

## Honesty leftovers

- Redirect still uses the existing `redirect_to_controller` helper
  (Metadata brokers or ListMembers on a hinted id miss). Hunt is
  unchanged. The hunt calls a private no-14 ListMembers path
  (`list_members_rpc`) so `list_members` and the helper are not
  mutually recursive `async fn`s (E0733). Hinted-id Metadata miss
  therefore does not re-enter the 14 wrap.
- Not Kafka `FindCoordinator`.
- Default `max_retries` stays **0**.
- `metadata()` / `metadata_topics` are not wrapped.
- Language clients are a later sibling.
- Broker / protocol still do not change whether ListMembers returns
  14 today.
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc / `config.rs`
should keep this wrap local to `list_members`:

- **Keep the 14 arm inside `list_members`**. Do not drop the v0.96
  transient retry (`metadata_list_members_round_trip`).
- Do **not** add 14 to `metadata_list_members_round_trip`.
- Do not wrap `metadata()` / `metadata_topics`.
- Do not change language clients, broker, or protocol.
- Do not change DeleteRecords, ListOffsets, or Auth.

Expect conflicts on:

- `crates/volant-client/src/lib.rs` (crate-doc)
- `crates/volant-client/src/config.rs` (`max_redirects` comment)
- hunk is otherwise local to `list_members`

## Related

- [V96_SPEC.md](./V96_SPEC.md) — Rust Metadata / ListMembers retry
  leftover this extends
- [V98_SPEC.md](./V98_SPEC.md) — Rust DeleteOffsets 14
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin 14 redirect (did not wrap
  ListMembers)
- [V114_SPEC.md](./V114_SPEC.md) — Rust metadata topic filter
  (`metadata()` stays unwrapped)
- [V72_SPEC.md](./V72_SPEC.md) — language admin 14
