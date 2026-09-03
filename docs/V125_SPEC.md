# v0.125 — Rust DescribeGroup / ListGroups NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V92_SPEC.md](./V92_SPEC.md): Rust
`describe_group` / `list_groups` share `describe_list_groups_round_trip`,
which retries transient 6/7/15/16 + Io but does **not** redirect on
error **14** (`NotController`). `list_members` already has its own 14
wrap (v0.120). Language clients are a sibling residual (v0.124).

Add 14 redirect **inside** `describe_list_groups_round_trip` so both
`describe_group` and `list_groups` inherit. This helper is only used
by those two.

Reuse existing `redirect_to_controller` + `max_redirects` (v0.79).
Keep existing transient retry (`max_retries`, default **0**). 14 uses
an independent `redirects` counter and does **not** increment
`retry_attempt`.

Do **not** put 14 into `metadata_list_members_round_trip`, or
`metadata()` would start hunting on 14. Do **not** wrap `list_members`
(already v0.120) or `metadata()`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / language clients.

## Goals

1. On DescribeGroup / ListGroups typed `error_code == 14` or
   `Response::Error { code: 14 }`: if `redirects < max_redirects` and
   `redirect_to_controller` returns true, resend the same request.
2. Parse `controller_id=N` from a `Response::Error` message the same
   way other 14 wraps do. Typed 14 has no hint (`None`).
3. 14 uses **redirect budget** (`max_redirects`), not `max_retries`.
   14 does **not** increment `retry_attempt`. Transient 6 / 7 / 15 /
   16 stay on `max_retries` (existing helper).
4. Budget is the same as produce/fetch / v0.79 admin:
   `1 + max_redirects` (default `max_redirects=1`).
   `max_redirects=0` surfaces 14 with no Metadata.
5. **Not redirected:** 13, 2, 9 / 10 / 11, 17 / 18, 21, 22, Protocol.
6. No new public methods. Existing `describe_group` / `list_groups`
   signatures stay. Range assignor inherits via `describe_group`.
7. Do **not** wrap `list_members` or `metadata()` /
   `metadata_topics`.
8. Do **not** change language clients, broker, or protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (`redirect_to_controller` as-is) |
| Language DescribeGroup / ListGroups 14 | Sibling v0.124 |
| `list_members` 14 | Already v0.120 |
| `metadata()` / `metadata_topics` 14 | Metadata is not controller-gated |
| Adding 14 to `metadata_list_members_round_trip` | Would make Metadata hunt |
| DeleteRecords / ListOffsets / Auth | Out of scope |
| Kafka `FindCoordinator` / API keys | Native DescribeGroup / ListGroups only |
| Broker / protocol | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

Same redirect budget as Produce/Fetch / v0.79 admin; independent of
the v0.92 transient retry budget:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- DescribeGroup / ListGroups may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`
  or a typed `Response::DescribeGroup` / `Response::ListGroups` with
  `error_code: 14` and no id.
- Transient 6 / 7 / 15 / 16 and `Error::Io` still retry on
  `max_retries` (default 0) as in v0.92. A 7-then-0 DescribeGroup
  still succeeds in two RPCs with `max_retries >= 1` and no
  Metadata.
- Error **13** / **2** / **9** / **10** / **11** / **17** / **18** /
  **21** / **22** and protocol are not retried and not redirected
  here. DescribeGroup **2** (no live members) still fails immediately.

Hunt is unchanged (existing helper). Message hint wins; otherwise
Metadata.controller_id when non-zero; otherwise the first other
advertised broker.

## API

No new public methods. Existing:

```rust
client.describe_group("g").await?;
client.list_groups().await?;
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
| First DescribeGroup 14 + `controller_id=2`; Metadata names node 2; second ok | success; 14 is redirect not retry |
| ListGroups typed 14 (no hint); Metadata has another advertised broker; second ok | success |
| DescribeGroup `max_redirects=0` + first 14 | 14; no Metadata |
| `max_retries=2`, first DescribeGroup 7 then 0 | two RPCs, no Metadata |
| DescribeGroup first 2 with `max_retries=2` | immediately 2 |

`v92_describe_list_groups_retry.rs` and `v120_list_members_14.rs`
must still pass.

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | 14 arm inside `describe_list_groups_round_trip` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/src/config.rs` | `max_redirects` comment: DescribeGroup / ListGroups 14 |
| `crates/volant-client/tests/v125_describe_list_groups_14.rs` | queued-code stub |
| `docs/V125_SPEC.md` | This spec |

## Honesty leftovers

- Redirect still uses the existing `redirect_to_controller` helper
  (Metadata brokers or ListMembers on a hinted id miss). Hunt is
  unchanged.
- Not Kafka `FindCoordinator`.
- Default `max_retries` stays **0**.
- `list_members` and `metadata()` / `metadata_topics` are not wrapped.
- Language clients are a sibling residual (v0.124).
- Broker / protocol still do not change whether DescribeGroup /
  ListGroups returns 14 today.
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc / `config.rs`
should keep this wrap local to `describe_list_groups_round_trip`:

- **Keep the 14 arm inside `describe_list_groups_round_trip`**. Do not
  drop the v0.92 transient retry.
- Do **not** add 14 to `metadata_list_members_round_trip`.
- Do not wrap `list_members` or `metadata()` / `metadata_topics`.
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/lib.rs` (crate-doc)
- `crates/volant-client/src/config.rs` (`max_redirects` comment)
- hunk is otherwise local to `describe_list_groups_round_trip`

## Related

- [V92_SPEC.md](./V92_SPEC.md) — Rust DescribeGroup / ListGroups retry
  leftover this extends
- [V120_SPEC.md](./V120_SPEC.md) — Rust ListMembers 14 (do not share
  the metadata helper)
- [V98_SPEC.md](./V98_SPEC.md) — Rust DeleteOffsets 14
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin 14 redirect (did not wrap
  DescribeGroup / ListGroups)
- [V72_SPEC.md](./V72_SPEC.md) — language admin 14
