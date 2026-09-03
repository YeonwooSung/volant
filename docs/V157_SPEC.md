# v0.157 — Rust Metadata NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V96_SPEC.md](./V96_SPEC.md) /
[V114_SPEC.md](./V114_SPEC.md) / [V120_SPEC.md](./V120_SPEC.md): Rust
`metadata` / `metadata_topics` go through
`metadata_list_members_round_trip`, which retries transient 6/7/15/16
but does **not** redirect on error **14** (`NotController`).
`redirect_to_controller` currently calls public `metadata()`. Language
sibling is **v0.156**. Same honesty as Heartbeat 14 ([V135_SPEC.md](./V135_SPEC.md)):
native Metadata has no top-level error_code; 14 arrives as
`Response::Error`. Client wrap only.

Add 14 redirect **inside** `metadata` / `metadata_topics` only. Do
**not** add 14 to `metadata_list_members_round_trip` or change the
`list_members` 14 wrap (v0.120) / `list_members_rpc`.

Reuse existing `redirect_to_controller` + `max_redirects` (v0.79).
Keep existing transient retry (`max_retries`, default **0**) via the
v0.96 helper. 14 uses an independent `redirects` counter and does
**not** increment `retry_attempt`.

Hunt must call a **no-14** Metadata path (`metadata_rpc`) so hunt and
metadata 14 are not mutually recursive.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / language clients.

## Goals

1. Extract private `metadata_rpc(topics: Vec<String>)` that is
   today's `metadata_topics` body (decode + v0.96 retry, **no** 14
   wrap). Public `metadata_topics` wraps 14 around that path.
2. On Metadata `Response::Error { code: 14 }` (and transport-as-14 if
   that exists): if `redirects < max_redirects` and
   `redirect_to_controller(parse_controller_id)` succeeds, resend the
   same Metadata. Native Metadata has no typed `error_code`.
3. 14 uses **redirect budget** (`max_redirects`), not `max_retries`.
   14 does **not** increment `retry_attempt`. Transient 6 / 7 / 15 /
   16 stay on `max_retries` (existing v0.96 helper).
4. Budget is the same as Heartbeat v0.135:
   `redirects < max_redirects` (default `max_redirects=1`).
   `max_redirects=0` surfaces the first 14 with no extra hunt Metadata.
5. **`redirect_to_controller` must call `metadata_rpc(vec![])`**, not
   public `metadata()`. Keep `list_members_rpc` on a hinted id miss.
6. **Not redirected:** 13, 2, 9 / 10 / 11, 17 / 18, 21, 22, Protocol.
7. No new public methods. Existing `metadata` / `metadata_topics`
   signatures stay. `metadata()` stays empty-list (all topics).
8. Do **not** change `list_members` 14 wrap or `list_members_rpc`.
9. Do **not** change language clients, broker, or protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen except hunt must use no-14 Metadata |
| Language Metadata 14 | Sibling residual (v0.156) |
| `list_members` 14 | Already v0.120; do not change |
| Adding 14 to `metadata_list_members_round_trip` | Would recurse through hunt |
| Kafka `FindCoordinator` / API keys | Native Metadata only |
| Broker / protocol | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

Same redirect budget as Heartbeat v0.135; independent of the v0.96
transient retry budget:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: surface the first 14; no extra hunt Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): surface the original 14.
- Metadata 14 arrives as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`.
  There is no typed `Response::Metadata { error_code }`.
- Transient 6 / 7 / 15 / 16 and `Error::Io` still retry on
  `max_retries` (default 0) as in v0.96. A 7-then-0 Metadata still
  succeeds in two RPCs with `max_retries >= 1` and no 14 path.
- Error **13** / **2** / **9** / **10** / **11** / **17** / **18** /
  **21** / **22** and protocol are not retried and not redirected
  here.

Hunt is unchanged except it calls `metadata_rpc` instead of public
`metadata()`. Message hint wins; otherwise Metadata.controller_id when
non-zero; otherwise the first other advertised broker. Hinted-id
Metadata miss still uses `list_members_rpc`.

## API

No new public methods. Existing:

```rust
client.metadata().await?;
client.metadata_topics(vec!["events".into()]).await?;
```

Error 14 now follows Heartbeat / Produce/Fetch redirect budget.
Transient 7 still uses `max_retries`. Not Kafka FindCoordinator.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Tokio TCP stub (no live broker). Clone the 14+Metadata stub from
`v135_heartbeat_14.rs` but count Metadata RPCs.

| Case | Expect |
|------|--------|
| First Metadata 14 + `controller_id=2`; Metadata names node 2; second ok | success; 14 is redirect not retry |
| `max_redirects=0` + first 14 | first 14 surfaces; no extra hunt Metadata |
| `max_retries=2`, first Metadata 7 then 0 | two Metadatas, no 14 path |

`v96_metadata_list_members_retry.rs` and `v114_metadata_topics.rs`
must still pass. `v120_list_members_14.rs` must still pass (ListMembers
14 wrap unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `metadata_rpc`; 14 wrap; hunt uses no-14 path |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/src/config.rs` | `max_redirects` comment: Metadata 14 |
| `crates/volant-client/tests/v157_metadata_14.rs` | queued-code stub |
| `docs/V157_SPEC.md` | This spec |

## Honesty leftovers

- Redirect still uses the existing `redirect_to_controller` helper
  (Metadata brokers or ListMembers on a hinted id miss). Hunt now
  calls private `metadata_rpc` so `metadata` / `metadata_topics` and
  the helper are not mutually recursive `async fn`s (E0733). Hunt
  Metadata 14 surfaces as helper-false (original 14).
- Native Metadata has no top-level error_code. 14 is
  `Response::Error` only. Same honesty as Heartbeat 14 (v0.135):
  client wrap only — the broker may not return 14 on Metadata today.
- Not Kafka `FindCoordinator`.
- Default `max_retries` stays **0**.
- `list_members` 14 wrap / `list_members_rpc` are not changed.
- Language clients are a sibling residual (v0.156).
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc / `config.rs`
should keep this wrap local to `metadata` / `metadata_topics` and the
hunt helper:

- **Keep the 14 arm inside `metadata_topics`**. Do not drop the v0.96
  transient retry (`metadata_list_members_round_trip`).
- Do **not** add 14 to `metadata_list_members_round_trip`.
- Hunt must keep calling `metadata_rpc` (not public `metadata()`).
- Do not change `list_members` 14 / `list_members_rpc`.
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/lib.rs` (crate-doc)
- `crates/volant-client/src/config.rs` (`max_redirects` comment)
- `crates/volant-client/src/client.rs` (`metadata_topics` +
  `redirect_to_controller`)

## Related

- [V96_SPEC.md](./V96_SPEC.md) — Rust Metadata / ListMembers retry
  leftover this extends
- [V114_SPEC.md](./V114_SPEC.md) — Rust `metadata_topics`
- [V120_SPEC.md](./V120_SPEC.md) — Rust ListMembers 14 (hunt no-14
  helper pattern)
- [V135_SPEC.md](./V135_SPEC.md) — Rust Heartbeat 14 pattern this
  copies
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin 14 redirect
