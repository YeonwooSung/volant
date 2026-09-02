# v0.98 — Rust DeleteOffsets NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V83_SPEC.md](./V83_SPEC.md) /
TODO: Rust `delete_offsets` uses `offset_admin_round_trip`
(transient retry only). It does **not** redirect on error **14**
(`NotController`). Language DeleteOffsets error-14 is a sibling
residual (v0.97).

Reuse existing `redirect_to_controller` + `max_redirects` (v0.79).
Keep existing transient retry (`max_retries`, default **0**). Prefer
adding 14 handling **inside `offset_admin_round_trip`** so
DeleteOffsets gets it; OffsetCommit / OffsetFetch then also redirect
on 14 if they share the helper. That is OK.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / language clients. Do **not** change
whether the broker returns 14 today.

## Goals

1. On DeleteOffsets typed `error_code == 14` or
   `Response::Error { code: 14 }`: if redirect attempts remain, call
   `redirect_to_controller(hint)` and retry the **same** request.
2. Prefer adding 14 handling **inside `offset_admin_round_trip`** so
   DeleteOffsets gets it; OffsetCommit / OffsetFetch then also
   redirect on 14. Document that share. (DeleteOffsets-only wrapper
   is an allowed alternative; this slice uses the shared helper.)
3. 14 uses **redirect budget** (`max_redirects`), not `max_retries`.
   Transient 6 / 7 / 15 / 16 stay on `max_retries`.
4. Budget is the same as produce/fetch / v0.79 admin:
   `1 + max_redirects` (default `max_redirects=1`).
   `max_redirects=0` does not redirect (no Metadata).
5. No new public methods. Existing `delete_offsets` / `commit_offsets`
   / `fetch_offsets` signatures stay.
6. Do **not** wrap `metadata`, `begin_transaction`, `list_members`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (existing helper after v0.77 / v0.81) |
| Broker returning 14 today | Frozen (protocol / broker) |
| Kafka `FindCoordinator` / OffsetDelete API key 47 | Native 38/39 only |
| Language clients | Sibling v0.97 |
| `metadata` / `list_members` retry | Sibling v0.96 |
| `begin_transaction` wrap | Out of scope |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same redirect budget as Produce/Fetch / v0.79 admin; independent of
the v0.83 transient retry budget:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- DeleteOffsets may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`
  or a typed `Response::DeleteOffsets { error_code: 14, .. }` with no
  id.
- Transient 6 / 7 / 15 / 16 and `Error::Io` still retry on
  `max_retries` (default 0) as in v0.83. A 7-then-0 DeleteOffsets
  still succeeds in two RPCs with `max_retries >= 1`.
- Error **13** / **9** / **10** / **11** / **2** are still not retried
  and not redirected here.

Hunt is unchanged (existing helper). Message hint wins; otherwise
Metadata.controller_id when non-zero; otherwise the first other
advertised broker.

OffsetCommit / OffsetFetch share `offset_admin_round_trip` and
therefore also redirect on 14. That is intentional.

## API

No new public methods. Existing:

```rust
client.delete_offsets("g", entries).await?;
```

Error 14 now follows Produce/Fetch redirect budget. Transient 7 still
uses `max_retries`. Not Kafka FindCoordinator / OffsetDelete.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Tokio TCP stub (no live broker):

| Case | Expect |
|------|--------|
| DeleteOffsets first 14 + `controller_id=2`; Metadata names node 2; second ok | success |
| DeleteOffsets typed 14 (no hint); Metadata has another broker; second ok | success |
| `max_redirects=0` + 14 | error 14; no Metadata |
| Existing v83 DeleteOffsets 7 then 0 still works | two RPCs, success (run `v83_offset_admin_retry` too) |

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | 14 arm inside `offset_admin_round_trip`; `delete_offsets` docs |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v98_delete_offsets_14.rs` | queued-code stub |
| `docs/V98_SPEC.md` | This spec |

Existing v44 / v60 / v67 / v73 / v76 / v79 / v80 / v83 / v84 / v87 /
v88 / v91 / v92 / v94 tests must still pass.

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is unchanged.
- Not Kafka `FindCoordinator` / OffsetDelete.
- OffsetCommit / OffsetFetch inherit 14 redirect via the shared
  helper. That is OK and documented.
- `metadata`, `begin_transaction`, and `list_members` are not wrapped.
- Broker / protocol still do not change whether DeleteOffsets returns
  14 today.
- No Kafka API keys / opcodes / Phase 155.
- Language clients are a sibling residual (v0.97).

## Merge notes

Sibling slices **v0.96** / **v0.100** also edit `client.rs`. When
merging:

- **Keep the 14 arm inside `offset_admin_round_trip`** (and the
  `delete_offsets` docstring). Do not drop the v0.83 transient retry.
- Do not change `redirect_to_controller` hunt logic.
- Do not wrap `metadata`, `begin_transaction`, `list_members`.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops or the
  v0.79 / v0.88 / v0.91 / v0.94 admin wraps.
- Do not change the broker, Kafka shim, or language clients.

## Related

- [V83_SPEC.md](./V83_SPEC.md) — Rust offset-admin transient retry
  leftover this extends
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin 14 redirect (did not wrap
  DeleteOffsets)
- [V94_SPEC.md](./V94_SPEC.md) — Rust Describe/AlterConfigs 14
- [V91_SPEC.md](./V91_SPEC.md) — Rust Add/RemoveBroker 14
- [V88_SPEC.md](./V88_SPEC.md) — Rust SCRAM-admin / ListAcls 14
- [V72_SPEC.md](./V72_SPEC.md) — language admin 14
- [V54_SPEC.md](./V54_SPEC.md) — language DeleteOffsets
