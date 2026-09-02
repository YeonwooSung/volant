# v0.88 — Rust SCRAM-admin / ListAcls NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V85_SPEC.md](./V85_SPEC.md) /
[V79_SPEC.md](./V79_SPEC.md): Rust `redirect_to_controller` already
wraps create_topic / create_partitions / reassign / create_acls /
delete_acls. `create_scram_user` / `delete_scram_user` /
`list_scram_users` / `list_acls` still do a single `round_trip`.

Reuse existing `redirect_to_controller` + `max_redirects` (v0.79).
Prefer Metadata.controller_id when the 14 message has no hint (already
in the helper after the v0.77 splice).

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / language clients (they already did this
in v0.85).

## Goals

1. Same loop as `create_acls`: on error **14** (typed `error_code` or
   `Response::Error`), if attempts remain (`1 + max_redirects`),
   `redirect_to_controller(hint)` and retry the same RPC.
2. Parse `controller_id=` from any 14 message (existing
   `parse_controller_id`).
3. Budget is the same as produce/fetch: `1 + max_redirects` (default
   `max_redirects=1`). `max_redirects=0` does not redirect (no
   Metadata).
4. Other errors (2 not found, etc.) still fail immediately.
5. No new public methods. Wrap only:
   - `create_scram_user` / `delete_scram_user` / `list_scram_users`
   - `list_acls`
6. Do **not** wrap AddBroker/RemoveBroker, Describe/AlterConfigs,
   leave_group, heartbeat.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (existing helper after v0.77) |
| Broker returning 14 today | Frozen (protocol / broker) |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| AddBroker / RemoveBroker redirect | Broker already forwards (v0.38) |
| Describe/AlterConfigs, offsets | Do not return 14 today |
| leave_group wrap | Sibling v0.87; keep hunks local |
| Heartbeat wrap | Already has its own retry (v0.80) |
| Language clients | Already have this (v0.85) |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.79 admin:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- CreateScramUser may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`.
  ListAcls / DeleteScramUser / ListScramUsers may return typed
  responses with `error_code=14` and no id.

Hunt is unchanged (existing helper). Message hint wins; otherwise
Metadata.controller_id when non-zero; otherwise the first other
advertised broker.

## API

No new public methods. Existing:

```rust
c.create_scram_user("alice", "s3cret", 0).await?;
c.delete_scram_user("alice").await?;
c.list_scram_users().await?;
c.list_acls("", 255, "").await?;
```

Error 14 now follows Produce/Fetch redirect budget. Not Kafka
FindCoordinator.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Tokio TCP stub (no live broker):

| Case | Expect |
|------|--------|
| CreateScramUser first 14 + `controller_id=2`; Metadata names node 2; second ok | success |
| ListAcls typed 14 (no hint); Metadata has another broker; second ok | success |
| DeleteScramUser `max_redirects=0` + 14 | error 14; no Metadata |
| ListScramUsers 14 then ok | success |

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | four wraps + `admin_round_trip` arms |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v88_scram_listacls_14.rs` | queued-code stub |
| `docs/V88_SPEC.md` | This spec |

Existing v44 / v60 / v67 / v73 / v79 / v80 / v83 / v84 tests must
still pass.

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is unchanged.
- Not Kafka `FindCoordinator`.
- AddBroker/RemoveBroker, Describe/AlterConfigs, leave_group, and
  offset admin RPCs still do not redirect.
- No Kafka API keys / opcodes / Phase 155.
- Language clients already have this (v0.85); this slice is Rust only.

## Merge notes

Sibling slices also edit `client.rs`. When merging:

- **Keep the four method wraps** (CreateScramUser / DeleteScramUser /
  ListScramUsers / ListAcls) and the matching `admin_round_trip` arms.
- Do not change `redirect_to_controller` hunt logic.
- Do not wrap AddBroker/RemoveBroker, Describe/AlterConfigs,
  leave_group, heartbeat.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops or the v0.79
  six admin wraps.
- Do not change the broker, Kafka shim, or language clients.

## Related

- [V85_SPEC.md](./V85_SPEC.md) — language leftover this closes
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin 14 redirect (six RPCs)
- [V72_SPEC.md](./V72_SPEC.md) — language admin 14 redirect
- [V55_SPEC.md](./V55_SPEC.md) — SCRAM admin
- [V56_SPEC.md](./V56_SPEC.md) — ACLs
