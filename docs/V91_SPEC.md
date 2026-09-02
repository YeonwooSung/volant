# v0.91 — Rust AddBroker / RemoveBroker NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V89_SPEC.md](./V89_SPEC.md) /
TODO: language clients already wrap Add/RemoveBroker 14 via
`_admin_round_trip`. Rust `add_broker` / `remove_broker` still do a
single `round_trip`. v0.72 / v0.79 / v0.88 skipped them because the
broker already forwards to the openraft leader (v0.38). When forward
is unavailable the client still sees native **14** (`NotController`).

Reuse existing `admin_round_trip` / `redirect_to_controller` and
`max_redirects` budget (and the v0.81 Metadata.controller_id hunt).
Do **not** rewrite the helper.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or language clients (they already did
this in v0.89).

## Goals

1. Same loop as `create_topic` / `create_acls`: route `add_broker` /
   `remove_broker` through `admin_round_trip`. On error **14**
   (`Response::Error` or typed `error_code`), if attempts remain, call
   the existing `redirect_to_controller` helper and retry the **same**
   AddBroker / RemoveBroker.
2. Add typed 14 arms for `Response::AddBroker` /
   `Response::RemoveBroker` in `admin_round_trip`. Parse
   `controller_id=` from any 14 message when present (existing helper).
3. Budget is the same as produce/fetch: `1 + max_redirects` (default
   `max_redirects=1`). `max_redirects=0` raises on the first 14; no
   Metadata.
4. Other errors still fail immediately.
5. No new public methods. Wrap `add_broker` and `remove_broker` only.
6. Overlay is still SoT. This is not Kafka broker catalog.
7. Do **not** wrap Describe/AlterConfigs, leave_group, describe_group,
   heartbeat.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (v0.81 already shipped) |
| Kafka `FindCoordinator` | Native opcodes only; no Kafka API keys |
| Describe/AlterConfigs, leave_group, describe_group, heartbeat | Out of scope |
| Broker / protocol / language clients | Frozen (broker already forwards; 14 when forward fails; languages did this in v0.89) |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.79 admin:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- AddBroker may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`.
  RemoveBroker may return a typed response with `error_code=14` and no
  id. When the broker cannot forward (v0.38), 14 is also typed.

Hunt is unchanged (existing helper + v0.81 Metadata.controller_id).

## API

No new public methods. Existing:

```rust
c.add_broker(3, "10.0.0.3", 9092, None).await?;
c.remove_broker(3).await?;
```

Error 14 now follows Produce/Fetch redirect budget when the broker
cannot forward. Not Kafka FindCoordinator. Overlay is still SoT.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Tokio TCP stub (no live broker):

| Case | Expect |
|------|--------|
| AddBroker first 14 + `controller_id=2`; Metadata names node 2; second ok | success; generation parsed |
| RemoveBroker typed 14 (no hint); Metadata has another broker; second ok | success |
| AddBroker `max_redirects=0` + 14 | error 14; no Metadata |

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | two wraps + `admin_round_trip` arms |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v91_add_remove_broker_14.rs` | queued-code stub |
| `docs/V91_SPEC.md` | This spec |

Existing v44 / v60 / v67 / v73 / v79 / v80 / v83 / v84 / v87 / v88
tests must still pass.

## Merge notes

Sibling slices **v0.92** / **v0.94** also edit `client.rs`. When
merging:

- **Keep the two method wraps** (AddBroker / RemoveBroker) and the
  matching `admin_round_trip` typed-14 arms.
- Do not change `redirect_to_controller` hunt logic (that is v0.81).
- Do not wrap Describe/AlterConfigs, leave_group, describe_group,
  heartbeat.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops or the v0.79
  / v0.88 admin wraps.
- Do not change the broker, Kafka shim, or language clients.

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is v0.81.
- Not Kafka `FindCoordinator`.
- Describe/AlterConfigs, leave_group, and describe_group still do not
  redirect on 14.
- Overlay is still SoT; this is not Kafka broker catalog.
- No Kafka API keys / opcodes / Phase 155.
- Language clients already have this (v0.89); this slice is Rust only.

See [V89_SPEC.md](./V89_SPEC.md) (language leftover this closes),
[V88_SPEC.md](./V88_SPEC.md) (Rust SCRAM/ListAcls 14),
[V79_SPEC.md](./V79_SPEC.md) (Rust admin 14),
[V72_SPEC.md](./V72_SPEC.md) (language admin 14),
[V81_SPEC.md](./V81_SPEC.md) (Metadata.controller_id hunt), and
[V38_SPEC.md](./V38_SPEC.md) (AddBroker forward).
