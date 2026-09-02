# v0.94 — Rust DescribeConfigs / AlterConfigs NotController redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V79_SPEC.md](./V79_SPEC.md) /
TODO: Rust `describe_configs` / `alter_configs`
(`crates/volant-client/src/client.rs`) still do a single `round_trip`.
Language Describe/AlterConfigs error-14 is a sibling residual
([V93_SPEC.md](./V93_SPEC.md)).

Reuse existing `admin_round_trip` / `redirect_to_controller` +
`max_redirects` (v0.79). Prefer Metadata.controller_id when the 14
message has no hint (already in the helper after the v0.77 splice).

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / language clients. Topic-only — not
Kafka BROKER configs.

## Goals

1. Same loop as `create_topic`: on error **14** (typed `error_code` or
   `Response::Error`), if attempts remain (`1 + max_redirects`),
   `redirect_to_controller(hint)` and retry the same RPC.
2. Parse `controller_id=` from any 14 message (existing
   `parse_controller_id`).
3. Budget is the same as produce/fetch: `1 + max_redirects` (default
   `max_redirects=1`). `max_redirects=0` does not redirect (no
   Metadata).
4. Other errors (2 not found, etc.) still fail immediately. Not-found
   (2) is not redirected.
5. No new public methods. Wrap only:
   - `describe_configs`
   - `alter_configs`
6. Topic-only. Not Kafka BROKER configs.
7. Do **not** wrap `add_broker`, `describe_group`, `delete_offsets`,
   `leave_group`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (existing helper after v0.77) |
| Broker returning 14 today | Frozen (protocol / broker) |
| Kafka `FindCoordinator` / DescribeConfigs API keys 32/33/44 | Native 40–43 only |
| BROKER resource | Phase 99 stays Kafka/Rust |
| AddBroker / RemoveBroker redirect | Broker already forwards (v0.38); language leftover is v0.89 |
| describe_group / list_groups wrap | Sibling retry residuals; 14 not returned here today |
| delete_offsets wrap | Already retried on transient (v0.83); 14 not redirected |
| leave_group wrap | Already retried on transient (v0.87) |
| Language clients | Sibling v0.93 |
| Phase 155 / homemade Raft | Frozen |
| New native opcodes / Kafka API keys | Frozen |

## Semantics

Same budget as Produce/Fetch / v0.79 admin:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 14; no Metadata.
- Helper false (no other broker / unknown id / empty host / reconnect
  fail): raise the original 14.
- DescribeConfigs may arrive as
  `Response::Error { code: 14, message: "not controller; controller_id=N" }`.
  AlterConfigs (and typed DescribeConfigs) may return
  `error_code=14` and no id.
- Error **2** (topic not found) fails immediately; no Metadata.

Hunt is unchanged (existing helper). Message hint wins; otherwise
Metadata.controller_id when non-zero; otherwise the first other
advertised broker.

## API

No new public methods. Existing:

```rust
c.describe_configs("events").await?;
c.alter_configs("events", vec![("retention.ms".into(), "86400000".into())]).await?;
c.alter_configs("events", vec![("retention.ms".into(), "".into())]).await?; // clear
```

Error 14 now follows Produce/Fetch redirect budget. Topic configs
only. Not Kafka FindCoordinator / BROKER DescribeConfigs.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Tokio TCP stub (no live broker):

| Case | Expect |
|------|--------|
| DescribeConfigs first 14 + `controller_id=2`; Metadata names node 2; second ok | success |
| AlterConfigs typed 14 (no hint); Metadata has another broker; second ok | success |
| DescribeConfigs `max_redirects=0` + 14 | error 14; no Metadata |

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | two wraps + `admin_round_trip` arms |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v94_describe_alter_configs_14.rs` | queued-code stub |
| `docs/V94_SPEC.md` | This spec |

Existing v44 / v60 / v67 / v73 / v76 / v79 / v80 / v83 / v84 / v87 /
v88 tests must still pass.

## Honesty leftovers

- Redirect still uses the existing helper (Metadata brokers or
  ListMembers on a hinted id miss). Hunt is unchanged.
- Not Kafka `FindCoordinator` / DescribeConfigs / IncrementalAlterConfigs.
- Topic-only. No BROKER resource on native 40–43.
- AddBroker/RemoveBroker, describe_group, delete_offsets, and
  leave_group still do not redirect on 14.
- No Kafka API keys / opcodes / Phase 155.
- Broker / protocol still do not change whether these RPCs return 14
  today.
- Language clients are a sibling residual (v0.93).

## Merge notes

Sibling slices also edit `client.rs` (v0.91 / v0.92). When merging:

- **Keep the two method wraps** (`describe_configs` / `alter_configs`)
  and the matching `admin_round_trip` arms
  (`Response::DescribeConfigs` / `Response::AlterConfigs`).
- Do not change `redirect_to_controller` hunt logic.
- Do not wrap `add_broker`, `describe_group`, `delete_offsets`,
  `leave_group`, heartbeat.
- Do not drop Produce/Fetch/DeleteRecords error-13 loops or the
  v0.79 / v0.88 admin wraps.
- Do not change the broker, Kafka shim, or language clients.

## Related

- [V93_SPEC.md](./V93_SPEC.md) — language leftover this closes
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin 14 redirect (six RPCs)
- [V88_SPEC.md](./V88_SPEC.md) — Rust SCRAM-admin / ListAcls 14
- [V72_SPEC.md](./V72_SPEC.md) — language admin 14 redirect
- [V53_SPEC.md](./V53_SPEC.md) — language Describe/AlterConfigs
- [PHASE13_SPEC.md](./PHASE13_SPEC.md) — native 40–43
