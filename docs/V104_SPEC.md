# v0.104 — Rust admin_round_trip transient retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V79_SPEC.md](./V79_SPEC.md) /
[V88_SPEC.md](./V88_SPEC.md) / [V91_SPEC.md](./V91_SPEC.md) /
[V94_SPEC.md](./V94_SPEC.md): Rust `admin_round_trip`
(`crates/volant-client/src/client.rs`) redirects on error **14**
(`NotController`) but `round_trip(...).await?` fails immediately on
transient IO / Timeout. CreateTopic / ACLs / SCRAM-admin /
Add/RemoveBroker / DescribeConfigs inherit that helper.

Reuse `ClientConfig.max_retries` / `retry_backoff_ms` (produce /
heartbeat / offset-admin) and `is_transient_error_code` /
`is_transient_transport`. No new public methods. Do **not** wrap
`ensure_producer_id` or `offset_admin_round_trip` (already retries).

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients (sibling
v0.103).

## Goals

1. Extra attempts after the first on transient **6 / 7 / 15 / 16**
   (typed `error_code` or `Response::Error`) and `Error::Io`.
2. **14 stays on `max_redirects`**. Independent counters: on 14
   redirect do not increment `retry_attempt`; on transient increment
   `retry_attempt` and sleep.
3. **Not retried:** 13, 9/10/11, Protocol, not-found (2), 21,
   InvalidTxnState (22).
4. Default `max_retries=0` so existing admin-14 tests stay valid (no
   extra CreateTopic / ACL / SCRAM / broker / config attempts).
5. Sleep between retry attempts using `retry_backoff_ms` (0 allowed in
   tests).
6. No new public methods. Existing CreateTopic / ACLs / SCRAM-admin /
   Add/RemoveBroker / Describe/AlterConfigs signatures stay and inherit
   via `admin_round_trip`.

## Transient errors

Match produce / heartbeat / offset-admin and `crates/volant-client`
`is_transient_error_code`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** `Error::Io` from the TCP layer (same as produce).
Today `admin_round_trip` does `self.round_trip(req.clone()).await?` —
this slice catches transient transport instead of `?`.

**Not retried here:**

- Error **14** (`NotController`) — stays on the v0.79
  `max_redirects` budget. Independent of `retry_attempt`.
- Error **13** (`NotLeaderForPartition`).
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Error **21** (`UnknownProducerId`).
- **InvalidTxnState** (22).
- Protocol / constructor errors.
- `ensure_producer_id` / `offset_admin_round_trip` (already retry).

## Non-goals

| Deferred | Why |
|----------|-----|
| Language-client admin retry | Sibling residual (v0.103) |
| Hunt algorithm change | Frozen (existing helper after v0.77 / v0.81) |
| Wrapping `ensure_producer_id` / `offset_admin_round_trip` | Already retry; out of scope |
| Produce-batch / fetch / heartbeat retry changes | Admin helper only |
| Kafka `retries` / CreateTopics vN | Native opcodes only; not Kafka |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing admin signatures and constructors are unchanged.
Controller-gated admin now shares produce / heartbeat knobs:

```rust
Client::connect(ClientConfig {
    max_retries: 0,        // default
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
client.create_topic("t", 1).await?;
```

Default is **0 extra attempts**. CreateTopic / DeleteTopic /
CreatePartitions / ReassignPartitions / CreateAcls / DeleteAcls /
ListAcls / SCRAM-admin / AddBroker / RemoveBroker / DescribeConfigs /
AlterConfigs all inherit via `admin_round_trip`.

## Semantics

Same retry budget as produce / heartbeat; **independent** of the
v0.79 redirect budget:

- Default `max_retries=0`: first transient 7 raises; one CreateTopic
  RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two CreateTopic RPCs,
  success.
- First CreateTopic **14** then Metadata then ok: redirect path
  (`max_redirects`); Metadata is used; `retry_attempt` is not
  incremented.
- First CreateTopic **2** (not found) raises immediately; no retry.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` CreateTopic RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

## Tests

Tiny protocol stub that queues CreateTopic error codes (enough to
cover the shared helper):

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first CreateTopic 7 | one RPC; 7 surfaced |
| `max_retries=2`, backoff 0, first 7 then 0 | success; two CreateTopic RPCs |
| first 14 then Metadata then ok | redirect path; Metadata used; not a retry |
| first 2 | immediately 2 |
| Exhaust always-7 | 7 after `1+max_retries` RPCs |

Existing v79 / v88 / v91 / v94 14-redirect tests must still pass.

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | transient retry inside `admin_round_trip` |
| `crates/volant-client/src/config.rs` | comments: knobs also apply to controller-gated admin |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v104_admin_round_trip_retry.rs` | queued-code stub |
| `docs/V104_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** `retries` / CreateTopics versions.
- **Default 0** (same as produce / heartbeat / language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- `ensure_producer_id` / `offset_admin_round_trip` are unchanged.
- Language-client admin retry is a sibling residual (v0.103).
- Error 14 is still redirected (v0.79 / v0.88 / v0.91 / v0.94), not
  retried on `max_retries`.
- Not a fully concurrent admin client. One TCP connection.

## Merge notes

Sibling **v0.102** also edits `client.rs`. Keep this hunk local to
`admin_round_trip`. Do **not** wrap `ensure_producer_id` or
`offset_admin_round_trip`. Do not drop the v0.79 / v0.88 / v0.91 /
v0.94 14 arms. Reuse `is_transient_error_code` /
`is_transient_transport` and the existing backoff field.

## Related

- [V79_SPEC.md](./V79_SPEC.md) — Rust admin 14 redirect leftover this
  extends
- [V88_SPEC.md](./V88_SPEC.md) — Rust SCRAM-admin / ListAcls 14
- [V91_SPEC.md](./V91_SPEC.md) — Rust Add/RemoveBroker 14
- [V94_SPEC.md](./V94_SPEC.md) — Rust Describe/AlterConfigs 14
- [V98_SPEC.md](./V98_SPEC.md) — Rust DeleteOffsets 14 + offset-admin
  retry (do not wrap)
- [V83_SPEC.md](./V83_SPEC.md) — Rust offset-admin transient retry
- [V80_SPEC.md](./V80_SPEC.md) — Rust heartbeat retry
- [V100_SPEC.md](./V100_SPEC.md) — Rust BeginTxn / EndTxn retry
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
