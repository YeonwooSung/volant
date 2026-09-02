# v0.102 — Rust InitProducerId retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [TODO.md](../TODO.md) / produce
retry: Rust `ensure_producer_id` (`crates/volant-client/src/client.rs`)
is a single `round_trip`. Produce already retries transient errors and
has a one-shot UnknownProducerId re-Init. Init itself is not retried.
Language InitProducerId retry is a sibling residual (v0.101).

Reuse `ClientConfig.max_retries` / `retry_backoff_ms` (produce /
heartbeat / offset-admin / ListOffsets / LeaveGroup / DescribeGroup /
ListGroups / Metadata / ListMembers / BeginTxn / EndTxn) and
`is_transient_error_code` / `is_transient_transport`. No new public
methods. Wrap `ensure_producer_id` only. Do **not** wrap `produce`,
`begin_transaction`, or `admin_round_trip`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients (sibling
v0.101).

## Goals

1. Extra InitProducerId attempts after the first on **transient**
   errors only. Budget is independent of `max_redirects` and of
   produce’s one-shot unknown-pid re-Init.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   ListOffsets / LeaveGroup / DescribeGroup / ListGroups / Metadata /
   ListMembers / BeginTxn / EndTxn / `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: IO errors that produce already retries
     (`is_transient_transport` — `Error::Io`)
3. **Not retried here:**
   - Error **13** / **14**
   - Error **9** / **10** / **11**
   - Error **2** (NotFound)
   - Protocol / constructor errors
   - **UnknownProducerId** (`ErrorCode::UnknownProducerId`, 21) on
     Init itself
   - Produce / BeginTxn / `admin_round_trip`
4. Default `max_retries=0` so existing idempotent / txn tests stay
   valid (no extra InitProducerId attempts).
5. Sleep between retry attempts using `retry_backoff_ms` (0 allowed in
   tests).
6. No new public methods. Existing produce / txn signatures stay.
   `ensure_producer_id` is wrapped; first idempotent produce and
   `begin_transaction` inherit.
7. If already `initialized`, return immediately (no extra Init).

## Transient errors

Match produce / fetch / heartbeat / offset-admin / ListOffsets /
LeaveGroup / DescribeGroup / ListGroups / Metadata / ListMembers /
BeginTxn / EndTxn and `crates/volant-client`
`is_transient_error_code`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** `Error::Io` from the TCP layer (same as produce /
heartbeat).

**Not retried here:**

- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Protocol / constructor errors.
- **UnknownProducerId** (21) on Init itself. Produce’s one-shot
  unknown-pid re-Init stays on that independent budget.
- Produce / BeginTxn / `admin_round_trip`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Language InitProducerId retry | Sibling residual (v0.101) |
| Produce / BeginTxn / `admin_round_trip` retry changes | Already shipped or not this slice |
| Kafka `retries` / InitProducerId vN | Native opcode only; not Kafka |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping `produce` / `begin_transaction` / `admin_round_trip` | Explicitly out of scope |

## API

Existing produce / txn signatures and constructors are unchanged.
InitProducerId now shares produce / heartbeat knobs:

```rust
Client::connect(ClientConfig {
    enable_idempotence: true,
    max_retries: 0,        // default
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
client.produce("t", Some(0), vec![msg]).await?;
```

Default is **0 extra attempts**. First idempotent produce and
`begin_transaction` call `ensure_producer_id` and inherit.

## Semantics

Same budget as produce / heartbeat (independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one InitProducerId
  RPC.
- `max_retries=2`, backoff 0: Init Timeout then ok → two Init
  RPCs, produce success (pid allocated).
- First Init **UnknownProducerId** (21) raises immediately; one RPC.
- Exhausted retries: always 7 on Init with `max_retries=2` → raise
  7 after `1 + max_retries` Init RPCs.
- Transport fail then ok with `max_retries >= 1` → success.
- Already `initialized` → return immediately; no extra Init.
- Produce / BeginTxn / `admin_round_trip` retry loops are unchanged.

## Tests

Tiny protocol stub that queues InitProducerId error codes and answers
Produce after a successful Init. Drive Init via `enable_idempotence`
+ first produce (partition pinned so Metadata is not required).

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first Init 7 | one RPC; 7 surfaced |
| `max_retries=2`, backoff 0, first Init 7 then 0 | success (pid allocated); two Init RPCs |
| first Init 21 | immediately 21; one RPC |
| Exhaust always-7 | 7 after `1+max_retries` RPCs |

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `ensure_producer_id` retry loop |
| `crates/volant-client/src/config.rs` | comments: knobs also apply to InitProducerId |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v102_init_producer_id_retry.rs` | queued-code stub |
| `docs/V102_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** `retries` / InitProducerId versions.
- **Default 0** (same as produce / heartbeat / language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- Produce / BeginTxn / `admin_round_trip` are unchanged.
- UnknownProducerId (21) on Init itself is not retried.
- OffsetCommit / OffsetFetch / DeleteOffsets already retried (v0.83).
- ListOffsets already retried (v0.84).
- Heartbeat already retried (v0.80).
- LeaveGroup already retried (v0.87).
- DescribeGroup / ListGroups already retried (v0.92).
- Metadata / ListMembers already retried (v0.96).
- BeginTxn / EndTxn already retried (v0.100).
- Language InitProducerId retry is a sibling residual (v0.101).
- Error 13 / 14 are not redirected here.
- Not a fully concurrent producer. One TCP connection.

## Merge notes

Sibling **v0.104** also edits `client.rs`. Keep this hunk local to
`ensure_producer_id`. Do **not** wrap `produce`, `begin_transaction`,
or `admin_round_trip`. Reuse `is_transient_error_code` /
`is_transient_transport` and the existing backoff field.

## Related

- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V100_SPEC.md](./V100_SPEC.md) — Rust BeginTxn / EndTxn retry
- [V96_SPEC.md](./V96_SPEC.md) — Rust Metadata / ListMembers retry
- [V92_SPEC.md](./V92_SPEC.md) — Rust DescribeGroup / ListGroups retry
- [V87_SPEC.md](./V87_SPEC.md) — Rust LeaveGroup retry
- [V84_SPEC.md](./V84_SPEC.md) — Rust ListOffsets retry
- [V83_SPEC.md](./V83_SPEC.md) — Rust offset-admin retry
- [V80_SPEC.md](./V80_SPEC.md) — Rust heartbeat retry
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
- [PHASE18_SPEC.md](./PHASE18_SPEC.md) — native InitProducerId / txn
