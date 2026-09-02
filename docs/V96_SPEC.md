# v0.96 — Rust Metadata / ListMembers retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V95_SPEC.md](./V95_SPEC.md) /
TODO: language Metadata / ListMembers share `max_retries`. Rust
`metadata` / `list_members` (`crates/volant-client/src/client.rs`
~686 / ~1678) are a single `round_trip`. Admin-14 and leader-13
redirect call these with no retry.

Reuse `ClientConfig.max_retries` / `retry_backoff_ms` (produce /
heartbeat / offset-admin / ListOffsets / LeaveGroup / DescribeGroup /
ListGroups) and `is_transient_error_code` / `is_transient_transport`.
No new public methods. Do **not** wrap `delete_offsets`,
`begin_transaction`, `add_broker`, or `describe_group`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients (they already
did this in v0.95).

## Goals

1. Extra Metadata / ListMembers attempts after the first on
   **transient** errors only. Budget is independent of `max_redirects`.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   ListOffsets / LeaveGroup / DescribeGroup / ListGroups /
   `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: IO errors that produce already retries
     (`is_transient_transport` — `Error::Io`)
3. **Not retried here:**
   - Error **13** / **14**
   - Error **9** / **10** / **11**
   - Error **2** (NotFound)
   - Protocol / constructor errors
   - DeleteOffsets / BeginTransaction / AddBroker / DescribeGroup
4. Default `max_retries=0` so existing Metadata / ListMembers /
   redirect tests stay valid (no extra RPCs).
5. Sleep between retry attempts using `retry_backoff_ms` (0 allowed in
   tests).
6. No new public methods. Existing `metadata` / `list_members`
   signatures stay. Admin-14 and leader-13 redirect already call those
   and inherit the retry.

## Transient errors

Match produce / fetch / heartbeat / offset-admin / ListOffsets /
LeaveGroup / DescribeGroup / ListGroups and
`crates/volant-client` `is_transient_error_code`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** `Error::Io` from the TCP layer (same as produce /
heartbeat).

Native Metadata has **no** top-level `error_code`. Failures arrive as
`Response::Error` or transport. Do not invent a Metadata error_code
the codec does not have. Topic-level `error_code` is unchanged and is
not a retry signal. ListMembers keeps its typed `error_code`.

**Not retried here:**

- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Protocol / constructor errors.
- DeleteOffsets / BeginTransaction / AddBroker / DescribeGroup.

## Non-goals

| Deferred | Why |
|----------|-----|
| DeleteOffsets / BeginTransaction / AddBroker / DescribeGroup retry | Already shipped or not this slice |
| Produce-batch / fetch / heartbeat / offset-admin / ListOffsets / LeaveGroup / DescribeGroup / ListGroups retry changes | Metadata / ListMembers only |
| Redirect hunt algorithm | Frozen (v0.72 / v0.81 / v0.79); helpers inherit via existing calls |
| Language clients | Already have this (v0.95) |
| Kafka `retries` / Metadata vN | Native opcodes only; not Kafka |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping `delete_offsets` / `begin_transaction` / `add_broker` / `describe_group` | Explicitly out of scope |

## API

Existing Metadata / ListMembers signatures and constructors are
unchanged. These RPCs now share produce / heartbeat knobs:

```rust
Client::connect(ClientConfig {
    max_retries: 0,        // default
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
client.metadata().await?;
client.list_members().await?;
```

Default is **0 extra attempts**. `redirect_to_controller` (admin-14)
and leader-13 redirect call `Client::metadata` / `list_members` and
inherit.

## Semantics

Same budget as produce / heartbeat (independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one Metadata RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two Metadata RPCs,
  success. Same for ListMembers (typed `error_code`).
- First Metadata **2** (`Response::Error`; native Metadata has no
  top-level error_code) raises immediately; no retry.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` Metadata RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

## Tests

Tiny protocol stub that queues Metadata Error codes / ListMembers
typed `error_code`:

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first Metadata Error 7 | one RPC; 7 surfaced |
| `max_retries=2`, backoff 0, first Metadata 7 then ok | success; two Metadata RPCs |
| ListMembers typed 7 then 0 | success; two ListMembers RPCs |
| first Metadata 2 | immediately 2; one RPC |
| Exhaust (always 7) | 7 after `1+max_retries` Metadata RPCs |

Existing redirect stubs that answer Metadata once must keep passing
(default `max_retries=0`).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `metadata_list_members_round_trip` + wrap |
| `crates/volant-client/src/config.rs` | comments: knobs also apply to Metadata / ListMembers |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v96_metadata_list_members_retry.rs` | queued-code stub |
| `docs/V96_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** `retries` / Metadata versions.
- **Default 0** (same as produce / heartbeat / language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- DeleteOffsets / BeginTransaction / AddBroker / DescribeGroup are
  unchanged (DescribeGroup already retried in v0.92).
- OffsetCommit / OffsetFetch / DeleteOffsets already retried (v0.83).
- ListOffsets already retried (v0.84).
- Heartbeat already retried (v0.80).
- LeaveGroup already retried (v0.87).
- DescribeGroup / ListGroups already retried (v0.92).
- Language clients already have this (v0.95); this slice is Rust only.
- Error 13 / 14 are not redirected here.
- Native Metadata still has no top-level `error_code`.
- Not a fully concurrent consumer. One TCP connection.

## Merge notes

Siblings that also edit `client.rs` (v0.98 / v0.100) should keep
produce/fetch/heartbeat/offset-admin/ListOffsets/LeaveGroup/DescribeGroup/ListGroups
retry. Only wrap `metadata` and `list_members`. Reuse
`is_transient_error_code` / `is_transient_transport` and the existing
backoff field. Do **not** wrap `delete_offsets`, `begin_transaction`,
`add_broker`, or `describe_group`.

Expect conflicts on:

- `crates/volant-client/src/client.rs` (`metadata` / `list_members` +
  `metadata_list_members_round_trip`)
- `crates/volant-client/src/config.rs` (`max_retries` comment)
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V95_SPEC.md](./V95_SPEC.md) — language Metadata / ListMembers retry
  leftover this closes
- [V92_SPEC.md](./V92_SPEC.md) — Rust DescribeGroup / ListGroups retry
- [V87_SPEC.md](./V87_SPEC.md) — Rust LeaveGroup retry
- [V84_SPEC.md](./V84_SPEC.md) — Rust ListOffsets retry
- [V83_SPEC.md](./V83_SPEC.md) — Rust offset-admin retry
- [V81_SPEC.md](./V81_SPEC.md) — admin-14 prefers Metadata.controller_id
- [V80_SPEC.md](./V80_SPEC.md) — Rust heartbeat retry
- [V79_SPEC.md](./V79_SPEC.md) — admin NotController redirect (calls
  Metadata / ListMembers)
- [V72_SPEC.md](./V72_SPEC.md) — admin NotController redirect
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
