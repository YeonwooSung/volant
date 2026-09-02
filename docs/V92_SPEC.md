# v0.92 — Rust DescribeGroup / ListGroups retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V90_SPEC.md](./V90_SPEC.md):
language DescribeGroup / ListGroups share `max_retries`. Rust
`describe_group` / `list_groups` (`crates/volant-client/src/client.rs`)
are a single `round_trip`. Range assignor
(`join_with_assignor("range")`) calls `describe_group`.

Reuse `ClientConfig.max_retries` / `retry_backoff_ms` (produce /
heartbeat / offset-admin / ListOffsets / LeaveGroup) and
`is_transient_error_code` / `is_transient_transport`. No new public
methods. Do **not** wrap `add_broker`, `describe_configs`,
`leave_group`, or `join_group`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients (they already
did this in v0.90).

## Goals

1. Extra DescribeGroup / ListGroups attempts after the first on
   **transient** errors only. Budget is independent of `max_redirects`.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   ListOffsets / LeaveGroup / `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: IO errors that produce already retries
     (`is_transient_transport` — `Error::Io`)
3. **Not retried here:**
   - Error **13** / **14**
   - Error **9** / **10** / **11**
   - Error **2** (NotFound). DescribeGroup **2** (no live members) must
     still fail immediately.
   - Protocol / constructor errors
   - JoinGroup / LeaveGroup / AddBroker / DescribeConfigs
4. Default `max_retries=0` so existing DescribeGroup / ListGroups
   tests stay valid (no extra RPCs).
5. Sleep between retry attempts using `retry_backoff_ms` (0 allowed in
   tests).
6. No new public methods. Existing `describe_group` / `list_groups`
   signatures stay. Range assignor already calls `describe_group` and
   inherits the retry.

## Transient errors

Match produce / fetch / heartbeat / offset-admin / ListOffsets /
LeaveGroup and `crates/volant-client` `is_transient_error_code`.

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
- Error **2** (`NotFound`). DescribeGroup **2** (no live members)
  fails immediately.
- Protocol / constructor errors.
- JoinGroup / LeaveGroup / AddBroker / DescribeConfigs.

## Non-goals

| Deferred | Why |
|----------|-----|
| JoinGroup retry | Not idempotent the same way |
| LeaveGroup / AddBroker / DescribeConfigs | Already shipped or not this slice |
| Produce-batch / fetch / heartbeat / offset-admin / ListOffsets retry changes | DescribeGroup / ListGroups only |
| Language clients | Already have this (v0.90) |
| Kafka `retries` / DescribeGroups vN | Native 34–37 only; not Kafka |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping `leave_group` / `join_group` / `add_broker` / `describe_configs` | Explicitly out of scope |

## API

Existing DescribeGroup / ListGroups signatures and constructors are
unchanged. These RPCs now share produce / heartbeat knobs:

```rust
Client::connect(ClientConfig {
    max_retries: 0,        // default
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
client.describe_group("g").await?;
client.list_groups().await?;
```

Default is **0 extra attempts**. `GroupConsumer::join_with_assignor("range")`
calls `Client::describe_group` and inherits.

## Semantics

Same budget as produce / heartbeat (independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one DescribeGroup
  RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two DescribeGroup
  RPCs, success. Same for ListGroups.
- First DescribeGroup **2** (no live members) raises immediately; no
  retry.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` DescribeGroup RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

## Tests

Tiny protocol stub that queues DescribeGroup / ListGroups error codes:

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first DescribeGroup 7 | one RPC; 7 surfaced |
| `max_retries=2`, backoff 0, first 7 then 0 | success; two DescribeGroup RPCs |
| first DescribeGroup 2 (no live members) | immediately 2; one RPC |
| ListGroups 7 then 0 | success; two ListGroups RPCs |
| Exhaust (always 7) | 7 after `1+max_retries` DescribeGroup RPCs |

Existing v73 range tests must still pass (their stubs already answer
DescribeGroup).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `describe_list_groups_round_trip` + wrap |
| `crates/volant-client/src/config.rs` | comments: knobs also apply to DescribeGroup / ListGroups |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v92_describe_list_groups_retry.rs` | queued-code stub |
| `docs/V92_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** `retries` / DescribeGroups versions.
- **Default 0** (same as produce / heartbeat / language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- JoinGroup / LeaveGroup / AddBroker / DescribeConfigs are unchanged.
- OffsetCommit / OffsetFetch / DeleteOffsets already retried (v0.83).
- ListOffsets already retried (v0.84).
- Heartbeat already retried (v0.80).
- LeaveGroup already retried (v0.87).
- Language clients already have this (v0.90); this slice is Rust only.
- Error 13 / 14 are not redirected here.
- Not a fully concurrent consumer. One TCP connection.

## Merge notes

Siblings **v0.91** / **v0.94** also edit `client.rs`. Keep this hunk
local to `describe_group` / `list_groups` + the
`describe_list_groups_round_trip` helper. Do **not** wrap
`add_broker`, `describe_configs`, `leave_group`, or `join_group`.
Reuse `is_transient_error_code` / `is_transient_transport` and the
existing backoff field.

## Related

- [V90_SPEC.md](./V90_SPEC.md) — language DescribeGroup / ListGroups
  retry leftover this closes
- [V87_SPEC.md](./V87_SPEC.md) — Rust LeaveGroup retry
- [V84_SPEC.md](./V84_SPEC.md) — Rust ListOffsets retry
- [V83_SPEC.md](./V83_SPEC.md) — Rust offset-admin retry
- [V80_SPEC.md](./V80_SPEC.md) — Rust heartbeat retry
- [V73_SPEC.md](./V73_SPEC.md) — range assignor via DescribeGroup
- [V69_SPEC.md](./V69_SPEC.md) — GroupConsumer range via DescribeGroup
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
