# v0.84 — Rust ListOffsets retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V78_SPEC.md](./V78_SPEC.md) /
[V71_SPEC.md](./V71_SPEC.md): language OffsetCommit / OffsetFetch /
DeleteOffsets already share `max_retries` (default **0**). Rust
`Client::list_offsets` (`crates/volant-client/src/client.rs`) does a
single `round_trip`. GroupConsumer `earliest` / `latest` reset calls
it; a transient 7 fails join.

Reuse `ClientConfig.max_retries` / `retry_backoff_ms` (produce /
heartbeat) and `is_transient_error_code` / `is_transient_transport`.
No new public methods. Do **not** wrap `commit_offsets` /
`fetch_offsets` / `delete_offsets` (v0.83), heartbeat, or
`create_topic`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients.

## Goals

1. Extra ListOffsets attempts after the first on **transient** errors
   only. Budget is independent of `max_redirects`.
2. Same transient set as produce / heartbeat /
   `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: IO errors that produce already retries
     (`is_transient_transport` — `Error::Io`)
3. **Not retried here:**
   - Error **13** (`NotLeaderForPartition`) / **14** (`NotController`)
   - Error **9** (`RebalanceInProgress`) / **10** (`UnknownMemberId`) /
     **11** (`IllegalGeneration`)
   - Error **2** (`NotFound`)
   - Protocol / constructor errors
   - OffsetCommit / OffsetFetch / DeleteOffsets / Heartbeat /
     CreateTopic
4. Default `max_retries=0` so existing ListOffsets tests stay valid (no
   extra ListOffsets attempts).
5. Sleep between retry attempts using `retry_backoff_ms` (0 allowed in
   tests).
6. No new public methods. Existing `list_offsets` signature stays.
   GroupConsumer `apply_reset` (`earliest` / `latest`) inherits via
   `Client::list_offsets`.

## Transient errors

Match produce / heartbeat and `crates/volant-client`
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
- OffsetCommit / OffsetFetch / DeleteOffsets / Heartbeat /
  CreateTopic.

## Non-goals

| Deferred | Why |
|----------|-----|
| OffsetCommit / OffsetFetch / DeleteOffsets retry | Sibling v0.83 |
| Heartbeat / CreateTopic retry or redirect | Already shipped (v0.80 / v0.79) |
| JoinGroup / LeaveGroup retry | Not idempotent the same way |
| Produce-batch / fetch retry changes | ListOffsets only |
| Language clients | ListOffsets already exists; this slice is Rust only |
| Kafka `retries` / ListOffsets vN | Native 48/49 only; not Kafka |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing ListOffsets signature and constructors are unchanged.
ListOffsets now shares produce / heartbeat knobs:

```rust
Client::connect(ClientConfig {
    max_retries: 0,        // default
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
client.list_offsets("t", vec![0]).await?
```

Non-zero codes still go through `check_ok` (Timeout **7** surfaces as
`Error::Io`; NotFound **2** as `Error::NotFound`). Transient **7** is
retried; after exhaust the caller sees `error_code=7`. NotFound **2**
returns immediately.

Default is **0 extra attempts**. `GroupConsumer` earliest / latest
reset call `Client::list_offsets` and inherit.

## Semantics

Same budget as produce / heartbeat (independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one ListOffsets
  RPC.
- `max_retries=2`, backoff 0: Timeout then ok (`earliest=0`,
  `latest=5`) → two ListOffsets RPCs, success.
- First ListOffsets **2** (not found) raises immediately; no retry.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` ListOffsets RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

## Tests

Tiny protocol stub that queues ListOffsets error codes:

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first ListOffsets 7 | one RPC; 7 surfaced |
| `max_retries=2`, backoff 0, first 7 then 0 with earliest=0 latest=5 | success; two ListOffsets RPCs |
| first ListOffsets 2 | immediately 2; one RPC |
| Exhaust (always 7) | 7 after `1+max_retries` RPCs |

Existing v67 / v71 / v73 stubs already answer ListOffsets and must
keep passing.

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `list_offsets` retry loop |
| `crates/volant-client/src/config.rs` | comments: knobs also apply to ListOffsets |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v84_list_offsets_retry.rs` | queued-code stub |
| `docs/V84_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** `retries` / ListOffsets versions.
- **Default 0** (same as produce / heartbeat / language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- OffsetCommit / OffsetFetch / DeleteOffsets are unchanged (v0.83).
- Heartbeat already retried (v0.80). CreateTopic already redirected
  (v0.79).
- Language ListOffsets already exists; this slice is Rust only.
- Not a fully concurrent consumer. One TCP connection.

## Merge notes

Sibling **v0.83** also edits `client.rs` (OffsetCommit / OffsetFetch /
DeleteOffsets retry). Keep this hunk local to `list_offsets` + the
existing `is_transient_error_code` / `is_transient_transport` helpers.
Do **not** wrap `commit_offsets` / `fetch_offsets` / `delete_offsets`,
heartbeat, or `create_topic`.

## Related

- [V78_SPEC.md](./V78_SPEC.md) — language OffsetCommit retry leftover
  this extends (ListOffsets was deferred there)
- [V71_SPEC.md](./V71_SPEC.md) — Rust GroupConsumer earliest via
  ListOffsets
- [V80_SPEC.md](./V80_SPEC.md) — Rust heartbeat retry pattern this
  copies
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V67_SPEC.md](./V67_SPEC.md) — auto offset reset
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
