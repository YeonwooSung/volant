# v0.83 — Rust OffsetCommit / OffsetFetch / DeleteOffsets retry

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V78_SPEC.md](./V78_SPEC.md): language
offset admin already shares `max_retries` (default **0**). Rust
`commit_offsets` / `fetch_offsets` / `delete_offsets`
(`crates/volant-client/src/client.rs`) do a single `round_trip`.
`GroupConsumer::commit` inherits via `commit_offsets`.

Reuse `ClientConfig.max_retries` / `retry_backoff_ms` (produce /
heartbeat) and `is_transient_error_code` / `is_transient_transport`.
No new public methods. Do **not** retry ListOffsets, LeaveGroup,
CreateTopic, or Heartbeat (heartbeat already retried in v0.80).

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients (they already
did this in v0.78).

## Goals

1. Extra OffsetCommit / OffsetFetch / DeleteOffsets attempts after the
   first on **transient** errors only. Budget is independent of
   `max_redirects`.
2. Same transient set as produce / heartbeat /
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
   - ListOffsets / LeaveGroup / CreateTopic / Heartbeat
4. Default `max_retries=0` so existing offset tests stay valid (no
   extra OffsetCommit / OffsetFetch / DeleteOffsets attempts).
5. Sleep between retry attempts using `retry_backoff_ms` (0 allowed in
   tests).
6. No new public methods. Existing `commit_offsets` / `fetch_offsets`
   / `delete_offsets` signatures stay. `GroupConsumer::commit` already
   calls `commit_offsets` and inherits the retry.

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

**Transport:** `Error::Io` from the TCP layer (same as produce).

**Not retried here:**

- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  v0.72 did not wrap these RPCs; this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Protocol / constructor errors.
- ListOffsets / LeaveGroup / CreateTopic / Heartbeat.

## Non-goals

| Deferred | Why |
|----------|-----|
| ListOffsets retry | Sibling v0.84 |
| LeaveGroup / JoinGroup / CreateTopic retry | Not this slice |
| Produce-batch / fetch / heartbeat retry changes | Offset admin only |
| Language clients | Already have this (v0.78) |
| Kafka `retries` / OffsetCommit vN | Native opcodes only; not Kafka |
| Changing the broker / protocol | Frozen |
| New native opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Existing offset signatures and constructors are unchanged.
Offset admin now shares produce / heartbeat knobs:

```rust
Client::connect(ClientConfig {
    max_retries: 0,        // default
    retry_backoff_ms: 50,  // default; 0 allowed in tests
    ..Default::default()
})
client.commit_offsets("g", "", 0, entries).await?;
client.fetch_offsets("g", entries).await?;
client.delete_offsets("g", entries).await?;
```

Default is **0 extra attempts**. `GroupConsumer::commit` calls
`Client::commit_offsets` and inherits.

## Semantics

Same budget as produce / heartbeat (independent of redirect):

- Default `max_retries=0`: first transient 7 raises; one OffsetCommit
  RPC.
- `max_retries=2`, backoff 0: Timeout then ok → two OffsetCommit RPCs,
  success. Same for OffsetFetch and DeleteOffsets.
- First OffsetCommit **2** (not found) raises immediately; no retry.
- Exhausted retries: always 7 with `max_retries=2` → raise 7 after
  `1 + max_retries` OffsetCommit RPCs.
- Transport fail then ok with `max_retries >= 1` → success.

## Tests

Tiny protocol stub that queues OffsetCommit / OffsetFetch /
DeleteOffsets error codes:

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first OffsetCommit 7 | one RPC; 7 surfaced |
| `max_retries=2`, backoff 0, first 7 then 0 | success; two OffsetCommit RPCs |
| OffsetFetch 7 then 0 | success; two OffsetFetch RPCs |
| DeleteOffsets 7 then 0 | success; two DeleteOffsets RPCs |
| first OffsetCommit 2 (not found) | immediately 2; one RPC |
| Exhaust (always 7) | 7 after `1+max_retries` OffsetCommit RPCs |

Existing v44 / v60 / v67 / v73 / v79 / v80 tests must still pass.

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | offset-admin retry helper + wrap |
| `crates/volant-client/src/config.rs` | comments: knobs also apply to offset admin |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v83_offset_admin_retry.rs` | queued-code stub |
| `docs/V83_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** `retries` / OffsetCommit versions.
- **Default 0** (same as produce / heartbeat / language clients).
- **No Kafka API keys / opcodes / Phase 155.**
- ListOffsets / LeaveGroup / CreateTopic are unchanged.
- Heartbeat already retried (v0.80).
- Language clients already have this (v0.78); this slice is Rust only.
- Error 13 / 14 are not redirected here (v0.72 leftover).
- Not a fully concurrent consumer. One TCP connection.

## Merge notes

Sibling **v0.84** also edits `client.rs` (`list_offsets`). Keep this
hunk local to `commit_offsets` / `fetch_offsets` / `delete_offsets` +
the `offset_admin_round_trip` helper. Do **not** wrap `list_offsets`.
Reuse `is_transient_error_code` / `is_transient_transport` and the
existing backoff field.

## Related

- [V78_SPEC.md](./V78_SPEC.md) — language OffsetCommit / OffsetFetch /
  DeleteOffsets retry leftover this closes
- [V80_SPEC.md](./V80_SPEC.md) — Rust heartbeat retry
- [V74_SPEC.md](./V74_SPEC.md) — language heartbeat retry
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this extends
- [V66_SPEC.md](./V66_SPEC.md) — fetch retry
- [V72_SPEC.md](./V72_SPEC.md) — admin 14 redirect (did not wrap
  OffsetCommit / OffsetFetch / DeleteOffsets)
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust `max_retries` / backoff
