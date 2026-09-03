# v0.113 — Rust ListOffsets NotLeader redirect

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V84_SPEC.md](./V84_SPEC.md): Rust
`list_offsets` retries transient 6/7/15/16 + Io. It does **not**
redirect on error **13** (`NotLeaderForPartition`). Produce / Fetch
already use `redirect_to_leader` + `max_redirects` for 13.

Reuse existing `redirect_to_leader` + `max_redirects` (v0.43 / Phase
8). Keep existing transient retry (`max_retries`, default **0**). 13
uses an independent `redirect_attempt` counter.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / language clients. Do **not** wrap
`delete_records` (sibling v0.111) or change `metadata()`’s topic list
(sibling v0.114).

## Goals

1. On ListOffsets typed `error_code == 13` or
   `Response::Error { code: 13 }`: if
   `redirect_attempt + 1 < 1 + max_redirects` and `redirect_to_leader`
   succeeds, retry the **same** request.
2. Partition for the helper:
   `partitions.first().copied().unwrap_or(0)`.
3. 13 uses **redirect budget** (`max_redirects`), not `max_retries`.
   13 does **not** increment `retry_attempt`. Transient 6 / 7 / 15 /
   16 stay on `max_retries`.
4. Budget is the same as produce/fetch:
   `1 + max_redirects` (default `max_redirects=1`).
   `max_redirects=0` surfaces 13 immediately (no Metadata).
5. **Not redirected:** 14, 2, 9 / 10 / 11, 17 / 18, 21, 22, Protocol.
6. No new public methods. Existing `list_offsets` signature stays.

## Non-goals

| Deferred | Why |
|----------|-----|
| Hunt algorithm change | Frozen (`redirect_to_leader` as-is) |
| Language clients | Sibling v0.112 |
| `delete_records` wrap | Sibling v0.111 |
| `metadata()` topic list | Sibling v0.114 |
| Kafka ListOffsets / API keys | Native 48/49 only |
| Broker / protocol | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

Same redirect budget as Produce/Fetch; independent of the v0.84
transient retry budget:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 13; no Metadata.
- Helper fail (unknown topic / unknown broker / empty host /
  reconnect fail): raise the original 13.
- Transient 6 / 7 / 15 / 16 and `Error::Io` still retry on
  `max_retries` (default 0) as in v0.84. A 7-then-0 ListOffsets
  still succeeds in two RPCs with `max_retries >= 1` and no
  Metadata.
- First ListOffsets **2** (not found) raises immediately; no retry,
  no Metadata.

## API

No new public methods. Existing:

```rust
client.list_offsets("t", vec![0]).await?;
```

Error 13 now follows Produce/Fetch redirect budget. Transient 7 still
uses `max_retries`. Not Kafka ListOffsets.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Tokio TCP stub (no live broker):

| Case | Expect |
|------|--------|
| First ListOffsets 13 + Metadata leader on second broker then ok | success; 13 is redirect not retry |
| `max_redirects=0` + first 13 | 13; no Metadata |
| `max_retries=2`, first 7 then 0 | two ListOffsets, no Metadata |
| First 2 with `max_retries=2` | immediately 2 |

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | 13 arm inside `list_offsets` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/src/config.rs` | `max_redirects` comment: ListOffsets 13 |
| `crates/volant-client/tests/v113_list_offsets_13.rs` | queued-code stub |
| `docs/V113_SPEC.md` | This spec |

Existing v67 / v71 / v73 / v84 stubs already answer ListOffsets and
must keep passing.

## Honesty leftovers

- Redirect still uses the existing `redirect_to_leader` helper
  (Metadata topic/partition leader). Hunt is unchanged.
- Not Kafka ListOffsets / `NOT_LEADER_OR_FOLLOWER`.
- Default `max_retries` stays **0**.
- `delete_records` is not wrapped (sibling v0.111).
- `metadata()` topic list is unchanged (sibling v0.114).
- Language clients are a sibling residual (v0.112).
- Broker / protocol still do not change whether ListOffsets returns
  13 today.
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling slices **v0.111** / **v0.114** also edit `client.rs`. When
merging:

- **Keep the 13 arm inside `list_offsets`** (hunk is local to that
  method). Do not drop the v0.84 transient retry.
- Do not wrap `delete_records`.
- Do not change `metadata()`’s topic list.
- Do not change language clients, broker, or protocol.

## Related

- [V84_SPEC.md](./V84_SPEC.md) — Rust ListOffsets transient retry
  leftover this extends
- [V43_SPEC.md](./V43_SPEC.md) — Produce/Fetch leader redirect
- [V65_SPEC.md](./V65_SPEC.md) — language DeleteRecords 13
- [V50_SPEC.md](./V50_SPEC.md) — language ListOffsets
