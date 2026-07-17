# Phase 3 Protocol — Review

## Checklist (vs PHASE3_SPEC)

| Item | Status | Notes |
|------|--------|-------|
| OffsetCommit request fields (group_id, member_id, generation, entries) | ✅ | `request.rs` |
| OffsetCommit response `error_code: u16` | ✅ | |
| OffsetFetch request (group_id, empty entries = all) | ✅ | count 0 encodes "all" |
| OffsetFetch response entries (offset u64::MAX = unknown) | ✅ | semantic only; wire is plain u64 |
| JoinGroup req/resp opcode 8 | ✅ | assignment as `Vec<TopicPartition>` |
| Heartbeat req/resp opcode 9 | ✅ | |
| LeaveGroup req/resp opcode 10 | ✅ | |
| ErrorCode 9–12 | ✅ | RebalanceInProgress…InconsistentGroupProtocol |
| LE multi-byte integers | ✅ | existing `put_*_le` / `get_*_le` path |
| Strings: `u16 len` + UTF-8 | ✅ | shared helpers |
| Roundtrip unit tests for every new req/resp | ✅ | see test list |
| No broker/client TCP logic implemented | ✅ | only match-arm compile fixes |

## Test results

```
cargo test -p volant-protocol  →  15 passed
cargo build --workspace        →  ok
```

### Phase 3 roundtrip coverage

- `offset_commit_request_roundtrip` / `offset_commit_response_roundtrip`
- `offset_fetch_request_roundtrip` (empty + filtered) / `offset_fetch_response_roundtrip`
- `join_group_roundtrip` (req + resp with assignment)
- `heartbeat_roundtrip` (incl. RebalanceInProgress error code)
- `leave_group_roundtrip`
- `group_error_codes`, `phase3_opcodes_parse`, `pack_join_group_frame`

## Findings

### F1 — Workspace compile break from enum expansion (fixed, iteration 1)

Changing `Request::OffsetCommit` / `OffsetFetch` unit variants to field structs, and adding
opcodes 8–10, broke:

1. `volant-broker` match on `Request` → fixed with `{ .. }` + NotImplemented arms
2. `volant-client` exhaustive match on `ErrorCode` → map new codes to `Error::Protocol`

These are minimal compile-only fixes; no group TCP logic added.

### F2 — Public re-exports

New types re-exported from `lib.rs`:

- `OffsetCommitEntry`, `OffsetFetchEntry`
- `OffsetFetchResult`, `TopicPartition`

### F3 — Docs / deny(missing_docs)

All new public items have `///` docs. Crate still builds under `#![deny(missing_docs)]`.

## Iterations

| # | Action | Result |
|---|--------|--------|
| 1 | PLAN + CODE + TEST | protocol tests green; workspace failed on client ErrorCode match |
| 1b | FIX broker/client match arms | workspace builds; 15/15 protocol tests green |
| 2 | REVIEW | checklist clean; no further code changes required |

**Iteration count: 1** (with one compile fix pass before review closed)

## Residual risks (out of scope)

- Broker still returns NotImplemented for group/offset opcodes (broker agent owns dispatch)
- Client maps group ErrorCodes to generic Protocol; GroupConsumer agent may want typed handling later
