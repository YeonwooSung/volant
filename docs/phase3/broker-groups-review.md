# Phase 3 — Broker Groups Review

## Iteration log

| Iter | Stage | Outcome |
|------|-------|---------|
| 1 | PLAN | Wrote `docs/phase3/broker-groups-plan.md` |
| 1 | CODE | Protocol opcodes 6–10; assignor; offset_store; GroupCoordinator; Broker + net wire-up |
| 1 | TEST | `cargo test -p volant-protocol -p volant-broker` — all green after one compile fix |
| 1 | FIX | Removed invalid doc-comment on function parameter in `assignor.rs` |
| 1 | FIX | Client `error_from_code` match extended for new ErrorCode variants (compile break from protocol) |
| 1 | REVIEW | Checklist below — pass |

**Total iterations: 1** (two small compile fixes within the same iteration)

## Deliverable checklist

| # | Requirement | Status | Notes |
|---|-------------|--------|-------|
| 1 | File-backed durable offset store under data_dir | ✅ | `{data_dir}/__consumer_offsets/{group}/{topic}/{partition}`; LE u64 + u16 meta + bytes; fsync + atomic rename |
| 2 | GroupCoordinator join/heartbeat/leave/commit/fetch/expire | ✅ | `crates/volant-broker/src/group.rs` |
| 3 | Range assignor + unit tests (uneven n/m) | ✅ | 5/2 → 3+2; 7/3 → 3+2+2; more members than partitions |
| 4 | Join/leave/timeout bump generation + reassign all | ✅ | Eager rebalance; expire reassigns remaining |
| 5 | Net dispatch opcodes 6–10; embed error_code | ✅ | `net.rs` maps group results into response `error_code` |
| 6 | Session expiry on RPC path and/or serve interval | ✅ | Expire on every group RPC; 1s tokio interval in `serve_listener` |
| 7 | Tests: two members split; leave reassigns; offsets reopen | ✅ | Unit + `tests/group_coordinator.rs` |

## Protocol extensions

- Opcodes: OffsetCommit=6, OffsetFetch=7, JoinGroup=8, Heartbeat=9, LeaveGroup=10
- Error codes: RebalanceInProgress=9, UnknownMemberId=10, IllegalGeneration=11, InconsistentGroupProtocol=12
- Wire format matches `docs/PHASE3_SPEC.md` (LE ints, `u16`+UTF-8 strings)
- Phase 2 opcodes 1–5 unchanged

## Files added/changed

**Added**
- `crates/volant-broker/src/assignor.rs`
- `crates/volant-broker/src/offset_store.rs`
- `crates/volant-broker/src/group.rs`
- `crates/volant-broker/tests/group_coordinator.rs`
- `docs/phase3/broker-groups-plan.md`
- `docs/phase3/broker-groups-review.md`

**Modified**
- `crates/volant-broker/src/{lib,broker,net}.rs`
- `crates/volant-broker/Cargo.toml` (uuid runtime dep)
- `crates/volant-protocol/src/{request,response,payload,lib}.rs`
- `crates/volant-client/src/client.rs` (exhaustive ErrorCode match)

## Test results (iteration 1)

```
volant-broker unit:     15 passed
volant-broker tests:    group_coordinator 4, durable 3, inprocess 1, partition_select 2
volant-protocol unit:   6 passed (incl. phase3_group_roundtrips)
```

## Known gaps / out of scope for this agent

- Client `GroupConsumer` API (client agent)
- CLI group commit/fetch-offsets (CLI agent)
- E2E TCP with two GroupConsumers
- Cooperative/sticky assignor

## Review notes

1. **Stale join assignment:** When a second member joins, the first member's previously returned assignment is stale (generation bumped). Clients must re-join or observe heartbeat error 9 — correct per eager rebalance design.
2. **Admin commits:** `generation == 0` skips member/generation checks (spec).
3. **Unknown offsets:** wire `u64::MAX`.
4. **Path sanitization:** group/topic components rejecting `/`, `\`, `..` map to `_` — fine for Phase 3; tighten if multi-tenant later.
