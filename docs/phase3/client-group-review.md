# Phase 3 Client — Group-Aware Consumer Review

**Iteration:** 1 (complete — tests green, no open findings)

## Checklist

- [x] `Client::join_group` / `heartbeat` / `leave_group` / `offset_commit` / `offset_fetch`
  - Implemented in `crates/volant-client/src/client.rs`
  - Exercised via mock TCP in `tests/group_mock_tcp.rs`
- [x] `GroupConsumer::join` / `poll` / `commit` / `leave` / `assignment`
  - Implemented in `crates/volant-client/src/group_consumer.rs`
  - Poll: heartbeat → on rebalance re-join + offset_fetch → fetch each assigned partition → track positions
  - Commit: next-read offset (last+1) per assigned partition
  - First assignment: OffsetFetch; `u64::MAX` / missing → start at 0
- [x] Config: `group_id`, `session_timeout_ms`, `max_messages` per poll
  - `GroupConsumerConfig` (+ optional `max_wait_ms`)
- [x] No `NotImplemented` on group APIs in client
- [x] Protocol group opcodes 6–10 encode/decode per PHASE3_SPEC
- [x] Error codes 9–12 mapped; rebalance detected via `is_rebalance_error`
- [x] docs + `missing_docs` (`#![deny(missing_docs)]`)

## Protocol support (light touch)

| Opcode | Type |
|--------|------|
| 6 | OffsetCommit |
| 7 | OffsetFetch |
| 8 | JoinGroup |
| 9 | Heartbeat |
| 10 | LeaveGroup |

Broker `net.rs` match arms updated so workspace compiles; group RPCs still return NotImplemented on real broker until coordinator lands (out of client ownership).

## Tests

| Suite | Result |
|-------|--------|
| `cargo test -p volant-protocol` | 7 passed (incl. group request/response roundtrips) |
| `cargo test -p volant-client` unit | 4 passed (opcodes, encode, rebalance mapping) |
| `tests/group_mock_tcp.rs` | 4 passed (RPC paths, poll/commit, rebalance rejoin, error surface) |
| `tests/e2e_tcp.rs` | 3 passed (existing produce/fetch e2e still green) |

## Findings

None open after iteration 1.

### Fixed during implementation

1. Mock JoinGroup borrow conflict (`entry` + later field access) — use get/insert instead of held `entry` ref.
2. Broker exhaustive match broken by expanded Request variants — light touch `NotImplemented` arms for group opcodes.

## Non-goals / deferred

- Real broker GroupCoordinator e2e (server-side Phase 3)
- Background heartbeat task (poll-driven heartbeats only)
- Cooperative sticky assignor

## Iteration log

- **Iteration 1:** Plan → protocol group payloads → Client group methods → GroupConsumer → mock tests → review. All green; no further iterations needed.
