# Phase 3 — Broker Groups Plan (Iteration 1)

## Goals

Implement server-side consumer group coordination in `volant-broker`:

1. File-backed durable offset store under `{data_dir}/__consumer_offsets/`
2. `GroupCoordinator` with join / heartbeat / leave / commit / fetch / expire_sessions
3. Range assignor (stable-sorted members; uneven `n/m` split)
4. Eager rebalance on join / leave / session timeout (bump generation, reassign all)
5. Net dispatch for opcodes 6–10 with embedded `error_code`
6. Session expiry on group RPC path + interval in serve loop

## Modules

```
volant-broker/
  assignor.rs      # range_assign(member_ids, n_partitions) -> Map<member, partitions>
  offset_store.rs  # file-backed commit/fetch under data_dir/__consumer_offsets
  group.rs         # GroupCoordinator + Group + Member state
  broker.rs        # holds GroupCoordinator; partition_count used at rebalance
  net.rs           # dispatch opcodes 6–10; expire on RPC + 1s interval
  lib.rs           # export new modules
```

## Protocol (light extension of volant-protocol)

Wire format per `docs/PHASE3_SPEC.md`. Keep Phase 2 opcodes 1–5 unchanged.

| Opcode | Request | Response |
|--------|---------|----------|
| 6 | OffsetCommit | OffsetCommit `{ error_code }` |
| 7 | OffsetFetch | OffsetFetch `{ error_code, entries }` |
| 8 | JoinGroup | JoinGroup `{ error_code, generation, member_id, assignment }` |
| 9 | Heartbeat | Heartbeat `{ error_code }` |
| 10 | LeaveGroup | LeaveGroup `{ error_code }` |

Error codes added: `RebalanceInProgress=9`, `UnknownMemberId=10`,
`IllegalGeneration=11`, `InconsistentGroupProtocol=12`.

Group RPC success frames always embed `error_code: u16` (0 = ok). Hard failures
still use `Response::Error` (opcode `0xFFFF`).

## Offset store

Path: `{data_dir}/__consumer_offsets/{group_id}/{topic}/{partition}`

File layout (LE):

```
offset: u64
meta_len: u16
metadata: [u8; meta_len]
```

- `commit`: write temp file → fsync → rename (or write + fsync in place)
- `fetch` one key: missing → `None` (wire uses `u64::MAX`)
- `fetch_all(group)`: walk group directory tree
- Unknown / not committed on wire: `offset = u64::MAX`

## GroupCoordinator

In-memory:

```
Group { group_id, generation, members: Map<member_id, Member> }
Member { member_id, session_timeout_ms, last_heartbeat, topics, assignment }
```

API:

| Method | Behavior |
|--------|----------|
| `join` | Empty member_id → uuid; add/update member; bump gen; reassign all; return this member's assignment |
| `heartbeat` | Unknown member → 10; gen mismatch → 9; else update last_heartbeat |
| `leave` | Remove member; bump gen; reassign remaining |
| `commit_offsets` | gen=0 skips check; else member+gen validate; persist via OffsetStore |
| `fetch_offsets` | empty entries → all for group; missing → MAX |
| `expire_sessions` | Drop members where `now - last_hb > timeout`; if any dropped, rebalance |

Rebalance needs partition counts: coordinator accepts `Fn(&str) -> u32` (or
`Option<u32>`) supplied by `Broker` / net using `partition_count`. Topics with
unknown/0 partitions contribute no assignment.

Range assignor (per topic, members subscribed to that topic, stable-sorted by id):

```
base = n / m
extra = n % m
member i gets base + (i < extra ? 1 : 0) consecutive partitions
```

## Net dispatch

- Decode full group request bodies
- Call `broker.groups()` / coordinator methods
- Map group error codes into response `error_code` field (not always Error frame)
- Before each group RPC (and on 1s tokio interval in accept loop): `expire_sessions`
- Join needs partition counts from broker for subscribed topics

## Tests

1. **assignor unit:** uneven n/m (e.g. 5 partitions / 2 members → 3+2; 7/3 → 3+2+2)
2. **two members join** same group/topic with 4 partitions → disjoint full cover
3. **leave** → remaining member gets all partitions; generation bumped
4. **offset commit + fetch** durable across `GroupCoordinator` recreate / process reopen
5. Existing broker tests stay green

## Files touched

- `docs/phase3/broker-groups-plan.md` (this file)
- `docs/phase3/broker-groups-review.md`
- `crates/volant-protocol/src/{request,response,payload,lib}.rs`
- `crates/volant-broker/src/{lib,broker,net,assignor,offset_store,group}.rs`
- `crates/volant-broker/Cargo.toml` (uuid dep for member ids)
- `crates/volant-broker/tests/` (group coordinator integration)

## Non-goals (this agent)

- Client `GroupConsumer` API
- CLI group commands
- E2E TCP two GroupConsumers (client agent)
- Sticky / cooperative assignor
