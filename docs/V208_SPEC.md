# v0.208 — Rust GroupConsumer SyncGroup peek after join

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Phase 155 leftover. Native SyncGroup opcodes **116 / 117**
already exist ([V206_SPEC.md](./V206_SPEC.md)). Thin `Client::sync_group`
is peek/confirm of the JoinGroup assignment. This slice has Rust
`GroupConsumer` call that RPC after a successful JoinGroup (including
rejoin). This is **not** Kafka CompletingRebalance. Kafka API key **14**
is unchanged. No new opcodes, no homemade Raft, no language-client
changes.

## Goals

1. After `join_group_with_instance` succeeds in `do_join`, call
   `client.sync_group(group_id, &result.member_id, result.generation)`.
2. If Ok and **non-empty**, use that assignment as `join_assignment`.
3. If Ok and **empty**, or **Err**, keep the JoinGroup assignment
   (best-effort peek; join must not fail because SyncGroup failed).
4. Existing range override still runs after the peek.
5. Do **not** increment `heartbeat_count` on SyncGroup.
6. Rejoin (poll / background heartbeat `needs_rebalance`) goes through
   the same `do_join` path.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka CompletingRebalance / PreparingRebalance | Coordinator rewrite; Empty/Stable only |
| Apply leader assignment bytes | Broker ignores them (v0.206 honesty) |
| New Kafka API key / opcodes | 116/117 already shipped; key 14 stays |
| Python / Go / Java GroupConsumer | Rust-only residual |
| Range assignor via SyncGroup | Still DescribeGroup |
| Flip openraft / grow homemade Raft | Other 155 PRs |
| Change `join_group_with_instance` | Sibling v0.210; keep this hunk in `group.rs` |

## Semantics

```
do_join
  │
  ├─ join_group_with_instance  (must succeed)
  │
  ├─ sync_group(group_id, member_id, generation)
  │       │
  │       ├─ Ok(non-empty) → join_assignment = peeked
  │       ├─ Ok(empty)     → keep JoinGroup assignment
  │       └─ Err           → keep JoinGroup assignment
  │
  └─ if assignor == "range"
         apply_range_override(join_assignment)
     else
         honor join_assignment
```

HeartbeatCount / `heartbeat_count` stays a Heartbeat RPC counter
(poll + background). SyncGroup is not a Heartbeat.

## Tests

```bash
cargo test -p volant-client --lib -- --test-threads=1
cargo test -p volant-client --test v208_group_sync -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Join + non-empty SyncGroup | assignment from SyncGroup; 1 Sync RPC |
| Join + empty SyncGroup | JoinGroup assignment kept |
| Join + SyncGroup error 10 | JoinGroup assignment kept; join Ok |
| `assignor="range"` after peek | DescribeGroup range still wins |
| Rejoin via Heartbeat 9 | second Join + second Sync |
| `heartbeat_count` after join | **0** (SyncGroup does not increment) |

Existing GroupConsumer fake clients stub `Request::SyncGroup` (empty
assignment → keep Join).

| File | What |
|------|------|
| `crates/volant-client/src/group.rs` | `do_join` peek after Join |
| `crates/volant-client/src/lib.rs` | crate-doc sentence |
| `crates/volant-client/tests/v208_group_sync.rs` | fake TCP |
| `crates/volant-client/tests/v44_group_heartbeat.rs` | SyncGroup stub |
| `crates/volant-client/tests/v60_group_auto_commit.rs` | SyncGroup stub |
| `crates/volant-client/tests/v67_group_auto_offset_reset.rs` | SyncGroup stub |
| `crates/volant-client/tests/v73_group_range_assign.rs` | SyncGroup stub |
| `crates/volant-client/tests/v76_group_poll_fetch_knobs.rs` | SyncGroup stub |
| `docs/V208_SPEC.md` | This spec |

## Honesty leftovers

- SyncGroup is peek, not CompletingRebalance.
- Leader assignment bytes are still ignored (empty).
- Range assignor is still DescribeGroup (no generation barrier).
- Empty first Join is still not retried (v0.205).
- Kafka stays 38 keys. Key 14 unchanged.
- Empty/Stable only. No PreparingRebalance.

## Merge notes

v0.210 edits `join_group_with_instance` in `client.rs`. Keep this hunk
in `group.rs` only.

## Related

- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — GroupConsumer may call SyncGroup after join
- [V206_SPEC.md](./V206_SPEC.md) — native SyncGroup 116/117
- [V73_SPEC.md](./V73_SPEC.md) — range assignor (DescribeGroup)
- [V205_SPEC.md](./V205_SPEC.md) — JoinGroup retry (guarded)
