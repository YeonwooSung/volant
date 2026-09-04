# v0.207 — GroupConsumer SyncGroup peek after join

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Phase 155 leftover. Language **GroupConsumer** (Python / Go /
Java) calls native **SyncGroup** (opcodes 116/117) after a successful
JoinGroup, including rejoin. This is peek/confirm. It is **not** Kafka
CompletingRebalance.

`Client.sync_group` / `SyncGroup` / `syncGroup` already exist (Phase
155 PR4 / [V206_SPEC.md](./V206_SPEC.md)). This residual does **not**
reimplement the RPC, change the Client methods, change Rust (sibling
v0.208), add Kafka keys, or grow homemade Raft.

## Goals

1. After `_do_join` / `doJoin` gets a successful `JoinGroupResult`,
   call `sync_group(group, member_id, generation)` with the **new**
   member_id and generation from Join.
2. Non-empty SyncGroup assignment becomes the fetch set (then range
   override still runs if assignor is range).
3. Empty SyncGroup assignment keeps the JoinGroup assignment.
4. Any SyncGroup error (including 9 / 10 / 11) is a failed peek:
   keep the JoinGroup assignment. Do **not** increment
   `heartbeat_count` / `HeartbeatCount`.
5. Range assignor still uses DescribeGroup. Do not change range.

## Non-goals

| Deferred | Why |
|----------|-----|
| Reimplement SyncGroup RPC / change Client methods | Phase 155 PR4 / v0.206 |
| Rust GroupConsumer peek | Sibling v0.208 |
| Kafka CompletingRebalance / PreparingRebalance | Coordinator rewrite; Empty/Stable only |
| Change range assignor | Still DescribeGroup; sibling v0.211 may add a members trailer |
| Change `Client.join_group` | Sibling v0.209 |
| New Kafka API key / homemade Raft | Frozen; key 14 already in the 38-key table |

## API

Existing Client methods are unchanged. GroupConsumer calls them after
join:

```python
asgn = c.sync_group(group, member_id, generation)  # list[Assignment]
```

```go
asgn, err := c.SyncGroup(group, memberID, generation) // []Assignment
```

```java
List<Codec.Assignment> asgn = backend.syncGroup(group, memberId, generation);
```

Java `GroupConsumer.Backend` gains `syncGroup` so the fake and
`ClientBackend` share the same peek path. `Client.syncGroup` is
unchanged.

## Semantics

After a successful Join (including rejoin on heartbeat 9 / 10 / 11):

1. Call SyncGroup with the new `member_id` and `generation` from Join.
2. If SyncGroup returns a **non-empty** assignment, use that as the
   fetch set. Range assignor still replaces it afterwards.
3. If SyncGroup returns an **empty** assignment, keep the JoinGroup
   assignment.
4. If SyncGroup raises (including 9 / 10 / 11, transport, protocol),
   treat it as a failed peek and **keep the JoinGroup assignment**.
   Peek is best-effort. Do not swallow into a second rejoin from
   this call; the existing rejoin-on-heartbeat path still sees later
   9 / 10 / 11 on Heartbeat.
5. Do not increment `heartbeat_count` on SyncGroup.

Range assignor is unchanged (DescribeGroup members; describe failure
falls back to solo).

## Tests

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_group -q
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

No full Python discover.

| Case | Expect |
|------|--------|
| Join issues SyncGroup | Called with new member_id + generation; `heartbeat_count` stays 0 |
| Non-empty SyncGroup assignment | Becomes the fetch set (then range still overrides if range) |
| Empty SyncGroup assignment | JoinGroup assignment kept (existing fakes return empty) |
| SyncGroup error 9 / 10 / 11 | JoinGroup assignment kept; join still succeeds |
| Existing GroupConsumer fakes | Implement `sync_group` / `SyncGroup` / `syncGroup` |

## Honesty leftovers

- SyncGroup is peek, not CompletingRebalance.
- Leader assignment bytes are still ignored by the broker (v0.206).
- Peek is best-effort: any SyncGroup error keeps Join assignment.
- Range assignor is still DescribeGroup (no generation barrier).
- Kafka stays 38 keys. Key 14 unchanged. Native 116/117 is not a
  39th key.
- Rust GroupConsumer is sibling v0.208.
- Empty/Stable only. No PreparingRebalance.

## Merge notes

v0.209 edits `Client.join_group` (not GroupConsumer). v0.211 may
edit `group.py` range. Keep this hunk local to the SyncGroup call
after join.

Do **not** edit living docs (`TODO.md`, `ROADMAP.md`, root
`README.md`, `docs/INDEX.md`, `docs/history/PHASE_HISTORY.md`,
`docs/PHASE155_SPEC.md`, `docs/V02_FREEZE.md`).

## Related

- [V206_SPEC.md](./V206_SPEC.md) — native SyncGroup opcodes 116/117
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155
- [PHASE26_SPEC.md](./PHASE26_SPEC.md) — Kafka SyncGroup honesty
