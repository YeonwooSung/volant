# v0.234 — Native Fetch honors group assignment trailer

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the dual-consume window: native **Fetch** with a
group+member trailer is allowed only if that member currently owns the
partition. After eager reassign, the old owner can no longer read
stolen partitions.

This is residual **v0.234**. It is **not** Kafka Fetch (key 1 has no
group fields). Kafka Fetch stays unfiltered. Do **not** change Join
park / GroupState / SCRAM / txn. Do **not** add Kafka keys. Do **not**
change `join()` signature.

## Goals

1. `GroupCoordinator::member_owns(group_id, member_id, topic, partition) -> Owns`.
2. Native `Request::Fetch` optional `group_id` / `member_id` trailer
   after existing fields.
3. `net/dispatch.rs` Fetch arm: both non-empty → `member_owns`.
4. `GroupConsumer::poll` (Rust / Python / Go / Java) sends the trailer.
5. Thin `Client::fetch` / `fetch_opts` stay unfiltered (empty trailer).

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka Fetch (key 1) group filter | Key 1 has no group fields; leftover |
| Filtering clients that omit the trailer | Admin / CLI / old clients stay open |
| Join park / GroupState / SCRAM / txn | Orthogonal |
| New Kafka API keys | Frozen |
| Changing `join()` | Siblings own join |

## `Owns`

| Variant | Meaning | Fetch `error_code` |
|---------|---------|-------------------:|
| `Allow` | Live member is assigned this partition | 0 + today's fetch |
| `UnknownMember` | Unknown group or unknown member | **10**, empty records |
| `NotAssigned` | Live member does not own this partition | **9**, empty records |

Unknown group is `UnknownMember`. Empty `group_id` **or** empty
`member_id` → caller skips the check (admin / CLI / old clients).

## Wire (native Fetch)

```
string topic
u32    partition
u64    from_offset
u32    max_messages
u32    max_bytes
u32    max_wait_ms
string group_id     // v0.234; omit on legacy
string member_id    // v0.234; omit on legacy
```

Encode after existing fields. Decode: if bytes remain, read two
strings; else empty. Legacy Fetch unchanged → unfiltered.

ReplicaFetch is unchanged.

## Clients

- Rust `Client::fetch` / `fetch_default` / `fetch_opts` write an empty
  trailer (unfiltered).
- `Client::fetch_opts_for(group_id, member_id, …)` writes the trailer.
- `GroupConsumer::poll` uses `self.group_id` + current `member_id`.
- Language GroupConsumer poll sends the same trailer. Thin Client
  fetch stays unfiltered.

## Tests

```bash
cargo test -p volant-protocol --lib -- --test-threads=1
cargo test -p volant-broker --lib group -- --test-threads=1
cargo test -p volant-client --lib -- --test-threads=1
cargo test -p volant-broker --test v234_fetch_group -- --test-threads=1
```

| Case | Expect |
|------|--------|
| A joins, syncs, assigned all; Fetch trailer A + partition 0 | data, error **0** |
| B joins (A synced first); A Fetch trailer on a partition now owned by B | **9**, empty |
| Fetch without trailer | still reads any partition |
| Unknown `member_id` | **10**, empty |
| Legacy Fetch decode (no trailer) | empty group/member |

## Honesty leftovers

- **Kafka Fetch (key 1) is still open.** No group fields on that API.
- Dual-consume remains for clients that omit the trailer (admin / CLI /
  old native clients, Kafka clients).
- Not Kafka consumer-group Fetch / IncrementalFetch.
- Not a new opcode; native Fetch (opcode 2) only.

## Related

- [V227_SPEC.md](./V227_SPEC.md) — park Join until SyncGroup
- [V215_SPEC.md](./V215_SPEC.md) — SyncGroup fence
- [PHASE3_SPEC.md](./PHASE3_SPEC.md) — native groups
