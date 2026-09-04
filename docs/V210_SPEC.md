# v0.210 — generate member_id on empty first JoinGroup (Rust)

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V205_SPEC.md](./V205_SPEC.md):
JoinGroup retries only when `member_id` or `group_instance_id` is
non-empty. Empty first join stayed one shot because a lost success
plus retry would create a ghost member. If the **client** picks the
`member_id` before the first send, retry is safe.

Generate a UUID `member_id` when both ids are empty, **before** the
first Join send. The existing v0.205 retry guard then sees a
non-empty `member_id`. Do **not** generate when `group_instance_id`
is set. Do **not** change the broker. Do **not** change Python / Go /
Java.

This is residual **v0.210**. It is **not** Phase 155. It does **not**
add Kafka API keys, add native opcodes, change homemade Raft, or flip
openraft defaults.

## Goals

1. In `Client::join_group_with_instance`, when both `member_id` and
   `group_instance_id` are empty, set
   `member_id = uuid::Uuid::new_v4().to_string()` before
   `join_group_once` / the retry loop.
2. After fill-in, the v0.205 guard (`member_id` empty **and**
   `group_instance_id` empty) is false, so transient 6 / 7 / 15 / 16
   + TCP retry applies.
3. Do **not** generate when `group_instance_id` is set (static
   membership still sends empty `member_id`; broker derives
   `static:{id}`).
4. Explicit non-empty `member_id` is unchanged.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Broker-side empty-id UUID | Already exists; client must pick the id first |
| Generate when `group_instance_id` is set | Static membership uses `static:{id}` |
| Python / Go / Java first-join UUID | Out of slice |
| New opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Openraft default flip | Phase 155 PR5 |
| Crate 0.3.0 | After 155 ships, not during |

## API

Existing `join_group` / `join_group_with_instance` signatures stay.

```rust
client.join_group("g", "", 10_000, topics).await?; // generates UUID
client.join_group("g", "m-1", 10_000, topics).await?; // unchanged
client.join_group_with_instance("g", "", 10_000, topics, "i-1").await?; // empty member_id
```

## Semantics

- Both ids empty: generate a hyphenated UUID **once**, then send that
  `member_id` on every attempt of this call (including retry).
- Static `group_instance_id`: still encode empty `member_id`.
- Stored / explicit `member_id`: unchanged.
- Default `max_retries=0`. Sleep `retry_backoff` between attempts.

## Tests

Fake TCP stub that records decoded JoinGroup `member_id` +
`group_instance_id`.

```bash
cargo test -p volant-client --test v210_join_member_id --test v205_join_group_retry -- --test-threads=1
```

| Case | Expect |
|------|--------|
| empty member + empty instance | encodes non-empty UUID `member_id` |
| empty member + static instance | encodes empty `member_id` |
| explicit `member_id` | unchanged on the wire |

After fill-in, `v205_join_group_retry` empty+empty + transient then
ok is **2** Join RPCs (the guard sees the generated id).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | fill `member_id` before first send |
| `crates/volant-client/src/lib.rs` | crate-doc sentence |
| `crates/volant-client/tests/v210_join_member_id.rs` | fake TCP |
| `docs/V210_SPEC.md` | This spec |

Do **not** change broker / protocol / Python / Go / Java. Do **not**
run the full workspace.

## Honesty leftovers

- Python / Go / Java empty first Join is still one shot (v0.205).
- Broker still assigns a UUID when both ids arrive empty.
- Default `max_retries=0`.
- Not Kafka `retries` / JoinGroup versions.
- No new opcodes / Kafka keys / openraft default change.

## Merge notes

v0.208 edits `group.rs` only. Keep this hunk local to
`join_group_with_instance` member_id fill-in. Crate-doc keep-both
with v0.208.

## Related

- [V205_SPEC.md](./V205_SPEC.md) — JoinGroup retry when member or instance id is set
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155 (PR3 was Join retry)
- [PHASE12_SPEC.md](./PHASE12_SPEC.md) — static `group_instance_id`
