# v0.209 — generate member_id on empty first JoinGroup

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Language **Client.join_group** (Python / Go / Java): when
**both** `member_id` and `group_instance_id` are empty, generate a
member id **before the first send** so JoinGroup retry is safe (no
ghost member). Do **not** change Rust (sibling v0.210). Do **not**
change the broker.

Today empty first join is one-shot because a lost success + retry
would create a second UUID on the broker. If the client picks the id
first, a retry hits the existing-member path.

## Goals

1. Inside the shared join implementation, before encode/send:

   ```
   if member_id empty AND instance_id empty:
       member_id = new random id
   ```

2. The existing retry guard (v0.205) then sees a non-empty
   `member_id` and may retry.
3. Do **not** generate an id when `group_instance_id` is set (static
   id is `static:{instance}`). If `member_id` is already non-empty,
   keep it.
4. JoinGroup **response** `member_id` remains source of truth for
   GroupConsumer (broker may echo the same id).

## ID format

| Client | Generator |
|--------|-----------|
| Python | `uuid.uuid4().hex` (stdlib) |
| Java | `UUID.randomUUID().toString()` |
| Go | 16 random bytes hex via `crypto/rand` + `encoding/hex` — **no new go.mod dependency** |

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust Client join generate | Sibling v0.210 |
| Broker JoinGroup | Existing-member path already handles a known id |
| JoinGroup **response** codec | Sibling v0.211 |
| GroupConsumer encode | v0.207; still calls Client join |
| New opcodes / Kafka API keys | Frozen |

## Semantics

- Both ids empty: generate, encode, then the v0.205 retry loop.
- Stored `member_id`: unchanged; still retries.
- Non-empty `group_instance_id`: still sends empty `member_id`; still
  retries (static membership).
- Response `member_id` is what GroupConsumer stores.

## Tests

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_join_member_id tests.test_group -q
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

| Case | Expect |
|------|--------|
| empty member + empty instance | **non-empty** `member_id` on the wire |
| empty member + static instance | empty `member_id` on the wire |
| explicit `member_id` | unchanged on the wire |
| empty first join + transient then ok | **2** Join RPCs (retry now safe) |

## Files

| File | What |
|------|------|
| `clients/python/src/volant/client.py` | `join_group` fill-in before encode |
| `clients/go/client.go` | `joinGroup` fill-in before encode |
| `clients/java/.../Client.java` | shared `joinGroup` fill-in |
| `clients/python/tests/test_join_member_id.py` | wire cases |
| Go tests next to join retry | same 3 wire cases |
| Java unique test method names | same 3 wire cases |
| Client READMEs | one sentence |
| `docs/V209_SPEC.md` | This spec |

## Honesty leftovers

- Rust still one-shots empty first Join until v0.210.
- Default `max_retries=0`.
- Not Kafka `retries` / JoinGroup versions.
- No new opcodes / Kafka keys / broker change.

## Merge notes

v0.207 edits GroupConsumer, not Client join encode. v0.211 may edit
JoinGroup **response** codec. Keep this hunk local to request
`member_id` fill-in before send.

## Related

- [V205_SPEC.md](./V205_SPEC.md) — JoinGroup retry when member or instance id is set
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155
