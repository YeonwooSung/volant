# v0.112 — ListOffsets NotLeader redirect on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V82_SPEC.md](./V82_SPEC.md):
language ListOffsets already retries transient **6 / 7 / 15 / 16** and
TCP/IO (`max_retries`, default **0**). It treats error **13**
(`NotLeaderForPartition`) as not retried. Produce / Fetch /
DeleteRecords already redirect on 13 via `_redirect_to_leader` /
`redirectToLeader` and `max_redirects`.

Reuse that same helper and an **independent** redirect counter
(`1 + max_redirects`). Do **not** count 13 as a transient retry. Do
**not** wrap Auth / SCRAM / DeleteRecords / `reconnect`. Do **not**
change the broker, protocol, or Rust client (sibling v0.113).

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change crate / client version **0.2.0**.

## Goals

1. Inside existing ListOffsets (`list_offsets` / `ListOffsets` /
   `listOffsets`): on **13** (typed `error_code` or `BrokerError` /
   Error opcode), if redirect attempts remain, call
   `_redirect_to_leader` / `redirectToLeader` and resend the **same**
   ListOffsets payload.
2. **13 is not a transient retry** — do not increment
   `retry_attempt`. Independent counters: redirect budget is
   `1 + max_redirects`; transient 6 / 7 / 15 / 16 + TCP stay on
   `max_retries` (v0.82, unchanged).
3. Partition for the helper: first requested partition if the request
   listed any; if the request is “all partitions” (empty list), use
   **0** (same as a single-partition topic). If the helper returns
   false, raise 13.
4. **Not redirected / not retried extra:** 14, 9 / 10 / 11, 2,
   17 / 18, 21, 22, Protocol.
5. `max_redirects=0` raises on the first 13 with no Metadata.
6. No new public methods. Existing signatures stay.

## Semantics

Same redirect budget as Produce / Fetch / DeleteRecords:

- Default `max_redirects=1`: first ListOffsets 13 → Metadata names a
  **leader** for topic/partition (`controller_id` not needed) → second
  ListOffsets ok on the leader. Follower: 1 ListOffsets + 1 Metadata;
  leader: 1 ListOffsets.
- Typed 13 (`ListOffsetsResponse.error_code=13`, not Error opcode)
  then ok: same path.
- `max_redirects=0`: first 13 raises immediately; no Metadata.
- `max_retries=2`, first **7** then 0: still **2** ListOffsets, no
  Metadata (13 wrap must not break v0.82 retry).
- First **2** with `max_retries=2`: still immediately 2.

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust `Client::list_offsets` 13 redirect | Sibling v0.113 |
| Auth / SCRAM / DeleteRecords / `reconnect` wrap | Other residuals |
| Broker / protocol / Kafka API keys | Frozen |
| Changing default `max_retries` (stays 0) | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

No new public methods. Existing ListOffsets now follows Produce/Fetch
redirect on 13:

```python
c = Client("127.0.0.1:9092", max_redirects=1)  # default
c.list_offsets("t")
c.list_offsets("t", [0, 1])
```

```go
c.SetMaxRedirects(1) // Dial default
c.ListOffsets(topic, nil)
c.ListOffsets(topic, []uint32{0, 1})
```

```java
c.setMaxRedirects(1); // connect default
c.listOffsets(topic);
c.listOffsets(topic, 0, 1);
```

Default `max_retries` stays **0**. Transient retry is unchanged
(v0.82).

## Tests

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| First ListOffsets 13 + leader Metadata, then ok | success; follower 1 ListOffsets + 1 Metadata; leader 1 ListOffsets |
| Typed 13 (`error_code=13`, not Error opcode) then ok | success; same redirect counts |
| `max_redirects=0` + first 13 | raise 13; no Metadata |
| `max_retries=2`, first 7 then 0 | still 2 ListOffsets, no Metadata |
| First 2 with `max_retries=2` | still immediately 2 |

## Honesty leftovers

- **Not Kafka** `retries` / ListOffsets versions.
- **Default `max_retries=0`** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- Error **14** is not redirected here (NotController).
- Rust `Client::list_offsets` is unchanged (language clients only).
- Auth / SCRAM / DeleteRecords / `reconnect` are unchanged.

## Merge notes

Sibling **v0.115** (reconnect) also edits the three `Client` files.
This hunk is local to `list_offsets` / `ListOffsets` / `listOffsets`.
Keep the v0.82 transient retry. Do not wrap Auth / SCRAM /
DeleteRecords / `reconnect`.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (`list_offsets`)
- Go `clients/go/client.go` (`ListOffsets`)
- Java `clients/java/src/main/java/io/volant/Client.java` (`listOffsets`)

## Related

- [V82_SPEC.md](./V82_SPEC.md) — ListOffsets transient retry leftover
  this extends
- [V65_SPEC.md](./V65_SPEC.md) — DeleteRecords 13 redirect
- [V43_SPEC.md](./V43_SPEC.md) — Produce/Fetch redirect
- [V50_SPEC.md](./V50_SPEC.md) — ListOffsets on Python / Go / Java
- [V70_SPEC.md](./V70_SPEC.md) — GroupConsumer earliest via ListOffsets
