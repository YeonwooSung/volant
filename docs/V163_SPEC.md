# v0.163 — Go ListOffsetsAll

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V50_SPEC.md](./V50_SPEC.md): Java
already has `listOffsets(topic)` (all partitions). Python
`list_offsets(topic)` with `partitions=None` already lists all. Go
only has `ListOffsets(topic, partitions)` — nil/empty already means
all, but there is no named all-partition helper matching Java.

Add `Client.ListOffsetsAll`. Reuse `ListOffsets` (do not
reimplement the RPC). `ListOffsets(topic, partitions)` stays
unchanged. This is **not** Kafka ListOffsets.

This is residual **v0.163** (Go ListOffsetsAll). It is **not**
Phase 155. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, or change the broker, protocol, Rust, Python, or Java.

## Goals

1. Add public `func (c *Client) ListOffsetsAll(topic string)
   ([]OffsetListing, error)` that calls `ListOffsets(topic, nil)`.
2. Inherit retry / error **13** from `ListOffsets` (v0.82 transient
   retry + v0.112 error 13). No new retry policy.
3. Do **not** change `ListOffsets(topic, partitions)`.
4. Do **not** change broker / protocol / Rust / Python / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `ListOffsets(topic, partitions)` | Frozen; nil/empty already means all |
| Kafka ListOffsets (API key 2) isolation / timestamp | Native opcode 48/49 only |
| Kafka specials (max-timestamp, earliest-local, tiered) | Kafka shim only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Python / Java | Already have topic-only overloads (v0.50) |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// ListOffsetsAll lists earliest/latest for every partition of topic
// (empty wire partitions). Same as ListOffsets(topic, nil).
func (c *Client) ListOffsetsAll(topic string) ([]OffsetListing, error) {
    return c.ListOffsets(topic, nil)
}
```

```go
bounds, _ := c.ListOffsetsAll("events")                 // all partitions
bounds, _ = c.ListOffsets("events", nil)                // unchanged: same wire
bounds, _ = c.ListOffsets("events", []uint32{0, 1})
```

## Semantics

- Empty wire partitions = all partitions of the topic (same as
  today).
- `ListOffsetsAll` is a named wrapper; it does not re-encode.
- `ListOffsets(topic, partitions)` is unchanged (nil/empty still
  mean all).
- Transient 6 / 7 / 15 / 16 and transport retry via `ListOffsets`
  (v0.82; default `max_retries=0`).
- Error 13 follows `max_redirects` (v0.112).
- Not Kafka ListOffsets (no timestamp or isolation); both ends of
  each log are returned.

## Tests

Fake TCP stub that records decoded ListOffsets partitions (same helper
as existing `list_offsets_test.go`).

```bash
(cd clients/go && go test ./...)
```

| Case | Expect |
|------|--------|
| `ListOffsetsAll("events")` | wire partitions empty (count 0); same as `ListOffsets(topic, nil)` |
| Existing `ListOffsets` empty / explicit / error cases | still pass |

Existing ListOffsets retry / 13 tests must still pass
(`ListOffsets` unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `ListOffsetsAll` wraps `ListOffsets(topic, nil)` |
| `clients/go/list_offsets_test.go` | empty-partitions wire check |
| `clients/go/README.md` | usage line + one prose sentence |
| `docs/V163_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** ListOffsets (API key 2). Native opcode **48/49**
  only. No isolation, timestamp, max-timestamp, earliest-local, or
  tiered specials.
- Empty partitions still list **all** partitions of the topic.
- `ListOffsets(topic, partitions)` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust / Python / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.go` should keep this hunk local
to the ListOffsetsAll wrapper:

- **Keep the wrapper only.** Do not change `ListOffsets`.
- Do not change the ListOffsets send loop (v0.82 retry + v0.112 13).
- Do not change Python, Java, broker, or protocol.

Expect conflicts on:

- `clients/go/client.go` — hunk is local to `ListOffsetsAll` after
  `ListOffsets`
- `clients/go/list_offsets_test.go`
- `clients/go/README.md`

## Related

- [V50_SPEC.md](./V50_SPEC.md) — language ListOffsets
- [V82_SPEC.md](./V82_SPEC.md) — language ListOffsets transient retry
- [V112_SPEC.md](./V112_SPEC.md) — language ListOffsets error 13
- [V158_SPEC.md](./V158_SPEC.md) — Go DeleteOffsetsAll (same wrapper pattern)
- [PHASE15_SPEC.md](./PHASE15_SPEC.md) — native 48/49
