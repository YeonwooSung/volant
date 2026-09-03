# v0.158 — Go DeleteOffsetsAll

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V54_SPEC.md](./V54_SPEC.md): Java
already has `deleteOffsets(group)` (empty entries = all). Python
`delete_offsets(group)` with `entries=None` already deletes all. Go
only has `DeleteOffsets(group, entries)` — nil/empty already means
all, but there is no named all-group helper matching Java.

Add `Client.DeleteOffsetsAll`. Reuse `DeleteOffsets` (do not
reimplement the RPC). `DeleteOffsets(group, entries)` stays unchanged.
This is **not** Kafka OffsetDelete.

This is residual **v0.158** (Go DeleteOffsetsAll). It is **not**
Phase 155. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, or change the broker, protocol, Rust, Python, or Java.

## Goals

1. Add public `func (c *Client) DeleteOffsetsAll(group string)
   (uint32, error)` that calls `DeleteOffsets(group, nil)`.
2. Inherit retry / error **14** from `DeleteOffsets` (v0.78 transient
   retry + v0.97 error 14). No new retry policy.
3. Do **not** change `DeleteOffsets(group, entries)`.
4. Do **not** change broker / protocol / Rust / Python / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `DeleteOffsets(group, entries)` | Frozen; nil/empty already means all |
| Kafka OffsetDelete (API key 47) | Native opcode 38 only |
| Kafka DeleteGroups (API key 42) | No native DeleteGroups opcode |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Python / Java | Already have group-only overloads (v0.54) |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// DeleteOffsetsAll deletes every committed offset for group
// (empty wire entries). Same as DeleteOffsets(group, nil).
func (c *Client) DeleteOffsetsAll(group string) (uint32, error) {
    return c.DeleteOffsets(group, nil)
}
```

```go
n, _ := c.DeleteOffsetsAll("g")                                 // all group offsets
n, _ = c.DeleteOffsets("g", nil)                                // unchanged: same wire
n, _ = c.DeleteOffsets("g", []codec.OffsetEntry{{Topic: "t", Partition: 0}})
```

## Semantics

- Empty wire entries = all committed offsets for the group (same as
  today).
- `DeleteOffsetsAll` is a named wrapper; it does not re-encode.
- `DeleteOffsets(group, entries)` is unchanged (nil/empty still mean
  all).
- Transient 6 / 7 / 15 / 16 and transport retry via `DeleteOffsets`
  (v0.78; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.97).
- Not Kafka OffsetDelete / DeleteGroups.

## Tests

Fake TCP stub that records decoded DeleteOffsets entries (same helper
as existing `delete_offsets_test.go`).

```bash
(cd clients/go && go test ./...)
```

| Case | Expect |
|------|--------|
| `DeleteOffsetsAll("g")` | wire entries empty (count 0); same as `DeleteOffsets(group, nil)` |
| Existing `DeleteOffsets` empty / explicit / error cases | still pass |

Existing DeleteOffsets retry / 14 tests must still pass
(`DeleteOffsets` unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `DeleteOffsetsAll` wraps `DeleteOffsets(group, nil)` |
| `clients/go/delete_offsets_test.go` | empty-entries wire check |
| `clients/go/README.md` | usage line + one prose sentence |
| `docs/V158_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** OffsetDelete / DeleteGroups.
- Empty entries still delete **all** committed offsets for the group.
- `DeleteOffsets(group, entries)` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust / Python / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.go` should keep this hunk local
to the DeleteOffsetsAll wrapper:

- **Keep the wrapper only.** Do not change `DeleteOffsets`.
- Do not change the DeleteOffsets send loop (v0.78 retry + v0.97 14).
- Do not change Python, Java, broker, or protocol.

Expect conflicts on:

- `clients/go/client.go` — hunk is local to `DeleteOffsetsAll` after
  `DeleteOffsets`
- `clients/go/delete_offsets_test.go`
- `clients/go/README.md`

## Related

- [V54_SPEC.md](./V54_SPEC.md) — language DeleteOffsets
- [V78_SPEC.md](./V78_SPEC.md) — language DeleteOffsets transient retry
- [V97_SPEC.md](./V97_SPEC.md) — language DeleteOffsets error 14
- [PHASE12_SPEC.md](./PHASE12_SPEC.md) — native 38/39
