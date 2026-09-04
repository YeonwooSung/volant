# v0.185 — Go SetEnableIdempotence / Idempotence

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V47_SPEC.md](./V47_SPEC.md):
Java already has `setEnableIdempotence(boolean)` and
`enableIdempotence()`. Python has a public `enable_idempotence`
field. Go only has one-way `EnableIdempotence()`.

Add `Client.SetEnableIdempotence` and `Client.Idempotence`.
`EnableIdempotence()` still turns it on (delegates to
`SetEnableIdempotence(true)`). Do **not** rename the existing
method — `EnableIdempotence() bool` would collide.

This is residual **v0.185**. It is **not** Phase 155. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Rust, Python, or Java.

## Goals

1. **Go:** public `func (c *Client) SetEnableIdempotence(on bool)`.
   Sets `c.enableIdempotence`. When `on=true` and `c.nextSeq` is
   nil, allocate the per-partition sequence map (same as today's
   `EnableIdempotence`).
2. **Go:** public `func (c *Client) Idempotence() bool`. Reports
   whether `EnableIdempotence` / `SetEnableIdempotence(true)` is
   set. Transactional id is separate.
3. Refactor `EnableIdempotence()` to call
   `SetEnableIdempotence(true)` so behavior stays identical.
4. Do **not** change produce trailer / InitProducerId logic beyond
   the setter delegation.
5. Do **not** change broker / protocol / Rust / Python / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Rename `EnableIdempotence()` to a getter | Would collide; cannot add `EnableIdempotence() bool` |
| Change produce trailer / InitProducerId | Frozen; setter only flips the existing flag |
| Kafka InitProducerId (API key 22) / idempotent produce v2 | Native opcode 32 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Java / Rust | Already have a setter or public field |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// SetEnableIdempotence turns InitProducerId + per-partition sequences on or off.
// Same trailer rules as EnableIdempotence when on=true.
func (c *Client) SetEnableIdempotence(on bool) {
    c.enableIdempotence = on
    if on && c.nextSeq == nil {
        c.nextSeq = make(map[seqKey]int32)
    }
}

// Idempotence reports whether EnableIdempotence / SetEnableIdempotence(true)
// is set. Transactional id is separate.
func (c *Client) Idempotence() bool {
    return c != nil && c.enableIdempotence
}
```

```go
c, _ := volant.Dial("127.0.0.1:9092")
_ = c.Idempotence()            // false (default)
c.EnableIdempotence()          // still turns it on
_ = c.Idempotence()            // true
c.SetEnableIdempotence(false)
_ = c.Idempotence()            // false
c.SetEnableIdempotence(true)   // same trailer rules as EnableIdempotence
```

Existing `EnableIdempotence()` / produce signatures are unchanged.

## Semantics

- Default is **off**. Trailer stays `(0, 0, -1)` until
  `EnableIdempotence` / `SetEnableIdempotence(true)`.
- `EnableIdempotence()` is `SetEnableIdempotence(true)`.
- `SetEnableIdempotence(true)` allocates `nextSeq` if nil (same as
  today's one-way enable).
- `SetEnableIdempotence(false)` only clears the flag. It does not
  drop pid / epoch / sequences.
- `Idempotence()` reads the stored flag. It does **not** send
  InitProducerId (opcode **32**).
- Transactional id (`SetTransactionalID`) is separate.
- Produce / InitProducerId trailer rules are unchanged beyond the
  setter delegation.
- Not Kafka idempotent produce v2.

## Tests

```bash
cd clients/go && go test ./...
```

| Case | Expect |
|------|--------|
| Default `Idempotence()` | `false` |
| After `EnableIdempotence()` | `Idempotence() == true` |
| After `SetEnableIdempotence(false)` | `Idempotence() == false` |

Existing EnableIdempotence produce tests must still pass
(`TestIdempotentProduceOnInitsThenSequences` and neighbors).

| File | What |
|------|------|
| `clients/go/client.go` | `SetEnableIdempotence` / `Idempotence`; `EnableIdempotence` delegates |
| `clients/go/client_test.go` | setter / getter cases |
| `clients/go/README.md` | usage / prose line |
| `docs/V185_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** InitProducerId (API key 22) / idempotent produce v2.
  Native opcode **32** only.
- `EnableIdempotence()` remains one-way on. The boolean getter is
  `Idempotence()` so the names do not collide.
- Produce trailer / InitProducerId logic is unchanged beyond the
  setter delegation.
- Java / Python already have a setter or public field.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust / Python / Java are unchanged.

## Merge notes

Sibling slices that also edit Go `Client` should keep this hunk
local to the setter / getter:

- **Keep `EnableIdempotence()` as `SetEnableIdempotence(true)`.**
  Do not rename it to a getter.
- Do not change produce trailer / InitProducerId.
- Do not change Rust, Python, Java, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`EnableIdempotence`)
- `clients/go/client_test.go`
- `clients/go/README.md`

The hunk is local to the setter / getter + flag tests.

## Related

- [V47_SPEC.md](./V47_SPEC.md) — idempotent produce / InitProducerId
- [V101_SPEC.md](./V101_SPEC.md) — language InitProducerId retry
- [V150_SPEC.md](./V150_SPEC.md) — language public InitProducerId
- [V160_SPEC.md](./V160_SPEC.md) — Go/Python/Rust producer id getters
