# v0.171 — Go AddBrokerNoRack

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V58_SPEC.md](./V58_SPEC.md): Java
already has `addBroker(id, host, port)` (no rack). Python
`add_broker(id, host, port, rack=None)` already omits rack. Go only
has `AddBroker(id, host, port, rack *string)` — nil already means no
rack (wire flag 0), but there is no named no-rack helper matching
Java.

Add `Client.AddBrokerNoRack`. Reuse `AddBroker` (do not reimplement
the RPC). `AddBroker(id, host, port, rack)` stays unchanged. This is
**not** Kafka broker catalog. Overlay remains SoT.

This is residual **v0.171** (Go AddBrokerNoRack). It is **not**
Phase 155. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, or change the broker, protocol, Rust, Python, or Java.

## Goals

1. Add public `func (c *Client) AddBrokerNoRack(id uint32, host string,
   port uint16) (uint64, error)` that calls
   `AddBroker(id, host, port, nil)`.
2. Inherit retry / error **14** from `AddBroker` (v0.89 error 14 +
   v0.103 transient retry via `adminRoundTrip`). No new retry policy.
3. Do **not** change `AddBroker(id, host, port, rack)`.
4. Do **not** change broker / protocol / Rust / Python / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `AddBroker(id, host, port, rack)` | Frozen; nil already means no rack |
| Kafka broker catalog / Metadata brokers | Overlay is still SoT; native 102/103 only |
| Overlay / assignment wait-rollback | Broker-side (v0.10 / v0.18 / v0.39) |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Java | Already have no-rack overloads (v0.58) |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// AddBrokerNoRack adds a broker with no rack (wire flag 0).
// Same as AddBroker(id, host, port, nil).
func (c *Client) AddBrokerNoRack(id uint32, host string, port uint16) (uint64, error) {
    return c.AddBroker(id, host, port, nil)
}
```

```go
gen, _ := c.AddBrokerNoRack(2, "10.0.0.2", 9092) // no rack
gen, _ = c.AddBroker(2, "10.0.0.2", 9092, nil)   // unchanged: same wire
rack := "r1"
gen, _ = c.AddBroker(2, "10.0.0.2", 9092, &rack)
```

## Semantics

- Wire rack flag 0 = absent (same as today).
- `AddBrokerNoRack` is a named wrapper; it does not re-encode.
- `AddBroker(id, host, port, rack)` is unchanged (`nil` still means
  no rack).
- Transient 6 / 7 / 15 / 16 and transport retry via `AddBroker` /
  `adminRoundTrip` (v0.103; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.89).
- Overlay is still SoT. Not Kafka broker catalog.

## Tests

Fake TCP stub that records decoded AddBroker rack (same helper as
existing `membership_test.go`).

```bash
(cd clients/go && go test ./...)
```

| Case | Expect |
|------|--------|
| `AddBrokerNoRack(2, "10.0.0.2", 9092)` | wire rack absent (`got.addRack == nil`) |
| Existing `TestAddBrokerReturnsGeneration` (with rack) | still pass |

Existing AddBroker retry / 14 tests must still pass
(`AddBroker` unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `AddBrokerNoRack` wraps `AddBroker(id, host, port, nil)` |
| `clients/go/membership_test.go` | absent-rack wire check |
| `clients/go/README.md` | usage line + one prose sentence |
| `docs/V171_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** broker catalog. Native opcode **102/103** only.
  Overlay remains SoT.
- Nil `rack` still encodes wire flag **0**.
- `AddBroker(id, host, port, rack)` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust / Python / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.go` should keep this hunk local
to the AddBrokerNoRack wrapper:

- **Keep the wrapper only.** Do not change `AddBroker`.
- Do not change the AddBroker send loop (v0.89 14 + v0.103
  transient retry).
- Do not change Python, Java, broker, or protocol.

Expect conflicts on:

- `clients/go/client.go` — hunk is local to `AddBrokerNoRack`
  after `AddBroker`
- `clients/go/membership_test.go`
- `clients/go/README.md`

## Related

- [V58_SPEC.md](./V58_SPEC.md) — language AddBroker / RemoveBroker / ListMembers
- [V89_SPEC.md](./V89_SPEC.md) — language AddBroker error 14
- [V103_SPEC.md](./V103_SPEC.md) — language admin_round_trip transient retry
- [V167_SPEC.md](./V167_SPEC.md) — Go ReassignAllPartitions (same wrapper pattern)
- [V10_SPEC.md](./V10_SPEC.md) — native 102/103
