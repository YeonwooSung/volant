# v0.115 — language public reconnect (Rust parity)

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V43_SPEC.md](./V43_SPEC.md) /
Rust `Client::reconnect`: language clients already have a **private**
reconnect used by leader / controller redirect (`_reconnect` /
`reconnect` / Java private `reconnect`). Rust
`volant-client` exposes public `reconnect(&self, addr) -> Result<()>`.
This slice publishes the same API without changing redirect behavior.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, or Rust client (`Client::reconnect` is
already public).

## Goals

1. **Python:** public `reconnect(self, addr: str) -> None` that calls
   the existing `_reconnect` (one implementation). Re-auth already
   happens in `_reconnect`.
2. **Go:** public `Reconnect(addr string) error` wrapping the existing
   unexported `reconnect`.
3. **Java:** public `reconnect(String host, int port)` — the existing
   private method made public (same body).
4. Semantics match Rust:
   - Close / replace the current socket, connect to the new addr,
     reset the read buffer as the private path already does.
   - Re-run Auth if `auth_token` is set; else SCRAM if configured.
   - Do **not** reset producer id / txn / sequences unless the private
     path already does (it does not).
   - Idempotent produce state stays (private reconnect does not clear
     it).
5. No new constructor args. Default retry / redirect knobs unchanged.
6. Existing redirect tests still pass (private path unchanged).

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust `Client::reconnect` | Already public |
| ListOffsets wrap | Sibling v0.112 |
| Auth retry internals / SCRAM handshake wrap | Already shipped (v0.106 / v0.108); reconnect only calls existing `maybe_authenticate` |
| DeleteRecords beyond calling private reconnect | Sibling / already shipped |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Reset producer id / txn / sequences | Private path does not; do not invent a reset |

## API

```python
c = Client("127.0.0.1:9092")
c.reconnect("127.0.0.1:9093")
```

```go
c, _ := volant.Dial("127.0.0.1:9092")
_ = c.Reconnect("127.0.0.1:9093")
```

```java
try (Client c = Client.connect("127.0.0.1", 9092)) {
  c.reconnect("127.0.0.1", 9093);
}
```

Token Auth wins over SCRAM on the new connection (same
`maybe_authenticate` / `_maybe_authenticate` order as connect and
leader redirect).

## Tests

Fake TCP. Two stubs, or one stub that accepts two connections.

| Case | Expect |
|------|--------|
| Connect, `reconnect` to a second listener, then `metadata()` | succeeds on the second |
| `auth_token` set, then `reconnect` | Auth sent again on the new connection |
| SCRAM configured, then `reconnect` | new first+final (at least ScramFirst again) |
| Existing redirect tests | still pass (private path unchanged) |

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- **Not Kafka** `bootstrap.servers` / reconnect.backoff.
- Still **one TCP connection** at a time.
- Public reconnect is the same path as leader / controller redirect.
- Producer id / txn / sequences are **not** reset.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Rust `volant-client` is unchanged.

## Merge notes

Sibling slice **v0.112** (ListOffsets) also edits the three `Client`
files. When merging:

- **Keep the public reconnect wrapper only.** Do not change
  `_reconnect` / unexported `reconnect` / Java reconnect body.
- Do not wrap ListOffsets, Auth retry internals, SCRAM, or
  DeleteRecords beyond calling the existing private reconnect.
- Do not change `_redirect_to_leader` / `redirectToLeader` /
  `_redirect_to_controller` / `redirectToController`.
- Do not change the broker, Kafka shim, or Rust client in this merge.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (public `reconnect`
  next to `_reconnect`)
- Go `clients/go/client.go` (public `Reconnect` next to unexported
  `reconnect`)
- Java `clients/java/src/main/java/io/volant/Client.java` (`reconnect`
  visibility + javadoc)

The hunk is local to the reconnect public wrapper.

## Related

- [V43_SPEC.md](./V43_SPEC.md) — leader redirect (private reconnect)
- [V42_SPEC.md](./V42_SPEC.md) — shared-token Auth
- [V46_SPEC.md](./V46_SPEC.md) — SCRAM-SHA-256
- [V57_SPEC.md](./V57_SPEC.md) — reconnect does not clear pid / txn
- [V106_SPEC.md](./V106_SPEC.md) — language Auth retry
- [V108_SPEC.md](./V108_SPEC.md) — language SCRAM handshake retry
