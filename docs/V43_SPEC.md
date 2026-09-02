# v0.43 — leader redirect on Python / Go / Java produce and fetch

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Python / Go / Java clients have no
**leader redirect**” by matching Rust `volant-client`: on
`NotLeaderForPartition` (**13**), refresh Metadata, reconnect to the
partition leader, and retry the same Produce or Fetch once.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, extend homemade metadata Raft, or
change the broker. Auth (v0.42 sibling) is not implemented here.

## Goals

1. **On Produce error 13** and **Fetch error 13**: if a redirect budget
   remains, call `metadata()`, find the leader broker for that
   topic+partition, close the old socket, open a new one to `host:port`
   (same timeout and TLS knobs), and retry the same request.
2. **Default budget = 1 extra attempt** (Rust `max_redirects` default 1
   → 1 initial + 1 redirect). After the budget is exhausted, raise
   error 13. Never follow redirects in an unbounded loop.
3. **Constructor / config** (do not break existing dial APIs):
   - Python: `Client(..., max_redirects: int = 1)`. `0` = never
     redirect (today’s raise-on-13).
   - Go: `Dial` / `DialTLS` stay. `func (c *Client) SetMaxRedirects(n int)`.
   - Java: `connect` / `connectTls` stay. `setMaxRedirects(int)`.
4. If Metadata has no matching topic/partition, unknown broker id, or
   empty host, raise the original error 13 (no reconnect).
5. Redirect applies to **Produce and Fetch only**. CreateTopic / group /
   offsets stay on the original connection.
6. Correlation ids still increment per request. Acks and the produce
   trailer `(0, 0, -1)` are unchanged.

## Non-goals

| Deferred | Why |
|----------|-----|
| ISR / preferred-replica redirect | Not in Rust `volant-client` either |
| Kafka `FindCoordinator` dance | Native opcodes only; no Kafka API keys |
| Multi-connection fan-out | Still one TCP connection at a time |
| Auth / shared-token re-Auth on reconnect | v0.42 sibling; preserve TLS only |
| CreateTopic / group / offset redirect | Those RPCs are not partition-leader-specific in this MVP |
| Broker / protocol / Rust client changes | Wire and Rust redirect already exist |
| New native opcodes | Reuse Produce (1), Fetch (2), Metadata (4) |

## Error 13

Native `ErrorCode::NotLeaderForPartition = 13`
(`crates/volant-protocol/src/response.rs`). Produce and Fetch replies
carry `error_code` on the typed response. Language clients previously
raised `BrokerError(13)` / `BrokerError` / `BrokerException` on the
first 13 and stayed on the original socket.

## Reconnect

Close the old socket, clear the read buffer, open a new TCP connection
to the leader `host:port` with the same timeout. If the original dial
was TLS (`tls=True` / `DialTLS` / `connectTls`), wrap the new socket
with the stored TLS knobs (CA, insecure, client cert). Address parsing
matches the existing IPv4 / `[ipv6]:port` helpers. Correlation ids are
**not** reset.

If the Metadata leader address is already the current connection,
skip reconnect and retry on the same socket (same as Rust).

## API

```python
c = Client("127.0.0.1:9092", max_redirects=1)
c = Client("127.0.0.1:9092", max_redirects=0)  # raise on first 13
```

```go
c, _ := volant.Dial("127.0.0.1:9092")
c.SetMaxRedirects(0)
```

```java
try (Client c = Client.connect("127.0.0.1", 9092)) {
  c.setMaxRedirects(0);
}
```

`max_redirects=0` is the pre-v0.43 raise-on-13 behavior (useful in
tests that assert broker-level rejection).

If v0.42 lands in the same tree, keep **both** `auth_token` and
`max_redirects` on the Python constructor; Go/Java setters do not
touch `Dial` / `connect`.

## Tests

Fake TCP brokers (no live multi-broker e2e):

1. Produce to broker A returns 13; Metadata points the partition leader
   at broker B; second produce to B succeeds. Assert two produce
   attempts and a reconnect to B.
2. `max_redirects=0` → first 13 raises, no second produce, no Metadata.
3. Fetch error 13 also redirects once.
4. Metadata missing the leader (unknown broker id / empty host) → raise
   13, no extra reconnect loop.

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty

Language clients now follow leader redirect on Produce/Fetch (default
one extra attempt). Still one connection at a time. Still no
idempotent produce, no Kafka client APIs, no assignor, no Auth in this
slice.
