# v0.145 — Go/Java Fetch high watermark

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V64_SPEC.md](./V64_SPEC.md):
Python `fetch` returns `FetchResult` with `high_watermark`, and Rust
`FetchResult` has `high_watermark`. Go `Fetch` / `FetchOpts` and Java
`fetch` return only `[]Record` / `List<Record>` and drop
`codec.FetchResponse.HighWatermark` / `highWatermark`.

Add a public Fetch result type and named methods that return records
**and** high watermark. Reuse the existing Fetch retry / error 13
path. This is **not** Kafka Fetch versions.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Python, Rust, or codec encode/decode.

## Goals

1. **Go:** export `FetchResult` (`Topic`, `Partition`,
   `HighWatermark`, `Records`). Add `FetchResult` / `FetchOptsResult`
   that return it. `Fetch` / `FetchOpts` stay `[]Record` (call the
   Result method and return `.Records`).
2. **Java:** add public `io.volant.FetchResult` (`topic`, `partition`,
   `highWatermark`, `records`). Add `fetchResult` 3-arg and 6-arg.
   Existing `fetch` overloads stay `List<Record>`.
3. Reuse the existing Fetch retry / error 13 path. Do not duplicate
   the RPC loop.
4. Do **not** change existing `Fetch` / `FetchOpts` / 3-arg or 6-arg
   `fetch` return types.
5. No new constructor args. Default retry / redirect knobs unchanged.
6. Existing Fetch redirect / retry tests still pass.

## Non-goals

| Deferred | Why |
|----------|-----|
| Python / Rust `FetchResult` | Already public |
| Change `Fetch` / `FetchOpts` / `fetch` return types | Frozen; records only |
| Kafka Fetch versions / isolation / forgotten topics | Native opcode 2 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Codec encode/decode | High watermark already on the wire |

## API

```go
recs, _ := c.Fetch("t", 0, 0)                         // []Record unchanged
recs, _ = c.FetchOpts("t", 0, 0, 10, 4096, 100)       // []Record unchanged
batch, _ := c.FetchResult("t", 0, 0)                  // Topic, Partition, HighWatermark, Records
batch, _ = c.FetchOptsResult("t", 0, 0, 10, 4096, 100)
_ = batch.HighWatermark
```

```java
List<Record> recs = c.fetch("t", 0, 0);                         // records only
recs = c.fetch("t", 0, 0, 10, 4096L, 100);                      // records only
FetchResult batch = c.fetchResult("t", 0, 0);
batch = c.fetchResult("t", 0, 0, 10, 4096L, 100);
long hwm = batch.highWatermark;
```

`Fetch` / `FetchOpts` / `fetch` still return records only.

## Semantics

- Public Fetch result types now carry the already-decoded high
  watermark plus the same records `Fetch` / `fetch` return.
- Transient 6 / 7 / 15 / 16 and transport retry via the existing
  Fetch loop (v0.66; default `max_retries=0`).
- Error 13 follows `max_redirects` (v0.28).
- Defaults stay 128 / 4MiB / 0.
- Not Kafka Fetch versions / isolation / forgotten topics.

## Tests

Fake TCP stub that injects a scripted Fetch reply.
Existing Fetch redirect / retry suites still pass.

| Case | Expect |
|------|--------|
| Scripted reply with `HighWatermark=42` and one record | `FetchResult` / `fetchResult` returns 42 + that record |
| Same reply via `Fetch` / `fetch` | same records; still records only |

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- **Not Kafka** Fetch versions / isolation / forgotten topics.
- Native opcode **2** only. High watermark is already on the wire.
- `Fetch` / `FetchOpts` / `fetch` stay records-only.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Python and Rust clients are unchanged.
- Codec encode/decode is unchanged (high watermark already decoded).

## Merge notes

Sibling slices that also edit Go/Java `Client` should keep this hunk
local to Fetch result mapping:

- **Keep the public result type only.** Do not change the Fetch send
  loop (v0.66 retry + v0.28 13) beyond returning the already-decoded
  high watermark.
- Do not change `Fetch` / `FetchOpts` / `fetch` return types.
- Do not change Python, Rust, codec, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`Fetch` / `FetchOpts`)
- Java `clients/java/src/main/java/io/volant/Client.java` (`fetch`)
- Java `clients/java/src/main/java/io/volant/FetchResult.java` (new)

The hunk is local to public Fetch result types.

## Related

- [V19_SPEC.md](./V19_SPEC.md) — Go Fetch
- [V23_SPEC.md](./V23_SPEC.md) — Java Fetch
- [V28_SPEC.md](./V28_SPEC.md) — Fetch error 13 redirect
- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Fetch knobs
- [V66_SPEC.md](./V66_SPEC.md) — Fetch transient retry
