# v0.149 — Rust fetch uses ClientConfig fetch_max_bytes

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V144_SPEC.md](./V144_SPEC.md):
v0.144 added `ClientConfig.fetch_max_messages` / `fetch_max_bytes` /
`fetch_max_wait_ms` and `fetch_default`. `Client::fetch` still
**hardcoded** `DEFAULT_FETCH_MAX_BYTES` (4 MiB) even when the caller
set `config.fetch_max_bytes`.

Wire `Client::fetch` to `self.config.fetch_max_bytes`. Do **not**
change `fetch` / `fetch_opts` / `fetch_default` signatures.
`fetch_opts` stays fully explicit. `fetch_default` is unchanged.
`DEFAULT_FETCH_MAX_BYTES` stays the **Default** for
`ClientConfig.fetch_max_bytes` (4 MiB). GroupConsumer poll knobs stay
historical (v0.76; 100 / 4 MiB).

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Python/Go/Java.

## Goals

1. Change `Client::fetch` to pass `self.config.fetch_max_bytes` into
   `fetch_opts` instead of `Self::DEFAULT_FETCH_MAX_BYTES`.
2. Keep `DEFAULT_FETCH_MAX_BYTES` (4 MiB) as the
   `ClientConfig.fetch_max_bytes` default.
3. Keep `Client::fetch(topic, partition, from, max_messages, max_wait_ms)`
   signature as-is: still requires the two explicit args.
4. Keep `Client::fetch_opts` as the explicit-`max_bytes` path
   (ignores config).
5. Leave `Client::fetch_default` unchanged (already uses all three
   config knobs).
6. Do **not** change GroupConsumer poll knobs (v0.76; default 100 /
   4 MiB).
7. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `Client::fetch` / `fetch_opts` / `fetch_default` signatures | Frozen; `fetch` still requires max_messages / max_wait_ms |
| GroupConsumer poll knobs | Historical 100 / 4 MiB (v0.76) |
| Language 3-arg Fetch defaults | Sibling residual (v0.143) |
| Kafka Fetch versions (API key 1) | Native opcode 2 only |
| New retry / redirect | Existing Fetch 13 redirect stays as-is |
| Broker / protocol / Python / Go / Java | Frozen |
| Kafka API keys / new opcodes | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

- Default-config `fetch(..., 7, 0)` sends `max_messages=7`,
  `max_wait_ms=0`, `max_bytes=4MiB`.
- After `cfg.fetch_max_bytes = 4096`, `fetch(..., 7, 0)` sends
  `max_bytes=4096`. Explicit `max_messages` / `max_wait_ms` still
  win; only `max_bytes` is taken from config.
- `fetch_opts(..., 7, 0, 8192)` still sends `max_bytes=8192` even
  when config is 4096.
- `fetch_default` still sends all three config knobs (v0.144).
- GroupConsumer `poll` still uses its own stored knobs (default 100 /
  4 MiB, `max_wait_ms=0`). It does **not** read `ClientConfig` Fetch
  fields.
- Leader-13 redirect and any existing Fetch retry stay as-is.

## API

```rust
impl Client {
    // signature unchanged; max_bytes now from config:
    pub async fn fetch(
        &self,
        topic: &str,
        partition: u32,
        from: Offset,
        max_messages: u32,
        max_wait_ms: u32,
    ) -> Result<FetchResult> {
        self.fetch_opts(
            topic, partition, from,
            max_messages,
            max_wait_ms,
            self.config.fetch_max_bytes,
        ).await
    }

    // still explicit:
    pub async fn fetch_opts(
        &self,
        topic: &str,
        partition: u32,
        from: Offset,
        max_messages: u32,
        max_wait_ms: u32,
        max_bytes: u32,
    ) -> Result<FetchResult>;
}
```

```rust
let c = Client::connect(ClientConfig::default()).await?;
c.fetch("t", 0, Offset::ZERO, 7, 0).await?;            // 7 / 4MiB / 0

let c = Client::connect(ClientConfig {
    fetch_max_bytes: 4096,
    ..ClientConfig::default()
}).await?;
c.fetch("t", 0, Offset::ZERO, 7, 0).await?;            // 7 / 4096 / 0
c.fetch_opts("t", 0, Offset::ZERO, 7, 0, 8192).await?; // 7 / 8192 / 0
```

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Fake TCP stub (v0.144 style) that records decoded Fetch
`max_messages` / `max_bytes` / `max_wait_ms`:

| Case | Expect |
|------|--------|
| Default config `fetch(..., 7, 0)` | max_messages=7, max_wait=0, max_bytes=4MiB |
| Config `fetch_max_bytes=4096` then `fetch(..., 7, 0)` | max_bytes=4096 |
| `fetch_opts(..., 7, 0, 8192)` | max_bytes=8192 (ignores config) |
| Existing `fetch_default` tests | still pass |

Existing `v76_group_poll_fetch_knobs.rs` must still pass (poll stays
100 / 4 MiB).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `fetch` uses `config.fetch_max_bytes` |
| `crates/volant-client/src/config.rs` | comments + Default uses `DEFAULT_FETCH_MAX_BYTES` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v144_fetch_default.rs` | existing `fetch` test matches new contract |
| `crates/volant-client/tests/v149_fetch_max_bytes.rs` | default / configured / fetch_opts |
| `docs/V149_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka Fetch.** Native opcode **2** only.
- `Client::fetch` signature is unchanged (`max_messages` /
  `max_wait_ms` still required).
- GroupConsumer poll knobs stay historical **100 / 4 MiB** (v0.76).
- Language 3-arg Fetch is a sibling residual (v0.143).
- No new retry / redirect. Existing Fetch 13 redirect stays.
- No Kafka API keys / opcodes / broker / protocol / Phase 155.

## Merge notes

Sibling slices that also edit `client.rs` / `config.rs` / crate-doc
should keep this hunk local to Fetch `max_bytes`:

- **Keep `fetch` as the config-`max_bytes` path.** Do not change
  `fetch` / `fetch_opts` / `fetch_default` signatures.
- Do **not** wire GroupConsumer poll to `ClientConfig` Fetch fields.
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to `fetch`
- `crates/volant-client/src/config.rs` (`fetch_max_bytes` docs /
  `Default`)
- `crates/volant-client/src/lib.rs` (crate-doc)
- `crates/volant-client/tests/v144_fetch_default.rs`

## Related

- [V144_SPEC.md](./V144_SPEC.md) — Rust ClientConfig Fetch knobs +
  `fetch_default`
- [V76_SPEC.md](./V76_SPEC.md) — Rust GroupConsumer poll fetch knobs
  (historical 100 / 4 MiB)
- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Fetch knobs (Client)
- [V143_SPEC.md](./V143_SPEC.md) — language Fetch client-level default
  knobs
