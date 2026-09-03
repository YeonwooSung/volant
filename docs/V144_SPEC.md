# v0.144 — Rust ClientConfig Fetch knobs

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V76_SPEC.md](./V76_SPEC.md) /
[V64_SPEC.md](./V64_SPEC.md) / [V129_SPEC.md](./V129_SPEC.md): Rust
`ClientConfig` already has `acks` for default produce (v0.129).
`Client::fetch` still **requires** `max_messages` / `max_wait_ms` and
hardcodes `max_bytes` to 4 MiB via `DEFAULT_FETCH_MAX_BYTES`.
Language 3-arg Fetch is a sibling residual (v0.143).

Add `ClientConfig` Fetch knobs and a thin `fetch_default` that uses
them. Do **not** change `Client::fetch` / `Client::fetch_opts`
signatures. GroupConsumer poll knobs stay historical (v0.76; default
**100 / 4 MiB**).

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Python/Go/Java.

## Goals

1. Add `ClientConfig` fields with today’s Client-fetch defaults:
   - `fetch_max_messages` default **128** (not GroupConsumer poll’s 100)
   - `fetch_max_bytes` default **4 MiB**
   - `fetch_max_wait_ms` default **0**
2. Add `Client::fetch_default(topic, partition, from)` that calls
   `fetch_opts` with those knobs. This is the config-knob path.
3. Keep `Client::fetch(topic, partition, from, max_messages, max_wait_ms)`
   as-is: still requires the two explicit args and still hardcodes
   4 MiB via `DEFAULT_FETCH_MAX_BYTES`.
4. Keep `Client::fetch_opts` as the explicit-`max_bytes` path.
5. Do **not** change GroupConsumer poll knobs (v0.76; default 100 /
   4 MiB).
6. Do **not** change language 3-arg Fetch (sibling v0.143).
7. Do **not** change broker / protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `Client::fetch` / `fetch_opts` signatures | Frozen; `fetch` still requires max_messages / max_wait_ms |
| GroupConsumer poll knobs | Historical 100 / 4 MiB (v0.76) |
| Language 3-arg Fetch defaults | Sibling residual (v0.143) |
| Kafka Fetch versions (API key 1) | Native opcode 2 only |
| New retry / redirect | Existing Fetch 13 redirect stays as-is |
| Broker / protocol / Python / Go / Java | Frozen |
| Kafka API keys / new opcodes | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

- Default `fetch_default` sends `max_messages=128`, `max_bytes=4MiB`,
  `max_wait_ms=0`.
- After set (`cfg.fetch_max_messages = 10`, `fetch_max_bytes = 4096`,
  `fetch_max_wait_ms = 100`), `fetch_default` sends those knobs.
- Existing `fetch(topic, part, off, 7, 0)` still sends **7 / 4MiB / 0**
  (unchanged contract). Config knobs do not leak into `fetch`.
- `fetch_opts` stays the explicit three-knob path.
- GroupConsumer `poll` still uses its own stored knobs (default 100 /
  4 MiB, `max_wait_ms=0`). It does **not** read `ClientConfig` Fetch
  fields.
- Leader-13 redirect and any existing Fetch retry stay as-is.

## API

```rust
pub struct ClientConfig {
    pub acks: u8,                 // existing produce default
    pub fetch_max_messages: u32,  // default 128
    pub fetch_max_bytes: u32,     // default 4 * 1024 * 1024
    pub fetch_max_wait_ms: u32,   // default 0
    ...
}

impl Client {
    // unchanged:
    pub async fn fetch(
        &self,
        topic: &str,
        partition: u32,
        from: Offset,
        max_messages: u32,
        max_wait_ms: u32,
    ) -> Result<FetchResult>;

    pub async fn fetch_opts(
        &self,
        topic: &str,
        partition: u32,
        from: Offset,
        max_messages: u32,
        max_wait_ms: u32,
        max_bytes: u32,
    ) -> Result<FetchResult>;

    // new config-knob path:
    pub async fn fetch_default(
        &self,
        topic: &str,
        partition: u32,
        from: Offset,
    ) -> Result<FetchResult> {
        self.fetch_opts(
            topic,
            partition,
            from,
            self.config.fetch_max_messages,
            self.config.fetch_max_wait_ms,
            self.config.fetch_max_bytes,
        ).await
    }
}
```

```rust
let c = Client::connect(ClientConfig::default()).await?;
c.fetch_default("t", 0, Offset::ZERO).await?;          // 128 / 4MiB / 0
c.fetch("t", 0, Offset::ZERO, 7, 0).await?;            // 7 / 4MiB / 0

let c = Client::connect(ClientConfig {
    fetch_max_messages: 10,
    fetch_max_bytes: 4096,
    fetch_max_wait_ms: 100,
    ..ClientConfig::default()
}).await?;
c.fetch_default("t", 0, Offset::ZERO).await?;          // 10 / 4096 / 100
```

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Fake TCP stub (v0.76 / v0.113 style) that records decoded Fetch
`max_messages` / `max_bytes` / `max_wait_ms`:

| Case | Expect |
|------|--------|
| Default ClientConfig `fetch_default` | wire max_messages=128, max_bytes=4MiB, max_wait_ms=0 |
| Config fetch_max_messages=10, fetch_max_bytes=4096, fetch_max_wait_ms=100 | those knobs on the Fetch request |
| Existing `fetch(topic, part, off, 7, 0)` | still 7 / 4MiB / 0 (unchanged) |

Existing `v76_group_poll_fetch_knobs.rs` must still pass (poll stays
100 / 4 MiB).

| File | What |
|------|------|
| `crates/volant-client/src/config.rs` | Fetch knobs + defaults |
| `crates/volant-client/src/client.rs` | `fetch_default` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v144_fetch_default.rs` | stub records Fetch knobs |
| `docs/V144_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka Fetch.** Native opcode **2** only.
- `Client::fetch` signature is unchanged and still hardcodes 4 MiB.
- GroupConsumer poll knobs stay historical **100 / 4 MiB** (v0.76).
- Language 3-arg Fetch is a sibling residual (v0.143).
- No new retry / redirect. Existing Fetch 13 redirect stays.
- No Kafka API keys / opcodes / broker / protocol / Phase 155.

## Merge notes

Sibling slices that also edit `client.rs` / `config.rs` / crate-doc
should keep this hunk local to Fetch defaults:

- **Keep `fetch_default` as the config-knob path.** Do not change
  `fetch` / `fetch_opts` signatures.
- Do **not** wire GroupConsumer poll to `ClientConfig` Fetch fields.
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/config.rs` (`ClientConfig` fields /
  `Default`)
- `crates/volant-client/src/client.rs` — hunk is local to `fetch` /
  new `fetch_default`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V76_SPEC.md](./V76_SPEC.md) — Rust GroupConsumer poll fetch knobs
  (historical 100 / 4 MiB)
- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Fetch knobs (Client)
- [V129_SPEC.md](./V129_SPEC.md) — language produce default acks
  (`ClientConfig.acks` already existed)
- [V75_SPEC.md](./V75_SPEC.md) — Python / Go / Java poll fetch knobs
