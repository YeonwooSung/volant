# v0.178 — Rust alter_config

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V53_SPEC.md](./V53_SPEC.md) /
[V94_SPEC.md](./V94_SPEC.md): language clients are adding a singular
`alter_config` wrapper (v0.177). Rust only has batch
`Client::alter_configs(topic, configs: Vec<(String, String)>)`
(empty value still clears that key). There is no one-key helper.

Add `Client::alter_config`. Reuse `alter_configs` (do not reimplement
the RPC). Batch API stays unchanged. `describe_configs` stays
unchanged. Topic configs only. This is **not** Kafka
IncrementalAlterConfigs.

This is residual **v0.178** (Rust alter_config). It is **not** Phase
178 work. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, or change the broker, protocol, or Python/Go/Java.

## Goals

1. Add public `Client::alter_config(topic, key, value)` that calls
   `alter_configs(topic, vec![(key, value)])` (one pair on the wire).
2. Inherit retry / error **14** from `alter_configs`
   (`admin_round_trip`: v0.104 transient retry + v0.94 error 14).
   No new retry policy.
3. Empty `value` still clears that key (unchanged).
4. Do **not** change `alter_configs`.
5. Do **not** change `describe_configs`.
6. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `alter_configs` | Frozen; batch stays public |
| Kafka IncrementalAlterConfigs (API key 44) | Native opcode 42/43 only; empty value already clears |
| BROKER resource | Topic configs only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Go / Java | Sibling v0.177; do not wait or edit |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
pub async fn alter_config(&self, topic: &str, key: &str, value: &str) -> Result<()> {
    self.alter_configs(topic, vec![(key.to_owned(), value.to_owned())]).await
}
```

```rust
client.alter_config("events", "retention.ms", "1").await?;
client.alter_config("events", "retention.ms", "").await?; // clear
let _ = client.alter_configs("events", vec![
    ("retention.ms".into(), "86400000".into()),
]).await?; // unchanged batch
```

Empty `value` still clears that key (same as `alter_configs`).

## Semantics

- `alter_config` sends AlterConfigs (opcode 42) with **one** pair.
- `alter_configs` is unchanged (batch still accepted).
- `describe_configs` is unchanged.
- Empty `value` still clears that key.
- Transient 6 / 7 / 15 / 16 and transport retry via `alter_configs` /
  `admin_round_trip` (v0.104; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.94).
- Topic configs only. Not Kafka IncrementalAlterConfigs.

## Tests

Fake TCP stub that records decoded AlterConfigs topic + pairs.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `alter_config("events", "retention.ms", "1")` | AlterConfigs with **one** pair |
| Existing batch | `alter_configs` unchanged |

Existing `v94_describe_alter_configs_14.rs` must still pass (batch
unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `alter_config` wraps batch API |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v178_alter_config.rs` | fake TCP one-pair wire check |
| `docs/V178_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** IncrementalAlterConfigs / DescribeConfigs BROKER.
- Topic configs only.
- `alter_configs` is unchanged.
- `describe_configs` is unchanged.
- Empty `value` still clears that key.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the singular AlterConfigs wrapper:

- **Keep the named wrapper only.** Do not change `alter_configs`.
- Do not change the AlterConfigs send loop (v0.104 retry + v0.94 14).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `alter_config` next to `alter_configs`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V53_SPEC.md](./V53_SPEC.md) — language Describe/AlterConfigs
- [V93_SPEC.md](./V93_SPEC.md) — language Describe/AlterConfigs error 14
- [V94_SPEC.md](./V94_SPEC.md) — Rust Describe/AlterConfigs error 14
- [V104_SPEC.md](./V104_SPEC.md) — Rust admin_round_trip transient retry
- [PHASE13_SPEC.md](./PHASE13_SPEC.md) — native topic config opcodes 40–43
