# v0.155 — Rust DeleteRecords default wait flag

This is a residual MVP, **not Phase 155**. Crate 0.2.0 (unchanged).

**Status:** Residual MVP (not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V144_SPEC.md](./V144_SPEC.md) /
[V149_SPEC.md](./V149_SPEC.md) / language sibling v0.152: Rust
`ClientConfig` already has `acks` and Fetch knobs.
`delete_records_with_wait_flag` already takes an explicit Phase 137
flag. `Client::delete_records` still **hardcodes** `wait_majority: 0`.

Wire `Client::delete_records` to `self.config.delete_records_wait`
(default **0** = broker default). Do **not** change the
`delete_records` (still 3 args) or `delete_records_with_wait_flag`
signatures. The explicit-flag path stays fully explicit.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Python/Go/Java.

## Goals

1. Add `ClientConfig.delete_records_wait: u8` default **0**
   (0 = broker default, 1 = force wait, 2 = force no-wait; Phase 137).
2. Change `Client::delete_records` to pass
   `self.config.delete_records_wait` into
   `delete_records_with_wait_flag` instead of hardcoded `0`.
3. Keep `Client::delete_records(topic, partition, before_offset)`
   signature as-is (still 3 args).
4. Keep `Client::delete_records_with_wait_flag` as the explicit-flag
   path (ignores config).
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `delete_records` / `delete_records_with_wait_flag` signatures | Frozen; `delete_records` still 3 args |
| Language client-level DeleteRecords wait | Sibling residual (v0.152) |
| Kafka DeleteRecords (API key 21) | Native opcode 44 only |
| New retry / redirect | Existing DeleteRecords 13 redirect + transient retry stay (v0.111) |
| Broker / protocol / Python / Go / Java | Frozen |
| Kafka API keys / new opcodes | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Semantics

- Default-config `delete_records(...)` sends `wait_majority=0`
  (broker default).
- After `cfg.delete_records_wait = 1`, `delete_records(...)` sends
  `wait_majority=1`.
- `delete_records_with_wait_flag(..., 2)` still sends
  `wait_majority=2` even when config is 1.
- Leader-13 redirect and transient retry stay as-is (v0.111).
  Retry / redirect resend the same request (same flag).

## API

```rust
impl Client {
    // signature unchanged; wait_majority now from config:
    pub async fn delete_records(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
    ) -> Result<DeleteRecordsResult> {
        self.delete_records_with_wait_flag(
            topic, partition, before_offset,
            self.config.delete_records_wait,
        ).await
    }

    // still explicit:
    pub async fn delete_records_with_wait_flag(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
        wait_majority: u8,
    ) -> Result<DeleteRecordsResult>;
}
```

```rust
let c = Client::connect(ClientConfig::default()).await?;
c.delete_records("t", 0, 100).await?;                 // wait_majority=0

let c = Client::connect(ClientConfig {
    delete_records_wait: 1,
    ..ClientConfig::default()
}).await?;
c.delete_records("t", 0, 100).await?;                 // wait_majority=1
c.delete_records_with_wait_flag("t", 0, 100, 2).await?; // wait_majority=2
```

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Fake TCP stub (v0.111 DeleteRecords style) that records decoded
`wait_majority`:

| Case | Expect |
|------|--------|
| Default config `delete_records` | wait_majority=0 |
| Config `delete_records_wait=1` then `delete_records` | wait_majority=1 |
| `delete_records_with_wait_flag(..., 2)` | wait_majority=2 (ignores config) |

Existing `v111_delete_records_retry.rs` must still pass (retry /
redirect unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/config.rs` | `delete_records_wait` field + Default 0 |
| `crates/volant-client/src/client.rs` | `delete_records` uses config flag |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v155_delete_records_wait.rs` | default / configured / explicit flag |
| `docs/V155_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka DeleteRecords.** Native opcode **44** only.
- `Client::delete_records` signature is unchanged (still 3 args).
- Language client-level wait is a sibling residual (v0.152).
- No new retry / redirect. Existing DeleteRecords 13 redirect +
  transient retry stay (v0.111).
- No Kafka API keys / opcodes / broker / protocol / Phase 155.

## Merge notes

Sibling slices that also edit `client.rs` / `config.rs` / crate-doc
should keep this hunk local to DeleteRecords wait:

- **Keep `delete_records` as the config-wait path.** Do not change
  `delete_records` / `delete_records_with_wait_flag` signatures.
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `delete_records`
- `crates/volant-client/src/config.rs` (`delete_records_wait` field /
  `Default`)
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V52_SPEC.md](./V52_SPEC.md) — language DeleteRecords
- [V111_SPEC.md](./V111_SPEC.md) — Rust DeleteRecords 13 redirect +
  transient retry
- [V144_SPEC.md](./V144_SPEC.md) — Rust ClientConfig Fetch knobs
- [V149_SPEC.md](./V149_SPEC.md) — Rust fetch uses ClientConfig
  `fetch_max_bytes`
- [PHASE137_SPEC.md](./PHASE137_SPEC.md) — per-request
  `wait_majority` trailer (0 / 1 / 2)
