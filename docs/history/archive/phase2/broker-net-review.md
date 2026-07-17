# Phase 2 — Broker + Net Review

## Checklist

- [x] `Arc<Broker>` shared safely (parking_lot `RwLock` + atomics; no interior mutability races)
- [x] Frame decode/encode with `correlation_id` preserved end-to-end (`pack_response(corr, ...)`)
- [x] Partition `-1` key-hash (Kafka murmur2) / round-robin (`select_partition`, `Topic.rr_counter`)
- [x] Fetch `max_wait_ms` long-poll (10ms poll until data or deadline; empty Fetch on timeout)
- [x] `delete_topic` + `metadata` (on-disk cleanup; missing topics → error_code 2)
- [x] Server binds and accepts (`volant-server --listen`, `net::serve`)
- [x] `missing_docs` clean (`#![deny(missing_docs)]`); no panics on bad clients (errors → Error frames / disconnect)

## Iteration log

### Iteration 1

**Plan:** `docs/phase2/broker-net-plan.md`

**Code:**
1. Extended `volant-protocol` with full Phase 2 Request/Response payloads, LE encode/decode,
   CRC verify, 16 MiB cap, opcodes DeleteTopic=5 / OffsetCommit=6 / OffsetFetch=7.
2. Broker: `delete_topic`, `metadata`, `partition_count`, `select_partition`, `high_watermark`,
   `fetch_limited`, `set_advertised`; murmur2 helper; RR on `Topic`.
3. `volant_broker::net::{serve, serve_addr}` — accept loop, per-conn task, dispatch, long-poll.
4. `volant-server` binds `--listen`, serves until Ctrl-C.

**Tests:**
```
cargo test -p volant-protocol   # 5 passed (frame + payload roundtrips)
cargo test -p volant-broker     # unit + partition_select + tcp_smoke + existing = all green
cargo build -p volant-server    # ok
```

Notable tests:
- `partition_select`: keyed stable; RR 0..n-1 cycle
- `tcp_smoke::tcp_create_produce_fetch`: bind `127.0.0.1:0`, create/produce/fetch/metadata/delete
- `tcp_smoke::tcp_fetch_long_poll_returns_empty`: waits ≥60ms for 80ms max_wait

**Findings / fixes in iteration 1:**
- Protocol was placeholder-only → extended crate (prefer over local stubs) matching PHASE2_SPEC.
- Removed unused imports in `net.rs`.
- Checksum verified on every frame decode; mismatch closes connection after protocol error path.
- No second iteration needed — all checklist items green on first full cycle.

## Iteration count

**1** (plan → code → review → test; all pass)

## Files changed

| Path | Change |
|------|--------|
| `docs/phase2/broker-net-plan.md` | Plan |
| `docs/phase2/broker-net-review.md` | This review |
| `crates/volant-protocol/src/request.rs` | Real request payloads + opcodes |
| `crates/volant-protocol/src/response.rs` | Real response payloads + ErrorCode |
| `crates/volant-protocol/src/codec.rs` | encode/decode/pack + CRC + tests |
| `crates/volant-protocol/src/lib.rs` | Re-exports |
| `crates/volant-broker/src/broker.rs` | Extensions + unit tests |
| `crates/volant-broker/src/topic.rs` | `rr_counter` |
| `crates/volant-broker/src/murmur.rs` | Kafka murmur2 |
| `crates/volant-broker/src/net.rs` | TCP server |
| `crates/volant-broker/src/lib.rs` | Module exports |
| `crates/volant-broker/tests/partition_select.rs` | Partition tests |
| `crates/volant-broker/tests/tcp_smoke.rs` | TCP integration |
| `crates/volant-server/src/main.rs` | Listen + serve + ctrl-c |
