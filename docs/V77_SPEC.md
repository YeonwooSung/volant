# v0.77 — Metadata controller_id trailer

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V72_SPEC.md](./V72_SPEC.md): native
Metadata has **no** `controller_id`. v0.72 hunts via `controller_id=` in
the error message or “first other advertised broker.” Add a
**backward-compatible trailer** on Metadata response (same pattern as
JoinGroup revoked trailer).

This is a residual slice. It does **not** open Phase 155, add Kafka API
keys, add new native opcodes, or change admin redirect helpers.

## Goals

1. `Response::Metadata` gains `controller_id: u32`.
2. Broker `Request::Metadata` handler fills `broker.controller_id()`
   (or 0 if none).
3. Rust `volant-client` `Metadata` struct + `Client::metadata()` expose
   `controller_id`.
4. Python / Go / Java codecs encode and decode the trailer. Existing
   constructors default to **0**.
5. Do **not** wrap create_topic / redirect helpers. Field is available;
   unused by redirect in this slice.

## Non-goals

| Deferred | Why |
|----------|-----|
| Consume trailer in admin 14 redirect | Next residual; v0.72 hunt stays |
| Kafka Metadata `controller_id` tagged field | Native opcode 5 only; not Kafka API keys |
| New native opcodes | Trailer on existing Metadata response |
| Phase 155 / homemade Raft election | Frozen |

## Wire

Current Metadata response is brokers then topics. After the last topic,
encoders **always write** `controller_id: u32` LE.

```
… existing Metadata fields (brokers, topics) …
controller_id: u32     # v0.77 trailer; always written
```

**Decode:** if remaining bytes ≥ 4, read `u32` LE as `controller_id`;
else treat as **0** (legacy broker). Do not fail on short legacy
payloads.

`0` means unknown / single-node / no openraft leader. This is **not**
the Kafka Metadata `controller_id` tagged field.

## Semantics

| Source | Value |
|--------|-------|
| `VOLANT_OPENRAFT_METADATA` on | openraft leader id, or `0` if none |
| Clustered, flag off | lowest live broker id (`Membership::controller_id`) |
| Single-node | `Broker::node_id` (typically `0`) |

## Tests

```bash
cargo test -p volant-protocol
cargo test -p volant-client -- --test-threads=1
(cd clients/python && PYTHONPATH=src python3 -m unittest discover -s tests -q)
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode Metadata with controller_id=2 | round-trip 2 |
| Decode legacy payload (no trailer) | controller_id=0, topics intact |
| Broker metadata (single-node e2e) | field present; 0 on single-node is ok |

Language codec tests live next to existing metadata codec tests.

## Files

| Path | Role |
|------|------|
| `crates/volant-protocol/src/response.rs` | `Response::Metadata.controller_id` |
| `crates/volant-protocol/src/payload.rs` | encode always; decode optional |
| `crates/volant-broker/src/net/dispatch.rs` | fill `broker.controller_id()` |
| `crates/volant-client/src/client.rs` | `Metadata.controller_id` |
| `clients/python/src/volant/codec.py` | `MetadataResponse.controller_id` default 0 |
| `clients/go/codec/codec.go` | `MetadataResponse.ControllerID` zero value |
| `clients/java/src/main/java/io/volant/Metadata.java` | ctor overload default 0 |
| `docs/V77_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka Metadata `controller_id`.** Native trailer after topics;
  no tagged fields / flexible versions.
- **`0` means unknown**, not “node 0 is never the controller.”
  Single-node `Broker::new` uses `node_id=0`, so 0 is also the real id.
- **Redirect helpers still use the v0.72 hunt** (`controller_id=` in
  the error message, else first other advertised broker). They do not
  read this trailer yet.
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Siblings **v0.79** (`Metadata` struct on rust `client.rs`) and **v0.78**
(language codecs) also edit these files. Keep this hunk local to
Metadata types / encode / decode. Do not wrap create_topic or redirect
helpers in this merge.

## Related

- [V72_SPEC.md](./V72_SPEC.md) — admin NotController redirect (hunt)
- [PHASE17_SPEC.md](./PHASE17_SPEC.md) — JoinGroup revoked trailer pattern
- [V11_SPEC.md](./V11_SPEC.md) — `controller_id()` / openraft leader
