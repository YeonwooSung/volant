# Phase 2 — Broker + Net Plan (Iteration 1)

## Goals

Implement broker API extensions, partition assignment (murmur2 / RR), and a Tokio TCP
server that multiplexes request/response frames over `correlation_id`.

## Broker methods to add

| Method | Behavior |
|--------|----------|
| `delete_topic(name)` | Remove topic from map; drop partition logs; `remove_dir_all` topic data dir |
| `metadata(topics: Option<&[TopicName]>)` | Snapshot: single broker node + topic/partition HWM list |
| `partition_count(topic)` | Number of partitions for topic |
| `select_partition(topic, key)` | Key → Kafka murmur2 `% n`; `None` → atomic RR counter per topic |
| `high_watermark(topic, partition)` | Next offset for metadata / fetch responses |
| `fetch_limited(..., max_bytes)` | Fetch with byte limit (uses storage `read_bytes`) |

Existing `create_topic` / `produce` / `fetch` remain; net handler maps wire types onto them.

Internal state additions:

- `Topic.rr_counter: AtomicU32` for null-key round-robin
- `Broker.node_id`, `advertised_host`, `advertised_port` for metadata (settable)

Partition helpers:

- Local `murmur2(data) -> u32` (Kafka seed `0x9747b28c`, positive mod via `& 0x7fff_ffff`)

## Net module design (`volant_broker::net`)

```
serve(listener, Arc<Broker>)
  └─ accept loop
       └─ spawn per-connection task
            └─ handle_connection(stream, Arc<Broker>)
                 read → BytesMut → decode_frame (checksum verify)
                 → decode_request(opcode, payload)
                 → dispatch_request(broker, req)  // async (long-poll)
                 → encode_response / pack_response
                 → write frame (same correlation_id)
```

Public API:

- `pub async fn serve(listener: TcpListener, broker: Arc<Broker>) -> Result<()>`
- `pub async fn serve_addr(addr: &str, broker: Arc<Broker>) -> Result<()>` (bind helper)

Error path: any failure → `Response::Error { code, message }` with opcode `0xFFFF`,
still using the request's `correlation_id`. Bad frames / checksum / oversized payload
close the connection after an Error frame when possible; decode failures on framing
close the connection cleanly (no panics).

## Request → Broker mapping

| Wire request | Broker call |
|--------------|-------------|
| Produce | If `partition == -1`, `select_partition` from first message key (else RR). Build `MessageBatch`, `produce`. Respond base_offset + count |
| Fetch | Long-poll loop: `fetch_limited` until records non-empty, timeout, or `max_wait_ms == 0`. HWM from `high_watermark` |
| CreateTopic | `create_topic(name, partitions)` |
| DeleteTopic | `delete_topic(name)` |
| Metadata | `metadata(Some(topics) or None)` → map to wire brokers/topics |

Unsupported opcodes (OffsetCommit/Fetch) → Error code `unsupported` (8).

## Long-poll fetch strategy

1. Try fetch immediately.
2. If empty and `max_wait_ms > 0`, sleep ~10ms and retry until data or deadline.
3. Return empty Fetch response (error_code=0) on timeout — not an Error frame.

## Protocol crate (light extension)

Protocol still has placeholder enums. Extend to PHASE2_SPEC field layouts so net can
encode/decode without local stubs:

- Opcodes: DeleteTopic=5; OffsetCommit=6; OffsetFetch=7
- Real `Request` / `Response` variants with fields
- `encode_request` / `decode_request` / `encode_response` / `decode_response`
- `pack_request` / `pack_response` with CRC
- Payload LE multi-byte ints; string `u16`+utf8; bytes `u32`+data; optional bytes `u32::MAX`=None
- Frame decode: verify CRC; reject payload > 16 MiB

## volant-server

- Parse `--listen`
- Build `Arc<Broker>` with storage `data_dir`
- Set advertised host/port from listen addr when possible
- `TcpListener::bind` + `net::serve` until `ctrl_c`

## Tests

1. **Unit (broker):** `select_partition` same key → same partition; null keys RR across partitions
2. **Unit (protocol):** request/response roundtrip for Produce/Fetch/Create/Delete/Metadata
3. **Integration (broker):** TCP smoke — bind `127.0.0.1:0`, create topic, produce, fetch
4. Existing in-process / durable tests must stay green

## Files touched

- `docs/phase2/broker-net-plan.md` (this file)
- `docs/phase2/broker-net-review.md`
- `crates/volant-protocol/src/{request,response,codec,lib}.rs`
- `crates/volant-broker/src/{broker,topic,lib}.rs` + new `net.rs`, `murmur.rs`
- `crates/volant-broker/Cargo.toml` (if needed)
- `crates/volant-broker/tests/*`
- `crates/volant-server/src/main.rs`
