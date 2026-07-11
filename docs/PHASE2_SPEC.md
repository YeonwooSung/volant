# Phase 2 — Network Protocol Spec (binding)

## Goals

External processes produce/consume over TCP against `volant-server`.

## Transport

- TCP, one connection multiplexes many request/response pairs via `correlation_id`
- Frame format: existing `volant-protocol` frame (magic `V`, version 1, BE multi-byte fields in header)
- Always set `checksum = crc32(payload)` on encode; verify on decode (optional strict verify flag; **do verify** in server/client)
- Max payload: 16 MiB (reject larger)

## Opcodes

| Opcode | Request | Response |
|--------|---------|----------|
| 1 | Produce | Produce |
| 2 | Fetch | Fetch |
| 3 | CreateTopic | CreateTopic |
| 4 | Metadata | Metadata |
| 5 | DeleteTopic | DeleteTopic |
| 6 | (reserved OffsetCommit) | |
| 0xFFFF | — | Error |

Note: Reassign DeleteTopic to 5; keep OffsetCommit/OffsetFetch as 6/7 for Phase 3. Update enums accordingly.

## Payload encoding

All multi-byte integers in **payloads** are **little-endian**. Strings: `u16 len` + UTF-8 bytes. Bytes: `u32 len` + data. Optional bytes: `u32 len` where `u32::MAX` means None.

### Error response payload

```
error_code: u16
message: string
```

Error codes: 0=ok (unused in Error frame), 1=unknown, 2=not_found, 3=invalid_arg, 4=storage, 5=protocol, 6=io, 7=timeout, 8=unsupported.

### Produce request

```
topic: string
partition: i32   # -1 = broker assigns (key-hash or round-robin)
acks: u8         # 0=no response wait needed still send response for simplicity; 1=default
message_count: u32
messages: repeated {
  key: optional bytes
  value: bytes
  timestamp_ms: i64   # -1 = broker now
  header_count: u32
  headers: repeated { name: string, value: bytes }
}
```

### Produce response

```
topic: string
partition: u32
base_offset: u64
count: u32
error_code: u16   # 0 ok
```

### Fetch request

```
topic: string
partition: u32
from_offset: u64
max_messages: u32
max_bytes: u32
max_wait_ms: u32   # 0 = non-blocking
```

### Fetch response

```
topic: string
partition: u32
high_watermark: u64
error_code: u16
record_count: u32
records: repeated {
  offset: u64
  timestamp_ms: i64
  key: optional bytes
  value: bytes
  header_count: u32
  headers: repeated { name: string, value: bytes }
}
```

### CreateTopic request/response

```
// req
name: string
partitions: u32

// resp
topic_id: u32
name: string
partitions: u32
error_code: u16
```

### DeleteTopic request/response

```
// req
name: string

// resp
name: string
error_code: u16
```

### Metadata request/response

```
// req
// empty topics list means all:
topic_count: u32
topics: repeated string

// resp
broker_count: u32
brokers: repeated { node_id: u32, host: string, port: u16 }
topic_count: u32
topics: repeated {
  name: string
  topic_id: u32
  error_code: u16
  partition_count: u32
  partitions: repeated {
    partition_id: u32
    leader: u32
    hwm: u64
  }
}
```

## Public Rust API (protocol crate)

```rust
// encode/decode Request and Response to/from payload Bytes
pub fn encode_request(req: &Request) -> Result<Bytes>;
pub fn decode_request(opcode: u16, payload: &[u8]) -> Result<Request>;
pub fn encode_response(resp: &Response) -> Result<Bytes>;
pub fn decode_response(opcode: u16, payload: &[u8]) -> Result<Response>;

// helpers wrapping frame header
pub fn pack_request(corr: u32, req: &Request) -> Result<Frame>;
pub fn pack_response(corr: u32, resp: &Response) -> Result<Frame>;
```

`Request` / `Response` enums must carry real fields (not placeholders).

## Broker extensions

```rust
impl Broker {
    pub fn delete_topic(&self, name: &TopicName) -> Result<()>;
    pub fn metadata(&self, topics: Option<&[TopicName]>) -> MetadataSnapshot;
    pub fn partition_count(&self, topic: &TopicName) -> Result<u32>;
    // produce with partition -1 handled at handler using:
    pub fn select_partition(&self, topic: &TopicName, key: Option<&[u8]>) -> Result<PartitionId>;
}
```

Partition assignment: if key Some, `murmur2(key) % n` (Kafka-compatible murmur2); if None, atomic round-robin per topic.

Fetch long-poll: if no data and max_wait_ms > 0, wait up to that duration (tokio sleep/interval poll) then return empty.

## Server (`volant-server` / broker net module)

Preferred structure:
- `volant-broker` gains `net` module: `run_server(addr, Arc<Broker>) -> Result<()>` OR keep server in `volant-server` calling broker.
- Accept loop, spawn task per connection
- Per connection: read frames into BytesMut, dispatch, write response frames
- Share `Arc<Broker>`

## Client (`volant-client`)

```rust
pub struct Client { ... }
impl Client {
  pub async fn connect(config: ClientConfig) -> Result<Self>;
  pub async fn create_topic(&self, name: &str, partitions: u32) -> Result<TopicId>;
  pub async fn delete_topic(&self, name: &str) -> Result<()>;
  pub async fn metadata(&self) -> Result<Metadata>;
  pub async fn produce(&self, topic: &str, partition: Option<u32>, messages: Vec<Message>) -> Result<ProduceResult>;
  pub async fn fetch(&self, topic: &str, partition: u32, from: Offset, max_messages: u32, max_wait_ms: u32) -> Result<FetchResult>;
}
// Producer/Consumer thin wrappers around Client
```

Connection: single TCP stream + Mutex for request serialization OR correlation-id map with split read/write tasks. **Simple approach OK:** `tokio::sync::Mutex` around stream, sequential request/response (still correct). Optional: better pipelining later.

## CLI

```
volant topic create NAME --partitions N --broker HOST:PORT
volant topic list --broker ...
volant topic delete NAME --broker ...
volant produce TOPIC --value STR [--key STR] [--partition N] --broker ...
volant consume TOPIC --partition N [--from OFFSET] [--max N] --broker ...
```

## Integration tests

Prefer `tests` in a crate or `volant-client/tests/e2e_tcp.rs`:
1. Bind server to `127.0.0.1:0`, spawn accept loop
2. Client create topic, produce, fetch, assert
3. Multi-partition key routing (same key → same partition)

## Non-goals

Auth, TLS, consumer groups, idempotent producer PID (stretch skip), Kafka wire compat.
