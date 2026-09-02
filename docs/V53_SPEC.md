# v0.53 — DescribeConfigs / AlterConfigs on Python / Go / Java clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “language clients still lack DescribeConfigs /
AlterConfigs.” Rust `volant-client` already has `Client::describe_configs`
and `alter_configs`. This slice ports native opcodes **40 / 41** and
**42 / 43** (Phase 13) to the Python, Go, and Java clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / protocol / Rust client.

These are **topic** configs only (same as Rust). Not BROKER
Describe/AlterConfigs.

## Goals

1. **Codec** encode/decode for DescribeConfigs request/response
   (opcodes **40 / 41**) and AlterConfigs request/response (opcodes
   **42 / 43**) in Python, Go, and Java. Match
   `crates/volant-protocol/src/payload.rs` `Request::DescribeConfigs` /
   `Response::DescribeConfigs` / `Request::AlterConfigs` /
   `Response::AlterConfigs`.
2. **Public RPC** on each language client. Config pairs reuse the same
   CreateTopic trailer encoding (`u32` count + key/value strings).
3. Non-zero `error_code` raises like every other RPC (`BrokerError` /
   `BrokerError` / `BrokerException`) with `op="describe_configs"` /
   `op="alter_configs"`.
4. Unit tests without a broker: payload fixture topic `"events"`, one
   config `retention.ms=86400000`; AlterConfigs empty-value clear;
   fake TCP describe returns pairs, alter ok, error 2 raises.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka DescribeConfigs / IncrementalAlterConfigs (API keys 32/33/44) | Native 40–43 only |
| BROKER resource | Phase 99 stays Kafka/Rust |
| New native opcodes | Reuse 40 / 41 / 42 / 43 |
| Broker / protocol / Rust client changes | Already shipped (Phase 13) |
| CreatePartitions / DeleteRecords / DeleteOffsets / SCRAM admin | Separate leftovers |
| Kafka API keys | Frozen (`SUPPORTED_APIS` at 38) |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`. Payload integers are
little-endian. Strings are `u16` length + UTF-8 (`put_string`).

### Request opcode 40 `DescribeConfigs`

```
topic: string
```

### Response opcode 41 `DescribeConfigs`

```
error_code: u16     # 0=ok; 2=not found
topic: string
topic_id: u32       # 0 if unknown
partition_count: u32
config_count: u32
  for each: key string, value string   # empty value = unset
```

### Request opcode 42 `AlterConfigs`

```
topic: string
config_count: u32
  for each: key string, value string   # empty value clears that key
```

### Response opcode 43 `AlterConfigs`

```
error_code: u16     # 0=ok; 2=not found
topic: string
```

This is **not** Kafka DescribeConfigs / AlterConfigs. There is no
resource type, no synonyms, no IncrementalAlterConfigs SET/DELETE
ops, and no BROKER resource on these opcodes.

## API

`DescribeConfigsResult` has `topic`, `topic_id`, `partition_count`,
`configs` (list of key/value pairs).

```python
c.describe_configs(topic: str) -> DescribeConfigsResult
# result: topic, topic_id, partition_count, configs: list[tuple[str,str]]
c.alter_configs(topic: str, configs: list[tuple[str,str]]) -> None
c.describe_configs("events")
c.alter_configs("events", [("retention.ms", "86400000")])
c.alter_configs("events", [("retention.ms", "")])  # clear
```

```go
c.DescribeConfigs(topic string) (DescribeConfigsResult, error)
c.AlterConfigs(topic string, configs [][2]string) error
c.DescribeConfigs("events")
c.AlterConfigs("events", [][2]string{{"retention.ms", "86400000"}})
c.AlterConfigs("events", [][2]string{{"retention.ms", ""}}) // clear
```

```java
c.describeConfigs(String topic)  // DescribeConfigsResult
c.alterConfigs(String topic, List<String[]> configs)
c.describeConfigs("events");
c.alterConfigs("events", List.of(new String[] {"retention.ms", "86400000"}));
c.alterConfigs("events", List.of(new String[] {"retention.ms", ""})); // clear
```

Non-zero `error_code` raises `BrokerError(..., op="describe_configs")`
/ `op="alter_configs"` (Python), `BrokerError{Code, Op}` (Go), or
`BrokerException(code, "", op)` (Java).

Python `create_topic` already accepts `configs=`. All three languages
reuse the same pair encoding helpers as the CreateTopic trailer.

## Tests

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode DescribeConfigs topic `"events"`, `retention.ms=86400000` | request topic string; response one pair |
| Encode/decode AlterConfigs empty value | request value length 0 (clear) |
| Fake server describe | parsed `DescribeConfigsResult` pairs |
| Fake server alter | ok |
| Fake server `error_code != 0` | raises with `op="describe_configs"` / `op="alter_configs"` |
| `decode_response(41, …)` / `decode_response(43, …)` | Describe / Alter response types |

## Merge notes

v0.51 / v0.52 / v0.54 / v0.55 are parallel residual slices that also
edit the language **codec** / Client / README files. When merging:

- **Keep all opcodes.** Do not drop 40–43 or any opcode another slice
  added (`decode_response` / `DecodeResponse` / `decodeResponse` is a
  switch — union every case).
- Describe/AlterConfigs is **additive**: request **40** / **42**,
  response **41** / **43**. Do not reuse those numbers for a new API.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- **Not Kafka DescribeConfigs / IncrementalAlterConfigs.** Native
  40–43 only. No resource type, synonyms, or IncrementalAlterConfigs
  SET/DELETE. Those stay on the Kafka shim (API keys 32/33/44).
- **Topic configs only.** No BROKER resource (Phase 99 stays
  Kafka/Rust).
- Empty alter value **clears** the key (same as Rust).
- Language clients still lack CreatePartitions / DeleteRecords /
  DeleteOffsets / SCRAM admin.
- No Kafka API keys / new opcodes / broker changes / Phase 155.

See [PHASE13_SPEC.md](./PHASE13_SPEC.md) (native 40–43) and
[PHASE99_SPEC.md](./PHASE99_SPEC.md) (BROKER Describe/AlterConfigs
on the Kafka shim).
