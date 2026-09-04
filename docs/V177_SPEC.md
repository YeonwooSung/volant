# v0.177 — language single-entry AlterConfig

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V53_SPEC.md](./V53_SPEC.md) /
[V93_SPEC.md](./V93_SPEC.md): `AlterConfigs` / `alter_configs` /
`alterConfigs` already take a list of key/value pairs. There is no
one-key helper matching `DeleteOffset` / `delete_offset`. Rust is
unchanged (already has batch-only AlterConfigs).

Add `AlterConfig` / `alterConfig` / `alter_config`. Reuse the existing
batch method (do not reimplement the RPC). `AlterConfigs` /
`alter_configs` / `alterConfigs` stay unchanged. Empty value still
clears that key. Topic-only. This is **not** Kafka IncrementalAlterConfigs.

This is residual **v0.177** (language single-entry AlterConfig). It is
**not** Phase 155. It does **not** open Phase 155, add Kafka API
keys, add native opcodes, or change the broker, protocol, or Rust.

## Goals

1. **Go:** public `func (c *Client) AlterConfig(topic, key, value string) error`
   that calls `AlterConfigs(topic, [][2]string{{key, value}})`.
2. **Java:** named `void alterConfig(String topic, String key, String value)`
   that calls `alterConfigs(topic, Collections.singletonList(new String[] {key, value}))`.
   Do not add an overload that could collide with `alterConfigs`.
3. **Python:** public `def alter_config(self, topic: str, key: str, value: str) -> None`
   that calls `self.alter_configs(topic, [(key, value)])`.
4. Inherit retry / error **14** from the existing batch method
   (`adminRoundTrip` / `_admin_round_trip`; v0.93 error 14 + v0.103
   transient retry). No new retry policy.
5. Do **not** change `AlterConfigs` / `alter_configs` / `alterConfigs`
   / `DescribeConfigs` / `describe_configs` / `describeConfigs`.
6. Do **not** change broker / protocol / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `AlterConfigs` / `alter_configs` / `alterConfigs` | Frozen; list still accepts one or many |
| Change `DescribeConfigs` / `describe_configs` / `describeConfigs` | Frozen; topic-only describe |
| Kafka DescribeConfigs / IncrementalAlterConfigs (API keys 32/33/44) | Native opcodes 40–43 only |
| BROKER resource | Phase 99 stays Kafka/Rust |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Rust `alter_config` | Unchanged this slice |
| Phase 155 / homemade Raft | Frozen |

## API

```go
func (c *Client) AlterConfig(topic, key, value string) error {
    return c.AlterConfigs(topic, [][2]string{{key, value}})
}
```

```java
public void alterConfig(String topic, String key, String value) {
    alterConfigs(topic, Collections.singletonList(new String[] {key, value}));
}
```

```python
def alter_config(self, topic: str, key: str, value: str) -> None:
    return self.alter_configs(topic, [(key, value)])
```

```go
_ = c.AlterConfig("events", "retention.ms", "1")                 // one key
_ = c.AlterConfigs("events", [][2]string{{"retention.ms", "1"}}) // unchanged
_ = c.AlterConfigs("events", [][2]string{{"retention.ms", ""}})  // unchanged: clear
```

```java
c.alterConfig("events", "retention.ms", "1");                    // one key
c.alterConfigs("events", Collections.singletonList(new String[] {"retention.ms", "1"})); // unchanged
c.alterConfigs("events", Collections.singletonList(new String[] {"retention.ms", ""}));  // unchanged: clear
```

```python
c.alter_config("events", "retention.ms", "1")                    # one key
c.alter_configs("events", [("retention.ms", "1")])               # unchanged
c.alter_configs("events", [("retention.ms", "")])                # unchanged: clear
```

## Semantics

- One-key helpers send wire count **1** with that key/value pair.
- They do not re-encode; they wrap the existing batch method.
- Empty value still clears that key (same as the batch method).
- `AlterConfigs` / `alter_configs` / `alterConfigs` are unchanged.
- `DescribeConfigs` / `describe_configs` / `describeConfigs` are
  unchanged.
- Topic configs only. Not Kafka IncrementalAlterConfigs SET/DELETE.
- Transient 6 / 7 / 15 / 16 and transport retry via the batch method
  (`adminRoundTrip`; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.93).
- Not Kafka DescribeConfigs / IncrementalAlterConfigs.

## Tests

Fake TCP stub that records decoded AlterConfigs pairs (same helpers
as existing `describe_alter_configs_test.go` /
`DescribeAlterConfigsTest.java` / `test_describe_alter_configs.py`).
Existing batch tests stay green.

```bash
(cd clients/go && go test ./...)
(cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_describe_alter_configs -q)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| `AlterConfig` / `alterConfig` / `alter_config` (`"events"`, `"retention.ms"`, `"1"`) | wire configs count 1; key `retention.ms`, value `1` |
| Existing `AlterConfigs` empty-value / error cases | still pass |

Existing AlterConfigs retry / 14 tests must still pass
(batch methods unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `AlterConfig` wraps `AlterConfigs` with one pair |
| `clients/go/describe_alter_configs_test.go` | one-pair wire check |
| `clients/go/README.md` | usage line + one prose sentence |
| `clients/java/src/main/java/io/volant/Client.java` | named `alterConfig` wraps batch |
| `clients/java/src/test/java/io/volant/DescribeAlterConfigsTest.java` | one-pair wire check |
| `clients/java/README.md` | usage line + one prose sentence |
| `clients/python/src/volant/client.py` | `alter_config` wraps `alter_configs` |
| `clients/python/tests/test_describe_alter_configs.py` | one-pair wire check |
| `clients/python/README.md` | usage line + one prose sentence |
| `docs/V177_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** DescribeConfigs / IncrementalAlterConfigs.
- Topic configs only (no BROKER resource).
- Empty value still clears that key.
- `AlterConfigs` / `alter_configs` / `alterConfigs` /
  `DescribeConfigs` are unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust are unchanged.

## Merge notes

Sibling slices that also edit language `Client` should keep this hunk
local to the one-key wrappers:

- **Keep the wrappers only.** Do not change `AlterConfigs` /
  `alter_configs` / `alterConfigs` / `DescribeConfigs` /
  `describe_configs` / `describeConfigs`.
- Do not change the AlterConfigs send loop (`adminRoundTrip` / v0.93
  error 14).
- Do not change Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` — hunk is local to `AlterConfig` after
  `AlterConfigs`
- Java `clients/java/src/main/java/io/volant/Client.java` —
  named `alterConfig` after the batch method
- Python `clients/python/src/volant/client.py` — `alter_config`
  after the batch method
- `clients/*/README.md` and the existing Describe/AlterConfigs test
  files

## Related

- [V53_SPEC.md](./V53_SPEC.md) — language Describe/AlterConfigs
- [V93_SPEC.md](./V93_SPEC.md) — language Describe/AlterConfigs error 14
- [V103_SPEC.md](./V103_SPEC.md) — language admin_round_trip transient retry
- [V164_SPEC.md](./V164_SPEC.md) — language single-entry DeleteOffset
- [V169_SPEC.md](./V169_SPEC.md) — language single-entry CreateAcl / DeleteAcl
- [PHASE13_SPEC.md](./PHASE13_SPEC.md) — native 40–43
