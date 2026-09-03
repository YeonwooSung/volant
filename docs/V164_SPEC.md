# v0.164 — language single-entry DeleteOffsets

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V54_SPEC.md](./V54_SPEC.md) /
[V158_SPEC.md](./V158_SPEC.md): `DeleteOffsets` / `delete_offsets` /
`deleteOffsets` already take a list of entries (empty = all).
`DeleteOffsetsAll` is the named all-group helper. There is no
one-entry helper matching `OffsetCommit` / `offset_commit`. Rust is
sibling **v0.165**.

Add `DeleteOffset` / `deleteOffset` / `delete_offset`. Reuse the
existing batch method (do not reimplement the RPC).
`DeleteOffsets` / `delete_offsets` / `deleteOffsets` /
`DeleteOffsetsAll` stay unchanged. This is **not** Kafka OffsetDelete.

This is residual **v0.164** (language single-entry DeleteOffsets). It
is **not** Phase 155. It does **not** open Phase 155, add Kafka API
keys, add native opcodes, or change the broker, protocol, or Rust.

## Goals

1. **Go:** public `func (c *Client) DeleteOffset(group, topic string,
   partition uint32) (uint32, error)` that calls
   `DeleteOffsets(group, []codec.OffsetEntry{{Topic: topic,
   Partition: partition}})`.
2. **Java:** public `int deleteOffset(String group, String topic, int
   partition)` that calls `deleteOffsets(group,
   Collections.singletonList(new Codec.OffsetEntry(topic, partition)))`.
3. **Python:** public `def delete_offset(self, group: str, topic: str,
   partition: int) -> int` that calls
   `self.delete_offsets(group, [(topic, partition)])`.
4. Inherit retry / error **14** from the existing batch method (v0.78
   transient retry + v0.97 error 14). No new retry policy.
5. Do **not** change `DeleteOffsets` / `delete_offsets` /
   `deleteOffsets` / `DeleteOffsetsAll`.
6. Do **not** change broker / protocol / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `DeleteOffsets` / `delete_offsets` / `deleteOffsets` | Frozen; list still accepts one or many |
| Change `DeleteOffsetsAll` | Frozen; empty-all already shipped (v0.158) |
| Kafka OffsetDelete (API key 47) | Native opcode 38 only |
| Kafka DeleteGroups (API key 42) | No native DeleteGroups opcode |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Rust `delete_offset` | Sibling **v0.165** |
| Phase 155 / homemade Raft | Frozen |

## API

```go
func (c *Client) DeleteOffset(group, topic string, partition uint32) (uint32, error) {
    return c.DeleteOffsets(group, []codec.OffsetEntry{{Topic: topic, Partition: partition}})
}
```

```java
public int deleteOffset(String group, String topic, int partition) {
    return deleteOffsets(group, Collections.singletonList(new Codec.OffsetEntry(topic, partition)));
}
```

```python
def delete_offset(self, group: str, topic: str, partition: int) -> int:
    return self.delete_offsets(group, [(topic, partition)])
```

```go
n, _ := c.DeleteOffset("g", "t", 0)                             // one OffsetEntry
n, _ = c.DeleteOffsets("g", []codec.OffsetEntry{{Topic: "t", Partition: 0}})
n, _ = c.DeleteOffsetsAll("g")                                  // unchanged: all
n, _ = c.DeleteOffsets("g", nil)                                // unchanged: all
```

```java
int n = c.deleteOffset("g", "t", 0);                            // one OffsetEntry
n = c.deleteOffsets("g", Collections.singletonList(new Codec.OffsetEntry("t", 0)));
n = c.deleteOffsets("g");                                       // unchanged: all
```

```python
n = c.delete_offset("g", "t", 0)                                # one OffsetEntry
n = c.delete_offsets("g", [("t", 0)])
n = c.delete_offsets("g")                                       # unchanged: all
```

## Semantics

- One-entry helpers send wire count **1** with that topic + partition.
- They do not re-encode; they wrap the existing batch method.
- `DeleteOffsets` / `delete_offsets` / `deleteOffsets` are unchanged
  (nil/empty still mean all).
- `DeleteOffsetsAll` is unchanged (empty wire entries).
- Transient 6 / 7 / 15 / 16 and transport retry via the batch method
  (v0.78; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.97).
- Not Kafka OffsetDelete / DeleteGroups.

## Tests

Fake TCP stub that records decoded DeleteOffsets entries (same helpers
as existing `delete_offsets_test.go` / `DeleteOffsetsTest.java` /
`test_delete_offsets.py`). Existing empty-all tests stay green.

```bash
(cd clients/go && go test ./...)
(cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_delete_offsets -q)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| `DeleteOffset` / `deleteOffset` / `delete_offset` (`"g"`, `"events"`, `0`) | wire entries count 1; topic `events`, partition 0 |
| Existing `DeleteOffsets` empty / explicit / error / All cases | still pass |

Existing DeleteOffsets retry / 14 tests must still pass
(`DeleteOffsets` unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `DeleteOffset` wraps `DeleteOffsets` with one entry |
| `clients/go/delete_offsets_test.go` | one-entry wire check |
| `clients/go/README.md` | usage line + one prose sentence |
| `clients/java/src/main/java/io/volant/Client.java` | `deleteOffset` wraps `deleteOffsets` with one entry |
| `clients/java/src/test/java/io/volant/DeleteOffsetsTest.java` | one-entry wire check |
| `clients/java/README.md` | usage line + one prose sentence |
| `clients/python/src/volant/client.py` | `delete_offset` wraps `delete_offsets` with one pair |
| `clients/python/tests/test_delete_offsets.py` | one-entry wire check |
| `clients/python/README.md` | usage line + one prose sentence |
| `docs/V164_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** OffsetDelete / DeleteGroups.
- Empty entries still delete **all** committed offsets for the group.
- `DeleteOffsets` / `delete_offsets` / `deleteOffsets` /
  `DeleteOffsetsAll` are unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust are unchanged (Rust is sibling **v0.165**).

## Merge notes

Sibling slices that also edit language `Client` should keep this hunk
local to the one-entry wrappers:

- **Keep the wrappers only.** Do not change `DeleteOffsets` /
  `delete_offsets` / `deleteOffsets` / `DeleteOffsetsAll`.
- Do not change the DeleteOffsets send loop (v0.78 retry + v0.97 14).
- Do not change Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` — hunk is local to `DeleteOffset` after
  `DeleteOffsetsAll`
- Java `clients/java/src/main/java/io/volant/Client.java` —
  `deleteOffset` after `deleteOffsets`
- Python `clients/python/src/volant/client.py` — `delete_offset`
  after `delete_offsets`
- `clients/*/README.md` and the existing DeleteOffsets test files

## Related

- [V54_SPEC.md](./V54_SPEC.md) — language DeleteOffsets
- [V78_SPEC.md](./V78_SPEC.md) — language DeleteOffsets transient retry
- [V97_SPEC.md](./V97_SPEC.md) — language DeleteOffsets error 14
- [V158_SPEC.md](./V158_SPEC.md) — Go DeleteOffsetsAll
- [PHASE12_SPEC.md](./PHASE12_SPEC.md) — native 38/39
