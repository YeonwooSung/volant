# v0.126 — Go CreateTopicID returns topic id

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V117_SPEC.md](./V117_SPEC.md): Go
`CreateTopic(name, partitions) error` still discards the
broker-assigned topic id. `CreateTopicWithConfigs` already returns
`uint32`. Python `create_topic` and Java `createTopic` already return
the id.

Add a thin public method that returns the id. Keep
`CreateTopic(name, partitions) error` unchanged (many callers). Reuse
`CreateTopicWithConfigs` / `adminRoundTrip` so error **14** and
transient retry are inherited. No new retry/redirect policy.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Python, Java, or Rust.

## Goals

1. **Go:** `CreateTopicID(name, partitions) (uint32, error)` via
   `CreateTopicWithConfigs(name, partitions, nil)`.
2. **Go:** `CreateTopic` stays `error`-only and still calls
   `CreateTopicWithConfigs` with nil configs, discarding the id.
3. No new retry/redirect policy. Error 14 and transient 6 / 7 / 15 /
   16 inherit `adminRoundTrip`.
4. Do **not** wrap JoinGroup, OffsetCommit, Produce, or acks
   (siblings v0.127–v0.130).
5. Do **not** change Python / Java (they already return topic id),
   Rust, broker, or protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `CreateTopic(name, n) error` | Compatibility; many callers |
| Python `create_topic` / Java `createTopic` | Already return topic id |
| Rust `create_topic` | Out of scope |
| JoinGroup / OffsetCommit / Produce / acks | Siblings v0.127–v0.130 |
| Kafka CreateTopics / IncrementalAlterConfigs | Native opcode 3 only |
| New retry / redirect policy | Inherit `adminRoundTrip` |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

```go
func (c *Client) CreateTopic(name string, partitions int) error {
    _, err := c.CreateTopicWithConfigs(name, partitions, nil)
    return err
}

func (c *Client) CreateTopicID(name string, partitions int) (uint32, error) {
    return c.CreateTopicWithConfigs(name, partitions, nil)
}

func (c *Client) CreateTopicWithConfigs(name string, partitions int, configs [][2]string) (uint32, error)
```

```go
c.CreateTopic("events", 1)
id, _ := c.CreateTopicID("events", 1)
id, _ = c.CreateTopicWithConfigs("events", 1, [][2]string{{"retention.ms", "1000"}})
```

## Semantics

- `CreateTopicID` is `CreateTopic` plus the response topic id. It
  sends empty configs (nil → zero-length Phase 13 trailer).
- Error 14 (`NotController`) follows `maxRedirects` via
  `adminRoundTrip`. Transient 6 / 7 / 15 / 16 and TCP/IO follow
  `maxRetries` (default 0). 14 is not a retry.
- Existing `CreateTopic` still returns only `error`.

## Tests

Fake TCP. Existing CreateTopic 14 / retry / configs tests must still
compile.

```bash
(cd clients/go && go test ./...)
```

| Case | Expect |
|------|--------|
| `CreateTopicID(name, n)` | returns the response topic id; empty configs |
| `CreateTopic(name, n)` | still `error`-only; still sends empty configs |
| One existing CreateTopic 14 test | still passes (`adminRoundTrip` inherit) |

## Honesty leftovers

- **Not Kafka** CreateTopics / IncrementalAlterConfigs.
- `CreateTopic(name, n)` still discards topic id (signature
  unchanged).
- No new retry/redirect policy.
- **No Kafka API keys / opcodes / Phase 155.**
- Python / Java / Rust / broker / protocol are unchanged.
- JoinGroup / OffsetCommit / Produce / acks are unchanged
  (siblings v0.127–v0.130).

## Merge notes

Sibling slices **v0.127–v0.130** (JoinGroup / OffsetCommit / Produce /
acks) also edit Go `client.go`. When merging:

- **Keep the CreateTopicID wrapper only.** Do not wrap JoinGroup,
  OffsetCommit, Produce, or acks.
- Keep `CreateTopic` as the `error`-only wrapper that sends empty
  configs and discards the id.
- Keep `adminRoundTrip` as the only transport (14 + transient retry
  inherit).
- Do not change the broker, Kafka shim, Python, Java, or Rust.

Expect conflicts on:

- Go `clients/go/client.go` — hunk is local to `CreateTopic` / new
  `CreateTopicID`

The hunk is local to CreateTopic.

## Related

- [V117_SPEC.md](./V117_SPEC.md) — Go/Java CreateTopic configs;
  leftover this closes
- [V72_SPEC.md](./V72_SPEC.md) — admin NotController 14 redirect
- Python `create_topic` / Java `createTopic` already return topic id
