# v0.204 — Go CreateTopic returns topic id

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Phase:** 155 PR2  
**Theme:** Break Go `CreateTopic` / `CreateTopicDefault` so they return
the broker-assigned topic id, matching Python `create_topic` / Java
`createTopic` / Rust `create_topic`. `CreateTopicID` stays as a named
alias.

This is Phase 155 PR2. It does **not** add Kafka API keys, homemade
Raft, or flip openraft defaults.

## Goals

1. **Go:** `CreateTopic(name, partitions) (uint32, error)` via
   `CreateTopicWithConfigs(name, partitions, nil)`.
2. **Go:** `CreateTopicDefault(name) (uint32, error)` via
   `CreateTopic(name, 1)`.
3. **Go:** `CreateTopicID` is an alias of `CreateTopic` (keep the name).
4. Update every Go caller that assigned `if err := c.CreateTopic(...)`
   / `CreateTopicDefault` to `if _, err := ...`.
5. `CreateTopicWithConfigs` is unchanged.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka CreateTopics / IncrementalAlterConfigs | Native opcode 3 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Homemade 154 RequestVote / InstallSnapshot | Freeze choice C; replace, do not finish |
| Openraft cluster default flip | Later Phase 155 PR |
| Native SyncGroup 116/117 | Later Phase 155 PR |
| JoinGroup retry | Later Phase 155 PR |
| Python / Java / Rust signatures | Already return topic id |
| Crate 0.3.0 | After 155 ships, not during |

## API

```go
func (c *Client) CreateTopic(name string, partitions int) (uint32, error) {
    return c.CreateTopicWithConfigs(name, partitions, nil)
}
func (c *Client) CreateTopicDefault(name string) (uint32, error) {
    return c.CreateTopic(name, 1)
}
func (c *Client) CreateTopicID(name string, partitions int) (uint32, error) {
    return c.CreateTopic(name, partitions) // alias; keep the name
}
```

`CreateTopicWithConfigs` is unchanged.

```go
id, err := c.CreateTopic("events", 1)
id, err = c.CreateTopicDefault("events")
id, err = c.CreateTopicID("events", 1) // same as CreateTopic
id, err = c.CreateTopicWithConfigs("events", 1, [][2]string{{"retention.ms", "1000"}})
```

## Semantics

- Success returns the CreateTopic response `topic_id`.
- Error 14 (`NotController`) follows `maxRedirects` via
  `adminRoundTrip`. Transient 6 / 7 / 15 / 16 and TCP/IO follow
  `maxRetries` (default 0). 14 is not a retry.
- Empty configs (nil → zero-length Phase 13 trailer) on
  `CreateTopic` / `CreateTopicDefault` / `CreateTopicID`.
- This is an API break: previous `error`-only signatures no longer
  compile. Callers must capture or discard the id.

## Tests

```bash
cd clients/go && go test ./...
```

| Case | Expect |
|------|--------|
| `CreateTopic(name, n)` | returns the response topic id; empty configs |
| `CreateTopicID(name, n)` | same id (alias) |
| `CreateTopicDefault("events")` | encodes partitions=1; returns `(uint32, error)` |
| Existing CreateTopic 14 / retry tests | still pass (`adminRoundTrip` inherit) |

Do **not** change broker / protocol / Python / Java / Rust. Do **not**
run cargo workspace. Do **not** rewrite historical specs
(`V199_SPEC.md`, `V126_SPEC.md`, …).

## Honesty leftovers

- Not Kafka CreateTopics / IncrementalAlterConfigs.
- No new retry/redirect policy.
- No Kafka API keys / opcodes.
- Homemade 154 and openraft defaults are unchanged (later 155 PRs).
- Python / Java / Rust / broker / protocol are unchanged.

## Related

- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155 (this is PR2)
- [V126_SPEC.md](./V126_SPEC.md) — Go CreateTopicID (error-only
  CreateTopic leftover this closes)
- [V199_SPEC.md](./V199_SPEC.md) — Go CreateTopicDefault (error-only)
- [V117_SPEC.md](./V117_SPEC.md) — Go/Java CreateTopic configs
- [V203_SPEC.md](./V203_SPEC.md) — Rust create_topic_default
