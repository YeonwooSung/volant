# v0.32 — Go high-level GroupConsumer

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Go has JoinGroup / Heartbeat / LeaveGroup
(v0.28) but no high-level consumer” with a `GroupConsumer` that matches
the Rust `volant-client` loop: join, positions from OffsetFetch (or 0),
poll = heartbeat + fetch assigned partitions, commit with
`member_id` + `generation`, rejoin on error 9, honor revoked.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change the broker protocol.

## Goals

1. **Go** `JoinGroupConsumer` / `Poll` / `Commit` / `Close` on the
   existing native client (`clients/go/group.go`).
2. Same semantics as Rust `crates/volant-client/src/group.rs`:
   first join sends empty `member_id`; positions from OffsetFetch
   (`u64::MAX` → 0); cooperative handoff (retain sticky, fetch only
   added, drop revoked); poll heartbeats then fetches assignment;
   commit last+1 with member + generation; rejoin on 9 / 10 / 11.
3. **Unit tests** against a local fake coordinator (no broker).
4. **E2E** gated by `VOLANT_E2E=1`: join → poll → commit → resume;
   two members split partitions. Skip if no server.

## Non-goals

| Deferred | Why |
|----------|-----|
| Python / Java GroupConsumer | This slice is Go only |
| Custom assignor | Broker already assigns; client honors it |
| Static membership (`join_static`) | Rust-only; empty `group_instance_id` |
| Kafka consumer / `kafka-go` | Native opcodes only |
| Broker / protocol changes | Wire already exists (v0.24 / v0.28) |
| Thin-client Heartbeat returning a result | Still `BrokerError` on nonzero code |
| Required CI language job | Existing optional smoke scripts only |

## API

```go
g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000)
recs, err := g.Poll(500 * time.Millisecond)
err = g.Commit()
err = g.Close()
```

`sessionTimeoutMs` 0 defaults to 10000. `Poll(0)` is non-blocking
(`max_wait_ms=0`). A positive timeout is the Fetch max-wait budget for
that call (one heartbeat + one fetch pass over assigned partitions).

`Commit` sends OffsetCommit with the joined `member_id` and
`generation` (not the admin empty-member / generation-0 path).
`Close` sends LeaveGroup and does **not** close the `Client`.
Idempotent; later `Poll` / `Commit` return `ErrGroupClosed`.

Accessors: `Assignment`, `LastRevoked`, `MemberID`, `Generation`,
`GroupID`, `Positions`. `FetchedRecord` carries topic, partition, and
the wire `Record`.

## Loop

```text
JoinGroupConsumer:
  JoinGroup (empty member_id on first join)
  OffsetFetch assigned partitions; unknown (u64::MAX) → 0

Poll:
  Heartbeat
  if error_code 9 / 10 / 11 → re-JoinGroup (keep member_id)
      drop revoked positions; OffsetFetch only newly added
  Fetch each assigned partition from current position
  advance position to last+1

Commit:
  OffsetCommit(member_id, generation, positions)

Close:
  LeaveGroup
```

Rejoin unions local `old − new` with the broker `revoked` trailer
(Phase 17). Sticky-kept partitions keep their in-memory position.

## Tests

| File | What |
|------|------|
| `clients/go/group_test.go` | Fake TCP loop: positions from OffsetFetch / 0; poll heartbeat + fetch; commit member+generation; rejoin on 9 + honor revoked; Close leaves |
| `clients/go/group_test.go` | Live join → poll → commit → resume; two-member split; skip unless `VOLANT_E2E=1` |

```bash
(cd clients/go && go test ./...)
# live:
cargo build -p volant-server
VOLANT_E2E=1 go test ./clients/go -count=1
```

## Honesty leftovers

- No Python / Java high-level GroupConsumer (Rust still has the
  reference; this slice is Go only).
- No static membership / `group_instance_id` on the Go constructor
  (always dynamic).
- No custom assignor; the broker’s assignment is authoritative.
- Thin `Client.Heartbeat` still fails on error 9 (`BrokerError`);
  only `GroupConsumer.Poll` rejoins.
- Thin `Client.OffsetCommit` remains the admin path (empty member,
  generation 0). `GroupConsumer.Commit` is the member path.
- `Poll` is one heartbeat + one fetch pass, not a long-running
  background loop. Sync only; one TCP connection; not concurrent-safe.
- Still no Kafka-wire SDK, SCRAM / shared-token auth, or leader
  redirect on the Go client.
- Broker and Rust `volant-client` are unchanged.

See [clients/go/README.md](../clients/go/README.md) and
[V28_SPEC.md](./V28_SPEC.md).
