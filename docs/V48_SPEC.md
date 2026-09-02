# v0.48 — GroupConsumer auto-commit

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “language GroupConsumers require an
explicit `commit()` / `Commit()`.” Python / Go / Java now offer
**opt-in** auto-commit after a successful `poll` that returned records.
Default stays **off**.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change the broker.

## Goals

1. After a successful `poll` that returned ≥1 record, if auto-commit is
   on:
   - interval **0**: commit immediately (same as explicit commit:
     member_id + generation, assigned positions only);
   - interval **> 0**: commit if never committed yet **or**
     `now - last_auto_commit >= interval`.
2. **First successful poll always auto-commits**, then the interval
   applies.
3. `close` / `Close`: if auto-commit is on and there are uncommitted
   (dirty) positions, **best-effort commit once**, then LeaveGroup.
4. Explicit `commit()` still works and resets the interval clock.
5. Commit failures: surface on explicit commit and on auto-commit after
   poll (do not swallow). On close, best-effort (swallow, still leave).
6. Default **off**: existing unit tests that poll without commit stay
   valid.

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust `volant-client` auto-commit | Language clients were required; Rust stays explicit |
| Kafka `enable.auto.commit` background thread | Commits run after `poll` / on `close`, not on a timer independent of poll |
| Changing the broker OffsetCommit path | Same member+generation commit as today |
| Kafka API keys / native opcodes | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Default is explicit commit (today). Opt-in:

| Language | How to opt in |
|----------|----------------|
| Python | `GroupConsumer.join(..., auto_commit=False, auto_commit_interval_ms=5000)` |
| Go | `WithAutoCommit(interval time.Duration)` — duration 0 = commit after every successful Poll. `JoinGroupConsumer` 4-arg signature is unchanged. |
| Java | `GroupConsumer.joinWithAutoCommit(client, group, topics, timeout, intervalMs)` — named method so it does not collide with `join(..., boolean heartbeat)` or `join(..., String assignor)` |

```python
g = GroupConsumer.join(c, "g", ["t"], auto_commit=True, auto_commit_interval_ms=5000)
g = GroupConsumer.join(c, "g", ["t"], auto_commit=True, auto_commit_interval_ms=0)
```

```go
g, err := JoinGroupConsumer(c, "g", []string{"t"}, 10_000, WithAutoCommit(5*time.Second))
g, err = JoinGroupConsumer(c, "g", []string{"t"}, 10_000, WithAutoCommit(0))
```

```java
GroupConsumer g = GroupConsumer.joinWithAutoCommit(c, "g", List.of("t"), 10_000, 5000);
GroupConsumer g0 = GroupConsumer.joinWithAutoCommit(c, "g", List.of("t"), 10_000, 0);
```

Existing constructors stay valid and combine:

- Python: `group_instance_id=`, `heartbeat=`, `assignor=`, `auto_commit=`
- Go: `JoinGroupConsumerStatic` + `WithBackgroundHeartbeat` + `WithAssignor` + `WithAutoCommit`
- Java: `join` / `joinStatic` / `join(..., boolean heartbeat)` / `join(..., String assignor)` unchanged; `joinWithAutoCommit` is additive

## Behavior

```
poll returns N records
    │
    ├─ N == 0 → no auto-commit
    │
    └─ N >= 1, auto-commit on
            │
            ├─ never committed yet → commit (first successful poll)
            ├─ interval == 0 → commit
            └─ interval > 0 and now - last < interval → skip (dirty stays)
```

- Dirty is set when a poll advances positions and cleared on a
  successful commit (auto or explicit).
- `close` with auto-commit on + dirty → best-effort commit, then leave.
- Empty-poll does **not** auto-commit even if the interval has elapsed;
  leftover dirty is flushed on close.

This is **not** Kafka `enable.auto.commit` / `auto.commit.interval.ms`
beyond “commit on an interval after poll.” There is no background
commit thread.

## Tests

Fake client / mock backend / fake TCP:

1. Default: poll does not commit.
2. Interval 0: poll of records → one commit with joined member+generation.
3. Interval 10_000: two quick polls → first successful poll auto-commits;
   the second does not.
4. Close with auto-commit on and pending positions → commit then leave.
5. Existing group tests still pass (`heartbeat=False` in unit tests).

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| File | What |
|------|------|
| `clients/python/tests/test_group.py` | Default off; interval 0; interval first-poll-only; close flushes dirty |
| `clients/go/group_test.go` | Same (`WithAutoCommit`, fake TCP) |
| `clients/java/.../GroupConsumerTest.java` | Same (`joinWithAutoCommit`, mock backend) |

## Files

| Path | Role |
|------|------|
| `clients/python/src/volant/group.py` | `auto_commit=` / `auto_commit_interval_ms=` |
| `clients/go/group.go` | `WithAutoCommit` |
| `clients/java/src/main/java/io/volant/GroupConsumer.java` | `joinWithAutoCommit` |
| `docs/V48_SPEC.md` | This spec |

## Honesty leftovers

- **Rust `GroupConsumer` is still explicit-only.** This slice is
  language clients.
- **Not Kafka `enable.auto.commit`.** No background commit timer
  independent of `poll`. Interval 0 means “after every successful poll,”
  not Kafka’s `auto.commit.interval.ms=0`.
- Default **off**. Callers that never `commit()` still do not commit.
- Close is **best-effort**: a failed auto-commit on close is swallowed
  so LeaveGroup still runs.
- Auto-commit after `poll` **raises** (Python / Java) or returns the
  error (Go). Positions may already have advanced.
- Not a fully concurrent consumer. One TCP connection. Sync only.
- No Kafka API keys / native opcodes / broker changes / Phase 155.

## Merge notes

This branch will conflict with **v0.36** (static membership),
**v0.37** (background heartbeat), and **v0.41** (assignor) on the
same group files. Keep **all four**:

- instance id (`group_instance_id` / `JoinGroupConsumerStatic` /
  `joinStatic`)
- heartbeat (`heartbeat=` / `WithBackgroundHeartbeat` /
  `join(..., boolean)`)
- assignor (`assignor=` / `WithAssignor` / `join(..., String)`)
- auto-commit (`auto_commit=` / `WithAutoCommit` /
  `joinWithAutoCommit`)

Do not drop any of those knobs to resolve the conflict.

## Related

- [V31_SPEC.md](./V31_SPEC.md) — Python GroupConsumer
- [V32_SPEC.md](./V32_SPEC.md) — Go GroupConsumer
- [V33_SPEC.md](./V33_SPEC.md) — Java GroupConsumer
- [V36_SPEC.md](./V36_SPEC.md) — static membership
- [V37_SPEC.md](./V37_SPEC.md) — background heartbeat
- [V41_SPEC.md](./V41_SPEC.md) — client-side range assignor
