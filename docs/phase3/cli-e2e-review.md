# Phase 3 — CLI + E2E Review

## Iterations

| Iter | Outcome | Notes |
|------|---------|-------|
| 1 | **Green** | Implemented full group stack (protocol → broker → client → CLI → e2e → docs). One e2e fix: first poll records were discarded during rebalance sync. |

**Iteration count: 1** (plus one test fix within the same iteration).

## Deliverables checklist

### CLI
- [x] `volant group fetch-offsets --group G [--topic T --partition P]`
- [x] `volant group commit --group G --topic T --partition P --offset N`
- [x] `volant consume --group G` joins, polls, commits, leaves
- [x] Standalone `volant consume --partition P` unchanged

### E2E (`crates/volant-client/tests/e2e_group.rs`)
- [x] Two consumers one group → disjoint partitions covering all
- [x] Commit + new consumer resumes from committed offsets
- [x] Admin commit/fetch offsets

### Docs
- [x] `ROADMAP.md` Phase 3 checkboxes; Phase 4 marked next
- [x] `README.md` consumer groups section + CLI examples
- [x] Plan: `docs/phase3/cli-e2e-plan.md`
- [x] Review: this file

### Supporting stack (implemented because placeholders)
- [x] Protocol opcodes 6–10 + error codes 9–12 + LE payloads
- [x] `GroupCoordinator`, range assignor, file-backed offset store
- [x] Net dispatch for group RPCs + session expiry task
- [x] `GroupConsumer` client API

## Test results

```
cargo test --workspace   # all pass (broker 13, protocol 7, e2e_group 3, e2e_tcp 3, …)
cargo build -p volant-cli -p volant-server  # ok
```

## Design notes / deviations

1. **Existing-member re-join does not bump generation** when topics unchanged.
   Prevents rebalance thrashing when lagging members re-sync after another
   member's join/leave. New members still bump + reassign (eager rebalance).

2. **Offset store** uses simple files under `{data_dir}/__consumer_offsets/`
   (not a user-visible topic), per PHASE3_SPEC preference.

3. **Admin commits** use `generation=0` + empty `member_id` (CLI path).

## Open / deferred

- Sticky / cooperative assignor (Phase 3.1)
- Lag metrics per group/partition
- Auth, TLS, multi-node coordinator

## Blockers

None. Workspace tests green.
