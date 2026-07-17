# Phase 5 — Docs + Affinity Review

## Iteration log

| Iteration | What | Result |
|-----------|------|--------|
| 1 | Plan → implement tuning guide, ROADMAP/README, server `thread-per-core` | Builds green; docs accurate to in-tree state |
| — | (no further code fix required) | — |

**Total iterations: 1**

## Deliverables checklist

| Deliverable | Path | Status |
|-------------|------|--------|
| Plan | `docs/phase5/docs-affinity-plan.md` | ✅ |
| Review | `docs/phase5/docs-affinity-review.md` | ✅ |
| Tuning guide | `docs/tuning.md` | ✅ |
| ROADMAP Phase 5 honest milestones | `ROADMAP.md` | ✅ |
| README flags + benches + tuning link | `README.md` | ✅ |
| PHASE5_SPEC exit checklist honesty | `docs/PHASE5_SPEC.md` | ✅ |
| Server affinity feature | `crates/volant-server` | ✅ |

## Tuning guide coverage (`docs/tuning.md`)

- [x] Overview / DMA definition
- [x] Feature flag matrix (storage + server)
- [x] `ulimit` / `LimitNOFILE`
- [x] `vm.dirty_ratio` / background / expire / writeback
- [x] Disk scheduler, mount options, fsync policy
- [x] mmap default path + `StorageConfig` knobs
- [x] O_DIRECT when/why, alignment, sealed mmap choice
- [x] io_uring when/why, Linux-only, submit+wait note
- [x] Huge pages note (THP / not required)
- [x] CPU affinity / `VOLANT_CPU_LIST` / isolation tips
- [x] Network ops notes
- [x] DPDK / AF_XDP **research only**
- [x] Benchmarks how-to
- [x] Production starting profile + troubleshooting

## Affinity implementation review

**Crate:** `volant-server`  
**Feature:** `thread-per-core` → optional dep `core_affinity 0.8`  
**Env:** `VOLANT_CPU_LIST=0,1,2`

| Requirement | Implementation | OK? |
|-------------|----------------|-----|
| Feature-gated | `#[cfg(feature = "thread-per-core")]` module; default features empty | ✅ |
| Env parse | comma-separated `usize`; skip invalid tokens with warn | ✅ |
| Unset env | info log; unpinned multi-thread runtime | ✅ |
| Pin workers | `worker_threads = list.len()` + `on_thread_start` pin | ✅ |
| Failure policy | warn, never abort | ✅ |
| macOS default build | no `core_affinity` without feature | ✅ |
| No storage rewrite | only `volant-server` touched for code | ✅ |

### Code shape

- `main()` builds runtime (affinity-aware when feature on), then `block_on(async_main)`
- Avoids `#[tokio::main]` so worker count / `on_thread_start` can be configured
- Pin uses `core_affinity::set_for_current(CoreId { id })`

## ROADMAP honesty

Checked **only** what exists in this worktree after this agent:

| Milestone | Checked? | Reason |
|-----------|----------|--------|
| io_uring path | no | storage feature not present |
| O_DIRECT path | no | storage feature not present |
| Batch produce single-lock | yes | `Broker::produce` loops batch under one lock |
| DPDK/AF_XDP research doc | yes | `docs/tuning.md` |
| thread-per-core | yes | server feature + env |
| Buffer pool | no | not present |
| Tuning guide | yes | new |
| Multi-mode bench | no | still single append micro-bench |
| Published numbers | no | not pasted |

Phase 5 title: **partial** (docs + affinity). Phase 6 marked as **next major phase** after full Phase 5 exit.

## Build verification

```text
cargo build -p volant-server                          → OK (exit 0)
cargo build -p volant-server --features thread-per-core → OK (exit 0)
cargo run -p volant-server --features thread-per-core -- --help → OK
```

Platform: macOS (default build must stay green — verified).

## Issues found / fixed

None blocking. Notes:

1. Storage `io-uring` / `direct-io` flags are **documented** as planned/feature-gated per PHASE5_SPEC but **not implemented** in this worktree — README and ROADMAP state this explicitly so operators are not misled.
2. Bench multi-mode CLI is documented as “when available”; current harness remains the Phase 1 append micro-bench.
3. `core_affinity` added to `Cargo.lock` only via optional dependency path; default build does not link it.

## Non-goals respected

- Did not rewrite storage engine core
- Did not implement DPDK / AF_XDP
- Did not force Linux-only deps into default features
- Did not claim full Phase 5 complete

## Sign-off

Docs accurate to in-tree reality; default and feature builds green; affinity is best-effort with warn-on-failure. Ready for merge of docs + server affinity slice.
