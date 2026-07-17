# Phase 5 — Docs + Affinity Plan (iteration 1)

## Goal

Ship the Phase 5 **tuning guide**, update **ROADMAP / README** for DMA /
high-performance I/O, and add optional **CPU affinity / thread-per-core**
hooks on `volant-server`. Do **not** rewrite the storage engine core
(owned by the storage-io agent).

Binding: **[docs/PHASE5_SPEC.md](../PHASE5_SPEC.md)**.

## Deliverables

| Item | Path / action |
|------|----------------|
| Plan | `docs/phase5/docs-affinity-plan.md` (this file) |
| Spec | `docs/PHASE5_SPEC.md` (exists; polish exit checklist honestly) |
| Review | `docs/phase5/docs-affinity-review.md` |
| Tuning guide | `docs/tuning.md` (new) |
| ROADMAP | Phase 5 milestones checked **honestly**; Phase 6 next when exit met |
| README | feature flags, bench how-to, link to tuning guide, Phase 5 status |
| Optional affinity | `volant-server` feature `thread-per-core` + `VOLANT_CPU_LIST` |

## Current codebase snapshot (plan time)

| Area | Status in this worktree | Owner |
|------|-------------------------|-------|
| `volant-storage` features `io-uring` / `direct-io` | **not present** | storage-io agent |
| Buffer pool / `IoBackend` | **not present** | storage-io agent |
| Broker batch coalesce | produce already loops batch under one lock; no intermediate flush | broker-batch agent polish |
| `volant-bench` multi-mode CLI | **single append micro-bench only** | bench agent |
| Tuning guide | **missing** | **this agent** |
| Server CPU affinity | **missing** | **this agent** |

## Tuning guide outline (`docs/tuning.md`)

1. **Overview** — what Phase 5 optimizes (copies, sequential I/O, pinning)
2. **OS limits** — `ulimit -n`, file descriptors, `nofile` systemd
3. **VM / page cache** — `vm.dirty_ratio`, `vm.dirty_background_ratio`, `vm.dirty_expire_centisecs`
4. **Disk** — scheduler (`none`/`mq-deadline` for NVMe), mount options, fsync policy vs throughput
5. **O_DIRECT** — when/why, 4 KiB alignment, sealed mmap vs active direct write
6. **io_uring** — when/why, Linux-only, feature flag, sync submit+wait vs full async
7. **Huge pages** — note only (transparent huge pages; no custom driver)
8. **CPU affinity / thread-per-core** — `VOLANT_CPU_LIST`, feature flag, isolation tips
9. **Network** — NIC queues, `SO_REUSEPORT` future, DPDK/AF_XDP **research only**
10. **Benchmarks** — how to run `volant-bench` release modes
11. **Feature flag matrix** — storage / server flags and platforms

## Server affinity design

### Feature flag

```toml
# crates/volant-server/Cargo.toml
[features]
default = []
thread-per-core = ["dep:core_affinity"]
```

### Env

- `VOLANT_CPU_LIST=0,1,2` — comma-separated CPU indices
- Feature **off** (default): no affinity code path; zero runtime cost
- Feature **on**, env unset/empty: log info and continue unpinned
- Feature **on**, env set:
  - Parse list; invalid tokens → warn + skip
  - Build multi-thread Tokio runtime with `worker_threads = list.len()`
  - `on_thread_start`: pin each worker to the next CPU in the list (round-robin)
  - Unsupported / pin failure → **warn**, do not abort (macOS best-effort)

### Platform policy

| Platform | Behavior |
|----------|----------|
| Linux + feature | pin via `core_affinity` |
| macOS + feature | best-effort pin; warn if unavailable |
| Feature off | no-op everywhere (default macOS CI green) |

### Non-goals

- Do not replace Tokio
- Do not pin accept loop separately from workers in Phase 5
- Do not require `thread-per-core` for default builds

## ROADMAP honesty rules

Mark a Phase 5 milestone `[x]` only if code/docs exist in-tree:

1. Linux `io_uring` path — only if feature + backend present
2. O_DIRECT path — only if feature present
3. Batch produce coalescing — check if single-lock batch append path exists
4. Kernel bypass research — check if documented in `tuning.md`
5. CPU affinity — check after server hooks land
6. Buffer pool — only if `pool.rs` present
7. Exit: tuning guide, feature flags documented, default build green

If storage-io / bench agents have not landed, Phase 5 stays **partial** with
completed docs/affinity items checked; do **not** claim full Phase 5 ✅ unless
exit criteria are met. If partial: keep “Phase 5 next / in progress” and still
point Phase 6 as the **following** phase in the roadmap table.

## README updates

- Status line: Phase 5 (DMA / high-perf I/O) — document progress honestly
- Link `docs/PHASE5_SPEC.md` + `docs/tuning.md`
- **Feature flags** section (storage + server)
- **Benchmarks** section: `cargo run -p volant-bench --release` (+ subcommands if present)
- Roadmap summary table: Phase 5 status accurate

## Implementation order

1. Write this plan
2. Implement `thread-per-core` on `volant-server`
3. Write `docs/tuning.md`
4. Update `ROADMAP.md` + `README.md`
5. Light polish `PHASE5_SPEC.md` exit checklist to match reality
6. Review doc + `cargo build -p volant-server` (default and `--features thread-per-core`)
7. Fix issues; document iterations in review

## Tests / verification

```bash
cargo build -p volant-server
cargo build -p volant-server --features thread-per-core
# optional smoke:
VOLANT_CPU_LIST=0 cargo run -p volant-server --features thread-per-core -- --help
```

## Non-goals

- Storage engine rewrite, `io_uring` / `O_DIRECT` implementation
- DPDK / AF_XDP code
- Published laptop bench numbers if bench agent has not expanded harness
- Changing default macOS build dependencies

## Success criteria

- [x] `docs/tuning.md` comprehensive and accurate to current + planned flags
- [x] ROADMAP Phase 5 milestones honest; Phase 6 described as next major phase
- [x] README documents flags, benches, tuning link
- [x] Optional affinity: feature-gated, env-driven, no-op/warn on failure
- [x] Default `cargo build -p volant-server` green on macOS
- [x] Plan + review under `docs/phase5/`
