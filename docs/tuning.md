# Volant Tuning Guide

Performance and I/O tuning for single-node Volant brokers (Phase 5+).

This guide covers **OS limits**, **page cache / dirty ratios**, **disk**,
**feature-gated I/O paths** (`O_DIRECT`, `io_uring`), **CPU affinity**,
**huge pages**, and **research-only** kernel-bypass notes.

> **DMA** in Volant means fewer user↔kernel copies and device-friendly sequential
> I/O — not a custom hardware driver. See [PHASE5_SPEC.md](./PHASE5_SPEC.md).

---

## Quick checklist

| Area | Default recommendation |
|------|------------------------|
| File descriptors | `ulimit -n` ≥ 65536 for many partitions / clients |
| Durability vs throughput | v0.2: keep `flush_every_n = 0` + explicit flush (acks=1). Group-commit is not implemented. |
| Reads | mmap sealed segments (always on by default) |
| Writes | buffered sequential append (default); optional `direct-io` / `io-uring` on Linux |
| CPU pinning | optional `thread-per-core` + `VOLANT_CPU_LIST` on dedicated hosts |
| Kernel bypass | **not supported** — research only (DPDK / AF_XDP) |

---

## Feature flags

Feature flags are **off by default** so `cargo build --workspace` stays green on
macOS and other non-Linux hosts.

### `volant-storage`

| Feature | Platforms | Effect |
|---------|-----------|--------|
| *(default mmap path)* | all | Memory-mapped reads of sealed segments; buffered sequential append |
| `io-uring` | **Linux only** | Optional `IoBackend` for append/fsync via `io_uring` (sync submit+wait is acceptable) |
| `direct-io` | Linux/Unix | Open active segment with `O_DIRECT`; 4 KiB-aligned buffers from the pool |

Enable examples:

```bash
# Linux CI / production experiments
cargo build -p volant-storage --features io-uring
cargo build -p volant-storage --features direct-io
cargo build -p volant-storage --features "io-uring,direct-io"

# Default path (macOS / Linux) — no extra features
cargo build -p volant-storage
```

If `io-uring` is enabled on a non-Linux target, the crate should fail at compile
time with a clear `compile_error!` (or document a no-op stub as unsupported).

### `volant-server`

| Feature | Platforms | Effect |
|---------|-----------|--------|
| `thread-per-core` | all (best-effort) | Pin Tokio worker threads using `VOLANT_CPU_LIST` |

```bash
cargo build -p volant-server
cargo build -p volant-server --features thread-per-core

VOLANT_CPU_LIST=0,1,2 cargo run -p volant-server --features thread-per-core -- \
  --data-dir /tmp/vdata --listen 127.0.0.1:9092
```

### `volant-broker`

Batch produce already appends all messages in a single produce call under one
partition lock, then the network layer flushes once for acks=1. Optional
coalescing polish is tracked under Phase 5 broker work; there is no separate
runtime feature required for the basic path.

### Combining flags (workspace)

```bash
# Example Linux release build with optional I/O features
cargo build --release -p volant-server \
  --features thread-per-core
# Storage features (io-uring / direct-io) on volant-storage:
#   cargo build -p volant-storage --features "io-uring,direct-io"
```

---

## File descriptors (`ulimit`)

Each open segment (`.log` + `.index`), client connection, and mmap region
consumes OS resources. Under partition fan-out, the broker can exhaust the
default soft limit quickly.

### Interactive shell

```bash
ulimit -n          # show soft limit
ulimit -n 65536    # raise soft limit for this session
```

### systemd unit (Linux)

```ini
[Service]
LimitNOFILE=65536
# or
LimitNOFILE=infinity
```

### macOS

```bash
# Session
ulimit -n 65536

# Persistent limits may require launchd / /etc/limiters — prefer raising
# only for the broker process in production wrappers.
```

### Heuristic

```
need ≈ (partitions × 2 files) + (active client conns) + headroom
```

For 1k partitions and 1k clients, start at **65536** and monitor
`Too many open files` errors.

---

## Virtual memory & dirty page ratios (Linux)

Append-heavy workloads dirty page cache quickly. If `vm.dirty_*` is too
aggressive (large dirty cache before writeback), latency spikes appear when
the kernel suddenly flushes gigabytes.

### Key sysctls

| Sysctl | Role | Starting point for brokers |
|--------|------|----------------------------|
| `vm.dirty_ratio` | % of RAM that can be dirty before sync writeback stalls writers | 10–20 |
| `vm.dirty_background_ratio` | % at which background flush starts | 3–10 |
| `vm.dirty_expire_centisecs` | age (1/100 s) before dirty pages are eligible | 3000 (30 s) default; lower for tighter durability windows |
| `vm.dirty_writeback_centisecs` | how often the flusher wakes | 500 (5 s) default |

```bash
# Inspect
sysctl vm.dirty_ratio vm.dirty_background_ratio \
       vm.dirty_expire_centisecs vm.dirty_writeback_centisecs

# Example (ephemeral — use sysctl.d for persistence)
sudo sysctl -w vm.dirty_background_ratio=5
sudo sysctl -w vm.dirty_ratio=15
```

### Interaction with Volant flush policy

| `StorageConfig.flush_every_n` | Behavior |
|-------------------------------|----------|
| `0` (default, **v0.2 decision**) | Rely on OS + explicit `flush` (broker acks=1 path calls flush after produce) |
| `N > 0` | fsync roughly every N appends on the hot path — lower throughput, smoother durability |

There is **no group-commit window**. `flush_every_n` is a count of appends since
the last fsync, not a time-based coalescer. v0.2 keeps the default at `0`:
changing it was not justified by measurement (see benches below).

For **throughput benches**, keep `flush_every_n = 0` and measure with a single
end-of-run flush (as `volant-bench` does). For **latency-sensitive production**,
prefer explicit batch flush (acks=1) over huge dirty caches, and consider
`O_DIRECT` if page-cache pollution from other processes is a problem.

---

## Disk & filesystem

### Hardware

- Prefer **local NVMe** for partition logs.
- Avoid network filesystems (NFS, typical cloud shared FS) for the hot log path.
- Separate OS / log disks when possible so writeback does not fight root FS noise.

### I/O scheduler (Linux)

```bash
# NVMe usually exposes "none" (or mq-deadline). Check:
cat /sys/block/nvme0n1/queue/scheduler

# Example: set none for low-latency NVMe (device name varies)
echo none | sudo tee /sys/block/nvme0n1/queue/scheduler
```

### Mount options

- `noatime` (or `relatime`) reduces metadata writes on read-heavy fetch.
- Ensure enough free space for segment roll; retention should be configured
  (`retention_bytes` / `retention_ms`) so disks do not fill unbounded.

### fsync behavior

- Volant segment append is **sequential** — good for both HDD and SSD.
- fsync cost dominates small-message acks=1 latency; batch produces aggressively
  at the client and broker.
- Do not disable battery-backed write cache assumptions without measuring power-loss risk.

---

## mmap path (default)

Default storage uses:

- **Buffered sequential writes** to the active segment
- **`mmap` reads** for sealed (and refreshed active) segments via `memmap2`

This is the portable, production-default path. It rides the page cache and is
the correct choice on macOS and for most Linux deployments.

Tuning knobs in `StorageConfig`:

| Field | Default | Notes |
|-------|---------|-------|
| `segment_size` | 256 MiB | Larger → fewer rolls; more recovery scan cost |
| `use_mmap` | `true` | Keep on unless debugging I/O paths |
| `flush_every_n` | `0` | See durability section |
| `index_interval_bytes` | 4096 | Sparse index density (offset → file position) |
| `retention_ms` / `retention_bytes` | `None` | Enable in production to bound disk |

---

## O_DIRECT (`direct-io` feature)

### When to enable

Enable **only** when you need:

- Predictable latency under heavy page-cache pressure from co-tenants
- To avoid double-caching (app buffer + page cache) for large sequential appends
- Linux/Unix deployments with aligned buffer support wired in storage

### When **not** to enable

- macOS / default developer laptops (feature off; default build)
- Small messages without batching (alignment padding waste)
- When you rely on page cache for hot fetch of recently written data

### Alignment rules

- Writes must be multiples of the filesystem logical block size (typically **4096** bytes)
- Buffers should be allocated from an aligned pool (`BufferPool` when present)
- Phase 5 design: **active segment** uses direct writes; **sealed segments keep mmap** for reads

### Operational tips

```bash
# Confirm feature build (Linux)
cargo build -p volant-storage --features direct-io --release

# Bench with direct path when the bench harness supports --direct-io:
cargo run -p volant-bench --release -- append --direct-io
```

Misaligned `O_DIRECT` writes fail with `EINVAL` — treat alignment bugs as
hard failures in tests.

---

## io_uring (`io-uring` feature, Linux)

### When to enable

- Linux kernels with stable `io_uring` (5.10+ recommended for production experiments)
- High append rates where syscall overhead of `write`+`fsync` shows up in profiles
- Batching many appends or fsyncs per submit

### When **not** to enable

- macOS / Windows / non-Linux CI (default build must not require the feature)
- Workloads already bottlenecked on disk bandwidth or fsync durability policy
- Early bring-up — prove correctness on the std/`mmap` path first

### Design notes (Phase 5)

- Storage exposes an `IoBackend` trait (`StdIoBackend` vs `UringIoBackend`)
- Sync **submit + wait** for append batches is an acceptable Phase 5 implementation
- Full async uring integrated with Tokio is optional stretch work
- Fetch may remain mmap for sealed segments (zero-copy style reads)

```bash
cargo build -p volant-storage --features io-uring --release
# When bench supports it:
cargo run -p volant-bench --release -- append --io-uring
```

### Caveats

- Some container runtimes restrict uring; fall back to std I/O if init fails
- Combine with `direct-io` only after each path is validated independently
- Always re-run durable log tests with the feature enabled on Linux CI

---

## Huge pages (note)

Volant does **not** require huge pages or a custom allocator for Phase 5.

| Mechanism | Relevance |
|-----------|-----------|
| Transparent huge pages (THP) | May help or hurt latency; measure before forcing `always` |
| Explicit `hugetlbfs` | Not used by Volant today |
| `mlock` / pinned DMA buffers | Not required for mmap/`io_uring` paths in-tree |

If you experiment with THP:

```bash
cat /sys/kernel/mm/transparent_hugepage/enabled
# [always] madvise never  — distribution dependent
```

For latency-sensitive brokers, many operators prefer `madvise` or `never` and
rely on sequential I/O + CPU isolation instead. Treat huge pages as an
**ops experiment**, not a Volant requirement.

---

## CPU affinity / thread-per-core

### Feature + env

| Knob | Meaning |
|------|---------|
| Cargo feature `thread-per-core` | Compile affinity hooks into `volant-server` |
| `VOLANT_CPU_LIST=0,1,2` | Pin Tokio worker threads to these CPU indices |

```bash
# Build
cargo build -p volant-server --release --features thread-per-core

# Run with 4 dedicated cores (example)
VOLANT_CPU_LIST=2,3,4,5 ./target/release/volant-server \
  --data-dir /var/lib/volant --listen 0.0.0.0:9092
```

### Behavior

1. Feature **disabled** (default): no affinity code; normal multi-thread Tokio runtime.
2. Feature **enabled**, env unset/empty: log that pinning is skipped; run unpinned.
3. Feature **enabled**, env set: worker thread count = number of CPUs in the list;
   each worker best-effort pins to the next CPU (round-robin at thread start).
4. Pin failure (permissions, unsupported OS, invalid id): **warning only** — broker continues.

### Platform notes

| OS | Support |
|----|---------|
| Linux | Full `core_affinity` pin when permitted |
| macOS | Best-effort; may warn and continue (default CI stays green **without** the feature) |
| Other | Warn on failure; never hard-fail startup |

### Isolation tips (Linux)

```bash
# Example: reserve CPUs 2-5 for the broker via isolcpus / cpuset (ops-level)
# Then pin the process:
VOLANT_CPU_LIST=2,3,4,5 volant-server --data-dir /var/lib/volant
```

- Disable turbo / set performance governor when measuring p99.
- Avoid sharing pinned cores with noisy neighbors (IRQs, other JVMs).
- IRQ affinity (`/proc/irq/.../smp_affinity`) is out of scope for Volant code
  but matters on high-pps hosts.

---

## Network (broker TCP)

Current stack: Tokio TCP + length-prefixed Volant frames.

| Topic | Guidance |
|-------|----------|
| Listen backlog | OS default via Tokio; raise `net.core.somaxconn` under connection storms |
| Nagle | Tokio/Tokio-tcp typically fine for request/response; batch produces client-side |
| Bandwidth | Fetch throughput targets sequential disk BW when payload-bound |
| TLS | Not in Phase 5 (Phase 7) — do not expect zero-copy sendfile through TLS |

```bash
# Linux examples (ops)
sysctl net.core.somaxconn
sysctl net.ipv4.tcp_tw_reuse
```

---

## Kernel bypass (DPDK / AF_XDP)

**Not supported.** Volant uses Tokio TCP. Network kernel bypass (DPDK, AF_XDP)
is research-only and not on the production roadmap. Prefer storage-path
features (`mmap` / `O_DIRECT` / `io_uring`) and optional CPU pinning first.

---

## Benchmarks

### Storage append micro-bench (always available)

```bash
cargo run -p volant-bench --release -- append
```

Reports messages/s for single-partition append (~100-byte values by default).
A bare `volant-bench` invocation (no subcommand) is not a valid mode.

### Multi-mode harness

```bash
# Append throughput (default std / mmap path)
cargo run -p volant-bench --release -- append --count 100000 --value-size 100

# Sequential fetch throughput (pre-fill not timed)
cargo run -p volant-bench --release -- fetch --count 100000 --value-size 100

# In-process broker batch produce
cargo run -p volant-bench --release -- produce-batch --count 100000 --batch-size 100
```

Optional CLI flags for `direct-io` / `io-uring` on the bench binary are not wired
yet; configure `StorageConfig::{direct_io, io_backend, buffer_pool_*}` from your
own harness or enable features and point server config at those paths.

Always use **`--release`** for published numbers. Pin CPUs and quiet the machine
when comparing feature flags.

### Interpreting results

Published v0.2 numbers (one host, not an SLA). Method: Apple M3 Pro / arm64,
macOS 26.3.1, APFS internal SSD, rustc 1.97.1, `volant-bench --release`,
`flush_every_n=0`, 100-byte values, `--count 100000`, default std/mmap path.
`--direct-io` / `--io-uring` are **not wired** on this binary; `io_uring` is
Linux-only and was not run.

| Metric | Measured 2026-08-14 | Notes |
|--------|---------------------|-------|
| Append msgs/s (100 B) | 669538 (repeat 663225) | `append --count 100000 --value-size 100` |
| Fetch msgs/s (100 B) | 663080 | sequential `PartitionLog::read`; not a disk-BW claim |
| Produce-batch msgs/s | 667061 | in-process broker, `--batch-size 100` |
| Append + `--flush-every 1` | 217 msgs/s | `--count 2000` only — fsync-per-message; not a published target |
| Produce p99 | **not measured** | `volant-bench` has no latency histogram; do not cite < 5 ms |
| Idle RSS / binary size | **not measured** | old < 50 MB / < 15 MB rows are unmet aspirational text |

The old ≥ 1M produce / ≥ disk-sequential-fetch / p99 / RSS / binary-size
targets stay **aspirational and unmet**. Re-run the commands above on the
target host; publish CPU, OS, disk, feature flags, `flush_every_n`, and
message size with any new figure.

---

## Recommended production starting profile

```text
Host:          Linux, local NVMe
Build:         cargo build --release -p volant-server
Features:      default storage (mmap); add thread-per-core if dedicated cores
Env:           VOLANT_CPU_LIST set only when cores are isolated
ulimit -n:     65536+
StorageConfig: segment_size=256MiB, flush_every_n=0, retention_bytes set
Clients:       batch produces; use consumer groups for scale-out readers
Observability: --metrics-addr for Prometheus; OS tools (iostat, perf, sar)
```

Enable `direct-io` / `io-uring` only after:

1. Default path meets functional tests
2. Linux CI covers the feature matrix
3. You have before/after bench numbers on the target hardware

---

## Troubleshooting

| Symptom | Likely cause | What to try |
|---------|--------------|-------------|
| `Too many open files` | low `ulimit -n` | raise nofile; reduce partition count |
| Latency spikes every few seconds | dirty page writeback storm | lower `vm.dirty_*` ratios; batch flush |
| `EINVAL` on write with direct-io | misaligned buffer/size | fix pool alignment (4 KiB) |
| io_uring init fails in container | seccomp / kernel | fall back to std I/O; check kernel |
| Affinity warnings on macOS | expected best-effort | ignore for dev; pin on Linux prod |
| Throughput far below baseline | debug build / fsync every msg | `--release`, `flush_every_n=0` |

---

## See also

- [PHASE5_SPEC.md](./PHASE5_SPEC.md) — binding design for I/O features
- [PHASE1_SPEC.md](./PHASE1_SPEC.md) — segment format & durable log API
- [ROADMAP.md](../ROADMAP.md) — phase exit criteria
- [README.md](../README.md) — build, run, feature flags overview
