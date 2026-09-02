# v0.15 — Long fuzz campaigns + chaos-mesh

**Status:** Shipped (bounded; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the Phase 112 leftover (corpus smoke only) and the v0.5
leftover (asymmetric / partial mesh is in-process, not chaos-mesh) without
a multi-hour CI job or a broker rewrite.

## Goals

1. **New fuzz target** `fuzz/fuzz_targets/decode_extended.rs` — feed
   `decode_request` / `decode_response` with focused opcodes:
   - txn: **32** InitProducerId, **50** BeginTxn, **52** EndTxn
   - v0.10 membership: **100–107** (req + resp)
   Plus the same raw LE-opcode path as `decode_request`.
2. **Seed corpus** under `fuzz/corpus/decode_extended/` (empty, truncated,
   oversize, valid-ish).
3. **CI replay** — `corpus_smoke_decode_paths` also loads the new corpus;
   `corpus_smoke_extended` mirrors the new target. Stable toolchain only.
4. **`scripts/fuzz_corpus_smoke.sh long`** — capped
   `cargo +nightly fuzz run -max_total_time=$FUZZ_LONG_SECS` (default **30s**
   per target) when nightly + cargo-fuzz exist. **CI does not run this**
   on push/PR.
5. **Chaos Mesh operator artifacts** in `deploy/chaos/` matching Helm
   labels. Not applied by GitHub Actions.
6. **In-process asymmetric isolate** — `v15_asymmetric_isolate.rs`:
   3-node, A→B dest-block only.

## Non-goals

| Deferred | Why |
|----------|-----|
| Multi-hour CI fuzz / corpus minimization bots | Default CI time stays the same |
| Required nightly job | Optional `fuzz-nightly` is `workflow_dispatch` only |
| Kafka wire-protocol fuzz | Native protocol only (same as Phase 112) |
| Replacing v0.5 tests | Symmetric isolate stays |
| New Kafka API keys / openraft / membership txn log | Sibling slices |
| Chaos Mesh in GitHub Actions | No cluster; operator-applied only |

## Fuzz

| Asset | Notes |
|-------|-------|
| `decode_frame` / `decode_request` | Unchanged (Phase 112) |
| `decode_extended` | Membership + txn decode; not a workspace member |
| `corpus_smoke` / `corpus_smoke_extended` | Deterministic seed replay on stable |
| `./scripts/fuzz_corpus_smoke.sh test` | CI path |
| `./scripts/fuzz_corpus_smoke.sh long` | Local / optional `workflow_dispatch` |

`.github/workflows/ci.yml` keeps the stable `test` job. A second job
`fuzz-nightly` runs **only** on `workflow_dispatch` (`if:
github.event_name == 'workflow_dispatch'`).

## Chaos

### Operator (Chaos Mesh)

`deploy/chaos/`:

| File | Experiment |
|------|------------|
| `pod-kill-leader.yaml` | `PodChaos` kill `volant-0` (lowest-id controller) |
| `network-partition.yaml` | `NetworkChaos` partition `volant-0` → `volant-1` (`direction: to`) |
| `README.md` | How to apply against the Helm release |

Labels match `deploy/helm/volant` (`app.kubernetes.io/name` +
`instance`). Default release `volant` in `default`.

### In-process (CI)

`Broker::test_block_inter_broker_peer(id, true)` dest-blocks outbound
`inter_broker_rpc` to that peer. Reverse direction and other peers stay
open. Listeners are **not** aborted (unlike v0.5).

Phase 134 heartbeat mesh: each broker heartbeats **all** peers and
marks a peer live on **successful outbound**. After A→B dest-block:

| Viewer | Live set | Controller |
|--------|----------|------------|
| A (1) | 1, 2, 3 (B→A still works) | 1 |
| B (2) | 1, 2, 3 (B→A outbound keeps A live) | 1 |
| C (3) | 1, 2, 3 | 1 |

This is **not** v0.5 death: listeners stay up and B can still reach A,
so nobody expires. `acks=1` to a leader that still reaches a majority
of ISR (typically leader 1 + C) still appends. A still cannot push
ClusterState / ReplicaFetch **to** B.

## Tests

```bash
cargo test -p volant-protocol corpus_smoke -- --nocapture
cargo test -p volant-broker --test v15_asymmetric_isolate -- --test-threads=1
cargo test -p volant-broker --test v05_ops_confidence -- --test-threads=1
```

## Honesty leftovers

- Seed replay + optional 30s local campaigns are **not** a security audit.
- Kafka shim codecs are still not fuzzed.
- Chaos Mesh is operator-applied; GitHub Actions has no k8s cluster.
- Asymmetric isolate is dest-block of `inter_broker_rpc` (and paths that
  go through it). `inter_broker_rpc_owned` fan-out used without a
  `Broker` is not dest-hooked.
- One-way A→B does **not** expire A on B (Phase 134 outbound `note_peer_live`).
  Split-brain expire needs both directions down (v0.5).
- Disk-full ENOSPC / slow-disk Chaos Mesh experiments are not in this
  slice (`pod-kill` + `network-partition` only).
