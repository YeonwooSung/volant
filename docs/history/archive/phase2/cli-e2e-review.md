# Phase 2 — CLI + E2E Review

**Iterations:** 1 (compile fix for checksum shadowing; tests green)

**Test command:** `cargo test --workspace` — **all passed** (2026-07-11)

Including:
- protocol payload roundtrips (4) + frame codec (1)
- broker murmur2 / key stickiness (2)
- client e2e TCP (3): create→produce→fetch, key stickiness, null-key RR
- existing Phase 1 storage/broker durable tests

---

## Checklist

- [x] **CLI topic create/list/delete**
  - `volant topic create NAME --partitions N --broker HOST:PORT`
  - `volant topic list --broker ...` (via Metadata)
  - `volant topic delete NAME --broker ...`
- [x] **CLI produce/consume**
  - `volant produce TOPIC --value ... [--key ...] [--partition N] --broker ...`
  - `volant consume TOPIC --partition N [--from OFFSET] [--max N] --broker ...`
- [x] **e2e test create→produce→fetch**
  - `crates/volant-client/tests/e2e_tcp.rs::create_produce_fetch`
  - Boots `TcpListener::bind("127.0.0.1:0")` + `serve_listener`
- [x] **multi-partition key stickiness**
  - `e2e_tcp.rs::multi_partition_key_stickiness` (same key → same partition ×5)
  - Also `produce_without_key_round_robin` covers null-key RR across 3 partitions
- [x] **ROADMAP Phase 2 checkboxes**
  - Core milestones marked done; auth / idempotent PID / p99 bench left open
  - Phase 3 marked **(next)**
- [x] **README quick start with server+client**
  - Server listen + CLI create/produce/consume/list examples
  - Networked `Client` library snippet

---

## Findings & fixes

| # | Finding | Resolution |
|---|---------|------------|
| 1 | Protocol/client/server net were placeholders in this worktree | Implemented full Phase 2 stack needed by CLI/e2e (`payload`, broker net, `Client`) |
| 2 | `decode_frame` shadowed `checksum` fn with local binding | Renamed wire field to `checksum_wire` |
| 3 | Opcode map still had OffsetCommit=5 | Reassigned DeleteTopic=5, OffsetCommit=6, OffsetFetch=7 per PHASE2_SPEC |
| 4 | Stretch: connection backpressure, auth, PID idempotence | Documented as deferred; not blocking CLI/e2e |

---

## Files owned / changed (this agent)

| Path | Role |
|------|------|
| `docs/phase2/cli-e2e-plan.md` | Plan |
| `docs/phase2/cli-e2e-review.md` | This review |
| `crates/volant-cli/src/main.rs` | CLI commands |
| `crates/volant-cli/Cargo.toml` | `bytes` dep |
| `crates/volant-client/src/{client,lib,producer,consumer}.rs` | Networked SDK |
| `crates/volant-client/Cargo.toml` | dev-deps for e2e |
| `crates/volant-client/tests/e2e_tcp.rs` | E2E TCP tests |
| `crates/volant-server/src/main.rs` | `run_server` wiring |
| `crates/volant-broker/src/{broker,net,lib}.rs` | Admin APIs + TCP serve |
| `crates/volant-protocol/src/{request,response,payload,codec,lib}.rs` | Wire payloads |
| `ROADMAP.md` | Phase 2 done / Phase 3 next |
| `README.md` | Quick start + status |

---

## Blockers

None for CLI + e2e core path. Deferred (honest):

- Multi-partition p99 latency bench
- Auth token hook
- Idempotent producer PID + sequence
- Connection-level backpressure beyond sequential Mutex I/O

---

## Iteration log

1. **Plan** → implement protocol/broker net/client/CLI/e2e/docs → checksum compile fix → `cargo test --workspace` green → review.
