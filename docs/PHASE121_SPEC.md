# Phase 121 — Sticky FindCoordinator assignment (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** sticky coordinator resolve (murmur2 + static ring + registry override) — **landed**  
- **PR2** Kafka FindCoordinator path uses sticky resolve (v0–6) — **landed**  
- **PR3** multi-node stability / spread / dead-node / Phase 120 interaction tests — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Cluster correctness — FindCoordinator no longer always returns the
first metadata broker; group and transactional keys map **stably** to a live
broker via consistent hash over static membership, with Init-owner override for
known transactions (Phase 120 registry).

## Goals

1. **Sticky assignment:** Same `group_id` / `transactional_id` → same
   coordinator node across FindCoordinator calls (and across brokers answering
   the request) while membership is stable.
2. **Spread:** Different keys **can** map to different live brokers (not forced
   single coordinator for the whole cluster).
3. **Phase 120 alignment:** When the txn coordinator registry already knows a
   `transactional_id` (Init owner), FindCoordinator **returns that owner**
   (overrides hash). Before Init, hash steers clients so Init lands on a stable
   sticky coordinator.
4. **Dead-node honesty:** Preferred sticky target dead → **next live** on the
   static ring (walk). Documented; not Raft reassignment.
5. Integration tests multi-node; living-docs honesty (not `__transaction_state`
   / full KIP-890).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Full Kafka `__transaction_state` / KIP-890/939 | Separate storage design |
| Raft / dynamic membership rebalance of coordinators | Orthogonal |
| Group state migration when sticky target dies | Groups re-form on failover node |
| Transparent forward for AddOffsets / TxnOffsetCommit | Still pin / Phase 120 deferral |
| Native FindCoordinator API | Kafka wire only (native has no FC API) |
| Multi-lang clients / chaos-mesh / long fuzz | Orthogonal |
| Rewrite of Phase 114–120 history | Forbidden |

## Problem (today — post Phase 120)

```text
  FindCoordinator(key) ──► always first metadata broker (node 1 / bootstrap)
  Clients that "follow" FindCoordinator still pin everything to one node
  Hash sticky assignment deferred; Init-owner registry only helps after Init
  LB / multi-bootstrap discovery still weak for pre-Init placement
```

Phase 120 closed EndTxn misroute via forward; FindCoordinator wire honesty gap
remained: discovery still returns first broker.

## Design principles

1. **Static membership ring** — sorted configured `broker_ids` from
   `cluster.toml` / `ClusterConfig` (no Raft join redesign).
2. **Kafka murmur2** — same hash family as partition key routing
   (`volant_broker::murmur2`) for predictability.
3. **Registry overrides hash for known txns** — Init owner is SoT once
   registered (Phase 120); FindCoordinator must not redirect clients away from
   the fence/prepare owner.
4. **Sticky hash before Init** — clients that call FindCoordinator then Init on
   the returned node get natural ownership = sticky target.
5. **Next-live failover** — preferred id dead in membership → walk static ring
   for first live. Prefer not to return CoordinatorNotAvailable for MVP (clients
   get a usable live endpoint).
6. **Single-node unchanged** — no cluster ⇒ self advertised (first = only).
7. **Honest gaps** — group coordinator state is still **local** to the broker;
   death failover does not migrate group membership; re-find after death may
   land on a different node until preferred returns live.

---

## Architecture

### Chosen design: **hash sticky + Init-owner override**

| Piece | Role |
|-------|------|
| Sorted configured broker ids | Static membership ring |
| `murmur2(key) % N` | Preferred coordinator index on ring |
| Live membership walk | Skip dead preferred → next live |
| `txn_coordinator_by_id` | Phase 120 override for known transactional_id |
| Kafka FindCoordinator v0–6 | Per-key resolve (batch v4–6 independent) |

### Algorithm

```text
resolve_find_coordinator(key, key_type):
  if no cluster:
    return self advertised (id, host, port)

  // Phase 120 registry override (transaction keys only)
  if key_type == transaction (1) && !key.is_empty():
    if let Some(owner) = resolve_txn_coordinator(key, None):
      return endpoint(owner)   // even if not "preferred" by hash
      // if owner unknown in config: fall through to sticky

  ring = sorted configured broker_ids
  live = membership.live_brokers()   // sorted
  if ring empty or live empty:
    return self advertised

  preferred_idx = (murmur2(key) & 0x7fff_ffff) % ring.len()
  for i in 0..ring.len():
    id = ring[(preferred_idx + i) % ring.len()]
    if id in live:
      return endpoint(id)

  return self advertised
```

### Endpoint resolution

Same as Metadata: for `self` use advertised host/port; for peers use
`ClusterConfig` host/port. Node id is the coordinator id clients pin to.

### key_type

| Type | Value | Sticky? | Registry override? |
|------|------:|---------|--------------------|
| group | 0 | yes | no |
| transaction | 1 | yes | yes (Init owner) |
| share (KIP-932) | 2 | rejected (unchanged InvalidRequest) | — |

### Empty key

Empty key still hashes (murmur2 of empty bytes) to a stable preferred node —
same as any other string. No special "first broker" shortcut in cluster mode.

### Interaction with Phase 120 EndTxn forward

```text
  FindCoordinator(txn) → sticky S (or Init owner O if registered)
  Client Init on S → register coordinator=S → peers learn via fan-out
  EndTxn on wrong broker B → still forward to O/S (Phase 120)
```

If client **ignores** FindCoordinator and Inits on random A:

```text
  registry owner = A
  FindCoordinator(txn) → A (override)   // not sticky hash of key
  EndTxn on B → forward to A
```

Sticky hash improves the common "discover then Init" path; registry keeps
correctness when Init already happened elsewhere.

### Dead-node failover honesty

| Situation | Behavior |
|-----------|----------|
| Preferred live | Return preferred |
| Preferred dead, peer live | Walk ring → first live after preferred |
| All but self dead | Return self if live |
| Registered Init owner | Returned even if hash preferred differs; if owner endpoint missing from config, fall through to sticky |
| Preferred returns live later | Hash keys return preferred again (may move off failover node) |

**Group death:** no group state migration — clients re-Join on the failover
coordinator (generation reset). **Txn death:** Init owner process loss still
loses in-memory producer SoT (pre-existing; not full `__transaction_state`).

---

## Contract preserved

- FindCoordinator wire versions **0–6** unchanged; never emits 123
- Single-node / no-cluster responses unchanged (self)
- Phase 120 EndTxn forward + registry still green
- Share key_type still rejected
- No new public Kafka API keys

## Tests

`crates/volant-broker/tests/phase121_sticky_find_coordinator.rs`:

1. Multi-node: same key → same node_id from every broker; repeated calls stable
2. Different keys can spread across ≥2 nodes (with enough samples)
3. Dead preferred: mark_dead → key maps to next live; revive → preferred again
4. Registry override: Init on non-sticky node → FindCoordinator returns Init owner
5. Phase 120 smoke: Init on sticky; EndTxn via other broker still succeeds
6. Single-node: still returns self

Unit: `sticky_coordinator_id` / resolve helper pure tests.

Regression band: `phase120_*`, `phase114_*`, `phase81_*`, `phase52_*`.

## Exit criteria

1. Cluster FindCoordinator is sticky (not always first broker)  
2. Known txn id returns Init owner when registered  
3. Dead-node next-live documented + tested  
4. `cargo test -p volant-broker --test phase121_sticky_find_coordinator` green  
5. Workspace builds; phase120/114 band green  

---

## Honest limitations (after ship)

- **Not** full KIP-890/939 / `__transaction_state`  
- Static membership only; death failover may re-home keys until preferred returns  
- Group coordinator state is local (no migrate on failover)  
- Init on a non-sticky broker still allowed; registry then overrides FindCoordinator  
- AddOffsets / TxnOffsetCommit still prefer client pin to coordinator  
- Native protocol has no FindCoordinator  

---

## PR plan (DAG)

```text
PR1  resolve helper + unit tests
 │
 ├─► PR2  encode_find_coordinator uses resolve
 │         │
 │         └─► PR3  phase121 multi-node tests
 │                   │
 └───────────────────┴─► PR4  living docs
```

---

## Decision log

| Decision | Choice | Alternative rejected |
|----------|--------|----------------------|
| Hash | Kafka murmur2 | FNV-only / md5 |
| Ring | Sorted **configured** ids + next-live | Live-only ring (churn on death) |
| Known txn | Registry **overrides** hash | Hash always (breaks Init-elsewhere) |
| Dead preferred | Next live on ring | COORDINATOR_NOT_AVAILABLE only |
| Init path | Client-driven via sticky FC | Force-reject Init on non-sticky |

---

## Document map

| Doc | Role after ship |
|-----|-----------------|
| This file | Binding ship record for Phase 121 |
| [ops.md](./ops.md) | FindCoordinator sticky / pin guidance |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | FindCoordinator sticky honesty |
| [features.md](./features.md) / [INDEX.md](./INDEX.md) | Short honesty line |
| [consistency.md](./consistency.md) | Coordinator discovery note |
| [../ROADMAP.md](../ROADMAP.md) | Phase 121 entry + deferred list |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index |

---

## Getting started

```bash
cargo test -p volant-broker --test phase121_sticky_find_coordinator
cargo test -p volant-broker --test phase120_endtxn_forward
cargo test -p volant-broker --test phase81_find_coordinator_v5_v6
cargo test -p volant-broker --test phase52_flexible_metadata_find_coordinator
cargo test -p volant-broker --test phase114_multi_broker_2pc
```
