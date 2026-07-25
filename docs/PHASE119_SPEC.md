# Phase 119 — Multi-broker fetch session handoff / affinity (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** owner-encoded session_id + `KafkaFetchForward` opcodes 82/83 — **landed**  
- **PR2** Kafka Fetch path: foreign-owner miss → transparent inter-broker forward — **landed**  
- **PR3** multi-node tests + metrics — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Fetch session affinity honesty — a session opened on broker A remains
usable when the client (or LB) hits broker B, without permanent silent broken
omit-unchanged or epoch corruption.

## Goals

1. **Cross-broker incremental Fetch:** A Fetch session created on broker A can
   be used on broker B (same `session_id` / `session_epoch`) via transparent
   inter-broker forward to the session owner.
2. **Epoch + omit-unchanged honesty:** Owner remains single SoT for session
   state (epoch, topics, Phase 91 `last_hwm`/`last_lso`); forward never dual-
   advances epoch on two nodes.
3. **Wire-compatible client API:** No new Kafka keys / fields. Errors 70/71,
   FINAL_EPOCH close, INITIAL create, omit-unchanged empty-topics unchanged
   from the client's point of view.
4. **Reuse inter-broker transport** (Phase 113–114 `inter_broker_rpc` + shared-
   token / TLS). Internal opcode only.
5. Integration tests (≥2 brokers): open on A, incremental on B succeeds with
   correct epoch; omit path preserved; wrong epoch still 71; FINAL on B closes
   on A.
6. Living-docs honesty (not full Kafka preferred-replica; not controller Raft
   session registry).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Controller / Raft shared session table | Encoding + forward sufficient for MVP |
| Session snapshot migrate (Option C full pull-serve) | Dual-owner + NotLeader risk; forward preferred |
| Preferred replica / rack-aware fetch | Orthogonal |
| Client Metadata "session owner" field | Avoid new public wire |
| Debounced durable persist (Phase 115 follow-up) | Orthogonal |
| Multi-lang clients / chaos-mesh / long fuzz | Orthogonal |
| Full KIP-890/939 / Raft membership | Orthogonal |
| Transparent EndTxn forward | Separate track |
| Rewrite of Phase 114–118 history | Forbidden |

## Problem (today — post Phase 115)

```text
  Fetch create ──► session_id=S on broker A (durable local ✅)
  Client / LB hits broker B with session_id=S
  ──► FETCH_SESSION_ID_NOT_FOUND (70)
  omit-unchanged cache still on A; client must full-fetch recreate
```

Phase 115 closed same-node restart. Multi-broker miss remained sticky-by-
convention only. LB misrouting permanently loses incremental efficiency.

## Design principles

1. **Single owner SoT** — the broker that created the session owns epoch and
   omit cache; other brokers **forward**, they do not clone-and-serve.
2. **Owner embedded in `session_id`** — cluster mode allocates
   `session_id = (node_id << 19) | local` so peers can resolve the owner without
   a controller registry (Option B lightweight).
3. **Transparent forward (Option A)** — on local miss for a foreign owner,
   proxy the Kafka Fetch **body** over native inter-broker RPC; return the
   owner's response body. Client corr/header stay on the receiving broker.
4. **No re-forward** — the inter-broker handler always runs local
   `encode_fetch` (no second hop).
5. **Single-node unchanged** — no cluster ⇒ sequential ids, no forward path.
6. **Honest gaps** — owner death / unreachable ⇒ 70 (client recreates); no
   preferred-replica; Metadata does not advertise session owners.

---

## Architecture

### Chosen design: hybrid **B (owner in id) + A (transparent forward)**

| Piece | Role |
|-------|------|
| Owner-encoded `session_id` | Discover owner without controller lookup |
| `KafkaFetchForward` opcode 82/83 | Carry Kafka Fetch request/response bodies |
| Kafka shim on non-owner | Peek session; if foreign miss → forward |
| Session owner | Local session table (Phase 88/91/95/115) + serve data |

Option C (pull snapshot then serve locally) is **deferred**: non-leader
partitions return `NotLeaderForPartition`, and clone would dual-own epoch.

### Session id layout (cluster only)

```text
  bits 31..19  owner node_id (12 bits, 1..4095)
  bits 18..0   local counter (19 bits, 1..0x7FFFF)
  session_id always > 0 (INVALID = 0)
```

| Mode | Allocation |
|------|------------|
| Single-node (`node_id` 0 / no cluster) | Sequential local `1, 2, 3…` (Phase 115 unchanged) |
| Cluster (`with_cluster`) | Owner-encoded via `FetchSessionManager::set_owner_node_id` |

`decode_session_owner(id) → Option<node_id>`: high bits non-zero ⇒ owner.

### Wire (native inter-broker)

| Opcode | Name | Direction |
|-------:|------|-----------|
| 82 | `KafkaFetchForward` | Non-owner → session owner |
| 83 | response | Owner → non-owner |

Payload (LE):

```text
KafkaFetchForward request:
  api_version i16
  principal_len u16 | principal (UTF-8)
  body_len u32 | body   // Kafka Fetch request body (after Kafka request header)

KafkaFetchForward response:
  error_code u16        // 0 = ok; non-zero = forward failed (peer maps to 70)
  body_len u32 | body   // Kafka Fetch response body (after Kafka response header)
```

Kafka public Fetch API keys/versions **unchanged**. Auth on the forward RPC =
shared-token / inter-broker TLS only (not ACL-gated), same as Phase 113/114.

### Algorithms

#### Create (INITIAL_EPOCH / session_id 0)

Unchanged local path on the broker that received the client Fetch. New
`session_id` encodes this node's id when clustered.

#### Incremental / FINAL on non-owner

```text
1. Peek session_id + session_epoch from Fetch body (v7+)
2. If session_id == 0 OR epoch == INITIAL → local encode_fetch (create)
3. If local session table contains session_id → local encode_fetch
4. Else if decode_session_owner(session_id) is Some(peer) and peer != self
     and broker_addr(peer) known:
       KafkaFetchForward → peer
       on success: write response body to client
       on failure: FETCH_SESSION_ID_NOT_FOUND (70) empty response
5. Else → local encode_fetch (→ 70 if missing)
```

FINAL_EPOCH close with foreign owner is **forwarded** so the owner drops the
session (durable remove under Phase 115).

#### Owner side

```text
KafkaFetchForward → encode_fetch(local) → return body
// never re-forwards
```

### Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `volant_fetch_session_forward_total` | counter | Successful transparent forwards |
| `volant_fetch_session_forward_errors_total` | counter | Forward RPC / peer failures |

Existing active / restored / evicted metrics unchanged (owner-local).

### Config

No new knobs. Forward uses existing cluster membership / `broker_addr`.

## Contract preserved

- Errors **70** / **71** semantics for true local miss / bad epoch on owner
- FINAL_EPOCH closes owner session (via forward when needed)
- Omit-unchanged empty-topics incremental still owner-side (Phase 91)
- Durable local restore still owner-side (Phase 115)
- Single-node: no encoding, no forward

## Tests

`crates/volant-broker/tests/phase119_fetch_session_handoff.rs`:

1. Open session on broker1; empty-topics incremental on broker2 succeeds (forward);
   session_id echoed; top error 0; epoch advances on owner only
2. Omit-unchanged still works when forwarded (HWM/LSO stable → empty topics)
3. Wrong epoch via broker2 still returns **71**
4. FINAL via broker2 closes session on broker1 (subsequent incremental → 70)
5. Unit: owner encode/decode; single-node sequential ids unchanged

Regression band: `phase115_*`, `phase88_*`, `phase91_*`, `phase95_*`.

## Exit criteria

1. Multi-node: create on A, incremental on B → success without client recreate  
2. Epoch not dual-advanced; omit-unchanged honest after forward  
3. FINAL on B removes session on A  
4. Metrics exposed; living docs no longer claim “wrong broker always 70” only  
5. `cargo test -p volant-broker --test phase119_fetch_session_handoff` green  
6. Workspace builds  

---

## Honest limitations (after ship)

- **Not** preferred-replica / rack-aware fetch  
- **Not** a controller-replicated session store (owner dies → 70)  
- **Not** Option C migrate-to-local (NotLeader + dual-epoch risk)  
- Forward adds one RTT + depends on native inter-broker reachability  
- Session id space: 12-bit owner + 19-bit local (cluster); single-node sequential  
- Large Fetch responses bounded by native `MAX_PAYLOAD` (16 MiB)  

---

## PR plan (DAG)

```text
PR1  protocol 82/83 + FetchSessionManager owner encoding
 │
 ├─► PR2  kafka handler forward + net dispatch + metrics
 │         │
 │         └─► PR3  phase119 multi-node tests
 │                   │
 └───────────────────┴─► PR4  living docs
```

---

## Decision log

| Decision | Choice | Alternative rejected |
|----------|--------|----------------------|
| MVP shape | Owner-in-id + transparent forward (A+B) | Full controller registry first; pure Option C pull-serve |
| SoT | Session-owner broker only | Dual-local clone after pull |
| Client wire | Unchanged Fetch session_id/epoch | New Metadata session-owner field |
| Miss when peer dead | **70** (honest recreate) | Retry all peers / invent redirect error |
| Single-node | Sequential ids, no forward | Always encode owner=0 |

---

## Document map

| Doc | Role after ship |
|-----|-----------------|
| This file | Binding ship record for Phase 119 |
| [ops.md](./ops.md) | Forward metrics; multi-broker session note |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | Handoff MVP honesty |
| [features.md](./features.md) / [INDEX.md](./INDEX.md) | Short honesty line |
| [consistency.md](./consistency.md) | Session handoff note |
| [../ROADMAP.md](../ROADMAP.md) | Phase 119 entry + deferred list |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index |

---

## Getting started

```bash
cargo test -p volant-broker --test phase119_fetch_session_handoff
cargo test -p volant-broker --test phase115_durable_fetch_sessions
cargo test -p volant-broker --test phase91_omit_unchanged_sessions
cargo test -p volant-broker --test phase95_fetch_session_limits
cargo test -p volant-broker --lib kafka::fetch_session
```
