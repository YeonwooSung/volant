# Phase 120 — Transparent EndTxn / txn RPC forward (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** protocol `KafkaTxnForward` 84/85 + `TxnParticipantOpen` coordinator fields — **landed**  
- **PR2** txn coordinator registry + Init/open registration + EndTxn forward path — **landed**  
- **PR3** multi-node tests + metrics — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Cluster correctness — when a client (or LB) sends EndTxn to a broker
that is not the txn coordinator, the broker transparent-forwards to the
coordinator instead of permanent `UnknownProducerId` / broken 2PC state.

## Goals

1. **Transparent EndTxn:** Init/Begin/AddPartitions on broker A; EndTxn on
   broker B succeeds for Enable2Pc (prepare + complete) and classic one-shot,
   via inter-broker forward to the coordinator.
2. **Single EndTxn SoT:** Non-coordinator brokers **forward** EndTxn; they do
   not dual-prepare. Coordinator remains the only node that runs local
   `end_txn` + Phase 114 prepare/complete fan-out for a given client EndTxn.
3. **Coordinator known without Raft:** Registry filled on Init (local) and
   carried on `TxnParticipantOpen` so peers learn the owner after open fan-out
   (and after Init registration fan-out without installing open).
4. **Reuse inter-broker transport** (Phase 113–119). Internal opcode only.
5. **Fence still correct:** Second EndTxn / epoch fence paths that go through
   the coordinator still drive cluster complete-abort semantics.
6. Integration tests multi-node; living-docs honesty (not full `__transaction_state`
   / KIP-890).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Full Kafka `__transaction_state` / KIP-890/939 | Separate storage design |
| Hash-based FindCoordinator sticky assignment | MVP uses Init-owner registry |
| Transparent forward for **all** txn APIs (AddOffsets, TxnOffsetCommit, …) | Document as deferred; EndTxn vertical first |
| Full InitProducerId re-home when client hits random broker without prior open | Prefer pin Init; optional forward when coordinator known |
| Dynamic membership / Raft | Orthogonal |
| Multi-lang clients / chaos-mesh / long fuzz | Orthogonal |
| Rewrite of Phase 114–119 history | Forbidden |

## Problem (today — post Phase 114)

```text
  Init/Begin/AddPartitions ──► broker A (producer SoT + open)
  open fan-out              ──► peers (participant open ranges)
  EndTxn                    ──► broker B that never saw the pid
  ──► UnknownProducerId / InvalidTxnState  (permanent until client re-homes)
```

Phase 114 documented: pin Init/Begin/EndTxn to the coordinator. LB misrouting
or produce-redirected clients that EndTxn on a random leader still break.

## Design principles

1. **Coordinator = Init owner** for a `transactional_id` (the broker that
   allocated / fenced the producer). Peers learn via open registration.
2. **Transparent forward (Phase 119 pattern)** — proxy Kafka EndTxn **body**
   over native inter-broker RPC; client corr/header stay on the receiving broker.
3. **No re-forward** — the inter-broker handler always runs local EndTxn + 2PC
   fan-out (no second hop).
4. **Single-node unchanged** — no cluster ⇒ no registry / no forward.
5. **Honest gaps** — not full Kafka txn coordinator; FindCoordinator still
   returns first metadata broker (unchanged wire); remaining txn APIs may still
   need client pin.

---

## Architecture

### Chosen design: **coordinator registry + KafkaTxnForward**

| Piece | Role |
|-------|------|
| `txn_coordinator` maps | `transactional_id` / `producer_id` → owner `node_id` |
| Init (local) | Register self as coordinator for the txn id / pid |
| `TxnParticipantOpen` | Carries `coordinator_node_id` + `install_open`; peers register owner |
| Init registration fan-out | Open fan-out with `install_open=false` so peers learn owner without opening |
| `KafkaTxnForward` 84/85 | Carry Kafka API key + version + principal + body |
| Kafka EndTxn on non-coord | Resolve owner → forward → return body |
| Coordinator | Local `encode_end_txn` + Phase 114 fan-out |

### Wire (native inter-broker)

| Opcode | Name | Direction |
|-------:|------|-----------|
| 84 | `KafkaTxnForward` | Non-coordinator → coordinator |
| 85 | response | Coordinator → non-coordinator |

Payload (LE):

```text
KafkaTxnForward request:
  api_key i16              // Kafka API key (26 = EndTxn for MVP)
  api_version i16
  principal_len u16 | principal (UTF-8)
  body_len u32 | body      // Kafka request body (after Kafka request header)

KafkaTxnForward response:
  error_code u16           // 0 = ok; non-zero = forward failed
  body_len u32 | body      // Kafka response body (after Kafka response header)
```

### TxnParticipantOpen extension (Phase 114 opcode 76)

```text
… existing fields …
coordinator_node_id u32    // 0 = unknown (legacy decode default)
install_open u8            // 1 = install empty open (default if truncated)
```

Backward compatible: missing trailer ⇒ `coordinator_node_id=0`, `install_open=1`.

### Algorithms

#### Init (transactional)

```text
local init_producer_id_with_opts as today
register coordinator(txn_id, pid) = self
if cluster && !txn_id.is_empty():
  best-effort TxnParticipantOpen {
    txn_id, pid, epoch, enable_2pc,
    coordinator_node_id=self, install_open=false
  }
```

#### Begin / AddPartitions open fan-out

```text
TxnParticipantOpen { …, coordinator_node_id=self (or known), install_open=true }
```

#### EndTxn (Kafka client path)

```text
1. Peek transactional_id + producer_id from EndTxn body
2. resolve_txn_coordinator(txn_id, pid):
     map by txn_id → map by pid → cluster prepared index coordinator → None
3. If Some(coord) && coord != self && broker_addr(coord) known:
     KafkaTxnForward(api_key=EndTxn) → coord
     on success: write response body to client
     on failure: local error body (UnknownProducerId / Unknown) + metric error
4. Else → local encode_end_txn + Phase 114 fan-out (coordinator / single-node)
```

#### Coordinator side

```text
KafkaTxnForward(EndTxn) → local encode_end_txn + run_txn_2pc_fanout
// never re-forwards
```

#### Native EndTxn

Same resolve + forward when cluster and coordinator ≠ self (proxy native
`Request::EndTxn` is **not** used; native clients that hit a non-coord still
use local `end_txn` when they **are** the coord after open fan-out). MVP focuses
on **Kafka** EndTxn forward; native path: if coordinator known and ≠ self,
return honest error or forward via inter-broker by mapping to the same local
handler on the peer (optional thin path: call peer through KafkaTxnForward is
Kafka-only). Native multi-broker tests use Kafka shim as Phase 114.

### Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `volant_txn_forward_total` | counter | Successful transparent EndTxn (txn) forwards |
| `volant_txn_forward_errors_total` | counter | Forward RPC / peer failures |

### APIs covered vs still client-pin

| API | Phase 120 |
|-----|-----------|
| **EndTxn** (Kafka 26) | Transparent forward when coordinator known |
| InitProducerId | Registration fan-out (no open); **pin still recommended** for fence correctness when no peer has learned the owner |
| BeginTxn / AddPartitionsToTxn | Open fan-out carries coordinator; still best on coordinator |
| AddOffsetsToTxn / TxnOffsetCommit | **Deferred** — pin to coordinator |

## Contract preserved

- Enable2Pc prepare/complete + fence fan-out (Phase 114)
- Single-node Phase 90 behavior when `cluster.is_none()`
- No dual prepare: only coordinator runs client EndTxn local path when registry hits
- Kafka public API keys/versions unchanged

## Tests

`crates/volant-broker/tests/phase120_endtxn_forward.rs`:

1. Enable2Pc: Init+AddPartitions+produce on coordinator; EndTxn prepare **and**
   complete via a **different** broker → LSO/commit correct on both leaders
2. Classic one-shot (`Enable2Pc=false`): EndTxn via non-coordinator succeeds
3. Fence: prepare via forward; Init fence on coordinator still aborts cluster-wide
4. Single-node: no forward metrics / path unchanged
5. Protocol: opcodes 84/85 roundtrip; TxnParticipantOpen trailer decode

Regression band: `phase114_*`, `phase90_*`, `phase18_*` (txn).

## Exit criteria

1. Multi-node EndTxn via non-coordinator succeeds (2PC + classic)  
2. No dual prepare; fence still correct  
3. Metrics exposed; living docs drop “always pin EndTxn” as sole guidance  
4. `cargo test -p volant-broker --test phase120_endtxn_forward` green  
5. Workspace builds  

---

## Honest limitations (after ship)

- **Not** full KIP-890/939 / `__transaction_state`  
- Coordinator discovery is Init-owner registry + open fan-out, not Raft  
- FindCoordinator wire still returns first metadata broker (unchanged)  
- AddOffsets / TxnOffsetCommit / full Init re-home still prefer client pin  
- Forward adds one RTT; depends on inter-broker reachability  
- Init on broker A then EndTxn on B **before** any registration reaches B may still fail until open/Init fan-out lands  

---

## PR plan (DAG)

```text
PR1  protocol 84/85 + TxnParticipantOpen trailer
 │
 ├─► PR2  coordinator registry + Init/open + EndTxn forward + metrics
 │         │
 │         └─► PR3  phase120 multi-node tests
 │                   │
 └───────────────────┴─► PR4  living docs
```

---

## Decision log

| Decision | Choice | Alternative rejected |
|----------|--------|----------------------|
| MVP surface | EndTxn transparent forward | Full multi-API txn proxy first |
| SoT | Init-owner coordinator + Phase 114 fan-out | Dual local prepare on any peer with open |
| Miss without registry | Local path (error) | Blind probe-all (dual-prepare risk) |
| Open on Init fan-out | `install_open=false` | Always install open (breaks BeginTxn) |
| Client wire | Unchanged EndTxn | New public redirect error only |

---

## Document map

| Doc | Role after ship |
|-----|-----------------|
| This file | Binding ship record for Phase 120 |
| [ops.md](./ops.md) | Forward metrics; pin guidance relaxed for EndTxn |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | EndTxn forward honesty |
| [features.md](./features.md) / [INDEX.md](./INDEX.md) | Short honesty line |
| [consistency.md](./consistency.md) | EndTxn forward note |
| [../ROADMAP.md](../ROADMAP.md) | Phase 120 entry + deferred list |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index |

---

## Getting started

```bash
cargo test -p volant-protocol
cargo test -p volant-broker --test phase120_endtxn_forward
cargo test -p volant-broker --test phase114_multi_broker_2pc
cargo test -p volant-broker --test phase90_prepared_txns
cargo test -p volant-broker --test phase18_transactions
```
