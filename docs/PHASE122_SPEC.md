# Phase 122 — Transparent AddOffsetsToTxn / TxnOffsetCommit forward (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** generalize `KafkaTxnForward` for AddOffsets (25) + TxnOffsetCommit (28) — **landed**  
- **PR2** Kafka client path forward on non-coordinator (peek + resolve + proxy) — **landed**  
- **PR3** multi-node tests + metrics honesty — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Cluster correctness — when a client (or LB) sends **AddOffsetsToTxn**
or **TxnOffsetCommit** to a broker that is not the txn coordinator, the broker
transparent-forwards to the Init-owner coordinator instead of failing permanently
or buffering deferred offsets on the wrong node.

## Goals

1. **Transparent AddOffsetsToTxn:** Init/Begin (or sticky FC + Init) on coordinator;
   AddOffsetsToTxn on another broker succeeds when the Init-owner registry is known.
2. **Transparent TxnOffsetCommit:** Same — offsets buffer only on the coordinator
   and apply on EndTxn commit (single SoT; no dual-commit).
3. **Reuse Phase 120 transport:** opcodes 84/85 `KafkaTxnForward` already carry
   arbitrary Kafka API key + version + principal + body; extend the coordinator
   handler beyond EndTxn (26).
4. **EndTxn still works** via the same forward path (Phase 120 regression green).
5. **Single-node unchanged** — no cluster ⇒ no forward.
6. Integration tests multi-node; living-docs honesty (not full `__transaction_state`
   / KIP-890).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Full Kafka `__transaction_state` / KIP-890/939 | Separate storage design |
| Full InitProducerId re-home when client hits random broker without prior registration | Prefer pin Init / sticky FindCoordinator (Phase 121) |
| Native separate AddOffsets / TxnOffsetCommit RPCs | Native embeds offsets in EndTxn; Kafka-only |
| Dynamic membership / Raft | Orthogonal |
| Multi-lang clients / chaos-mesh / long fuzz | Orthogonal |
| Rewrite of Phase 114–121 history | Forbidden |

## Problem (today — post Phase 121)

```text
  Init/Begin/AddPartitions ──► coordinator A (producer SoT + open + deferred buffer)
  open/Init fan-out         ──► peers learn owner + may install empty open
  AddOffsets / TxnOffsetCommit ──► broker B
  ──► if B has open fan-out: buffers offsets locally (wrong SoT / dual-commit risk)
  ──► if B has no open: UnknownProducerId / InvalidTxnState (permanent until pin)
  EndTxn already forwards (Phase 120) but offset buffer may be on the wrong node
```

Phase 120 closed EndTxn misroute; Phase 121 closed sticky discovery. Remaining
gap: deferred offset APIs still required pin to coordinator.

## Design principles

1. **Coordinator = Init owner** (Phase 120 registry) remains SoT for open txn +
   deferred offsets + EndTxn prepare/complete.
2. **Transparent forward** — proxy Kafka body over 84/85; client corr/header stay
   on the receiving broker (Phase 119/120 pattern).
3. **No re-forward** — inter-broker handler always runs local encode only.
4. **No dual-commit** — when coordinator known and ≠ self, **always** forward;
   never fall through to local `ensure_txn_open` / `buffer_txn_offsets` (peers may
   have empty open from fan-out).
5. **Registry miss** — handle locally (honest local error if producer absent;
   do not invent coordinator ownership).
6. **Single-node unchanged** — `cluster_config().is_none()` ⇒ no forward.
7. **Honest gaps** — not full Kafka txn coordinator; forward needs Init/open
   registration to have reached the receiving broker's registry.

---

## Architecture

### Chosen design: **extend KafkaTxnForward (84/85)**

| Piece | Role |
|-------|------|
| `txn_coordinator_*` maps | Unchanged (Phase 120 Init owner) |
| Peek helpers | Extract `transactional_id` + `producer_id` from API bodies |
| `maybe_forward_kafka_txn` | Resolve owner → forward → return body (or local None) |
| Kafka AddOffsets / TxnOffsetCommit / EndTxn client path | Call maybe-forward before local encode |
| Coordinator `KafkaTxnForward` handler | Dispatch api_key 25 / 26 / 28 |

### Wire (unchanged)

Opcodes **84/85** payload already carries:

```text
KafkaTxnForward request:
  api_key i16              // 25 | 26 | 28 (Phase 122 expands beyond EndTxn)
  api_version i16
  principal_len u16 | principal
  body_len u32 | body      // Kafka request body after Kafka request header

KafkaTxnForward response:
  error_code u16           // 0 = ok
  body_len u32 | body      // Kafka response body after Kafka response header
```

No protocol version bump.

### Algorithms

#### Peek IDs

```text
AddOffsetsToTxn (25):  transactional_id, producer_id  (before epoch/group)
TxnOffsetCommit (28):  transactional_id, group_id, producer_id
EndTxn (26):           transactional_id, producer_id  (existing peek_end_txn_ids)
```

Flexible framing uses the same compact string rules as the encode path
(version ≥ 3 for these APIs).

#### Kafka client path (AddOffsets / TxnOffsetCommit / EndTxn)

```text
1. If no cluster → local encode
2. Peek txn_id + producer_id
3. resolve_txn_coordinator(txn_id, pid)
4. If Some(coord) && coord != self && broker_addr(coord) known:
     KafkaTxnForward(api_key) → coord
     success → write body to client + volant_txn_forward_total
     failure → honest error body + volant_txn_forward_errors_total
5. Else → local encode (coordinator / single-node / registry miss)
```

#### Coordinator side

```text
KafkaTxnForward:
  25 → encode_add_offsets_to_txn (local; never re-forwards)
  26 → encode_end_txn + Phase 114 2PC fan-out (unchanged)
  28 → encode_txn_offset_commit (local; never re-forwards)
  other → error_code InvalidArg
```

#### Forward failure bodies

| API | Body on peer/RPC failure |
|-----|--------------------------|
| EndTxn (26) | throttle + UnknownProducerId (59) [existing] |
| AddOffsetsToTxn (25) | throttle + UnknownProducerId (59) |
| TxnOffsetCommit (28) | throttle + empty topics array (no silent local buffer) |

### Metrics

Same counters as Phase 120; HELP text covers EndTxn **and** offset APIs:

| Metric | Meaning |
|--------|---------|
| `volant_txn_forward_total` | Successful transparent Kafka txn forwards (25/26/28) |
| `volant_txn_forward_errors_total` | Forward RPC / peer failures |

### APIs covered

| API | Phase |
|-----|-------|
| EndTxn (26) | 120 |
| **AddOffsetsToTxn (25)** | **122** |
| **TxnOffsetCommit (28)** | **122** |
| Init / Begin / AddPartitions | still best on coordinator; open/Init fan-out registers owner |

## Contract preserved

- Single SoT: only coordinator mutates deferred offsets + EndTxn prepare/complete
- Enable2Pc prepare/complete + fence fan-out (Phase 114/120)
- Sticky FindCoordinator (Phase 121)
- Single-node Phase 18/31/47 offset paths unchanged
- Kafka public API keys/versions unchanged

## Tests

`crates/volant-broker/tests/phase122_txn_offset_forward.rs`:

1. Multi-node Enable2Pc-ish classic path: Init (+ optional AddPartitions) on
   coordinator (or sticky owner); **AddOffsetsToTxn via another broker** → 0;
   **TxnOffsetCommit via another broker** → 0; EndTxn commit (coord or other)
   applies deferred group offsets
2. EndTxn still forwards (smoke with offset path)
3. Forward metrics advance on non-coordinator for AddOffsets and/or TxnOffsetCommit
4. Single-node: AddOffsets + TxnOffsetCommit still work; no forward required
5. Registry path: after Init registration fan-out lands, non-coord resolves owner

Regression band: `phase120_*`, `phase121_*`, `phase114_*`.

## Exit criteria

1. Multi-node AddOffsets + TxnOffsetCommit via non-coordinator succeed  
2. Offsets apply only from coordinator buffer on EndTxn commit (no dual-commit)  
3. EndTxn forward still green  
4. `cargo test -p volant-broker --test phase122_txn_offset_forward` green  
5. Workspace builds; phase120/121/114 band green  

---

## Honest limitations (after ship)

- **Not** full KIP-890/939 / `__transaction_state`  
- Coordinator discovery remains Init-owner registry + sticky FindCoordinator  
- Init on random broker before any peer has the registry entry may still fail
  until Init/open fan-out lands  
- TxnOffsetCommit forward-failure body is empty topics (not per-partition 59)  
- Native protocol has no separate AddOffsets/TxnOffsetCommit RPCs  
- Forward adds one RTT; depends on inter-broker reachability  

---

## PR plan (DAG)

```text
PR1  KafkaTxnForward handler dispatch 25/28 + peek helpers
 │
 ├─► PR2  client path maybe_forward for AddOffsets + TxnOffsetCommit
 │         │
 │         └─► PR3  phase122 multi-node tests
 │                   │
 └───────────────────┴─► PR4  living docs
```

---

## Decision log

| Decision | Choice | Alternative rejected |
|----------|--------|----------------------|
| Transport | Reuse 84/85 with api_key | New opcodes per API |
| SoT | Forward when coordinator known ≠ self | Local buffer on peer open (dual-commit) |
| Registry miss | Local path (honest error) | Blind probe-all |
| TxnOffsetCommit fail body | Empty topics + metric | Re-parse full structure for 59 |
| Native | Kafka-only (native offsets in EndTxn) | Invent native AddOffsets RPC |

---

## Document map

| Doc | Role after ship |
|-----|-----------------|
| This file | Binding ship record for Phase 122 |
| [ops.md](./ops.md) | Forward metrics cover 25/26/28; pin guidance relaxed |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | AddOffsets / TxnOffsetCommit forward honesty |
| [features.md](./features.md) / [INDEX.md](./INDEX.md) | Short honesty line |
| [consistency.md](./consistency.md) | Offset API forward note |
| [../ROADMAP.md](../ROADMAP.md) | Phase 122 entry + deferred list |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index |

---

## Getting started

```bash
cargo test -p volant-broker --test phase122_txn_offset_forward
cargo test -p volant-broker --test phase120_endtxn_forward
cargo test -p volant-broker --test phase121_sticky_find_coordinator
cargo test -p volant-broker --test phase114_multi_broker_2pc
```
