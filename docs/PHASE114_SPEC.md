# Phase 114 — Multi-broker 2PC / KIP-890-ish MVP

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** protocol opcodes 76–81 + dispatch handlers — **landed**  
- **PR2** participant open / prepare / complete on brokers + controller-durable prepared index — **landed**  
- **PR3** EndTxn / BeginTxn / fence fan-out paths (native + Kafka) — **landed**  
- **PR4** multi-node tests + metrics — **landed**  
- **PR5** living docs honesty — **landed**  
**Theme:** Cluster correctness — Enable2Pc prepare/commit spans partition leaders on
different brokers, coordinated over inter-broker RPC with a controller-durable
prepared index (not a full Kafka `__transaction_state` log).

## Goals

1. When an **Enable2Pc** producer writes partitions whose leaders live on
   **different brokers**, first EndTxn **prepares** and second EndTxn **completes**
   consistently across those leaders (control markers + LSO isolation).
2. **Durable prepared state** survives the coordinating / controller broker restart
   via local `__txn_prepared` on each participant **plus** a controller-side
   cluster prepared index (`__txn_prepared/cluster.json`).
3. **Fencing:** InitProducerId with `KeepPreparedTxn=false` (or concurrent re-init)
   aborts prepared state **cluster-wide** (fan-out complete-abort).
4. Reuse Phase 113 inter-broker transport (`inter_broker_rpc` + shared-token /
   inter-broker TLS) — **no new transport**.
5. Single-node / non-cluster mode keeps **Phase 90** behavior (local prepared path).
6. `Enable2Pc=false` remains one-shot EndTxn (unchanged).
7. Integration tests under multi-node in-process harness + living-docs honesty
   (no false full KIP-890/939 parity).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Full Kafka `__transaction_state` topic / txn log replication | Separate storage design |
| Full KIP-890 / KIP-939 surface (TRANSACTION_ABORTABLE everywhere, resume fields) | Later |
| Cross-DC / multi-region 2PC | Out of scope |
| Dynamic membership / Raft metadata quorum | Membership redesign |
| Multi-broker fetch session affinity | Orthogonal |
| Transparent EndTxn forward when client hits a broker that never saw the pid | Client should pin txn RPCs to the coordinator broker that ran Init/Begin (document) |
| Exactly-once streams / multi-lang clients / long fuzz | Orthogonal |

## Problem (today)

```text
  Init/Begin/EndTxn ──► broker A (open_txns + prepared local only)
  Produce p0        ──► leader B  (NotLeader / InvalidTxnState — no open)
  Produce p1        ──► leader C
  Prepare           ──► only A knows written ranges
```

Phase 90 prepared state is **process-local**. Multi-leader transactions cannot
prepare/finalize consistently; LSO and control markers diverge.

## Design principles

1. **Controller remains SoT** for the **cluster prepared index** (same class as
   Phase 113 ACL/config generations). Partition leaders remain SoT for **local
   log ranges + local LSO**.
2. **Reuse inter-broker RPC** — opcodes 76–81; auth = shared-token / TLS only
   (not ACL-gated), same as ReplicaFetch / Phase 113 admin.
3. **Strict prepare fan-out** for live peers (unlike DeleteRecords best-effort):
   client prepare succeeds only if every live peer ACKs prepare (or has nothing
   to prepare). Complete is strict for live peers as well.
4. **Single-node unchanged** — `cluster.is_none()` ⇒ no fan-out; Phase 90 only.
5. **Honest gaps** — not full Kafka txn coordinator; no `__transaction_state`
   topic; no automatic client re-home of EndTxn to controller.

---

## Architecture

### Roles

| Role | Responsibility |
|------|----------------|
| **Txn client broker** | Broker that handled InitProducerId / BeginTxn / EndTxn for this producer (typically controller or first bootstrap). Holds producer state + open/prepared maps for ranges written **locally**. |
| **Partition leader** | Accepts write-through produce; holds local open/prepared ranges for that partition's LSO. |
| **Controller** | SoT for `{data_dir}/__txn_prepared/cluster.json` prepared index (identity + decision + generation). |

### Wire (native inter-broker)

| Opcode | Name | Direction |
|-------:|------|-----------|
| 76 | `TxnParticipantOpen` | Coordinator → live peers |
| 77 | response | Peer → coordinator |
| 78 | `TxnParticipantPrepare` | Coordinator → live peers |
| 79 | response | Peer → coordinator |
| 80 | `TxnParticipantComplete` | Coordinator → live peers |
| 81 | response | Peer → coordinator |

Payload (LE):

```text
TxnParticipantOpen request:
  transactional_id_len u16 | transactional_id
  producer_id u64 | producer_epoch u16 | enable_2pc u8

TxnParticipantOpen response:
  error_code u16

TxnParticipantPrepare request:
  transactional_id_len u16 | transactional_id
  producer_id u64 | producer_epoch u16 | commit u8

TxnParticipantPrepare response:
  error_code u16

TxnParticipantComplete request:
  transactional_id_len u16 | transactional_id
  producer_id u64 | producer_epoch u16 | commit u8

TxnParticipantComplete response:
  error_code u16
```

Kafka public API keys are **unchanged**. Client InitProducerId v6 / EndTxn still
map to local `Broker` methods; fan-out is internal.

### Algorithms

#### Open (BeginTxn / ensure_txn_open success)

```text
local begin/ensure as today
if cluster:
  for peer in live - {self}:
    RPC TxnParticipantOpen { txn_id, pid, epoch, enable_2pc }
    on error: metric++; log warn  # open fan-out is best-effort for dead peers
                                  # (produce to dead leader fails separately)
```

Peer open handler: install producer state (pid/epoch/txn_id/enable_2pc) and
empty `open_txns` entry if missing (idempotent for same epoch).

#### Produce

Unchanged once peer has open state: `buffer_txn_produce` on the **partition
leader** records local written ranges + holds LSO.

#### Prepare (EndTxn #1, enable_2pc)

```text
local: open → Prepared (Phase 90)
if cluster:
  for peer in live - {self}:
    RPC TxnParticipantPrepare { txn_id, pid, epoch, commit }
    if any live peer fails: metric++; abort local prepare (rollback to open or
      force-abort); return InvalidTxnState / Unknown to client
  if controller: upsert cluster prepared index + persist
  else: include controller in fan-out (controller upserts index even with no ranges)
```

Peer prepare: if open for pid+epoch → move to prepared (same decision rules as
Phase 90). If already prepared with matching decision → OK. If nothing known → OK
(empty participant). Wrong epoch → InvalidProducerEpoch.

#### Complete (EndTxn #2)

```text
local finalize (Phase 90)
if cluster:
  for peer in live - {self}:
    RPC TxnParticipantComplete
  controller clears cluster prepared index entry
```

#### Fence (Init KeepPreparedTxn=false or re-init abort)

```text
local force-abort prepared (Phase 90)
if cluster:
  for peer in live - {self}:
    RPC TxnParticipantComplete { commit=false }  # abort finalize
  controller clears cluster index
```

### Controller-durable prepared index

`{data_dir}/__txn_prepared/cluster.json` on the **controller** only:

```json
{
  "prepared": [
    {
      "transactional_id": "app-1",
      "producer_id": 1,
      "producer_epoch": 0,
      "commit": true,
      "prepared_at_ms": 0,
      "coordinator_node_id": 1
    }
  ]
}
```

Local ranges still live in each participant's Phase 90 `__txn_prepared/state.json`.
The cluster index is coordination metadata (who/what/decision), not the full
write set.

### Metrics

| Metric | Type |
|--------|------|
| `volant_txn_2pc_fanout_errors_total` | counter — prepare/complete/open fan-out RPC failures |
| `volant_cluster_prepared_txns` | gauge — controller cluster prepared index size |

---

## Tests

| Test file | Cases |
|-----------|-------|
| `phase114_multi_broker_2pc.rs` | 3-node, 2 partitions different leaders: Enable2Pc produce both → prepare holds LSO on both leaders → complete commit makes READ_COMMITTED data visible; prepare-then-fence aborts cluster-wide; single-node regression path still prepares locally |
| Protocol unit | opcodes 76–81 encode/decode roundtrips |
| Regression | `phase90_prepared_txns`, `phase113_*` |

Harness: reuse Phase 113 multi-broker helpers + Kafka wire for Init v6 Enable2Pc
(or direct `init_producer_id_with_opts` + native produce/EndTxn).

---

## Exit criteria

1. `cargo test -p volant-protocol` (new opcode tests) green  
2. `cargo test -p volant-broker --test phase114_multi_broker_2pc` green  
3. `cargo test -p volant-broker --test phase90_prepared_txns` green  
4. Living docs updated; no false full KIP-890 parity claims  
5. Single-node Phase 90 behavior unchanged when `cluster.is_none()`  
6. Workspace builds  

---

## Honest limitations (after ship)

- **Not** full KIP-890/939; no Kafka `__transaction_state` topic  
- Open fan-out is best-effort for down peers; prepare is strict for **live** peers  
- Client must send EndTxn to a broker that knows the producer (Init/Begin target);
  native client may land on a partition leader after produce redirects — tests pin
  EndTxn to the coordinator; a later phase may add transparent forward  
- Controller failover: cluster index lives on old controller disk; new controller
  starts empty until a new prepare (local participant prepared still holds LSO)  
- Inter-broker 2PC RPCs are **not** ACL-gated  
- Resume pid/epoch fields on InitProducerId still ignored for allocation  

---

## PR plan (DAG)

```text
PR1  protocol 76–81 + dispatch stubs
 │
 ├─► PR2  participant handlers + controller cluster index
 │         │
 │         └─► PR3  Begin/EndTxn/fence fan-out (native + Kafka)
 │                   │
 │                   └─► PR4  phase114 tests + metrics
 │                             │
 └─────────────────────────────┴─► PR5  living docs
```

---

## Decision log

| Decision | Choice | Alternative rejected |
|----------|--------|----------------------|
| SoT for multi-broker prepared index | Controller file | Invent Raft / txn log topic |
| Transport | Existing inter_broker_rpc | New gRPC / raft channel |
| Prepare durability | Strict for live peers | Best-effort (would break 2PC) |
| Open fan-out | Best-effort to live peers | Fail Begin if any peer down |
| Non-2PC EndTxn | Unchanged one-shot | Always multi-phase |
| Kafka public API | No new keys | Expose internal opcodes |

---

## Document map

| Doc | Role after ship |
|-----|-----------------|
| This file | Binding ship record for Phase 114 |
| [consistency.md](./consistency.md) | Multi-broker prepare/complete notes |
| [ops.md](./ops.md) | Pin txn RPCs; metrics |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | Honest multi-broker 2PC MVP claim |
| [../ROADMAP.md](../ROADMAP.md) | Phase 114 entry + deferred list |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index |

---

## Getting started

```bash
cargo test -p volant-protocol
cargo test -p volant-broker --test phase114_multi_broker_2pc
cargo test -p volant-broker --test phase90_prepared_txns
cargo test -p volant-broker --test phase113_delete_records_fanout
```
