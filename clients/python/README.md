# Volant Python client (native protocol MVP)

Sync TCP client for the **native** Volant wire protocol. This is **not**
`kafka-python` and does **not** speak the Kafka shim (`--kafka-listen`).

Package name: `volant` (import `volant`). Crate / client version **0.2.0**.

## Install

```bash
cd clients/python
python3 -m pip install -e ".[dev]"
# system Python without write access: skip install and use PYTHONPATH=src
```

## Usage

```python
from volant import Client

c = Client("127.0.0.1:9092")
c.create_topic("t", partitions=1)
c.create_partitions("t", 2)
c.reassign_partitions("t", [1, 2])  # all partitions; or partition=0
c.produce("t", 0, value=b"hello")
batch = c.fetch("t", 0, offset=0)
for offset, key, value in batch.tuples():
    print(offset, key, value)
c.offset_commit(group="g", topic="t", partition=0, offset=5)
offs = c.offset_fetch(group="g", topic="t")  # [(partition, offset), ...]
bounds = c.list_offsets("t")  # [OffsetListing(partition, earliest, latest), ...]
cfg = c.describe_configs("t")
c.alter_configs("t", [("retention.ms", "86400000")])
cut = c.delete_records("t", 0, 100)  # DeleteRecordsResult; wait_majority=0
# cut = c.delete_records("t", 0, 100, wait_majority=1)  # force majority wait
member_id, generation, assignment = c.join_group(
    "g", topics=["t"], session_timeout_ms=10000
)
c.heartbeat("g", member_id, generation)
c.leave_group("g", member_id)

# High-level group consumer (v0.31). Two members need two Clients.
from volant import GroupConsumer
g = GroupConsumer.join(c, group="g", topics=["t"], session_timeout_ms=10_000)
# Phase 12 static membership (empty / omitted = dynamic):
g = GroupConsumer.join(
    c, group="g", topics=["t"], session_timeout_ms=10_000, group_instance_id="inst-1"
)
recs = g.poll(max_wait_ms=500)
g.commit()
g.close()
# Opt-in auto-commit (v0.48). Default off. interval 0 = after every poll.
g = GroupConsumer.join(
    c, group="g", topics=["t"], auto_commit=True, auto_commit_interval_ms=5000
)
# Opt-in auto_offset_reset (v0.62). Default earliest (position 0).
g = GroupConsumer.join(c, group="g", topics=["t"], auto_offset_reset="latest")

meta = c.metadata()
c.close()

# Optional TLS (v0.27). Plain TCP is still the default.
c = Client("127.0.0.1:9092", tls=True, tls_ca="ca.pem")
# Lab / tests only:
c = Client("127.0.0.1:9092", tls=True, tls_insecure=True)
# Optional mTLS (client cert + key PEMs, both required):
c = Client(
    "127.0.0.1:9092",
    tls=True,
    tls_ca="ca.pem",
    tls_cert="client.pem",
    tls_key="client.key",
)
# Optional shared-token Auth (v0.42). Empty / unset skips Auth.
c = Client("127.0.0.1:9092", auth_token="s3cret")
c = Client("127.0.0.1:9092", tls=True, tls_ca="ca.pem", auth_token="s3cret")
# Optional idempotent produce (v0.47). Default off (trailer (0, 0, -1)).
c = Client("127.0.0.1:9092", enable_idempotence=True)
# Optional native transactions (v0.57). Opcodes 50–53; not Kafka txns.
c = Client("127.0.0.1:9092", transactional_id="txn-1")
c.begin_transaction()
c.produce("t", 0, value=b"hello")
c.commit_transaction()  # or commit_transaction(offsets=[TxnOffsetCommit(...)])
c.abort_transaction()
# Optional TransactionalProducer helper (v0.63). Queues offsets until commit.
from volant import TransactionalProducer
p = TransactionalProducer(c)  # c must have transactional_id
p.begin()
p.produce("t", 0, value=b"x")
p.add_offsets("g", [("t", 0, 1)])
results = p.commit()  # or p.abort()
_ = p.is_open()
# Optional SCRAM-SHA-256 (v0.46). Token wins if both are set.
c = Client("127.0.0.1:9092", scram_username="alice", scram_password="s3cret")
# SCRAM admin (v0.55). Opcodes 64–69; not the handshake. Password in clear.
c.create_scram_user("alice", "s3cret")  # iterations=0 → broker default 4096
names = c.list_scram_users()
c.delete_scram_user("alice")
# ACL admin (v0.56). Opcodes 54–59; exact-match delete. Not Kafka CreateAcls.
from volant import AclBinding
e = AclBinding("User:alice", 0, "events", 3, 1)  # Topic, op 3, Allow
c.create_acls([e])
listed = c.list_acls()  # any/any/any
n = c.delete_acls([e])
```

`Client` is also a context manager. `produce(..., key=b"...")` is supported;
null key is the default. `fetch` returns a `FetchResult` (iterable of records
with `offset`, `key`, `value`). `metadata()` returns brokers + topics.
`offset_commit` is an admin commit (`member_id=""`, `generation=0` unless
overridden). `offset_fetch` returns committed `(partition, offset)` pairs
for the given topic. `create_partitions(topic, total_count)` grows the topic to
`total_count` partitions and returns the new total (native opcode 46,
not Kafka CreatePartitions).
`reassign_partitions(topic, replicas, partition=None)` reassigns
replicas and returns the assignment generation (native opcode 114,
not Kafka AlterPartitionReassignments). `partition=None` is all
partitions (`u32::MAX`); `replicas=[]` is auto-place.
`list_offsets(topic, partitions=None)` returns
earliest/latest (`OffsetListing`) for the topic (`None` / `[]` = all
partitions; native opcode 48, not Kafka timestamp ListOffsets).
`delete_records(topic, partition, before_offset, wait_majority=0)`
returns `DeleteRecordsResult` (`topic`, `partition`, `low_watermark`);
native opcode 44, not Kafka DeleteRecords (API key 21). `wait_majority`
0 = broker default, 1 = force wait, 2 = force no-wait. Error 13 is
not redirected (Produce/Fetch only).
`join_group` sends empty `member_id` on first join
(broker assigns one) and unpacks as
`(member_id, generation, assignment)`.
`GroupConsumer.join` / `poll` / `commit` / `close` is the high-level
loop (heartbeat on poll, re-join on error 9/10/11, cooperative revoke).
Optional `group_instance_id=` is Phase 12 static membership (empty =
dynamic); re-join resends the same instance id. `commit` sends the
joined `member_id` + `generation`. `close` leaves the group and does
not close the `Client`.
`volant.range_assign` / `range_assign_multi` match the broker range
algorithm. `GroupConsumer.join(..., assignor="range")` replaces the
fetch set with a **solo** local range (this member only — JoinGroup
does not return the live member list). Default `assignor="broker"`
keeps the broker assignment as SoT.

Produce and Fetch follow `NotLeaderForPartition` (error 13) by default:
Metadata, reconnect to the partition leader, retry once
(`max_redirects=1`). `max_redirects=0` raises on the first 13. Still
one TCP connection at a time.

Correlation ids increment per request. Decode verifies magic `V` (0x56),
protocol version 1, and IEEE CRC32 of the **payload only**.

## Tests

Codec / frame tests need no broker:

```bash
cd clients/python
python3 -m pytest -q
# or without pytest:
PYTHONPATH=src python3 -m unittest discover -s tests -q
```

Live create → produce → fetch (spawns `volant-server` on a free port):

```bash
# from repo root
cargo build -p volant-server
VOLANT_E2E=1 python3 -m pytest clients/python/tests/test_e2e.py -q
```

- `VOLANT_E2E=1` — enable the e2e test (skipped otherwise).
- `VOLANT_BROKER=127.0.0.1:9092` — use an already-running native listener.
- `VOLANT_SERVER=/path/to/volant-server` — override the binary.

Repo helper: `scripts/python_client_smoke.sh` (skips if `python3` is missing).

TLS knobs match the Rust client as closely as stdlib `ssl` allows:
`tls` (wrap after TCP connect), `tls_ca` (PEM added to the default
trust store), `tls_insecure` (skip verify; tests / lab only), optional
`tls_cert` + `tls_key` for mTLS. `tls_cert` and `tls_key` must both be
set or both unset. Handshake failures close the TCP socket.

Shared-token Auth (v0.42) sends native opcode 30 after connect (and
TLS, if any) when `auth_token` is a non-empty string. A rejected token
raises `BrokerError` with code 17 and closes the socket.

Idempotent produce (v0.47) is opt-in via `enable_idempotence=True`.
The first produce sends native InitProducerId (opcode 32) with an
empty transactional_id; later produces attach pid/epoch/seq. Default
off keeps trailer `(0, 0, -1)`. Redirect keeps the same pid. If the
broker returns UnknownProducerId (21), the client re-Inits once and
resets sequences. Not Kafka idempotent produce v2.
Native transactions (v0.57) are opt-in via `transactional_id=`.
`begin_transaction` / `commit_transaction` / `abort_transaction` send
opcodes 50–53. Init uses that id. Abort rewinds sequences. Not Kafka
transactions (API keys 22/24/25/26/28).
`TransactionalProducer` (v0.63) is a thin helper: `begin` / `produce` /
`add_offsets` (local queue) / `commit` / `abort`. Produce is
write-through; LSO/commit is broker-side. Constructor fails if
`transactional_id` is unset.
SCRAM-SHA-256 (v0.46) sends opcodes 60 then 62 after connect when
`scram_username` and `scram_password` are both set and `auth_token` is
unset. Username without password (or vice versa) is a constructor
error. A rejected proof or server-signature mismatch fails the
constructor. Leader redirect re-runs the same auth path.
Create/Delete/ListScramUsers (v0.55) are admin RPCs (opcodes 64–69),
not the handshake. `create_scram_user(user, password, iterations=0)`
sends the password in the clear (use TLS). Not Kafka
AlterUserScramCredentials.
Create/Delete/ListAcls (v0.56) are admin RPCs (opcodes 54–59).
`create_acls([AclBinding(...)])` / `delete_acls(...)` (returns
removed) / `list_acls(principal="", resource_type=255, resource="")`.
Delete is exact-match only. Not Kafka CreateAcls / DeleteAcls /
DescribeAcls (API keys 30/31/29).

## Honesty

`GroupConsumer` starts a background heartbeat thread after join
(interval `session_timeout_ms / 3`, clamped 100–3000 ms; v0.37).
Pass `heartbeat=False` for the v0.31 poll-only loop. Not a fully
concurrent API: do not share the `Client` while the consumer is open.

Opt-in auto-commit (`auto_commit=True`, default **off**; v0.48)
commits assigned positions after a successful `poll` that returned
records. `auto_commit_interval_ms=0` commits every such poll; `> 0`
commits on the first successful poll, then on the interval. Explicit
`commit()` still works and resets the clock. `close` best-effort
commits dirty positions then leaves. This is **not** Kafka
`enable.auto.commit` (no background commit thread).

`auto_offset_reset` (v0.62) is a tiny Kafka subset: `earliest`
(default, position 0, no ListOffsets), `latest` (native ListOffsets
LEO), `none` (raise if OffsetFetch is missing / `OFFSET_UNKNOWN`).
Invalid strings raise `ValueError` before JoinGroup. Not Kafka
`auto.offset.reset` (no timestamp). Rust GroupConsumer still starts
at 0 / OffsetFetch only.

Not implemented: `kafka-python`, Kafka cooperative-sticky / SyncGroup,
seeing other group members on the wire, SCRAM, async I/O,
Kafka transactions (API keys 22/24/25/26/28). Native BeginTxn/EndTxn
(opcodes 50–53) is opt-in via `transactional_id=`. Idempotent produce
is opt-in (`enable_idempotence=True`); default off. Local
`assignor="range"` cannot
seeing other group members on the wire, SCRAM-SHA-512, Kafka SASL,
async I/O, idempotent
produce, auto-commit. Local `assignor="range"` cannot
split across live members. Thin `join_group` still defaults to empty
`group_instance_id` unless the caller (or `GroupConsumer.join`) passes
one. Offset commit/fetch is the
admin path only (empty member, generation 0) unless the caller (or
`GroupConsumer.commit`) passes a joined `member_id` / `generation`.
Sync only; one TCP connection; acks=1 by default. TLS does not change
broker TLS (Phase 8/19) and does not add Kafka API keys. Leader
redirect is Produce/Fetch only (default one extra attempt).

See [docs/V14_SPEC.md](../../docs/V14_SPEC.md),
[docs/V24_SPEC.md](../../docs/V24_SPEC.md),
[docs/V27_SPEC.md](../../docs/V27_SPEC.md),
[docs/V28_SPEC.md](../../docs/V28_SPEC.md),
[docs/V31_SPEC.md](../../docs/V31_SPEC.md),
[docs/V36_SPEC.md](../../docs/V36_SPEC.md),
[docs/V37_SPEC.md](../../docs/V37_SPEC.md),
[docs/V41_SPEC.md](../../docs/V41_SPEC.md),
[docs/V42_SPEC.md](../../docs/V42_SPEC.md),
[docs/V43_SPEC.md](../../docs/V43_SPEC.md), and
[docs/V47_SPEC.md](../../docs/V47_SPEC.md),
[docs/V48_SPEC.md](../../docs/V48_SPEC.md),
[docs/V49_SPEC.md](../../docs/V49_SPEC.md),
[docs/V50_SPEC.md](../../docs/V50_SPEC.md),
[docs/V51_SPEC.md](../../docs/V51_SPEC.md),
[docs/V52_SPEC.md](../../docs/V52_SPEC.md),
[docs/V53_SPEC.md](../../docs/V53_SPEC.md),
[docs/V54_SPEC.md](../../docs/V54_SPEC.md),
[docs/V46_SPEC.md](../../docs/V46_SPEC.md),
[docs/V57_SPEC.md](../../docs/V57_SPEC.md).
[docs/V50_SPEC.md](../../docs/V50_SPEC.md).,
[docs/V46_SPEC.md](../../docs/V46_SPEC.md),
[docs/V55_SPEC.md](../../docs/V55_SPEC.md),
[docs/V56_SPEC.md](../../docs/V56_SPEC.md),
[docs/V58_SPEC.md](../../docs/V58_SPEC.md),
[docs/V59_SPEC.md](../../docs/V59_SPEC.md),
[docs/V63_SPEC.md](../../docs/V63_SPEC.md).
[docs/V62_SPEC.md](../../docs/V62_SPEC.md).
