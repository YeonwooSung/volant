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
c.reassign_partitions_all("t", [1, 2])  # v0.198; same as reassign_partitions(topic, replicas)
c.produce("t", 0, value=b"hello")
# acks=1 by default; acks=255 is acks=all (already shipped).
# Client default acks (v0.129): Client(..., acks=255) or c.acks = 255; produce(..., acks=) still wins.
c.produce("t", 0, value=b"hello", acks=255)
batch = c.fetch("t", 0, offset=0)
# max_messages=128, max_bytes=4MiB, max_wait_ms=0 by default (already shipped).
# Client default knobs (v0.143): Client(..., fetch_max_messages=10) or c.fetch_max_messages = 10; explicit kwargs still win. 0 stays 0.
batch = c.fetch("t", 0, offset=0, max_messages=10, max_bytes=4096, max_wait_ms=100)
for offset, key, value in batch.tuples():
    print(offset, key, value)
c.offset_commit(group="g", topic="t", partition=0, offset=5)
c.commit_offsets("g", [("t", 0, 5), ("t", 1, 9)])  # v0.119 batch
offs = c.offset_fetch(group="g", topic="t")  # [(partition, offset), ...]
all_offs = c.offset_fetch_all("g")  # v0.118; [(topic, partition, offset), ...]
topic_offs = c.offset_fetch_entries("g", "t")  # v0.148; [OffsetFetchEntry, ...] with metadata
rows = c.fetch_offsets("g", [("t", 0)])  # v0.122; empty/None = all
rows = c.fetch_offset("g", "t", 0)  # v0.179; one OffsetEntry
deleted = c.delete_offsets("g", [("t", 0)])
deleted = c.delete_offset("g", "t", 0)  # v0.164; one OffsetEntry
bounds = c.list_offsets("t")  # [OffsetListing(partition, earliest, latest), ...]
bounds = c.list_offsets_all("t")  # v0.197; same as list_offsets(topic)
cfg = c.describe_configs("t")
c.alter_configs("t", [("retention.ms", "86400000")])
c.alter_config("t", "retention.ms", "86400000")  # v0.177; one key
cut = c.delete_records("t", 0, 100)  # DeleteRecordsResult; wait_majority=0
# Client default wait (v0.152): Client(..., delete_records_wait=1) or c.delete_records_wait = 1; wait_majority= still wins.
# cut = c.delete_records("t", 0, 100, wait_majority=1)  # force majority wait
member_id, generation, assignment = c.join_group(
    "g", topics=["t"], session_timeout_ms=10000
)
c.heartbeat("g", member_id, generation)
_ = c.sync_group("g", member_id, generation)  # v0.206 peek/confirm; same assignment as Join
c.leave_group("g", member_id)

# High-level group consumer (v0.31). Two members need two Clients.
from volant import GroupConsumer
g = GroupConsumer.join(c, group="g", topics=["t"], session_timeout_ms=10_000)
# Phase 12 static membership (empty / omitted = dynamic):
g = GroupConsumer.join(
    c, group="g", topics=["t"], session_timeout_ms=10_000, group_instance_id="inst-1"
)
recs = g.poll(max_wait_ms=500)
_ = g.heartbeat_count  # v0.188; poll + background Heartbeats (not JoinGroup)
g.commit()
g.close()
# Opt-in auto-commit (v0.48). Default off. interval 0 = after every poll.
g = GroupConsumer.join(
    c, group="g", topics=["t"], auto_commit=True, auto_commit_interval_ms=5000
)
# Opt-in auto_offset_reset (v0.62/v0.70). Default earliest (ListOffsets earliest).
g = GroupConsumer.join(c, group="g", topics=["t"], auto_offset_reset="latest")
# Poll fetch size (v0.75). Default 100 / 4MiB; not Kafka max.poll.records.
g = GroupConsumer.join(c, group="g", topics=["t"], fetch_max_messages=10, fetch_max_bytes=4096)

meta = c.metadata()
meta = c.metadata_topic("events")  # v0.181; one topic
c.reconnect("127.0.0.1:9093")  # v0.115; re-Auth / re-SCRAM
_ = c.timeout  # v0.195; dial / RPC timeout (constructor default 10.0)
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
# Pre-allocate pid (v0.150). Second call is a no-op. Produce / BeginTxn still init implicitly.
pid, epoch = c.init_producer_id()
# Stored pid/epoch without Init (v0.160). Uninitialized is 0.
_ = c.producer_id
_ = c.producer_epoch
# Optional produce/fetch retry (v0.61 / v0.66). Default 0 extra attempts.
c = Client("127.0.0.1:9092", max_retries=3, retry_backoff_ms=50)
c.max_retries = 3
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
c.create_acl(e)  # v0.169; one binding
c.create_acls([e])
listed = c.list_acls()  # any/any/any
listed = c.list_acls_all()  # v0.196; same as list_acls()
n = c.delete_acl(e)  # v0.169; one binding
n = c.delete_acls([e])
```

`Client` is also a context manager. `produce(..., key=b"...")` is supported;
null key is the default. `fetch` returns a `FetchResult` (iterable of records
with `offset`, `key`, `value`). `metadata()` returns brokers + topics.
`metadata_topic(topic)` fetches one named topic; same as
`metadata([topic])`.
`offset_commit` is an admin commit (`member_id=""`, `generation=0` unless
overridden). `offset_fetch` returns committed `(partition, offset)` pairs
for the given topic. `offset_fetch_entries` returns the same topic
filter as `OffsetFetchEntry` rows including metadata.
`fetch_offset(group, topic, partition)` fetches one committed offset
(one OffsetEntry); same as `fetch_offsets(group, [(topic, partition)])`.
`delete_offset(group, topic, partition)` deletes one committed offset
(one OffsetEntry); same as `delete_offsets(group, [(topic, partition)])`.
`alter_config(topic, key, value)` alters one topic config key; same as
`alter_configs(topic, [(key, value)])`.
`create_partitions(topic, total_count)` grows the topic to
`total_count` partitions and returns the new total (native opcode 46,
not Kafka CreatePartitions).
`reassign_partitions(topic, replicas, partition=None)` reassigns
replicas and returns the assignment generation (native opcode 114,
not Kafka AlterPartitionReassignments). `partition=None` is all
partitions (`u32::MAX`); `replicas=[]` is auto-place.
`reassign_partitions_all(topic, replicas)` reassigns every partition
(wire `u32::MAX`); same as `reassign_partitions(topic, replicas)` /
`reassign_partitions(topic, replicas, None)`.
`list_offsets(topic, partitions=None)` returns
earliest/latest (`OffsetListing`) for the topic (`None` / `[]` = all
partitions; native opcode 48, not Kafka timestamp ListOffsets).
`list_offsets_all(topic)` lists earliest/latest for every partition
(empty wire partitions); same as `list_offsets(topic)`.
`delete_records(topic, partition, before_offset, wait_majority=None)`
returns `DeleteRecordsResult` (`topic`, `partition`, `low_watermark`);
native opcode 44, not Kafka DeleteRecords (API key 21). `wait_majority`
0 = broker default, 1 = force wait, 2 = force no-wait. Omitted
`wait_majority` uses `self.delete_records_wait` (constructor default
0; v0.152). An explicit `wait_majority=` wins. Error 13 follows
Produce/Fetch redirect. Transient 6 / 7 / 15 / 16 follow ``max_retries``.
`join_group` sends empty `member_id` on first join
(broker assigns one) and unpacks as
`(member_id, generation, assignment)`.
`GroupConsumer.join` / `poll` / `commit` / `close` is the high-level
loop (heartbeat on poll, re-join on error 9/10/11, cooperative revoke).
Optional `group_instance_id=` is Phase 12 static membership (empty =
dynamic); re-join resends the same instance id. `commit` sends the
joined `member_id` + `generation` in one OffsetCommit for all assigned
positions (v0.123). `close` leaves the group and does
not close the `Client`.
`volant.range_assign` / `range_assign_multi` match the broker range
algorithm. `GroupConsumer.join(..., assignor="range")` replaces the
fetch set with a local range over **DescribeGroup** members (still no
SyncGroup; describe failure falls back to solo). Default
`assignor="broker"` keeps the broker assignment as SoT.

Produce, Fetch, and DeleteRecords follow `NotLeaderForPartition`
(error 13) by default: Metadata, reconnect to the partition leader,
retry once (`max_redirects=1`). `max_redirects=0` raises on the first
13. CreateTopic / DeleteTopic / CreatePartitions / ReassignPartitions /
CreateAcls / DeleteAcls / CreateScramUser / DeleteScramUser /
ListScramUsers / ListAcls / AddBroker / RemoveBroker /
DescribeConfigs / AlterConfigs / DeleteOffsets / OffsetCommit /
OffsetFetch / ListMembers / DescribeGroup / ListGroups / Heartbeat /
LeaveGroup / Metadata follow
`NotController` (error 14) the same way (Metadata `controller_id`
trailer when the message has no hint, else `controller_id=N` or the
first other advertised broker; not Kafka FindCoordinator). AddBroker /
RemoveBroker follow error 14 when the broker cannot forward.
Controller-gated admin shares ``max_retries`` for transient 6 / 7 /
15 / 16 and TCP/IO (default 0); error 14 stays on ``max_redirects``.
Still one TCP connection at a time.
Produce and Fetch retry transient broker codes 6 / 7 / 15 / 16 and TCP
I/O errors up to ``max_retries`` extra attempts (default 0). Sleep
``retry_backoff_ms`` (default 50) between attempts; tests may set 0.
Error 13 stays on the redirect budget; error 21 stays on the one
re-Init. Heartbeat shares produce/fetch ``max_retries`` (default 0);
rebalance codes 9 / 10 / 11 are not retried. LeaveGroup shares
``max_retries``; error 10 is success (already left); error 14 follows
``max_redirects``. JoinGroup shares
``max_retries`` when ``member_id`` or ``group_instance_id`` is
non-empty (rejoin / static membership); empty first join is one shot.
OffsetCommit / OffsetFetch / DeleteOffsets / ListOffsets /
DescribeGroup / ListGroups / Metadata / ListMembers / BeginTxn /
EndTxn / InitProducerId / Auth / SCRAM handshake / DeleteRecords
share the same ``max_retries`` (default 0).
InvalidTxnState (22) is not retried. Error 21 on InitProducerId
itself is not retried (distinct from produce's one re-Init).
Auth retries transient 6 / 7 / 15 / 16 and TCP/IO; 17 / 18 is not
retried. SCRAM first+final is one unit (new nonce on restart);
17 / 18 and server-signature mismatch are not retried. DeleteRecords
error 13 stays on ``max_redirects``. ListOffsets error 13 follows
Produce/Fetch redirect (``max_redirects``).
This is not Kafka ``retries``.

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
`init_producer_id()` (v0.150) pre-allocates the pid; a second call
is a no-op. Produce / BeginTxn still init implicitly.
`producer_id` / `producer_epoch` (v0.160) read the stored values
without Init. Uninitialized is 0.
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
Transient 6 / 7 / 15 / 16 and TCP/IO retry the whole handshake from first with a new nonce (v0.108; default `max_retries=0`).
Create/Delete/ListScramUsers (v0.55) are admin RPCs (opcodes 64–69),
not the handshake. `create_scram_user(user, password, iterations=0)`
sends the password in the clear (use TLS). Not Kafka
AlterUserScramCredentials.
Create/Delete/ListAcls (v0.56) are admin RPCs (opcodes 54–59).
`create_acls([AclBinding(...)])` / `delete_acls(...)` (returns
removed) / `list_acls(principal="", resource_type=255, resource="")`.
`list_acls_all()` lists every ACL binding (empty filters); same as
`list_acls()` / `list_acls("", 255, "")`.
`create_acl(entry)` / `delete_acl(entry)` create or delete one binding
(same as a one-element list).
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

`auto_offset_reset` (v0.62/v0.70) is a tiny Kafka subset: `earliest`
(default, native ListOffsets earliest), `latest` (ListOffsets latest /
LEO), `none` (raise if OffsetFetch is missing / `OFFSET_UNKNOWN`).
Invalid strings raise `ValueError` before JoinGroup. Not Kafka
`auto.offset.reset` (no timestamp). Rust GroupConsumer still starts
at 0 / OffsetFetch only.

Poll fetch size is tunable (`fetch_max_messages` / `fetch_max_bytes`,
default **100 / 4MiB**; v0.75). `poll` still takes only
`max_wait_ms`. Values `<= 0` clamp to the defaults. This is **not**
Kafka `max.poll.records` (and not Client `fetch`'s default 128).

Not implemented: `kafka-python`, Kafka cooperative-sticky / SyncGroup,
seeing other group members on the wire, SCRAM, async I/O,
Kafka transactions (API keys 22/24/25/26/28). Native BeginTxn/EndTxn
(opcodes 50–53) is opt-in via `transactional_id=`. Idempotent produce
is opt-in (`enable_idempotence=True`); default off. Local
`assignor="range"` uses DescribeGroup members (still no SyncGroup).
seeing other group members on the wire, SCRAM-SHA-512, Kafka SASL,
async I/O, idempotent
produce, auto-commit. Local `assignor="range"` uses DescribeGroup
members (still no SyncGroup). Thin `join_group` still defaults to empty
`group_instance_id` unless the caller (or `GroupConsumer.join`) passes
one. Offset commit/fetch is the
admin path only (empty member, generation 0) unless the caller (or
`GroupConsumer.commit`) passes a joined `member_id` / `generation`.
Sync only; one TCP connection; acks=1 by default (`acks=255` is
acks=all). `fetch` already takes `max_messages` / `max_bytes` /
`max_wait_ms`. Omitted knobs use `self.fetch_max_*` (constructor
defaults 128 / 4MiB / 0; v0.143). Explicit kwargs still win.
GroupConsumer poll knobs stay 100 / 4MiB (v0.75). Convenience batch is `messages=` (Go `ProduceBatch` /
Java list `produce` now match; not Kafka Produce; native opcode 1).
TLS does not change
broker TLS (Phase 8/19) and does not add Kafka API keys. Leader
redirect is Produce/Fetch/DeleteRecords (error 13) and the six
controller-gated admin RPCs (error 14; default one extra attempt).

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
[docs/V64_SPEC.md](../../docs/V64_SPEC.md),
[docs/V61_SPEC.md](../../docs/V61_SPEC.md).
