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
c.produce("t", 0, value=b"hello")
batch = c.fetch("t", 0, offset=0)
for offset, key, value in batch.tuples():
    print(offset, key, value)
c.offset_commit(group="g", topic="t", partition=0, offset=5)
offs = c.offset_fetch(group="g", topic="t")  # [(partition, offset), ...]
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
```

`Client` is also a context manager. `produce(..., key=b"...")` is supported;
null key is the default. `fetch` returns a `FetchResult` (iterable of records
with `offset`, `key`, `value`). `metadata()` returns brokers + topics.
`offset_commit` is an admin commit (`member_id=""`, `generation=0` unless
overridden). `offset_fetch` returns committed `(partition, offset)` pairs
for the given topic. `join_group` sends empty `member_id` on first join
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

## Honesty

`GroupConsumer` starts a background heartbeat thread after join
(interval `session_timeout_ms / 3`, clamped 100–3000 ms; v0.37).
Pass `heartbeat=False` for the v0.31 poll-only loop. Not a fully
concurrent API: do not share the `Client` while the consumer is open.

Not implemented: `kafka-python`, Kafka cooperative-sticky / SyncGroup,
seeing other group members on the wire, SCRAM / shared-token auth,
async I/O, idempotent produce, leader redirect, auto-commit. Local
`assignor="range"` cannot split across live members. Thin `join_group`
still defaults to empty `group_instance_id` unless the caller (or
`GroupConsumer.join`) passes one. Offset commit/fetch is the
admin path only (empty member, generation 0) unless the caller (or
`GroupConsumer.commit`) passes a joined `member_id` / `generation`.
Sync only; one TCP connection; acks=1 by default. TLS does not change
broker TLS (Phase 8/19) and does not add Kafka API keys.

See [docs/V14_SPEC.md](../../docs/V14_SPEC.md),
[docs/V24_SPEC.md](../../docs/V24_SPEC.md),
[docs/V27_SPEC.md](../../docs/V27_SPEC.md),
[docs/V28_SPEC.md](../../docs/V28_SPEC.md),
[docs/V31_SPEC.md](../../docs/V31_SPEC.md),
[docs/V36_SPEC.md](../../docs/V36_SPEC.md),
[docs/V37_SPEC.md](../../docs/V37_SPEC.md), and
[docs/V41_SPEC.md](../../docs/V41_SPEC.md).
