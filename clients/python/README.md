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
meta = c.metadata()
c.close()
```

`Client` is also a context manager. `produce(..., key=b"...")` is supported;
null key is the default. `fetch` returns a `FetchResult` (iterable of records
with `offset`, `key`, `value`). `metadata()` returns brokers + topics.
`offset_commit` is an admin commit (`member_id=""`, `generation=0` unless
overridden). `offset_fetch` returns committed `(partition, offset)` pairs
for the given topic.

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

## Honesty

Not implemented: Java client, `kafka-python`, JoinGroup / Heartbeat /
LeaveGroup, TLS / SCRAM / shared-token auth, async I/O, idempotent
produce, leader redirect. Offset commit/fetch is the admin path only
(empty member, generation 0). Sync only; one TCP connection; acks=1 by
default.

See [docs/V14_SPEC.md](../../docs/V14_SPEC.md) and
[docs/V24_SPEC.md](../../docs/V24_SPEC.md).
