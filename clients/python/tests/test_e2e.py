"""Optional live broker e2e. Skipped unless VOLANT_E2E=1.

Recipe (from repo root)::

    cargo build -p volant-server
    VOLANT_E2E=1 python3 -m pytest clients/python/tests/test_e2e.py -q

Or point at an already-running broker::

    VOLANT_E2E=1 VOLANT_BROKER=127.0.0.1:9092 python3 -m pytest ...
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from volant import Client, GroupConsumer


def _e2e_enabled() -> bool:
    return os.environ.get("VOLANT_E2E") == "1"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _find_server_bin() -> Path | None:
    env = os.environ.get("VOLANT_SERVER")
    if env:
        p = Path(env)
        if p.is_file():
            return p
    root = _repo_root()
    for candidate in (
        root / "target" / "debug" / "volant-server",
        root / "target" / "release" / "volant-server",
    ):
        if candidate.is_file():
            return candidate
    return None


def _ensure_server_bin() -> Path | None:
    found = _find_server_bin()
    if found is not None:
        return found
    root = _repo_root()
    cargo = shutil.which("cargo")
    if cargo is None:
        return None
    proc = subprocess.run(
        [cargo, "build", "-p", "volant-server"],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=300,
        check=False,
    )
    if proc.returncode != 0:
        return None
    return _find_server_bin()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_port(host: str, port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except OSError as e:
            last = e
            time.sleep(0.05)
    raise TimeoutError(f"broker did not listen on {host}:{port}: {last}")


@unittest.skipUnless(_e2e_enabled(), "set VOLANT_E2E=1 to run live broker e2e")
class TestE2E(unittest.TestCase):
    proc: subprocess.Popen | None = None
    data_dir: str | None = None
    addr: str = ""

    @classmethod
    def setUpClass(cls) -> None:
        existing = os.environ.get("VOLANT_BROKER")
        if existing:
            cls.addr = existing
            cls.proc = None
            cls.data_dir = None
            host, port_s = existing.rsplit(":", 1)
            _wait_port(host, int(port_s), timeout=5.0)
            return

        binary = _ensure_server_bin()
        if binary is None:
            raise unittest.SkipTest(
                "volant-server not found; build with `cargo build -p volant-server` "
                "or set VOLANT_SERVER / VOLANT_BROKER"
            )
        cls.data_dir = tempfile.mkdtemp(prefix="volant-py-e2e-")
        port = _free_port()
        cls.addr = f"127.0.0.1:{port}"
        cls.proc = subprocess.Popen(
            [
                str(binary),
                "--listen",
                cls.addr,
                "--data-dir",
                cls.data_dir,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=_repo_root(),
        )
        try:
            _wait_port("127.0.0.1", port)
        except Exception:
            cls.tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.proc is not None:
            cls.proc.terminate()
            try:
                cls.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                cls.proc.kill()
                cls.proc.wait(timeout=5)
            cls.proc = None
        if cls.data_dir and os.path.isdir(cls.data_dir):
            shutil.rmtree(cls.data_dir, ignore_errors=True)

    def test_create_produce_fetch_metadata(self) -> None:
        topic = f"py-e2e-{os.getpid()}-{int(time.time())}"
        with Client(self.addr, timeout=5.0) as c:
            topic_id = c.create_topic(topic, partitions=1)
            self.assertGreaterEqual(topic_id, 1)

            produced = c.produce(topic, 0, value=b"hello")
            self.assertEqual(produced.partition, 0)
            self.assertEqual(produced.count, 1)
            self.assertEqual(produced.base_offset, 0)

            batch = c.fetch(topic, 0, offset=0)
            self.assertEqual(len(batch), 1)
            rec = batch.records[0]
            self.assertEqual(rec.offset, 0)
            self.assertIsNone(rec.key)
            self.assertEqual(rec.value, b"hello")
            self.assertGreaterEqual(batch.high_watermark, 1)
            self.assertEqual(batch.tuples(), [(0, None, b"hello")])

            meta = c.metadata()
            names = [t.name for t in meta.topics]
            self.assertIn(topic, names)
            self.assertTrue(meta.brokers)

            c.delete_topic(topic)
            meta2 = c.metadata()
            self.assertNotIn(topic, [t.name for t in meta2.topics])

    def test_offset_commit_fetch(self) -> None:
        topic = f"py-off-{os.getpid()}-{int(time.time())}"
        group = f"py-g-{os.getpid()}"
        with Client(self.addr, timeout=5.0) as c:
            c.create_topic(topic, partitions=1)
            produced = c.produce(topic, 0, value=b"hello")
            self.assertEqual(produced.base_offset, 0)
            c.offset_commit(group=group, topic=topic, partition=0, offset=5)
            offs = c.offset_fetch(group=group, topic=topic)
            self.assertEqual(offs, [(0, 5)])
            c.delete_topic(topic)

    def test_join_heartbeat_leave(self) -> None:
        topic = f"py-grp-{os.getpid()}-{int(time.time())}"
        group = f"py-cg-{os.getpid()}"
        with Client(self.addr, timeout=5.0) as c:
            c.create_topic(topic, partitions=1)
            member_id, generation, assignment = c.join_group(
                group, topics=[topic], session_timeout_ms=10000
            )
            self.assertTrue(member_id)
            self.assertGreaterEqual(generation, 1)
            self.assertEqual([(a.topic, a.partition) for a in assignment], [(topic, 0)])
            self.assertEqual(c.heartbeat(group, member_id, generation), 0)
            c.leave_group(group, member_id)
            c.delete_topic(topic)

    def test_group_consumer_poll_commit_resume(self) -> None:
        topic = f"py-gc-{os.getpid()}-{int(time.time())}"
        group = f"py-gcg-{os.getpid()}"
        with Client(self.addr, timeout=5.0) as c:
            c.create_topic(topic, partitions=1)
            c.produce(topic, 0, value=b"a")
            c.produce(topic, 0, value=b"b")
            g = GroupConsumer.join(
                c, group=group, topics=[topic], session_timeout_ms=10_000
            )
            recs: list = []
            for _ in range(8):
                recs.extend(g.poll(max_wait_ms=200))
                if len(recs) >= 2:
                    break
            self.assertEqual([r.value for r in recs], [b"a", b"b"])
            g.commit()
            g.close()
            offs = c.offset_fetch(group=group, topic=topic)
            self.assertEqual(offs, [(0, 2)])

            c.produce(topic, 0, value=b"c")
            g2 = GroupConsumer.join(
                c, group=group, topics=[topic], session_timeout_ms=10_000
            )
            recs2: list = []
            for _ in range(8):
                recs2.extend(g2.poll(max_wait_ms=200))
                if recs2:
                    break
            self.assertEqual([r.value for r in recs2], [b"c"])
            g2.close()
            c.delete_topic(topic)

    def test_group_consumer_two_members_split(self) -> None:
        topic = f"py-gc2-{os.getpid()}-{int(time.time())}"
        group = f"py-gc2g-{os.getpid()}"
        c1 = Client(self.addr, timeout=5.0)
        c2 = Client(self.addr, timeout=5.0)
        try:
            c1.create_topic(topic, partitions=2)
            c1.produce(topic, 0, value=b"p0")
            c1.produce(topic, 1, value=b"p1")
            g1 = GroupConsumer.join(
                c1, group=group, topics=[topic], session_timeout_ms=10_000
            )
            g2 = GroupConsumer.join(
                c2, group=group, topics=[topic], session_timeout_ms=10_000
            )
            seen: set[int] = set()
            for _ in range(8):
                for g in (g1, g2):
                    for r in g.poll(max_wait_ms=100):
                        seen.add(r.partition)
            a1 = set(g1.assignment)
            a2 = set(g2.assignment)
            self.assertTrue(
                a1.isdisjoint(a2), f"assignments overlap: {a1!r} vs {a2!r}"
            )
            self.assertEqual(a1 | a2, {(topic, 0), (topic, 1)})
            self.assertEqual(seen, {0, 1})
            g1.close()
            g2.close()
            c1.delete_topic(topic)
        finally:
            c1.close()
            c2.close()


if __name__ == "__main__":
    unittest.main()
