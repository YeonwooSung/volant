"""Leader-redirect tests against a scripted TCP broker (no live volant-server)."""

from __future__ import annotations

import socket
import threading
import unittest

from volant import BrokerError, Client
from volant.codec import (
    OP_FETCH,
    OP_METADATA,
    OP_PRODUCE,
    BrokerInfo,
    FetchResponse,
    MetadataResponse,
    PartitionInfo,
    ProduceResponse,
    TopicInfo,
    decode_fetch_request,
    decode_produce_request,
    encode_fetch_response,
    encode_metadata_response,
    encode_produce_response,
)
from volant.frame import encode_frame, try_decode_frame

NOT_LEADER = 13


class ScriptedBroker:
    """Accepts connections and replies to Produce / Fetch / Metadata.

    ``produce_codes`` / ``fetch_codes`` are queues of error_code values
    consumed across connections. Metadata is a fixed response (or a
    callable of ``() -> MetadataResponse``).
    """

    def __init__(self) -> None:
        self.produce_codes: list[int] = []
        self.fetch_codes: list[int] = []
        self.metadata: MetadataResponse | None = None
        self.opcodes: list[int] = []
        self.produce_count = 0
        self.fetch_count = 0
        self.metadata_count = 0
        self.accept_count = 0
        self.error: BaseException | None = None
        self._lsock: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self.port = 0

    @property
    def addr(self) -> str:
        return f"127.0.0.1:{self.port}"

    def start(self) -> ScriptedBroker:
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(("127.0.0.1", 0))
        lsock.listen(8)
        lsock.settimeout(5.0)
        self._lsock = lsock
        self.port = lsock.getsockname()[1]
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._lsock is not None:
            try:
                self._lsock.close()
            except OSError:
                pass
            self._lsock = None
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def __enter__(self) -> ScriptedBroker:
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    def _accept_loop(self) -> None:
        assert self._lsock is not None
        while True:
            try:
                conn, _ = self._lsock.accept()
            except OSError:
                return
            self.accept_count += 1
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(5.0)
            buf = bytearray()
            while True:
                frame, rest = try_decode_frame(bytes(buf))
                if frame is None:
                    chunk = conn.recv(4096)
                    if not chunk:
                        return
                    buf.extend(chunk)
                    continue
                buf = bytearray(rest)
                self.opcodes.append(frame.opcode)
                payload = self._handle(frame.opcode, frame.payload)
                conn.sendall(encode_frame(frame.opcode, frame.correlation_id, payload))
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _handle(self, opcode: int, raw: bytes) -> bytes:
        if opcode == OP_PRODUCE:
            self.produce_count += 1
            req = decode_produce_request(raw)
            code = self.produce_codes.pop(0) if self.produce_codes else 0
            return encode_produce_response(
                ProduceResponse(
                    topic=req.topic,
                    partition=req.partition if req.partition >= 0 else 0,
                    base_offset=7 if code == 0 else 0,
                    count=1 if code == 0 else 0,
                    error_code=code,
                )
            )
        if opcode == OP_FETCH:
            self.fetch_count += 1
            req = decode_fetch_request(raw)
            code = self.fetch_codes.pop(0) if self.fetch_codes else 0
            return encode_fetch_response(
                FetchResponse(
                    topic=req.topic,
                    partition=req.partition,
                    high_watermark=0,
                    error_code=code,
                    records=[],
                )
            )
        if opcode == OP_METADATA:
            self.metadata_count += 1
            meta = self.metadata
            if callable(meta):
                meta = meta()
            if meta is None:
                meta = MetadataResponse(brokers=[], topics=[])
            return encode_metadata_response(meta)
        raise ProtocolErrorForTest(f"unexpected opcode {opcode}")


class ProtocolErrorForTest(Exception):
    pass


def _leader_meta(topic: str, partition: int, leader_id: int, host: str, port: int) -> MetadataResponse:
    return MetadataResponse(
        brokers=[
            BrokerInfo(node_id=1, host="127.0.0.1", port=1),
            BrokerInfo(node_id=leader_id, host=host, port=port),
        ],
        topics=[
            TopicInfo(
                name=topic,
                topic_id=1,
                error_code=0,
                partitions=[
                    PartitionInfo(
                        partition_id=partition,
                        leader=leader_id,
                        hwm=0,
                        replicas=[1, leader_id],
                        isr=[leader_id],
                        leader_epoch=1,
                    )
                ],
            )
        ],
    )


class TestLeaderRedirect(unittest.TestCase):
    def test_produce_redirects_to_leader(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            follower.metadata = _leader_meta("t", 0, 2, "127.0.0.1", leader.port)
            leader.produce_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                result = c.produce("t", 0, value=b"hello")
            self.assertEqual(result.base_offset, 7)
            self.assertEqual(result.topic, "t")
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(follower.metadata_count, 1)
            self.assertEqual(leader.produce_count, 1)
            self.assertEqual(c.addr, leader.addr)

    def test_max_redirects_zero_raises_on_first_13(self) -> None:
        with ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            follower.metadata = _leader_meta("t", 0, 2, "127.0.0.1", 9)
            with Client(follower.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, NOT_LEADER)
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(follower.metadata_count, 0)
            self.assertEqual(follower.accept_count, 1)

    def test_fetch_redirects_once(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.fetch_codes = [NOT_LEADER]
            follower.metadata = _leader_meta("t", 0, 2, "127.0.0.1", leader.port)
            leader.fetch_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                batch = c.fetch("t", 0, offset=0)
            self.assertEqual(len(batch), 0)
            self.assertEqual(follower.fetch_count, 1)
            self.assertEqual(follower.metadata_count, 1)
            self.assertEqual(leader.fetch_count, 1)
            self.assertEqual(c.addr, leader.addr)

    def test_missing_leader_raises_13(self) -> None:
        with ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            # Topic/partition present but leader broker id is unknown.
            follower.metadata = MetadataResponse(
                brokers=[BrokerInfo(node_id=1, host="127.0.0.1", port=follower.port)],
                topics=[
                    TopicInfo(
                        name="t",
                        topic_id=1,
                        error_code=0,
                        partitions=[
                            PartitionInfo(
                                partition_id=0,
                                leader=99,
                                hwm=0,
                                replicas=[],
                                isr=[],
                                leader_epoch=0,
                            )
                        ],
                    )
                ],
            )
            with Client(follower.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, NOT_LEADER)
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(follower.metadata_count, 1)
            self.assertEqual(follower.accept_count, 1)
            self.assertEqual(c.addr, follower.addr)

    def test_empty_host_raises_13(self) -> None:
        with ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            follower.metadata = MetadataResponse(
                brokers=[BrokerInfo(node_id=2, host="", port=9092)],
                topics=[
                    TopicInfo(
                        name="t",
                        topic_id=1,
                        error_code=0,
                        partitions=[
                            PartitionInfo(
                                partition_id=0,
                                leader=2,
                                hwm=0,
                                replicas=[],
                                isr=[],
                                leader_epoch=0,
                            )
                        ],
                    )
                ],
            )
            with Client(follower.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"x")
            self.assertEqual(ctx.exception.code, NOT_LEADER)
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(follower.metadata_count, 1)
            self.assertEqual(follower.accept_count, 1)


if __name__ == "__main__":
    unittest.main()
