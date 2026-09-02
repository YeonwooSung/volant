"""Leader-redirect tests against a scripted TCP broker (no live volant-server)."""

from __future__ import annotations

import socket
import threading
import unittest

from volant import BrokerError, Client
from volant.codec import (
    OP_FETCH,
    OP_INIT_PRODUCER_ID,
    OP_INIT_PRODUCER_ID_RESPONSE,
    OP_METADATA,
    OP_PRODUCE,
    BrokerInfo,
    FetchResponse,
    InitProducerIdResponse,
    MetadataResponse,
    PartitionInfo,
    ProduceRequest,
    ProduceResponse,
    TopicInfo,
    decode_fetch_request,
    decode_init_producer_id_request,
    decode_produce_request,
    encode_fetch_response,
    encode_init_producer_id_response,
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
        self.produce_reqs: list[ProduceRequest] = []
        self.init_txn_ids: list[str] = []
        self.init_count = 0
        self.produce_count = 0
        self.fetch_count = 0
        self.metadata_count = 0
        self.accept_count = 0
        self.init_pid = 42
        self.init_epoch = 1
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
                payload, reply_op = self._handle(frame.opcode, frame.payload)
                conn.sendall(encode_frame(reply_op, frame.correlation_id, payload))
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _handle(self, opcode: int, raw: bytes) -> tuple[bytes, int]:
        if opcode == OP_INIT_PRODUCER_ID:
            self.init_count += 1
            req = decode_init_producer_id_request(raw)
            self.init_txn_ids.append(req.transactional_id)
            return (
                encode_init_producer_id_response(
                    InitProducerIdResponse(
                        producer_id=self.init_pid,
                        epoch=self.init_epoch,
                        error_code=0,
                    )
                ),
                OP_INIT_PRODUCER_ID_RESPONSE,
            )
        if opcode == OP_PRODUCE:
            self.produce_count += 1
            req = decode_produce_request(raw)
            self.produce_reqs.append(req)
            code = self.produce_codes.pop(0) if self.produce_codes else 0
            return (
                encode_produce_response(
                    ProduceResponse(
                        topic=req.topic,
                        partition=req.partition if req.partition >= 0 else 0,
                        base_offset=7 if code == 0 else 0,
                        count=len(req.messages) if code == 0 else 0,
                        error_code=code,
                    )
                ),
                OP_PRODUCE,
            )
        if opcode == OP_FETCH:
            self.fetch_count += 1
            req = decode_fetch_request(raw)
            code = self.fetch_codes.pop(0) if self.fetch_codes else 0
            return (
                encode_fetch_response(
                    FetchResponse(
                        topic=req.topic,
                        partition=req.partition,
                        high_watermark=0,
                        error_code=code,
                        records=[],
                    )
                ),
                OP_FETCH,
            )
        if opcode == OP_METADATA:
            self.metadata_count += 1
            meta = self.metadata
            if callable(meta):
                meta = meta()
            if meta is None:
                meta = MetadataResponse(brokers=[], topics=[])
            return encode_metadata_response(meta), OP_METADATA
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


class TestIdempotentProduce(unittest.TestCase):
    def test_enable_on_inits_then_sequences(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0, enable_idempotence=True) as c:
                c.produce("t", 0, value=b"a")
                c.produce("t", 0, value=b"b")
            self.assertEqual(srv.init_count, 1)
            self.assertEqual(srv.init_txn_ids, [""])
            self.assertEqual(srv.opcodes, [OP_INIT_PRODUCER_ID, OP_PRODUCE, OP_PRODUCE])
            self.assertEqual(len(srv.produce_reqs), 2)
            first, second = srv.produce_reqs
            self.assertEqual(
                (first.producer_id, first.producer_epoch, first.base_sequence),
                (42, 1, 0),
            )
            self.assertEqual(
                (second.producer_id, second.producer_epoch, second.base_sequence),
                (42, 1, 1),
            )

    def test_enable_off_no_init_default_trailer(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.produce("t", 0, value=b"a")
                c.produce("t", 0, value=b"b")
            self.assertEqual(srv.init_count, 0)
            self.assertEqual(srv.opcodes, [OP_PRODUCE, OP_PRODUCE])
            for req in srv.produce_reqs:
                self.assertEqual(
                    (req.producer_id, req.producer_epoch, req.base_sequence),
                    (0, 0, -1),
                )

    def test_batch_increments_by_message_count(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0, enable_idempotence=True) as c:
                c.produce("t", 0, messages=[b"a", b"b"])
                c.produce("t", 0, value=b"c")
            self.assertEqual(srv.produce_reqs[0].base_sequence, 0)
            self.assertEqual(srv.produce_reqs[1].base_sequence, 2)

    def test_redirect_keeps_pid_and_sequence(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            follower.metadata = _leader_meta("t", 0, 2, "127.0.0.1", leader.port)
            with Client(follower.addr, timeout=5.0, enable_idempotence=True) as c:
                c.produce("t", 0, value=b"hello")
                c.produce("t", 0, value=b"again")
            self.assertEqual(follower.init_count, 1)
            self.assertEqual(leader.init_count, 0)
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(leader.produce_count, 2)
            self.assertEqual(
                (
                    follower.produce_reqs[0].producer_id,
                    follower.produce_reqs[0].producer_epoch,
                    follower.produce_reqs[0].base_sequence,
                ),
                (42, 1, 0),
            )
            self.assertEqual(leader.produce_reqs[0].base_sequence, 0)
            self.assertEqual(leader.produce_reqs[1].base_sequence, 1)
            self.assertEqual(leader.produce_reqs[0].producer_id, 42)
            self.assertEqual(c.addr, leader.addr)


TIMEOUT = 7


class TestProduceRetry(unittest.TestCase):
    def test_default_max_retries_zero_raises_on_timeout(self) -> None:
        with ScriptedBroker() as srv:
            srv.produce_codes = [TIMEOUT]
            with Client(srv.addr, timeout=5.0) as c:
                self.assertEqual(c.max_retries, 0)
                self.assertEqual(c.retry_backoff_ms, 50)
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.produce_count, 1)

    def test_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.produce_codes = [TIMEOUT, TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                result = c.produce("t", 0, value=b"hello")
            self.assertEqual(result.base_offset, 7)
            self.assertEqual(srv.produce_count, 3)

    def test_exhausted_retries_raises(self) -> None:
        with ScriptedBroker() as srv:
            srv.produce_codes = [TIMEOUT, TIMEOUT, TIMEOUT]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.produce_count, 3)

    def test_error_13_does_not_consume_retries(self) -> None:
        with ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            follower.metadata = _leader_meta("t", 0, 2, "127.0.0.1", 9)
            with Client(
                follower.addr,
                timeout=5.0,
                max_redirects=0,
                max_retries=2,
                retry_backoff_ms=0,
            ) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, NOT_LEADER)
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(follower.metadata_count, 0)

    def test_failed_retries_do_not_increment_sequence(self) -> None:
        with ScriptedBroker() as srv:
            srv.produce_codes = [0, TIMEOUT, TIMEOUT, TIMEOUT, 0]
            with Client(
                srv.addr,
                timeout=5.0,
                enable_idempotence=True,
                max_retries=2,
                retry_backoff_ms=0,
            ) as c:
                c.produce("t", 0, value=b"a")
                with self.assertRaises(BrokerError):
                    c.produce("t", 0, value=b"b")
                c.produce("t", 0, value=b"c")
            seqs = [r.base_sequence for r in srv.produce_reqs]
            self.assertEqual(seqs, [0, 1, 1, 1, 1])


if __name__ == "__main__":
    unittest.main()
