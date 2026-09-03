"""DeleteRecords client tests against a scripted TCP broker (no live server)."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import BrokerError, Client, DeleteRecordsResult
from volant.codec import (
    OP_DELETE_RECORDS,
    OP_DELETE_RECORDS_RESPONSE,
    OP_METADATA,
    BrokerInfo,
    DeleteRecordsRequest,
    DeleteRecordsResponse,
    MetadataResponse,
    PartitionInfo,
    TopicInfo,
    decode_delete_records_request,
    encode_delete_records_response,
    encode_metadata_response,
)
from volant.frame import encode_frame, try_decode_frame

NOT_LEADER = 13


class _DeleteRecordsServer:
    """Accept connections and reply to DeleteRecords / Metadata."""

    def __init__(
        self,
        *,
        error_code: int = 0,
        low_watermark: int = 96,
        error_codes: Optional[list[int]] = None,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.error_codes: list[int] = (
            list(error_codes) if error_codes is not None else [error_code]
        )
        self.low_watermark = low_watermark
        self.metadata: Optional[MetadataResponse] = None
        self.got_topic: Optional[str] = None
        self.got_partition: Optional[int] = None
        self.got_before_offset: Optional[int] = None
        self.got_wait_majority: Optional[int] = None
        self.reqs: list[DeleteRecordsRequest] = []
        self.opcodes: list[int] = []
        self.delete_count = 0
        self.metadata_count = 0
        self.accept_count = 0
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_DeleteRecordsServer":
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind((self.host, 0))
        lsock.listen(8)
        lsock.settimeout(5.0)
        self._lsock = lsock
        self.port = lsock.getsockname()[1]
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        if self._lsock is not None:
            try:
                self._lsock.close()
            except OSError:
                pass
            self._lsock = None
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _accept_loop(self) -> None:
        assert self._lsock is not None
        while True:
            try:
                conn, _ = self._lsock.accept()
            except OSError:
                return
            with self._lock:
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
        with self._lock:
            self.opcodes.append(opcode)
            if opcode == OP_DELETE_RECORDS:
                self.delete_count += 1
                req = decode_delete_records_request(raw)
                self.reqs.append(req)
                self.got_topic = req.topic
                self.got_partition = req.partition
                self.got_before_offset = req.before_offset
                self.got_wait_majority = req.wait_majority
                code = self.error_codes.pop(0) if self.error_codes else 0
                return (
                    encode_delete_records_response(
                        DeleteRecordsResponse(
                            error_code=code,
                            topic=req.topic,
                            partition=req.partition,
                            low_watermark=self.low_watermark if code == 0 else 0,
                        )
                    ),
                    OP_DELETE_RECORDS_RESPONSE,
                )
            if opcode == OP_METADATA:
                self.metadata_count += 1
                meta = self.metadata
                if meta is None:
                    meta = MetadataResponse(brokers=[], topics=[])
                return encode_metadata_response(meta), OP_METADATA
            raise RuntimeError(f"unexpected opcode {opcode}")


def _leader_meta(
    topic: str, partition: int, leader_id: int, host: str, port: int
) -> MetadataResponse:
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


class TestDeleteRecordsClient(unittest.TestCase):
    def test_success_returns_low_watermark(self) -> None:
        with _DeleteRecordsServer(low_watermark=96) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                got = c.delete_records("events", 2, 100)
                also = c.delete_records("events", 2, 100, wait_majority=1)
        if srv.error is not None:
            raise srv.error
        self.assertEqual(
            got, DeleteRecordsResult(topic="events", partition=2, low_watermark=96)
        )
        self.assertEqual(also, got)
        self.assertEqual(srv.got_topic, "events")
        self.assertEqual(srv.got_partition, 2)
        self.assertEqual(srv.got_before_offset, 100)
        self.assertEqual(srv.got_wait_majority, 1)
        self.assertEqual(srv.opcodes, [OP_DELETE_RECORDS, OP_DELETE_RECORDS])

    def test_error_13_max_redirects_zero_raises(self) -> None:
        with _DeleteRecordsServer(error_code=13) as srv:
            with Client(srv.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.delete_records("events", 0, 10)
        self.assertEqual(ctx.exception.code, NOT_LEADER)
        self.assertEqual(ctx.exception.op, "delete_records")
        self.assertEqual(srv.opcodes, [OP_DELETE_RECORDS])
        self.assertEqual(srv.delete_count, 1)
        self.assertEqual(srv.metadata_count, 0)

    def test_error_13_redirects_to_leader(self) -> None:
        with _DeleteRecordsServer(low_watermark=96) as leader, _DeleteRecordsServer(
            error_code=13
        ) as follower:
            follower.metadata = _leader_meta("events", 2, 2, "127.0.0.1", leader.port)
            with Client(follower.addr, timeout=5.0) as c:
                got = c.delete_records("events", 2, 100, wait_majority=1)
            self.assertEqual(
                got,
                DeleteRecordsResult(topic="events", partition=2, low_watermark=96),
            )
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.delete_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.delete_count, 1)
        self.assertEqual(leader.got_wait_majority, 1)
        self.assertEqual(leader.got_before_offset, 100)

    def test_error_13_unknown_topic_raises(self) -> None:
        with _DeleteRecordsServer(error_code=13) as srv:
            srv.metadata = MetadataResponse(
                brokers=[BrokerInfo(node_id=1, host="127.0.0.1", port=srv.port)],
                topics=[],
            )
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.delete_records("events", 0, 10)
            self.assertEqual(c.addr, srv.addr)
        self.assertEqual(ctx.exception.code, NOT_LEADER)
        self.assertEqual(ctx.exception.op, "delete_records")
        self.assertEqual(srv.delete_count, 1)
        self.assertEqual(srv.metadata_count, 1)
        self.assertEqual(srv.accept_count, 1)

    def test_default_max_retries_zero_raises_on_timeout(self) -> None:
        with _DeleteRecordsServer(error_code=7) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                self.assertEqual(c.max_retries, 0)
                with self.assertRaises(BrokerError) as ctx:
                    c.delete_records("events", 0, 10)
            self.assertEqual(ctx.exception.code, 7)
            self.assertEqual(ctx.exception.op, "delete_records")
        self.assertEqual(srv.delete_count, 1)
        self.assertEqual(srv.metadata_count, 0)
        self.assertEqual(srv.opcodes, [OP_DELETE_RECORDS])

    def test_retries_timeout_then_ok(self) -> None:
        with _DeleteRecordsServer(error_codes=[7, 0], low_watermark=96) as srv:
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                got = c.delete_records("events", 2, 100)
            self.assertEqual(
                got,
                DeleteRecordsResult(topic="events", partition=2, low_watermark=96),
            )
        self.assertEqual(srv.delete_count, 2)
        self.assertEqual(srv.metadata_count, 0)
        self.assertEqual(srv.opcodes, [OP_DELETE_RECORDS, OP_DELETE_RECORDS])

    def test_error_13_redirect_not_counted_as_retry(self) -> None:
        with _DeleteRecordsServer(low_watermark=96) as leader, _DeleteRecordsServer(
            error_code=13
        ) as follower:
            follower.metadata = _leader_meta("events", 2, 2, "127.0.0.1", leader.port)
            with Client(follower.addr, timeout=5.0) as c:
                self.assertEqual(c.max_retries, 0)
                got = c.delete_records("events", 2, 100, wait_majority=1)
            self.assertEqual(
                got,
                DeleteRecordsResult(topic="events", partition=2, low_watermark=96),
            )
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.delete_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.delete_count, 1)
        self.assertEqual(leader.got_wait_majority, 1)

    def test_not_found_not_retried(self) -> None:
        with _DeleteRecordsServer(error_code=2) as srv:
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.delete_records("events", 0, 10)
            self.assertEqual(ctx.exception.code, 2)
            self.assertEqual(ctx.exception.op, "delete_records")
        self.assertEqual(srv.delete_count, 1)
        self.assertEqual(srv.metadata_count, 0)
        self.assertEqual(srv.opcodes, [OP_DELETE_RECORDS])

    def test_exhausted_retries_raises(self) -> None:
        with _DeleteRecordsServer(error_codes=[7, 7, 7]) as srv:
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.delete_records("events", 0, 10)
            self.assertEqual(ctx.exception.code, 7)
            self.assertEqual(ctx.exception.op, "delete_records")
        self.assertEqual(srv.delete_count, 3)
        self.assertEqual(srv.metadata_count, 0)
        self.assertEqual(
            srv.opcodes, [OP_DELETE_RECORDS, OP_DELETE_RECORDS, OP_DELETE_RECORDS]
        )


if __name__ == "__main__":
    unittest.main()
