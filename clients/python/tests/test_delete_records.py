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
    DeleteRecordsResponse,
    decode_delete_records_request,
    encode_delete_records_response,
)
from volant.frame import encode_frame, try_decode_frame


class _DeleteRecordsServer:
    """Accept one connection and reply to DeleteRecords (opcode 44 → 45)."""

    def __init__(
        self,
        *,
        error_code: int = 0,
        low_watermark: int = 96,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.error_code = error_code
        self.low_watermark = low_watermark
        self.got_topic: Optional[str] = None
        self.got_partition: Optional[int] = None
        self.got_before_offset: Optional[int] = None
        self.got_wait_majority: Optional[int] = None
        self.opcodes: list[int] = []
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_DeleteRecordsServer":
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind((self.host, 0))
        lsock.listen(1)
        lsock.settimeout(5.0)
        self._lsock = lsock
        self.port = lsock.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
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

    def _serve(self) -> None:
        assert self._lsock is not None
        try:
            conn, _ = self._lsock.accept()
        except OSError as e:
            self.error = e
            return
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
                if frame.opcode != OP_DELETE_RECORDS:
                    self.error = RuntimeError(f"unexpected opcode {frame.opcode}")
                    return
                req = decode_delete_records_request(frame.payload)
                self.got_topic = req.topic
                self.got_partition = req.partition
                self.got_before_offset = req.before_offset
                self.got_wait_majority = req.wait_majority
                payload = encode_delete_records_response(
                    DeleteRecordsResponse(
                        error_code=self.error_code,
                        topic=req.topic,
                        partition=req.partition,
                        low_watermark=self.low_watermark,
                    )
                )
                conn.sendall(
                    encode_frame(
                        OP_DELETE_RECORDS_RESPONSE, frame.correlation_id, payload
                    )
                )
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass


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

    def test_error_13_raises_without_redirect(self) -> None:
        with _DeleteRecordsServer(error_code=13) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.delete_records("events", 0, 10)
        self.assertEqual(ctx.exception.code, 13)
        self.assertEqual(ctx.exception.op, "delete_records")
        self.assertEqual(srv.opcodes, [OP_DELETE_RECORDS])


if __name__ == "__main__":
    unittest.main()
