"""CreatePartitions client tests against a scripted TCP broker (no live server)."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import BrokerError, Client
from volant.codec import (
    OP_CREATE_PARTITIONS,
    OP_CREATE_PARTITIONS_RESPONSE,
    CreatePartitionsResponse,
    decode_create_partitions_request,
    encode_create_partitions_response,
)
from volant.frame import encode_frame, try_decode_frame


class _CreatePartitionsServer:
    """Accept one connection and reply to CreatePartitions (opcode 46 → 47)."""

    def __init__(self, *, error_code: int = 0, partitions: int = 4) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.error_code = error_code
        self.partitions = partitions
        self.got_topic: Optional[str] = None
        self.got_total_count: Optional[int] = None
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_CreatePartitionsServer":
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
                if frame.opcode != OP_CREATE_PARTITIONS:
                    self.error = RuntimeError(f"unexpected opcode {frame.opcode}")
                    return
                req = decode_create_partitions_request(frame.payload)
                self.got_topic = req.topic
                self.got_total_count = req.total_count
                new_total = 0 if self.error_code != 0 else self.partitions
                payload = encode_create_partitions_response(
                    CreatePartitionsResponse(
                        error_code=self.error_code,
                        topic=req.topic,
                        partitions=new_total,
                    )
                )
                conn.sendall(
                    encode_frame(
                        OP_CREATE_PARTITIONS_RESPONSE, frame.correlation_id, payload
                    )
                )
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass


class TestCreatePartitionsClient(unittest.TestCase):
    def test_success_returns_new_count(self) -> None:
        with _CreatePartitionsServer(partitions=4) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                got = c.create_partitions("events", 4)
        if srv.error is not None:
            raise srv.error
        self.assertEqual(srv.got_topic, "events")
        self.assertEqual(srv.got_total_count, 4)
        self.assertEqual(got, 4)

    def test_nonzero_error_code_raises(self) -> None:
        with _CreatePartitionsServer(error_code=2, partitions=0) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.create_partitions("missing", 4)
        self.assertEqual(ctx.exception.code, 2)
        self.assertEqual(ctx.exception.op, "create_partitions")


if __name__ == "__main__":
    unittest.main()
