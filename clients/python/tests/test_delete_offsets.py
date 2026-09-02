"""DeleteOffsets client tests against a scripted TCP broker (no live server)."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import BrokerError, Client
from volant.codec import (
    OP_DELETE_OFFSETS,
    OP_DELETE_OFFSETS_RESPONSE,
    DeleteOffsetsResponse,
    decode_delete_offsets_request,
    encode_delete_offsets_response,
)
from volant.frame import encode_frame, try_decode_frame


class _DeleteOffsetsServer:
    """Accept one connection and reply to DeleteOffsets (opcode 38 → 39)."""

    def __init__(
        self,
        *,
        error_code: int = 0,
        deleted_count: int = 1,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.error_code = error_code
        self.deleted_count = deleted_count
        self.got_group: Optional[str] = None
        self.got_entries: Optional[list[tuple[str, int]]] = None
        self.seen_entries: list[list[tuple[str, int]]] = []
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_DeleteOffsetsServer":
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
                if frame.opcode != OP_DELETE_OFFSETS:
                    self.error = RuntimeError(f"unexpected opcode {frame.opcode}")
                    return
                req = decode_delete_offsets_request(frame.payload)
                self.got_group = req.group_id
                pairs = [(e.topic, e.partition) for e in req.entries]
                self.got_entries = pairs
                self.seen_entries.append(pairs)
                payload = encode_delete_offsets_response(
                    DeleteOffsetsResponse(
                        error_code=self.error_code,
                        deleted_count=self.deleted_count,
                    )
                )
                conn.sendall(
                    encode_frame(
                        OP_DELETE_OFFSETS_RESPONSE, frame.correlation_id, payload
                    )
                )
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass


class TestDeleteOffsetsClient(unittest.TestCase):
    def test_empty_entries_encoded_as_count_zero(self) -> None:
        with _DeleteOffsetsServer(deleted_count=3) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                got = c.delete_offsets("g")
                also = c.delete_offsets("g", [])
        if srv.error is not None:
            raise srv.error
        self.assertEqual(srv.got_group, "g")
        self.assertEqual(srv.got_entries, [])
        self.assertEqual(srv.seen_entries, [[], []])
        self.assertEqual(got, 3)
        self.assertEqual(also, 3)

    def test_explicit_entry_roundtrip(self) -> None:
        with _DeleteOffsetsServer(deleted_count=1) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                got = c.delete_offsets("g", [("events", 0)])
        if srv.error is not None:
            raise srv.error
        self.assertEqual(srv.got_group, "g")
        self.assertEqual(srv.got_entries, [("events", 0)])
        self.assertEqual(got, 1)

    def test_nonzero_error_code_raises(self) -> None:
        with _DeleteOffsetsServer(error_code=2) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.delete_offsets("missing")
        self.assertEqual(ctx.exception.code, 2)
        self.assertEqual(ctx.exception.op, "delete_offsets")


if __name__ == "__main__":
    unittest.main()
