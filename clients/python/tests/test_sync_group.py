"""SyncGroup client tests against a scripted TCP broker (no live server)."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import Assignment, BrokerError, Client
from volant.codec import (
    OP_SYNC_GROUP,
    OP_SYNC_GROUP_RESPONSE,
    SyncGroupResponse,
    decode_sync_group_request,
    encode_sync_group_request,
    encode_sync_group_response,
    decode_sync_group_response,
    SyncGroupRequest,
)
from volant.frame import encode_frame, try_decode_frame


class _SyncGroupServer:
    """Accept one connection and reply to SyncGroup (opcode 116 → 117)."""

    def __init__(
        self,
        *,
        error_code: int = 0,
        assignment: Optional[list[Assignment]] = None,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.error_code = error_code
        self.assignment = list(assignment or [])
        self.got_group: Optional[str] = None
        self.got_member: Optional[str] = None
        self.got_generation: Optional[int] = None
        self.got_bytes_len: Optional[int] = None
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_SyncGroupServer":
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
                if frame.opcode != OP_SYNC_GROUP:
                    self.error = RuntimeError(f"unexpected opcode {frame.opcode}")
                    return
                req = decode_sync_group_request(frame.payload)
                self.got_group = req.group_id
                self.got_member = req.member_id
                self.got_generation = req.generation
                self.got_bytes_len = len(req.assignment_bytes)
                asgn = [] if self.error_code != 0 else self.assignment
                payload = encode_sync_group_response(
                    SyncGroupResponse(error_code=self.error_code, assignment=asgn)
                )
                conn.sendall(
                    encode_frame(OP_SYNC_GROUP_RESPONSE, frame.correlation_id, payload)
                )
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass


class TestSyncGroupCodec(unittest.TestCase):
    def test_request_response_roundtrip(self) -> None:
        req = SyncGroupRequest(
            group_id="g1", member_id="m1", generation=3, assignment_bytes=b""
        )
        raw = encode_sync_group_request(req)
        self.assertEqual(decode_sync_group_request(raw), req)
        resp = SyncGroupResponse(
            error_code=0, assignment=[Assignment(topic="events", partition=2)]
        )
        rraw = encode_sync_group_response(resp)
        self.assertEqual(decode_sync_group_response(rraw), resp)


class TestSyncGroupClient(unittest.TestCase):
    def test_success_returns_assignment(self) -> None:
        asgn = [Assignment(topic="events", partition=2)]
        with _SyncGroupServer(assignment=asgn) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                got = c.sync_group("g1", "m1", 3)
        if srv.error is not None:
            raise srv.error
        self.assertEqual(srv.got_group, "g1")
        self.assertEqual(srv.got_member, "m1")
        self.assertEqual(srv.got_generation, 3)
        self.assertEqual(srv.got_bytes_len, 0)
        self.assertEqual(got, asgn)

    def test_unknown_member_is_10(self) -> None:
        with _SyncGroupServer(error_code=10) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.sync_group("g", "ghost", 1)
        self.assertEqual(ctx.exception.code, 10)
        self.assertEqual(ctx.exception.op, "sync_group")

    def test_generation_mismatch_is_9(self) -> None:
        with _SyncGroupServer(error_code=9) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.sync_group("g", "m1", 99)
        self.assertEqual(ctx.exception.code, 9)
        self.assertEqual(ctx.exception.op, "sync_group")


if __name__ == "__main__":
    unittest.main()
