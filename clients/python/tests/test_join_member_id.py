"""v0.209: Client.join_group generates member_id on empty first join."""

from __future__ import annotations

import socket
import threading
import unittest

from volant.client import Client
from volant.codec import (
    OP_JOIN_GROUP,
    JoinGroupResponse,
    decode_join_group_request,
    encode_join_group_response,
)
from volant.frame import encode_frame, try_decode_frame


class _JoinMemberStub:
    """Tiny TCP stub that records the decoded JoinGroup member_id."""

    def __init__(self) -> None:
        self.member_id = ""
        self.instance_id = ""
        self.error: BaseException | None = None
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(("127.0.0.1", 0))
        lsock.listen(1)
        lsock.settimeout(5.0)
        self._lsock = lsock
        self.port = lsock.getsockname()[1]
        self.addr = f"127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
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
                if frame.opcode != OP_JOIN_GROUP:
                    raise OSError(f"unexpected opcode {frame.opcode}")
                req = decode_join_group_request(frame.payload)
                self.member_id = req.member_id
                self.instance_id = req.group_instance_id
                payload = encode_join_group_response(
                    JoinGroupResponse(
                        error_code=0, generation=1, member_id="m-1"
                    )
                )
                conn.sendall(encode_frame(OP_JOIN_GROUP, frame.correlation_id, payload))
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def close(self) -> None:
        try:
            self._lsock.close()
        except OSError:
            pass
        self._thread.join(timeout=2.0)

    def __enter__(self) -> _JoinMemberStub:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class TestJoinGroupGeneratedMemberId(unittest.TestCase):
    def test_empty_member_and_instance_encodes_nonempty_member_id(self) -> None:
        with _JoinMemberStub() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                result = c.join_group("g", topics=["t"])
            self.assertEqual(result.member_id, "m-1")
            self.assertTrue(srv.member_id)
            self.assertEqual(srv.instance_id, "")
        if srv.error is not None:
            raise srv.error

    def test_static_instance_sends_empty_member_id(self) -> None:
        with _JoinMemberStub() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.join_group("g", topics=["t"], group_instance_id="inst-1")
            self.assertEqual(srv.member_id, "")
            self.assertEqual(srv.instance_id, "inst-1")
        if srv.error is not None:
            raise srv.error

    def test_explicit_member_id_unchanged(self) -> None:
        with _JoinMemberStub() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.join_group("g", topics=["t"], member_id="rejoin-1")
            self.assertEqual(srv.member_id, "rejoin-1")
            self.assertEqual(srv.instance_id, "")
        if srv.error is not None:
            raise srv.error


if __name__ == "__main__":
    unittest.main()
