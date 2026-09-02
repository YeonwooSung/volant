"""Create/Delete/ListScramUsers client tests against a fake native server."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import BrokerError, Client
from volant.codec import (
    OP_CREATE_SCRAM_USER,
    OP_CREATE_SCRAM_USER_RESPONSE,
    OP_DELETE_SCRAM_USER,
    OP_DELETE_SCRAM_USER_RESPONSE,
    OP_LIST_SCRAM_USERS,
    OP_LIST_SCRAM_USERS_RESPONSE,
    CreateScramUserResponse,
    DeleteScramUserResponse,
    ListScramUsersResponse,
    decode_create_scram_user_request,
    decode_delete_scram_user_request,
    encode_create_scram_user_response,
    encode_delete_scram_user_response,
    encode_list_scram_users_response,
)
from volant.frame import encode_frame, try_decode_frame


class _ScramAdminServer:
    """Accept one connection and reply to Create/Delete/ListScramUsers."""

    def __init__(
        self,
        *,
        create_error: int = 0,
        delete_error: int = 0,
        list_error: int = 0,
        usernames: Optional[list[str]] = None,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.create_error = create_error
        self.delete_error = delete_error
        self.list_error = list_error
        self.usernames = list(usernames) if usernames is not None else ["alice", "bob"]
        self.got_create: Optional[tuple[str, str, int]] = None
        self.got_delete: Optional[str] = None
        self.list_payload: Optional[bytes] = None
        self.opcodes: list[int] = []
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_ScramAdminServer":
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
                if frame.opcode == OP_CREATE_SCRAM_USER:
                    req = decode_create_scram_user_request(frame.payload)
                    self.got_create = (req.username, req.password, req.iterations)
                    payload = encode_create_scram_user_response(
                        CreateScramUserResponse(error_code=self.create_error)
                    )
                    conn.sendall(
                        encode_frame(
                            OP_CREATE_SCRAM_USER_RESPONSE,
                            frame.correlation_id,
                            payload,
                        )
                    )
                elif frame.opcode == OP_DELETE_SCRAM_USER:
                    req = decode_delete_scram_user_request(frame.payload)
                    self.got_delete = req.username
                    payload = encode_delete_scram_user_response(
                        DeleteScramUserResponse(error_code=self.delete_error)
                    )
                    conn.sendall(
                        encode_frame(
                            OP_DELETE_SCRAM_USER_RESPONSE,
                            frame.correlation_id,
                            payload,
                        )
                    )
                elif frame.opcode == OP_LIST_SCRAM_USERS:
                    self.list_payload = bytes(frame.payload)
                    payload = encode_list_scram_users_response(
                        ListScramUsersResponse(
                            error_code=self.list_error, usernames=self.usernames
                        )
                    )
                    conn.sendall(
                        encode_frame(
                            OP_LIST_SCRAM_USERS_RESPONSE,
                            frame.correlation_id,
                            payload,
                        )
                    )
                else:
                    self.error = RuntimeError(f"unexpected opcode {frame.opcode}")
                    return
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass


class TestScramAdminClient(unittest.TestCase):
    def test_create_ok(self) -> None:
        with _ScramAdminServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.create_scram_user("alice", "s3cret", 4096)
        if srv.error is not None:
            raise srv.error
        self.assertEqual(srv.got_create, ("alice", "s3cret", 4096))
        self.assertEqual(srv.opcodes, [OP_CREATE_SCRAM_USER])

    def test_delete_not_found_raises(self) -> None:
        with _ScramAdminServer(delete_error=2) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.delete_scram_user("missing")
        self.assertEqual(ctx.exception.code, 2)
        self.assertEqual(ctx.exception.op, "delete_scram_user")
        self.assertEqual(srv.got_delete, "missing")

    def test_list_returns_names(self) -> None:
        with _ScramAdminServer(usernames=["alice", "bob"]) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                names = c.list_scram_users()
        if srv.error is not None:
            raise srv.error
        self.assertEqual(names, ["alice", "bob"])
        self.assertEqual(srv.list_payload, b"")
        self.assertEqual(srv.opcodes, [OP_LIST_SCRAM_USERS])

    def test_unauthorized_raises(self) -> None:
        with _ScramAdminServer(list_error=23) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.list_scram_users()
        self.assertEqual(ctx.exception.code, 23)
        self.assertEqual(ctx.exception.op, "list_scram_users")


if __name__ == "__main__":
    unittest.main()
