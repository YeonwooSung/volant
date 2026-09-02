"""Create/Delete/ListAcls client tests against a fake native server."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import AclBinding, BrokerError, Client
from volant.codec import (
    OP_CREATE_ACLS,
    OP_CREATE_ACLS_RESPONSE,
    OP_DELETE_ACLS,
    OP_DELETE_ACLS_RESPONSE,
    OP_LIST_ACLS,
    OP_LIST_ACLS_RESPONSE,
    CreateAclsResponse,
    DeleteAclsResponse,
    ListAclsRequest,
    ListAclsResponse,
    decode_create_acls_request,
    decode_delete_acls_request,
    decode_list_acls_request,
    encode_create_acls_response,
    encode_delete_acls_response,
    encode_list_acls_response,
)
from volant.frame import encode_frame, try_decode_frame


def _sample() -> AclBinding:
    return AclBinding(
        principal="User:alice",
        resource_type=0,
        resource="events",
        operation=3,
        permission=1,
    )


class _AclServer:
    """Accept one connection and reply to Create/Delete/ListAcls."""

    def __init__(
        self,
        *,
        create_error: int = 0,
        delete_error: int = 0,
        list_error: int = 0,
        removed: int = 1,
        entries: Optional[list[AclBinding]] = None,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.create_error = create_error
        self.delete_error = delete_error
        self.list_error = list_error
        self.removed = removed
        self.entries = list(entries) if entries is not None else [_sample()]
        self.got_create: Optional[list[AclBinding]] = None
        self.got_delete: Optional[list[AclBinding]] = None
        self.got_list: Optional[ListAclsRequest] = None
        self.opcodes: list[int] = []
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_AclServer":
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
                if frame.opcode == OP_CREATE_ACLS:
                    req = decode_create_acls_request(frame.payload)
                    self.got_create = list(req.entries)
                    payload = encode_create_acls_response(
                        CreateAclsResponse(error_code=self.create_error)
                    )
                    conn.sendall(
                        encode_frame(
                            OP_CREATE_ACLS_RESPONSE, frame.correlation_id, payload
                        )
                    )
                elif frame.opcode == OP_DELETE_ACLS:
                    req = decode_delete_acls_request(frame.payload)
                    self.got_delete = list(req.entries)
                    payload = encode_delete_acls_response(
                        DeleteAclsResponse(
                            error_code=self.delete_error, removed=self.removed
                        )
                    )
                    conn.sendall(
                        encode_frame(
                            OP_DELETE_ACLS_RESPONSE, frame.correlation_id, payload
                        )
                    )
                elif frame.opcode == OP_LIST_ACLS:
                    self.got_list = decode_list_acls_request(frame.payload)
                    payload = encode_list_acls_response(
                        ListAclsResponse(
                            error_code=self.list_error, entries=self.entries
                        )
                    )
                    conn.sendall(
                        encode_frame(
                            OP_LIST_ACLS_RESPONSE, frame.correlation_id, payload
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


class TestAclsClient(unittest.TestCase):
    def test_create_ok(self) -> None:
        entry = _sample()
        with _AclServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.create_acls([entry])
        if srv.error is not None:
            raise srv.error
        self.assertEqual(srv.got_create, [entry])
        self.assertEqual(srv.opcodes, [OP_CREATE_ACLS])

    def test_delete_returns_removed(self) -> None:
        entry = _sample()
        with _AclServer(removed=1) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                n = c.delete_acls([entry])
        if srv.error is not None:
            raise srv.error
        self.assertEqual(n, 1)
        self.assertEqual(srv.got_delete, [entry])
        self.assertEqual(srv.opcodes, [OP_DELETE_ACLS])

    def test_list_returns_bindings(self) -> None:
        entry = _sample()
        with _AclServer(entries=[entry]) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                listed = c.list_acls()
        if srv.error is not None:
            raise srv.error
        self.assertEqual(listed, [entry])
        self.assertEqual(srv.got_list, ListAclsRequest("", 255, ""))
        self.assertEqual(srv.opcodes, [OP_LIST_ACLS])

    def test_unauthorized_raises(self) -> None:
        with _AclServer(create_error=23) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.create_acls([_sample()])
        self.assertEqual(ctx.exception.code, 23)
        self.assertEqual(ctx.exception.op, "create_acls")


if __name__ == "__main__":
    unittest.main()
