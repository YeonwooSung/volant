"""Shared-token Auth constructor tests against a fake native server."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import Client
from volant.codec import (
    OP_AUTH,
    OP_AUTH_RESPONSE,
    OP_METADATA,
    AuthResponse,
    BrokerError,
    BrokerInfo,
    MetadataResponse,
    decode_auth_request,
    encode_auth_response,
    encode_metadata_response,
)
from volant.frame import encode_frame, try_decode_frame


class _OneShotServer:
    """Accept one connection, record the first frame, optionally reply.

    ``auth_error`` is the single-shot path (close after a non-zero Auth).
    ``auth_codes`` is a same-socket queue: after each Auth reply the
    connection stays open so the client can send Auth again.
    """

    def __init__(
        self,
        *,
        auth_error: Optional[int] = None,
        auth_codes: Optional[list[int]] = None,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.auth_error = auth_error
        self.auth_codes = list(auth_codes) if auth_codes is not None else None
        self.auth_count = 0
        self.first_opcode: Optional[int] = None
        self.got_token: Optional[str] = None
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_OneShotServer":
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
                if self.first_opcode is None:
                    self.first_opcode = frame.opcode
                if frame.opcode == OP_AUTH:
                    self.auth_count += 1
                    self.got_token = decode_auth_request(frame.payload).token
                    if self.auth_codes is not None:
                        if self.auth_codes:
                            code = self.auth_codes.pop(0)
                        else:
                            code = 0
                    else:
                        code = 0 if self.auth_error is None else self.auth_error
                    conn.sendall(
                        encode_frame(
                            OP_AUTH_RESPONSE,
                            frame.correlation_id,
                            encode_auth_response(AuthResponse(error_code=code)),
                        )
                    )
                    if code != 0 and self.auth_codes is None:
                        return
                    continue
                if frame.opcode == OP_METADATA:
                    payload = encode_metadata_response(
                        MetadataResponse(
                            brokers=[
                                BrokerInfo(node_id=1, host="127.0.0.1", port=self.port)
                            ],
                            topics=[],
                        )
                    )
                    conn.sendall(encode_frame(OP_METADATA, frame.correlation_id, payload))
                    return
                return
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass


class TestAuthClient(unittest.TestCase):
    def test_token_sends_auth(self) -> None:
        with _OneShotServer() as srv:
            with Client(srv.addr, timeout=5.0, auth_token="s3cret") as c:
                self.assertEqual(c.auth_token, "s3cret")
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)
        self.assertEqual(srv.first_opcode, OP_AUTH)
        self.assertEqual(srv.got_token, "s3cret")
        if srv.error is not None:
            raise srv.error

    def test_rejected_token_raises(self) -> None:
        with _OneShotServer(auth_error=17) as srv:
            with self.assertRaises(BrokerError) as cm:
                Client(srv.addr, timeout=5.0, auth_token="nope")
        self.assertEqual(cm.exception.code, 17)
        self.assertEqual(cm.exception.op, "auth")
        self.assertEqual(srv.got_token, "nope")

    def test_no_token_sends_no_auth(self) -> None:
        with _OneShotServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                self.assertIsNone(c.auth_token)
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)
        self.assertEqual(srv.first_opcode, OP_METADATA)
        self.assertIsNone(srv.got_token)

    def test_empty_token_skips_auth(self) -> None:
        with _OneShotServer() as srv:
            with Client(srv.addr, timeout=5.0, auth_token="") as c:
                self.assertIsNone(c.auth_token)
                c.metadata()
        self.assertEqual(srv.first_opcode, OP_METADATA)


TIMEOUT = 7
AUTH_FAILED = 17


class TestAuthRetry(unittest.TestCase):
    def test_default_max_retries_zero_raises_on_auth_timeout(self) -> None:
        with _OneShotServer(auth_codes=[TIMEOUT]) as srv:
            with self.assertRaises(BrokerError) as cm:
                Client(srv.addr, timeout=5.0, auth_token="s3cret")
        self.assertEqual(cm.exception.code, TIMEOUT)
        self.assertEqual(cm.exception.op, "auth")
        self.assertEqual(srv.auth_count, 1)

    def test_retries_auth_timeout_then_ok(self) -> None:
        with _OneShotServer(auth_codes=[TIMEOUT, 0]) as srv:
            with Client(
                srv.addr,
                timeout=5.0,
                auth_token="s3cret",
                max_retries=2,
                retry_backoff_ms=0,
            ) as c:
                self.assertEqual(c.max_retries, 2)
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)
        self.assertEqual(srv.auth_count, 2)
        self.assertEqual(srv.got_token, "s3cret")
        if srv.error is not None:
            raise srv.error

    def test_auth_failed_not_retried(self) -> None:
        with _OneShotServer(auth_codes=[AUTH_FAILED]) as srv:
            with self.assertRaises(BrokerError) as cm:
                Client(
                    srv.addr,
                    timeout=5.0,
                    auth_token="nope",
                    max_retries=2,
                    retry_backoff_ms=0,
                )
        self.assertEqual(cm.exception.code, AUTH_FAILED)
        self.assertEqual(cm.exception.op, "auth")
        self.assertEqual(srv.auth_count, 1)

    def test_auth_exhausted_retries_raises(self) -> None:
        with _OneShotServer(auth_codes=[TIMEOUT, TIMEOUT, TIMEOUT]) as srv:
            with self.assertRaises(BrokerError) as cm:
                Client(
                    srv.addr,
                    timeout=5.0,
                    auth_token="s3cret",
                    max_retries=2,
                    retry_backoff_ms=0,
                )
        self.assertEqual(cm.exception.code, TIMEOUT)
        self.assertEqual(cm.exception.op, "auth")
        self.assertEqual(srv.auth_count, 3)


if __name__ == "__main__":
    unittest.main()
