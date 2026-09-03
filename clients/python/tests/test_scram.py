"""SCRAM-SHA-256 crypto pin + constructor tests against a fake native server."""

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
    OP_SCRAM_FINAL,
    OP_SCRAM_FINAL_RESPONSE,
    OP_SCRAM_FIRST,
    OP_SCRAM_FIRST_RESPONSE,
    AuthResponse,
    BrokerError,
    BrokerInfo,
    MetadataResponse,
    ScramFinalResponse,
    ScramFirstResponse,
    decode_auth_request,
    decode_scram_final_request,
    decode_scram_first_request,
    encode_auth_response,
    encode_metadata_response,
    encode_scram_final_response,
    encode_scram_first_response,
)
from volant.frame import ProtocolError, encode_frame, try_decode_frame
from volant.scram import client_proof_and_server_sig

# Pinned vector matching crates/volant-client/src/scram.rs.
_USER = "alice"
_PASS = "s3cret"
_CLIENT_NONCE = "rOprNGfwEbeRWgbNEkqO"
_SALT = b"saltSALTsaltSALT"
_ITERS = 4096
_COMBINED = _CLIENT_NONCE + "server"
_PROOF_HEX = "82aa6ee69043dd3c43785fba02fe220ea4a74a44b12d31b3a3a3ad17c1e0b5f3"
_SIG_HEX = "d3068040897e7eaaa647e45356dab05074e5d48f6a283ec72a5181421768783d"


class TestScramCrypto(unittest.TestCase):
    def test_pinned_vector(self) -> None:
        proof, sig = client_proof_and_server_sig(
            _USER, _PASS, _CLIENT_NONCE, _COMBINED, _SALT, _ITERS
        )
        self.assertEqual(proof.hex(), _PROOF_HEX)
        self.assertEqual(sig.hex(), _SIG_HEX)


class _ScramServer:
    """Accept connections and speak Auth / SCRAM / Metadata."""

    def __init__(
        self,
        *,
        password: str = _PASS,
        salt: bytes = _SALT,
        iterations: int = _ITERS,
        final_error: Optional[int] = None,
        bad_signature: bool = False,
        connections: int = 1,
        first_errors: Optional[list[int]] = None,
        final_errors: Optional[list[int]] = None,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.password = password
        self.salt = salt
        self.iterations = iterations
        self.final_error = final_error
        self.bad_signature = bad_signature
        self.connections = connections
        self._first_errors = list(first_errors or [])
        self._final_errors = list(final_errors or [])
        self.opcodes: list[int] = []
        self.first_usernames: list[str] = []
        self.final_usernames: list[str] = []
        self.first_nonces: list[str] = []
        self.got_token: Optional[str] = None
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_ScramServer":
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind((self.host, 0))
        lsock.listen(self.connections)
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
            for _ in range(self.connections):
                conn, _ = self._lsock.accept()
                try:
                    self._handle(conn)
                finally:
                    try:
                        conn.close()
                    except OSError:
                        pass
        except BaseException as e:
            self.error = e

    def _handle(self, conn: socket.socket) -> None:
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
            if frame.opcode == OP_AUTH:
                self.got_token = decode_auth_request(frame.payload).token
                conn.sendall(
                    encode_frame(
                        OP_AUTH_RESPONSE,
                        frame.correlation_id,
                        encode_auth_response(AuthResponse(error_code=0)),
                    )
                )
                continue
            if frame.opcode == OP_SCRAM_FIRST:
                req = decode_scram_first_request(frame.payload)
                self.first_usernames.append(req.username)
                self.first_nonces.append(req.client_nonce)
                combined = req.client_nonce + "s"
                first_code = self._first_errors.pop(0) if self._first_errors else 0
                conn.sendall(
                    encode_frame(
                        OP_SCRAM_FIRST_RESPONSE,
                        frame.correlation_id,
                        encode_scram_first_response(
                            ScramFirstResponse(
                                error_code=first_code,
                                combined_nonce=combined,
                                salt=self.salt,
                                iterations=self.iterations,
                            )
                        ),
                    )
                )
                continue
            if frame.opcode == OP_SCRAM_FINAL:
                req = decode_scram_final_request(frame.payload)
                self.final_usernames.append(req.username)
                client_nonce = req.combined_nonce[: -1] if req.combined_nonce.endswith("s") else ""
                expected_proof, expected_sig = client_proof_and_server_sig(
                    req.username,
                    self.password,
                    client_nonce,
                    req.combined_nonce,
                    self.salt,
                    self.iterations,
                )
                if self._final_errors:
                    code = self._final_errors.pop(0)
                    sig = expected_sig
                elif self.final_error is not None:
                    code = self.final_error
                    sig = expected_sig
                elif req.client_proof != expected_proof:
                    code = 17
                    sig = expected_sig
                else:
                    code = 0
                    sig = bytes(32) if self.bad_signature else expected_sig
                conn.sendall(
                    encode_frame(
                        OP_SCRAM_FINAL_RESPONSE,
                        frame.correlation_id,
                        encode_scram_final_response(
                            ScramFinalResponse(error_code=code, server_signature=sig)
                        ),
                    )
                )
                # Keep the connection open after a transient reply so the
                # client can restart the handshake on the same socket.
                if code in (6, 7, 15, 16):
                    continue
                if code != 0 or self.bad_signature:
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


class TestScramClient(unittest.TestCase):
    def test_sends_first_and_final(self) -> None:
        with _ScramServer() as srv:
            with Client(srv.addr, timeout=5.0, scram_username=_USER, scram_password=_PASS) as c:
                self.assertEqual(c.scram_username, _USER)
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)
        self.assertEqual(srv.opcodes[:2], [OP_SCRAM_FIRST, OP_SCRAM_FINAL])
        self.assertEqual(srv.first_usernames, [_USER])
        self.assertEqual(srv.final_usernames, [_USER])
        if srv.error is not None:
            raise srv.error

    def test_bad_password_fails(self) -> None:
        with _ScramServer() as srv:
            with self.assertRaises(BrokerError) as cm:
                Client(srv.addr, timeout=5.0, scram_username=_USER, scram_password="wrong")
        self.assertEqual(cm.exception.code, 17)
        self.assertEqual(cm.exception.op, "scram final")
        self.assertEqual(srv.opcodes[:2], [OP_SCRAM_FIRST, OP_SCRAM_FINAL])

    def test_signature_mismatch_fails(self) -> None:
        with _ScramServer(bad_signature=True) as srv:
            with self.assertRaises(ProtocolError) as cm:
                Client(srv.addr, timeout=5.0, scram_username=_USER, scram_password=_PASS)
        self.assertIn("signature mismatch", str(cm.exception))

    def test_no_creds_sends_neither(self) -> None:
        with _ScramServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.metadata()
        self.assertEqual(srv.opcodes, [OP_METADATA])
        self.assertIsNone(srv.got_token)
        self.assertEqual(srv.first_usernames, [])

    def test_auth_token_wins_over_scram(self) -> None:
        with _ScramServer() as srv:
            with Client(
                srv.addr,
                timeout=5.0,
                auth_token="s3cret",
                scram_username=_USER,
                scram_password=_PASS,
            ) as c:
                c.metadata()
        self.assertEqual(srv.opcodes[0], OP_AUTH)
        self.assertEqual(srv.got_token, "s3cret")
        self.assertNotIn(OP_SCRAM_FIRST, srv.opcodes)
        self.assertNotIn(OP_SCRAM_FINAL, srv.opcodes)

    def test_username_without_password_errors(self) -> None:
        with self.assertRaises(ValueError):
            Client("127.0.0.1:1", scram_username="alice")
        with self.assertRaises(ValueError):
            Client("127.0.0.1:1", scram_password="s3cret")

    def test_reconnect_reruns_scram(self) -> None:
        with _ScramServer(connections=2) as srv:
            with Client(srv.addr, timeout=5.0, scram_username=_USER, scram_password=_PASS) as c:
                c.metadata()
                c._reconnect(srv.addr)
                c.metadata()
        self.assertEqual(srv.first_usernames, [_USER, _USER])
        self.assertEqual(srv.final_usernames, [_USER, _USER])
        if srv.error is not None:
            raise srv.error

    def test_default_max_retries_zero_raises_on_first_timeout(self) -> None:
        with _ScramServer(first_errors=[7]) as srv:
            with self.assertRaises(BrokerError) as cm:
                Client(srv.addr, timeout=5.0, scram_username=_USER, scram_password=_PASS)
        self.assertEqual(cm.exception.code, 7)
        self.assertEqual(cm.exception.op, "scram first")
        self.assertEqual(len(srv.first_usernames), 1)
        self.assertEqual(len(srv.final_usernames), 0)

    def test_retries_first_timeout_then_ok(self) -> None:
        with _ScramServer(first_errors=[7]) as srv:
            with Client(
                srv.addr,
                timeout=5.0,
                scram_username=_USER,
                scram_password=_PASS,
                max_retries=2,
                retry_backoff_ms=0,
            ) as c:
                c.metadata()
        self.assertEqual(len(srv.first_usernames), 2)
        self.assertEqual(len(srv.final_usernames), 1)
        self.assertEqual(len(srv.first_nonces), 2)
        self.assertNotEqual(srv.first_nonces[0], srv.first_nonces[1])
        if srv.error is not None:
            raise srv.error

    def test_retries_final_timeout_restarts_handshake(self) -> None:
        with _ScramServer(final_errors=[7]) as srv:
            with Client(
                srv.addr,
                timeout=5.0,
                scram_username=_USER,
                scram_password=_PASS,
                max_retries=2,
                retry_backoff_ms=0,
            ) as c:
                c.metadata()
        self.assertGreaterEqual(len(srv.first_usernames), 2)
        self.assertEqual(len(srv.final_usernames), 2)
        self.assertEqual(len(srv.first_nonces), len(srv.first_usernames))
        self.assertNotEqual(srv.first_nonces[0], srv.first_nonces[1])
        if srv.error is not None:
            raise srv.error

    def test_auth_failed_on_first_is_not_retried(self) -> None:
        with _ScramServer(first_errors=[17]) as srv:
            with self.assertRaises(BrokerError) as cm:
                Client(
                    srv.addr,
                    timeout=5.0,
                    scram_username=_USER,
                    scram_password=_PASS,
                    max_retries=2,
                    retry_backoff_ms=0,
                )
        self.assertEqual(cm.exception.code, 17)
        self.assertEqual(cm.exception.op, "scram first")
        self.assertEqual(len(srv.first_usernames), 1)
        self.assertEqual(len(srv.final_usernames), 0)


if __name__ == "__main__":
    unittest.main()
