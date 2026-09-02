"""TLS wrap + handshake tests (no live volant-server).

Ephemeral certs are generated in-process via the ``cryptography`` package
when installed, else ``openssl`` on PATH. Skip if neither is available.

Live broker TLS (``volant-server --tls-*``) is gated on ``VOLANT_E2E=1``.
"""

from __future__ import annotations

import os
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Optional

from volant import Client
from volant.client import wrap_tls
from volant.codec import (
    OP_METADATA,
    BrokerInfo,
    MetadataResponse,
    encode_metadata_response,
)
from volant.frame import encode_frame, try_decode_frame


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _e2e_enabled() -> bool:
    return os.environ.get("VOLANT_E2E") == "1"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_port(host: str, port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except OSError as e:
            last = e
            time.sleep(0.05)
    raise TimeoutError(f"did not listen on {host}:{port}: {last}")


def _openssl(*args: str) -> None:
    subprocess.run(
        ["openssl", *args],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _try_cryptography(directory: Path) -> bool:
    try:
        import datetime

        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError:
        return False

    def _key() -> rsa.RSAPrivateKey:
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)

    def _name(cn: str) -> x509.Name:
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])

    def _write_key(path: Path, key: rsa.RSAPrivateKey) -> None:
        path.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )

    def _write_cert(path: Path, cert: x509.Certificate) -> None:
        path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))

    now = datetime.datetime.now(datetime.timezone.utc)
    until = now + datetime.timedelta(days=1)

    ca_key = _key()
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(_name("volant-test-ca"))
        .issuer_name(_name("volant-test-ca"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(until)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    _write_key(directory / "ca.key", ca_key)
    _write_cert(directory / "ca.crt", ca_cert)

    server_key = _key()
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(_name("localhost"))
        .issuer_name(ca_cert.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(until)
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(__import__("ipaddress").IPv4Address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    _write_key(directory / "server.key", server_key)
    _write_cert(directory / "server.crt", server_cert)

    client_key = _key()
    client_cert = (
        x509.CertificateBuilder()
        .subject_name(_name("volant-test-client"))
        .issuer_name(ca_cert.subject)
        .public_key(client_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(until)
        .sign(ca_key, hashes.SHA256())
    )
    _write_key(directory / "client.key", client_key)
    _write_cert(directory / "client.crt", client_cert)
    return True


def _try_openssl(directory: Path) -> bool:
    if shutil.which("openssl") is None:
        return False
    ca_key = directory / "ca.key"
    ca_crt = directory / "ca.crt"
    server_key = directory / "server.key"
    server_csr = directory / "server.csr"
    server_crt = directory / "server.crt"
    server_ext = directory / "server.ext"
    client_key = directory / "client.key"
    client_csr = directory / "client.csr"
    client_crt = directory / "client.crt"
    _openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        str(ca_key),
        "-out",
        str(ca_crt),
        "-days",
        "1",
        "-subj",
        "/CN=volant-test-ca",
    )
    _openssl(
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        str(server_key),
        "-out",
        str(server_csr),
        "-subj",
        "/CN=localhost",
    )
    server_ext.write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\n")
    _openssl(
        "x509",
        "-req",
        "-in",
        str(server_csr),
        "-CA",
        str(ca_crt),
        "-CAkey",
        str(ca_key),
        "-CAcreateserial",
        "-out",
        str(server_crt),
        "-days",
        "1",
        "-extfile",
        str(server_ext),
    )
    _openssl(
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        str(client_key),
        "-out",
        str(client_csr),
        "-subj",
        "/CN=volant-test-client",
    )
    _openssl(
        "x509",
        "-req",
        "-in",
        str(client_csr),
        "-CA",
        str(ca_crt),
        "-CAkey",
        str(ca_key),
        "-CAcreateserial",
        "-out",
        str(client_crt),
        "-days",
        "1",
    )
    return True


def make_certs(directory: Path) -> None:
    """Write ca/server/client PEM material into ``directory``."""
    if _try_cryptography(directory):
        return
    if _try_openssl(directory):
        return
    raise unittest.SkipTest(
        "neither cryptography nor openssl available to generate ephemeral TLS certs"
    )


class _TlsMetadataServer:
    """One-shot TLS server that answers Metadata with a single broker."""

    def __init__(
        self,
        certfile: str,
        keyfile: str,
        *,
        cafile: Optional[str] = None,
        require_client: bool = False,
    ) -> None:
        self.certfile = certfile
        self.keyfile = keyfile
        self.cafile = cafile
        self.require_client = require_client
        self.host = "127.0.0.1"
        self.port = 0
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_TlsMetadataServer":
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
            raw, _ = self._lsock.accept()
        except OSError as e:
            self.error = e
            return
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(self.certfile, self.keyfile)
        if self.require_client:
            ctx.verify_mode = ssl.CERT_REQUIRED
            assert self.cafile is not None
            ctx.load_verify_locations(cafile=self.cafile)
        try:
            with ctx.wrap_socket(raw, server_side=True) as ssock:
                ssock.settimeout(5.0)
                buf = bytearray()
                while True:
                    frame, rest = try_decode_frame(bytes(buf))
                    if frame is not None:
                        payload = encode_metadata_response(
                            MetadataResponse(
                                brokers=[
                                    BrokerInfo(node_id=1, host="127.0.0.1", port=self.port)
                                ],
                                topics=[],
                            )
                        )
                        ssock.sendall(
                            encode_frame(OP_METADATA, frame.correlation_id, payload)
                        )
                        return
                    chunk = ssock.recv(4096)
                    if not chunk:
                        return
                    buf.extend(chunk)
        except BaseException as e:
            self.error = e


class TestTls(unittest.TestCase):
    tmp: tempfile.TemporaryDirectory[str]
    ca_crt: str
    server_crt: str
    server_key: str
    client_crt: str
    client_key: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory(prefix="volant-py-tls-")
        root = Path(cls.tmp.name)
        make_certs(root)
        cls.ca_crt = str(root / "ca.crt")
        cls.server_crt = str(root / "server.crt")
        cls.server_key = str(root / "server.key")
        cls.client_crt = str(root / "client.crt")
        cls.client_key = str(root / "client.key")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.tmp.cleanup()

    def test_plain_tcp_default_unchanged(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as lsock:
            lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            lsock.bind(("127.0.0.1", 0))
            lsock.listen(1)
            lsock.settimeout(5.0)
            port = lsock.getsockname()[1]

            def accept_once() -> None:
                conn, _ = lsock.accept()
                try:
                    buf = bytearray()
                    while True:
                        frame, rest = try_decode_frame(bytes(buf))
                        if frame is not None:
                            payload = encode_metadata_response(
                                MetadataResponse(
                                    brokers=[BrokerInfo(node_id=1, host="127.0.0.1", port=port)],
                                    topics=[],
                                )
                            )
                            conn.sendall(encode_frame(OP_METADATA, frame.correlation_id, payload))
                            return
                        chunk = conn.recv(4096)
                        if not chunk:
                            return
                        buf.extend(chunk)
                finally:
                    conn.close()

            t = threading.Thread(target=accept_once, daemon=True)
            t.start()
            with Client(f"127.0.0.1:{port}", timeout=5.0) as c:
                self.assertFalse(c.tls)
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)
            t.join(timeout=2.0)

    def test_tls_ca_metadata_roundtrip(self) -> None:
        with _TlsMetadataServer(self.server_crt, self.server_key) as srv:
            with Client(
                srv.addr, timeout=5.0, tls=True, tls_ca=self.ca_crt
            ) as c:
                self.assertTrue(c.tls)
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)
                self.assertEqual(meta.brokers[0].host, "127.0.0.1")

    def test_tls_insecure_skips_verify(self) -> None:
        with _TlsMetadataServer(self.server_crt, self.server_key) as srv:
            with Client(srv.addr, timeout=5.0, tls=True, tls_insecure=True) as c:
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)

    def test_tls_rejects_untrusted_ca(self) -> None:
        with _TlsMetadataServer(self.server_crt, self.server_key) as srv:
            with self.assertRaises(ssl.SSLError):
                Client(srv.addr, timeout=5.0, tls=True)

    def test_mtls_client_cert(self) -> None:
        with _TlsMetadataServer(
            self.server_crt,
            self.server_key,
            cafile=self.ca_crt,
            require_client=True,
        ) as srv:
            with Client(
                srv.addr,
                timeout=5.0,
                tls=True,
                tls_ca=self.ca_crt,
                tls_cert=self.client_crt,
                tls_key=self.client_key,
            ) as c:
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)

    def test_mtls_without_client_cert_fails(self) -> None:
        with _TlsMetadataServer(
            self.server_crt,
            self.server_key,
            cafile=self.ca_crt,
            require_client=True,
        ) as srv:
            with self.assertRaises(ssl.SSLError):
                Client(srv.addr, timeout=5.0, tls=True, tls_ca=self.ca_crt)

    def test_cert_and_key_must_be_paired(self) -> None:
        with socket.socket() as s:
            with self.assertRaises(ValueError):
                wrap_tls(
                    s,
                    "127.0.0.1",
                    tls_insecure=True,
                    tls_cert=self.client_crt,
                )
        with socket.socket() as s:
            with self.assertRaises(ValueError):
                wrap_tls(
                    s,
                    "127.0.0.1",
                    tls_insecure=True,
                    tls_key=self.client_key,
                )


def _find_server_bin() -> Optional[Path]:
    env = os.environ.get("VOLANT_SERVER")
    if env:
        p = Path(env)
        if p.is_file():
            return p
    root = _repo_root()
    for candidate in (
        root / "target" / "debug" / "volant-server",
        root / "target" / "release" / "volant-server",
    ):
        if candidate.is_file():
            return candidate
    return None


@unittest.skipUnless(_e2e_enabled(), "set VOLANT_E2E=1 to run live broker TLS e2e")
class TestE2ETls(unittest.TestCase):
    """Optional: ``volant-server --tls-cert --tls-key`` (needs ``--features tls``)."""

    def test_metadata_over_server_tls(self) -> None:
        binary = _find_server_bin()
        if binary is None:
            raise unittest.SkipTest(
                "volant-server not found; build with "
                "`cargo build -p volant-server --features tls`"
            )
        with tempfile.TemporaryDirectory(prefix="volant-py-tls-e2e-") as tmp:
            root = Path(tmp)
            make_certs(root)
            data_dir = root / "data"
            data_dir.mkdir()
            port = _free_port()
            addr = f"127.0.0.1:{port}"
            proc = subprocess.Popen(
                [
                    str(binary),
                    "--listen",
                    addr,
                    "--data-dir",
                    str(data_dir),
                    "--tls-cert",
                    str(root / "server.crt"),
                    "--tls-key",
                    str(root / "server.key"),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                cwd=_repo_root(),
            )
            try:
                try:
                    _wait_port("127.0.0.1", port, timeout=8.0)
                except TimeoutError:
                    _ = proc.poll()
                    err = (proc.stderr.read() if proc.stderr else b"") or b""
                    raise unittest.SkipTest(
                        "volant-server did not listen with --tls-*; "
                        "build with `cargo build -p volant-server --features tls` "
                        f"({err[-400:]!r})"
                    )
                with Client(addr, timeout=5.0, tls=True, tls_ca=str(root / "ca.crt")) as c:
                    meta = c.metadata()
                    self.assertTrue(meta.brokers)
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
