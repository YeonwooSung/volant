"""Client.timeout getter tests against a fake TCP listener."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import Client


class _AcceptOnce:
    """Accept one TCP connection and hold it until close."""

    def __init__(self) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self._lsock: Optional[socket.socket] = None
        self._conn: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_AcceptOnce":
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
        if self._conn is not None:
            try:
                self._conn.close()
            except OSError:
                pass
            self._conn = None
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
            self._conn = conn
        except OSError:
            return


class TestTimeout(unittest.TestCase):
    def test_explicit_timeout(self) -> None:
        with _AcceptOnce() as srv:
            with Client(srv.addr, timeout=2.5) as c:
                self.assertEqual(c.timeout, 2.5)

    def test_default_timeout(self) -> None:
        with _AcceptOnce() as srv:
            with Client(srv.addr) as c:
                self.assertEqual(c.timeout, 10.0)


if __name__ == "__main__":
    unittest.main()
