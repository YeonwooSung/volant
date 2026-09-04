"""FetchOffset client tests against a scripted TCP broker (no live server)."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import Client, OffsetEntry, OffsetFetchEntry
from volant.codec import (
    OP_OFFSET_FETCH,
    OffsetFetchResponse,
    decode_offset_fetch_request,
    encode_offset_fetch_response,
)
from volant.frame import encode_frame, try_decode_frame


class _OffsetFetchServer:
    """Accept one connection and reply to OffsetFetch (opcode 7)."""

    def __init__(self, *, entries: Optional[list[OffsetFetchEntry]] = None) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.entries = list(entries or ())
        self.got_group: Optional[str] = None
        self.got_entries: Optional[list[OffsetEntry]] = None
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_OffsetFetchServer":
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
                if frame.opcode != OP_OFFSET_FETCH:
                    self.error = RuntimeError(f"unexpected opcode {frame.opcode}")
                    return
                req = decode_offset_fetch_request(frame.payload)
                self.got_group = req.group_id
                self.got_entries = list(req.entries)
                payload = encode_offset_fetch_response(
                    OffsetFetchResponse(error_code=0, entries=list(self.entries))
                )
                conn.sendall(
                    encode_frame(OP_OFFSET_FETCH, frame.correlation_id, payload)
                )
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass


class TestFetchOffsetClient(unittest.TestCase):
    def test_fetch_offset_encodes_one_entry(self) -> None:
        with _OffsetFetchServer(
            entries=[OffsetFetchEntry(topic="t", partition=0, offset=5)]
        ) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                offs = c.fetch_offset("g", "t", 0)
        if srv.error is not None:
            raise srv.error
        self.assertEqual(
            offs, [OffsetFetchEntry(topic="t", partition=0, offset=5)]
        )
        self.assertEqual(srv.got_group, "g")
        self.assertEqual(srv.got_entries, [OffsetEntry(topic="t", partition=0)])


if __name__ == "__main__":
    unittest.main()
