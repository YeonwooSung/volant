"""MetadataTopic client tests against a scripted TCP broker (no live server)."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import Client, MetadataResponse
from volant.codec import (
    OP_METADATA,
    decode_metadata_request,
    encode_metadata_response,
)
from volant.frame import encode_frame, try_decode_frame


class _MetadataServer:
    """Accept one connection and reply to Metadata (opcode 4)."""

    def __init__(self) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.got_topics: Optional[list[str]] = None
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_MetadataServer":
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
                if frame.opcode != OP_METADATA:
                    self.error = RuntimeError(f"unexpected opcode {frame.opcode}")
                    return
                req = decode_metadata_request(frame.payload)
                self.got_topics = list(req.topics)
                payload = encode_metadata_response(
                    MetadataResponse(brokers=[], topics=[])
                )
                conn.sendall(
                    encode_frame(OP_METADATA, frame.correlation_id, payload)
                )
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass


class TestMetadataTopicClient(unittest.TestCase):
    def test_metadata_topic_encodes_one_topic(self) -> None:
        with _MetadataServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                got = c.metadata_topic("events")
        if srv.error is not None:
            raise srv.error
        self.assertEqual(got.brokers, [])
        self.assertEqual(got.topics, [])
        self.assertEqual(srv.got_topics, ["events"])


if __name__ == "__main__":
    unittest.main()
