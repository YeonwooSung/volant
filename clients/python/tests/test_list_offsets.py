"""ListOffsets client tests against a scripted TCP broker (no live server)."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import BrokerError, Client, OffsetListing
from volant.codec import (
    OP_LIST_OFFSETS,
    OP_LIST_OFFSETS_RESPONSE,
    ListOffsetsResponse,
    OffsetListing as WireListing,
    decode_list_offsets_request,
    encode_list_offsets_response,
)
from volant.frame import encode_frame, try_decode_frame


class _ListOffsetsServer:
    """Accept one connection and reply to ListOffsets (opcode 48 → 49)."""

    def __init__(
        self,
        *,
        error_code: int = 0,
        entries: Optional[list[WireListing]] = None,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.error_code = error_code
        self.entries = entries if entries is not None else [
            WireListing(partition=0, earliest=0, latest=10)
        ]
        self.got_topic: Optional[str] = None
        self.got_partitions: Optional[list[int]] = None
        self.seen_partitions: list[list[int]] = []
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_ListOffsetsServer":
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
                if frame.opcode != OP_LIST_OFFSETS:
                    self.error = RuntimeError(f"unexpected opcode {frame.opcode}")
                    return
                req = decode_list_offsets_request(frame.payload)
                self.got_topic = req.topic
                self.got_partitions = list(req.partitions)
                self.seen_partitions.append(list(req.partitions))
                payload = encode_list_offsets_response(
                    ListOffsetsResponse(
                        error_code=self.error_code,
                        topic=req.topic,
                        entries=self.entries,
                    )
                )
                conn.sendall(
                    encode_frame(
                        OP_LIST_OFFSETS_RESPONSE, frame.correlation_id, payload
                    )
                )
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass


class TestListOffsetsClient(unittest.TestCase):
    def test_empty_partitions_encoded_as_count_zero(self) -> None:
        entries = [
            WireListing(partition=0, earliest=0, latest=10),
            WireListing(partition=1, earliest=2, latest=5),
        ]
        with _ListOffsetsServer(entries=entries) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                got = c.list_offsets("events")
                also = c.list_offsets("events", [])
        if srv.error is not None:
            raise srv.error
        self.assertEqual(srv.got_topic, "events")
        self.assertEqual(srv.got_partitions, [])
        self.assertEqual(srv.seen_partitions, [[], []])
        self.assertEqual(
            got,
            [
                OffsetListing(partition=0, earliest=0, latest=10),
                OffsetListing(partition=1, earliest=2, latest=5),
            ],
        )
        self.assertEqual(also, got)

    def test_explicit_partitions_roundtrip(self) -> None:
        entries = [WireListing(partition=0, earliest=0, latest=10)]
        with _ListOffsetsServer(entries=entries) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                got = c.list_offsets("events", [0, 1])
        if srv.error is not None:
            raise srv.error
        self.assertEqual(srv.got_partitions, [0, 1])
        self.assertEqual(got, [OffsetListing(partition=0, earliest=0, latest=10)])

    def test_nonzero_error_code_raises(self) -> None:
        with _ListOffsetsServer(error_code=2) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.list_offsets("missing")
        self.assertEqual(ctx.exception.code, 2)
        self.assertEqual(ctx.exception.op, "list_offsets")


if __name__ == "__main__":
    unittest.main()
