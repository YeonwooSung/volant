"""ListOffsets client tests against a scripted TCP broker (no live server)."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import BrokerError, Client, OffsetListing
from volant.codec import (
    OP_ERROR,
    OP_LIST_OFFSETS,
    OP_LIST_OFFSETS_RESPONSE,
    OP_METADATA,
    BrokerInfo,
    ErrorResponse,
    ListOffsetsResponse,
    MetadataResponse,
    OffsetListing as WireListing,
    PartitionInfo,
    TopicInfo,
    decode_list_offsets_request,
    encode_error_response,
    encode_list_offsets_response,
    encode_metadata_response,
)
from volant.frame import encode_frame, try_decode_frame

NOT_LEADER = 13


class _ListOffsetsServer:
    """Accept connections and reply to ListOffsets / Metadata."""

    def __init__(
        self,
        *,
        error_code: int = 0,
        entries: Optional[list[WireListing]] = None,
        error_codes: Optional[list[int]] = None,
        error_as_opcode: bool = False,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.error_codes: list[int] = (
            list(error_codes) if error_codes is not None else [error_code]
        )
        self.entries = entries if entries is not None else [
            WireListing(partition=0, earliest=0, latest=10)
        ]
        self.error_as_opcode = error_as_opcode
        self.metadata: Optional[MetadataResponse] = None
        self.got_topic: Optional[str] = None
        self.got_partitions: Optional[list[int]] = None
        self.seen_partitions: list[list[int]] = []
        self.opcodes: list[int] = []
        self.list_count = 0
        self.metadata_count = 0
        self.accept_count = 0
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_ListOffsetsServer":
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind((self.host, 0))
        lsock.listen(8)
        lsock.settimeout(5.0)
        self._lsock = lsock
        self.port = lsock.getsockname()[1]
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
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

    def _accept_loop(self) -> None:
        assert self._lsock is not None
        while True:
            try:
                conn, _ = self._lsock.accept()
            except OSError:
                return
            with self._lock:
                self.accept_count += 1
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
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
                payload, reply_op = self._handle(frame.opcode, frame.payload)
                conn.sendall(encode_frame(reply_op, frame.correlation_id, payload))
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _handle(self, opcode: int, raw: bytes) -> tuple[bytes, int]:
        with self._lock:
            self.opcodes.append(opcode)
            if opcode == OP_LIST_OFFSETS:
                self.list_count += 1
                req = decode_list_offsets_request(raw)
                self.got_topic = req.topic
                self.got_partitions = list(req.partitions)
                self.seen_partitions.append(list(req.partitions))
                code = self.error_codes.pop(0) if self.error_codes else 0
                if self.error_as_opcode and code != 0:
                    return (
                        encode_error_response(
                            ErrorResponse(code=code, message="")
                        ),
                        OP_ERROR,
                    )
                return (
                    encode_list_offsets_response(
                        ListOffsetsResponse(
                            error_code=code,
                            topic=req.topic,
                            entries=self.entries if code == 0 else [],
                        )
                    ),
                    OP_LIST_OFFSETS_RESPONSE,
                )
            if opcode == OP_METADATA:
                self.metadata_count += 1
                meta = self.metadata
                if meta is None:
                    meta = MetadataResponse(brokers=[], topics=[])
                return encode_metadata_response(meta), OP_METADATA
            raise RuntimeError(f"unexpected opcode {opcode}")


def _leader_meta(
    topic: str, partition: int, leader_id: int, host: str, port: int
) -> MetadataResponse:
    return MetadataResponse(
        brokers=[
            BrokerInfo(node_id=1, host="127.0.0.1", port=1),
            BrokerInfo(node_id=leader_id, host=host, port=port),
        ],
        topics=[
            TopicInfo(
                name=topic,
                topic_id=1,
                error_code=0,
                partitions=[
                    PartitionInfo(
                        partition_id=partition,
                        leader=leader_id,
                        hwm=0,
                        replicas=[1, leader_id],
                        isr=[leader_id],
                        leader_epoch=1,
                    )
                ],
            )
        ],
    )


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

    def test_list_offsets_all_records_empty_partitions(self) -> None:
        entries = [
            WireListing(partition=0, earliest=0, latest=10),
            WireListing(partition=1, earliest=2, latest=5),
        ]
        with _ListOffsetsServer(entries=entries) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                got = c.list_offsets_all("events")
                same = c.list_offsets("events")
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
        self.assertEqual(same, got)

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

    def test_error_13_redirects_to_leader(self) -> None:
        entries = [WireListing(partition=0, earliest=0, latest=10)]
        with _ListOffsetsServer(entries=entries) as leader, _ListOffsetsServer(
            error_code=13, error_as_opcode=True
        ) as follower:
            follower.metadata = _leader_meta("events", 0, 2, "127.0.0.1", leader.port)
            with Client(follower.addr, timeout=5.0) as c:
                got = c.list_offsets("events")
            self.assertEqual(
                got, [OffsetListing(partition=0, earliest=0, latest=10)]
            )
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.list_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.list_count, 1)
        self.assertEqual(leader.got_topic, "events")
        self.assertEqual(leader.got_partitions, [])

    def test_typed_error_13_redirects_to_leader(self) -> None:
        entries = [WireListing(partition=0, earliest=0, latest=10)]
        with _ListOffsetsServer(entries=entries) as leader, _ListOffsetsServer(
            error_code=13
        ) as follower:
            follower.metadata = _leader_meta("events", 0, 2, "127.0.0.1", leader.port)
            with Client(follower.addr, timeout=5.0) as c:
                got = c.list_offsets("events")
            self.assertEqual(
                got, [OffsetListing(partition=0, earliest=0, latest=10)]
            )
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.list_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.list_count, 1)
        self.assertEqual(follower.opcodes, [OP_LIST_OFFSETS, OP_METADATA])

    def test_error_13_max_redirects_zero_raises(self) -> None:
        with _ListOffsetsServer(error_code=13) as srv:
            with Client(srv.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.list_offsets("events")
        self.assertEqual(ctx.exception.code, NOT_LEADER)
        self.assertEqual(ctx.exception.op, "list_offsets")
        self.assertEqual(srv.list_count, 1)
        self.assertEqual(srv.metadata_count, 0)
        self.assertEqual(srv.opcodes, [OP_LIST_OFFSETS])

    def test_retries_timeout_then_ok_no_metadata(self) -> None:
        entries = [WireListing(partition=0, earliest=0, latest=10)]
        with _ListOffsetsServer(error_codes=[7, 0], entries=entries) as srv:
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                got = c.list_offsets("events")
            self.assertEqual(
                got, [OffsetListing(partition=0, earliest=0, latest=10)]
            )
        self.assertEqual(srv.list_count, 2)
        self.assertEqual(srv.metadata_count, 0)
        self.assertEqual(srv.opcodes, [OP_LIST_OFFSETS, OP_LIST_OFFSETS])

    def test_not_found_not_retried(self) -> None:
        with _ListOffsetsServer(error_code=2) as srv:
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.list_offsets("missing")
            self.assertEqual(ctx.exception.code, 2)
            self.assertEqual(ctx.exception.op, "list_offsets")
        self.assertEqual(srv.list_count, 1)
        self.assertEqual(srv.metadata_count, 0)
        self.assertEqual(srv.opcodes, [OP_LIST_OFFSETS])


if __name__ == "__main__":
    unittest.main()
