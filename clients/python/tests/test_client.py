"""Leader-redirect tests against a scripted TCP broker (no live volant-server)."""

from __future__ import annotations

import socket
import threading
import unittest

from volant import BrokerError, Client, OffsetCommitEntry, OffsetEntry, OffsetFetchEntry
from volant.codec import (
    OP_DELETE_OFFSETS,
    OP_DELETE_OFFSETS_RESPONSE,
    OP_DESCRIBE_GROUP,
    OP_DESCRIBE_GROUP_RESPONSE,
    OP_ERROR,
    OP_FETCH,
    OP_HEARTBEAT,
    OP_INIT_PRODUCER_ID,
    OP_LEAVE_GROUP,
    OP_INIT_PRODUCER_ID_RESPONSE,
    OP_LIST_GROUPS,
    OP_LIST_GROUPS_RESPONSE,
    OP_LIST_MEMBERS,
    OP_LIST_MEMBERS_RESPONSE,
    OP_LIST_OFFSETS,
    OP_LIST_OFFSETS_RESPONSE,
    OP_METADATA,
    OP_OFFSET_COMMIT,
    OP_OFFSET_FETCH,
    OP_PRODUCE,
    BrokerInfo,
    DeleteOffsetsResponse,
    DescribeGroupResponse,
    ErrorResponse,
    FetchResponse,
    HeartbeatResponse,
    InitProducerIdResponse,
    LeaveGroupResponse,
    ListGroupsResponse,
    ListMembersResponse,
    ListOffsetsResponse,
    MetadataResponse,
    OffsetCommitRequest,
    OffsetCommitResponse,
    OffsetFetchRequest,
    OffsetFetchResponse,
    PartitionInfo,
    ProduceRequest,
    ProduceResponse,
    TopicInfo,
    decode_fetch_request,
    decode_init_producer_id_request,
    decode_offset_commit_request,
    decode_offset_fetch_request,
    decode_produce_request,
    encode_delete_offsets_response,
    encode_describe_group_response,
    encode_error_response,
    encode_fetch_response,
    encode_heartbeat_response,
    encode_init_producer_id_response,
    encode_leave_group_response,
    encode_list_groups_response,
    encode_list_members_response,
    encode_list_offsets_response,
    encode_metadata_response,
    encode_offset_commit_response,
    encode_offset_fetch_response,
    encode_produce_response,
)
from volant.frame import encode_frame, try_decode_frame

NOT_LEADER = 13
NOT_CONTROLLER = 14


class ScriptedBroker:
    """Accepts connections and replies to Produce / Fetch / Metadata.

    ``produce_codes`` / ``fetch_codes`` / ``heartbeat_codes`` /
    ``leave_group_codes`` / ``offset_commit_codes`` / ``offset_fetch_codes`` /
    ``delete_offsets_codes`` / ``list_offsets_codes`` /
    ``describe_group_codes`` / ``list_groups_codes`` /
    ``metadata_codes`` / ``list_members_codes`` / ``init_codes`` are queues of
    error_code values consumed across connections. ``heartbeat_replies`` /
    ``describe_group_replies`` / ``list_groups_replies`` are
    ``(code, message, as_error_opcode)`` (Error opcode when the flag is
    set). Metadata is a fixed response (or a callable of
    ``() -> MetadataResponse``). Non-zero ``metadata_codes`` reply as
    Error opcode (native Metadata has no top-level error_code).
    """

    def __init__(self) -> None:
        self.produce_codes: list[int] = []
        self.init_codes: list[int] = []
        self.fetch_codes: list[int] = []
        self.heartbeat_codes: list[int] = []
        self.heartbeat_replies: list[tuple[int, str, bool]] = []
        self.leave_group_codes: list[int] = []
        self.offset_commit_codes: list[int] = []
        self.offset_fetch_codes: list[int] = []
        self.offset_fetch_entries: list[OffsetFetchEntry] = []
        self.delete_offsets_codes: list[int] = []
        self.list_offsets_codes: list[int] = []
        self.describe_group_codes: list[int] = []
        self.describe_group_replies: list[tuple[int, str, bool]] = []
        self.list_groups_codes: list[int] = []
        self.list_groups_replies: list[tuple[int, str, bool]] = []
        self.metadata_codes: list[int] = []
        self.list_members_codes: list[int] = []
        # (code, message, as_error_opcode). Takes precedence over codes.
        self.list_members_replies: list[tuple[int, str, bool]] = []
        self.metadata: MetadataResponse | None = None
        self.opcodes: list[int] = []
        self.produce_reqs: list[ProduceRequest] = []
        self.offset_commit_reqs: list[OffsetCommitRequest] = []
        self.offset_fetch_reqs: list[OffsetFetchRequest] = []
        self.init_txn_ids: list[str] = []
        self.init_count = 0
        self.produce_count = 0
        self.fetch_count = 0
        self.heartbeat_count = 0
        self.leave_group_count = 0
        self.offset_commit_count = 0
        self.offset_fetch_count = 0
        self.delete_offsets_count = 0
        self.list_offsets_count = 0
        self.describe_group_count = 0
        self.list_groups_count = 0
        self.list_members_count = 0
        self.metadata_count = 0
        self.accept_count = 0
        self.init_pid = 42
        self.init_epoch = 1
        self.error: BaseException | None = None
        self._lsock: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self.port = 0

    @property
    def addr(self) -> str:
        return f"127.0.0.1:{self.port}"

    def start(self) -> ScriptedBroker:
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(("127.0.0.1", 0))
        lsock.listen(8)
        lsock.settimeout(5.0)
        self._lsock = lsock
        self.port = lsock.getsockname()[1]
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._lsock is not None:
            try:
                self._lsock.close()
            except OSError:
                pass
            self._lsock = None
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def __enter__(self) -> ScriptedBroker:
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    def _accept_loop(self) -> None:
        assert self._lsock is not None
        while True:
            try:
                conn, _ = self._lsock.accept()
            except OSError:
                return
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
                self.opcodes.append(frame.opcode)
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
        if opcode == OP_INIT_PRODUCER_ID:
            self.init_count += 1
            req = decode_init_producer_id_request(raw)
            self.init_txn_ids.append(req.transactional_id)
            code = self.init_codes.pop(0) if self.init_codes else 0
            return (
                encode_init_producer_id_response(
                    InitProducerIdResponse(
                        producer_id=self.init_pid,
                        epoch=self.init_epoch,
                        error_code=code,
                    )
                ),
                OP_INIT_PRODUCER_ID_RESPONSE,
            )
        if opcode == OP_PRODUCE:
            self.produce_count += 1
            req = decode_produce_request(raw)
            self.produce_reqs.append(req)
            code = self.produce_codes.pop(0) if self.produce_codes else 0
            return (
                encode_produce_response(
                    ProduceResponse(
                        topic=req.topic,
                        partition=req.partition if req.partition >= 0 else 0,
                        base_offset=7 if code == 0 else 0,
                        count=len(req.messages) if code == 0 else 0,
                        error_code=code,
                    )
                ),
                OP_PRODUCE,
            )
        if opcode == OP_FETCH:
            self.fetch_count += 1
            req = decode_fetch_request(raw)
            code = self.fetch_codes.pop(0) if self.fetch_codes else 0
            return (
                encode_fetch_response(
                    FetchResponse(
                        topic=req.topic,
                        partition=req.partition,
                        high_watermark=0,
                        error_code=code,
                        records=[],
                    )
                ),
                OP_FETCH,
            )
        if opcode == OP_HEARTBEAT:
            self.heartbeat_count += 1
            if self.heartbeat_replies:
                code, message, as_error = self.heartbeat_replies.pop(0)
                if as_error:
                    return (
                        encode_error_response(
                            ErrorResponse(code=code, message=message)
                        ),
                        OP_ERROR,
                    )
            else:
                code = self.heartbeat_codes.pop(0) if self.heartbeat_codes else 0
            return (
                encode_heartbeat_response(HeartbeatResponse(error_code=code)),
                OP_HEARTBEAT,
            )
        if opcode == OP_LEAVE_GROUP:
            self.leave_group_count += 1
            code = self.leave_group_codes.pop(0) if self.leave_group_codes else 0
            return (
                encode_leave_group_response(LeaveGroupResponse(error_code=code)),
                OP_LEAVE_GROUP,
            )
        if opcode == OP_OFFSET_COMMIT:
            self.offset_commit_count += 1
            self.offset_commit_reqs.append(decode_offset_commit_request(raw))
            code = self.offset_commit_codes.pop(0) if self.offset_commit_codes else 0
            return (
                encode_offset_commit_response(OffsetCommitResponse(error_code=code)),
                OP_OFFSET_COMMIT,
            )
        if opcode == OP_OFFSET_FETCH:
            self.offset_fetch_count += 1
            self.offset_fetch_reqs.append(decode_offset_fetch_request(raw))
            code = self.offset_fetch_codes.pop(0) if self.offset_fetch_codes else 0
            return (
                encode_offset_fetch_response(
                    OffsetFetchResponse(
                        error_code=code, entries=list(self.offset_fetch_entries)
                    )
                ),
                OP_OFFSET_FETCH,
            )
        if opcode == OP_DELETE_OFFSETS:
            self.delete_offsets_count += 1
            code = self.delete_offsets_codes.pop(0) if self.delete_offsets_codes else 0
            return (
                encode_delete_offsets_response(
                    DeleteOffsetsResponse(error_code=code, deleted_count=0)
                ),
                OP_DELETE_OFFSETS_RESPONSE,
            )
        if opcode == OP_LIST_OFFSETS:
            self.list_offsets_count += 1
            code = self.list_offsets_codes.pop(0) if self.list_offsets_codes else 0
            return (
                encode_list_offsets_response(
                    ListOffsetsResponse(error_code=code, topic="", entries=[])
                ),
                OP_LIST_OFFSETS_RESPONSE,
            )
        if opcode == OP_DESCRIBE_GROUP:
            self.describe_group_count += 1
            if self.describe_group_replies:
                code, message, as_error = self.describe_group_replies.pop(0)
                if as_error:
                    return (
                        encode_error_response(
                            ErrorResponse(code=code, message=message)
                        ),
                        OP_ERROR,
                    )
            else:
                code = self.describe_group_codes.pop(0) if self.describe_group_codes else 0
            return (
                encode_describe_group_response(
                    DescribeGroupResponse(
                        error_code=code, group_id="", generation=0, members=[]
                    )
                ),
                OP_DESCRIBE_GROUP_RESPONSE,
            )
        if opcode == OP_LIST_GROUPS:
            self.list_groups_count += 1
            if self.list_groups_replies:
                code, message, as_error = self.list_groups_replies.pop(0)
                if as_error:
                    return (
                        encode_error_response(
                            ErrorResponse(code=code, message=message)
                        ),
                        OP_ERROR,
                    )
            else:
                code = self.list_groups_codes.pop(0) if self.list_groups_codes else 0
            return (
                encode_list_groups_response(
                    ListGroupsResponse(error_code=code, groups=[])
                ),
                OP_LIST_GROUPS_RESPONSE,
            )
        if opcode == OP_LIST_MEMBERS:
            self.list_members_count += 1
            if self.list_members_replies:
                code, message, as_error = self.list_members_replies.pop(0)
            else:
                code = self.list_members_codes.pop(0) if self.list_members_codes else 0
                message, as_error = "", False
            if as_error:
                return (
                    encode_error_response(ErrorResponse(code=code, message=message)),
                    OP_ERROR,
                )
            return (
                encode_list_members_response(
                    ListMembersResponse(
                        error_code=code, generation=0, brokers=[], live=[]
                    )
                ),
                OP_LIST_MEMBERS_RESPONSE,
            )
        if opcode == OP_METADATA:
            self.metadata_count += 1
            code = self.metadata_codes.pop(0) if self.metadata_codes else 0
            if code != 0:
                return (
                    encode_error_response(ErrorResponse(code=code, message="")),
                    OP_ERROR,
                )
            meta = self.metadata
            if callable(meta):
                meta = meta()
            if meta is None:
                meta = MetadataResponse(brokers=[], topics=[])
            return encode_metadata_response(meta), OP_METADATA
        raise ProtocolErrorForTest(f"unexpected opcode {opcode}")


class ProtocolErrorForTest(Exception):
    pass


def _controller_meta(node_id: int, host: str, port: int) -> MetadataResponse:
    return MetadataResponse(
        brokers=[
            BrokerInfo(node_id=1, host="127.0.0.1", port=1),
            BrokerInfo(node_id=node_id, host=host, port=port),
        ],
        topics=[],
    )


def _other_broker_meta(current_port: int, host: str, port: int) -> MetadataResponse:
    return MetadataResponse(
        brokers=[
            BrokerInfo(node_id=1, host="127.0.0.1", port=current_port),
            BrokerInfo(node_id=2, host=host, port=port),
        ],
        topics=[],
    )


def _leader_meta(topic: str, partition: int, leader_id: int, host: str, port: int) -> MetadataResponse:
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


class TestLeaderRedirect(unittest.TestCase):
    def test_produce_redirects_to_leader(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            follower.metadata = _leader_meta("t", 0, 2, "127.0.0.1", leader.port)
            leader.produce_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                result = c.produce("t", 0, value=b"hello")
            self.assertEqual(result.base_offset, 7)
            self.assertEqual(result.topic, "t")
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(follower.metadata_count, 1)
            self.assertEqual(leader.produce_count, 1)
            self.assertEqual(c.addr, leader.addr)

    def test_max_redirects_zero_raises_on_first_13(self) -> None:
        with ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            follower.metadata = _leader_meta("t", 0, 2, "127.0.0.1", 9)
            with Client(follower.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, NOT_LEADER)
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(follower.metadata_count, 0)
            self.assertEqual(follower.accept_count, 1)

    def test_fetch_redirects_once(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.fetch_codes = [NOT_LEADER]
            follower.metadata = _leader_meta("t", 0, 2, "127.0.0.1", leader.port)
            leader.fetch_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                batch = c.fetch("t", 0, offset=0)
            self.assertEqual(len(batch), 0)
            self.assertEqual(follower.fetch_count, 1)
            self.assertEqual(follower.metadata_count, 1)
            self.assertEqual(leader.fetch_count, 1)
            self.assertEqual(c.addr, leader.addr)

    def test_missing_leader_raises_13(self) -> None:
        with ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            # Topic/partition present but leader broker id is unknown.
            follower.metadata = MetadataResponse(
                brokers=[BrokerInfo(node_id=1, host="127.0.0.1", port=follower.port)],
                topics=[
                    TopicInfo(
                        name="t",
                        topic_id=1,
                        error_code=0,
                        partitions=[
                            PartitionInfo(
                                partition_id=0,
                                leader=99,
                                hwm=0,
                                replicas=[],
                                isr=[],
                                leader_epoch=0,
                            )
                        ],
                    )
                ],
            )
            with Client(follower.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, NOT_LEADER)
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(follower.metadata_count, 1)
            self.assertEqual(follower.accept_count, 1)
            self.assertEqual(c.addr, follower.addr)

    def test_empty_host_raises_13(self) -> None:
        with ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            follower.metadata = MetadataResponse(
                brokers=[BrokerInfo(node_id=2, host="", port=9092)],
                topics=[
                    TopicInfo(
                        name="t",
                        topic_id=1,
                        error_code=0,
                        partitions=[
                            PartitionInfo(
                                partition_id=0,
                                leader=2,
                                hwm=0,
                                replicas=[],
                                isr=[],
                                leader_epoch=0,
                            )
                        ],
                    )
                ],
            )
            with Client(follower.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"x")
            self.assertEqual(ctx.exception.code, NOT_LEADER)
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(follower.metadata_count, 1)
            self.assertEqual(follower.accept_count, 1)


class TestIdempotentProduce(unittest.TestCase):
    def test_enable_on_inits_then_sequences(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0, enable_idempotence=True) as c:
                c.produce("t", 0, value=b"a")
                c.produce("t", 0, value=b"b")
            self.assertEqual(srv.init_count, 1)
            self.assertEqual(srv.init_txn_ids, [""])
            self.assertEqual(srv.opcodes, [OP_INIT_PRODUCER_ID, OP_PRODUCE, OP_PRODUCE])
            self.assertEqual(len(srv.produce_reqs), 2)
            first, second = srv.produce_reqs
            self.assertEqual(
                (first.producer_id, first.producer_epoch, first.base_sequence),
                (42, 1, 0),
            )
            self.assertEqual(
                (second.producer_id, second.producer_epoch, second.base_sequence),
                (42, 1, 1),
            )

    def test_enable_off_no_init_default_trailer(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.produce("t", 0, value=b"a")
                c.produce("t", 0, value=b"b")
            self.assertEqual(srv.init_count, 0)
            self.assertEqual(srv.opcodes, [OP_PRODUCE, OP_PRODUCE])
            for req in srv.produce_reqs:
                self.assertEqual(
                    (req.producer_id, req.producer_epoch, req.base_sequence),
                    (0, 0, -1),
                )

    def test_batch_increments_by_message_count(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0, enable_idempotence=True) as c:
                c.produce("t", 0, messages=[b"a", b"b"])
                c.produce("t", 0, value=b"c")
            self.assertEqual(srv.produce_reqs[0].base_sequence, 0)
            self.assertEqual(srv.produce_reqs[1].base_sequence, 2)

    def test_redirect_keeps_pid_and_sequence(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            follower.metadata = _leader_meta("t", 0, 2, "127.0.0.1", leader.port)
            with Client(follower.addr, timeout=5.0, enable_idempotence=True) as c:
                c.produce("t", 0, value=b"hello")
                c.produce("t", 0, value=b"again")
            self.assertEqual(follower.init_count, 1)
            self.assertEqual(leader.init_count, 0)
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(leader.produce_count, 2)
            self.assertEqual(
                (
                    follower.produce_reqs[0].producer_id,
                    follower.produce_reqs[0].producer_epoch,
                    follower.produce_reqs[0].base_sequence,
                ),
                (42, 1, 0),
            )
            self.assertEqual(leader.produce_reqs[0].base_sequence, 0)
            self.assertEqual(leader.produce_reqs[1].base_sequence, 1)
            self.assertEqual(leader.produce_reqs[0].producer_id, 42)
            self.assertEqual(c.addr, leader.addr)


TIMEOUT = 7
UNKNOWN_PRODUCER = 21


class TestInitProducerIdRetry(unittest.TestCase):
    def test_default_max_retries_zero_raises_on_init_timeout(self) -> None:
        with ScriptedBroker() as srv:
            srv.init_codes = [TIMEOUT]
            with Client(srv.addr, timeout=5.0, enable_idempotence=True) as c:
                self.assertEqual(c.max_retries, 0)
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(ctx.exception.op, "init_producer_id")
            self.assertEqual(srv.init_count, 1)
            self.assertEqual(srv.produce_count, 0)

    def test_retries_init_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.init_codes = [TIMEOUT, 0]
            with Client(
                srv.addr,
                timeout=5.0,
                enable_idempotence=True,
                max_retries=2,
                retry_backoff_ms=0,
            ) as c:
                result = c.produce("t", 0, value=b"hello")
            self.assertEqual(result.base_offset, 7)
            self.assertEqual(srv.init_count, 2)
            self.assertEqual(srv.produce_count, 1)

    def test_init_unknown_producer_id_not_retried(self) -> None:
        with ScriptedBroker() as srv:
            srv.init_codes = [UNKNOWN_PRODUCER]
            with Client(
                srv.addr,
                timeout=5.0,
                enable_idempotence=True,
                max_retries=2,
                retry_backoff_ms=0,
            ) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, UNKNOWN_PRODUCER)
            self.assertEqual(ctx.exception.op, "init_producer_id")
            self.assertEqual(srv.init_count, 1)
            self.assertEqual(srv.produce_count, 0)

    def test_init_exhausted_retries_raises(self) -> None:
        with ScriptedBroker() as srv:
            srv.init_codes = [TIMEOUT, TIMEOUT, TIMEOUT]
            with Client(
                srv.addr,
                timeout=5.0,
                enable_idempotence=True,
                max_retries=2,
                retry_backoff_ms=0,
            ) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(ctx.exception.op, "init_producer_id")
            self.assertEqual(srv.init_count, 3)
            self.assertEqual(srv.produce_count, 0)


class TestProduceDefaultAcks(unittest.TestCase):
    def test_produce_default_acks(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                self.assertEqual(c.acks, 1)
                c.produce("t", 0, value=b"hello")
            self.assertEqual(len(srv.produce_reqs), 1)
            self.assertEqual(srv.produce_reqs[0].acks, 1)

    def test_produce_set_acks_all(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.acks = 255
                c.produce("t", 0, value=b"hello")
            self.assertEqual(len(srv.produce_reqs), 1)
            self.assertEqual(srv.produce_reqs[0].acks, 255)

    def test_produce_constructor_acks(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0, acks=255) as c:
                self.assertEqual(c.acks, 255)
                c.produce("t", 0, value=b"hello")
            self.assertEqual(len(srv.produce_reqs), 1)
            self.assertEqual(srv.produce_reqs[0].acks, 255)

    def test_produce_explicit_acks_wins(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0, acks=255) as c:
                c.produce("t", 0, value=b"hello", acks=1)
            self.assertEqual(len(srv.produce_reqs), 1)
            self.assertEqual(srv.produce_reqs[0].acks, 1)


class TestProduceRetry(unittest.TestCase):
    def test_default_max_retries_zero_raises_on_timeout(self) -> None:
        with ScriptedBroker() as srv:
            srv.produce_codes = [TIMEOUT]
            with Client(srv.addr, timeout=5.0) as c:
                self.assertEqual(c.max_retries, 0)
                self.assertEqual(c.retry_backoff_ms, 50)
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.produce_count, 1)

    def test_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.produce_codes = [TIMEOUT, TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                result = c.produce("t", 0, value=b"hello")
            self.assertEqual(result.base_offset, 7)
            self.assertEqual(srv.produce_count, 3)

    def test_exhausted_retries_raises(self) -> None:
        with ScriptedBroker() as srv:
            srv.produce_codes = [TIMEOUT, TIMEOUT, TIMEOUT]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.produce_count, 3)

    def test_error_13_does_not_consume_retries(self) -> None:
        with ScriptedBroker() as follower:
            follower.produce_codes = [NOT_LEADER]
            follower.metadata = _leader_meta("t", 0, 2, "127.0.0.1", 9)
            with Client(
                follower.addr,
                timeout=5.0,
                max_redirects=0,
                max_retries=2,
                retry_backoff_ms=0,
            ) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.produce("t", 0, value=b"hello")
            self.assertEqual(ctx.exception.code, NOT_LEADER)
            self.assertEqual(follower.produce_count, 1)
            self.assertEqual(follower.metadata_count, 0)

    def test_failed_retries_do_not_increment_sequence(self) -> None:
        with ScriptedBroker() as srv:
            srv.produce_codes = [0, TIMEOUT, TIMEOUT, TIMEOUT, 0]
            with Client(
                srv.addr,
                timeout=5.0,
                enable_idempotence=True,
                max_retries=2,
                retry_backoff_ms=0,
            ) as c:
                c.produce("t", 0, value=b"a")
                with self.assertRaises(BrokerError):
                    c.produce("t", 0, value=b"b")
                c.produce("t", 0, value=b"c")
            seqs = [r.base_sequence for r in srv.produce_reqs]
            self.assertEqual(seqs, [0, 1, 1, 1, 1])


class TestFetchRetry(unittest.TestCase):
    def test_default_max_retries_zero_raises_on_timeout(self) -> None:
        with ScriptedBroker() as srv:
            srv.fetch_codes = [TIMEOUT]
            with Client(srv.addr, timeout=5.0) as c:
                self.assertEqual(c.max_retries, 0)
                with self.assertRaises(BrokerError) as ctx:
                    c.fetch("t", 0, offset=0)
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.fetch_count, 1)

    def test_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.fetch_codes = [TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                batch = c.fetch("t", 0, offset=0)
            self.assertEqual(len(batch), 0)
            self.assertEqual(srv.fetch_count, 2)

    def test_error_13_still_redirects_not_retry(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.fetch_codes = [NOT_LEADER]
            follower.metadata = _leader_meta("t", 0, 2, "127.0.0.1", leader.port)
            leader.fetch_codes = [0]
            with Client(
                follower.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0
            ) as c:
                batch = c.fetch("t", 0, offset=0)
            self.assertEqual(len(batch), 0)
            self.assertEqual(follower.fetch_count, 1)
            self.assertEqual(follower.metadata_count, 1)
            self.assertEqual(leader.fetch_count, 1)
            self.assertEqual(c.addr, leader.addr)

    def test_transport_fail_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            orig = Client._round_trip
            calls = {"n": 0}

            def flaky(self, opcode, payload):  # type: ignore[no-untyped-def]
                if opcode == OP_FETCH:
                    calls["n"] += 1
                    if calls["n"] == 1:
                        raise OSError("injected transport")
                return orig(self, opcode, payload)

            with Client(srv.addr, timeout=5.0, max_retries=1, retry_backoff_ms=0) as c:
                Client._round_trip = flaky  # type: ignore[method-assign]
                try:
                    batch = c.fetch("t", 0, offset=0)
                finally:
                    Client._round_trip = orig  # type: ignore[method-assign]
            self.assertEqual(len(batch), 0)
            self.assertEqual(srv.fetch_count, 1)
            self.assertEqual(calls["n"], 2)

    def test_exhausted_retries_raises(self) -> None:
        with ScriptedBroker() as srv:
            srv.fetch_codes = [TIMEOUT, TIMEOUT, TIMEOUT]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.fetch("t", 0, offset=0)
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.fetch_count, 3)


REBALANCE = 9


class TestHeartbeatRetry(unittest.TestCase):
    def test_default_max_retries_zero_raises_on_timeout(self) -> None:
        with ScriptedBroker() as srv:
            srv.heartbeat_codes = [TIMEOUT]
            with Client(srv.addr, timeout=5.0) as c:
                self.assertEqual(c.max_retries, 0)
                with self.assertRaises(BrokerError) as ctx:
                    c.heartbeat("g", "m1", 1)
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.heartbeat_count, 1)

    def test_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.heartbeat_codes = [TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                code = c.heartbeat("g", "m1", 1)
            self.assertEqual(code, 0)
            self.assertEqual(srv.heartbeat_count, 2)
            self.assertEqual(srv.metadata_count, 0)

    def test_rebalance_is_not_retried(self) -> None:
        with ScriptedBroker() as srv:
            srv.heartbeat_codes = [REBALANCE, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.heartbeat("g", "m1", 1)
            self.assertEqual(ctx.exception.code, REBALANCE)
            self.assertEqual(srv.heartbeat_count, 1)

    def test_transport_fail_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            orig = Client._round_trip
            calls = {"n": 0}

            def flaky(self, opcode, payload):  # type: ignore[no-untyped-def]
                if opcode == OP_HEARTBEAT:
                    calls["n"] += 1
                    if calls["n"] == 1:
                        raise OSError("injected transport")
                return orig(self, opcode, payload)

            with Client(srv.addr, timeout=5.0, max_retries=1, retry_backoff_ms=0) as c:
                Client._round_trip = flaky  # type: ignore[method-assign]
                try:
                    code = c.heartbeat("g", "m1", 1)
                finally:
                    Client._round_trip = orig  # type: ignore[method-assign]
            self.assertEqual(code, 0)
            self.assertEqual(srv.heartbeat_count, 1)
            self.assertEqual(calls["n"], 2)

    def test_exhausted_retries_raises(self) -> None:
        with ScriptedBroker() as srv:
            srv.heartbeat_codes = [TIMEOUT, TIMEOUT, TIMEOUT]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.heartbeat("g", "m1", 1)
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.heartbeat_count, 3)

    def test_heartbeat_error_14_redirects_via_controller_id(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.heartbeat_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", leader.port)
            leader.heartbeat_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                code = c.heartbeat("g", "m1", 1)
            self.assertEqual(code, 0)
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.heartbeat_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.heartbeat_count, 1)

    def test_heartbeat_typed_14_no_hint_then_ok(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.heartbeat_codes = [NOT_CONTROLLER]
            follower.metadata = _other_broker_meta(
                follower.port, "127.0.0.1", leader.port
            )
            leader.heartbeat_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                code = c.heartbeat("g", "m1", 1)
            self.assertEqual(code, 0)
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.heartbeat_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.heartbeat_count, 1)

    def test_heartbeat_max_redirects_zero_raises_on_first_14(self) -> None:
        with ScriptedBroker() as follower:
            follower.heartbeat_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", 9)
            with Client(follower.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.heartbeat("g", "m1", 1)
            self.assertEqual(ctx.exception.code, NOT_CONTROLLER)
        self.assertEqual(follower.heartbeat_count, 1)
        self.assertEqual(follower.metadata_count, 0)
        self.assertEqual(follower.accept_count, 1)


UNKNOWN_MEMBER = 10


class TestLeaveGroupRetry(unittest.TestCase):
    def test_default_max_retries_zero_raises_on_timeout(self) -> None:
        with ScriptedBroker() as srv:
            srv.leave_group_codes = [TIMEOUT]
            with Client(srv.addr, timeout=5.0) as c:
                self.assertEqual(c.max_retries, 0)
                with self.assertRaises(BrokerError) as ctx:
                    c.leave_group("g", "m1")
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.leave_group_count, 1)

    def test_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.leave_group_codes = [TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                c.leave_group("g", "m1")
            self.assertEqual(srv.leave_group_count, 2)

    def test_unknown_member_is_success(self) -> None:
        with ScriptedBroker() as srv:
            srv.leave_group_codes = [UNKNOWN_MEMBER]
            with Client(srv.addr, timeout=5.0) as c:
                c.leave_group("g", "m1")
            self.assertEqual(srv.leave_group_count, 1)

    def test_retries_timeout_then_unknown_member(self) -> None:
        with ScriptedBroker() as srv:
            srv.leave_group_codes = [TIMEOUT, UNKNOWN_MEMBER]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                c.leave_group("g", "m1")
            self.assertEqual(srv.leave_group_count, 2)

    def test_rebalance_is_not_retried(self) -> None:
        with ScriptedBroker() as srv:
            srv.leave_group_codes = [REBALANCE, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.leave_group("g", "m1")
            self.assertEqual(ctx.exception.code, REBALANCE)
            self.assertEqual(srv.leave_group_count, 1)


NOT_FOUND = 2


class TestOffsetAdminRetry(unittest.TestCase):
    def test_default_max_retries_zero_raises_on_timeout(self) -> None:
        with ScriptedBroker() as srv:
            srv.offset_commit_codes = [TIMEOUT]
            with Client(srv.addr, timeout=5.0) as c:
                self.assertEqual(c.max_retries, 0)
                with self.assertRaises(BrokerError) as ctx:
                    c.offset_commit("g", "t", 0, 5)
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.offset_commit_count, 1)

    def test_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.offset_commit_codes = [TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                c.offset_commit("g", "t", 0, 5)
            self.assertEqual(srv.offset_commit_count, 2)

    def test_offset_fetch_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.offset_fetch_codes = [TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                offs = c.offset_fetch("g", "t")
            self.assertEqual(offs, [])
            self.assertEqual(srv.offset_fetch_count, 2)

    def test_offset_fetch_all_two_topics(self) -> None:
        with ScriptedBroker() as srv:
            srv.offset_fetch_entries = [
                OffsetFetchEntry(topic="t", partition=0, offset=5),
                OffsetFetchEntry(topic="u", partition=1, offset=9),
            ]
            with Client(srv.addr, timeout=5.0) as c:
                offs = c.offset_fetch_all("g")
            self.assertEqual(offs, [("t", 0, 5), ("u", 1, 9)])
            self.assertEqual(srv.offset_fetch_count, 1)

    def test_offset_fetch_still_filters_topic(self) -> None:
        with ScriptedBroker() as srv:
            srv.offset_fetch_entries = [
                OffsetFetchEntry(topic="t", partition=0, offset=5),
                OffsetFetchEntry(topic="u", partition=1, offset=9),
            ]
            with Client(srv.addr, timeout=5.0) as c:
                offs = c.offset_fetch("g", "t")
            self.assertEqual(offs, [(0, 5)])
            self.assertEqual(srv.offset_fetch_count, 1)

    def test_delete_offsets_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.delete_offsets_codes = [TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                deleted = c.delete_offsets("g")
            self.assertEqual(deleted, 0)
            self.assertEqual(srv.delete_offsets_count, 2)

    def test_not_found_is_not_retried(self) -> None:
        with ScriptedBroker() as srv:
            srv.offset_commit_codes = [NOT_FOUND, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.offset_commit("g", "t", 0, 5)
            self.assertEqual(ctx.exception.code, NOT_FOUND)
            self.assertEqual(srv.offset_commit_count, 1)

    def test_exhausted_retries_raises(self) -> None:
        with ScriptedBroker() as srv:
            srv.offset_commit_codes = [TIMEOUT, TIMEOUT, TIMEOUT]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.offset_commit("g", "t", 0, 5)
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.offset_commit_count, 3)


class TestFetchOffsetsEntries(unittest.TestCase):
    def test_fetch_offsets_encodes_specific_entries(self) -> None:
        with ScriptedBroker() as srv:
            srv.offset_fetch_entries = [
                OffsetFetchEntry(topic="t", partition=0, offset=5),
            ]
            with Client(srv.addr, timeout=5.0) as c:
                offs = c.fetch_offsets("g", [("t", 0)])
            self.assertEqual(
                offs, [OffsetFetchEntry(topic="t", partition=0, offset=5)]
            )
            self.assertEqual(srv.offset_fetch_count, 1)
            req = srv.offset_fetch_reqs[0]
            self.assertEqual(req.group_id, "g")
            self.assertEqual(req.entries, [OffsetEntry(topic="t", partition=0)])

    def test_fetch_offsets_none_or_empty_sends_all(self) -> None:
        with ScriptedBroker() as srv:
            srv.offset_fetch_entries = [
                OffsetFetchEntry(topic="t", partition=0, offset=5),
                OffsetFetchEntry(topic="u", partition=1, offset=9),
            ]
            with Client(srv.addr, timeout=5.0) as c:
                none_offs = c.fetch_offsets("g")
                empty_offs = c.fetch_offsets("g", [])
            self.assertEqual(
                none_offs,
                [
                    OffsetFetchEntry(topic="t", partition=0, offset=5),
                    OffsetFetchEntry(topic="u", partition=1, offset=9),
                ],
            )
            self.assertEqual(empty_offs, none_offs)
            self.assertEqual(srv.offset_fetch_count, 2)
            self.assertEqual(srv.offset_fetch_reqs[0].entries, [])
            self.assertEqual(srv.offset_fetch_reqs[1].entries, [])

    def test_offset_fetch_still_filters_topic(self) -> None:
        with ScriptedBroker() as srv:
            srv.offset_fetch_entries = [
                OffsetFetchEntry(topic="t", partition=0, offset=5),
                OffsetFetchEntry(topic="u", partition=1, offset=9),
            ]
            with Client(srv.addr, timeout=5.0) as c:
                offs = c.offset_fetch("g", "t")
            self.assertEqual(offs, [(0, 5)])
            self.assertEqual(srv.offset_fetch_count, 1)
            self.assertEqual(srv.offset_fetch_reqs[0].entries, [])

    def test_offset_fetch_all_still_works(self) -> None:
        with ScriptedBroker() as srv:
            srv.offset_fetch_entries = [
                OffsetFetchEntry(topic="t", partition=0, offset=5),
                OffsetFetchEntry(topic="u", partition=1, offset=9),
            ]
            with Client(srv.addr, timeout=5.0) as c:
                offs = c.offset_fetch_all("g")
            self.assertEqual(offs, [("t", 0, 5), ("u", 1, 9)])
            self.assertEqual(srv.offset_fetch_count, 1)
            self.assertEqual(srv.offset_fetch_reqs[0].entries, [])


class TestCommitOffsetsBatch(unittest.TestCase):
    def test_batch_of_two_entries_on_the_wire(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.commit_offsets(
                    "g",
                    [
                        OffsetCommitEntry("t", 0, 5, "m0"),
                        ("u", 1, 9, "m1"),
                    ],
                )
            self.assertEqual(srv.offset_commit_count, 1)
            req = srv.offset_commit_reqs[0]
            self.assertEqual(req.group_id, "g")
            self.assertEqual(req.member_id, "")
            self.assertEqual(req.generation, 0)
            self.assertEqual(
                req.entries,
                [
                    OffsetCommitEntry("t", 0, 5, "m0"),
                    OffsetCommitEntry("u", 1, 9, "m1"),
                ],
            )

    def test_one_entry_offset_commit_still_works(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.offset_commit("g", "t", 0, 5)
            self.assertEqual(srv.offset_commit_count, 1)
            req = srv.offset_commit_reqs[0]
            self.assertEqual(req.group_id, "g")
            self.assertEqual(req.member_id, "")
            self.assertEqual(req.generation, 0)
            self.assertEqual(
                req.entries, [OffsetCommitEntry("t", 0, 5, "")]
            )

    def test_member_id_and_generation_are_sent(self) -> None:
        with ScriptedBroker() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.commit_offsets(
                    "g",
                    [("t", 0, 5)],
                    member_id="m1",
                    generation=3,
                )
            req = srv.offset_commit_reqs[0]
            self.assertEqual(req.member_id, "m1")
            self.assertEqual(req.generation, 3)
            self.assertEqual(
                req.entries, [OffsetCommitEntry("t", 0, 5, "")]
            )


class TestListOffsetsRetry(unittest.TestCase):
    def test_default_max_retries_zero_raises_on_timeout(self) -> None:
        with ScriptedBroker() as srv:
            srv.list_offsets_codes = [TIMEOUT]
            with Client(srv.addr, timeout=5.0) as c:
                self.assertEqual(c.max_retries, 0)
                with self.assertRaises(BrokerError) as ctx:
                    c.list_offsets("t")
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.list_offsets_count, 1)

    def test_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.list_offsets_codes = [TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                got = c.list_offsets("t")
            self.assertEqual(got, [])
            self.assertEqual(srv.list_offsets_count, 2)
            self.assertEqual(srv.metadata_count, 0)

    def test_not_found_is_not_retried(self) -> None:
        with ScriptedBroker() as srv:
            srv.list_offsets_codes = [NOT_FOUND, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.list_offsets("missing")
            self.assertEqual(ctx.exception.code, NOT_FOUND)
            self.assertEqual(srv.list_offsets_count, 1)
            self.assertEqual(srv.metadata_count, 0)

    def test_exhausted_retries_raises(self) -> None:
        with ScriptedBroker() as srv:
            srv.list_offsets_codes = [TIMEOUT, TIMEOUT, TIMEOUT]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.list_offsets("t")
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.list_offsets_count, 3)


class TestDescribeListGroupsRetry(unittest.TestCase):
    def test_default_max_retries_zero_raises_on_timeout(self) -> None:
        with ScriptedBroker() as srv:
            srv.describe_group_codes = [TIMEOUT]
            with Client(srv.addr, timeout=5.0) as c:
                self.assertEqual(c.max_retries, 0)
                with self.assertRaises(BrokerError) as ctx:
                    c.describe_group("g")
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.describe_group_count, 1)

    def test_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.describe_group_codes = [TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                got = c.describe_group("g")
            self.assertEqual(got.group_id, "")
            self.assertEqual(got.members, [])
            self.assertEqual(srv.describe_group_count, 2)
            self.assertEqual(srv.metadata_count, 0)

    def test_not_found_is_not_retried(self) -> None:
        with ScriptedBroker() as srv:
            srv.describe_group_codes = [NOT_FOUND, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.describe_group("missing")
            self.assertEqual(ctx.exception.code, NOT_FOUND)
            self.assertEqual(srv.describe_group_count, 1)
            self.assertEqual(srv.metadata_count, 0)

    def test_list_groups_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.list_groups_codes = [TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                got = c.list_groups()
            self.assertEqual(got, [])
            self.assertEqual(srv.list_groups_count, 2)

    def test_exhausted_retries_raises(self) -> None:
        with ScriptedBroker() as srv:
            srv.describe_group_codes = [TIMEOUT, TIMEOUT, TIMEOUT]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.describe_group("g")
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.describe_group_count, 3)

    def test_describe_group_error_14_redirects_via_controller_id(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.describe_group_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", leader.port)
            leader.describe_group_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                got = c.describe_group("g")
            self.assertEqual(got.group_id, "")
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.describe_group_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.describe_group_count, 1)

    def test_list_groups_typed_14_no_hint_then_ok(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.list_groups_codes = [NOT_CONTROLLER]
            follower.metadata = _other_broker_meta(
                follower.port, "127.0.0.1", leader.port
            )
            leader.list_groups_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                got = c.list_groups()
            self.assertEqual(got, [])
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.list_groups_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.list_groups_count, 1)

    def test_describe_group_max_redirects_zero_raises_on_first_14(self) -> None:
        with ScriptedBroker() as follower:
            follower.describe_group_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", 9)
            with Client(follower.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.describe_group("g")
            self.assertEqual(ctx.exception.code, NOT_CONTROLLER)
        self.assertEqual(follower.describe_group_count, 1)
        self.assertEqual(follower.metadata_count, 0)
        self.assertEqual(follower.accept_count, 1)


class TestMetadataListMembersRetry(unittest.TestCase):
    def test_default_max_retries_zero_raises_on_timeout(self) -> None:
        with ScriptedBroker() as srv:
            srv.metadata_codes = [TIMEOUT]
            with Client(srv.addr, timeout=5.0) as c:
                self.assertEqual(c.max_retries, 0)
                with self.assertRaises(BrokerError) as ctx:
                    c.metadata()
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.metadata_count, 1)

    def test_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.metadata_codes = [TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                got = c.metadata()
            self.assertEqual(got.brokers, [])
            self.assertEqual(got.topics, [])
            self.assertEqual(srv.metadata_count, 2)

    def test_not_found_is_not_retried(self) -> None:
        with ScriptedBroker() as srv:
            srv.metadata_codes = [NOT_FOUND, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.metadata()
            self.assertEqual(ctx.exception.code, NOT_FOUND)
            self.assertEqual(srv.metadata_count, 1)

    def test_list_members_retries_timeout_then_ok(self) -> None:
        with ScriptedBroker() as srv:
            srv.list_members_codes = [TIMEOUT, 0]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                got = c.list_members()
            self.assertEqual(got.generation, 0)
            self.assertEqual(got.brokers, [])
            self.assertEqual(got.live, [])
            self.assertEqual(srv.list_members_count, 2)
            self.assertEqual(srv.metadata_count, 0)

    def test_exhausted_retries_raises(self) -> None:
        with ScriptedBroker() as srv:
            srv.metadata_codes = [TIMEOUT, TIMEOUT, TIMEOUT]
            with Client(srv.addr, timeout=5.0, max_retries=2, retry_backoff_ms=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.metadata()
            self.assertEqual(ctx.exception.code, TIMEOUT)
            self.assertEqual(srv.metadata_count, 3)


NOT_CONTROLLER = 14


def _controller_meta(node_id: int, host: str, port: int) -> MetadataResponse:
    return MetadataResponse(
        brokers=[
            BrokerInfo(node_id=1, host="127.0.0.1", port=1),
            BrokerInfo(node_id=node_id, host=host, port=port),
        ],
        topics=[],
    )


def _other_broker_meta(current_port: int, host: str, port: int) -> MetadataResponse:
    return MetadataResponse(
        brokers=[
            BrokerInfo(node_id=1, host="127.0.0.1", port=current_port),
            BrokerInfo(node_id=2, host=host, port=port),
        ],
        topics=[],
    )


class TestListMembersNotControllerRedirect(unittest.TestCase):
    def test_list_members_error_14_redirects_via_controller_id(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.list_members_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", leader.port)
            with Client(follower.addr, timeout=5.0) as c:
                got = c.list_members()
            self.assertEqual(got.generation, 0)
            self.assertEqual(got.brokers, [])
            self.assertEqual(got.live, [])
            self.assertEqual(c.addr, leader.addr)
            self.assertEqual(follower.list_members_count, 1)
            self.assertEqual(follower.metadata_count, 1)
            self.assertEqual(leader.list_members_count, 1)
            self.assertEqual(leader.metadata_count, 0)

    def test_list_members_typed_14_no_hint_then_ok(self) -> None:
        with ScriptedBroker() as leader, ScriptedBroker() as follower:
            follower.list_members_codes = [NOT_CONTROLLER]
            follower.metadata = _other_broker_meta(
                follower.port, "127.0.0.1", leader.port
            )
            with Client(follower.addr, timeout=5.0) as c:
                got = c.list_members()
            self.assertEqual(got.generation, 0)
            self.assertEqual(c.addr, leader.addr)
            self.assertEqual(follower.list_members_count, 1)
            self.assertEqual(follower.metadata_count, 1)
            self.assertEqual(leader.list_members_count, 1)

    def test_list_members_max_redirects_zero_raises_on_first_14(self) -> None:
        with ScriptedBroker() as follower:
            follower.list_members_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", 9)
            with Client(follower.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.list_members()
            self.assertEqual(ctx.exception.code, NOT_CONTROLLER)
            self.assertEqual(follower.list_members_count, 1)
            self.assertEqual(follower.metadata_count, 0)


if __name__ == "__main__":
    unittest.main()
