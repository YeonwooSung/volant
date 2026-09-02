"""Admin NotController (error 14) redirect tests (fake TCP, no live server)."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import AclBinding, BrokerError, Client
from volant.codec import (
    OP_ADD_BROKER,
    OP_ADD_BROKER_RESPONSE,
    OP_ALTER_CONFIGS,
    OP_ALTER_CONFIGS_RESPONSE,
    OP_CREATE_ACLS,
    OP_CREATE_ACLS_RESPONSE,
    OP_CREATE_PARTITIONS,
    OP_CREATE_PARTITIONS_RESPONSE,
    OP_CREATE_SCRAM_USER,
    OP_CREATE_SCRAM_USER_RESPONSE,
    OP_CREATE_TOPIC,
    OP_DELETE_OFFSETS,
    OP_DELETE_OFFSETS_RESPONSE,
    OP_DELETE_SCRAM_USER,
    OP_DELETE_SCRAM_USER_RESPONSE,
    OP_DESCRIBE_CONFIGS,
    OP_DESCRIBE_CONFIGS_RESPONSE,
    OP_ERROR,
    OP_LIST_ACLS,
    OP_LIST_ACLS_RESPONSE,
    OP_LIST_MEMBERS,
    OP_LIST_MEMBERS_RESPONSE,
    OP_LIST_SCRAM_USERS,
    OP_LIST_SCRAM_USERS_RESPONSE,
    OP_METADATA,
    OP_REASSIGN_PARTITIONS,
    OP_REASSIGN_PARTITIONS_RESPONSE,
    OP_REMOVE_BROKER,
    OP_REMOVE_BROKER_RESPONSE,
    AddBrokerResponse,
    AlterConfigsResponse,
    BrokerInfo,
    CreateAclsResponse,
    CreatePartitionsResponse,
    CreateScramUserResponse,
    CreateTopicResponse,
    DeleteOffsetsResponse,
    DeleteScramUserResponse,
    DescribeConfigsResponse,
    ErrorResponse,
    ListAclsResponse,
    ListMembersResponse,
    ListScramUsersResponse,
    MetadataResponse,
    ReassignPartitionsResponse,
    RemoveBrokerResponse,
    decode_alter_configs_request,
    decode_create_partitions_request,
    decode_create_topic_request,
    decode_describe_configs_request,
    encode_add_broker_response,
    encode_alter_configs_response,
    encode_create_acls_response,
    encode_create_partitions_response,
    encode_create_scram_user_response,
    encode_create_topic_response,
    encode_delete_offsets_response,
    encode_delete_scram_user_response,
    encode_describe_configs_response,
    encode_error_response,
    encode_list_acls_response,
    encode_list_members_response,
    encode_list_scram_users_response,
    encode_metadata_response,
    encode_reassign_partitions_response,
    encode_remove_broker_response,
)
from volant.frame import encode_frame, try_decode_frame

NOT_CONTROLLER = 14


class _AdminServer:
    """Accept connections and reply to controller-gated admin RPCs + Metadata."""

    def __init__(self) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        # CreateTopic: (code, message, as_error_opcode). Empty queue → success.
        self.create_topic_replies: list[tuple[int, str, bool]] = []
        self.create_partitions_codes: list[int] = []
        self.create_acls_codes: list[int] = []
        self.reassign_codes: list[int] = []
        # CreateScramUser: (code, message, as_error_opcode). Empty queue → success.
        self.create_scram_replies: list[tuple[int, str, bool]] = []
        self.delete_scram_codes: list[int] = []
        self.list_scram_codes: list[int] = []
        self.list_acls_codes: list[int] = []
        # AddBroker: (code, message, as_error_opcode). Empty queue → success.
        self.add_broker_replies: list[tuple[int, str, bool]] = []
        self.remove_broker_codes: list[int] = []
        # DescribeConfigs: (code, message, as_error_opcode). Empty queue → success.
        self.describe_configs_replies: list[tuple[int, str, bool]] = []
        self.alter_configs_codes: list[int] = []
        # DeleteOffsets: (code, message, as_error_opcode). Empty queue → success.
        self.delete_offsets_replies: list[tuple[int, str, bool]] = []
        self.delete_offsets_codes: list[int] = []
        self.metadata: Optional[MetadataResponse] = None
        self.opcodes: list[int] = []
        self.create_topic_count = 0
        self.create_partitions_count = 0
        self.create_acls_count = 0
        self.reassign_count = 0
        self.create_scram_count = 0
        self.delete_scram_count = 0
        self.list_scram_count = 0
        self.list_acls_count = 0
        self.add_broker_count = 0
        self.remove_broker_count = 0
        self.describe_configs_count = 0
        self.alter_configs_count = 0
        self.delete_offsets_count = 0
        self.metadata_count = 0
        self.list_members_count = 0
        self.accept_count = 0
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_AdminServer":
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
            if opcode == OP_CREATE_TOPIC:
                self.create_topic_count += 1
                req = decode_create_topic_request(raw)
                if self.create_topic_replies:
                    code, message, as_error = self.create_topic_replies.pop(0)
                else:
                    code, message, as_error = 0, "", False
                if as_error:
                    return (
                        encode_error_response(ErrorResponse(code=code, message=message)),
                        OP_ERROR,
                    )
                return (
                    encode_create_topic_response(
                        CreateTopicResponse(
                            topic_id=1 if code == 0 else 0,
                            name=req.name,
                            partitions=req.partitions if code == 0 else 0,
                            error_code=code,
                        )
                    ),
                    OP_CREATE_TOPIC,
                )
            if opcode == OP_CREATE_PARTITIONS:
                self.create_partitions_count += 1
                req = decode_create_partitions_request(raw)
                code = (
                    self.create_partitions_codes.pop(0)
                    if self.create_partitions_codes
                    else 0
                )
                return (
                    encode_create_partitions_response(
                        CreatePartitionsResponse(
                            error_code=code,
                            topic=req.topic,
                            partitions=0 if code else req.total_count,
                        )
                    ),
                    OP_CREATE_PARTITIONS_RESPONSE,
                )
            if opcode == OP_CREATE_ACLS:
                self.create_acls_count += 1
                code = self.create_acls_codes.pop(0) if self.create_acls_codes else 0
                return (
                    encode_create_acls_response(CreateAclsResponse(error_code=code)),
                    OP_CREATE_ACLS_RESPONSE,
                )
            if opcode == OP_REASSIGN_PARTITIONS:
                self.reassign_count += 1
                code = self.reassign_codes.pop(0) if self.reassign_codes else 0
                return (
                    encode_reassign_partitions_response(
                        ReassignPartitionsResponse(
                            error_code=code, generation=0 if code else 7
                        )
                    ),
                    OP_REASSIGN_PARTITIONS_RESPONSE,
                )
            if opcode == OP_CREATE_SCRAM_USER:
                self.create_scram_count += 1
                if self.create_scram_replies:
                    code, message, as_error = self.create_scram_replies.pop(0)
                else:
                    code, message, as_error = 0, "", False
                if as_error:
                    return (
                        encode_error_response(ErrorResponse(code=code, message=message)),
                        OP_ERROR,
                    )
                return (
                    encode_create_scram_user_response(
                        CreateScramUserResponse(error_code=code)
                    ),
                    OP_CREATE_SCRAM_USER_RESPONSE,
                )
            if opcode == OP_DELETE_SCRAM_USER:
                self.delete_scram_count += 1
                code = self.delete_scram_codes.pop(0) if self.delete_scram_codes else 0
                return (
                    encode_delete_scram_user_response(
                        DeleteScramUserResponse(error_code=code)
                    ),
                    OP_DELETE_SCRAM_USER_RESPONSE,
                )
            if opcode == OP_LIST_SCRAM_USERS:
                self.list_scram_count += 1
                code = self.list_scram_codes.pop(0) if self.list_scram_codes else 0
                return (
                    encode_list_scram_users_response(
                        ListScramUsersResponse(
                            error_code=code, usernames=[] if code else ["alice"]
                        )
                    ),
                    OP_LIST_SCRAM_USERS_RESPONSE,
                )
            if opcode == OP_LIST_ACLS:
                self.list_acls_count += 1
                code = self.list_acls_codes.pop(0) if self.list_acls_codes else 0
                return (
                    encode_list_acls_response(ListAclsResponse(error_code=code, entries=[])),
                    OP_LIST_ACLS_RESPONSE,
                )
            if opcode == OP_ADD_BROKER:
                self.add_broker_count += 1
                if self.add_broker_replies:
                    code, message, as_error = self.add_broker_replies.pop(0)
                else:
                    code, message, as_error = 0, "", False
                if as_error:
                    return (
                        encode_error_response(ErrorResponse(code=code, message=message)),
                        OP_ERROR,
                    )
                return (
                    encode_add_broker_response(
                        AddBrokerResponse(
                            error_code=code, generation=11 if code == 0 else 0
                        )
                    ),
                    OP_ADD_BROKER_RESPONSE,
                )
            if opcode == OP_REMOVE_BROKER:
                self.remove_broker_count += 1
                code = (
                    self.remove_broker_codes.pop(0) if self.remove_broker_codes else 0
                )
                return (
                    encode_remove_broker_response(
                        RemoveBrokerResponse(
                            error_code=code, generation=0 if code else 12
                        )
                    ),
                    OP_REMOVE_BROKER_RESPONSE,
                )
            if opcode == OP_DESCRIBE_CONFIGS:
                self.describe_configs_count += 1
                req = decode_describe_configs_request(raw)
                if self.describe_configs_replies:
                    code, message, as_error = self.describe_configs_replies.pop(0)
                else:
                    code, message, as_error = 0, "", False
                if as_error:
                    return (
                        encode_error_response(ErrorResponse(code=code, message=message)),
                        OP_ERROR,
                    )
                return (
                    encode_describe_configs_response(
                        DescribeConfigsResponse(
                            error_code=code,
                            topic=req.topic,
                            topic_id=1 if code == 0 else 0,
                            partition_count=1 if code == 0 else 0,
                            configs=[("retention.ms", "86400000")] if code == 0 else [],
                        )
                    ),
                    OP_DESCRIBE_CONFIGS_RESPONSE,
                )
            if opcode == OP_ALTER_CONFIGS:
                self.alter_configs_count += 1
                req = decode_alter_configs_request(raw)
                code = (
                    self.alter_configs_codes.pop(0) if self.alter_configs_codes else 0
                )
                return (
                    encode_alter_configs_response(
                        AlterConfigsResponse(error_code=code, topic=req.topic)
                    ),
                    OP_ALTER_CONFIGS_RESPONSE,
                )
            if opcode == OP_DELETE_OFFSETS:
                self.delete_offsets_count += 1
                if self.delete_offsets_replies:
                    code, message, as_error = self.delete_offsets_replies.pop(0)
                elif self.delete_offsets_codes:
                    code, message, as_error = self.delete_offsets_codes.pop(0), "", False
                else:
                    code, message, as_error = 0, "", False
                if as_error:
                    return (
                        encode_error_response(ErrorResponse(code=code, message=message)),
                        OP_ERROR,
                    )
                return (
                    encode_delete_offsets_response(
                        DeleteOffsetsResponse(
                            error_code=code, deleted_count=3 if code == 0 else 0
                        )
                    ),
                    OP_DELETE_OFFSETS_RESPONSE,
                )
            if opcode == OP_METADATA:
                self.metadata_count += 1
                meta = self.metadata
                if meta is None:
                    meta = MetadataResponse(brokers=[], topics=[])
                return encode_metadata_response(meta), OP_METADATA
            if opcode == OP_LIST_MEMBERS:
                self.list_members_count += 1
                return (
                    encode_list_members_response(
                        ListMembersResponse(
                            error_code=0, generation=0, brokers=[], live=[]
                        )
                    ),
                    OP_LIST_MEMBERS_RESPONSE,
                )
            raise RuntimeError(f"unexpected opcode {opcode}")


def _controller_meta(node_id: int, host: str, port: int) -> MetadataResponse:
    return MetadataResponse(
        brokers=[
            BrokerInfo(node_id=1, host="127.0.0.1", port=1),
            BrokerInfo(node_id=node_id, host=host, port=port),
        ],
        topics=[],
    )


def _other_broker_meta(
    current_port: int, host: str, port: int
) -> MetadataResponse:
    """Current node plus another advertised broker (no controller_id hint)."""
    return MetadataResponse(
        brokers=[
            BrokerInfo(node_id=1, host="127.0.0.1", port=current_port),
            BrokerInfo(node_id=2, host=host, port=port),
        ],
        topics=[],
    )


class TestAdminNotControllerRedirect(unittest.TestCase):
    def test_create_topic_error_14_redirects_via_controller_id(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.create_topic_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", leader.port)
            leader.create_topic_replies = [(0, "", False)]
            with Client(follower.addr, timeout=5.0) as c:
                topic_id = c.create_topic("events", partitions=1)
            self.assertEqual(topic_id, 1)
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.create_topic_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.create_topic_count, 1)
        self.assertEqual(follower.list_members_count, 0)

    def test_create_partitions_error_14_no_hint_picks_other_broker(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.create_partitions_codes = [NOT_CONTROLLER]
            follower.metadata = _other_broker_meta(
                follower.port, "127.0.0.1", leader.port
            )
            leader.create_partitions_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                got = c.create_partitions("events", 4)
            self.assertEqual(got, 4)
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.create_partitions_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.create_partitions_count, 1)

    def test_create_partitions_prefers_metadata_controller_id(self) -> None:
        with (
            _AdminServer() as controller,
            _AdminServer() as decoy,
            _AdminServer() as follower,
        ):
            follower.create_partitions_codes = [NOT_CONTROLLER]
            follower.metadata = MetadataResponse(
                brokers=[
                    BrokerInfo(node_id=1, host="127.0.0.1", port=follower.port),
                    BrokerInfo(node_id=3, host="127.0.0.1", port=decoy.port),
                    BrokerInfo(node_id=2, host="127.0.0.1", port=controller.port),
                ],
                topics=[],
                controller_id=2,
            )
            with Client(follower.addr, timeout=5.0) as c:
                got = c.create_partitions("events", 4)
            self.assertEqual(got, 4)
            self.assertEqual(c.addr, controller.addr)
        self.assertEqual(follower.create_partitions_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(controller.create_partitions_count, 1)
        self.assertEqual(decoy.create_partitions_count, 0)

    def test_create_partitions_metadata_controller_id_zero_picks_other(self) -> None:
        with (
            _AdminServer() as later,
            _AdminServer() as first_other,
            _AdminServer() as follower,
        ):
            follower.create_partitions_codes = [NOT_CONTROLLER]
            follower.metadata = MetadataResponse(
                brokers=[
                    BrokerInfo(node_id=1, host="127.0.0.1", port=follower.port),
                    BrokerInfo(node_id=3, host="127.0.0.1", port=first_other.port),
                    BrokerInfo(node_id=2, host="127.0.0.1", port=later.port),
                ],
                topics=[],
                controller_id=0,
            )
            with Client(follower.addr, timeout=5.0) as c:
                got = c.create_partitions("events", 4)
            self.assertEqual(got, 4)
            self.assertEqual(c.addr, first_other.addr)
        self.assertEqual(follower.create_partitions_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(first_other.create_partitions_count, 1)
        self.assertEqual(later.create_partitions_count, 0)

    def test_create_topic_message_controller_id_wins_over_metadata(self) -> None:
        with (
            _AdminServer() as hinted,
            _AdminServer() as meta_ctrl,
            _AdminServer() as follower,
        ):
            follower.create_topic_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=3", True)
            ]
            follower.metadata = MetadataResponse(
                brokers=[
                    BrokerInfo(node_id=1, host="127.0.0.1", port=follower.port),
                    BrokerInfo(node_id=2, host="127.0.0.1", port=meta_ctrl.port),
                    BrokerInfo(node_id=3, host="127.0.0.1", port=hinted.port),
                ],
                topics=[],
                controller_id=2,
            )
            with Client(follower.addr, timeout=5.0) as c:
                topic_id = c.create_topic("events", partitions=1)
            self.assertEqual(topic_id, 1)
            self.assertEqual(c.addr, hinted.addr)
        self.assertEqual(follower.create_topic_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(hinted.create_topic_count, 1)
        self.assertEqual(meta_ctrl.create_topic_count, 0)

    def test_max_redirects_zero_raises_on_first_14(self) -> None:
        with _AdminServer() as follower:
            follower.create_topic_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", 9)
            with Client(follower.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.create_topic("events", partitions=1)
            self.assertEqual(ctx.exception.code, NOT_CONTROLLER)
        self.assertEqual(follower.create_topic_count, 1)
        self.assertEqual(follower.metadata_count, 0)
        self.assertEqual(follower.accept_count, 1)

    def test_helper_no_other_broker_raises_14(self) -> None:
        with _AdminServer() as follower:
            follower.create_partitions_codes = [NOT_CONTROLLER]
            follower.metadata = MetadataResponse(
                brokers=[
                    BrokerInfo(node_id=1, host="127.0.0.1", port=follower.port),
                ],
                topics=[],
            )
            with Client(follower.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.create_partitions("events", 4)
            self.assertEqual(ctx.exception.code, NOT_CONTROLLER)
            self.assertEqual(c.addr, follower.addr)
        self.assertEqual(follower.create_partitions_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(follower.accept_count, 1)

    def test_helper_empty_host_raises_14(self) -> None:
        with _AdminServer() as follower:
            follower.create_partitions_codes = [NOT_CONTROLLER]
            follower.metadata = MetadataResponse(
                brokers=[BrokerInfo(node_id=2, host="", port=9092)],
                topics=[],
            )
            with Client(follower.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.create_partitions("events", 4)
            self.assertEqual(ctx.exception.code, NOT_CONTROLLER)
        self.assertEqual(follower.create_partitions_count, 1)
        self.assertEqual(follower.metadata_count, 1)

    def test_create_acls_error_14_then_ok(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.create_acls_codes = [NOT_CONTROLLER]
            follower.metadata = _other_broker_meta(
                follower.port, "127.0.0.1", leader.port
            )
            leader.create_acls_codes = [0]
            entry = AclBinding("User:alice", 0, "events", 3, 1)
            with Client(follower.addr, timeout=5.0) as c:
                c.create_acls([entry])
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.create_acls_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.create_acls_count, 1)

    def test_reassign_partitions_error_14_then_ok(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.reassign_codes = [NOT_CONTROLLER]
            follower.metadata = _other_broker_meta(
                follower.port, "127.0.0.1", leader.port
            )
            leader.reassign_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                gen = c.reassign_partitions("events", [1, 2])
            self.assertEqual(gen, 7)
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.reassign_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.reassign_count, 1)

    def test_other_error_raises_immediately(self) -> None:
        with _AdminServer() as follower:
            follower.create_partitions_codes = [2]
            follower.metadata = _other_broker_meta(follower.port, "127.0.0.1", 9)
            with Client(follower.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.create_partitions("missing", 4)
            self.assertEqual(ctx.exception.code, 2)
        self.assertEqual(follower.create_partitions_count, 1)
        self.assertEqual(follower.metadata_count, 0)

    def test_create_scram_user_error_14_redirects_via_controller_id(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.create_scram_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", leader.port)
            leader.create_scram_replies = [(0, "", False)]
            with Client(follower.addr, timeout=5.0) as c:
                c.create_scram_user("alice", "s3cret")
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.create_scram_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.create_scram_count, 1)
        self.assertEqual(follower.list_members_count, 0)

    def test_list_acls_typed_14_no_hint_then_ok(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.list_acls_codes = [NOT_CONTROLLER]
            follower.metadata = _other_broker_meta(
                follower.port, "127.0.0.1", leader.port
            )
            leader.list_acls_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                listed = c.list_acls()
            self.assertEqual(listed, [])
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.list_acls_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.list_acls_count, 1)

    def test_delete_scram_user_max_redirects_zero_raises_on_first_14(self) -> None:
        with _AdminServer() as follower:
            follower.delete_scram_codes = [NOT_CONTROLLER]
            follower.metadata = _controller_meta(2, "127.0.0.1", 9)
            with Client(follower.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.delete_scram_user("alice")
            self.assertEqual(ctx.exception.code, NOT_CONTROLLER)
        self.assertEqual(follower.delete_scram_count, 1)
        self.assertEqual(follower.metadata_count, 0)
        self.assertEqual(follower.accept_count, 1)

    def test_list_scram_users_error_14_then_ok(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.list_scram_codes = [NOT_CONTROLLER]
            follower.metadata = _other_broker_meta(
                follower.port, "127.0.0.1", leader.port
            )
            leader.list_scram_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                names = c.list_scram_users()
            self.assertEqual(names, ["alice"])
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.list_scram_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.list_scram_count, 1)

    def test_add_broker_error_14_redirects_via_controller_id(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.add_broker_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", leader.port)
            leader.add_broker_replies = [(0, "", False)]
            with Client(follower.addr, timeout=5.0) as c:
                gen = c.add_broker(3, "10.0.0.3", 9092)
            self.assertEqual(gen, 11)
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.add_broker_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.add_broker_count, 1)
        self.assertEqual(follower.list_members_count, 0)

    def test_remove_broker_typed_14_no_hint_then_ok(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.remove_broker_codes = [NOT_CONTROLLER]
            follower.metadata = _other_broker_meta(
                follower.port, "127.0.0.1", leader.port
            )
            leader.remove_broker_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                gen = c.remove_broker(3)
            self.assertEqual(gen, 12)
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.remove_broker_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.remove_broker_count, 1)

    def test_add_broker_max_redirects_zero_raises_on_first_14(self) -> None:
        with _AdminServer() as follower:
            follower.add_broker_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", 9)
            with Client(follower.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.add_broker(3, "10.0.0.3", 9092)
            self.assertEqual(ctx.exception.code, NOT_CONTROLLER)
        self.assertEqual(follower.add_broker_count, 1)
        self.assertEqual(follower.metadata_count, 0)
        self.assertEqual(follower.accept_count, 1)

    def test_describe_configs_error_14_redirects_via_controller_id(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.describe_configs_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", leader.port)
            leader.describe_configs_replies = [(0, "", False)]
            with Client(follower.addr, timeout=5.0) as c:
                got = c.describe_configs("events")
            self.assertEqual(got.topic, "events")
            self.assertEqual(got.topic_id, 1)
            self.assertEqual(got.partition_count, 1)
            self.assertEqual(got.configs, [("retention.ms", "86400000")])
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.describe_configs_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.describe_configs_count, 1)
        self.assertEqual(follower.list_members_count, 0)

    def test_alter_configs_typed_14_no_hint_then_ok(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.alter_configs_codes = [NOT_CONTROLLER]
            follower.metadata = _other_broker_meta(
                follower.port, "127.0.0.1", leader.port
            )
            leader.alter_configs_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                c.alter_configs("events", [("retention.ms", "86400000")])
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.alter_configs_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.alter_configs_count, 1)

    def test_describe_configs_max_redirects_zero_raises_on_first_14(self) -> None:
        with _AdminServer() as follower:
            follower.describe_configs_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", 9)
            with Client(follower.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.describe_configs("events")
            self.assertEqual(ctx.exception.code, NOT_CONTROLLER)
        self.assertEqual(follower.describe_configs_count, 1)
        self.assertEqual(follower.metadata_count, 0)
        self.assertEqual(follower.accept_count, 1)

    def test_delete_offsets_error_14_redirects_via_controller_id(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.delete_offsets_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", leader.port)
            leader.delete_offsets_replies = [(0, "", False)]
            with Client(follower.addr, timeout=5.0) as c:
                got = c.delete_offsets("g")
            self.assertEqual(got, 3)
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.delete_offsets_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.delete_offsets_count, 1)
        self.assertEqual(follower.list_members_count, 0)

    def test_delete_offsets_typed_14_no_hint_then_ok(self) -> None:
        with _AdminServer() as leader, _AdminServer() as follower:
            follower.delete_offsets_codes = [NOT_CONTROLLER]
            follower.metadata = _other_broker_meta(
                follower.port, "127.0.0.1", leader.port
            )
            leader.delete_offsets_codes = [0]
            with Client(follower.addr, timeout=5.0) as c:
                got = c.delete_offsets("g", [("events", 0)])
            self.assertEqual(got, 3)
            self.assertEqual(c.addr, leader.addr)
        self.assertEqual(follower.delete_offsets_count, 1)
        self.assertEqual(follower.metadata_count, 1)
        self.assertEqual(leader.delete_offsets_count, 1)

    def test_delete_offsets_max_redirects_zero_raises_on_first_14(self) -> None:
        with _AdminServer() as follower:
            follower.delete_offsets_replies = [
                (NOT_CONTROLLER, "not controller; controller_id=2", True)
            ]
            follower.metadata = _controller_meta(2, "127.0.0.1", 9)
            with Client(follower.addr, timeout=5.0, max_redirects=0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.delete_offsets("g")
            self.assertEqual(ctx.exception.code, NOT_CONTROLLER)
        self.assertEqual(follower.delete_offsets_count, 1)
        self.assertEqual(follower.metadata_count, 0)
        self.assertEqual(follower.accept_count, 1)


if __name__ == "__main__":
    unittest.main()
