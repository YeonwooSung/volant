"""ListGroups / DescribeGroup client tests against a fake native server."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import BrokerError, Client, GroupState
from volant.codec import (
    OP_DESCRIBE_GROUP,
    OP_DESCRIBE_GROUP_RESPONSE,
    OP_LIST_GROUPS,
    OP_LIST_GROUPS_RESPONSE,
    Assignment,
    DescribeGroupResponse,
    GroupListing,
    GroupMemberInfo,
    ListGroupsResponse,
    decode_describe_group_request,
    encode_describe_group_response,
    encode_list_groups_response,
)
from volant.frame import encode_frame, try_decode_frame


class _GroupAdminServer:
    """Accept one connection and reply to ListGroups / DescribeGroup."""

    def __init__(self, *, describe_error: Optional[int] = None) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.describe_error = describe_error
        self.described: Optional[str] = None
        self.opcodes: list[int] = []
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_GroupAdminServer":
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
                self.opcodes.append(frame.opcode)
                if frame.opcode == OP_LIST_GROUPS:
                    payload = encode_list_groups_response(
                        ListGroupsResponse(
                            error_code=0,
                            groups=[
                                GroupListing(
                                    group_id="g2",
                                    state=GroupState.EMPTY,
                                    member_count=0,
                                    generation=0,
                                ),
                                GroupListing(
                                    group_id="g1",
                                    state=GroupState.STABLE,
                                    member_count=2,
                                    generation=5,
                                ),
                            ],
                        )
                    )
                    conn.sendall(
                        encode_frame(
                            OP_LIST_GROUPS_RESPONSE, frame.correlation_id, payload
                        )
                    )
                    continue
                if frame.opcode == OP_DESCRIBE_GROUP:
                    self.described = decode_describe_group_request(frame.payload).group_id
                    if self.describe_error is not None:
                        payload = encode_describe_group_response(
                            DescribeGroupResponse(
                                error_code=self.describe_error,
                                group_id=self.described,
                                generation=0,
                                members=[],
                            )
                        )
                    else:
                        payload = encode_describe_group_response(
                            DescribeGroupResponse(
                                error_code=0,
                                group_id=self.described,
                                generation=3,
                                members=[
                                    GroupMemberInfo(
                                        member_id="m-a",
                                        topics=["events"],
                                        assignment=[
                                            Assignment(topic="events", partition=0),
                                            Assignment(topic="events", partition=2),
                                        ],
                                    )
                                ],
                            )
                        )
                    conn.sendall(
                        encode_frame(
                            OP_DESCRIBE_GROUP_RESPONSE, frame.correlation_id, payload
                        )
                    )
                    if self.describe_error is not None:
                        return
                    continue
                return
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass


class TestGroupAdminClient(unittest.TestCase):
    def test_group_state_from_u8_completing_rebalance(self) -> None:
        self.assertEqual(GroupState.from_u8(0), GroupState.EMPTY)
        self.assertEqual(GroupState.from_u8(1), GroupState.STABLE)
        self.assertEqual(GroupState.from_u8(2), GroupState.COMPLETING_REBALANCE)
        self.assertEqual(GroupState.from_u8(3), GroupState.PREPARING_REBALANCE)
        self.assertEqual(GroupState.from_u8(99), GroupState.EMPTY)

    def test_list_groups_empty_and_stable(self) -> None:
        with _GroupAdminServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                groups = c.list_groups()
        self.assertEqual(len(groups), 2)
        by_id = {g.group_id: g for g in groups}
        self.assertEqual(by_id["g2"].state, GroupState.EMPTY)
        self.assertEqual(by_id["g2"].member_count, 0)
        self.assertEqual(by_id["g2"].generation, 0)
        self.assertEqual(by_id["g1"].state, GroupState.STABLE)
        self.assertEqual(by_id["g1"].member_count, 2)
        self.assertEqual(by_id["g1"].generation, 5)
        self.assertEqual(srv.opcodes, [OP_LIST_GROUPS])
        if srv.error is not None:
            raise srv.error

    def test_describe_group_members_and_assignment(self) -> None:
        with _GroupAdminServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                desc = c.describe_group("cg-1")
        self.assertEqual(desc.group_id, "cg-1")
        self.assertEqual(desc.generation, 3)
        self.assertEqual(len(desc.members), 1)
        member = desc.members[0]
        self.assertEqual(member.member_id, "m-a")
        self.assertEqual(member.topics, ["events"])
        self.assertEqual(
            member.assignment,
            [
                Assignment(topic="events", partition=0),
                Assignment(topic="events", partition=2),
            ],
        )
        self.assertEqual(srv.described, "cg-1")
        self.assertEqual(srv.opcodes, [OP_DESCRIBE_GROUP])
        if srv.error is not None:
            raise srv.error

    def test_describe_group_not_found_raises(self) -> None:
        with _GroupAdminServer(describe_error=2) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as cm:
                    c.describe_group("missing")
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(cm.exception.op, "describe_group")
        self.assertEqual(srv.described, "missing")


if __name__ == "__main__":
    unittest.main()
