"""AddBroker / RemoveBroker / ListMembers client tests against a fake server."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import BrokerError, Client, MembershipBroker, MembershipList
from volant.codec import (
    OP_ADD_BROKER,
    OP_ADD_BROKER_RESPONSE,
    OP_LIST_MEMBERS,
    OP_LIST_MEMBERS_RESPONSE,
    OP_REMOVE_BROKER,
    OP_REMOVE_BROKER_RESPONSE,
    AddBrokerResponse,
    ListMembersResponse,
    RemoveBrokerResponse,
    decode_add_broker_request,
    decode_remove_broker_request,
    encode_add_broker_response,
    encode_list_members_response,
    encode_remove_broker_response,
)
from volant.frame import encode_frame, try_decode_frame


class _MembershipServer:
    """Accept one connection and reply to Add/RemoveBroker / ListMembers."""

    def __init__(
        self,
        *,
        add_error: int = 0,
        add_generation: int = 5,
        remove_error: int = 0,
        remove_generation: int = 6,
        list_error: int = 0,
        list_generation: int = 4,
        brokers: Optional[list[MembershipBroker]] = None,
        live: Optional[list[int]] = None,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.add_error = add_error
        self.add_generation = add_generation
        self.remove_error = remove_error
        self.remove_generation = remove_generation
        self.list_error = list_error
        self.list_generation = list_generation
        self.brokers = (
            list(brokers)
            if brokers is not None
            else [
                MembershipBroker(id=1, host="10.0.0.1", port=9092, rack=None),
                MembershipBroker(id=2, host="10.0.0.2", port=9092, rack="r1"),
            ]
        )
        self.live = list(live) if live is not None else [1, 2]
        self.got_add: Optional[tuple[int, str, int, Optional[str]]] = None
        self.got_remove: Optional[int] = None
        self.list_payload: Optional[bytes] = None
        self.opcodes: list[int] = []
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_MembershipServer":
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
                if frame.opcode == OP_ADD_BROKER:
                    req = decode_add_broker_request(frame.payload)
                    self.got_add = (req.id, req.host, req.port, req.rack)
                    payload = encode_add_broker_response(
                        AddBrokerResponse(
                            error_code=self.add_error,
                            generation=0 if self.add_error else self.add_generation,
                        )
                    )
                    conn.sendall(
                        encode_frame(
                            OP_ADD_BROKER_RESPONSE, frame.correlation_id, payload
                        )
                    )
                elif frame.opcode == OP_REMOVE_BROKER:
                    req = decode_remove_broker_request(frame.payload)
                    self.got_remove = req.id
                    payload = encode_remove_broker_response(
                        RemoveBrokerResponse(
                            error_code=self.remove_error,
                            generation=0
                            if self.remove_error
                            else self.remove_generation,
                        )
                    )
                    conn.sendall(
                        encode_frame(
                            OP_REMOVE_BROKER_RESPONSE, frame.correlation_id, payload
                        )
                    )
                elif frame.opcode == OP_LIST_MEMBERS:
                    self.list_payload = bytes(frame.payload)
                    payload = encode_list_members_response(
                        ListMembersResponse(
                            error_code=self.list_error,
                            generation=0
                            if self.list_error
                            else self.list_generation,
                            brokers=self.brokers,
                            live=self.live,
                        )
                    )
                    conn.sendall(
                        encode_frame(
                            OP_LIST_MEMBERS_RESPONSE, frame.correlation_id, payload
                        )
                    )
                else:
                    self.error = RuntimeError(f"unexpected opcode {frame.opcode}")
                    return
        except BaseException as e:
            self.error = e
        finally:
            try:
                conn.close()
            except OSError:
                pass


class TestMembershipClient(unittest.TestCase):
    def test_add_returns_generation(self) -> None:
        with _MembershipServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                gen = c.add_broker(2, "10.0.0.2", 9092, rack="r1")
        if srv.error is not None:
            raise srv.error
        self.assertEqual(gen, 5)
        self.assertEqual(srv.got_add, (2, "10.0.0.2", 9092, "r1"))
        self.assertEqual(srv.opcodes, [OP_ADD_BROKER])

    def test_remove_returns_generation(self) -> None:
        with _MembershipServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                gen = c.remove_broker(2)
        if srv.error is not None:
            raise srv.error
        self.assertEqual(gen, 6)
        self.assertEqual(srv.got_remove, 2)
        self.assertEqual(srv.opcodes, [OP_REMOVE_BROKER])

    def test_list_parses_brokers_and_live(self) -> None:
        with _MembershipServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                members = c.list_members()
        if srv.error is not None:
            raise srv.error
        self.assertIsInstance(members, MembershipList)
        self.assertEqual(members.generation, 4)
        self.assertEqual(
            members.brokers,
            [
                MembershipBroker(id=1, host="10.0.0.1", port=9092, rack=None),
                MembershipBroker(id=2, host="10.0.0.2", port=9092, rack="r1"),
            ],
        )
        self.assertEqual(members.live, [1, 2])
        self.assertEqual(srv.list_payload, b"")
        self.assertEqual(srv.opcodes, [OP_LIST_MEMBERS])

    def test_add_error_raises(self) -> None:
        with _MembershipServer(add_error=3) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.add_broker(2, "10.0.0.2", 9092)
        self.assertEqual(ctx.exception.code, 3)
        self.assertEqual(ctx.exception.op, "add_broker")

    def test_remove_error_raises(self) -> None:
        with _MembershipServer(remove_error=2) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.remove_broker(2)
        self.assertEqual(ctx.exception.code, 2)
        self.assertEqual(ctx.exception.op, "remove_broker")

    def test_list_error_raises(self) -> None:
        with _MembershipServer(list_error=23) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.list_members()
        self.assertEqual(ctx.exception.code, 23)
        self.assertEqual(ctx.exception.op, "list_members")


if __name__ == "__main__":
    unittest.main()
