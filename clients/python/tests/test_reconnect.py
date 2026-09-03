"""Public Client.reconnect tests against fake native servers."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

from volant import Client
from volant.codec import OP_AUTH, OP_METADATA, OP_SCRAM_FINAL, OP_SCRAM_FIRST

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from test_auth import _OneShotServer
from test_scram import _PASS, _USER, _ScramServer


class TestReconnect(unittest.TestCase):
    def test_reconnect_second_listener_metadata(self) -> None:
        with _OneShotServer() as first, _OneShotServer() as second:
            with Client(first.addr, timeout=5.0) as c:
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)
                c.reconnect(second.addr)
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)
        self.assertEqual(first.first_opcode, OP_METADATA)
        self.assertEqual(second.first_opcode, OP_METADATA)
        if first.error is not None:
            raise first.error
        if second.error is not None:
            raise second.error

    def test_reconnect_resends_auth(self) -> None:
        with _OneShotServer() as first, _OneShotServer() as second:
            with Client(first.addr, timeout=5.0, auth_token="s3cret") as c:
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)
                c.reconnect(second.addr)
                meta = c.metadata()
                self.assertEqual(len(meta.brokers), 1)
        self.assertEqual(first.auth_count, 1)
        self.assertEqual(first.got_token, "s3cret")
        self.assertEqual(first.first_opcode, OP_AUTH)
        self.assertEqual(second.auth_count, 1)
        self.assertEqual(second.got_token, "s3cret")
        self.assertEqual(second.first_opcode, OP_AUTH)
        if first.error is not None:
            raise first.error
        if second.error is not None:
            raise second.error

    def test_reconnect_reruns_scram(self) -> None:
        with _ScramServer(connections=2) as srv:
            with Client(
                srv.addr, timeout=5.0, scram_username=_USER, scram_password=_PASS
            ) as c:
                c.metadata()
                c.reconnect(srv.addr)
                c.metadata()
        self.assertEqual(srv.first_usernames, [_USER, _USER])
        self.assertEqual(srv.final_usernames, [_USER, _USER])
        self.assertEqual(srv.opcodes.count(OP_SCRAM_FIRST), 2)
        self.assertEqual(srv.opcodes.count(OP_SCRAM_FINAL), 2)
        if srv.error is not None:
            raise srv.error


if __name__ == "__main__":
    unittest.main()
