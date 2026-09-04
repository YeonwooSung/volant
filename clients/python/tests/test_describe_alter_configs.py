"""DescribeConfigs / AlterConfigs client tests against a scripted TCP broker."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import BrokerError, Client, DescribeConfigsResult
from volant.codec import (
    OP_ALTER_CONFIGS,
    OP_ALTER_CONFIGS_RESPONSE,
    OP_DESCRIBE_CONFIGS,
    OP_DESCRIBE_CONFIGS_RESPONSE,
    AlterConfigsResponse,
    DescribeConfigsResponse,
    decode_alter_configs_request,
    decode_describe_configs_request,
    encode_alter_configs_response,
    encode_describe_configs_response,
)
from volant.frame import encode_frame, try_decode_frame


class _ConfigsServer:
    """Accept one connection and reply to DescribeConfigs / AlterConfigs."""

    def __init__(
        self,
        *,
        error_code: int = 0,
        configs: Optional[list[tuple[str, str]]] = None,
        topic_id: int = 1,
        partition_count: int = 1,
    ) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.error_code = error_code
        self.configs = configs if configs is not None else [("retention.ms", "86400000")]
        self.topic_id = topic_id
        self.partition_count = partition_count
        self.opcodes: list[int] = []
        self.got_topic: Optional[str] = None
        self.got_alter: Optional[list[tuple[str, str]]] = None
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_ConfigsServer":
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
                if frame.opcode == OP_DESCRIBE_CONFIGS:
                    req = decode_describe_configs_request(frame.payload)
                    self.got_topic = req.topic
                    payload = encode_describe_configs_response(
                        DescribeConfigsResponse(
                            error_code=self.error_code,
                            topic=req.topic,
                            topic_id=self.topic_id,
                            partition_count=self.partition_count,
                            configs=self.configs,
                        )
                    )
                    conn.sendall(
                        encode_frame(
                            OP_DESCRIBE_CONFIGS_RESPONSE,
                            frame.correlation_id,
                            payload,
                        )
                    )
                elif frame.opcode == OP_ALTER_CONFIGS:
                    req = decode_alter_configs_request(frame.payload)
                    self.got_topic = req.topic
                    self.got_alter = list(req.configs)
                    payload = encode_alter_configs_response(
                        AlterConfigsResponse(
                            error_code=self.error_code, topic=req.topic
                        )
                    )
                    conn.sendall(
                        encode_frame(
                            OP_ALTER_CONFIGS_RESPONSE,
                            frame.correlation_id,
                            payload,
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


class TestDescribeAlterConfigsClient(unittest.TestCase):
    def test_describe_returns_pairs(self) -> None:
        with _ConfigsServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                got = c.describe_configs("events")
        if srv.error is not None:
            raise srv.error
        self.assertEqual(srv.got_topic, "events")
        self.assertEqual(srv.opcodes, [OP_DESCRIBE_CONFIGS])
        self.assertEqual(
            got,
            DescribeConfigsResult(
                topic="events",
                topic_id=1,
                partition_count=1,
                configs=[("retention.ms", "86400000")],
            ),
        )

    def test_alter_ok_empty_value_clear(self) -> None:
        with _ConfigsServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.alter_configs("events", [("retention.ms", "")])
        if srv.error is not None:
            raise srv.error
        self.assertEqual(srv.got_topic, "events")
        self.assertEqual(srv.got_alter, [("retention.ms", "")])
        self.assertEqual(srv.opcodes, [OP_ALTER_CONFIGS])

    def test_alter_config_encodes_one_pair(self) -> None:
        with _ConfigsServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                c.alter_config("events", "retention.ms", "1")
        if srv.error is not None:
            raise srv.error
        self.assertEqual(srv.got_topic, "events")
        self.assertEqual(srv.got_alter, [("retention.ms", "1")])
        self.assertEqual(srv.opcodes, [OP_ALTER_CONFIGS])

    def test_describe_error_code_raises(self) -> None:
        with _ConfigsServer(error_code=2) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.describe_configs("missing")
        self.assertEqual(ctx.exception.code, 2)
        self.assertEqual(ctx.exception.op, "describe_configs")

    def test_alter_error_code_raises(self) -> None:
        with _ConfigsServer(error_code=2) as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.alter_configs("missing", [("retention.ms", "1")])
        self.assertEqual(ctx.exception.code, 2)
        self.assertEqual(ctx.exception.op, "alter_configs")


if __name__ == "__main__":
    unittest.main()
