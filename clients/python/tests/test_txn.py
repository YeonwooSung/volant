"""BeginTxn / EndTxn client tests against a scripted TCP broker (no live server)."""

from __future__ import annotations

import socket
import threading
import unittest
from typing import Optional

from volant import BrokerError, Client, TransactionalProducer, TxnOffsetCommit, TxnProduceResult
from volant.codec import (
    OP_BEGIN_TXN,
    OP_BEGIN_TXN_RESPONSE,
    OP_END_TXN,
    OP_END_TXN_RESPONSE,
    OP_INIT_PRODUCER_ID,
    OP_INIT_PRODUCER_ID_RESPONSE,
    OP_PRODUCE,
    BeginTxnResponse,
    EndTxnResponse,
    InitProducerIdResponse,
    ProduceResponse,
    TxnProduceResult as WireTxnProduceResult,
    decode_begin_txn_request,
    decode_end_txn_request,
    decode_init_producer_id_request,
    decode_produce_request,
    encode_begin_txn_response,
    encode_end_txn_response,
    encode_init_producer_id_response,
    encode_produce_response,
)
from volant.frame import ProtocolError, encode_frame, try_decode_frame


class _TxnServer:
    """Accept one connection and reply to Init / BeginTxn / Produce / EndTxn."""

    def __init__(self, *, begin_error: int = 0, end_error: int = 0) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self.begin_error = begin_error
        self.end_error = end_error
        self.opcodes: list[int] = []
        self.init_txn_ids: list[str] = []
        self.begin_reqs = []
        self.produce_reqs = []
        self.end_reqs = []
        self.error: Optional[BaseException] = None
        self._lsock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"

    def __enter__(self) -> "_TxnServer":
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
            req = decode_init_producer_id_request(raw)
            self.init_txn_ids.append(req.transactional_id)
            return (
                encode_init_producer_id_response(
                    InitProducerIdResponse(producer_id=7, epoch=0, error_code=0)
                ),
                OP_INIT_PRODUCER_ID_RESPONSE,
            )
        if opcode == OP_BEGIN_TXN:
            req = decode_begin_txn_request(raw)
            self.begin_reqs.append(req)
            return (
                encode_begin_txn_response(BeginTxnResponse(error_code=self.begin_error)),
                OP_BEGIN_TXN_RESPONSE,
            )
        if opcode == OP_PRODUCE:
            req = decode_produce_request(raw)
            self.produce_reqs.append(req)
            return (
                encode_produce_response(
                    ProduceResponse(
                        topic=req.topic,
                        partition=req.partition if req.partition >= 0 else 0,
                        base_offset=0,
                        count=len(req.messages),
                        error_code=0,
                    )
                ),
                OP_PRODUCE,
            )
        if opcode == OP_END_TXN:
            req = decode_end_txn_request(raw)
            self.end_reqs.append(req)
            results = []
            if req.committed and self.end_error == 0:
                results = [
                    WireTxnProduceResult(
                        topic="t", partition=0, base_offset=10, count=1
                    )
                ]
            return (
                encode_end_txn_response(
                    EndTxnResponse(error_code=self.end_error, results=results)
                ),
                OP_END_TXN_RESPONSE,
            )
        raise ProtocolError(f"unexpected opcode {opcode}")


class TestTxnClient(unittest.TestCase):
    def test_begin_produce_commit(self) -> None:
        with _TxnServer() as srv:
            with Client(srv.addr, timeout=5.0, transactional_id="txn-1") as c:
                c.begin_transaction()
                c.produce("t", 0, value=b"hello")
                results = c.commit_transaction(
                    offsets=[
                        TxnOffsetCommit(
                            group_id="g", topic="t", partition=0, offset=1, metadata=""
                        )
                    ]
                )
            self.assertEqual(
                srv.opcodes,
                [OP_INIT_PRODUCER_ID, OP_BEGIN_TXN, OP_PRODUCE, OP_END_TXN],
            )
            self.assertEqual(srv.init_txn_ids, ["txn-1"])
            self.assertEqual(srv.begin_reqs[0].producer_id, 7)
            self.assertEqual(srv.begin_reqs[0].producer_epoch, 0)
            first = srv.produce_reqs[0]
            self.assertEqual(
                (first.producer_id, first.producer_epoch, first.base_sequence),
                (7, 0, 0),
            )
            end = srv.end_reqs[0]
            self.assertTrue(end.committed)
            self.assertEqual(len(end.offsets), 1)
            self.assertEqual(end.offsets[0].group_id, "g")
            self.assertEqual(results, [TxnProduceResult("t", 0, 10, 1)])
            self.assertFalse(c._in_transaction)

    def test_abort_rewinds_sequence(self) -> None:
        with _TxnServer() as srv:
            with Client(srv.addr, timeout=5.0, transactional_id="txn-1") as c:
                c.begin_transaction()
                c.produce("t", 0, value=b"a")
                c.abort_transaction()
                c.begin_transaction()
                c.produce("t", 0, value=b"b")
            self.assertEqual(len(srv.produce_reqs), 2)
            self.assertEqual(srv.produce_reqs[0].base_sequence, 0)
            self.assertEqual(srv.produce_reqs[1].base_sequence, 0)
            self.assertFalse(srv.end_reqs[0].committed)
            self.assertEqual(srv.end_reqs[0].offsets, [])

    def test_missing_transactional_id_errors_before_send(self) -> None:
        with _TxnServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(ValueError) as ctx:
                    c.begin_transaction()
            self.assertIn("transactional_id", str(ctx.exception))
            self.assertEqual(srv.opcodes, [])

    def test_error_22_raises_begin_txn(self) -> None:
        with _TxnServer(begin_error=22) as srv:
            with Client(srv.addr, timeout=5.0, transactional_id="txn-1") as c:
                with self.assertRaises(BrokerError) as ctx:
                    c.begin_transaction()
            self.assertEqual(ctx.exception.code, 22)
            self.assertEqual(ctx.exception.op, "begin_txn")
            self.assertEqual(srv.opcodes, [OP_INIT_PRODUCER_ID, OP_BEGIN_TXN])


class TestTransactionalProducer(unittest.TestCase):
    def test_begin_produce_add_offsets_commit(self) -> None:
        with _TxnServer() as srv:
            with Client(srv.addr, timeout=5.0, transactional_id="txn-1") as c:
                p = TransactionalProducer(c)
                self.assertFalse(p.is_open())
                p.begin()
                self.assertTrue(p.is_open())
                p.produce("t", 0, value=b"x")
                p.add_offsets("g", [("t", 0, 1)])
                results = p.commit()
                self.assertFalse(p.is_open())
            self.assertEqual(
                srv.opcodes,
                [OP_INIT_PRODUCER_ID, OP_BEGIN_TXN, OP_PRODUCE, OP_END_TXN],
            )
            end = srv.end_reqs[0]
            self.assertTrue(end.committed)
            self.assertEqual(len(end.offsets), 1)
            self.assertEqual(end.offsets[0].group_id, "g")
            self.assertEqual(end.offsets[0].topic, "t")
            self.assertEqual(end.offsets[0].partition, 0)
            self.assertEqual(end.offsets[0].offset, 1)
            self.assertEqual(results, [TxnProduceResult("t", 0, 10, 1)])

    def test_abort_clears_queue(self) -> None:
        with _TxnServer() as srv:
            with Client(srv.addr, timeout=5.0, transactional_id="txn-1") as c:
                p = TransactionalProducer(c)
                p.begin()
                p.produce("t", 0, value=b"x")
                p.add_offsets("g", [("t", 0, 1)])
                p.abort()
                self.assertFalse(p.is_open())
                p.begin()
                p.commit()
            self.assertFalse(srv.end_reqs[0].committed)
            self.assertEqual(srv.end_reqs[0].offsets, [])
            self.assertTrue(srv.end_reqs[1].committed)
            self.assertEqual(srv.end_reqs[1].offsets, [])

    def test_missing_transactional_id_constructor_fails(self) -> None:
        with _TxnServer() as srv:
            with Client(srv.addr, timeout=5.0) as c:
                with self.assertRaises(ValueError) as ctx:
                    TransactionalProducer(c)
            self.assertIn("transactional_id", str(ctx.exception))
            self.assertEqual(srv.opcodes, [])

    def test_commit_while_not_open(self) -> None:
        with _TxnServer() as srv:
            with Client(srv.addr, timeout=5.0, transactional_id="txn-1") as c:
                p = TransactionalProducer(c)
                with self.assertRaises(ValueError) as ctx:
                    p.commit()
                self.assertIn("not open", str(ctx.exception))
                with self.assertRaises(ValueError):
                    p.abort()
            self.assertEqual(srv.opcodes, [])

    def test_double_begin_raises(self) -> None:
        with _TxnServer() as srv:
            with Client(srv.addr, timeout=5.0, transactional_id="txn-1") as c:
                p = TransactionalProducer(c)
                p.begin()
                with self.assertRaises(ValueError) as ctx:
                    p.begin()
                self.assertIn("already open", str(ctx.exception))
            self.assertEqual(srv.opcodes, [OP_INIT_PRODUCER_ID, OP_BEGIN_TXN])


if __name__ == "__main__":
    unittest.main()
