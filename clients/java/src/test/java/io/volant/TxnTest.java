package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** BeginTxn / EndTxn client tests against a scripted TCP broker (no live server). */
class TxnTest {
    @Test
    void beginProduceCommit() throws Exception {
        try (TxnServer srv = new TxnServer(0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                c.beginTransaction();
                c.produce("t", 0, null, "hello".getBytes(StandardCharsets.US_ASCII));
                List<TxnProduceResult> results = c.commitTransaction(
                        Collections.singletonList(new TxnOffsetCommit("g", "t", 0, 1L, "")));
                assertEquals(1, results.size());
                assertEquals(10L, results.get(0).baseOffset);
            }
            assertEquals(List.of(
                    Codec.OP_INIT_PRODUCER_ID,
                    Codec.OP_BEGIN_TXN,
                    Codec.OP_PRODUCE,
                    Codec.OP_END_TXN), srv.opcodes);
            assertEquals(List.of("txn-1"), srv.initTxnIds);
            assertEquals(7L, srv.produceReqs.get(0).producerId);
            assertEquals(0, srv.produceReqs.get(0).baseSequence);
            assertTrue(srv.endReqs.get(0).committed);
            assertEquals(1, srv.endReqs.get(0).offsets.size());
        }
    }

    @Test
    void abortRewindsSequence() throws Exception {
        try (TxnServer srv = new TxnServer(0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                c.beginTransaction();
                c.produce("t", 0, null, "a".getBytes(StandardCharsets.US_ASCII));
                c.abortTransaction();
                c.beginTransaction();
                c.produce("t", 0, null, "b".getBytes(StandardCharsets.US_ASCII));
            }
            assertEquals(2, srv.produceReqs.size());
            assertEquals(0, srv.produceReqs.get(0).baseSequence);
            assertEquals(0, srv.produceReqs.get(1).baseSequence);
            assertFalse(srv.endReqs.get(0).committed);
        }
    }

    @Test
    void missingTransactionalIdErrorsBeforeSend() throws Exception {
        try (TxnServer srv = new TxnServer(0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                IllegalStateException ex =
                        assertThrows(IllegalStateException.class, c::beginTransaction);
                assertTrue(ex.getMessage().contains("transactional_id"));
            }
            assertTrue(srv.opcodes.isEmpty());
        }
    }

    @Test
    void error22RaisesBeginTxn() throws Exception {
        try (TxnServer srv = new TxnServer(Client.INVALID_TXN_STATE, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                BrokerException ex = assertThrows(BrokerException.class, c::beginTransaction);
                assertEquals(Client.INVALID_TXN_STATE, ex.code);
                assertEquals("begin_txn", ex.op);
            }
            assertEquals(List.of(Codec.OP_INIT_PRODUCER_ID, Codec.OP_BEGIN_TXN), srv.opcodes);
        }
    }

    @Test
    void defaultMaxRetriesZeroRaisesOnBeginTimeout() throws Exception {
        try (TxnServer srv = new TxnServer(TIMEOUT, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                assertEquals(0, c.maxRetries());
                BrokerException ex = assertThrows(BrokerException.class, c::beginTransaction);
                assertEquals(TIMEOUT, ex.code);
                assertEquals("begin_txn", ex.op);
            }
            assertEquals(1, srv.beginCount());
            assertEquals(List.of(Codec.OP_INIT_PRODUCER_ID, Codec.OP_BEGIN_TXN), srv.opcodes);
        }
    }

    @Test
    void endTxnRetriesTimeoutThenOk() throws Exception {
        try (TxnServer srv = new TxnServer(new int[] {0}, new int[] {TIMEOUT, 0})) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                c.beginTransaction();
                List<TxnProduceResult> results = c.commitTransaction();
                assertEquals(1, results.size());
                assertEquals(10L, results.get(0).baseOffset);
            }
            assertEquals(2, srv.endReqs.size());
            assertTrue(srv.endReqs.get(0).committed);
            assertTrue(srv.endReqs.get(1).committed);
        }
    }

    @Test
    void abortRetriesTimeoutThenOk() throws Exception {
        try (TxnServer srv = new TxnServer(new int[] {0}, new int[] {TIMEOUT, 0})) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                c.beginTransaction();
                c.abortTransaction();
            }
            assertEquals(2, srv.endReqs.size());
            assertFalse(srv.endReqs.get(0).committed);
            assertFalse(srv.endReqs.get(1).committed);
        }
    }

    @Test
    void invalidTxnStateIsNotRetried() throws Exception {
        try (TxnServer srv = new TxnServer(Client.INVALID_TXN_STATE, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex = assertThrows(BrokerException.class, c::beginTransaction);
                assertEquals(Client.INVALID_TXN_STATE, ex.code);
                assertEquals("begin_txn", ex.op);
            }
            assertEquals(1, srv.beginCount());
            assertEquals(List.of(Codec.OP_INIT_PRODUCER_ID, Codec.OP_BEGIN_TXN), srv.opcodes);
        }
    }

    @Test
    void endTxnExhaustedRetriesRaises() throws Exception {
        try (TxnServer srv = new TxnServer(0, TIMEOUT)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                c.beginTransaction();
                BrokerException ex = assertThrows(BrokerException.class, c::commitTransaction);
                assertEquals(TIMEOUT, ex.code);
                assertEquals("end_txn", ex.op);
            }
            assertEquals(3, srv.endReqs.size());
        }
    }

    @Test
    void transactionalProducerBeginProduceAddOffsetsCommit() throws Exception {
        try (TxnServer srv = new TxnServer(0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                TransactionalProducer p = TransactionalProducer.from(c);
                assertFalse(p.isOpen());
                p.begin();
                assertTrue(p.isOpen());
                p.produce("t", 0, null, "x".getBytes(StandardCharsets.US_ASCII));
                p.addOffsets("g", "t", 0, 1L);
                List<TxnProduceResult> results = p.commit();
                assertFalse(p.isOpen());
                assertEquals(1, results.size());
                assertEquals(10L, results.get(0).baseOffset);
            }
            assertEquals(List.of(
                    Codec.OP_INIT_PRODUCER_ID,
                    Codec.OP_BEGIN_TXN,
                    Codec.OP_PRODUCE,
                    Codec.OP_END_TXN), srv.opcodes);
            assertTrue(srv.endReqs.get(0).committed);
            assertEquals(1, srv.endReqs.get(0).offsets.size());
            TxnOffsetCommit off = srv.endReqs.get(0).offsets.get(0);
            assertEquals("g", off.groupId);
            assertEquals("t", off.topic);
            assertEquals(0, off.partition);
            assertEquals(1L, off.offset);
        }
    }

    @Test
    void transactionalProducerAbortClearsQueue() throws Exception {
        try (TxnServer srv = new TxnServer(0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                TransactionalProducer p = TransactionalProducer.from(c);
                p.begin();
                p.produce("t", 0, null, "x".getBytes(StandardCharsets.US_ASCII));
                p.addOffsets("g", Collections.singletonList(new TxnOffsetCommit("ignored", "t", 0, 1L, "")));
                p.abort();
                assertFalse(p.isOpen());
                p.begin();
                p.commit();
            }
            assertFalse(srv.endReqs.get(0).committed);
            assertTrue(srv.endReqs.get(0).offsets.isEmpty());
            assertTrue(srv.endReqs.get(1).committed);
            assertTrue(srv.endReqs.get(1).offsets.isEmpty());
        }
    }

    @Test
    void transactionalProducerMissingTransactionalId() throws Exception {
        try (TxnServer srv = new TxnServer(0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                IllegalStateException ex =
                        assertThrows(IllegalStateException.class, () -> TransactionalProducer.from(c));
                assertTrue(ex.getMessage().contains("transactional_id"));
            }
            assertTrue(srv.opcodes.isEmpty());
        }
    }

    @Test
    void transactionalProducerCommitWhileNotOpen() throws Exception {
        try (TxnServer srv = new TxnServer(0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                TransactionalProducer p = TransactionalProducer.from(c);
                IllegalStateException commitEx =
                        assertThrows(IllegalStateException.class, p::commit);
                assertTrue(commitEx.getMessage().contains("not open"));
                IllegalStateException abortEx =
                        assertThrows(IllegalStateException.class, p::abort);
                assertTrue(abortEx.getMessage().contains("not open"));
            }
            assertTrue(srv.opcodes.isEmpty());
        }
    }

    @Test
    void transactionalProducerDoubleBegin() throws Exception {
        try (TxnServer srv = new TxnServer(0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                TransactionalProducer p = TransactionalProducer.from(c);
                p.begin();
                IllegalStateException ex = assertThrows(IllegalStateException.class, p::begin);
                assertTrue(ex.getMessage().contains("already open"));
            }
            assertEquals(List.of(Codec.OP_INIT_PRODUCER_ID, Codec.OP_BEGIN_TXN), srv.opcodes);
        }
    }

    private static final int TIMEOUT = 7;

    private static final class TxnServer implements AutoCloseable {
        final int port;
        final List<Integer> opcodes = Collections.synchronizedList(new ArrayList<>());
        final List<String> initTxnIds = Collections.synchronizedList(new ArrayList<>());
        final List<Codec.ProduceRequest> produceReqs = Collections.synchronizedList(new ArrayList<>());
        final List<Codec.EndTxnRequest> endReqs = Collections.synchronizedList(new ArrayList<>());
        private final List<Integer> beginCodes;
        private final List<Integer> endCodes;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();

        TxnServer(int beginError, int endError) throws IOException {
            this(new int[] {beginError}, new int[] {endError});
        }

        TxnServer(int[] beginCodes, int[] endCodes) throws IOException {
            this.beginCodes = new ArrayList<>();
            for (int c : beginCodes) {
                this.beginCodes.add(c);
            }
            this.endCodes = new ArrayList<>();
            for (int c : endCodes) {
                this.endCodes.add(c);
            }
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            thread = new Thread(this::serve, "volant-txn");
            thread.setDaemon(true);
            thread.start();
        }

        int beginCount() {
            int n = 0;
            for (int op : opcodes) {
                if (op == Codec.OP_BEGIN_TXN) {
                    n++;
                }
            }
            return n;
        }

        private static int nextCode(List<Integer> codes) {
            if (codes.isEmpty()) {
                return 0;
            }
            if (codes.size() == 1) {
                return codes.get(0);
            }
            return codes.remove(0);
        }

        private void serve() {
            try (Socket conn = listen.accept()) {
                conn.setSoTimeout(5_000);
                InputStream in = conn.getInputStream();
                OutputStream out = conn.getOutputStream();
                byte[] buf = new byte[0];
                while (true) {
                    Frame.Decode d = Frame.tryDecode(buf);
                    if (d.frame == null) {
                        byte[] tmp = new byte[4096];
                        int n = in.read(tmp);
                        if (n < 0) {
                            return;
                        }
                        byte[] next = new byte[buf.length + n];
                        System.arraycopy(buf, 0, next, 0, buf.length);
                        System.arraycopy(tmp, 0, next, buf.length, n);
                        buf = next;
                        continue;
                    }
                    buf = d.rest;
                    opcodes.add(d.frame.opcode);
                    int replyOp;
                    byte[] payload;
                    if (d.frame.opcode == Codec.OP_INIT_PRODUCER_ID) {
                        Codec.InitProducerIdRequest req = Codec.decodeInitProducerIdRequest(d.frame.payload);
                        initTxnIds.add(req.transactionalId);
                        payload = Codec.encodeInitProducerIdResponse(
                                new Codec.InitProducerIdResponse(7L, 0, 0));
                        replyOp = Codec.OP_INIT_PRODUCER_ID_RESPONSE;
                    } else if (d.frame.opcode == Codec.OP_BEGIN_TXN) {
                        payload = Codec.encodeBeginTxnResponse(
                                new Codec.BeginTxnResponse(nextCode(beginCodes)));
                        replyOp = Codec.OP_BEGIN_TXN_RESPONSE;
                    } else if (d.frame.opcode == Codec.OP_PRODUCE) {
                        Codec.ProduceRequest req = Codec.decodeProduceRequest(d.frame.payload);
                        produceReqs.add(req);
                        long part = req.partition >= 0 ? req.partition : 0;
                        payload = Codec.encodeProduceResponse(
                                new Codec.ProduceResponse(req.topic, part, 0L, req.messages.size(), 0));
                        replyOp = Codec.OP_PRODUCE;
                    } else if (d.frame.opcode == Codec.OP_END_TXN) {
                        Codec.EndTxnRequest req = Codec.decodeEndTxnRequest(d.frame.payload);
                        endReqs.add(req);
                        int endError = nextCode(endCodes);
                        List<TxnProduceResult> results = Collections.emptyList();
                        if (req.committed && endError == 0) {
                            results = Collections.singletonList(new TxnProduceResult("t", 0, 10L, 1));
                        }
                        payload = Codec.encodeEndTxnResponse(new Codec.EndTxnResponse(endError, results));
                        replyOp = Codec.OP_END_TXN_RESPONSE;
                    } else {
                        error.set(new ProtocolException("unexpected opcode " + d.frame.opcode));
                        return;
                    }
                    out.write(Frame.encode(replyOp, d.frame.correlationId, payload));
                    out.flush();
                }
            } catch (Exception e) {
                error.set(e);
            }
        }

        @Override
        public void close() {
            try {
                listen.close();
            } catch (IOException ignored) {
                // best-effort
            }
            try {
                thread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
