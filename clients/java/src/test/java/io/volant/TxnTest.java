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
        try (TxnServer srv = new TxnServer(22, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setTransactionalId("txn-1");
                BrokerException ex = assertThrows(BrokerException.class, c::beginTransaction);
                assertEquals(22, ex.code);
                assertEquals("begin_txn", ex.op);
            }
            assertEquals(List.of(Codec.OP_INIT_PRODUCER_ID, Codec.OP_BEGIN_TXN), srv.opcodes);
        }
    }

    private static final class TxnServer implements AutoCloseable {
        final int port;
        final List<Integer> opcodes = Collections.synchronizedList(new ArrayList<>());
        final List<String> initTxnIds = Collections.synchronizedList(new ArrayList<>());
        final List<Codec.ProduceRequest> produceReqs = Collections.synchronizedList(new ArrayList<>());
        final List<Codec.EndTxnRequest> endReqs = Collections.synchronizedList(new ArrayList<>());
        private final int beginError;
        private final int endError;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();

        TxnServer(int beginError, int endError) throws IOException {
            this.beginError = beginError;
            this.endError = endError;
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            thread = new Thread(this::serve, "volant-txn");
            thread.setDaemon(true);
            thread.start();
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
                        payload = Codec.encodeBeginTxnResponse(new Codec.BeginTxnResponse(beginError));
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
