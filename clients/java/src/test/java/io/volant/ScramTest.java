package io.volant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** SCRAM-SHA-256 crypto pin + constructor tests against a fake native server. */
class ScramTest {
    private static final String USER = "alice";
    private static final String PASS = "s3cret";
    private static final byte[] SALT = "saltSALTsaltSALT".getBytes(StandardCharsets.US_ASCII);
    private static final int ITERS = 4096;

    @Test
    void pinnedVector() {
        Scram.Proof p = Scram.clientProofAndServerSig(
                USER,
                PASS,
                "rOprNGfwEbeRWgbNEkqO",
                "rOprNGfwEbeRWgbNEkqOserver",
                SALT,
                ITERS);
        assertEquals(
                "82aa6ee69043dd3c43785fba02fe220ea4a74a44b12d31b3a3a3ad17c1e0b5f3",
                hex(p.clientProof));
        assertEquals(
                "d3068040897e7eaaa647e45356dab05074e5d48f6a283ec72a5181421768783d",
                hex(p.serverSignature));
    }

    @Test
    void connectScramSendsFirstAndFinal() throws Exception {
        try (ScramServer srv = ScramServer.ok()) {
            try (Client c = Client.connectScram("127.0.0.1", srv.port, 5_000, USER, PASS)) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
            assertEquals(Arrays.asList(Codec.OP_SCRAM_FIRST, Codec.OP_SCRAM_FINAL, Codec.OP_METADATA), srv.opcodes);
            assertEquals(USER, srv.firstUser);
            assertEquals(USER, srv.finalUser);
        }
    }

    @Test
    void badPasswordFails() throws Exception {
        try (ScramServer srv = ScramServer.ok()) {
            BrokerException ex = assertThrows(
                    BrokerException.class, () -> Client.connectScram("127.0.0.1", srv.port, 5_000, USER, "wrong"));
            assertEquals(17, ex.code);
            assertEquals("scram final", ex.op);
            srv.assertOk();
        }
    }

    @Test
    void signatureMismatchFails() throws Exception {
        try (ScramServer srv = ScramServer.badSignature()) {
            ProtocolException ex = assertThrows(
                    ProtocolException.class, () -> Client.connectScram("127.0.0.1", srv.port, 5_000, USER, PASS));
            assertTrue(ex.getMessage().contains("signature mismatch"));
        }
    }

    @Test
    void noCredsSendsNeither() throws Exception {
        try (ScramServer srv = ScramServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
            assertEquals(Collections.singletonList(Codec.OP_METADATA), srv.opcodes);
            assertNull(srv.token);
            assertNull(srv.firstUser);
        }
    }

    @Test
    void authTokenWinsOverScram() throws Exception {
        try (ScramServer srv = ScramServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000, "s3cret")) {
                c.metadata();
            }
            srv.assertOk();
            assertEquals(Codec.OP_AUTH, srv.opcodes.get(0));
            assertEquals("s3cret", srv.token);
            assertTrue(!srv.opcodes.contains(Codec.OP_SCRAM_FIRST));
            assertTrue(!srv.opcodes.contains(Codec.OP_SCRAM_FINAL));
        }
    }

    @Test
    void incompleteCredsFailBeforeConnect() {
        assertThrows(IllegalArgumentException.class, () -> Client.connectScram("127.0.0.1", 1, "alice", ""));
        assertThrows(IllegalArgumentException.class, () -> Client.connectScram("127.0.0.1", 1, "", "s3cret"));
        assertThrows(IllegalArgumentException.class, () -> Client.connectScram("127.0.0.1", 1, null, PASS));
    }

    @Test
    void defaultMaxRetriesZeroRaisesOnFirstTimeout() throws Exception {
        try (ScramServer srv = ScramServer.scripted(Collections.singletonList(7), Collections.emptyList())) {
            BrokerException ex = assertThrows(
                    BrokerException.class, () -> Client.connectScram("127.0.0.1", srv.port, 5_000, USER, PASS));
            assertEquals(7, ex.code);
            assertEquals("scram first", ex.op);
            assertEquals(1, srv.firstUsers.size());
            assertEquals(0, srv.finalUsers.size());
        }
    }

    @Test
    void retriesFirstTimeoutThenOk() throws Exception {
        try (ScramServer srv = ScramServer.scripted(Collections.singletonList(7), Collections.emptyList())) {
            try (Client c = Client.connectScram("127.0.0.1", srv.port, 5_000, USER, PASS, 2, 0)) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
            assertEquals(2, srv.firstUsers.size());
            assertEquals(1, srv.finalUsers.size());
            assertEquals(2, srv.firstNonces.size());
            assertTrue(!srv.firstNonces.get(0).equals(srv.firstNonces.get(1)));
        }
    }

    @Test
    void retriesFinalTimeoutRestartsHandshake() throws Exception {
        try (ScramServer srv = ScramServer.scripted(Collections.emptyList(), Collections.singletonList(7))) {
            try (Client c = Client.connectScram("127.0.0.1", srv.port, 5_000, USER, PASS, 2, 0)) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
            assertTrue(srv.firstUsers.size() >= 2);
            assertEquals(2, srv.finalUsers.size());
            assertTrue(srv.firstNonces.size() >= 2);
            assertTrue(!srv.firstNonces.get(0).equals(srv.firstNonces.get(1)));
        }
    }

    @Test
    void authFailedOnFirstIsNotRetried() throws Exception {
        try (ScramServer srv = ScramServer.scripted(Collections.singletonList(17), Collections.emptyList())) {
            BrokerException ex = assertThrows(
                    BrokerException.class,
                    () -> Client.connectScram("127.0.0.1", srv.port, 5_000, USER, PASS, 2, 0));
            assertEquals(17, ex.code);
            assertEquals("scram first", ex.op);
            assertEquals(1, srv.firstUsers.size());
            assertEquals(0, srv.finalUsers.size());
        }
    }

    @Test
    void reconnectRerunsScram() throws Exception {
        try (ScramServer first = ScramServer.ok();
                ScramServer second = ScramServer.ok()) {
            try (Client c = Client.connectScram("127.0.0.1", first.port, 5_000, USER, PASS)) {
                assertEquals(1, c.metadata().brokers.size());
                c.reconnect("127.0.0.1", second.port);
                assertEquals(1, c.metadata().brokers.size());
            }
            first.assertOk();
            second.assertOk();
            assertEquals(USER, first.firstUser);
            assertEquals(USER, first.finalUser);
            assertEquals(USER, second.firstUser);
            assertEquals(USER, second.finalUser);
            assertEquals(1, first.firstUsers.size());
            assertEquals(1, second.firstUsers.size());
        }
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte v : b) {
            sb.append(String.format("%02x", v));
        }
        return sb.toString();
    }

    private static final class ScramServer implements AutoCloseable {
        final int port;
        final List<Integer> opcodes = Collections.synchronizedList(new ArrayList<>());
        final List<String> firstUsers = Collections.synchronizedList(new ArrayList<>());
        final List<String> finalUsers = Collections.synchronizedList(new ArrayList<>());
        final List<String> firstNonces = Collections.synchronizedList(new ArrayList<>());
        volatile String firstUser;
        volatile String finalUser;
        volatile String token;
        private final boolean badSignature;
        private final List<Integer> firstErrors;
        private final List<Integer> finalErrors;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();
        private final CountDownLatch done = new CountDownLatch(1);

        static ScramServer ok() throws Exception {
            return new ScramServer(false, Collections.emptyList(), Collections.emptyList());
        }

        static ScramServer badSignature() throws Exception {
            return new ScramServer(true, Collections.emptyList(), Collections.emptyList());
        }

        static ScramServer scripted(List<Integer> firstErrors, List<Integer> finalErrors) throws Exception {
            return new ScramServer(false, firstErrors, finalErrors);
        }

        private ScramServer(boolean badSignature, List<Integer> firstErrors, List<Integer> finalErrors)
                throws IOException {
            this.badSignature = badSignature;
            this.firstErrors = new ArrayList<>(firstErrors);
            this.finalErrors = new ArrayList<>(finalErrors);
            ServerSocket ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress("127.0.0.1", 0));
            ss.setSoTimeout(5_000);
            this.listen = ss;
            this.port = ss.getLocalPort();
            this.thread = new Thread(
                    () -> {
                        try (Socket conn = listen.accept()) {
                            handle(conn);
                        } catch (Exception e) {
                            error.set(e);
                        } finally {
                            done.countDown();
                        }
                    },
                    "volant-scram");
            this.thread.setDaemon(true);
            this.thread.start();
        }

        void assertOk() throws Exception {
            if (!done.await(5, TimeUnit.SECONDS)) {
                throw new IOException("server did not finish");
            }
            Exception e = error.get();
            if (e != null) {
                throw e;
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

        private void handle(Socket conn) throws Exception {
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
                if (d.frame.opcode == Codec.OP_AUTH) {
                    token = Codec.decodeAuthRequest(d.frame.payload).token;
                    byte[] payload = Codec.encodeAuthResponse(new Codec.AuthResponse(0));
                    out.write(Frame.encode(Codec.OP_AUTH_RESPONSE, d.frame.correlationId, payload));
                    out.flush();
                    continue;
                }
                if (d.frame.opcode == Codec.OP_SCRAM_FIRST) {
                    Codec.ScramFirstRequest req = Codec.decodeScramFirstRequest(d.frame.payload);
                    firstUser = req.username;
                    firstUsers.add(req.username);
                    firstNonces.add(req.clientNonce);
                    int firstCode = firstErrors.isEmpty() ? 0 : firstErrors.remove(0);
                    byte[] payload = Codec.encodeScramFirstResponse(
                            new Codec.ScramFirstResponse(firstCode, req.clientNonce + "s", SALT, ITERS));
                    out.write(Frame.encode(Codec.OP_SCRAM_FIRST_RESPONSE, d.frame.correlationId, payload));
                    out.flush();
                    continue;
                }
                if (d.frame.opcode == Codec.OP_SCRAM_FINAL) {
                    Codec.ScramFinalRequest req = Codec.decodeScramFinalRequest(d.frame.payload);
                    finalUser = req.username;
                    finalUsers.add(req.username);
                    String clientNonce = req.combinedNonce.endsWith("s")
                            ? req.combinedNonce.substring(0, req.combinedNonce.length() - 1)
                            : "";
                    Scram.Proof expected = Scram.clientProofAndServerSig(
                            req.username, PASS, clientNonce, req.combinedNonce, SALT, ITERS);
                    int code = 0;
                    byte[] sig = expected.serverSignature;
                    if (!finalErrors.isEmpty()) {
                        code = finalErrors.remove(0);
                    } else if (!Arrays.equals(req.clientProof, expected.clientProof)) {
                        code = 17;
                    } else if (badSignature) {
                        sig = new byte[32];
                    }
                    byte[] payload = Codec.encodeScramFinalResponse(new Codec.ScramFinalResponse(code, sig));
                    out.write(Frame.encode(Codec.OP_SCRAM_FINAL_RESPONSE, d.frame.correlationId, payload));
                    out.flush();
                    // Keep the connection open after a transient reply so the
                    // client can restart the handshake on the same socket.
                    if (code == 6 || code == 7 || code == 15 || code == 16) {
                        continue;
                    }
                    if (code != 0 || badSignature) {
                        return;
                    }
                    continue;
                }
                if (d.frame.opcode == Codec.OP_METADATA) {
                    byte[] payload = Codec.encodeMetadataResponse(
                            new Metadata(
                                    Collections.singletonList(new Metadata.BrokerInfo(1, "127.0.0.1", 1)),
                                    Collections.emptyList()));
                    out.write(Frame.encode(Codec.OP_METADATA, d.frame.correlationId, payload));
                    out.flush();
                    return;
                }
                return;
            }
        }
    }
}
