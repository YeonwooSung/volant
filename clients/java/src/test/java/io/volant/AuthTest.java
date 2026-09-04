package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Shared-token Auth constructor tests against a fake native server. */
class AuthTest {
    @Test
    void connectWithTokenSendsAuth() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000, "s3cret")) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
            assertEquals(Codec.OP_AUTH, srv.firstOpcode.get());
            assertEquals("s3cret", srv.token.get());
        }
    }

    @Test
    void rejectedTokenRaises() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.reject(17)) {
            BrokerException ex = assertThrows(
                    BrokerException.class, () -> Client.connect("127.0.0.1", srv.port, 5_000, "nope"));
            assertEquals(17, ex.code);
            assertEquals("auth", ex.op);
            srv.assertOk();
            assertEquals("nope", srv.token.get());
        }
    }

    @Test
    void noTokenSendsNoAuth() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
            assertEquals(Codec.OP_METADATA, srv.firstOpcode.get());
            assertNull(srv.token.get());
        }
    }

    @Test
    void emptyTokenSkipsAuth() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000, "")) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
            assertEquals(Codec.OP_METADATA, srv.firstOpcode.get());
        }
    }

    @Test
    void defaultMaxRetriesZeroRaisesOnAuthTimeout() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.codes(7)) {
            BrokerException ex = assertThrows(
                    BrokerException.class, () -> Client.connect("127.0.0.1", srv.port, 5_000, "s3cret"));
            assertEquals(7, ex.code);
            assertEquals("auth", ex.op);
            assertEquals(1, srv.authCount.get());
        }
    }

    @Test
    void retriesAuthTimeoutThenOk() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.codes(7, 0)) {
            try (Client c = Client.connectWithRetries("127.0.0.1", srv.port, 5_000, "s3cret", 2, 0)) {
                assertEquals(2, c.maxRetries());
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
            assertEquals(2, srv.authCount.get());
            assertEquals("s3cret", srv.token.get());
        }
    }

    @Test
    void authFailedNotRetried() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.codes(17)) {
            BrokerException ex = assertThrows(
                    BrokerException.class,
                    () -> Client.connectWithRetries("127.0.0.1", srv.port, 5_000, "nope", 2, 0));
            assertEquals(17, ex.code);
            assertEquals("auth", ex.op);
            assertEquals(1, srv.authCount.get());
        }
    }

    @Test
    void authExhaustedRetriesRaises() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.codes(7, 7, 7)) {
            BrokerException ex = assertThrows(
                    BrokerException.class,
                    () -> Client.connectWithRetries("127.0.0.1", srv.port, 5_000, "s3cret", 2, 0));
            assertEquals(7, ex.code);
            assertEquals("auth", ex.op);
            assertEquals(3, srv.authCount.get());
        }
    }

    @Test
    void reconnectSecondListenerMetadata() throws Exception {
        try (OneShotAuthServer first = OneShotAuthServer.ok();
                OneShotAuthServer second = OneShotAuthServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", first.port, 5_000)) {
                assertEquals(1, c.metadata().brokers.size());
                c.reconnect("127.0.0.1", second.port);
                assertEquals(1, c.metadata().brokers.size());
            }
            first.assertOk();
            second.assertOk();
            assertEquals(Codec.OP_METADATA, first.firstOpcode.get());
            assertEquals(Codec.OP_METADATA, second.firstOpcode.get());
        }
    }

    @Test
    void timeoutMsAfterConnect() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 2500)) {
                assertEquals(2500, c.timeoutMs());
            }
        }
    }

    @Test
    void authTokenAfterConnectWithToken() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000, "s3cret")) {
                assertEquals("s3cret", c.authToken());
            }
        }
    }

    @Test
    void authTokenAfterConnectWithoutToken() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertNull(c.authToken());
            }
        }
    }

    @Test
    void reconnectResendsAuth() throws Exception {
        try (OneShotAuthServer first = OneShotAuthServer.ok();
                OneShotAuthServer second = OneShotAuthServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", first.port, 5_000, "s3cret")) {
                assertEquals(1, c.metadata().brokers.size());
                c.reconnect("127.0.0.1", second.port);
                assertEquals(1, c.metadata().brokers.size());
            }
            first.assertOk();
            second.assertOk();
            assertEquals(1, first.authCount.get());
            assertEquals("s3cret", first.token.get());
            assertEquals(Codec.OP_AUTH, first.firstOpcode.get());
            assertEquals(1, second.authCount.get());
            assertEquals("s3cret", second.token.get());
            assertEquals(Codec.OP_AUTH, second.firstOpcode.get());
        }
    }

    private static final class OneShotAuthServer implements AutoCloseable {
        final int port;
        final AtomicInteger firstOpcode = new AtomicInteger(-1);
        final AtomicInteger authCount = new AtomicInteger();
        final AtomicReference<String> token = new AtomicReference<>();
        private final int authError;
        private final int[] authCodes;
        private int authCodeIndex;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();
        private final CountDownLatch done = new CountDownLatch(1);

        static OneShotAuthServer ok() throws Exception {
            return new OneShotAuthServer(0, null);
        }

        static OneShotAuthServer reject(int code) throws Exception {
            return new OneShotAuthServer(code, null);
        }

        static OneShotAuthServer codes(int... codes) throws Exception {
            return new OneShotAuthServer(0, codes);
        }

        private OneShotAuthServer(int authError) throws IOException {
            this(authError, null);
        }

        private OneShotAuthServer(int authError, int[] authCodes) throws IOException {
            this.authError = authError;
            this.authCodes = authCodes;
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
                    "volant-auth");
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
                if (firstOpcode.get() < 0) {
                    firstOpcode.set(d.frame.opcode);
                }
                if (d.frame.opcode == Codec.OP_AUTH) {
                    authCount.incrementAndGet();
                    token.set(Codec.decodeAuthRequest(d.frame.payload).token);
                    int code;
                    if (authCodes != null) {
                        code = authCodeIndex < authCodes.length ? authCodes[authCodeIndex++] : 0;
                    } else {
                        code = authError;
                    }
                    byte[] payload = Codec.encodeAuthResponse(new Codec.AuthResponse(code));
                    out.write(Frame.encode(Codec.OP_AUTH_RESPONSE, d.frame.correlationId, payload));
                    out.flush();
                    if (authCodes != null && authCodes.length > 1) {
                        continue;
                    }
                    if (code != 0) {
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