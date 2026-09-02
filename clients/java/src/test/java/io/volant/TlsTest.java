package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * TLS wrap + handshake tests (no live volant-server).
 *
 * <p>Ephemeral certs are generated with {@code keytool} (JDK). Skip if
 * keytool is missing. Live broker TLS is gated on {@code VOLANT_E2E=1}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TlsTest {
    private static final String PASS = "changeit";

    private Path tmp;
    private Path serverCrt;
    private Path serverKey;
    private Path clientCrt;
    private Path clientKey;
    private Path serverP12;
    private Path clientP12;

    @BeforeAll
    void generateCerts() throws Exception {
        Path keytool = keytoolBin();
        assumeTrue(keytool != null, "keytool not found; needed to generate ephemeral TLS certs");

        tmp = Files.createTempDirectory("volant-java-tls-");
        serverP12 = tmp.resolve("server.p12");
        clientP12 = tmp.resolve("client.p12");
        run(
                keytool,
                "-genkeypair",
                "-alias",
                "server",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-dname",
                "CN=localhost",
                "-validity",
                "2",
                "-keystore",
                serverP12.toString(),
                "-storetype",
                "PKCS12",
                "-storepass",
                PASS,
                "-keypass",
                PASS,
                "-ext",
                "SAN=DNS:localhost,IP:127.0.0.1");
        run(
                keytool,
                "-genkeypair",
                "-alias",
                "client",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-dname",
                "CN=volant-test-client",
                "-validity",
                "2",
                "-keystore",
                clientP12.toString(),
                "-storetype",
                "PKCS12",
                "-storepass",
                PASS,
                "-keypass",
                PASS);

        serverCrt = tmp.resolve("server.crt");
        serverKey = tmp.resolve("server.key");
        clientCrt = tmp.resolve("client.crt");
        clientKey = tmp.resolve("client.key");
        exportPem(serverP12, "server", serverCrt, serverKey);
        exportPem(clientP12, "client", clientCrt, clientKey);
    }

    @AfterAll
    void cleanupCerts() throws IOException {
        if (tmp != null) {
            deleteRecursively(tmp);
        }
    }

    @Test
    void plainTcpDefaultUnchanged() throws Exception {
        try (OneShotMetadataServer srv = OneShotMetadataServer.plain()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertFalse(c.isTls());
                Metadata meta = c.metadata();
                assertEquals(1, meta.brokers.size());
            }
            srv.assertOk();
        }
    }

    @Test
    void connectTlsWithCa() throws Exception {
        try (OneShotMetadataServer srv = OneShotMetadataServer.tls(serverP12, null, false)) {
            try (Client c = Client.connectTls("127.0.0.1", srv.port, TlsOptions.ca(serverCrt.toString()), 5_000)) {
                assertTrue(c.isTls());
                Metadata meta = c.metadata();
                assertEquals(1, meta.brokers.size());
                assertEquals("127.0.0.1", meta.brokers.get(0).host);
            }
            srv.assertOk();
        }
    }

    @Test
    void connectTlsInsecure() throws Exception {
        try (OneShotMetadataServer srv = OneShotMetadataServer.tls(serverP12, null, false)) {
            try (Client c = Client.connectTls("127.0.0.1", srv.port, TlsOptions.insecure(), 5_000)) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
        }
    }

    @Test
    void connectTlsRejectsUntrusted() throws Exception {
        try (OneShotMetadataServer srv = OneShotMetadataServer.tls(serverP12, null, false)) {
            assertThrows(
                    ProtocolException.class,
                    () -> Client.connectTls("127.0.0.1", srv.port, TlsOptions.systemDefaults(), 5_000));
        }
    }

    @Test
    void connectTlsMtls() throws Exception {
        try (OneShotMetadataServer srv = OneShotMetadataServer.tls(serverP12, clientP12, true)) {
            TlsOptions opt = TlsOptions.ca(serverCrt.toString()).clientCert(clientCrt.toString(), clientKey.toString());
            try (Client c = Client.connectTls("127.0.0.1", srv.port, opt, 5_000)) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
        }
    }

    @Test
    void connectTlsMtlsWithoutClientCertFails() throws Exception {
        try (OneShotMetadataServer srv = OneShotMetadataServer.tls(serverP12, clientP12, true)) {
            assertThrows(ProtocolException.class, () -> {
                try (Client c = Client.connectTls("127.0.0.1", srv.port, TlsOptions.ca(serverCrt.toString()), 5_000)) {
                    c.metadata();
                }
            });
        }
    }

    @Test
    void clientCertAndKeyMustBePaired() {
        assertThrows(IllegalArgumentException.class, () -> TlsOptions.insecure().clientCert("only.pem", null));
        assertThrows(IllegalArgumentException.class, () -> TlsOptions.insecure().clientCert(null, "only.key"));
        assertThrows(IllegalArgumentException.class, () -> TlsOptions.insecure().clientCert("", "k"));
    }

    @Test
    void e2eTlsAgainstServer() throws Exception {
        assumeTrue("1".equals(System.getenv("VOLANT_E2E")), "set VOLANT_E2E=1 to run live broker TLS e2e");
        Path binary = findServerBin();
        assumeTrue(binary != null, "volant-server not found; build with `cargo build -p volant-server --features tls`");
        Path dataDir = Files.createTempDirectory("volant-java-tls-e2e-");
        int port = freePort();
        Process proc = new ProcessBuilder(
                        binary.toString(),
                        "--listen",
                        "127.0.0.1:" + port,
                        "--data-dir",
                        dataDir.toString(),
                        "--tls-cert",
                        serverCrt.toString(),
                        "--tls-key",
                        serverKey.toString())
                .directory(repoRoot().toFile())
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start();
        try {
            try {
                waitPort("127.0.0.1", port, 8_000);
            } catch (Exception e) {
                assumeTrue(false, "volant-server did not listen with --tls-*; build with --features tls");
                return;
            }
            try (Client c = Client.connectTls("127.0.0.1", port, TlsOptions.ca(serverCrt.toString()), 5_000)) {
                assertFalse(c.metadata().brokers.isEmpty());
            }
        } finally {
            proc.destroy();
            if (!proc.waitFor(5, TimeUnit.SECONDS)) {
                proc.destroyForcibly();
                proc.waitFor(5, TimeUnit.SECONDS);
            }
            deleteRecursively(dataDir);
        }
    }

    private static final class OneShotMetadataServer implements AutoCloseable {
        final int port;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();
        private final CountDownLatch done = new CountDownLatch(1);

        static OneShotMetadataServer plain() throws Exception {
            ServerSocket ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress("127.0.0.1", 0));
            ss.setSoTimeout(5_000);
            return new OneShotMetadataServer(ss, false);
        }

        static OneShotMetadataServer tls(Path serverP12, Path clientP12, boolean requireClient) throws Exception {
            SSLContext ctx = serverContext(serverP12, clientP12, requireClient);
            SSLServerSocket ss = (SSLServerSocket)
                    ctx.getServerSocketFactory().createServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
            ss.setSoTimeout(5_000);
            ss.setNeedClientAuth(requireClient);
            return new OneShotMetadataServer(ss, true);
        }

        private OneShotMetadataServer(ServerSocket listen, boolean tls) {
            this.listen = listen;
            this.port = listen.getLocalPort();
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
                    "volant-tls-meta");
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

        private static void handle(Socket conn) throws Exception {
            conn.setSoTimeout(5_000);
            InputStream in = conn.getInputStream();
            OutputStream out = conn.getOutputStream();
            byte[] buf = new byte[0];
            while (true) {
                Frame.Decode d = Frame.tryDecode(buf);
                if (d.frame != null) {
                    byte[] payload = Codec.encodeMetadataResponse(
                            new Metadata(
                                    Collections.singletonList(new Metadata.BrokerInfo(1, "127.0.0.1", 1)),
                                    Collections.emptyList()));
                    out.write(Frame.encode(Codec.OP_METADATA, d.frame.correlationId, payload));
                    out.flush();
                    return;
                }
                byte[] tmp = new byte[4096];
                int n = in.read(tmp);
                if (n < 0) {
                    return;
                }
                byte[] next = new byte[buf.length + n];
                System.arraycopy(buf, 0, next, 0, buf.length);
                System.arraycopy(tmp, 0, next, buf.length, n);
                buf = next;
            }
        }
    }

    private static SSLContext serverContext(Path serverP12, Path clientP12, boolean requireClient) throws Exception {
        char[] pass = PASS.toCharArray();
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(serverP12)) {
            ks.load(in, pass);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, pass);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        if (requireClient && clientP12 != null) {
            KeyStore clientKs = KeyStore.getInstance("PKCS12");
            try (InputStream in = Files.newInputStream(clientP12)) {
                clientKs.load(in, pass);
            }
            KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
            trust.load(null);
            trust.setCertificateEntry("client", clientKs.getCertificate("client"));
            tmf.init(trust);
        } else {
            tmf.init((KeyStore) null);
        }
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return ctx;
    }

    private static void exportPem(Path p12, String alias, Path certPem, Path keyPem) throws Exception {
        char[] pass = PASS.toCharArray();
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(p12)) {
            ks.load(in, pass);
        }
        Certificate cert = ks.getCertificate(alias);
        PrivateKey key = (PrivateKey) ks.getKey(alias, pass);
        Files.write(certPem, toPem("CERTIFICATE", cert.getEncoded()).getBytes(StandardCharsets.US_ASCII));
        Files.write(keyPem, toPem("PRIVATE KEY", key.getEncoded()).getBytes(StandardCharsets.US_ASCII));
    }

    private static String toPem(String type, byte[] der) {
        String b64 = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(der);
        return "-----BEGIN " + type + "-----\n" + b64 + "\n-----END " + type + "-----\n";
    }

    private static void run(Path bin, String... args) throws Exception {
        List<String> cmd = new java.util.ArrayList<>();
        cmd.add(bin.toString());
        Collections.addAll(cmd, args);
        Process p = new ProcessBuilder(cmd)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start();
        if (!p.waitFor(30, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IOException("keytool failed: " + cmd);
        }
    }

    private static Path keytoolBin() {
        Path home = Paths.get(System.getProperty("java.home"), "bin", "keytool");
        if (Files.isRegularFile(home) && Files.isExecutable(home)) {
            return home;
        }
        Path homeExe = Paths.get(System.getProperty("java.home"), "bin", "keytool.exe");
        if (Files.isRegularFile(homeExe)) {
            return homeExe;
        }
        return null;
    }

    private static Path repoRoot() {
        Path dir = Paths.get("").toAbsolutePath();
        for (int i = 0; i < 8 && dir != null; i++) {
            if (Files.isRegularFile(dir.resolve("Cargo.toml")) && Files.isDirectory(dir.resolve("clients"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        Path guess = Paths.get("").toAbsolutePath().resolve("../..").normalize();
        return Files.isRegularFile(guess.resolve("Cargo.toml")) ? guess : Paths.get("").toAbsolutePath();
    }

    private static Path findServerBin() {
        String env = System.getenv("VOLANT_SERVER");
        if (env != null && !env.isEmpty()) {
            Path p = Paths.get(env);
            if (Files.isRegularFile(p)) {
                return p;
            }
        }
        Path root = repoRoot();
        Path debug = root.resolve("target").resolve("debug").resolve("volant-server");
        if (Files.isRegularFile(debug)) {
            return debug;
        }
        Path release = root.resolve("target").resolve("release").resolve("volant-server");
        return Files.isRegularFile(release) ? release : null;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket()) {
            s.setReuseAddress(true);
            s.bind(new InetSocketAddress("127.0.0.1", 0));
            return s.getLocalPort();
        }
    }

    private static void waitPort(String host, int port, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, port), 250);
                return;
            } catch (IOException e) {
                last = e;
                Thread.sleep(50);
            }
        }
        throw new IOException("did not listen on " + host + ":" + port + ": " + last);
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        Files.walk(root)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                        // best-effort
                    }
                });
    }
}
