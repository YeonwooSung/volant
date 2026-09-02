package io.volant;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/** Package-private TLS helpers: PEM load + {@link SSLSocket} wrap. */
final class Tls {
    private Tls() {}

    static SSLSocket wrap(Socket plain, String host, int port, TlsOptions opt) {
        try {
            SSLContext ctx = context(opt);
            SSLSocket ssl = (SSLSocket) ctx.getSocketFactory().createSocket(plain, host, port, true);
            SSLParameters params = ssl.getSSLParameters();
            if (!opt.isInsecure()) {
                params.setEndpointIdentificationAlgorithm("HTTPS");
            }
            ssl.setSSLParameters(params);
            ssl.startHandshake();
            return ssl;
        } catch (IOException | GeneralSecurityException e) {
            throw new ProtocolException("tls handshake failed: " + e.getMessage(), e);
        }
    }

    static SSLContext context(TlsOptions opt) throws GeneralSecurityException, IOException {
        TrustManager[] trust = opt.isInsecure() ? trustAll() : trustManagers(opt.caFile);
        KeyManager[] keys = keyManagers(opt.certFile, opt.keyFile);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(keys, trust, new SecureRandom());
        return ctx;
    }

    private static TrustManager[] trustManagers(String caFile) throws GeneralSecurityException, IOException {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        if (caFile == null || caFile.isEmpty()) {
            tmf.init((KeyStore) null);
            return tmf.getTrustManagers();
        }
        KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
        ts.load(null);
        int i = 0;
        for (X509Certificate cert : loadPemCerts(caFile)) {
            ts.setCertificateEntry("ca-" + i++, cert);
        }
        tmf.init(ts);
        return tmf.getTrustManagers();
    }

    private static KeyManager[] keyManagers(String certFile, String keyFile)
            throws GeneralSecurityException, IOException {
        if (certFile == null && keyFile == null) {
            return null;
        }
        if (certFile == null || certFile.isEmpty() || keyFile == null || keyFile.isEmpty()) {
            throw new ProtocolException("tls_cert and tls_key must both be set or both unset");
        }
        List<X509Certificate> certs = loadPemCerts(certFile);
        PrivateKey key = loadPemKey(keyFile);
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null);
        ks.setKeyEntry("client", key, new char[0], certs.toArray(new X509Certificate[0]));
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, new char[0]);
        return kmf.getKeyManagers();
    }

    static List<X509Certificate> loadPemCerts(String path) throws GeneralSecurityException, IOException {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        String text = readString(path);
        List<X509Certificate> out = new ArrayList<>();
        for (byte[] der : pemBlocks(text, "CERTIFICATE")) {
            try (InputStream in = new ByteArrayInputStream(der)) {
                out.add((X509Certificate) cf.generateCertificate(in));
            }
        }
        if (out.isEmpty()) {
            throw new ProtocolException("no certificates in " + path);
        }
        return out;
    }

    static PrivateKey loadPemKey(String path) throws GeneralSecurityException, IOException {
        String text = readString(path);
        List<byte[]> pkcs8 = pemBlocks(text, "PRIVATE KEY");
        if (!pkcs8.isEmpty()) {
            return decodePkcs8(pkcs8.get(0));
        }
        List<byte[]> pkcs1 = pemBlocks(text, "RSA PRIVATE KEY");
        if (!pkcs1.isEmpty()) {
            return decodePkcs8(wrapPkcs1Rsa(pkcs1.get(0)));
        }
        throw new ProtocolException("no private key in " + path);
    }

    private static PrivateKey decodePkcs8(byte[] der) throws GeneralSecurityException {
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
        GeneralSecurityException last = null;
        for (String alg : new String[] {"RSA", "EC"}) {
            try {
                return KeyFactory.getInstance(alg).generatePrivate(spec);
            } catch (InvalidKeySpecException e) {
                last = e;
            }
        }
        throw new ProtocolException("unsupported private key", last);
    }

    /** Wrap PKCS#1 RSAPrivateKey DER in a PKCS#8 PrivateKeyInfo. */
    static byte[] wrapPkcs1Rsa(byte[] pkcs1) {
        byte[] algId = new byte[] {
            0x30, 0x0d,
            0x06, 0x09, 0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7, 0x0d, 0x01, 0x01, 0x01,
            0x05, 0x00
        };
        byte[] version = new byte[] {0x02, 0x01, 0x00};
        byte[] octet = derTlv((byte) 0x04, pkcs1);
        byte[] body = concat(version, algId, octet);
        return derTlv((byte) 0x30, body);
    }

    private static byte[] derTlv(byte tag, byte[] value) {
        byte[] len = derLen(value.length);
        byte[] out = new byte[1 + len.length + value.length];
        out[0] = tag;
        System.arraycopy(len, 0, out, 1, len.length);
        System.arraycopy(value, 0, out, 1 + len.length, value.length);
        return out;
    }

    private static byte[] derLen(int n) {
        if (n < 0x80) {
            return new byte[] {(byte) n};
        }
        if (n < 0x100) {
            return new byte[] {(byte) 0x81, (byte) n};
        }
        if (n < 0x10000) {
            return new byte[] {(byte) 0x82, (byte) (n >> 8), (byte) n};
        }
        if (n < 0x1000000) {
            return new byte[] {(byte) 0x83, (byte) (n >> 16), (byte) (n >> 8), (byte) n};
        }
        return new byte[] {
            (byte) 0x84, (byte) (n >> 24), (byte) (n >> 16), (byte) (n >> 8), (byte) n
        };
    }

    private static byte[] concat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) {
            n += p.length;
        }
        byte[] out = new byte[n];
        int i = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, i, p.length);
            i += p.length;
        }
        return out;
    }

    static List<byte[]> pemBlocks(String text, String type) {
        String begin = "-----BEGIN " + type + "-----";
        String end = "-----END " + type + "-----";
        List<byte[]> out = new ArrayList<>();
        int from = 0;
        while (true) {
            int s = text.indexOf(begin, from);
            if (s < 0) {
                break;
            }
            int e = text.indexOf(end, s);
            if (e < 0) {
                break;
            }
            String b64 = text.substring(s + begin.length(), e).replaceAll("\\s+", "");
            out.add(Base64.getDecoder().decode(b64));
            from = e + end.length();
        }
        return out;
    }

    private static String readString(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.US_ASCII);
    }

    private static TrustManager[] trustAll() {
        return new TrustManager[] {
            new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) {}

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) {}

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            }
        };
    }
}
