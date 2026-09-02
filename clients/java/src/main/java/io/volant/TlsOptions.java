package io.volant;

/**
 * Optional TLS settings for {@link Client#connectTls}.
 *
 * <p>Plaintext remains {@link Client#connect}. Knobs match the Rust client
 * as closely as the JDK {@code SSLSocket} API allows.
 */
public final class TlsOptions {
    final String caFile;
    final boolean insecure;
    final String certFile;
    final String keyFile;

    private TlsOptions(String caFile, boolean insecure, String certFile, String keyFile) {
        this.caFile = caFile;
        this.insecure = insecure;
        this.certFile = certFile;
        this.keyFile = keyFile;
    }

    /** Trust {@code caFile} (PEM). Typical lab / private-CA setup. */
    public static TlsOptions ca(String caFile) {
        if (caFile == null || caFile.isEmpty()) {
            throw new IllegalArgumentException("caFile is required");
        }
        return new TlsOptions(caFile, false, null, null);
    }

    /** Skip certificate verification (tests / lab only). */
    public static TlsOptions insecure() {
        return new TlsOptions(null, true, null, null);
    }

    /**
     * Verify with the JVM default trust store (public CAs). Use {@link #ca}
     * for a private CA.
     */
    public static TlsOptions systemDefaults() {
        return new TlsOptions(null, false, null, null);
    }

    /**
     * Present a client certificate for mTLS. {@code certFile} and {@code
     * keyFile} are PEMs and must both be non-empty.
     */
    public TlsOptions clientCert(String certFile, String keyFile) {
        if (certFile == null || certFile.isEmpty() || keyFile == null || keyFile.isEmpty()) {
            throw new IllegalArgumentException("tls_cert and tls_key must both be set or both unset");
        }
        return new TlsOptions(this.caFile, this.insecure, certFile, keyFile);
    }

    public String caFile() {
        return caFile;
    }

    public boolean isInsecure() {
        return insecure;
    }

    public String certFile() {
        return certFile;
    }

    public String keyFile() {
        return keyFile;
    }
}
