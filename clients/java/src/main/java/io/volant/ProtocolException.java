package io.volant;

/** Magic, version, checksum, framing, or I/O error on the native protocol. */
public class ProtocolException extends RuntimeException {
    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
