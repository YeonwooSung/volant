package io.volant;

/** Non-zero broker {@code error_code} or Error opcode. */
public class BrokerException extends RuntimeException {
    public final int code;
    public final String message;
    public final String op;

    public BrokerException(int code, String message) {
        this(code, message, "");
    }

    public BrokerException(int code, String message, String op) {
        super(format(code, message, op));
        this.code = code;
        this.message = message == null ? "" : message;
        this.op = op == null ? "" : op;
    }

    private static String format(int code, String message, String op) {
        String prefix = (op == null || op.isEmpty()) ? "" : op + ": ";
        String detail = (message == null || message.isEmpty()) ? ("error_code=" + code) : message;
        return prefix + detail + " (code=" + code + ")";
    }
}
