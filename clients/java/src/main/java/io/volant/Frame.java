package io.volant;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

/**
 * Native Volant frame encode/decode.
 *
 * <p>On-wire header is 16 bytes, big-endian (matches {@code
 * crates/volant-protocol/src/codec.rs}):
 *
 * <pre>
 * magic u8 | version u8 | opcode u16 | correlation_id u32 | payload_len u32 | crc32 u32 | payload
 * </pre>
 *
 * <p>CRC32 is IEEE of the <strong>payload only</strong> ({@code java.util.zip.CRC32}
 * ≡ {@code crc32fast} / {@code zlib.crc32}).
 */
public final class Frame {
    public static final int MAGIC = 0x56; // 'V'
    public static final int PROTOCOL_VERSION = 1;
    public static final int HEADER_LEN = 16;
    public static final int MAX_PAYLOAD = 16 * 1024 * 1024;

    public final int opcode;
    public final long correlationId;
    public final byte[] payload;
    public final int version;
    public final long checksum;

    public Frame(int opcode, long correlationId, byte[] payload, int version, long checksum) {
        this.opcode = opcode & 0xFFFF;
        this.correlationId = correlationId & 0xFFFFFFFFL;
        this.payload = payload == null ? new byte[0] : payload;
        this.version = version;
        this.checksum = checksum & 0xFFFFFFFFL;
    }

    /** IEEE CRC32 of {@code payload} (same polynomial as Rust {@code crc32fast::hash}). */
    public static long checksum(byte[] payload) {
        CRC32 crc = new CRC32();
        if (payload != null && payload.length > 0) {
            crc.update(payload, 0, payload.length);
        }
        return crc.getValue();
    }

    /** Encode a complete frame (header + payload) at protocol version 1. */
    public static byte[] encode(int opcode, long correlationId, byte[] payload) {
        return encode(opcode, correlationId, payload, PROTOCOL_VERSION);
    }

    /**
     * Encode a frame with an explicit version (tests use this to produce a
     * rejected version byte).
     */
    public static byte[] encode(int opcode, long correlationId, byte[] payload, int version) {
        if (payload == null) {
            payload = new byte[0];
        }
        if (payload.length > MAX_PAYLOAD) {
            throw new ProtocolException(
                    "payload too large: " + payload.length + " > " + MAX_PAYLOAD);
        }
        long crc = checksum(payload);
        ByteBuffer buf = ByteBuffer.allocate(HEADER_LEN + payload.length);
        buf.order(ByteOrder.BIG_ENDIAN);
        buf.put((byte) MAGIC);
        buf.put((byte) version);
        buf.putShort((short) opcode);
        buf.putInt((int) correlationId);
        buf.putInt(payload.length);
        buf.putInt((int) crc);
        buf.put(payload);
        return buf.array();
    }

    /**
     * Decode one frame from {@code data}.
     *
     * <p>Returns {@code frame == null} if more bytes are needed. Throws
     * {@link ProtocolException} on magic / version / checksum mismatch.
     */
    public static Decode tryDecode(byte[] data) {
        if (data == null || data.length < HEADER_LEN) {
            return new Decode(null, data == null ? new byte[0] : data);
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        int magic = buf.get() & 0xFF;
        int version = buf.get() & 0xFF;
        int opcode = buf.getShort() & 0xFFFF;
        long corr = buf.getInt() & 0xFFFFFFFFL;
        long payloadLen = buf.getInt() & 0xFFFFFFFFL;
        long crcWire = buf.getInt() & 0xFFFFFFFFL;
        if (magic != MAGIC) {
            throw new ProtocolException(String.format("invalid frame magic: 0x%x", magic));
        }
        if (payloadLen > MAX_PAYLOAD) {
            throw new ProtocolException("payload too large: " + payloadLen + " > " + MAX_PAYLOAD);
        }
        long totalLong = HEADER_LEN + payloadLen;
        if (totalLong > Integer.MAX_VALUE || data.length < (int) totalLong) {
            return new Decode(null, data);
        }
        int total = (int) totalLong;
        byte[] payload = new byte[(int) payloadLen];
        System.arraycopy(data, HEADER_LEN, payload, 0, payload.length);
        if (version != PROTOCOL_VERSION) {
            throw new ProtocolException("unsupported protocol version: " + version);
        }
        long expected = checksum(payload);
        if (crcWire != expected) {
            throw new ProtocolException(
                    String.format("checksum mismatch: got 0x%x, expected 0x%x", crcWire, expected));
        }
        byte[] rest = new byte[data.length - total];
        if (rest.length > 0) {
            System.arraycopy(data, total, rest, 0, rest.length);
        }
        return new Decode(new Frame(opcode, corr, payload, version, crcWire), rest);
    }

    /** Decode a complete frame. Throws if truncated, invalid, or trailing bytes remain. */
    public static Frame decode(byte[] data) {
        Decode d = tryDecode(data);
        if (d.frame == null) {
            throw new ProtocolException("incomplete frame");
        }
        if (d.rest.length > 0) {
            throw new ProtocolException("trailing bytes after frame: " + d.rest.length);
        }
        return d.frame;
    }

    /** Result of {@link #tryDecode(byte[])}. {@code frame} is null when more bytes are needed. */
    public static final class Decode {
        public final Frame frame;
        public final byte[] rest;

        public Decode(Frame frame, byte[] rest) {
            this.frame = frame;
            this.rest = rest == null ? new byte[0] : rest;
        }
    }
}
