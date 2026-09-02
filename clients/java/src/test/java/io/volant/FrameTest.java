package io.volant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Frame roundtrip and checksum rejection (matches Python/Go fixtures). */
class FrameTest {
    @Test
    void roundtripPayload() {
        byte[] raw = Frame.encode(1, 7, "ping".getBytes(StandardCharsets.US_ASCII));
        assertEquals(Frame.HEADER_LEN + 4, raw.length);
        Frame frame = Frame.decode(raw);
        assertEquals(1, frame.opcode);
        assertEquals(7, frame.correlationId);
        assertArrayEquals("ping".getBytes(StandardCharsets.US_ASCII), frame.payload);
        assertEquals(Frame.PROTOCOL_VERSION, frame.version);
        assertEquals(Frame.checksum("ping".getBytes(StandardCharsets.US_ASCII)), frame.checksum);
    }

    @Test
    void checksumIsIeeePayloadOnly() {
        assertEquals(0x25D53DFDL, Frame.checksum("ping".getBytes(StandardCharsets.US_ASCII)));
        assertEquals(0L, Frame.checksum(new byte[0]));
        assertEquals(0L, Frame.checksum(null));
    }

    @Test
    void headerIsBigEndian() {
        byte[] raw = Frame.encode(0x0102, 0x03040506L, "ab".getBytes(StandardCharsets.US_ASCII));
        ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        assertEquals(Frame.MAGIC, buf.get() & 0xFF);
        assertEquals(Frame.PROTOCOL_VERSION, buf.get() & 0xFF);
        assertEquals(0x0102, buf.getShort() & 0xFFFF);
        assertEquals(0x03040506L, buf.getInt() & 0xFFFFFFFFL);
        assertEquals(2, buf.getInt());
        assertEquals(Frame.checksum("ab".getBytes(StandardCharsets.US_ASCII)), buf.getInt() & 0xFFFFFFFFL);
        byte[] payload = new byte[2];
        System.arraycopy(raw, 16, payload, 0, 2);
        assertArrayEquals("ab".getBytes(StandardCharsets.US_ASCII), payload);
    }

    @Test
    void checksumMismatchRejected() {
        byte[] raw = Frame.encode(1, 1, "ping".getBytes(StandardCharsets.US_ASCII));
        raw[15] ^= (byte) 0xFF;
        ProtocolException ex = assertThrows(ProtocolException.class, () -> Frame.decode(raw));
        assertTrue(ex.getMessage().contains("checksum mismatch"), ex.getMessage());
    }

    @Test
    void payloadMutationRejected() {
        byte[] raw = Frame.encode(1, 1, "ping".getBytes(StandardCharsets.US_ASCII));
        raw[raw.length - 1] ^= 0x01;
        ProtocolException ex = assertThrows(ProtocolException.class, () -> Frame.decode(raw));
        assertTrue(ex.getMessage().contains("checksum mismatch"), ex.getMessage());
    }

    @Test
    void badMagicRejected() {
        byte[] raw = Frame.encode(1, 1, "x".getBytes(StandardCharsets.US_ASCII));
        raw[0] = 'X';
        ProtocolException ex = assertThrows(ProtocolException.class, () -> Frame.decode(raw));
        assertTrue(ex.getMessage().contains("magic"), ex.getMessage());
    }

    @Test
    void badVersionRejected() {
        byte[] raw = Frame.encode(1, 1, "x".getBytes(StandardCharsets.US_ASCII), 2);
        ProtocolException ex = assertThrows(ProtocolException.class, () -> Frame.decode(raw));
        assertTrue(ex.getMessage().contains("version"), ex.getMessage());
    }

    @Test
    void incompleteReturnsNull() {
        byte[] raw = Frame.encode(2, 9, "hello".getBytes(StandardCharsets.US_ASCII));
        byte[] partial = new byte[10];
        System.arraycopy(raw, 0, partial, 0, 10);
        Frame.Decode d = Frame.tryDecode(partial);
        assertNull(d.frame);
        assertArrayEquals(partial, d.rest);
        d = Frame.tryDecode(raw);
        assertNotNull(d.frame);
        assertEquals(0, d.rest.length);
        assertArrayEquals("hello".getBytes(StandardCharsets.US_ASCII), d.frame.payload);
    }

    @Test
    void emptyPayload() {
        byte[] raw = Frame.encode(4, 1, new byte[0]);
        Frame frame = Frame.decode(raw);
        assertEquals(0, frame.payload.length);
        assertEquals(0L, frame.checksum);
    }

    @Test
    void trailingBytesDecodeFrame() {
        byte[] frame = Frame.encode(1, 1, "x".getBytes(StandardCharsets.US_ASCII));
        byte[] raw = new byte[frame.length + 4];
        System.arraycopy(frame, 0, raw, 0, frame.length);
        System.arraycopy("junk".getBytes(StandardCharsets.US_ASCII), 0, raw, frame.length, 4);
        assertThrows(ProtocolException.class, () -> Frame.decode(raw));
        Frame.Decode d = Frame.tryDecode(raw);
        assertArrayEquals("x".getBytes(StandardCharsets.US_ASCII), d.frame.payload);
        assertArrayEquals("junk".getBytes(StandardCharsets.US_ASCII), d.rest);
    }
}
