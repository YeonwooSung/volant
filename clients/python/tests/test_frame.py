"""Frame roundtrip and checksum rejection (stdlib unittest; pytest-compatible)."""

from __future__ import annotations

import struct
import unittest
import zlib

from volant.frame import (
    FRAME_MAGIC,
    HEADER_LEN,
    PROTOCOL_VERSION,
    ProtocolError,
    checksum,
    decode_frame,
    encode_frame,
    try_decode_frame,
)


class TestFrame(unittest.TestCase):
    def test_roundtrip_payload(self) -> None:
        raw = encode_frame(1, 7, b"ping")
        self.assertEqual(len(raw), HEADER_LEN + 4)
        frame = decode_frame(raw)
        self.assertEqual(frame.opcode, 1)
        self.assertEqual(frame.correlation_id, 7)
        self.assertEqual(frame.payload, b"ping")
        self.assertEqual(frame.version, PROTOCOL_VERSION)
        self.assertEqual(frame.checksum, checksum(b"ping"))

    def test_checksum_is_ieee_payload_only(self) -> None:
        self.assertEqual(checksum(b"ping"), zlib.crc32(b"ping") & 0xFFFFFFFF)
        self.assertEqual(checksum(b"ping"), 0x25D53DFD)
        self.assertEqual(checksum(b""), 0)

    def test_header_is_big_endian(self) -> None:
        raw = encode_frame(0x0102, 0x03040506, b"ab")
        magic, version, opcode, corr, plen, crc = struct.unpack_from(">BBHIII", raw, 0)
        self.assertEqual(magic, FRAME_MAGIC)
        self.assertEqual(version, PROTOCOL_VERSION)
        self.assertEqual(opcode, 0x0102)
        self.assertEqual(corr, 0x03040506)
        self.assertEqual(plen, 2)
        self.assertEqual(crc, checksum(b"ab"))
        self.assertEqual(raw[16:], b"ab")

    def test_checksum_mismatch_rejected(self) -> None:
        raw = bytearray(encode_frame(1, 1, b"ping"))
        # Flip a CRC byte in the header (bytes 12..16).
        raw[15] ^= 0xFF
        with self.assertRaises(ProtocolError) as ctx:
            decode_frame(bytes(raw))
        self.assertIn("checksum mismatch", str(ctx.exception))

    def test_payload_mutation_rejected(self) -> None:
        raw = bytearray(encode_frame(1, 1, b"ping"))
        raw[-1] ^= 0x01
        with self.assertRaises(ProtocolError) as ctx:
            decode_frame(bytes(raw))
        self.assertIn("checksum mismatch", str(ctx.exception))

    def test_bad_magic_rejected(self) -> None:
        raw = bytearray(encode_frame(1, 1, b"x"))
        raw[0] = ord("X")
        with self.assertRaises(ProtocolError) as ctx:
            decode_frame(bytes(raw))
        self.assertIn("magic", str(ctx.exception))

    def test_bad_version_rejected(self) -> None:
        raw = bytearray(encode_frame(1, 1, b"x", version=2))
        # encode_frame writes version 2; decode must reject.
        with self.assertRaises(ProtocolError) as ctx:
            decode_frame(bytes(raw))
        self.assertIn("version", str(ctx.exception))

    def test_incomplete_returns_none(self) -> None:
        raw = encode_frame(2, 9, b"hello")
        frame, rest = try_decode_frame(raw[:10])
        self.assertIsNone(frame)
        self.assertEqual(rest, raw[:10])
        frame, rest = try_decode_frame(raw)
        self.assertIsNotNone(frame)
        self.assertEqual(rest, b"")
        self.assertEqual(frame.payload, b"hello")

    def test_empty_payload(self) -> None:
        raw = encode_frame(4, 1, b"")
        frame = decode_frame(raw)
        self.assertEqual(frame.payload, b"")
        self.assertEqual(frame.checksum, 0)

    def test_trailing_bytes_decode_frame(self) -> None:
        raw = encode_frame(1, 1, b"x") + b"junk"
        with self.assertRaises(ProtocolError):
            decode_frame(raw)
        frame, rest = try_decode_frame(raw)
        self.assertEqual(frame.payload, b"x")
        self.assertEqual(rest, b"junk")


if __name__ == "__main__":
    unittest.main()
