"""Native Volant frame encode/decode.

On-wire header is 16 bytes, big-endian (matches `bytes::BufMut::put_u16` /
`put_u32` in `crates/volant-protocol/src/codec.rs`):

    magic u8 | version u8 | opcode u16 | correlation_id u32 | payload_len u32 | crc32 u32 | payload

CRC32 is IEEE of the **payload only** (`crc32fast` / `zlib.crc32`).
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass

FRAME_MAGIC = 0x56  # b'V'
PROTOCOL_VERSION = 1
HEADER_LEN = 16
MAX_PAYLOAD = 16 * 1024 * 1024

_HEADER_STRUCT = struct.Struct(">BBHIII")


class ProtocolError(Exception):
    """Magic, version, checksum, or framing error."""


@dataclass(frozen=True)
class Frame:
    opcode: int
    correlation_id: int
    payload: bytes
    version: int = PROTOCOL_VERSION
    checksum: int = 0


def checksum(payload: bytes) -> int:
    """IEEE CRC32 of `payload` (same polynomial as Rust `crc32fast::hash`)."""
    return zlib.crc32(payload) & 0xFFFFFFFF


def encode_frame(
    opcode: int,
    correlation_id: int,
    payload: bytes,
    *,
    version: int = PROTOCOL_VERSION,
) -> bytes:
    """Encode a complete frame (header + payload)."""
    if len(payload) > MAX_PAYLOAD:
        raise ProtocolError(f"payload too large: {len(payload)} > {MAX_PAYLOAD}")
    crc = checksum(payload)
    header = _HEADER_STRUCT.pack(
        FRAME_MAGIC, version, opcode, correlation_id, len(payload), crc
    )
    return header + payload


def try_decode_frame(data: bytes) -> tuple[Frame | None, bytes]:
    """Decode one frame from `data`.

    Returns `(None, data)` if more bytes are needed.
    Raises :class:`ProtocolError` on magic / version / checksum mismatch.
    """
    if len(data) < HEADER_LEN:
        return None, data
    magic, version, opcode, corr, payload_len, crc_wire = _HEADER_STRUCT.unpack_from(
        data, 0
    )
    if magic != FRAME_MAGIC:
        raise ProtocolError(f"invalid frame magic: {magic:#x}")
    if payload_len > MAX_PAYLOAD:
        raise ProtocolError(f"payload too large: {payload_len} > {MAX_PAYLOAD}")
    total = HEADER_LEN + payload_len
    if len(data) < total:
        return None, data
    payload = data[HEADER_LEN:total]
    if version != PROTOCOL_VERSION:
        raise ProtocolError(f"unsupported protocol version: {version}")
    expected = checksum(payload)
    if crc_wire != expected:
        raise ProtocolError(
            f"checksum mismatch: got {crc_wire:#x}, expected {expected:#x}"
        )
    frame = Frame(
        opcode=opcode,
        correlation_id=corr,
        payload=payload,
        version=version,
        checksum=crc_wire,
    )
    return frame, data[total:]


def decode_frame(data: bytes) -> Frame:
    """Decode a complete frame. Raises if truncated or invalid."""
    frame, rest = try_decode_frame(data)
    if frame is None:
        raise ProtocolError("incomplete frame")
    if rest:
        raise ProtocolError(f"trailing bytes after frame: {len(rest)}")
    return frame
