// Package frame implements the native Volant 16-byte frame header.
//
// On-wire layout is big-endian (matches crates/volant-protocol/src/codec.rs):
//
//	magic u8 | version u8 | opcode u16 | correlation_id u32 | payload_len u32 | crc32 u32 | payload
//
// CRC32 is IEEE of the payload only (hash/crc32 ≡ zlib.crc32 ≡ crc32fast).
package frame

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	// Magic is the frame identifier byte 'V'.
	Magic = 0x56
	// ProtocolVersion is the only accepted header version.
	ProtocolVersion = 1
	// HeaderLen is the on-wire header size in bytes.
	HeaderLen = 16
	// MaxPayload is the 16 MiB payload cap.
	MaxPayload = 16 * 1024 * 1024
)

// ProtocolError is a magic, version, checksum, or framing error.
type ProtocolError struct {
	Msg string
}

func (e *ProtocolError) Error() string { return e.Msg }

// Frame is a decoded native-protocol frame.
type Frame struct {
	Opcode        uint16
	CorrelationID uint32
	Payload       []byte
	Version       uint8
	Checksum      uint32
}

// Checksum returns the IEEE CRC32 of payload (same polynomial as crc32fast).
func Checksum(payload []byte) uint32 {
	return crc32.ChecksumIEEE(payload)
}

// Encode writes a complete frame (header + payload) at protocol version 1.
func Encode(opcode uint16, correlationID uint32, payload []byte) ([]byte, error) {
	return EncodeVersion(opcode, correlationID, payload, ProtocolVersion)
}

// EncodeVersion writes a frame with an explicit version (tests use this to
// produce a rejected version byte).
func EncodeVersion(opcode uint16, correlationID uint32, payload []byte, version uint8) ([]byte, error) {
	if len(payload) > MaxPayload {
		return nil, &ProtocolError{Msg: fmt.Sprintf("payload too large: %d > %d", len(payload), MaxPayload)}
	}
	crc := Checksum(payload)
	out := make([]byte, HeaderLen+len(payload))
	out[0] = Magic
	out[1] = version
	binary.BigEndian.PutUint16(out[2:4], opcode)
	binary.BigEndian.PutUint32(out[4:8], correlationID)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint32(out[12:16], crc)
	copy(out[HeaderLen:], payload)
	return out, nil
}

// TryDecode parses one frame from data.
//
// Returns (nil, data, nil) if more bytes are needed.
// Returns a non-nil error on magic / version / checksum mismatch.
func TryDecode(data []byte) (*Frame, []byte, error) {
	if len(data) < HeaderLen {
		return nil, data, nil
	}
	magic := data[0]
	version := data[1]
	opcode := binary.BigEndian.Uint16(data[2:4])
	corr := binary.BigEndian.Uint32(data[4:8])
	payloadLen := binary.BigEndian.Uint32(data[8:12])
	crcWire := binary.BigEndian.Uint32(data[12:16])
	if magic != Magic {
		return nil, data, &ProtocolError{Msg: fmt.Sprintf("invalid frame magic: %#x", magic)}
	}
	if payloadLen > MaxPayload {
		return nil, data, &ProtocolError{Msg: fmt.Sprintf("payload too large: %d > %d", payloadLen, MaxPayload)}
	}
	total := HeaderLen + int(payloadLen)
	if len(data) < total {
		return nil, data, nil
	}
	payload := make([]byte, payloadLen)
	copy(payload, data[HeaderLen:total])
	if version != ProtocolVersion {
		return nil, data, &ProtocolError{Msg: fmt.Sprintf("unsupported protocol version: %d", version)}
	}
	expected := Checksum(payload)
	if crcWire != expected {
		return nil, data, &ProtocolError{Msg: fmt.Sprintf("checksum mismatch: got %#x, expected %#x", crcWire, expected)}
	}
	return &Frame{
		Opcode:        opcode,
		CorrelationID: corr,
		Payload:       payload,
		Version:       version,
		Checksum:      crcWire,
	}, data[total:], nil
}

// Decode parses a complete frame. It errors if the buffer is truncated,
// invalid, or has trailing bytes after one frame.
func Decode(data []byte) (*Frame, error) {
	f, rest, err := TryDecode(data)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, &ProtocolError{Msg: "incomplete frame"}
	}
	if len(rest) > 0 {
		return nil, &ProtocolError{Msg: fmt.Sprintf("trailing bytes after frame: %d", len(rest))}
	}
	return f, nil
}
