package frame

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestRoundtripPayload(t *testing.T) {
	raw, err := Encode(1, 7, []byte("ping"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != HeaderLen+4 {
		t.Fatalf("len=%d want %d", len(raw), HeaderLen+4)
	}
	f, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if f.Opcode != 1 {
		t.Fatalf("opcode=%d", f.Opcode)
	}
	if f.CorrelationID != 7 {
		t.Fatalf("corr=%d", f.CorrelationID)
	}
	if string(f.Payload) != "ping" {
		t.Fatalf("payload=%q", f.Payload)
	}
	if f.Version != ProtocolVersion {
		t.Fatalf("version=%d", f.Version)
	}
	if f.Checksum != Checksum([]byte("ping")) {
		t.Fatalf("checksum=%#x", f.Checksum)
	}
}

func TestChecksumIsIEEEPayloadOnly(t *testing.T) {
	if got := Checksum([]byte("ping")); got != 0x25D53DFD {
		t.Fatalf("ping crc=%#x want 0x25d53dfd", got)
	}
	if got := Checksum(nil); got != 0 {
		t.Fatalf("empty crc=%#x want 0", got)
	}
	if got := Checksum([]byte{}); got != 0 {
		t.Fatalf("empty-slice crc=%#x want 0", got)
	}
}

func TestHeaderIsBigEndian(t *testing.T) {
	raw, err := Encode(0x0102, 0x03040506, []byte("ab"))
	if err != nil {
		t.Fatal(err)
	}
	if raw[0] != Magic {
		t.Fatalf("magic=%#x", raw[0])
	}
	if raw[1] != ProtocolVersion {
		t.Fatalf("version=%d", raw[1])
	}
	if binary.BigEndian.Uint16(raw[2:4]) != 0x0102 {
		t.Fatalf("opcode=%#x", binary.BigEndian.Uint16(raw[2:4]))
	}
	if binary.BigEndian.Uint32(raw[4:8]) != 0x03040506 {
		t.Fatalf("corr=%#x", binary.BigEndian.Uint32(raw[4:8]))
	}
	if binary.BigEndian.Uint32(raw[8:12]) != 2 {
		t.Fatalf("plen=%d", binary.BigEndian.Uint32(raw[8:12]))
	}
	if binary.BigEndian.Uint32(raw[12:16]) != Checksum([]byte("ab")) {
		t.Fatalf("crc=%#x", binary.BigEndian.Uint32(raw[12:16]))
	}
	if string(raw[16:]) != "ab" {
		t.Fatalf("payload=%q", raw[16:])
	}
}

func TestChecksumMismatchRejected(t *testing.T) {
	raw, err := Encode(1, 1, []byte("ping"))
	if err != nil {
		t.Fatal(err)
	}
	raw[15] ^= 0xFF
	_, err = Decode(raw)
	if !isProtocol(err) || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("err=%v", err)
	}
}

func TestPayloadMutationRejected(t *testing.T) {
	raw, err := Encode(1, 1, []byte("ping"))
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0x01
	_, err = Decode(raw)
	if !isProtocol(err) || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("err=%v", err)
	}
}

func TestBadMagicRejected(t *testing.T) {
	raw, err := Encode(1, 1, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	raw[0] = 'X'
	_, err = Decode(raw)
	if !isProtocol(err) || !strings.Contains(err.Error(), "magic") {
		t.Fatalf("err=%v", err)
	}
}

func TestBadVersionRejected(t *testing.T) {
	raw, err := EncodeVersion(1, 1, []byte("x"), 2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Decode(raw)
	if !isProtocol(err) || !strings.Contains(err.Error(), "version") {
		t.Fatalf("err=%v", err)
	}
}

func TestIncompleteReturnsNil(t *testing.T) {
	raw, err := Encode(2, 9, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	f, rest, err := TryDecode(raw[:10])
	if err != nil {
		t.Fatal(err)
	}
	if f != nil {
		t.Fatalf("expected incomplete, got %+v", f)
	}
	if len(rest) != 10 {
		t.Fatalf("rest len=%d", len(rest))
	}
	f, rest, err = TryDecode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if f == nil {
		t.Fatal("expected frame")
	}
	if len(rest) != 0 {
		t.Fatalf("rest=%q", rest)
	}
	if string(f.Payload) != "hello" {
		t.Fatalf("payload=%q", f.Payload)
	}
}

func TestEmptyPayload(t *testing.T) {
	raw, err := Encode(4, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	f, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Payload) != 0 {
		t.Fatalf("payload=%q", f.Payload)
	}
	if f.Checksum != 0 {
		t.Fatalf("checksum=%#x", f.Checksum)
	}
}

func TestTrailingBytesDecodeFrame(t *testing.T) {
	raw, err := Encode(1, 1, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, []byte("junk")...)
	_, err = Decode(raw)
	if !isProtocol(err) {
		t.Fatalf("err=%v", err)
	}
	f, rest, err := TryDecode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(f.Payload) != "x" {
		t.Fatalf("payload=%q", f.Payload)
	}
	if string(rest) != "junk" {
		t.Fatalf("rest=%q", rest)
	}
}

func isProtocol(err error) bool {
	var p *ProtocolError
	return errors.As(err, &p)
}
