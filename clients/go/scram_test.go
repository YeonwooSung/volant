package volant

import (
	"encoding/hex"
	"net"
	"testing"
	"time"

	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

func TestScramPinnedVector(t *testing.T) {
	proof, sig, err := ClientProofAndServerSig(
		"alice",
		"s3cret",
		"rOprNGfwEbeRWgbNEkqO",
		"rOprNGfwEbeRWgbNEkqOserver",
		[]byte("saltSALTsaltSALT"),
		4096,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantProof := "82aa6ee69043dd3c43785fba02fe220ea4a74a44b12d31b3a3a3ad17c1e0b5f3"
	wantSig := "d3068040897e7eaaa647e45356dab05074e5d48f6a283ec72a5181421768783d"
	if hex.EncodeToString(proof) != wantProof {
		t.Fatalf("proof %x want %s", proof, wantProof)
	}
	if hex.EncodeToString(sig) != wantSig {
		t.Fatalf("sig %x want %s", sig, wantSig)
	}
}

func TestScramSHA512ProofLen(t *testing.T) {
	proof, sig, err := ClientProofAndServerSigSHA512(
		"alice",
		"s3cret",
		"rOprNGfwEbeRWgbNEkqO",
		"rOprNGfwEbeRWgbNEkqOserver",
		[]byte("saltSALTsaltSALT"),
		4096,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(proof) != 64 || len(sig) != 64 {
		t.Fatalf("sha512 proof/sig len %d/%d want 64/64", len(proof), len(sig))
	}
	p256, _, err := ClientProofAndServerSig(
		"alice",
		"s3cret",
		"rOprNGfwEbeRWgbNEkqO",
		"rOprNGfwEbeRWgbNEkqOserver",
		[]byte("saltSALTsaltSALT"),
		4096,
	)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(proof[:32]) == hex.EncodeToString(p256) {
		t.Fatal("sha512 proof prefix must differ from sha256")
	}
}

func TestDialPlainTokenWinsOverScram(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	first := make(chan uint16, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 0, 4096)
		tmp := make([]byte, 4096)
		for {
			f, rest, err := frame.TryDecode(buf)
			if err != nil || (f == nil && err != nil) {
				return
			}
			if f == nil {
				n, rerr := conn.Read(tmp)
				if n > 0 {
					buf = append(buf, tmp[:n]...)
				}
				if rerr != nil {
					return
				}
				continue
			}
			buf = append([]byte(nil), rest...)
			select {
			case first <- f.Opcode:
			default:
			}
			switch f.Opcode {
			case codec.OpAuth:
				payload, _ := codec.EncodeAuthResponse(codec.AuthResponse{ErrorCode: 0})
				raw, _ := frame.Encode(codec.OpAuthResponse, f.CorrelationID, payload)
				_, _ = conn.Write(raw)
			case codec.OpMetadata:
				payload, _ := codec.EncodeMetadataResponse(codec.MetadataResponse{
					Brokers: []codec.BrokerInfo{{NodeID: 1, Host: "127.0.0.1", Port: 1}},
				})
				raw, _ := frame.Encode(codec.OpMetadata, f.CorrelationID, payload)
				_, _ = conn.Write(raw)
				return
			default:
				return
			}
		}
	}()
	c, err := dialPlain(ln.Addr().String(), 5*time.Second, "s3cret", "alice", "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	select {
	case op := <-first:
		if op != codec.OpAuth {
			t.Fatalf("first opcode %d want auth", op)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no first opcode")
	}
}
