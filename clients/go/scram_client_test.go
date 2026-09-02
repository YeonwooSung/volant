package volant_test

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	volant "github.com/volant-mq/volant/clients/go"
	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

const (
	scramUser    = "alice"
	scramPass    = "s3cret"
	scramSaltStr = "saltSALTsaltSALT"
	scramIters   = 4096
)

type scramServerResult struct {
	opcodes        []uint16
	firstUsernames []string
	finalUsernames []string
	token          string
	err            error
}

func serveScram(t *testing.T, password string, badSig bool, conns int) (addr string, got *scramServerResult, stop func()) {
	t.Helper()
	if conns < 1 {
		conns = 1
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &scramServerResult{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < conns; i++ {
			conn, err := ln.Accept()
			if err != nil {
				res.err = err
				return
			}
			if err := handleScramConn(conn, password, badSig, res); err != nil {
				res.err = err
				_ = conn.Close()
				return
			}
			_ = conn.Close()
		}
	}()
	return ln.Addr().String(), res, func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

func handleScramConn(conn net.Conn, password string, badSig bool, res *scramServerResult) error {
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		f, rest, err := frame.TryDecode(buf)
		if err != nil {
			return err
		}
		if f == nil {
			n, err := conn.Read(tmp)
			if n > 0 {
				buf = append(buf, tmp[:n]...)
			}
			if err != nil {
				return err
			}
			continue
		}
		buf = append([]byte(nil), rest...)
		res.opcodes = append(res.opcodes, f.Opcode)
		switch f.Opcode {
		case codec.OpAuth:
			req, err := codec.DecodeAuthRequest(f.Payload)
			if err != nil {
				return err
			}
			res.token = req.Token
			payload, err := codec.EncodeAuthResponse(codec.AuthResponse{ErrorCode: 0})
			if err != nil {
				return err
			}
			raw, err := frame.Encode(codec.OpAuthResponse, f.CorrelationID, payload)
			if err != nil {
				return err
			}
			if _, err := conn.Write(raw); err != nil {
				return err
			}
		case codec.OpScramFirst:
			req, err := codec.DecodeScramFirstRequest(f.Payload)
			if err != nil {
				return err
			}
			res.firstUsernames = append(res.firstUsernames, req.Username)
			combined := req.ClientNonce + "s"
			payload, err := codec.EncodeScramFirstResponse(codec.ScramFirstResponse{
				ErrorCode:     0,
				CombinedNonce: combined,
				Salt:          []byte(scramSaltStr),
				Iterations:    scramIters,
			})
			if err != nil {
				return err
			}
			raw, err := frame.Encode(codec.OpScramFirstResponse, f.CorrelationID, payload)
			if err != nil {
				return err
			}
			if _, err := conn.Write(raw); err != nil {
				return err
			}
		case codec.OpScramFinal:
			req, err := codec.DecodeScramFinalRequest(f.Payload)
			if err != nil {
				return err
			}
			res.finalUsernames = append(res.finalUsernames, req.Username)
			clientNonce := ""
			if len(req.CombinedNonce) > 0 && req.CombinedNonce[len(req.CombinedNonce)-1] == 's' {
				clientNonce = req.CombinedNonce[:len(req.CombinedNonce)-1]
			}
			proof, sig, err := volant.ClientProofAndServerSig(
				req.Username, password, clientNonce, req.CombinedNonce, []byte(scramSaltStr), scramIters,
			)
			if err != nil {
				return err
			}
			code := uint16(0)
			outSig := sig
			if !bytes.Equal(req.ClientProof, proof) {
				code = 17
			} else if badSig {
				outSig = make([]byte, 32)
			}
			payload, err := codec.EncodeScramFinalResponse(codec.ScramFinalResponse{
				ErrorCode:       code,
				ServerSignature: outSig,
			})
			if err != nil {
				return err
			}
			raw, err := frame.Encode(codec.OpScramFinalResponse, f.CorrelationID, payload)
			if err != nil {
				return err
			}
			if _, err := conn.Write(raw); err != nil {
				return err
			}
			if code != 0 || badSig {
				return nil
			}
		case codec.OpMetadata:
			payload, err := codec.EncodeMetadataResponse(codec.MetadataResponse{
				Brokers: []codec.BrokerInfo{{NodeID: 1, Host: "127.0.0.1", Port: 1}},
			})
			if err != nil {
				return err
			}
			raw, err := frame.Encode(codec.OpMetadata, f.CorrelationID, payload)
			if err != nil {
				return err
			}
			_, _ = conn.Write(raw)
			return nil
		default:
			return nil
		}
	}
}

func TestDialScramSendsFirstAndFinal(t *testing.T) {
	addr, got, stop := serveScram(t, scramPass, false, 1)
	defer stop()
	c, err := volant.DialScram(addr, scramUser, scramPass)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if len(got.opcodes) < 2 || got.opcodes[0] != codec.OpScramFirst || got.opcodes[1] != codec.OpScramFinal {
		t.Fatalf("opcodes %v", got.opcodes)
	}
	if len(got.firstUsernames) != 1 || got.firstUsernames[0] != scramUser {
		t.Fatalf("first users %v", got.firstUsernames)
	}
}

func TestDialScramBadPassword(t *testing.T) {
	addr, got, stop := serveScram(t, scramPass, false, 1)
	defer stop()
	_, err := volant.DialScram(addr, scramUser, "wrong")
	if err == nil {
		t.Fatal("expected error")
	}
	var be *codec.BrokerError
	if !errors.As(err, &be) || be.Code != 17 || be.Op != "scram final" {
		t.Fatalf("err=%v", err)
	}
	if len(got.opcodes) < 2 {
		t.Fatalf("opcodes %v", got.opcodes)
	}
}

func TestDialScramSignatureMismatch(t *testing.T) {
	addr, _, stop := serveScram(t, scramPass, true, 1)
	defer stop()
	_, err := volant.DialScram(addr, scramUser, scramPass)
	if err == nil {
		t.Fatal("expected signature mismatch")
	}
	var pe *frame.ProtocolError
	if !errors.As(err, &pe) || pe.Msg != "scram server signature mismatch" {
		t.Fatalf("err=%v", err)
	}
}

func TestDialNoCredsSkipsAuthAndScram(t *testing.T) {
	addr, got, stop := serveScram(t, scramPass, false, 1)
	defer stop()
	c, err := volant.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if len(got.opcodes) != 1 || got.opcodes[0] != codec.OpMetadata {
		t.Fatalf("opcodes %v", got.opcodes)
	}
	if got.token != "" || len(got.firstUsernames) != 0 {
		t.Fatalf("unexpected auth/scram %+v", got)
	}
}

func TestDialAuthWinsOverScram(t *testing.T) {
	addr, got, stop := serveScram(t, scramPass, false, 1)
	defer stop()
	c, err := volant.DialAuth(addr, "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if got.opcodes[0] != codec.OpAuth {
		t.Fatalf("first opcode %d", got.opcodes[0])
	}
	for _, op := range got.opcodes {
		if op == codec.OpScramFirst || op == codec.OpScramFinal {
			t.Fatalf("unexpected scram opcode in %v", got.opcodes)
		}
	}
}

func TestDialScramIncompleteCreds(t *testing.T) {
	if _, err := volant.DialScram("127.0.0.1:1", "alice", ""); err == nil {
		t.Fatal("expected error")
	}
	if _, err := volant.DialScram("127.0.0.1:1", "", "s3cret"); err == nil {
		t.Fatal("expected error")
	}
}
