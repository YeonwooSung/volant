package volant_test

import (
	"errors"
	"net"
	"testing"
	"time"

	volant "github.com/volant-mq/volant/clients/go"
	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

type authServerResult struct {
	firstOpcode uint16
	token       string
	authCount   int
	err         error
}

func serveAuth(t *testing.T, replyCode uint16, alsoMeta bool) (addr string, got *authServerResult, stop func()) {
	t.Helper()
	return serveAuthQueue(t, []uint16{replyCode}, alsoMeta)
}

func serveAuthQueue(t *testing.T, codes []uint16, alsoMeta bool) (addr string, got *authServerResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	queued := append([]uint16(nil), codes...)
	res := &authServerResult{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			res.err = err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 0, 4096)
		tmp := make([]byte, 4096)
		for {
			f, rest, err := frame.TryDecode(buf)
			if err != nil {
				res.err = err
				return
			}
			if f == nil {
				n, err := conn.Read(tmp)
				if n > 0 {
					buf = append(buf, tmp[:n]...)
				}
				if err != nil {
					res.err = err
					return
				}
				continue
			}
			buf = append([]byte(nil), rest...)
			if res.firstOpcode == 0 {
				res.firstOpcode = f.Opcode
			}
			switch f.Opcode {
			case codec.OpAuth:
				req, err := codec.DecodeAuthRequest(f.Payload)
				if err != nil {
					res.err = err
					return
				}
				res.token = req.Token
				res.authCount++
				code := uint16(0)
				if len(queued) > 0 {
					code = queued[0]
					queued = queued[1:]
				}
				payload, err := codec.EncodeAuthResponse(codec.AuthResponse{ErrorCode: code})
				if err != nil {
					res.err = err
					return
				}
				raw, err := frame.Encode(codec.OpAuthResponse, f.CorrelationID, payload)
				if err != nil {
					res.err = err
					return
				}
				if _, err := conn.Write(raw); err != nil {
					res.err = err
					return
				}
				// Multi-code queue stays on the same socket so Auth can retry.
				if len(codes) > 1 {
					continue
				}
				if code != 0 || !alsoMeta {
					return
				}
			case codec.OpMetadata:
				payload, err := codec.EncodeMetadataResponse(codec.MetadataResponse{
					Brokers: []codec.BrokerInfo{{NodeID: 1, Host: "127.0.0.1", Port: 1}},
				})
				if err != nil {
					res.err = err
					return
				}
				raw, err := frame.Encode(codec.OpMetadata, f.CorrelationID, payload)
				if err != nil {
					res.err = err
					return
				}
				_, _ = conn.Write(raw)
				return
			default:
				return
			}
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

func TestDialAuthSendsToken(t *testing.T) {
	addr, got, stop := serveAuth(t, 0, true)
	defer stop()
	c, err := volant.DialAuth(addr, "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if got.firstOpcode != codec.OpAuth {
		t.Fatalf("first opcode %d want %d", got.firstOpcode, codec.OpAuth)
	}
	if got.token != "s3cret" {
		t.Fatalf("token %q", got.token)
	}
}

func TestDialAuthRejected(t *testing.T) {
	addr, got, stop := serveAuth(t, 17, false)
	defer stop()
	_, err := volant.DialAuth(addr, "nope")
	if err == nil {
		t.Fatal("expected auth error")
	}
	var be *codec.BrokerError
	if !errors.As(err, &be) || be.Code != 17 || be.Op != "auth" {
		t.Fatalf("err=%v", err)
	}
	if got.token != "nope" {
		t.Fatalf("token %q", got.token)
	}
}

func TestDialNoTokenSkipsAuth(t *testing.T) {
	addr, got, stop := serveAuth(t, 0, true)
	defer stop()
	c, err := volant.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if got.firstOpcode != codec.OpMetadata {
		t.Fatalf("first opcode %d want metadata", got.firstOpcode)
	}
	if got.token != "" {
		t.Fatalf("unexpected token %q", got.token)
	}
}

func TestDialAuthEmptyTokenSkipsAuth(t *testing.T) {
	addr, got, stop := serveAuth(t, 0, true)
	defer stop()
	c, err := volant.DialAuth(addr, "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if got.firstOpcode != codec.OpMetadata {
		t.Fatalf("first opcode %d want metadata", got.firstOpcode)
	}
}

const (
	authTimeout    uint16 = 7
	authFailed     uint16 = 17
)

func TestDialAuthDefaultMaxRetriesZeroRaisesOnTimeout(t *testing.T) {
	addr, got, stop := serveAuthQueue(t, []uint16{authTimeout}, false)
	defer stop()
	_, err := volant.DialAuth(addr, "s3cret")
	if err == nil {
		t.Fatal("expected auth timeout")
	}
	var be *codec.BrokerError
	if !errors.As(err, &be) || be.Code != authTimeout || be.Op != "auth" {
		t.Fatalf("err=%v", err)
	}
	if got.authCount != 1 {
		t.Fatalf("auth count %d want 1", got.authCount)
	}
}

func TestDialAuthRetriesTimeoutThenOk(t *testing.T) {
	addr, got, stop := serveAuthQueue(t, []uint16{authTimeout, 0}, true)
	defer stop()
	c, err := volant.DialAuthRetries(addr, "s3cret", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if got.authCount != 2 {
		t.Fatalf("auth count %d want 2", got.authCount)
	}
	if got.token != "s3cret" {
		t.Fatalf("token %q", got.token)
	}
}

func TestDialAuthFailedNotRetried(t *testing.T) {
	addr, got, stop := serveAuthQueue(t, []uint16{authFailed}, false)
	defer stop()
	_, err := volant.DialAuthRetries(addr, "nope", 2, 0)
	if err == nil {
		t.Fatal("expected auth failed")
	}
	var be *codec.BrokerError
	if !errors.As(err, &be) || be.Code != authFailed || be.Op != "auth" {
		t.Fatalf("err=%v", err)
	}
	if got.authCount != 1 {
		t.Fatalf("auth count %d want 1", got.authCount)
	}
}

func TestDialAuthExhaustedRetriesRaises(t *testing.T) {
	addr, got, stop := serveAuthQueue(t, []uint16{authTimeout, authTimeout, authTimeout}, false)
	defer stop()
	_, err := volant.DialAuthRetries(addr, "s3cret", 2, 0)
	if err == nil {
		t.Fatal("expected auth timeout")
	}
	var be *codec.BrokerError
	if !errors.As(err, &be) || be.Code != authTimeout || be.Op != "auth" {
		t.Fatalf("err=%v", err)
	}
	if got.authCount != 3 {
		t.Fatalf("auth count %d want 3", got.authCount)
	}
}
