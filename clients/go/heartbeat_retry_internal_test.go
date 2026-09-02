package volant

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

// hbFailOnceConn fails the first Write with a net.Error, then passes through.
type hbFailOnceConn struct {
	net.Conn
	remaining int32
}

func (c *hbFailOnceConn) Write(p []byte) (int, error) {
	if atomic.AddInt32(&c.remaining, -1) >= 0 {
		return 0, &net.OpError{Op: "write", Err: errors.New("injected transport")}
	}
	return c.Conn.Write(p)
}

func TestHeartbeatRetriesTransportThenOk(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		buf := []byte{}
		tmp := make([]byte, 4096)
		for {
			f, rest, err := frame.TryDecode(buf)
			if err != nil {
				errCh <- err
				return
			}
			buf = append([]byte(nil), rest...)
			if f == nil {
				n, err := conn.Read(tmp)
				if n > 0 {
					buf = append(buf, tmp[:n]...)
				}
				if err != nil {
					errCh <- err
					return
				}
				continue
			}
			if f.Opcode != codec.OpHeartbeat {
				errCh <- fmt.Errorf("opcode %d want heartbeat", f.Opcode)
				return
			}
			payload, err := codec.EncodeHeartbeatResponse(codec.HeartbeatResponse{ErrorCode: 0})
			if err != nil {
				errCh <- err
				return
			}
			raw, err := frame.Encode(codec.OpHeartbeat, f.CorrelationID, payload)
			if err != nil {
				errCh <- err
				return
			}
			if _, err := conn.Write(raw); err != nil {
				errCh <- err
				return
			}
			errCh <- nil
			return
		}
	}()

	c, err := DialTimeout(ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(1)
	c.SetRetryBackoff(0)
	c.conn = &hbFailOnceConn{Conn: c.conn, remaining: 1}

	if err := c.Heartbeat("g", "m1", 1); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}
