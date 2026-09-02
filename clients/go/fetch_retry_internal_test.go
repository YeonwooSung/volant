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

// failOnceConn fails the first Write with a net.Error, then passes through.
// The underlying connection stays open so the retry can succeed.
type failOnceConn struct {
	net.Conn
	remaining int32
}

func (c *failOnceConn) Write(p []byte) (int, error) {
	if atomic.AddInt32(&c.remaining, -1) >= 0 {
		return 0, &net.OpError{Op: "write", Err: errors.New("injected transport")}
	}
	return c.Conn.Write(p)
}

func TestFetchRetriesTransportThenOk(t *testing.T) {
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
			if f.Opcode != codec.OpFetch {
				errCh <- fmt.Errorf("opcode %d want fetch", f.Opcode)
				return
			}
			payload, err := codec.EncodeFetchResponse(codec.FetchResponse{
				Topic: "t", Partition: 0, HighWatermark: 0, ErrorCode: 0,
			})
			if err != nil {
				errCh <- err
				return
			}
			raw, err := frame.Encode(codec.OpFetch, f.CorrelationID, payload)
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
	c.conn = &failOnceConn{Conn: c.conn, remaining: 1}

	recs, err := c.Fetch("t", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("records %d want 0", len(recs))
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}