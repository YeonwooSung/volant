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

type createPartitionsServerResult struct {
	topic      string
	totalCount uint32
	err        error
}

func serveCreatePartitions(t *testing.T, errorCode uint16, partitions uint32) (addr string, got *createPartitionsServerResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &createPartitionsServerResult{}
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
			if f.Opcode != codec.OpCreatePartitions {
				res.err = &frame.ProtocolError{Msg: "unexpected opcode"}
				return
			}
			req, err := codec.DecodeCreatePartitionsRequest(f.Payload)
			if err != nil {
				res.err = err
				return
			}
			res.topic = req.Topic
			res.totalCount = req.TotalCount
			newTotal := partitions
			if errorCode != 0 {
				newTotal = 0
			}
			payload, err := codec.EncodeCreatePartitionsResponse(codec.CreatePartitionsResponse{
				ErrorCode:  errorCode,
				Topic:      req.Topic,
				Partitions: newTotal,
			})
			if err != nil {
				res.err = err
				return
			}
			raw, err := frame.Encode(codec.OpCreatePartitionsResponse, f.CorrelationID, payload)
			if err != nil {
				res.err = err
				return
			}
			if _, err := conn.Write(raw); err != nil {
				res.err = err
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

func TestCreatePartitionsSuccessReturnsNewCount(t *testing.T) {
	addr, got, stop := serveCreatePartitions(t, 0, 4)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.CreatePartitions("events", 4)
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.topic != "events" || got.totalCount != 4 {
		t.Fatalf("wire request topic=%q totalCount=%d", got.topic, got.totalCount)
	}
	if out != 4 {
		t.Fatalf("parsed %d", out)
	}
}

func TestCreatePartitionsNonzeroErrorRaises(t *testing.T) {
	addr, _, stop := serveCreatePartitions(t, 2, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.CreatePartitions("missing", 4)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 2 || be.Op != "create_partitions" {
		t.Fatalf("broker error %+v", be)
	}
}
