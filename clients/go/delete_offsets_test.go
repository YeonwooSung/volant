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

type deleteOffsetsServerResult struct {
	group   string
	entries []codec.OffsetEntry
	err     error
}

func serveDeleteOffsets(t *testing.T, errorCode uint16, deletedCount uint32) (addr string, got *deleteOffsetsServerResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &deleteOffsetsServerResult{}
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
			if f.Opcode != codec.OpDeleteOffsets {
				res.err = &frame.ProtocolError{Msg: "unexpected opcode"}
				return
			}
			req, err := codec.DecodeDeleteOffsetsRequest(f.Payload)
			if err != nil {
				res.err = err
				return
			}
			res.group = req.GroupID
			res.entries = req.Entries
			payload, err := codec.EncodeDeleteOffsetsResponse(codec.DeleteOffsetsResponse{
				ErrorCode:    errorCode,
				DeletedCount: deletedCount,
			})
			if err != nil {
				res.err = err
				return
			}
			raw, err := frame.Encode(codec.OpDeleteOffsetsResponse, f.CorrelationID, payload)
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

func TestDeleteOffsetsEmptyEntriesEncodedAsCountZero(t *testing.T) {
	addr, got, stop := serveDeleteOffsets(t, 0, 3)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.DeleteOffsets("g", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.group != "g" || len(got.entries) != 0 {
		t.Fatalf("wire request group=%q entries=%v", got.group, got.entries)
	}
	if out != 3 {
		t.Fatalf("deleted_count %d", out)
	}
}

func TestDeleteOffsetsExplicitEntry(t *testing.T) {
	addr, got, stop := serveDeleteOffsets(t, 0, 1)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.DeleteOffsets("g", []codec.OffsetEntry{{Topic: "events", Partition: 0}})
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.group != "g" || len(got.entries) != 1 || got.entries[0].Topic != "events" || got.entries[0].Partition != 0 {
		t.Fatalf("entries %v", got.entries)
	}
	if out != 1 {
		t.Fatalf("deleted_count %d", out)
	}
}

func TestDeleteOffsetsNonzeroErrorRaises(t *testing.T) {
	addr, _, stop := serveDeleteOffsets(t, 2, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.DeleteOffsets("missing", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 2 || be.Op != "delete_offsets" {
		t.Fatalf("broker error %+v", be)
	}
}
