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

type listOffsetsServerResult struct {
	topic      string
	partitions []uint32
	err        error
}

func serveListOffsets(t *testing.T, errorCode uint16, entries []codec.OffsetListing) (addr string, got *listOffsetsServerResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &listOffsetsServerResult{}
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
			if f.Opcode != codec.OpListOffsets {
				res.err = &frame.ProtocolError{Msg: "unexpected opcode"}
				return
			}
			req, err := codec.DecodeListOffsetsRequest(f.Payload)
			if err != nil {
				res.err = err
				return
			}
			res.topic = req.Topic
			res.partitions = req.Partitions
			payload, err := codec.EncodeListOffsetsResponse(codec.ListOffsetsResponse{
				ErrorCode: errorCode,
				Topic:     req.Topic,
				Entries:   entries,
			})
			if err != nil {
				res.err = err
				return
			}
			raw, err := frame.Encode(codec.OpListOffsetsResponse, f.CorrelationID, payload)
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

func TestListOffsetsEmptyPartitionsEncodedAsCountZero(t *testing.T) {
	entries := []codec.OffsetListing{
		{Partition: 0, Earliest: 0, Latest: 10},
		{Partition: 1, Earliest: 2, Latest: 5},
	}
	addr, got, stop := serveListOffsets(t, 0, entries)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.ListOffsets("events", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.topic != "events" || len(got.partitions) != 0 {
		t.Fatalf("wire request topic=%q partitions=%v", got.topic, got.partitions)
	}
	if len(out) != 2 || out[0] != (volant.OffsetListing{Partition: 0, Earliest: 0, Latest: 10}) {
		t.Fatalf("parsed %+v", out)
	}
	if out[1] != (volant.OffsetListing{Partition: 1, Earliest: 2, Latest: 5}) {
		t.Fatalf("parsed entry1 %+v", out[1])
	}
}

func TestListOffsetsExplicitPartitions(t *testing.T) {
	entries := []codec.OffsetListing{{Partition: 0, Earliest: 0, Latest: 10}}
	addr, got, stop := serveListOffsets(t, 0, entries)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.ListOffsets("events", []uint32{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if len(got.partitions) != 2 || got.partitions[0] != 0 || got.partitions[1] != 1 {
		t.Fatalf("partitions %v", got.partitions)
	}
	if len(out) != 1 || out[0].Latest != 10 {
		t.Fatalf("parsed %+v", out)
	}
}

func TestListOffsetsNonzeroErrorRaises(t *testing.T) {
	addr, _, stop := serveListOffsets(t, 2, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.ListOffsets("missing", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 2 || be.Op != "list_offsets" {
		t.Fatalf("broker error %+v", be)
	}
}
