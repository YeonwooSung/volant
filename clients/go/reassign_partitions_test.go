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

type reassignPartitionsServerResult struct {
	topic     string
	partition uint32
	replicas  []uint32
	err       error
}

func serveReassignPartitions(t *testing.T, errorCode uint16, generation uint32) (addr string, got *reassignPartitionsServerResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &reassignPartitionsServerResult{}
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
			if f.Opcode != codec.OpReassignPartitions {
				res.err = &frame.ProtocolError{Msg: "unexpected opcode"}
				return
			}
			req, err := codec.DecodeReassignPartitionsRequest(f.Payload)
			if err != nil {
				res.err = err
				return
			}
			res.topic = req.Topic
			res.partition = req.Partition
			res.replicas = req.Replicas
			gen := generation
			if errorCode != 0 {
				gen = 0
			}
			payload, err := codec.EncodeReassignPartitionsResponse(codec.ReassignPartitionsResponse{
				ErrorCode:  errorCode,
				Generation: gen,
			})
			if err != nil {
				res.err = err
				return
			}
			raw, err := frame.Encode(codec.OpReassignPartitionsResponse, f.CorrelationID, payload)
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

func TestReassignPartitionsSuccessReturnsGeneration(t *testing.T) {
	addr, got, stop := serveReassignPartitions(t, 0, 7)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	part := uint32(0)
	out, err := c.ReassignPartitions("events", []uint32{1, 2}, &part)
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.topic != "events" || got.partition != 0 || len(got.replicas) != 2 || got.replicas[0] != 1 || got.replicas[1] != 2 {
		t.Fatalf("wire request topic=%q partition=%d replicas=%v", got.topic, got.partition, got.replicas)
	}
	if out != 7 {
		t.Fatalf("parsed %d", out)
	}
}

func TestReassignPartitionsNilPartitionEncodesAll(t *testing.T) {
	addr, got, stop := serveReassignPartitions(t, 0, 3)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.ReassignPartitions("events", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.topic != "events" || got.partition != codec.ReassignAllPartitions || len(got.replicas) != 0 {
		t.Fatalf("wire request topic=%q partition=%d replicas=%v", got.topic, got.partition, got.replicas)
	}
	if out != 3 {
		t.Fatalf("parsed %d", out)
	}
}

func TestReassignAllPartitionsEncodesAll(t *testing.T) {
	addr, got, stop := serveReassignPartitions(t, 0, 3)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.ReassignAllPartitions("events", []uint32{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.topic != "events" || got.partition != codec.ReassignAllPartitions || len(got.replicas) != 2 || got.replicas[0] != 1 || got.replicas[1] != 2 {
		t.Fatalf("wire request topic=%q partition=%d replicas=%v", got.topic, got.partition, got.replicas)
	}
	if out != 3 {
		t.Fatalf("parsed %d", out)
	}
}

func TestReassignPartitionsNonzeroErrorRaises(t *testing.T) {
	addr, _, stop := serveReassignPartitions(t, 2, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.ReassignPartitions("missing", []uint32{1, 2}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 2 || be.Op != "reassign_partitions" {
		t.Fatalf("broker error %+v", be)
	}
}
