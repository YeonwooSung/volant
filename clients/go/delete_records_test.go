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

type deleteRecordsServerResult struct {
	topic        string
	partition    uint32
	beforeOffset uint64
	waitMajority uint8
	opcodes      []uint16
	err          error
}

func serveDeleteRecords(t *testing.T, errorCode uint16, lowWatermark uint64) (addr string, got *deleteRecordsServerResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &deleteRecordsServerResult{}
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
			res.opcodes = append(res.opcodes, f.Opcode)
			if f.Opcode != codec.OpDeleteRecords {
				res.err = &frame.ProtocolError{Msg: "unexpected opcode"}
				return
			}
			req, err := codec.DecodeDeleteRecordsRequest(f.Payload)
			if err != nil {
				res.err = err
				return
			}
			res.topic = req.Topic
			res.partition = req.Partition
			res.beforeOffset = req.BeforeOffset
			res.waitMajority = req.WaitMajority
			payload, err := codec.EncodeDeleteRecordsResponse(codec.DeleteRecordsResponse{
				ErrorCode:    errorCode,
				Topic:        req.Topic,
				Partition:    req.Partition,
				LowWatermark: lowWatermark,
			})
			if err != nil {
				res.err = err
				return
			}
			raw, err := frame.Encode(codec.OpDeleteRecordsResponse, f.CorrelationID, payload)
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

func TestDeleteRecordsSuccessReturnsLowWatermark(t *testing.T) {
	addr, got, stop := serveDeleteRecords(t, 0, 96)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.DeleteRecords("events", 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if out != (volant.DeleteRecordsResult{Topic: "events", Partition: 2, LowWatermark: 96}) {
		t.Fatalf("parsed %+v", out)
	}
	if got.topic != "events" || got.partition != 2 || got.beforeOffset != 100 || got.waitMajority != 0 {
		t.Fatalf("wire request %+v", got)
	}
	flagged, err := c.DeleteRecordsWithWaitFlag("events", 2, 100, 1)
	if err != nil {
		t.Fatal(err)
	}
	if flagged.LowWatermark != 96 {
		t.Fatalf("flagged %+v", flagged)
	}
	if got.waitMajority != 1 {
		t.Fatalf("wait flag %d", got.waitMajority)
	}
}

func TestDeleteRecordsError13RaisesWithoutRedirect(t *testing.T) {
	addr, got, stop := serveDeleteRecords(t, 13, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.DeleteRecords("events", 0, 10)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 13 || be.Op != "delete_records" {
		t.Fatalf("broker error %+v", be)
	}
	if len(got.opcodes) != 1 || got.opcodes[0] != codec.OpDeleteRecords {
		t.Fatalf("opcodes %v (redirect would send metadata)", got.opcodes)
	}
}
