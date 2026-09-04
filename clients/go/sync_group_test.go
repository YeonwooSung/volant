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

type syncGroupServerResult struct {
	groupID         string
	memberID        string
	generation      uint32
	assignmentBytes []byte
	err             error
}

func serveSyncGroup(t *testing.T, errorCode uint16, assignment []codec.Assignment) (addr string, got *syncGroupServerResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &syncGroupServerResult{}
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
			if f.Opcode != codec.OpSyncGroup {
				res.err = &frame.ProtocolError{Msg: "unexpected opcode"}
				return
			}
			req, err := codec.DecodeSyncGroupRequest(f.Payload)
			if err != nil {
				res.err = err
				return
			}
			res.groupID = req.GroupID
			res.memberID = req.MemberID
			res.generation = req.Generation
			res.assignmentBytes = req.AssignmentBytes
			asgn := assignment
			if errorCode != 0 {
				asgn = nil
			}
			payload, err := codec.EncodeSyncGroupResponse(codec.SyncGroupResponse{
				ErrorCode:  errorCode,
				Assignment: asgn,
			})
			if err != nil {
				res.err = err
				return
			}
			raw, err := frame.Encode(codec.OpSyncGroupResponse, f.CorrelationID, payload)
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

func TestSyncGroupCodecRoundtrip(t *testing.T) {
	req := codec.SyncGroupRequest{
		GroupID: "g1", MemberID: "m1", Generation: 3, AssignmentBytes: []byte{},
	}
	raw, err := codec.EncodeSyncGroupRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.DecodeSyncGroupRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GroupID != "g1" || decoded.MemberID != "m1" || decoded.Generation != 3 || len(decoded.AssignmentBytes) != 0 {
		t.Fatalf("decoded %+v", decoded)
	}
	resp := codec.SyncGroupResponse{
		ErrorCode:  0,
		Assignment: []codec.Assignment{{Topic: "events", Partition: 2}},
	}
	rraw, err := codec.EncodeSyncGroupResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := codec.DecodeSyncGroupResponse(rraw)
	if err != nil {
		t.Fatal(err)
	}
	if got.ErrorCode != 0 || len(got.Assignment) != 1 || got.Assignment[0].Topic != "events" || got.Assignment[0].Partition != 2 {
		t.Fatalf("decoded resp %+v", got)
	}
	dispatched, err := codec.DecodeResponse(codec.OpSyncGroupResponse, rraw)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := dispatched.(codec.SyncGroupResponse); !ok {
		t.Fatalf("dispatch %T", dispatched)
	}
}

func TestSyncGroupSuccessReturnsAssignment(t *testing.T) {
	asgn := []codec.Assignment{{Topic: "events", Partition: 2}}
	addr, got, stop := serveSyncGroup(t, 0, asgn)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.SyncGroup("g1", "m1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.groupID != "g1" || got.memberID != "m1" || got.generation != 3 || len(got.assignmentBytes) != 0 {
		t.Fatalf("wire %+v", got)
	}
	if len(out) != 1 || out[0].Topic != "events" || out[0].Partition != 2 {
		t.Fatalf("assignment %+v", out)
	}
}

func TestSyncGroupUnknownMemberIs10(t *testing.T) {
	addr, _, stop := serveSyncGroup(t, 10, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.SyncGroup("g", "ghost", 1)
	var be *codec.BrokerError
	if !errors.As(err, &be) || be.Code != 10 || be.Op != "sync_group" {
		t.Fatalf("err=%v", err)
	}
}

func TestSyncGroupGenerationMismatchIs9(t *testing.T) {
	addr, _, stop := serveSyncGroup(t, 9, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.SyncGroup("g", "m1", 99)
	var be *codec.BrokerError
	if !errors.As(err, &be) || be.Code != 9 || be.Op != "sync_group" {
		t.Fatalf("err=%v", err)
	}
}
