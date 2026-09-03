package volant_test

import (
	"net"
	"testing"
	"time"

	volant "github.com/volant-mq/volant/clients/go"
	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

// v0.127: thin Client JoinGroup instance id (records decoded group_instance_id).

type joinGroupInstanceResult struct {
	opcodes    []uint16
	instanceID string
	group      string
	err        error
}

func serveJoinGroupInstance(t *testing.T) (addr string, got *joinGroupInstanceResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &joinGroupInstanceResult{}
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
			if f.Opcode != codec.OpJoinGroup {
				res.err = &frame.ProtocolError{Msg: "unexpected opcode"}
				return
			}
			req, err := codec.DecodeJoinGroupRequest(f.Payload)
			if err != nil {
				res.err = err
				return
			}
			res.group = req.GroupID
			res.instanceID = req.GroupInstanceID
			payload, err := codec.EncodeJoinGroupResponse(codec.JoinGroupResponse{
				ErrorCode:  0,
				Generation: 1,
				MemberID:   "m-1",
			})
			if err != nil {
				res.err = err
				return
			}
			raw, err := frame.Encode(codec.OpJoinGroup, f.CorrelationID, payload)
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

func TestJoinGroupSendsEmptyInstanceID(t *testing.T) {
	addr, got, stop := serveJoinGroupInstance(t)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	j, err := c.JoinGroup("g", []string{"t"}, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	if j.MemberID != "m-1" || j.Generation != 1 {
		t.Fatalf("result=%+v", j)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.instanceID != "" {
		t.Fatalf("instance %q want empty", got.instanceID)
	}
	if got.group != "g" {
		t.Fatalf("group %q", got.group)
	}
	if len(got.opcodes) != 1 || got.opcodes[0] != codec.OpJoinGroup {
		t.Fatalf("opcodes %+v", got.opcodes)
	}
}

func TestJoinGroupWithInstanceEncodesID(t *testing.T) {
	addr, got, stop := serveJoinGroupInstance(t)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	j, err := c.JoinGroupWithInstance("g", []string{"t"}, 10_000, "pod-1")
	if err != nil {
		t.Fatal(err)
	}
	if j.MemberID != "m-1" {
		t.Fatalf("member %q", j.MemberID)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.instanceID != "pod-1" {
		t.Fatalf("instance %q want pod-1", got.instanceID)
	}
}

func TestJoinGroupWithInstanceEmptyMatchesPublicAPI(t *testing.T) {
	addr, got, stop := serveJoinGroupInstance(t)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.JoinGroupWithInstance("g", []string{"t"}, 10_000, ""); err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.instanceID != "" {
		t.Fatalf("instance %q want empty", got.instanceID)
	}
}
