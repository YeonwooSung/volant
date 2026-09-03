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

func sampleAcl() codec.AclBinding {
	return codec.AclBinding{
		Principal:    "User:alice",
		ResourceType: 0,
		Resource:     "events",
		Operation:    3,
		Permission:   1,
	}
}

type aclAdminResult struct {
	opcodes    []uint16
	create     []codec.AclBinding
	delete     []codec.AclBinding
	listReq    codec.ListAclsRequest
	err        error
}

func serveAcls(t *testing.T, createErr, deleteErr, listErr uint16, removed uint32, entries []codec.AclBinding) (addr string, got *aclAdminResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &aclAdminResult{}
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
			var (
				payload []byte
				op      uint16
			)
			switch f.Opcode {
			case codec.OpCreateAcls:
				req, err := codec.DecodeCreateAclsRequest(f.Payload)
				if err != nil {
					res.err = err
					return
				}
				res.create = req.Entries
				payload, err = codec.EncodeCreateAclsResponse(codec.CreateAclsResponse{ErrorCode: createErr})
				if err != nil {
					res.err = err
					return
				}
				op = codec.OpCreateAclsResponse
			case codec.OpDeleteAcls:
				req, err := codec.DecodeDeleteAclsRequest(f.Payload)
				if err != nil {
					res.err = err
					return
				}
				res.delete = req.Entries
				payload, err = codec.EncodeDeleteAclsResponse(codec.DeleteAclsResponse{ErrorCode: deleteErr, Removed: removed})
				if err != nil {
					res.err = err
					return
				}
				op = codec.OpDeleteAclsResponse
			case codec.OpListAcls:
				req, err := codec.DecodeListAclsRequest(f.Payload)
				if err != nil {
					res.err = err
					return
				}
				res.listReq = req
				payload, err = codec.EncodeListAclsResponse(codec.ListAclsResponse{
					ErrorCode: listErr,
					Entries:   entries,
				})
				if err != nil {
					res.err = err
					return
				}
				op = codec.OpListAclsResponse
			default:
				res.err = &frame.ProtocolError{Msg: "unexpected opcode"}
				return
			}
			raw, err := frame.Encode(op, f.CorrelationID, payload)
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

func TestCreateAclsOk(t *testing.T) {
	entry := sampleAcl()
	addr, got, stop := serveAcls(t, 0, 0, 0, 1, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.CreateAcls([]codec.AclBinding{entry}); err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if len(got.create) != 1 || got.create[0] != entry {
		t.Fatalf("create %#v", got.create)
	}
	if len(got.opcodes) != 1 || got.opcodes[0] != codec.OpCreateAcls {
		t.Fatalf("opcodes %#v", got.opcodes)
	}
}

func TestCreateAclEncodesOneEntry(t *testing.T) {
	entry := sampleAcl()
	addr, got, stop := serveAcls(t, 0, 0, 0, 1, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.CreateAcl(entry); err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if len(got.create) != 1 || got.create[0] != entry {
		t.Fatalf("create %#v", got.create)
	}
	if len(got.opcodes) != 1 || got.opcodes[0] != codec.OpCreateAcls {
		t.Fatalf("opcodes %#v", got.opcodes)
	}
}

func TestDeleteAclsReturnsRemoved(t *testing.T) {
	entry := sampleAcl()
	addr, got, stop := serveAcls(t, 0, 0, 0, 1, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	n, err := c.DeleteAcls([]codec.AclBinding{entry})
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if n != 1 {
		t.Fatalf("removed %d", n)
	}
	if len(got.delete) != 1 || got.delete[0] != entry {
		t.Fatalf("delete %#v", got.delete)
	}
}

func TestDeleteAclEncodesOneEntry(t *testing.T) {
	entry := sampleAcl()
	addr, got, stop := serveAcls(t, 0, 0, 0, 1, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	n, err := c.DeleteAcl(entry)
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if n != 1 {
		t.Fatalf("removed %d", n)
	}
	if len(got.delete) != 1 || got.delete[0] != entry {
		t.Fatalf("delete %#v", got.delete)
	}
}

func TestListAclsReturnsBindings(t *testing.T) {
	entry := sampleAcl()
	addr, got, stop := serveAcls(t, 0, 0, 0, 0, []codec.AclBinding{entry})
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	listed, err := c.ListAcls("", 255, "")
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if len(listed) != 1 || listed[0] != entry {
		t.Fatalf("listed %#v", listed)
	}
	if got.listReq.Principal != "" || got.listReq.ResourceType != 255 || got.listReq.Resource != "" {
		t.Fatalf("list req %+v", got.listReq)
	}
}

func TestListAclsAllEncodesEmptyFilters(t *testing.T) {
	entry := sampleAcl()
	addr, got, stop := serveAcls(t, 0, 0, 0, 0, []codec.AclBinding{entry})
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	listed, err := c.ListAclsAll()
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if len(listed) != 1 || listed[0] != entry {
		t.Fatalf("listed %#v", listed)
	}
	if got.listReq.Principal != "" || got.listReq.ResourceType != 255 || got.listReq.Resource != "" {
		t.Fatalf("list req %+v", got.listReq)
	}
}

func TestCreateAclsUnauthorizedRaises(t *testing.T) {
	addr, _, stop := serveAcls(t, 23, 0, 0, 0, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.CreateAcls([]codec.AclBinding{sampleAcl()})
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 23 || be.Op != "create_acls" {
		t.Fatalf("broker error %+v", be)
	}
}
