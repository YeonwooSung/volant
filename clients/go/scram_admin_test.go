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

type scramAdminResult struct {
	opcodes     []uint16
	createUser  string
	createPass  string
	createIters uint32
	deleteUser  string
	listPayload []byte
	err         error
}

func serveScramAdmin(t *testing.T, createErr, deleteErr, listErr uint16, names []string) (addr string, got *scramAdminResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &scramAdminResult{}
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
			case codec.OpCreateScramUser:
				req, err := codec.DecodeCreateScramUserRequest(f.Payload)
				if err != nil {
					res.err = err
					return
				}
				res.createUser = req.Username
				res.createPass = req.Password
				res.createIters = req.Iterations
				payload, err = codec.EncodeCreateScramUserResponse(codec.CreateScramUserResponse{ErrorCode: createErr})
				if err != nil {
					res.err = err
					return
				}
				op = codec.OpCreateScramUserResponse
			case codec.OpDeleteScramUser:
				req, err := codec.DecodeDeleteScramUserRequest(f.Payload)
				if err != nil {
					res.err = err
					return
				}
				res.deleteUser = req.Username
				payload, err = codec.EncodeDeleteScramUserResponse(codec.DeleteScramUserResponse{ErrorCode: deleteErr})
				if err != nil {
					res.err = err
					return
				}
				op = codec.OpDeleteScramUserResponse
			case codec.OpListScramUsers:
				res.listPayload = append([]byte(nil), f.Payload...)
				payload, err = codec.EncodeListScramUsersResponse(codec.ListScramUsersResponse{
					ErrorCode: listErr,
					Usernames: names,
				})
				if err != nil {
					res.err = err
					return
				}
				op = codec.OpListScramUsersResponse
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

func TestCreateScramUserOk(t *testing.T) {
	addr, got, stop := serveScramAdmin(t, 0, 0, 0, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.CreateScramUser("alice", "s3cret", 4096); err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.createUser != "alice" || got.createPass != "s3cret" || got.createIters != 4096 {
		t.Fatalf("create %#v", got)
	}
	if len(got.opcodes) != 1 || got.opcodes[0] != codec.OpCreateScramUser {
		t.Fatalf("opcodes %#v", got.opcodes)
	}
}

func TestCreateScramUserDefaultEncodesZeroIterations(t *testing.T) {
	addr, got, stop := serveScramAdmin(t, 0, 0, 0, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.CreateScramUserDefault("alice", "s3cret"); err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.createUser != "alice" || got.createPass != "s3cret" || got.createIters != 0 {
		t.Fatalf("create %#v", got)
	}
	if len(got.opcodes) != 1 || got.opcodes[0] != codec.OpCreateScramUser {
		t.Fatalf("opcodes %#v", got.opcodes)
	}
}

func TestDeleteScramUserNotFoundRaises(t *testing.T) {
	addr, got, stop := serveScramAdmin(t, 0, 2, 0, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.DeleteScramUser("missing")
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 2 || be.Op != "delete_scram_user" {
		t.Fatalf("broker error %+v", be)
	}
	if got.deleteUser != "missing" {
		t.Fatalf("delete %q", got.deleteUser)
	}
}

func TestListScramUsersReturnsNames(t *testing.T) {
	addr, got, stop := serveScramAdmin(t, 0, 0, 0, []string{"alice", "bob"})
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	names, err := c.ListScramUsers()
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if len(names) != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("names %#v", names)
	}
	if len(got.listPayload) != 0 {
		t.Fatalf("list request %x", got.listPayload)
	}
}

func TestListScramUsersUnauthorizedRaises(t *testing.T) {
	addr, _, stop := serveScramAdmin(t, 0, 0, 23, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.ListScramUsers()
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 23 || be.Op != "list_scram_users" {
		t.Fatalf("broker error %+v", be)
	}
}
