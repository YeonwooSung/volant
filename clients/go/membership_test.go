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

type membershipResult struct {
	opcodes     []uint16
	addID       uint32
	addHost     string
	addPort     uint16
	addRack     *string
	removeID    uint32
	listPayload []byte
	err         error
}

func serveMembership(t *testing.T, addErr, removeErr, listErr uint16) (addr string, got *membershipResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &membershipResult{}
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
			case codec.OpAddBroker:
				req, err := codec.DecodeAddBrokerRequest(f.Payload)
				if err != nil {
					res.err = err
					return
				}
				res.addID = req.ID
				res.addHost = req.Host
				res.addPort = req.Port
				res.addRack = req.Rack
				gen := uint64(5)
				if addErr != 0 {
					gen = 0
				}
				payload, err = codec.EncodeAddBrokerResponse(codec.AddBrokerResponse{ErrorCode: addErr, Generation: gen})
				if err != nil {
					res.err = err
					return
				}
				op = codec.OpAddBrokerResponse
			case codec.OpRemoveBroker:
				req, err := codec.DecodeRemoveBrokerRequest(f.Payload)
				if err != nil {
					res.err = err
					return
				}
				res.removeID = req.ID
				gen := uint64(6)
				if removeErr != 0 {
					gen = 0
				}
				payload, err = codec.EncodeRemoveBrokerResponse(codec.RemoveBrokerResponse{ErrorCode: removeErr, Generation: gen})
				if err != nil {
					res.err = err
					return
				}
				op = codec.OpRemoveBrokerResponse
			case codec.OpListMembers:
				res.listPayload = append([]byte(nil), f.Payload...)
				rack := "r1"
				gen := uint64(4)
				if listErr != 0 {
					gen = 0
				}
				payload, err = codec.EncodeListMembersResponse(codec.ListMembersResponse{
					ErrorCode:  listErr,
					Generation: gen,
					Brokers: []codec.MembershipBroker{
						{ID: 1, Host: "10.0.0.1", Port: 9092, Rack: nil},
						{ID: 2, Host: "10.0.0.2", Port: 9092, Rack: &rack},
					},
					Live: []uint32{1, 2},
				})
				if err != nil {
					res.err = err
					return
				}
				op = codec.OpListMembersResponse
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

func TestAddBrokerReturnsGeneration(t *testing.T) {
	addr, got, stop := serveMembership(t, 0, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	rack := "r1"
	gen, err := c.AddBroker(2, "10.0.0.2", 9092, &rack)
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if gen != 5 {
		t.Fatalf("generation %d", gen)
	}
	if got.addID != 2 || got.addHost != "10.0.0.2" || got.addPort != 9092 || got.addRack == nil || *got.addRack != "r1" {
		t.Fatalf("add %#v", got)
	}
	if len(got.opcodes) != 1 || got.opcodes[0] != codec.OpAddBroker {
		t.Fatalf("opcodes %#v", got.opcodes)
	}
}

func TestRemoveBrokerReturnsGeneration(t *testing.T) {
	addr, got, stop := serveMembership(t, 0, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	gen, err := c.RemoveBroker(2)
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if gen != 6 || got.removeID != 2 {
		t.Fatalf("remove gen=%d id=%d", gen, got.removeID)
	}
}

func TestListMembersParsesBrokersAndLive(t *testing.T) {
	addr, got, stop := serveMembership(t, 0, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	members, err := c.ListMembers()
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if members.Generation != 4 || len(members.Brokers) != 2 || len(members.Live) != 2 {
		t.Fatalf("members %+v", members)
	}
	if members.Brokers[0].ID != 1 || members.Brokers[0].Host != "10.0.0.1" || members.Brokers[0].Rack != nil {
		t.Fatalf("broker0 %+v", members.Brokers[0])
	}
	if members.Brokers[1].ID != 2 || members.Brokers[1].Rack == nil || *members.Brokers[1].Rack != "r1" {
		t.Fatalf("broker1 %+v", members.Brokers[1])
	}
	if members.Live[0] != 1 || members.Live[1] != 2 {
		t.Fatalf("live %+v", members.Live)
	}
	if len(got.listPayload) != 0 {
		t.Fatalf("list request %x", got.listPayload)
	}
}

func TestAddBrokerErrorRaises(t *testing.T) {
	addr, _, stop := serveMembership(t, 3, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.AddBroker(2, "10.0.0.2", 9092, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 3 || be.Op != "add_broker" {
		t.Fatalf("broker error %+v", be)
	}
}

func TestRemoveBrokerErrorRaises(t *testing.T) {
	addr, _, stop := serveMembership(t, 0, 2, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.RemoveBroker(2)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 2 || be.Op != "remove_broker" {
		t.Fatalf("broker error %+v", be)
	}
}

func TestListMembersErrorRaises(t *testing.T) {
	addr, _, stop := serveMembership(t, 0, 0, 23)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.ListMembers()
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 23 || be.Op != "list_members" {
		t.Fatalf("broker error %+v", be)
	}
}
