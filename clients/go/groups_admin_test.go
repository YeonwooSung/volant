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

type groupAdminResult struct {
	opcodes   []uint16
	described string
	err       error
}

func serveGroupAdmin(t *testing.T, describeError uint16) (addr string, got *groupAdminResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &groupAdminResult{}
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
			switch f.Opcode {
			case codec.OpListGroups:
				payload, err := codec.EncodeListGroupsResponse(codec.ListGroupsResponse{
					ErrorCode: 0,
					Groups: []codec.GroupListing{
						{GroupID: "g2", State: codec.GroupStateEmpty, MemberCount: 0, Generation: 0},
						{GroupID: "g1", State: codec.GroupStateStable, MemberCount: 2, Generation: 5},
					},
				})
				if err != nil {
					res.err = err
					return
				}
				raw, err := frame.Encode(codec.OpListGroupsResponse, f.CorrelationID, payload)
				if err != nil {
					res.err = err
					return
				}
				if _, err := conn.Write(raw); err != nil {
					res.err = err
					return
				}
			case codec.OpDescribeGroup:
				req, err := codec.DecodeDescribeGroupRequest(f.Payload)
				if err != nil {
					res.err = err
					return
				}
				res.described = req.GroupID
				var resp codec.DescribeGroupResponse
				if describeError != 0 {
					resp = codec.DescribeGroupResponse{
						ErrorCode: describeError,
						GroupID:   req.GroupID,
					}
				} else {
					resp = codec.DescribeGroupResponse{
						ErrorCode:  0,
						GroupID:    req.GroupID,
						Generation: 3,
						Members: []codec.GroupMemberInfo{
							{
								MemberID: "m-a",
								Topics:   []string{"events"},
								Assignment: []codec.Assignment{
									{Topic: "events", Partition: 0},
									{Topic: "events", Partition: 2},
								},
							},
						},
					}
				}
				payload, err := codec.EncodeDescribeGroupResponse(resp)
				if err != nil {
					res.err = err
					return
				}
				raw, err := frame.Encode(codec.OpDescribeGroupResponse, f.CorrelationID, payload)
				if err != nil {
					res.err = err
					return
				}
				if _, err := conn.Write(raw); err != nil {
					res.err = err
					return
				}
				if describeError != 0 {
					return
				}
			default:
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

func TestListGroupsEmptyAndStable(t *testing.T) {
	addr, got, stop := serveGroupAdmin(t, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	groups, err := c.ListGroups()
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("groups %#v", groups)
	}
	byID := map[string]volant.GroupListing{}
	for _, g := range groups {
		byID[g.GroupID] = g
	}
	if byID["g2"].State != volant.GroupStateEmpty || byID["g2"].MemberCount != 0 || byID["g2"].Generation != 0 {
		t.Fatalf("empty %#v", byID["g2"])
	}
	if byID["g1"].State != volant.GroupStateStable || byID["g1"].MemberCount != 2 || byID["g1"].Generation != 5 {
		t.Fatalf("stable %#v", byID["g1"])
	}
	if len(got.opcodes) != 1 || got.opcodes[0] != codec.OpListGroups {
		t.Fatalf("opcodes %#v", got.opcodes)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
}

func TestDescribeGroupMembersAndAssignment(t *testing.T) {
	addr, got, stop := serveGroupAdmin(t, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	desc, err := c.DescribeGroup("cg-1")
	if err != nil {
		t.Fatal(err)
	}
	if desc.GroupID != "cg-1" || desc.Generation != 3 || len(desc.Members) != 1 {
		t.Fatalf("desc %#v", desc)
	}
	m := desc.Members[0]
	if m.MemberID != "m-a" || len(m.Topics) != 1 || m.Topics[0] != "events" {
		t.Fatalf("member %#v", m)
	}
	if len(m.Assignment) != 2 || m.Assignment[0].Topic != "events" || m.Assignment[1].Partition != 2 {
		t.Fatalf("assignment %#v", m.Assignment)
	}
	if got.described != "cg-1" {
		t.Fatalf("described %q", got.described)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
}

func TestDescribeGroupNotFoundRaises(t *testing.T) {
	addr, got, stop := serveGroupAdmin(t, 2)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.DescribeGroup("missing")
	if err == nil {
		t.Fatal("expected not found")
	}
	var be *codec.BrokerError
	if !errors.As(err, &be) || be.Code != 2 || be.Op != "describe_group" {
		t.Fatalf("err=%v", err)
	}
	if got.described != "missing" {
		t.Fatalf("described %q", got.described)
	}
}
