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

type configsServerResult struct {
	opcodes []uint16
	topic   string
	alter   [][2]string
	err     error
}

func serveConfigs(t *testing.T, errorCode uint16, configs [][2]string) (addr string, got *configsServerResult, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &configsServerResult{}
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
			case codec.OpDescribeConfigs:
				req, err := codec.DecodeDescribeConfigsRequest(f.Payload)
				if err != nil {
					res.err = err
					return
				}
				res.topic = req.Topic
				payload, err := codec.EncodeDescribeConfigsResponse(codec.DescribeConfigsResponse{
					ErrorCode:      errorCode,
					Topic:          req.Topic,
					TopicID:        1,
					PartitionCount: 1,
					Configs:        configs,
				})
				if err != nil {
					res.err = err
					return
				}
				raw, err := frame.Encode(codec.OpDescribeConfigsResponse, f.CorrelationID, payload)
				if err != nil {
					res.err = err
					return
				}
				if _, err := conn.Write(raw); err != nil {
					res.err = err
					return
				}
			case codec.OpAlterConfigs:
				req, err := codec.DecodeAlterConfigsRequest(f.Payload)
				if err != nil {
					res.err = err
					return
				}
				res.topic = req.Topic
				res.alter = req.Configs
				payload, err := codec.EncodeAlterConfigsResponse(codec.AlterConfigsResponse{
					ErrorCode: errorCode,
					Topic:     req.Topic,
				})
				if err != nil {
					res.err = err
					return
				}
				raw, err := frame.Encode(codec.OpAlterConfigsResponse, f.CorrelationID, payload)
				if err != nil {
					res.err = err
					return
				}
				if _, err := conn.Write(raw); err != nil {
					res.err = err
					return
				}
			default:
				res.err = &frame.ProtocolError{Msg: "unexpected opcode"}
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

func TestDescribeConfigsReturnsPairs(t *testing.T) {
	pairs := [][2]string{{"retention.ms", "86400000"}}
	addr, got, stop := serveConfigs(t, 0, pairs)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.DescribeConfigs("events")
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.topic != "events" || len(got.opcodes) != 1 || got.opcodes[0] != codec.OpDescribeConfigs {
		t.Fatalf("wire request topic=%q opcodes=%v", got.topic, got.opcodes)
	}
	if out.Topic != "events" || out.TopicID != 1 || out.PartitionCount != 1 {
		t.Fatalf("parsed %+v", out)
	}
	if len(out.Configs) != 1 || out.Configs[0] != [2]string{"retention.ms", "86400000"} {
		t.Fatalf("configs %+v", out.Configs)
	}
}

func TestAlterConfigsOkEmptyValueClear(t *testing.T) {
	addr, got, stop := serveConfigs(t, 0, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.AlterConfigs("events", [][2]string{{"retention.ms", ""}}); err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.topic != "events" || len(got.alter) != 1 || got.alter[0] != [2]string{"retention.ms", ""} {
		t.Fatalf("wire alter topic=%q configs=%v", got.topic, got.alter)
	}
}

func TestAlterConfigEncodesOnePair(t *testing.T) {
	addr, got, stop := serveConfigs(t, 0, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.AlterConfig("events", "retention.ms", "1"); err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.topic != "events" || len(got.alter) != 1 || got.alter[0] != [2]string{"retention.ms", "1"} {
		t.Fatalf("wire alter topic=%q configs=%v", got.topic, got.alter)
	}
}

func TestDescribeConfigsNonzeroErrorRaises(t *testing.T) {
	addr, _, stop := serveConfigs(t, 2, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.DescribeConfigs("missing")
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 2 || be.Op != "describe_configs" {
		t.Fatalf("broker error %+v", be)
	}
}

func TestAlterConfigsNonzeroErrorRaises(t *testing.T) {
	addr, _, stop := serveConfigs(t, 2, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.AlterConfigs("missing", [][2]string{{"retention.ms", "1"}})
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 2 || be.Op != "alter_configs" {
		t.Fatalf("broker error %+v", be)
	}
}
