package volant_test

import (
	"net"
	"sync"
	"testing"
	"time"

	volant "github.com/volant-mq/volant/clients/go"
	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

// v0.116: Go Client Metadata topic filter (MetadataTopics).

type metaTopicsStub struct {
	mu         sync.Mutex
	codes      []uint16
	seenTopics [][]string
	count      int
	ln         net.Listener
}

func startMetaTopicsStub(t *testing.T, codes []uint16) (addr string, stub *metaTopicsStub, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stub = &metaTopicsStub{codes: append([]uint16(nil), codes...), ln: ln}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go stub.serve(conn)
		}
	}()
	return ln.Addr().String(), stub, func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *metaTopicsStub) serve(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	buf := []byte{}
	tmp := make([]byte, 4096)
	for {
		f, rest, err := frame.TryDecode(buf)
		if err != nil {
			return
		}
		if f == nil {
			n, err := conn.Read(tmp)
			if n > 0 {
				buf = append(buf, tmp[:n]...)
			}
			if err != nil {
				return
			}
			continue
		}
		buf = append([]byte(nil), rest...)
		raw, err := s.handle(f)
		if err != nil {
			return
		}
		if _, err := conn.Write(raw); err != nil {
			return
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}
}

func (s *metaTopicsStub) handle(f *frame.Frame) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.Opcode != codec.OpMetadata {
		return nil, &frame.ProtocolError{Msg: "unexpected opcode"}
	}
	req, err := codec.DecodeMetadataRequest(f.Payload)
	if err != nil {
		return nil, err
	}
	s.count++
	topics := append([]string(nil), req.Topics...)
	s.seenTopics = append(s.seenTopics, topics)
	code := uint16(0)
	if len(s.codes) > 0 {
		code = s.codes[0]
		s.codes = s.codes[1:]
	}
	var payload []byte
	replyOp := uint16(codec.OpMetadata)
	if code != 0 {
		payload, err = codec.EncodeErrorResponse(codec.ErrorResponse{Code: code})
		replyOp = codec.OpError
	} else {
		payload, err = codec.EncodeMetadataResponse(codec.MetadataResponse{})
	}
	if err != nil {
		return nil, err
	}
	return frame.Encode(replyOp, f.CorrelationID, payload)
}

func (s *metaTopicsStub) rpcs() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (s *metaTopicsStub) seen() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]string, len(s.seenTopics))
	for i, t := range s.seenTopics {
		out[i] = append([]string(nil), t...)
	}
	return out
}

func TestMetadataSendsEmptyTopicsList(t *testing.T) {
	addr, stub, stop := startMetaTopicsStub(t, nil)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	got, err := c.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Brokers) != 0 || len(got.Topics) != 0 {
		t.Fatalf("metadata %v want empty", got)
	}
	if n := stub.rpcs(); n != 1 {
		t.Fatalf("metadata count %d want 1", n)
	}
	seen := stub.seen()
	if len(seen) != 1 || len(seen[0]) != 0 {
		t.Fatalf("seen topics %v want [[]]", seen)
	}
}

func TestMetadataTopicsEncodesNamedFilter(t *testing.T) {
	addr, stub, stop := startMetaTopicsStub(t, nil)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	got, err := c.MetadataTopics([]string{"events"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Brokers) != 0 {
		t.Fatalf("brokers %v want empty", got.Brokers)
	}
	if n := stub.rpcs(); n != 1 {
		t.Fatalf("metadata count %d want 1", n)
	}
	seen := stub.seen()
	if len(seen) != 1 || len(seen[0]) != 1 || seen[0][0] != "events" {
		t.Fatalf("seen topics %v want [[events]]", seen)
	}
}

func TestMetadataTopicsEmptyMatchesMetadata(t *testing.T) {
	addr, stub, stop := startMetaTopicsStub(t, nil)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MetadataTopics(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MetadataTopics([]string{}); err != nil {
		t.Fatal(err)
	}
	if n := stub.rpcs(); n != 3 {
		t.Fatalf("metadata count %d want 3", n)
	}
	seen := stub.seen()
	if len(seen) != 3 {
		t.Fatalf("seen %v", seen)
	}
	for i, topics := range seen {
		if len(topics) != 0 {
			t.Fatalf("seen[%d]=%v want empty", i, topics)
		}
	}
}

func TestMetadataStillRetriesTimeout(t *testing.T) {
	addr, stub, stop := startMetaTopicsStub(t, []uint16{timeoutCode, 0})
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	got, err := c.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Brokers) != 0 || len(got.Topics) != 0 {
		t.Fatalf("metadata %v want empty", got)
	}
	if n := stub.rpcs(); n != 2 {
		t.Fatalf("metadata count %d want 2", n)
	}
	seen := stub.seen()
	if len(seen) != 2 || len(seen[0]) != 0 || len(seen[1]) != 0 {
		t.Fatalf("seen topics %v want two empty lists", seen)
	}
}
