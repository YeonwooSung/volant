package volant_test

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	volant "github.com/volant-mq/volant/clients/go"
	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

type listOffsetsServer struct {
	mu             sync.Mutex
	errorCodes     []uint16
	entries        []codec.OffsetListing
	errorAsOpcode  bool
	meta           codec.MetadataResponse
	topic          string
	partitions     []uint32
	opcodes        []uint16
	listCount      int
	metadataCount  int
	acceptCount    int
	result         *listOffsetsServerResult
	err            error
	ln             net.Listener
}

type listOffsetsServerResult struct {
	topic      string
	partitions []uint32
	err        error
}

func startListOffsets(t *testing.T, errorCode uint16, entries []codec.OffsetListing) (*listOffsetsServer, string, func()) {
	t.Helper()
	return startListOffsetsCodes(t, []uint16{errorCode}, entries, false)
}

func startListOffsetsCodes(t *testing.T, errorCodes []uint16, entries []codec.OffsetListing, errorAsOpcode bool) (*listOffsetsServer, string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	codes := make([]uint16, len(errorCodes))
	copy(codes, errorCodes)
	s := &listOffsetsServer{
		errorCodes:    codes,
		entries:       entries,
		errorAsOpcode: errorAsOpcode,
		ln:            ln,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.acceptCount++
			s.mu.Unlock()
			go s.serve(conn)
		}
	}()
	return s, ln.Addr().String(), func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

func serveListOffsets(t *testing.T, errorCode uint16, entries []codec.OffsetListing) (addr string, got *listOffsetsServerResult, stop func()) {
	t.Helper()
	s, addr, innerStop := startListOffsets(t, errorCode, entries)
	got = &listOffsetsServerResult{}
	s.mu.Lock()
	s.result = got
	s.mu.Unlock()
	return addr, got, innerStop
}

func (s *listOffsetsServer) port() int {
	return s.ln.Addr().(*net.TCPAddr).Port
}

func (s *listOffsetsServer) snapshot() (lists, metas int, partitions []uint32, topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parts := make([]uint32, len(s.partitions))
	copy(parts, s.partitions)
	return s.listCount, s.metadataCount, parts, s.topic
}

func (s *listOffsetsServer) serve(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	buf := []byte{}
	tmp := make([]byte, 4096)
	for {
		f, rest, err := frame.TryDecode(buf)
		if err != nil {
			s.mu.Lock()
			s.err = err
			s.mu.Unlock()
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
			s.mu.Lock()
			s.err = err
			s.mu.Unlock()
			return
		}
		if _, err := conn.Write(raw); err != nil {
			return
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}
}

func (s *listOffsetsServer) handle(f *frame.Frame) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opcodes = append(s.opcodes, f.Opcode)
	var payload []byte
	var err error
	replyOp := f.Opcode
	switch f.Opcode {
	case codec.OpListOffsets:
		s.listCount++
		req, e := codec.DecodeListOffsetsRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.topic = req.Topic
		s.partitions = req.Partitions
		if s.result != nil {
			s.result.topic = req.Topic
			s.result.partitions = append([]uint32(nil), req.Partitions...)
		}
		code := uint16(0)
		if len(s.errorCodes) > 0 {
			code = s.errorCodes[0]
			s.errorCodes = s.errorCodes[1:]
		}
		if s.errorAsOpcode && code != 0 {
			payload, err = codec.EncodeErrorResponse(codec.ErrorResponse{Code: code})
			replyOp = codec.OpError
			break
		}
		entries := s.entries
		if code != 0 {
			entries = nil
		}
		payload, err = codec.EncodeListOffsetsResponse(codec.ListOffsetsResponse{
			ErrorCode: code,
			Topic:     req.Topic,
			Entries:   entries,
		})
		replyOp = codec.OpListOffsetsResponse
	case codec.OpMetadata:
		s.metadataCount++
		payload, err = codec.EncodeMetadataResponse(s.meta)
	default:
		return nil, &frame.ProtocolError{Msg: "unexpected opcode"}
	}
	if err != nil {
		return nil, err
	}
	return frame.Encode(replyOp, f.CorrelationID, payload)
}

func TestListOffsetsAllEncodesEmptyPartitions(t *testing.T) {
	entries := []codec.OffsetListing{
		{Partition: 0, Earliest: 0, Latest: 10},
		{Partition: 1, Earliest: 2, Latest: 5},
	}
	addr, got, stop := serveListOffsets(t, 0, entries)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.ListOffsetsAll("events")
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.topic != "events" || len(got.partitions) != 0 {
		t.Fatalf("wire request topic=%q partitions=%v", got.topic, got.partitions)
	}
	if len(out) != 2 || out[0] != (volant.OffsetListing{Partition: 0, Earliest: 0, Latest: 10}) {
		t.Fatalf("parsed %+v", out)
	}
	if out[1] != (volant.OffsetListing{Partition: 1, Earliest: 2, Latest: 5}) {
		t.Fatalf("parsed entry1 %+v", out[1])
	}
}

func TestListOffsetsEmptyPartitionsEncodedAsCountZero(t *testing.T) {
	entries := []codec.OffsetListing{
		{Partition: 0, Earliest: 0, Latest: 10},
		{Partition: 1, Earliest: 2, Latest: 5},
	}
	addr, got, stop := serveListOffsets(t, 0, entries)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.ListOffsets("events", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.topic != "events" || len(got.partitions) != 0 {
		t.Fatalf("wire request topic=%q partitions=%v", got.topic, got.partitions)
	}
	if len(out) != 2 || out[0] != (volant.OffsetListing{Partition: 0, Earliest: 0, Latest: 10}) {
		t.Fatalf("parsed %+v", out)
	}
	if out[1] != (volant.OffsetListing{Partition: 1, Earliest: 2, Latest: 5}) {
		t.Fatalf("parsed entry1 %+v", out[1])
	}
}

func TestListOffsetsExplicitPartitions(t *testing.T) {
	entries := []codec.OffsetListing{{Partition: 0, Earliest: 0, Latest: 10}}
	addr, got, stop := serveListOffsets(t, 0, entries)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.ListOffsets("events", []uint32{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if len(got.partitions) != 2 || got.partitions[0] != 0 || got.partitions[1] != 1 {
		t.Fatalf("partitions %v", got.partitions)
	}
	if len(out) != 1 || out[0].Latest != 10 {
		t.Fatalf("parsed %+v", out)
	}
}

func TestListOffsetsNonzeroErrorRaises(t *testing.T) {
	addr, _, stop := serveListOffsets(t, 2, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.ListOffsets("missing", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 2 || be.Op != "list_offsets" {
		t.Fatalf("broker error %+v", be)
	}
}

func TestListOffsetsError13RedirectsToLeader(t *testing.T) {
	entries := []codec.OffsetListing{{Partition: 0, Earliest: 0, Latest: 10}}
	leader, _, stopL := startListOffsets(t, 0, entries)
	defer stopL()
	follower, faddr, stopF := startListOffsetsCodes(t, []uint16{13}, nil, true)
	defer stopF()
	follower.mu.Lock()
	follower.meta = leaderMeta("events", 0, 2, "127.0.0.1", leader.port())
	follower.mu.Unlock()

	c, err := volant.DialTimeout(faddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.ListOffsets("events", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0] != (volant.OffsetListing{Partition: 0, Earliest: 0, Latest: 10}) {
		t.Fatalf("parsed %+v", out)
	}
	fl, fm, _, _ := follower.snapshot()
	ll, _, parts, topic := leader.snapshot()
	if fl != 1 || fm != 1 {
		t.Fatalf("follower list/metadata = %d/%d want 1/1", fl, fm)
	}
	if ll != 1 || topic != "events" || len(parts) != 0 {
		t.Fatalf("leader list=%d topic=%s parts=%v", ll, topic, parts)
	}
}

func TestListOffsetsTypedError13RedirectsToLeader(t *testing.T) {
	entries := []codec.OffsetListing{{Partition: 0, Earliest: 0, Latest: 10}}
	leader, _, stopL := startListOffsets(t, 0, entries)
	defer stopL()
	follower, faddr, stopF := startListOffsets(t, 13, nil)
	defer stopF()
	follower.mu.Lock()
	follower.meta = leaderMeta("events", 0, 2, "127.0.0.1", leader.port())
	follower.mu.Unlock()

	c, err := volant.DialTimeout(faddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.ListOffsets("events", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Latest != 10 {
		t.Fatalf("parsed %+v", out)
	}
	fl, fm, _, _ := follower.snapshot()
	ll, _, _, _ := leader.snapshot()
	if fl != 1 || fm != 1 {
		t.Fatalf("follower list/metadata = %d/%d want 1/1", fl, fm)
	}
	if ll != 1 {
		t.Fatalf("leader list=%d want 1", ll)
	}
}

func TestListOffsetsError13MaxRedirectsZeroRaises(t *testing.T) {
	srv, addr, stop := startListOffsets(t, 13, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRedirects(0)
	_, err = c.ListOffsets("events", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 13 || be.Op != "list_offsets" {
		t.Fatalf("broker error %+v", be)
	}
	lists, metas, _, _ := srv.snapshot()
	if lists != 1 || metas != 0 {
		t.Fatalf("list/metadata = %d/%d want 1/0", lists, metas)
	}
}

func TestListOffsetsRetriesTimeoutThenOkNoMetadata(t *testing.T) {
	entries := []codec.OffsetListing{{Partition: 0, Earliest: 0, Latest: 10}}
	srv, addr, stop := startListOffsetsCodes(t, []uint16{7, 0}, entries, false)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	out, err := c.ListOffsets("events", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Latest != 10 {
		t.Fatalf("parsed %+v", out)
	}
	lists, metas, _, _ := srv.snapshot()
	if lists != 2 || metas != 0 {
		t.Fatalf("list/metadata = %d/%d want 2/0", lists, metas)
	}
}

func TestListOffsetsNotFoundNotRetriedNoMetadata(t *testing.T) {
	srv, addr, stop := startListOffsets(t, 2, nil)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	_, err = c.ListOffsets("missing", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 2 || be.Op != "list_offsets" {
		t.Fatalf("broker error %+v", be)
	}
	lists, metas, _, _ := srv.snapshot()
	if lists != 1 || metas != 0 {
		t.Fatalf("list/metadata = %d/%d want 1/0", lists, metas)
	}
}
