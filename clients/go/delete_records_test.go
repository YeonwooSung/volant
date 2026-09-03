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

type deleteRecordsServer struct {
	mu            sync.Mutex
	errorCodes    []uint16
	lowWatermark  uint64
	meta          codec.MetadataResponse
	topic         string
	partition     uint32
	beforeOffset  uint64
	waitMajority  uint8
	opcodes       []uint16
	deleteCount   int
	metadataCount int
	acceptCount   int
	err           error
	ln            net.Listener
}

func startDeleteRecords(t *testing.T, errorCode uint16, lowWatermark uint64) (*deleteRecordsServer, string, func()) {
	t.Helper()
	return startDeleteRecordsCodes(t, []uint16{errorCode}, lowWatermark)
}

func startDeleteRecordsCodes(t *testing.T, errorCodes []uint16, lowWatermark uint64) (*deleteRecordsServer, string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	codes := make([]uint16, len(errorCodes))
	copy(codes, errorCodes)
	s := &deleteRecordsServer{
		errorCodes:   codes,
		lowWatermark: lowWatermark,
		ln:           ln,
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

func (s *deleteRecordsServer) port() int {
	return s.ln.Addr().(*net.TCPAddr).Port
}

func (s *deleteRecordsServer) snapshot() (deletes, metas, accepts int, opcodes []uint16, wait uint8, before uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ops := make([]uint16, len(s.opcodes))
	copy(ops, s.opcodes)
	return s.deleteCount, s.metadataCount, s.acceptCount, ops, s.waitMajority, s.beforeOffset
}

func (s *deleteRecordsServer) serve(conn net.Conn) {
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

func (s *deleteRecordsServer) handle(f *frame.Frame) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opcodes = append(s.opcodes, f.Opcode)
	var payload []byte
	var err error
	replyOp := f.Opcode
	switch f.Opcode {
	case codec.OpDeleteRecords:
		s.deleteCount++
		req, e := codec.DecodeDeleteRecordsRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.topic = req.Topic
		s.partition = req.Partition
		s.beforeOffset = req.BeforeOffset
		s.waitMajority = req.WaitMajority
		code := uint16(0)
		if len(s.errorCodes) > 0 {
			code = s.errorCodes[0]
			s.errorCodes = s.errorCodes[1:]
		}
		lw := uint64(0)
		if code == 0 {
			lw = s.lowWatermark
		}
		payload, err = codec.EncodeDeleteRecordsResponse(codec.DeleteRecordsResponse{
			ErrorCode:    code,
			Topic:        req.Topic,
			Partition:    req.Partition,
			LowWatermark: lw,
		})
		replyOp = codec.OpDeleteRecordsResponse
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

func TestDeleteRecordsSuccessReturnsLowWatermark(t *testing.T) {
	srv, addr, stop := startDeleteRecords(t, 0, 96)
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
	if out != (volant.DeleteRecordsResult{Topic: "events", Partition: 2, LowWatermark: 96}) {
		t.Fatalf("parsed %+v", out)
	}
	srv.mu.Lock()
	if srv.topic != "events" || srv.partition != 2 || srv.beforeOffset != 100 || srv.waitMajority != 0 {
		t.Fatalf("wire request topic=%s part=%d before=%d wait=%d", srv.topic, srv.partition, srv.beforeOffset, srv.waitMajority)
	}
	srv.mu.Unlock()
	flagged, err := c.DeleteRecordsWithWaitFlag("events", 2, 100, 1)
	if err != nil {
		t.Fatal(err)
	}
	if flagged.LowWatermark != 96 {
		t.Fatalf("flagged %+v", flagged)
	}
	deletes, _, _, ops, wait, _ := srv.snapshot()
	if wait != 1 {
		t.Fatalf("wait flag %d", wait)
	}
	if deletes != 2 || len(ops) != 2 || ops[0] != codec.OpDeleteRecords || ops[1] != codec.OpDeleteRecords {
		t.Fatalf("opcodes %v deletes=%d", ops, deletes)
	}
}

func TestDeleteRecordsDefaultWaitMajorityZero(t *testing.T) {
	srv, addr, stop := startDeleteRecords(t, 0, 96)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.DeleteRecordsWait() != 0 {
		t.Fatalf("DeleteRecordsWait() %d want 0", c.DeleteRecordsWait())
	}
	if _, err := c.DeleteRecords("events", 2, 100); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, wait, _ := srv.snapshot()
	if wait != 0 {
		t.Fatalf("wait flag %d want 0", wait)
	}
}

func TestDeleteRecordsSetDeleteRecordsWait(t *testing.T) {
	srv, addr, stop := startDeleteRecords(t, 0, 96)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeleteRecordsWait(1)
	if c.DeleteRecordsWait() != 1 {
		t.Fatalf("DeleteRecordsWait() %d want 1", c.DeleteRecordsWait())
	}
	if _, err := c.DeleteRecords("events", 2, 100); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, wait, _ := srv.snapshot()
	if wait != 1 {
		t.Fatalf("wait flag %d want 1", wait)
	}
}

func TestDeleteRecordsWithWaitFlagExplicitWins(t *testing.T) {
	srv, addr, stop := startDeleteRecords(t, 0, 96)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeleteRecordsWait(1)
	if _, err := c.DeleteRecordsWithWaitFlag("events", 2, 100, 2); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, wait, _ := srv.snapshot()
	if wait != 2 {
		t.Fatalf("wait flag %d want 2", wait)
	}
}

func TestDeleteRecordsError13MaxRedirectsZeroRaises(t *testing.T) {
	srv, addr, stop := startDeleteRecords(t, 13, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRedirects(0)
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
	deletes, metas, _, ops, _, _ := srv.snapshot()
	if deletes != 1 || metas != 0 || len(ops) != 1 || ops[0] != codec.OpDeleteRecords {
		t.Fatalf("opcodes %v deletes=%d metas=%d", ops, deletes, metas)
	}
}

func TestDeleteRecordsError13RedirectsToLeader(t *testing.T) {
	leader, _, stopL := startDeleteRecords(t, 0, 96)
	defer stopL()
	follower, faddr, stopF := startDeleteRecords(t, 13, 0)
	defer stopF()
	follower.mu.Lock()
	follower.meta = leaderMeta("events", 2, 2, "127.0.0.1", leader.port())
	follower.mu.Unlock()

	c, err := volant.DialTimeout(faddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.DeleteRecordsWithWaitFlag("events", 2, 100, 1)
	if err != nil {
		t.Fatal(err)
	}
	if out != (volant.DeleteRecordsResult{Topic: "events", Partition: 2, LowWatermark: 96}) {
		t.Fatalf("parsed %+v", out)
	}
	fd, fm, _, _, _, _ := follower.snapshot()
	ld, _, _, _, wait, before := leader.snapshot()
	if fd != 1 || fm != 1 {
		t.Fatalf("follower delete/metadata = %d/%d want 1/1", fd, fm)
	}
	if ld != 1 || wait != 1 || before != 100 {
		t.Fatalf("leader delete=%d wait=%d before=%d", ld, wait, before)
	}
}

func TestDeleteRecordsError13UnknownTopicRaises(t *testing.T) {
	srv, addr, stop := startDeleteRecords(t, 13, 0)
	defer stop()
	srv.mu.Lock()
	srv.meta = codec.MetadataResponse{
		Brokers: []codec.BrokerInfo{{NodeID: 1, Host: "127.0.0.1", Port: uint16(srv.port())}},
		Topics:  nil,
	}
	srv.mu.Unlock()

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
	deletes, metas, accepts, _, _, _ := srv.snapshot()
	if deletes != 1 || metas != 1 || accepts != 1 {
		t.Fatalf("delete/metadata/accepts = %d/%d/%d want 1/1/1", deletes, metas, accepts)
	}
}

func TestDeleteRecordsDefaultMaxRetriesZeroRaisesOnTimeout(t *testing.T) {
	srv, addr, stop := startDeleteRecords(t, 7, 0)
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
	if be.Code != 7 || be.Op != "delete_records" {
		t.Fatalf("broker error %+v", be)
	}
	deletes, metas, _, ops, _, _ := srv.snapshot()
	if deletes != 1 || metas != 0 || len(ops) != 1 || ops[0] != codec.OpDeleteRecords {
		t.Fatalf("opcodes %v deletes=%d metas=%d", ops, deletes, metas)
	}
}

func TestDeleteRecordsRetriesTimeoutThenOk(t *testing.T) {
	srv, addr, stop := startDeleteRecordsCodes(t, []uint16{7, 0}, 96)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	out, err := c.DeleteRecords("events", 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if out != (volant.DeleteRecordsResult{Topic: "events", Partition: 2, LowWatermark: 96}) {
		t.Fatalf("parsed %+v", out)
	}
	deletes, metas, _, ops, _, _ := srv.snapshot()
	if deletes != 2 || metas != 0 || len(ops) != 2 || ops[0] != codec.OpDeleteRecords || ops[1] != codec.OpDeleteRecords {
		t.Fatalf("opcodes %v deletes=%d metas=%d", ops, deletes, metas)
	}
}

func TestDeleteRecordsError13RedirectNotCountedAsRetry(t *testing.T) {
	leader, _, stopL := startDeleteRecords(t, 0, 96)
	defer stopL()
	follower, faddr, stopF := startDeleteRecords(t, 13, 0)
	defer stopF()
	follower.mu.Lock()
	follower.meta = leaderMeta("events", 2, 2, "127.0.0.1", leader.port())
	follower.mu.Unlock()

	c, err := volant.DialTimeout(faddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	out, err := c.DeleteRecordsWithWaitFlag("events", 2, 100, 1)
	if err != nil {
		t.Fatal(err)
	}
	if out != (volant.DeleteRecordsResult{Topic: "events", Partition: 2, LowWatermark: 96}) {
		t.Fatalf("parsed %+v", out)
	}
	fd, fm, _, _, _, _ := follower.snapshot()
	ld, _, _, _, wait, before := leader.snapshot()
	if fd != 1 || fm != 1 {
		t.Fatalf("follower delete/metadata = %d/%d want 1/1", fd, fm)
	}
	if ld != 1 || wait != 1 || before != 100 {
		t.Fatalf("leader delete=%d wait=%d before=%d", ld, wait, before)
	}
}

func TestDeleteRecordsNotFoundNotRetried(t *testing.T) {
	srv, addr, stop := startDeleteRecords(t, 2, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	_, err = c.DeleteRecords("events", 0, 10)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 2 || be.Op != "delete_records" {
		t.Fatalf("broker error %+v", be)
	}
	deletes, metas, _, ops, _, _ := srv.snapshot()
	if deletes != 1 || metas != 0 || len(ops) != 1 || ops[0] != codec.OpDeleteRecords {
		t.Fatalf("opcodes %v deletes=%d metas=%d", ops, deletes, metas)
	}
}

func TestDeleteRecordsExhaustedRetriesRaises(t *testing.T) {
	srv, addr, stop := startDeleteRecordsCodes(t, []uint16{7, 7, 7}, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	_, err = c.DeleteRecords("events", 0, 10)
	if err == nil {
		t.Fatal("expected error")
	}
	var be *volant.BrokerError
	if !errors.As(err, &be) {
		t.Fatalf("got %T %v", err, err)
	}
	if be.Code != 7 || be.Op != "delete_records" {
		t.Fatalf("broker error %+v", be)
	}
	deletes, metas, _, ops, _, _ := srv.snapshot()
	if deletes != 3 || metas != 0 || len(ops) != 3 {
		t.Fatalf("opcodes %v deletes=%d metas=%d", ops, deletes, metas)
	}
}
