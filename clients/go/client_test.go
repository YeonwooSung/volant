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

const notLeader = 13
const timeoutCode uint16 = 7

type scriptedBroker struct {
	mu            sync.Mutex
	produceCodes  []uint16
	fetchCodes    []uint16
	meta          codec.MetadataResponse
	opcodes       []uint16
	produceReqs   []codec.ProduceRequest
	fetchReqs     []codec.FetchRequest
	initTxnIDs    []string
	initCount     int
	produceCount  int
	fetchCount    int
	metadataCount int
	acceptCount   int
	initPID       uint64
	initEpoch     uint16
	ln            net.Listener
}

func startScripted(t *testing.T, s *scriptedBroker) (addr string, stop func()) {
	t.Helper()
	if s.initPID == 0 {
		s.initPID = 42
	}
	if s.initEpoch == 0 {
		s.initEpoch = 1
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.ln = ln
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
	return ln.Addr().String(), func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *scriptedBroker) port() int {
	return s.ln.Addr().(*net.TCPAddr).Port
}

func (s *scriptedBroker) snapshot() (produces, fetches, metas, accepts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.produceCount, s.fetchCount, s.metadataCount, s.acceptCount
}

func (s *scriptedBroker) inits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.initCount
}

func (s *scriptedBroker) copyProduces() []codec.ProduceRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]codec.ProduceRequest, len(s.produceReqs))
	copy(out, s.produceReqs)
	return out
}

func (s *scriptedBroker) copyFetches() []codec.FetchRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]codec.FetchRequest, len(s.fetchReqs))
	copy(out, s.fetchReqs)
	return out
}

func (s *scriptedBroker) copyOpcodes() []uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint16, len(s.opcodes))
	copy(out, s.opcodes)
	return out
}

func (s *scriptedBroker) copyInitTxnIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.initTxnIDs))
	copy(out, s.initTxnIDs)
	return out
}

func (s *scriptedBroker) serve(conn net.Conn) {
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

func (s *scriptedBroker) handle(f *frame.Frame) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opcodes = append(s.opcodes, f.Opcode)
	var payload []byte
	var err error
	replyOp := f.Opcode
	switch f.Opcode {
	case codec.OpInitProducerId:
		s.initCount++
		req, e := codec.DecodeInitProducerIdRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.initTxnIDs = append(s.initTxnIDs, req.TransactionalID)
		payload, err = codec.EncodeInitProducerIdResponse(codec.InitProducerIdResponse{
			ProducerID: s.initPID, Epoch: s.initEpoch, ErrorCode: 0,
		})
		replyOp = codec.OpInitProducerIdResponse
	case codec.OpProduce:
		s.produceCount++
		req, e := codec.DecodeProduceRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.produceReqs = append(s.produceReqs, req)
		code := uint16(0)
		if len(s.produceCodes) > 0 {
			code = s.produceCodes[0]
			s.produceCodes = s.produceCodes[1:]
		}
		part := uint32(0)
		if req.Partition >= 0 {
			part = uint32(req.Partition)
		}
		off := uint64(0)
		count := uint32(0)
		if code == 0 {
			off = 7
			count = uint32(len(req.Messages))
		}
		payload, err = codec.EncodeProduceResponse(codec.ProduceResponse{
			Topic: req.Topic, Partition: part, BaseOffset: off, Count: count, ErrorCode: code,
		})
	case codec.OpFetch:
		s.fetchCount++
		req, e := codec.DecodeFetchRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.fetchReqs = append(s.fetchReqs, req)
		code := uint16(0)
		if len(s.fetchCodes) > 0 {
			code = s.fetchCodes[0]
			s.fetchCodes = s.fetchCodes[1:]
		}
		payload, err = codec.EncodeFetchResponse(codec.FetchResponse{
			Topic: req.Topic, Partition: req.Partition, HighWatermark: 0, ErrorCode: code, Records: nil,
		})
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

func leaderMeta(topic string, partition, leaderID uint32, host string, port int) codec.MetadataResponse {
	return codec.MetadataResponse{
		Brokers: []codec.BrokerInfo{
			{NodeID: 1, Host: "127.0.0.1", Port: 1},
			{NodeID: leaderID, Host: host, Port: uint16(port)},
		},
		Topics: []codec.TopicInfo{{
			Name:    topic,
			TopicID: 1,
			Partitions: []codec.PartitionInfo{{
				PartitionID: partition,
				Leader:      leaderID,
				Replicas:    []uint32{1, leaderID},
				ISR:         []uint32{leaderID},
				LeaderEpoch: 1,
			}},
		}},
	}
}

func TestProduceRedirectsToLeader(t *testing.T) {
	leader := &scriptedBroker{}
	leaderAddr, stopL := startScripted(t, leader)
	defer stopL()
	_ = leaderAddr

	follower := &scriptedBroker{
		produceCodes: []uint16{notLeader},
		meta:         leaderMeta("t", 0, 2, "127.0.0.1", leader.port()),
	}
	followerAddr, stopF := startScripted(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(followerAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	off, err := c.Produce("t", 0, nil, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if off != 7 {
		t.Fatalf("base offset: got %d want 7", off)
	}
	fp, _, fm, _ := follower.snapshot()
	lp, _, _, _ := leader.snapshot()
	if fp != 1 || fm != 1 {
		t.Fatalf("follower produce/metadata = %d/%d want 1/1", fp, fm)
	}
	if lp != 1 {
		t.Fatalf("leader produce = %d want 1", lp)
	}
}

func TestMaxRedirectsZeroRaisesOnFirst13(t *testing.T) {
	follower := &scriptedBroker{
		produceCodes: []uint16{notLeader},
		meta:         leaderMeta("t", 0, 2, "127.0.0.1", 9),
	}
	addr, stop := startScripted(t, follower)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRedirects(0)

	_, err = c.Produce("t", 0, nil, []byte("hello"))
	if err == nil {
		t.Fatal("expected BrokerError 13")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != notLeader {
		t.Fatalf("got %v want BrokerError code=13", err)
	}
	fp, _, fm, fa := follower.snapshot()
	if fp != 1 || fm != 0 || fa != 1 {
		t.Fatalf("produce/metadata/accepts = %d/%d/%d want 1/0/1", fp, fm, fa)
	}
}

func TestFetchRedirectsOnce(t *testing.T) {
	leader := &scriptedBroker{}
	_, stopL := startScripted(t, leader)
	defer stopL()

	follower := &scriptedBroker{
		fetchCodes: []uint16{notLeader},
		meta:       leaderMeta("t", 0, 2, "127.0.0.1", leader.port()),
	}
	addr, stopF := startScripted(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	recs, err := c.Fetch("t", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if recs == nil {
		recs = []volant.Record{}
	}
	if len(recs) != 0 {
		t.Fatalf("records: got %d want 0", len(recs))
	}
	_, ff, fm, _ := follower.snapshot()
	_, lf, _, _ := leader.snapshot()
	if ff != 1 || fm != 1 {
		t.Fatalf("follower fetch/metadata = %d/%d want 1/1", ff, fm)
	}
	if lf != 1 {
		t.Fatalf("leader fetch = %d want 1", lf)
	}
}

func TestMissingLeaderRaises13(t *testing.T) {
	follower := &scriptedBroker{
		produceCodes: []uint16{notLeader},
		meta: codec.MetadataResponse{
			Brokers: []codec.BrokerInfo{{NodeID: 1, Host: "127.0.0.1", Port: 1}},
			Topics: []codec.TopicInfo{{
				Name:    "t",
				TopicID: 1,
				Partitions: []codec.PartitionInfo{{
					PartitionID: 0,
					Leader:      99,
				}},
			}},
		},
	}
	addr, stop := startScripted(t, follower)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Produce("t", 0, nil, []byte("hello"))
	if err == nil {
		t.Fatal("expected BrokerError 13")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != notLeader {
		t.Fatalf("got %v want BrokerError code=13", err)
	}
	fp, _, fm, fa := follower.snapshot()
	if fp != 1 || fm != 1 || fa != 1 {
		t.Fatalf("produce/metadata/accepts = %d/%d/%d want 1/1/1 (no extra reconnect loop)", fp, fm, fa)
	}
}

func TestEmptyHostRaises13(t *testing.T) {
	follower := &scriptedBroker{
		produceCodes: []uint16{notLeader},
		meta: codec.MetadataResponse{
			Brokers: []codec.BrokerInfo{{NodeID: 2, Host: "", Port: 9092}},
			Topics: []codec.TopicInfo{{
				Name:       "t",
				TopicID:    1,
				Partitions: []codec.PartitionInfo{{PartitionID: 0, Leader: 2}},
			}},
		},
	}
	addr, stop := startScripted(t, follower)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Produce("t", 0, nil, []byte("x"))
	if err == nil {
		t.Fatal("expected BrokerError 13")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != notLeader {
		t.Fatalf("got %v want BrokerError code=13", err)
	}
	fp, _, fm, fa := follower.snapshot()
	if fp != 1 || fm != 1 || fa != 1 {
		t.Fatalf("produce/metadata/accepts = %d/%d/%d want 1/1/1", fp, fm, fa)
	}
}

func TestIdempotentProduceOnInitsThenSequences(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.EnableIdempotence()

	if _, err := c.Produce("t", 0, nil, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Produce("t", 0, nil, []byte("b")); err != nil {
		t.Fatal(err)
	}
	if srv.inits() != 1 {
		t.Fatalf("init count %d want 1", srv.inits())
	}
	txns := srv.copyInitTxnIDs()
	if len(txns) != 1 || txns[0] != "" {
		t.Fatalf("init txn ids %#v", txns)
	}
	ops := srv.copyOpcodes()
	if len(ops) != 3 || ops[0] != codec.OpInitProducerId || ops[1] != codec.OpProduce || ops[2] != codec.OpProduce {
		t.Fatalf("opcodes %#v", ops)
	}
	reqs := srv.copyProduces()
	if len(reqs) != 2 {
		t.Fatalf("produces %d", len(reqs))
	}
	if reqs[0].ProducerID != 42 || reqs[0].ProducerEpoch != 1 || reqs[0].BaseSequence != 0 {
		t.Fatalf("first trailer %+v", reqs[0])
	}
	if reqs[1].ProducerID != 42 || reqs[1].ProducerEpoch != 1 || reqs[1].BaseSequence != 1 {
		t.Fatalf("second trailer %+v", reqs[1])
	}
}

func TestIdempotentProduceOffDefaultTrailer(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Produce("t", 0, nil, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Produce("t", 0, nil, []byte("b")); err != nil {
		t.Fatal(err)
	}
	if srv.inits() != 0 {
		t.Fatalf("init count %d want 0", srv.inits())
	}
	for i, req := range srv.copyProduces() {
		if req.ProducerID != 0 || req.ProducerEpoch != 0 || req.BaseSequence != -1 {
			t.Fatalf("produce %d trailer %+v", i, req)
		}
	}
}

func TestIdempotentProduceRedirectKeepsSequence(t *testing.T) {
	leader := &scriptedBroker{}
	_, stopL := startScripted(t, leader)
	defer stopL()

	follower := &scriptedBroker{
		produceCodes: []uint16{notLeader},
		meta:         leaderMeta("t", 0, 2, "127.0.0.1", leader.port()),
	}
	addr, stopF := startScripted(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.EnableIdempotence()

	if _, err := c.Produce("t", 0, nil, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Produce("t", 0, nil, []byte("again")); err != nil {
		t.Fatal(err)
	}
	if follower.inits() != 1 || leader.inits() != 0 {
		t.Fatalf("init follower/leader = %d/%d want 1/0", follower.inits(), leader.inits())
	}
	fp, _, _, _ := follower.snapshot()
	lp, _, _, _ := leader.snapshot()
	if fp != 1 || lp != 2 {
		t.Fatalf("produce follower/leader = %d/%d want 1/2", fp, lp)
	}
	fReqs := follower.copyProduces()
	if fReqs[0].ProducerID != 42 || fReqs[0].BaseSequence != 0 {
		t.Fatalf("follower first %+v", fReqs[0])
	}
	lReqs := leader.copyProduces()
	if lReqs[0].BaseSequence != 0 || lReqs[1].BaseSequence != 1 {
		t.Fatalf("leader seqs %d %d", lReqs[0].BaseSequence, lReqs[1].BaseSequence)
	}
	if lReqs[0].ProducerID != 42 {
		t.Fatalf("leader pid %d", lReqs[0].ProducerID)
	}
}

func TestFetchOptsSendsKnobs(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.FetchOpts("t", 0, 0, 10, 4096, 100); err != nil {
		t.Fatal(err)
	}
	reqs := srv.copyFetches()
	if len(reqs) != 1 {
		t.Fatalf("fetches %d want 1", len(reqs))
	}
	req := reqs[0]
	if req.MaxMessages != 10 || req.MaxBytes != 4096 || req.MaxWaitMs != 100 {
		t.Fatalf("knobs %+v want max_messages=10 max_bytes=4096 max_wait_ms=100", req)
	}
}

func TestFetchDefaultKnobs(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Fetch("t", 0, 0); err != nil {
		t.Fatal(err)
	}
	reqs := srv.copyFetches()
	if len(reqs) != 1 {
		t.Fatalf("fetches %d want 1", len(reqs))
	}
	req := reqs[0]
	if req.MaxMessages != 128 || req.MaxBytes != 4*1024*1024 || req.MaxWaitMs != 0 {
		t.Fatalf("defaults %+v want 128 / 4MiB / 0", req)
	}
}

func TestProduceAcksAll(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.ProduceAcks("t", 0, nil, []byte("hello"), 255); err != nil {
		t.Fatal(err)
	}
	reqs := srv.copyProduces()
	if len(reqs) != 1 {
		t.Fatalf("produces %d want 1", len(reqs))
	}
	if reqs[0].Acks != 255 {
		t.Fatalf("acks %d want 255", reqs[0].Acks)
	}
}

func TestProduceDefaultAcks(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Produce("t", 0, nil, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	reqs := srv.copyProduces()
	if len(reqs) != 1 {
		t.Fatalf("produces %d want 1", len(reqs))
	}
	if reqs[0].Acks != 1 {
		t.Fatalf("acks %d want 1", reqs[0].Acks)
	}
}

func TestFetchOptsRedirectsOnce(t *testing.T) {
	leader := &scriptedBroker{}
	_, stopL := startScripted(t, leader)
	defer stopL()

	follower := &scriptedBroker{
		fetchCodes: []uint16{notLeader},
		meta:       leaderMeta("t", 0, 2, "127.0.0.1", leader.port()),
	}
	addr, stopF := startScripted(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	recs, err := c.FetchOpts("t", 0, 0, 10, 4096, 100)
	if err != nil {
		t.Fatal(err)
	}
	if recs == nil {
		recs = []volant.Record{}
	}
	if len(recs) != 0 {
		t.Fatalf("records: got %d want 0", len(recs))
	}
	_, ff, fm, _ := follower.snapshot()
	_, lf, _, _ := leader.snapshot()
	if ff != 1 || fm != 1 {
		t.Fatalf("follower fetch/metadata = %d/%d want 1/1", ff, fm)
	}
	if lf != 1 {
		t.Fatalf("leader fetch = %d want 1", lf)
	}
	fReq := follower.copyFetches()[0]
	lReq := leader.copyFetches()[0]
	if fReq.MaxMessages != 10 || fReq.MaxBytes != 4096 || fReq.MaxWaitMs != 100 {
		t.Fatalf("follower knobs %+v", fReq)
	}
	if lReq.MaxMessages != 10 || lReq.MaxBytes != 4096 || lReq.MaxWaitMs != 100 {
		t.Fatalf("leader knobs %+v", lReq)
	}
}

func TestProduceAcksRedirectsToLeader(t *testing.T) {
	leader := &scriptedBroker{}
	_, stopL := startScripted(t, leader)
	defer stopL()

	follower := &scriptedBroker{
		produceCodes: []uint16{notLeader},
		meta:         leaderMeta("t", 0, 2, "127.0.0.1", leader.port()),
	}
	addr, stopF := startScripted(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	off, err := c.ProduceAcks("t", 0, nil, []byte("hello"), 255)
	if err != nil {
		t.Fatal(err)
	}
	if off != 7 {
		t.Fatalf("base offset: got %d want 7", off)
	}
	fp, _, fm, _ := follower.snapshot()
	lp, _, _, _ := leader.snapshot()
	if fp != 1 || fm != 1 {
		t.Fatalf("follower produce/metadata = %d/%d want 1/1", fp, fm)
	}
	if lp != 1 {
		t.Fatalf("leader produce = %d want 1", lp)
	}
	if follower.copyProduces()[0].Acks != 255 {
		t.Fatalf("follower acks %d want 255", follower.copyProduces()[0].Acks)
	}
	if leader.copyProduces()[0].Acks != 255 {
		t.Fatalf("leader acks %d want 255", leader.copyProduces()[0].Acks)
	}
}

func TestDefaultMaxRetriesZeroRaisesOnTimeout(t *testing.T) {
	srv := &scriptedBroker{produceCodes: []uint16{timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Produce("t", 0, nil, []byte("hello"))
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	fp, _, _, _ := srv.snapshot()
	if fp != 1 {
		t.Fatalf("produce count %d want 1", fp)
	}
}

func TestRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{produceCodes: []uint16{timeoutCode, timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	off, err := c.Produce("t", 0, nil, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if off != 7 {
		t.Fatalf("base offset: got %d want 7", off)
	}
	fp, _, _, _ := srv.snapshot()
	if fp != 3 {
		t.Fatalf("produce count %d want 3", fp)
	}
}

func TestExhaustedRetriesRaises(t *testing.T) {
	srv := &scriptedBroker{produceCodes: []uint16{timeoutCode, timeoutCode, timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	_, err = c.Produce("t", 0, nil, []byte("hello"))
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	fp, _, _, _ := srv.snapshot()
	if fp != 3 {
		t.Fatalf("produce count %d want 3", fp)
	}
}

func TestError13DoesNotConsumeRetries(t *testing.T) {
	follower := &scriptedBroker{
		produceCodes: []uint16{notLeader},
		meta:         leaderMeta("t", 0, 2, "127.0.0.1", 9),
	}
	addr, stop := startScripted(t, follower)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRedirects(0)
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	_, err = c.Produce("t", 0, nil, []byte("hello"))
	if err == nil {
		t.Fatal("expected BrokerError 13")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != notLeader {
		t.Fatalf("got %v want BrokerError code=13", err)
	}
	fp, _, fm, _ := follower.snapshot()
	if fp != 1 || fm != 0 {
		t.Fatalf("produce/metadata = %d/%d want 1/0", fp, fm)
	}
}

func TestFailedRetriesDoNotIncrementSequence(t *testing.T) {
	srv := &scriptedBroker{produceCodes: []uint16{0, timeoutCode, timeoutCode, timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.EnableIdempotence()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	if _, err := c.Produce("t", 0, nil, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Produce("t", 0, nil, []byte("b")); err == nil {
		t.Fatal("expected timeout")
	}
	if _, err := c.Produce("t", 0, nil, []byte("c")); err != nil {
		t.Fatal(err)
	}
	reqs := srv.copyProduces()
	if len(reqs) != 5 {
		t.Fatalf("produces %d want 5", len(reqs))
	}
	want := []int32{0, 1, 1, 1, 1}
	for i, seq := range want {
		if reqs[i].BaseSequence != seq {
			t.Fatalf("produce %d seq %d want %d", i, reqs[i].BaseSequence, seq)
		}
	}
}

func TestFetchDefaultMaxRetriesZeroRaisesOnTimeout(t *testing.T) {
	srv := &scriptedBroker{fetchCodes: []uint16{timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Fetch("t", 0, 0)
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	_, ff, _, _ := srv.snapshot()
	if ff != 1 {
		t.Fatalf("fetch count %d want 1", ff)
	}
}

func TestFetchRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{fetchCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	recs, err := c.Fetch("t", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("records %d want 0", len(recs))
	}
	_, ff, _, _ := srv.snapshot()
	if ff != 2 {
		t.Fatalf("fetch count %d want 2", ff)
	}
}

func TestFetchError13StillRedirectsNotRetry(t *testing.T) {
	leader := &scriptedBroker{}
	_, stopL := startScripted(t, leader)
	defer stopL()

	follower := &scriptedBroker{
		fetchCodes: []uint16{notLeader},
		meta:       leaderMeta("t", 0, 2, "127.0.0.1", leader.port()),
	}
	addr, stopF := startScripted(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	recs, err := c.Fetch("t", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("records %d want 0", len(recs))
	}
	_, ff, fm, _ := follower.snapshot()
	_, lf, _, _ := leader.snapshot()
	if ff != 1 || fm != 1 {
		t.Fatalf("follower fetch/metadata = %d/%d want 1/1", ff, fm)
	}
	if lf != 1 {
		t.Fatalf("leader fetch = %d want 1", lf)
	}
}

func TestFetchExhaustedRetriesRaises(t *testing.T) {
	srv := &scriptedBroker{fetchCodes: []uint16{timeoutCode, timeoutCode, timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	_, err = c.Fetch("t", 0, 0)
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	_, ff, _, _ := srv.snapshot()
	if ff != 3 {
		t.Fatalf("fetch count %d want 3", ff)
	}
}


func batchMsgs(values ...string) []codec.ProduceMessage {
	out := make([]codec.ProduceMessage, len(values))
	for i, v := range values {
		out[i] = codec.ProduceMessage{Value: []byte(v), TimestampMs: -1}
	}
	return out
}

func TestProduceBatchThreeMessages(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	off, err := c.ProduceBatch("t", 0, batchMsgs("a", "b", "c"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if off != 7 {
		t.Fatalf("base offset: got %d want 7", off)
	}
	reqs := srv.copyProduces()
	if len(reqs) != 1 {
		t.Fatalf("produces %d want 1", len(reqs))
	}
	if n := len(reqs[0].Messages); n != 3 {
		t.Fatalf("messages %d want 3", n)
	}
	if reqs[0].Acks != 1 {
		t.Fatalf("acks %d want 1", reqs[0].Acks)
	}
	got := string(reqs[0].Messages[0].Value) + string(reqs[0].Messages[1].Value) + string(reqs[0].Messages[2].Value)
	if got != "abc" {
		t.Fatalf("values %q want abc", got)
	}
}

func TestProduceBatchEmpty(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.ProduceBatch("t", 0, nil, 1); err == nil {
		t.Fatal("expected empty-batch error")
	}
	if _, err := c.ProduceBatch("t", 0, []codec.ProduceMessage{}, 1); err == nil {
		t.Fatal("expected empty-batch error")
	}
	fp, _, _, _ := srv.snapshot()
	if fp != 0 {
		t.Fatalf("produce count %d want 0", fp)
	}
}

func TestProduceStillOneMessage(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Produce("t", 0, nil, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	reqs := srv.copyProduces()
	if len(reqs) != 1 {
		t.Fatalf("produces %d want 1", len(reqs))
	}
	if n := len(reqs[0].Messages); n != 1 {
		t.Fatalf("messages %d want 1", n)
	}
	if string(reqs[0].Messages[0].Value) != "hello" {
		t.Fatalf("value %q want hello", reqs[0].Messages[0].Value)
	}
}

func TestProduceBatchRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{produceCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	off, err := c.ProduceBatch("t", 0, batchMsgs("a", "b", "c"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if off != 7 {
		t.Fatalf("base offset: got %d want 7", off)
	}
	reqs := srv.copyProduces()
	if len(reqs) != 2 {
		t.Fatalf("produces %d want 2", len(reqs))
	}
	for i, req := range reqs {
		if n := len(req.Messages); n != 3 {
			t.Fatalf("produce %d messages %d want 3", i, n)
		}
		got := string(req.Messages[0].Value) + string(req.Messages[1].Value) + string(req.Messages[2].Value)
		if got != "abc" {
			t.Fatalf("produce %d values %q want abc", i, got)
		}
	}
}

func TestProduceBatchRedirectsToLeader(t *testing.T) {
	leader := &scriptedBroker{}
	_, stopL := startScripted(t, leader)
	defer stopL()

	follower := &scriptedBroker{
		produceCodes: []uint16{notLeader},
		meta:         leaderMeta("t", 0, 2, "127.0.0.1", leader.port()),
	}
	addr, stopF := startScripted(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	off, err := c.ProduceBatch("t", 0, batchMsgs("a", "b", "c"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if off != 7 {
		t.Fatalf("base offset: got %d want 7", off)
	}
	fp, _, fm, _ := follower.snapshot()
	lp, _, _, _ := leader.snapshot()
	if fp != 1 || fm != 1 {
		t.Fatalf("follower produce/metadata = %d/%d want 1/1", fp, fm)
	}
	if lp != 1 {
		t.Fatalf("leader produce = %d want 1", lp)
	}
	if n := len(follower.copyProduces()[0].Messages); n != 3 {
		t.Fatalf("follower messages %d want 3", n)
	}
	if n := len(leader.copyProduces()[0].Messages); n != 3 {
		t.Fatalf("leader messages %d want 3", n)
	}
}

func TestProduceBatchIncrementsSequenceByCount(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.EnableIdempotence()

	if _, err := c.ProduceBatch("t", 0, batchMsgs("a", "b", "c"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ProduceBatch("t", 0, batchMsgs("d", "e"), 1); err != nil {
		t.Fatal(err)
	}
	reqs := srv.copyProduces()
	if len(reqs) != 2 {
		t.Fatalf("produces %d want 2", len(reqs))
	}
	if reqs[0].BaseSequence != 0 {
		t.Fatalf("first seq %d want 0", reqs[0].BaseSequence)
	}
	if reqs[1].BaseSequence != 3 {
		t.Fatalf("second seq %d want 3", reqs[1].BaseSequence)
	}
}

const notController uint16 = 14

type createTopicReply struct {
	code    uint16
	message string
	asError bool
}

type adminBroker struct {
	mu                    sync.Mutex
	createTopicReplies    []createTopicReply
	createPartitionsCodes []uint16
	createAclsCodes       []uint16
	reassignCodes         []uint16
	meta                  codec.MetadataResponse
	createTopicCount      int
	createPartitionsCount int
	createAclsCount       int
	reassignCount         int
	metadataCount         int
	listMembersCount      int
	acceptCount           int
	ln                    net.Listener
}

func startAdmin(t *testing.T, s *adminBroker) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.ln = ln
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
	return ln.Addr().String(), func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *adminBroker) port() int {
	return s.ln.Addr().(*net.TCPAddr).Port
}

func (s *adminBroker) snapshot() (createTopic, createParts, createAcls, reassign, metas, accepts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createTopicCount, s.createPartitionsCount, s.createAclsCount, s.reassignCount, s.metadataCount, s.acceptCount
}

func (s *adminBroker) serve(conn net.Conn) {
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

func (s *adminBroker) handle(f *frame.Frame) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var payload []byte
	var err error
	replyOp := f.Opcode
	switch f.Opcode {
	case codec.OpCreateTopic:
		s.createTopicCount++
		req, e := codec.DecodeCreateTopicRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		rep := createTopicReply{}
		if len(s.createTopicReplies) > 0 {
			rep = s.createTopicReplies[0]
			s.createTopicReplies = s.createTopicReplies[1:]
		}
		if rep.asError {
			payload, err = codec.EncodeErrorResponse(codec.ErrorResponse{Code: rep.code, Message: rep.message})
			replyOp = codec.OpError
			break
		}
		id := uint32(0)
		parts := uint32(0)
		if rep.code == 0 {
			id = 1
			parts = req.Partitions
		}
		payload, err = codec.EncodeCreateTopicResponse(codec.CreateTopicResponse{
			TopicID: id, Name: req.Name, Partitions: parts, ErrorCode: rep.code,
		})
	case codec.OpCreatePartitions:
		s.createPartitionsCount++
		req, e := codec.DecodeCreatePartitionsRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		code := uint16(0)
		if len(s.createPartitionsCodes) > 0 {
			code = s.createPartitionsCodes[0]
			s.createPartitionsCodes = s.createPartitionsCodes[1:]
		}
		n := uint32(0)
		if code == 0 {
			n = req.TotalCount
		}
		payload, err = codec.EncodeCreatePartitionsResponse(codec.CreatePartitionsResponse{
			ErrorCode: code, Topic: req.Topic, Partitions: n,
		})
		replyOp = codec.OpCreatePartitionsResponse
	case codec.OpCreateAcls:
		s.createAclsCount++
		code := uint16(0)
		if len(s.createAclsCodes) > 0 {
			code = s.createAclsCodes[0]
			s.createAclsCodes = s.createAclsCodes[1:]
		}
		payload, err = codec.EncodeCreateAclsResponse(codec.CreateAclsResponse{ErrorCode: code})
		replyOp = codec.OpCreateAclsResponse
	case codec.OpReassignPartitions:
		s.reassignCount++
		code := uint16(0)
		if len(s.reassignCodes) > 0 {
			code = s.reassignCodes[0]
			s.reassignCodes = s.reassignCodes[1:]
		}
		gen := uint32(0)
		if code == 0 {
			gen = 7
		}
		payload, err = codec.EncodeReassignPartitionsResponse(codec.ReassignPartitionsResponse{
			ErrorCode: code, Generation: gen,
		})
		replyOp = codec.OpReassignPartitionsResponse
	case codec.OpMetadata:
		s.metadataCount++
		payload, err = codec.EncodeMetadataResponse(s.meta)
	case codec.OpListMembers:
		s.listMembersCount++
		payload, err = codec.EncodeListMembersResponse(codec.ListMembersResponse{
			ErrorCode: 0, Generation: 0, Brokers: nil, Live: nil,
		})
		replyOp = codec.OpListMembersResponse
	default:
		return nil, &frame.ProtocolError{Msg: "unexpected opcode"}
	}
	if err != nil {
		return nil, err
	}
	return frame.Encode(replyOp, f.CorrelationID, payload)
}

func controllerMeta(nodeID uint32, host string, port int) codec.MetadataResponse {
	return codec.MetadataResponse{
		Brokers: []codec.BrokerInfo{
			{NodeID: 1, Host: "127.0.0.1", Port: 1},
			{NodeID: nodeID, Host: host, Port: uint16(port)},
		},
	}
}

func otherBrokerMeta(currentPort int, host string, port int) codec.MetadataResponse {
	return codec.MetadataResponse{
		Brokers: []codec.BrokerInfo{
			{NodeID: 1, Host: "127.0.0.1", Port: uint16(currentPort)},
			{NodeID: 2, Host: host, Port: uint16(port)},
		},
	}
}

func brokerCode(err error) uint16 {
	be, ok := err.(*codec.BrokerError)
	if !ok {
		return 0
	}
	return be.Code
}

func TestCreateTopicError14RedirectsViaControllerID(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		createTopicReplies: []createTopicReply{{
			code: notController, message: "not controller; controller_id=2", asError: true,
		}},
		meta: controllerMeta(2, "127.0.0.1", leader.port()),
	}
	fAddr, stopF := startAdmin(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(fAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.CreateTopic("events", 1); err != nil {
		t.Fatal(err)
	}
	ct, _, _, _, metas, _ := follower.snapshot()
	if ct != 1 || metas != 1 {
		t.Fatalf("follower create_topic=%d metadata=%d want 1,1", ct, metas)
	}
	lct, _, _, _, _, _ := leader.snapshot()
	if lct != 1 {
		t.Fatalf("leader create_topic=%d want 1", lct)
	}
}

func TestCreatePartitionsError14NoHintPicksOtherBroker(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		createPartitionsCodes: []uint16{notController},
	}
	fAddr, stopF := startAdmin(t, follower)
	defer stopF()
	follower.mu.Lock()
	follower.meta = otherBrokerMeta(follower.port(), "127.0.0.1", leader.port())
	follower.mu.Unlock()

	c, err := volant.DialTimeout(fAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	n, err := c.CreatePartitions("events", 4)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("partitions %d want 4", n)
	}
	_, cp, _, _, metas, _ := follower.snapshot()
	if cp != 1 || metas != 1 {
		t.Fatalf("follower create_partitions=%d metadata=%d want 1,1", cp, metas)
	}
	_, lcp, _, _, _, _ := leader.snapshot()
	if lcp != 1 {
		t.Fatalf("leader create_partitions=%d want 1", lcp)
	}
}

func TestMaxRedirectsZeroRaisesOnFirst14(t *testing.T) {
	follower := &adminBroker{
		createTopicReplies: []createTopicReply{{
			code: notController, message: "not controller; controller_id=2", asError: true,
		}},
		meta: controllerMeta(2, "127.0.0.1", 9),
	}
	fAddr, stopF := startAdmin(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(fAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRedirects(0)
	err = c.CreateTopic("events", 1)
	if brokerCode(err) != notController {
		t.Fatalf("err=%v want code 14", err)
	}
	ct, _, _, _, metas, accepts := follower.snapshot()
	if ct != 1 || metas != 0 || accepts != 1 {
		t.Fatalf("create_topic=%d metadata=%d accepts=%d want 1,0,1", ct, metas, accepts)
	}
}

func TestHelperNoOtherBrokerRaises14(t *testing.T) {
	follower := &adminBroker{
		createPartitionsCodes: []uint16{notController},
	}
	fAddr, stopF := startAdmin(t, follower)
	defer stopF()
	follower.meta = codec.MetadataResponse{
		Brokers: []codec.BrokerInfo{{NodeID: 1, Host: "127.0.0.1", Port: uint16(follower.port())}},
	}

	c, err := volant.DialTimeout(fAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.CreatePartitions("events", 4)
	if brokerCode(err) != notController {
		t.Fatalf("err=%v want code 14", err)
	}
	_, cp, _, _, metas, _ := follower.snapshot()
	if cp != 1 || metas != 1 {
		t.Fatalf("create_partitions=%d metadata=%d want 1,1", cp, metas)
	}
}

func TestHelperEmptyHostRaises14(t *testing.T) {
	follower := &adminBroker{
		createPartitionsCodes: []uint16{notController},
		meta: codec.MetadataResponse{
			Brokers: []codec.BrokerInfo{{NodeID: 2, Host: "", Port: 9092}},
		},
	}
	fAddr, stopF := startAdmin(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(fAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.CreatePartitions("events", 4)
	if brokerCode(err) != notController {
		t.Fatalf("err=%v want code 14", err)
	}
	_, cp, _, _, metas, _ := follower.snapshot()
	if cp != 1 || metas != 1 {
		t.Fatalf("create_partitions=%d metadata=%d want 1,1", cp, metas)
	}
}

func TestCreateAclsError14ThenOk(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		createAclsCodes: []uint16{notController},
	}
	fAddr, stopF := startAdmin(t, follower)
	defer stopF()
	follower.mu.Lock()
	follower.meta = otherBrokerMeta(follower.port(), "127.0.0.1", leader.port())
	follower.mu.Unlock()

	c, err := volant.DialTimeout(fAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	entry := codec.AclBinding{Principal: "User:alice", ResourceType: 0, Resource: "events", Operation: 3, Permission: 1}
	if err := c.CreateAcls([]codec.AclBinding{entry}); err != nil {
		t.Fatal(err)
	}
	_, _, ca, _, metas, _ := follower.snapshot()
	if ca != 1 || metas != 1 {
		t.Fatalf("follower create_acls=%d metadata=%d want 1,1", ca, metas)
	}
	_, _, lca, _, _, _ := leader.snapshot()
	if lca != 1 {
		t.Fatalf("leader create_acls=%d want 1", lca)
	}
}

func TestReassignPartitionsError14ThenOk(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		reassignCodes: []uint16{notController},
	}
	fAddr, stopF := startAdmin(t, follower)
	defer stopF()
	follower.mu.Lock()
	follower.meta = otherBrokerMeta(follower.port(), "127.0.0.1", leader.port())
	follower.mu.Unlock()

	c, err := volant.DialTimeout(fAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	gen, err := c.ReassignPartitions("events", []uint32{1, 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 7 {
		t.Fatalf("generation %d want 7", gen)
	}
	_, _, _, rs, metas, _ := follower.snapshot()
	if rs != 1 || metas != 1 {
		t.Fatalf("follower reassign=%d metadata=%d want 1,1", rs, metas)
	}
	_, _, _, lrs, _, _ := leader.snapshot()
	if lrs != 1 {
		t.Fatalf("leader reassign=%d want 1", lrs)
	}
}

