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
	mu                 sync.Mutex
	produceCodes       []uint16
	initCodes          []uint16
	fetchCodes         []uint16
	heartbeatCodes     []uint16
	leaveGroupCodes    []uint16
	offsetCommitCodes   []uint16
	offsetFetchCodes    []uint16
	offsetFetchEntries  []codec.OffsetFetchEntry
	deleteOffsetsCodes  []uint16
	listOffsetsCodes    []uint16
	describeGroupCodes  []uint16
	listGroupsCodes     []uint16
	metadataCodes       []uint16
	listMembersCodes    []uint16
	listMembersReplies  []createTopicReply
	meta                codec.MetadataResponse
	opcodes             []uint16
	produceReqs         []codec.ProduceRequest
	fetchReqs           []codec.FetchRequest
	offsetCommitReqs    []codec.OffsetCommitRequest
	initTxnIDs          []string
	initCount           int
	produceCount        int
	fetchCount          int
	heartbeatCount      int
	leaveGroupCount     int
	offsetCommitCount   int
	offsetFetchCount    int
	deleteOffsetsCount  int
	listOffsetsCount    int
	describeGroupCount  int
	listGroupsCount     int
	listMembersCount    int
	metadataCount      int
	acceptCount        int
	initPID            uint64
	initEpoch          uint16
	ln                 net.Listener
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

func (s *scriptedBroker) heartbeats() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heartbeatCount
}

func (s *scriptedBroker) leaveGroups() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaveGroupCount
}

func (s *scriptedBroker) offsetCommits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsetCommitCount
}

func (s *scriptedBroker) offsetFetches() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsetFetchCount
}

func (s *scriptedBroker) deleteOffsets() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteOffsetsCount
}

func (s *scriptedBroker) listOffsets() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listOffsetsCount
}

func (s *scriptedBroker) describeGroups() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.describeGroupCount
}

func (s *scriptedBroker) listGroups() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listGroupsCount
}

func (s *scriptedBroker) metadatas() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metadataCount
}

func (s *scriptedBroker) listMembers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listMembersCount
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

func (s *scriptedBroker) copyOffsetCommits() []codec.OffsetCommitRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]codec.OffsetCommitRequest, len(s.offsetCommitReqs))
	copy(out, s.offsetCommitReqs)
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
		code := uint16(0)
		if len(s.initCodes) > 0 {
			code = s.initCodes[0]
			s.initCodes = s.initCodes[1:]
		}
		payload, err = codec.EncodeInitProducerIdResponse(codec.InitProducerIdResponse{
			ProducerID: s.initPID, Epoch: s.initEpoch, ErrorCode: code,
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
	case codec.OpHeartbeat:
		s.heartbeatCount++
		code := uint16(0)
		if len(s.heartbeatCodes) > 0 {
			code = s.heartbeatCodes[0]
			s.heartbeatCodes = s.heartbeatCodes[1:]
		}
		payload, err = codec.EncodeHeartbeatResponse(codec.HeartbeatResponse{ErrorCode: code})
	case codec.OpLeaveGroup:
		s.leaveGroupCount++
		code := uint16(0)
		if len(s.leaveGroupCodes) > 0 {
			code = s.leaveGroupCodes[0]
			s.leaveGroupCodes = s.leaveGroupCodes[1:]
		}
		payload, err = codec.EncodeLeaveGroupResponse(codec.LeaveGroupResponse{ErrorCode: code})
	case codec.OpOffsetCommit:
		s.offsetCommitCount++
		req, e := codec.DecodeOffsetCommitRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.offsetCommitReqs = append(s.offsetCommitReqs, req)
		code := uint16(0)
		if len(s.offsetCommitCodes) > 0 {
			code = s.offsetCommitCodes[0]
			s.offsetCommitCodes = s.offsetCommitCodes[1:]
		}
		payload, err = codec.EncodeOffsetCommitResponse(codec.OffsetCommitResponse{ErrorCode: code})
	case codec.OpOffsetFetch:
		s.offsetFetchCount++
		code := uint16(0)
		if len(s.offsetFetchCodes) > 0 {
			code = s.offsetFetchCodes[0]
			s.offsetFetchCodes = s.offsetFetchCodes[1:]
		}
		payload, err = codec.EncodeOffsetFetchResponse(codec.OffsetFetchResponse{
			ErrorCode: code,
			Entries:   s.offsetFetchEntries,
		})
	case codec.OpDeleteOffsets:
		s.deleteOffsetsCount++
		code := uint16(0)
		if len(s.deleteOffsetsCodes) > 0 {
			code = s.deleteOffsetsCodes[0]
			s.deleteOffsetsCodes = s.deleteOffsetsCodes[1:]
		}
		payload, err = codec.EncodeDeleteOffsetsResponse(codec.DeleteOffsetsResponse{ErrorCode: code})
		replyOp = codec.OpDeleteOffsetsResponse
	case codec.OpListOffsets:
		s.listOffsetsCount++
		code := uint16(0)
		if len(s.listOffsetsCodes) > 0 {
			code = s.listOffsetsCodes[0]
			s.listOffsetsCodes = s.listOffsetsCodes[1:]
		}
		payload, err = codec.EncodeListOffsetsResponse(codec.ListOffsetsResponse{ErrorCode: code})
		replyOp = codec.OpListOffsetsResponse
	case codec.OpDescribeGroup:
		s.describeGroupCount++
		code := uint16(0)
		if len(s.describeGroupCodes) > 0 {
			code = s.describeGroupCodes[0]
			s.describeGroupCodes = s.describeGroupCodes[1:]
		}
		payload, err = codec.EncodeDescribeGroupResponse(codec.DescribeGroupResponse{ErrorCode: code})
		replyOp = codec.OpDescribeGroupResponse
	case codec.OpListGroups:
		s.listGroupsCount++
		code := uint16(0)
		if len(s.listGroupsCodes) > 0 {
			code = s.listGroupsCodes[0]
			s.listGroupsCodes = s.listGroupsCodes[1:]
		}
		payload, err = codec.EncodeListGroupsResponse(codec.ListGroupsResponse{ErrorCode: code})
		replyOp = codec.OpListGroupsResponse
	case codec.OpListMembers:
		s.listMembersCount++
		rep := createTopicReply{}
		if len(s.listMembersReplies) > 0 {
			rep = s.listMembersReplies[0]
			s.listMembersReplies = s.listMembersReplies[1:]
		} else if len(s.listMembersCodes) > 0 {
			rep.code = s.listMembersCodes[0]
			s.listMembersCodes = s.listMembersCodes[1:]
		}
		if rep.asError {
			payload, err = codec.EncodeErrorResponse(codec.ErrorResponse{Code: rep.code, Message: rep.message})
			replyOp = codec.OpError
			break
		}
		payload, err = codec.EncodeListMembersResponse(codec.ListMembersResponse{ErrorCode: rep.code})
		replyOp = codec.OpListMembersResponse
	case codec.OpMetadata:
		s.metadataCount++
		code := uint16(0)
		if len(s.metadataCodes) > 0 {
			code = s.metadataCodes[0]
			s.metadataCodes = s.metadataCodes[1:]
		}
		if code != 0 {
			payload, err = codec.EncodeErrorResponse(codec.ErrorResponse{Code: code})
			replyOp = codec.OpError
			break
		}
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

const unknownProducer uint16 = 21

func TestDefaultMaxRetriesZeroRaisesOnInitTimeout(t *testing.T) {
	srv := &scriptedBroker{initCodes: []uint16{timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.EnableIdempotence()

	_, err = c.Produce("t", 0, nil, []byte("hello"))
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if srv.inits() != 1 {
		t.Fatalf("init count %d want 1", srv.inits())
	}
	fp, _, _, _ := srv.snapshot()
	if fp != 0 {
		t.Fatalf("produce count %d want 0", fp)
	}
}

func TestRetriesInitTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{initCodes: []uint16{timeoutCode, 0}}
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

	off, err := c.Produce("t", 0, nil, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if off != 7 {
		t.Fatalf("base offset: got %d want 7", off)
	}
	if srv.inits() != 2 {
		t.Fatalf("init count %d want 2", srv.inits())
	}
	fp, _, _, _ := srv.snapshot()
	if fp != 1 {
		t.Fatalf("produce count %d want 1", fp)
	}
}

func TestInitUnknownProducerIdNotRetried(t *testing.T) {
	srv := &scriptedBroker{initCodes: []uint16{unknownProducer}}
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

	_, err = c.Produce("t", 0, nil, []byte("hello"))
	if err == nil {
		t.Fatal("expected BrokerError 21")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != unknownProducer {
		t.Fatalf("got %v want BrokerError code=21", err)
	}
	if srv.inits() != 1 {
		t.Fatalf("init count %d want 1", srv.inits())
	}
	fp, _, _, _ := srv.snapshot()
	if fp != 0 {
		t.Fatalf("produce count %d want 0", fp)
	}
}

func TestInitExhaustedRetriesRaises(t *testing.T) {
	srv := &scriptedBroker{initCodes: []uint16{timeoutCode, timeoutCode, timeoutCode}}
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

	_, err = c.Produce("t", 0, nil, []byte("hello"))
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if srv.inits() != 3 {
		t.Fatalf("init count %d want 3", srv.inits())
	}
	fp, _, _, _ := srv.snapshot()
	if fp != 0 {
		t.Fatalf("produce count %d want 0", fp)
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

const rebalanceCode uint16 = 9

func TestHeartbeatDefaultMaxRetriesZeroRaisesOnTimeout(t *testing.T) {
	srv := &scriptedBroker{heartbeatCodes: []uint16{timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.Heartbeat("g", "m1", 1)
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if n := srv.heartbeats(); n != 1 {
		t.Fatalf("heartbeat count %d want 1", n)
	}
}

func TestHeartbeatRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{heartbeatCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	if err := c.Heartbeat("g", "m1", 1); err != nil {
		t.Fatal(err)
	}
	if n := srv.heartbeats(); n != 2 {
		t.Fatalf("heartbeat count %d want 2", n)
	}
}

func TestHeartbeatRebalanceIsNotRetried(t *testing.T) {
	srv := &scriptedBroker{heartbeatCodes: []uint16{rebalanceCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	err = c.Heartbeat("g", "m1", 1)
	if err == nil {
		t.Fatal("expected BrokerError 9")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != rebalanceCode {
		t.Fatalf("got %v want BrokerError code=9", err)
	}
	if n := srv.heartbeats(); n != 1 {
		t.Fatalf("heartbeat count %d want 1", n)
	}
}

func TestHeartbeatExhaustedRetriesRaises(t *testing.T) {
	srv := &scriptedBroker{heartbeatCodes: []uint16{timeoutCode, timeoutCode, timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	err = c.Heartbeat("g", "m1", 1)
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if n := srv.heartbeats(); n != 3 {
		t.Fatalf("heartbeat count %d want 3", n)
	}
}

const unknownMemberCode uint16 = 10

func TestLeaveGroupDefaultMaxRetriesZeroRaisesOnTimeout(t *testing.T) {
	srv := &scriptedBroker{leaveGroupCodes: []uint16{timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.LeaveGroup("g", "m1")
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if n := srv.leaveGroups(); n != 1 {
		t.Fatalf("leave group count %d want 1", n)
	}
}

func TestLeaveGroupRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{leaveGroupCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	if err := c.LeaveGroup("g", "m1"); err != nil {
		t.Fatal(err)
	}
	if n := srv.leaveGroups(); n != 2 {
		t.Fatalf("leave group count %d want 2", n)
	}
}

func TestLeaveGroupUnknownMemberIsSuccess(t *testing.T) {
	srv := &scriptedBroker{leaveGroupCodes: []uint16{unknownMemberCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.LeaveGroup("g", "m1"); err != nil {
		t.Fatal(err)
	}
	if n := srv.leaveGroups(); n != 1 {
		t.Fatalf("leave group count %d want 1", n)
	}
}

func TestLeaveGroupRetriesTimeoutThenUnknownMember(t *testing.T) {
	srv := &scriptedBroker{leaveGroupCodes: []uint16{timeoutCode, unknownMemberCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	if err := c.LeaveGroup("g", "m1"); err != nil {
		t.Fatal(err)
	}
	if n := srv.leaveGroups(); n != 2 {
		t.Fatalf("leave group count %d want 2", n)
	}
}

func TestLeaveGroupRebalanceIsNotRetried(t *testing.T) {
	srv := &scriptedBroker{leaveGroupCodes: []uint16{rebalanceCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	err = c.LeaveGroup("g", "m1")
	if err == nil {
		t.Fatal("expected BrokerError 9")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != rebalanceCode {
		t.Fatalf("got %v want BrokerError code=9", err)
	}
	if n := srv.leaveGroups(); n != 1 {
		t.Fatalf("leave group count %d want 1", n)
	}
}

const notFoundCode uint16 = 2

func TestOffsetCommitDefaultMaxRetriesZeroRaisesOnTimeout(t *testing.T) {
	srv := &scriptedBroker{offsetCommitCodes: []uint16{timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.OffsetCommit("g", "t", 0, 5)
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if n := srv.offsetCommits(); n != 1 {
		t.Fatalf("offset commit count %d want 1", n)
	}
}

func TestOffsetCommitRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{offsetCommitCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	if err := c.OffsetCommit("g", "t", 0, 5); err != nil {
		t.Fatal(err)
	}
	if n := srv.offsetCommits(); n != 2 {
		t.Fatalf("offset commit count %d want 2", n)
	}
}

func TestOffsetFetchRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{offsetFetchCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	offs, err := c.OffsetFetch("g", "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(offs) != 0 {
		t.Fatalf("offsets %v want empty", offs)
	}
	if n := srv.offsetFetches(); n != 2 {
		t.Fatalf("offset fetch count %d want 2", n)
	}
}

func TestOffsetFetchAllTwoTopics(t *testing.T) {
	srv := &scriptedBroker{offsetFetchEntries: []codec.OffsetFetchEntry{
		{Topic: "t", Partition: 0, Offset: 5},
		{Topic: "u", Partition: 1, Offset: 9},
	}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	offs, err := c.OffsetFetchAll("g")
	if err != nil {
		t.Fatal(err)
	}
	want := []volant.OffsetFetchEntry{
		{Topic: "t", Partition: 0, Offset: 5},
		{Topic: "u", Partition: 1, Offset: 9},
	}
	if len(offs) != len(want) || offs[0] != want[0] || offs[1] != want[1] {
		t.Fatalf("offsets %v want %v", offs, want)
	}
	if n := srv.offsetFetches(); n != 1 {
		t.Fatalf("offset fetch count %d want 1", n)
	}
}

func TestOffsetFetchStillFiltersTopic(t *testing.T) {
	srv := &scriptedBroker{offsetFetchEntries: []codec.OffsetFetchEntry{
		{Topic: "t", Partition: 0, Offset: 5},
		{Topic: "u", Partition: 1, Offset: 9},
	}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	offs, err := c.OffsetFetch("g", "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(offs) != 1 || offs[0] != (volant.Offset{Partition: 0, Offset: 5}) {
		t.Fatalf("offsets %v want [{0 5}]", offs)
	}
	if n := srv.offsetFetches(); n != 1 {
		t.Fatalf("offset fetch count %d want 1", n)
	}
}

func TestDeleteOffsetsRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{deleteOffsetsCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	n, err := c.DeleteOffsets("g", nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("deleted %d want 0", n)
	}
	if got := srv.deleteOffsets(); got != 2 {
		t.Fatalf("delete offsets count %d want 2", got)
	}
}

func TestOffsetCommitNotFoundIsNotRetried(t *testing.T) {
	srv := &scriptedBroker{offsetCommitCodes: []uint16{notFoundCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	err = c.OffsetCommit("g", "t", 0, 5)
	if err == nil {
		t.Fatal("expected BrokerError 2")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != notFoundCode {
		t.Fatalf("got %v want BrokerError code=2", err)
	}
	if n := srv.offsetCommits(); n != 1 {
		t.Fatalf("offset commit count %d want 1", n)
	}
}

func TestOffsetCommitExhaustedRetriesRaises(t *testing.T) {
	srv := &scriptedBroker{offsetCommitCodes: []uint16{timeoutCode, timeoutCode, timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	err = c.OffsetCommit("g", "t", 0, 5)
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if n := srv.offsetCommits(); n != 3 {
		t.Fatalf("offset commit count %d want 3", n)
	}
}

func TestCommitOffsetsBatchOfTwoOnTheWire(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.CommitOffsets("g", "", 0, []codec.OffsetCommitEntry{
		{Topic: "t", Partition: 0, Offset: 5, Metadata: "m0"},
		{Topic: "u", Partition: 1, Offset: 9, Metadata: "m1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := srv.offsetCommits(); n != 1 {
		t.Fatalf("offset commit count %d want 1", n)
	}
	reqs := srv.copyOffsetCommits()
	if len(reqs) != 1 {
		t.Fatalf("decoded %d requests want 1", len(reqs))
	}
	req := reqs[0]
	if req.GroupID != "g" || req.MemberID != "" || req.Generation != 0 {
		t.Fatalf("header %+v", req)
	}
	if len(req.Entries) != 2 {
		t.Fatalf("entries %v want 2", req.Entries)
	}
	if req.Entries[0] != (codec.OffsetCommitEntry{Topic: "t", Partition: 0, Offset: 5, Metadata: "m0"}) {
		t.Fatalf("entry0 %+v", req.Entries[0])
	}
	if req.Entries[1] != (codec.OffsetCommitEntry{Topic: "u", Partition: 1, Offset: 9, Metadata: "m1"}) {
		t.Fatalf("entry1 %+v", req.Entries[1])
	}
}

func TestOffsetCommitOneEntryStillWorks(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.OffsetCommit("g", "t", 0, 5); err != nil {
		t.Fatal(err)
	}
	reqs := srv.copyOffsetCommits()
	if len(reqs) != 1 {
		t.Fatalf("decoded %d requests want 1", len(reqs))
	}
	req := reqs[0]
	if req.GroupID != "g" || req.MemberID != "" || req.Generation != 0 {
		t.Fatalf("header %+v", req)
	}
	if len(req.Entries) != 1 || req.Entries[0] != (codec.OffsetCommitEntry{Topic: "t", Partition: 0, Offset: 5, Metadata: ""}) {
		t.Fatalf("entries %+v", req.Entries)
	}
}

func TestCommitOffsetsSendsMemberIDAndGeneration(t *testing.T) {
	srv := &scriptedBroker{}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.CommitOffsets("g", "m1", 3, []codec.OffsetCommitEntry{
		{Topic: "t", Partition: 0, Offset: 5},
	}); err != nil {
		t.Fatal(err)
	}
	reqs := srv.copyOffsetCommits()
	if len(reqs) != 1 {
		t.Fatalf("decoded %d requests want 1", len(reqs))
	}
	req := reqs[0]
	if req.MemberID != "m1" || req.Generation != 3 {
		t.Fatalf("member/gen %+v", req)
	}
	if len(req.Entries) != 1 || req.Entries[0].Topic != "t" || req.Entries[0].Offset != 5 {
		t.Fatalf("entries %+v", req.Entries)
	}
}

func TestListOffsetsDefaultMaxRetriesZeroRaisesOnTimeout(t *testing.T) {
	srv := &scriptedBroker{listOffsetsCodes: []uint16{timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.ListOffsets("t", nil)
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if n := srv.listOffsets(); n != 1 {
		t.Fatalf("list offsets count %d want 1", n)
	}
}

func TestListOffsetsRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{listOffsetsCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	out, err := c.ListOffsets("t", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("listings %v want empty", out)
	}
	if n := srv.listOffsets(); n != 2 {
		t.Fatalf("list offsets count %d want 2", n)
	}
	if n := srv.metadatas(); n != 0 {
		t.Fatalf("metadata count %d want 0", n)
	}
}

func TestListOffsetsNotFoundIsNotRetried(t *testing.T) {
	srv := &scriptedBroker{listOffsetsCodes: []uint16{notFoundCode, 0}}
	addr, stop := startScripted(t, srv)
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
		t.Fatal("expected BrokerError 2")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != notFoundCode {
		t.Fatalf("got %v want BrokerError code=2", err)
	}
	if n := srv.listOffsets(); n != 1 {
		t.Fatalf("list offsets count %d want 1", n)
	}
	if n := srv.metadatas(); n != 0 {
		t.Fatalf("metadata count %d want 0", n)
	}
}

func TestListOffsetsExhaustedRetriesRaises(t *testing.T) {
	srv := &scriptedBroker{listOffsetsCodes: []uint16{timeoutCode, timeoutCode, timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	_, err = c.ListOffsets("t", nil)
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if n := srv.listOffsets(); n != 3 {
		t.Fatalf("list offsets count %d want 3", n)
	}
}

func TestDescribeGroupDefaultMaxRetriesZeroRaisesOnTimeout(t *testing.T) {
	srv := &scriptedBroker{describeGroupCodes: []uint16{timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.DescribeGroup("g")
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if n := srv.describeGroups(); n != 1 {
		t.Fatalf("describe group count %d want 1", n)
	}
}

func TestDescribeGroupRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{describeGroupCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	got, err := c.DescribeGroup("g")
	if err != nil {
		t.Fatal(err)
	}
	if got.GroupID != "" || len(got.Members) != 0 {
		t.Fatalf("describe %v want empty", got)
	}
	if n := srv.describeGroups(); n != 2 {
		t.Fatalf("describe group count %d want 2", n)
	}
}

func TestDescribeGroupNotFoundIsNotRetried(t *testing.T) {
	srv := &scriptedBroker{describeGroupCodes: []uint16{notFoundCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	_, err = c.DescribeGroup("missing")
	if err == nil {
		t.Fatal("expected BrokerError 2")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != notFoundCode {
		t.Fatalf("got %v want BrokerError code=2", err)
	}
	if n := srv.describeGroups(); n != 1 {
		t.Fatalf("describe group count %d want 1", n)
	}
}

func TestListGroupsRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{listGroupsCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	out, err := c.ListGroups()
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("listings %v want empty", out)
	}
	if n := srv.listGroups(); n != 2 {
		t.Fatalf("list groups count %d want 2", n)
	}
}

func TestDescribeGroupExhaustedRetriesRaises(t *testing.T) {
	srv := &scriptedBroker{describeGroupCodes: []uint16{timeoutCode, timeoutCode, timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	_, err = c.DescribeGroup("g")
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if n := srv.describeGroups(); n != 3 {
		t.Fatalf("describe group count %d want 3", n)
	}
}

func TestMetadataDefaultMaxRetriesZeroRaisesOnTimeout(t *testing.T) {
	srv := &scriptedBroker{metadataCodes: []uint16{timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Metadata()
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if n := srv.metadatas(); n != 1 {
		t.Fatalf("metadata count %d want 1", n)
	}
}

func TestMetadataRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{metadataCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
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
	if n := srv.metadatas(); n != 2 {
		t.Fatalf("metadata count %d want 2", n)
	}
}

func TestMetadataNotFoundIsNotRetried(t *testing.T) {
	srv := &scriptedBroker{metadataCodes: []uint16{notFoundCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	_, err = c.Metadata()
	if err == nil {
		t.Fatal("expected BrokerError 2")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != notFoundCode {
		t.Fatalf("got %v want BrokerError code=2", err)
	}
	if n := srv.metadatas(); n != 1 {
		t.Fatalf("metadata count %d want 1", n)
	}
}

func TestListMembersRetriesTimeoutThenOk(t *testing.T) {
	srv := &scriptedBroker{listMembersCodes: []uint16{timeoutCode, 0}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	got, err := c.ListMembers()
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 0 || len(got.Brokers) != 0 || len(got.Live) != 0 {
		t.Fatalf("list members %v want empty", got)
	}
	if n := srv.listMembers(); n != 2 {
		t.Fatalf("list members count %d want 2", n)
	}
	if n := srv.metadatas(); n != 0 {
		t.Fatalf("metadata count %d want 0", n)
	}
}

func TestListMembersError14RedirectsViaControllerID(t *testing.T) {
	leader := &scriptedBroker{}
	_, stopL := startScripted(t, leader)
	defer stopL()
	follower := &scriptedBroker{
		listMembersReplies: []createTopicReply{{
			code: notController, message: "not controller; controller_id=2", asError: true,
		}},
		meta: controllerMeta(2, "127.0.0.1", leader.port()),
	}
	fAddr, stopF := startScripted(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(fAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, err := c.ListMembers()
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 0 || len(got.Brokers) != 0 || len(got.Live) != 0 {
		t.Fatalf("list members %v want empty", got)
	}
	if n := follower.listMembers(); n != 1 {
		t.Fatalf("follower list members %d want 1", n)
	}
	if n := follower.metadatas(); n != 1 {
		t.Fatalf("follower metadata %d want 1", n)
	}
	if n := leader.listMembers(); n != 1 {
		t.Fatalf("leader list members %d want 1", n)
	}
	if n := leader.metadatas(); n != 0 {
		t.Fatalf("leader metadata %d want 0", n)
	}
}

func TestListMembersTyped14NoHintThenOk(t *testing.T) {
	leader := &scriptedBroker{}
	_, stopL := startScripted(t, leader)
	defer stopL()
	follower := &scriptedBroker{
		listMembersCodes: []uint16{notController},
	}
	fAddr, stopF := startScripted(t, follower)
	defer stopF()
	follower.mu.Lock()
	follower.meta = otherBrokerMeta(follower.port(), "127.0.0.1", leader.port())
	follower.mu.Unlock()

	c, err := volant.DialTimeout(fAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, err := c.ListMembers()
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 0 {
		t.Fatalf("generation %d want 0", got.Generation)
	}
	if n := follower.listMembers(); n != 1 {
		t.Fatalf("follower list members %d want 1", n)
	}
	if n := follower.metadatas(); n != 1 {
		t.Fatalf("follower metadata %d want 1", n)
	}
	if n := leader.listMembers(); n != 1 {
		t.Fatalf("leader list members %d want 1", n)
	}
}

func TestListMembersMaxRedirectsZeroRaisesOnFirst14(t *testing.T) {
	follower := &scriptedBroker{
		listMembersReplies: []createTopicReply{{
			code: notController, message: "not controller; controller_id=2", asError: true,
		}},
		meta: controllerMeta(2, "127.0.0.1", 9),
	}
	fAddr, stopF := startScripted(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(fAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRedirects(0)
	_, err = c.ListMembers()
	if brokerCode(err) != notController {
		t.Fatalf("err=%v want code 14", err)
	}
	if n := follower.listMembers(); n != 1 {
		t.Fatalf("list members %d want 1", n)
	}
	if n := follower.metadatas(); n != 0 {
		t.Fatalf("metadata %d want 0", n)
	}
}

func TestMetadataExhaustedRetriesRaises(t *testing.T) {
	srv := &scriptedBroker{metadataCodes: []uint16{timeoutCode, timeoutCode, timeoutCode}}
	addr, stop := startScripted(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)

	_, err = c.Metadata()
	if err == nil {
		t.Fatal("expected BrokerError 7")
	}
	be, ok := err.(*volant.BrokerError)
	if !ok || be.Code != timeoutCode {
		t.Fatalf("got %v want BrokerError code=7", err)
	}
	if n := srv.metadatas(); n != 3 {
		t.Fatalf("metadata count %d want 3", n)
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
	topicID uint32
}

type adminBroker struct {
	mu                    sync.Mutex
	createTopicReplies    []createTopicReply
	createPartitionsCodes []uint16
	createAclsCodes       []uint16
	reassignCodes         []uint16
	createScramReplies    []createTopicReply
	deleteScramCodes      []uint16
	listScramCodes        []uint16
	listAclsCodes         []uint16
	addBrokerReplies       []createTopicReply
	removeBrokerCodes      []uint16
	describeConfigsReplies []createTopicReply
	alterConfigsCodes      []uint16
	deleteOffsetsReplies   []createTopicReply
	deleteOffsetsCodes     []uint16
	offsetCommitReplies    []createTopicReply
	offsetCommitCodes      []uint16
	offsetFetchReplies     []createTopicReply
	offsetFetchCodes       []uint16
	meta                   codec.MetadataResponse
	createTopicCount       int
	createPartitionsCount  int
	createAclsCount        int
	reassignCount          int
	createScramCount       int
	deleteScramCount       int
	listScramCount         int
	listAclsCount          int
	addBrokerCount         int
	removeBrokerCount      int
	describeConfigsCount   int
	alterConfigsCount      int
	deleteOffsetsCount     int
	offsetCommitCount      int
	offsetFetchCount       int
	metadataCount          int
	listMembersCount       int
	acceptCount            int
	lastCreateTopicConfigs [][2]string
	ln                     net.Listener
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

func (s *adminBroker) lastCreateConfigs() [][2]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][2]string, len(s.lastCreateTopicConfigs))
	copy(out, s.lastCreateTopicConfigs)
	return out
}

func (s *adminBroker) scramAclSnapshot() (createScram, deleteScram, listScram, listAcls, metas, accepts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createScramCount, s.deleteScramCount, s.listScramCount, s.listAclsCount, s.metadataCount, s.acceptCount
}

func (s *adminBroker) membershipSnapshot() (addBroker, removeBroker, metas, accepts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addBrokerCount, s.removeBrokerCount, s.metadataCount, s.acceptCount
}

func (s *adminBroker) configsSnapshot() (describe, alter, metas, accepts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.describeConfigsCount, s.alterConfigsCount, s.metadataCount, s.acceptCount
}

func (s *adminBroker) deleteOffsetsSnapshot() (n, metas, accepts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteOffsetsCount, s.metadataCount, s.acceptCount
}

func (s *adminBroker) offsetCommitSnapshot() (n, metas, accepts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsetCommitCount, s.metadataCount, s.acceptCount
}

func (s *adminBroker) offsetFetchSnapshot() (n, metas, accepts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsetFetchCount, s.metadataCount, s.acceptCount
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
		s.lastCreateTopicConfigs = append([][2]string(nil), req.Configs...)
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
			id = rep.topicID
			if id == 0 {
				id = 1
			}
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
	case codec.OpCreateScramUser:
		s.createScramCount++
		rep := createTopicReply{}
		if len(s.createScramReplies) > 0 {
			rep = s.createScramReplies[0]
			s.createScramReplies = s.createScramReplies[1:]
		}
		if rep.asError {
			payload, err = codec.EncodeErrorResponse(codec.ErrorResponse{Code: rep.code, Message: rep.message})
			replyOp = codec.OpError
			break
		}
		payload, err = codec.EncodeCreateScramUserResponse(codec.CreateScramUserResponse{ErrorCode: rep.code})
		replyOp = codec.OpCreateScramUserResponse
	case codec.OpDeleteScramUser:
		s.deleteScramCount++
		code := uint16(0)
		if len(s.deleteScramCodes) > 0 {
			code = s.deleteScramCodes[0]
			s.deleteScramCodes = s.deleteScramCodes[1:]
		}
		payload, err = codec.EncodeDeleteScramUserResponse(codec.DeleteScramUserResponse{ErrorCode: code})
		replyOp = codec.OpDeleteScramUserResponse
	case codec.OpListScramUsers:
		s.listScramCount++
		code := uint16(0)
		if len(s.listScramCodes) > 0 {
			code = s.listScramCodes[0]
			s.listScramCodes = s.listScramCodes[1:]
		}
		names := []string(nil)
		if code == 0 {
			names = []string{"alice"}
		}
		payload, err = codec.EncodeListScramUsersResponse(codec.ListScramUsersResponse{
			ErrorCode: code, Usernames: names,
		})
		replyOp = codec.OpListScramUsersResponse
	case codec.OpListAcls:
		s.listAclsCount++
		code := uint16(0)
		if len(s.listAclsCodes) > 0 {
			code = s.listAclsCodes[0]
			s.listAclsCodes = s.listAclsCodes[1:]
		}
		payload, err = codec.EncodeListAclsResponse(codec.ListAclsResponse{ErrorCode: code})
		replyOp = codec.OpListAclsResponse
	case codec.OpAddBroker:
		s.addBrokerCount++
		rep := createTopicReply{}
		if len(s.addBrokerReplies) > 0 {
			rep = s.addBrokerReplies[0]
			s.addBrokerReplies = s.addBrokerReplies[1:]
		}
		if rep.asError {
			payload, err = codec.EncodeErrorResponse(codec.ErrorResponse{Code: rep.code, Message: rep.message})
			replyOp = codec.OpError
			break
		}
		gen := uint64(0)
		if rep.code == 0 {
			gen = 11
		}
		payload, err = codec.EncodeAddBrokerResponse(codec.AddBrokerResponse{ErrorCode: rep.code, Generation: gen})
		replyOp = codec.OpAddBrokerResponse
	case codec.OpRemoveBroker:
		s.removeBrokerCount++
		code := uint16(0)
		if len(s.removeBrokerCodes) > 0 {
			code = s.removeBrokerCodes[0]
			s.removeBrokerCodes = s.removeBrokerCodes[1:]
		}
		gen := uint64(0)
		if code == 0 {
			gen = 12
		}
		payload, err = codec.EncodeRemoveBrokerResponse(codec.RemoveBrokerResponse{ErrorCode: code, Generation: gen})
		replyOp = codec.OpRemoveBrokerResponse
	case codec.OpDescribeConfigs:
		s.describeConfigsCount++
		req, e := codec.DecodeDescribeConfigsRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		rep := createTopicReply{}
		if len(s.describeConfigsReplies) > 0 {
			rep = s.describeConfigsReplies[0]
			s.describeConfigsReplies = s.describeConfigsReplies[1:]
		}
		if rep.asError {
			payload, err = codec.EncodeErrorResponse(codec.ErrorResponse{Code: rep.code, Message: rep.message})
			replyOp = codec.OpError
			break
		}
		cfgs := [][2]string(nil)
		tid := uint32(0)
		parts := uint32(0)
		if rep.code == 0 {
			cfgs = [][2]string{{"retention.ms", "86400000"}}
			tid = 1
			parts = 1
		}
		payload, err = codec.EncodeDescribeConfigsResponse(codec.DescribeConfigsResponse{
			ErrorCode:      rep.code,
			Topic:          req.Topic,
			TopicID:        tid,
			PartitionCount: parts,
			Configs:        cfgs,
		})
		replyOp = codec.OpDescribeConfigsResponse
	case codec.OpAlterConfigs:
		s.alterConfigsCount++
		req, e := codec.DecodeAlterConfigsRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		code := uint16(0)
		if len(s.alterConfigsCodes) > 0 {
			code = s.alterConfigsCodes[0]
			s.alterConfigsCodes = s.alterConfigsCodes[1:]
		}
		payload, err = codec.EncodeAlterConfigsResponse(codec.AlterConfigsResponse{
			ErrorCode: code, Topic: req.Topic,
		})
		replyOp = codec.OpAlterConfigsResponse
	case codec.OpDeleteOffsets:
		s.deleteOffsetsCount++
		rep := createTopicReply{}
		if len(s.deleteOffsetsReplies) > 0 {
			rep = s.deleteOffsetsReplies[0]
			s.deleteOffsetsReplies = s.deleteOffsetsReplies[1:]
		} else if len(s.deleteOffsetsCodes) > 0 {
			rep.code = s.deleteOffsetsCodes[0]
			s.deleteOffsetsCodes = s.deleteOffsetsCodes[1:]
		}
		if rep.asError {
			payload, err = codec.EncodeErrorResponse(codec.ErrorResponse{Code: rep.code, Message: rep.message})
			replyOp = codec.OpError
			break
		}
		deleted := uint32(0)
		if rep.code == 0 {
			deleted = 3
		}
		payload, err = codec.EncodeDeleteOffsetsResponse(codec.DeleteOffsetsResponse{
			ErrorCode: rep.code, DeletedCount: deleted,
		})
		replyOp = codec.OpDeleteOffsetsResponse
	case codec.OpOffsetCommit:
		s.offsetCommitCount++
		rep := createTopicReply{}
		if len(s.offsetCommitReplies) > 0 {
			rep = s.offsetCommitReplies[0]
			s.offsetCommitReplies = s.offsetCommitReplies[1:]
		} else if len(s.offsetCommitCodes) > 0 {
			rep.code = s.offsetCommitCodes[0]
			s.offsetCommitCodes = s.offsetCommitCodes[1:]
		}
		if rep.asError {
			payload, err = codec.EncodeErrorResponse(codec.ErrorResponse{Code: rep.code, Message: rep.message})
			replyOp = codec.OpError
			break
		}
		payload, err = codec.EncodeOffsetCommitResponse(codec.OffsetCommitResponse{ErrorCode: rep.code})
	case codec.OpOffsetFetch:
		s.offsetFetchCount++
		rep := createTopicReply{}
		if len(s.offsetFetchReplies) > 0 {
			rep = s.offsetFetchReplies[0]
			s.offsetFetchReplies = s.offsetFetchReplies[1:]
		} else if len(s.offsetFetchCodes) > 0 {
			rep.code = s.offsetFetchCodes[0]
			s.offsetFetchCodes = s.offsetFetchCodes[1:]
		}
		if rep.asError {
			payload, err = codec.EncodeErrorResponse(codec.ErrorResponse{Code: rep.code, Message: rep.message})
			replyOp = codec.OpError
			break
		}
		payload, err = codec.EncodeOffsetFetchResponse(codec.OffsetFetchResponse{ErrorCode: rep.code})
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

func TestCreatePartitionsPrefersMetadataControllerID(t *testing.T) {
	controller := &adminBroker{}
	_, stopC := startAdmin(t, controller)
	defer stopC()
	decoy := &adminBroker{}
	_, stopD := startAdmin(t, decoy)
	defer stopD()
	follower := &adminBroker{
		createPartitionsCodes: []uint16{notController},
	}
	fAddr, stopF := startAdmin(t, follower)
	defer stopF()
	follower.mu.Lock()
	follower.meta = codec.MetadataResponse{
		Brokers: []codec.BrokerInfo{
			{NodeID: 1, Host: "127.0.0.1", Port: uint16(follower.port())},
			{NodeID: 3, Host: "127.0.0.1", Port: uint16(decoy.port())},
			{NodeID: 2, Host: "127.0.0.1", Port: uint16(controller.port())},
		},
		ControllerID: 2,
	}
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
	_, ccp, _, _, _, _ := controller.snapshot()
	if ccp != 1 {
		t.Fatalf("controller create_partitions=%d want 1", ccp)
	}
	_, dcp, _, _, _, _ := decoy.snapshot()
	if dcp != 0 {
		t.Fatalf("decoy create_partitions=%d want 0", dcp)
	}
}

func TestCreatePartitionsMetadataControllerIDZeroPicksOther(t *testing.T) {
	later := &adminBroker{}
	_, stopL := startAdmin(t, later)
	defer stopL()
	firstOther := &adminBroker{}
	_, stopO := startAdmin(t, firstOther)
	defer stopO()
	follower := &adminBroker{
		createPartitionsCodes: []uint16{notController},
	}
	fAddr, stopF := startAdmin(t, follower)
	defer stopF()
	follower.mu.Lock()
	follower.meta = codec.MetadataResponse{
		Brokers: []codec.BrokerInfo{
			{NodeID: 1, Host: "127.0.0.1", Port: uint16(follower.port())},
			{NodeID: 3, Host: "127.0.0.1", Port: uint16(firstOther.port())},
			{NodeID: 2, Host: "127.0.0.1", Port: uint16(later.port())},
		},
		ControllerID: 0,
	}
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
	_, ocp, _, _, _, _ := firstOther.snapshot()
	if ocp != 1 {
		t.Fatalf("first-other create_partitions=%d want 1", ocp)
	}
	_, lcp, _, _, _, _ := later.snapshot()
	if lcp != 0 {
		t.Fatalf("later create_partitions=%d want 0", lcp)
	}
}

func TestCreateTopicMessageControllerIDWinsOverMetadata(t *testing.T) {
	hinted := &adminBroker{}
	_, stopH := startAdmin(t, hinted)
	defer stopH()
	metaCtrl := &adminBroker{}
	_, stopM := startAdmin(t, metaCtrl)
	defer stopM()
	follower := &adminBroker{
		createTopicReplies: []createTopicReply{{
			code: notController, message: "not controller; controller_id=3", asError: true,
		}},
	}
	fAddr, stopF := startAdmin(t, follower)
	defer stopF()
	follower.mu.Lock()
	follower.meta = codec.MetadataResponse{
		Brokers: []codec.BrokerInfo{
			{NodeID: 1, Host: "127.0.0.1", Port: uint16(follower.port())},
			{NodeID: 2, Host: "127.0.0.1", Port: uint16(metaCtrl.port())},
			{NodeID: 3, Host: "127.0.0.1", Port: uint16(hinted.port())},
		},
		ControllerID: 2,
	}
	follower.mu.Unlock()

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
	hct, _, _, _, _, _ := hinted.snapshot()
	if hct != 1 {
		t.Fatalf("hinted create_topic=%d want 1", hct)
	}
	mct, _, _, _, _, _ := metaCtrl.snapshot()
	if mct != 0 {
		t.Fatalf("metadata-controller create_topic=%d want 0", mct)
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

func TestCreateScramUserError14RedirectsViaControllerID(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		createScramReplies: []createTopicReply{{
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
	if err := c.CreateScramUser("alice", "s3cret", 0); err != nil {
		t.Fatal(err)
	}
	cs, _, _, _, metas, _ := follower.scramAclSnapshot()
	if cs != 1 || metas != 1 {
		t.Fatalf("follower create_scram=%d metadata=%d want 1,1", cs, metas)
	}
	lcs, _, _, _, _, _ := leader.scramAclSnapshot()
	if lcs != 1 {
		t.Fatalf("leader create_scram=%d want 1", lcs)
	}
}

func TestListAclsTyped14NoHintThenOk(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		listAclsCodes: []uint16{notController},
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
	got, err := c.ListAcls("", 255, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("list_acls entries %d want 0", len(got))
	}
	_, _, _, la, metas, _ := follower.scramAclSnapshot()
	if la != 1 || metas != 1 {
		t.Fatalf("follower list_acls=%d metadata=%d want 1,1", la, metas)
	}
	_, _, _, lla, _, _ := leader.scramAclSnapshot()
	if lla != 1 {
		t.Fatalf("leader list_acls=%d want 1", lla)
	}
}

func TestDeleteScramUserMaxRedirectsZeroRaisesOnFirst14(t *testing.T) {
	follower := &adminBroker{
		deleteScramCodes: []uint16{notController},
		meta:             controllerMeta(2, "127.0.0.1", 9),
	}
	fAddr, stopF := startAdmin(t, follower)
	defer stopF()

	c, err := volant.DialTimeout(fAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRedirects(0)
	err = c.DeleteScramUser("alice")
	if brokerCode(err) != notController {
		t.Fatalf("err=%v want code 14", err)
	}
	_, ds, _, _, metas, accepts := follower.scramAclSnapshot()
	if ds != 1 || metas != 0 || accepts != 1 {
		t.Fatalf("delete_scram=%d metadata=%d accepts=%d want 1,0,1", ds, metas, accepts)
	}
}

func TestListScramUsersError14ThenOk(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		listScramCodes: []uint16{notController},
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
	names, err := c.ListScramUsers()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "alice" {
		t.Fatalf("usernames %v want [alice]", names)
	}
	_, _, ls, _, metas, _ := follower.scramAclSnapshot()
	if ls != 1 || metas != 1 {
		t.Fatalf("follower list_scram=%d metadata=%d want 1,1", ls, metas)
	}
	_, _, lls, _, _, _ := leader.scramAclSnapshot()
	if lls != 1 {
		t.Fatalf("leader list_scram=%d want 1", lls)
	}
}

func TestAddBrokerError14RedirectsViaControllerID(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		addBrokerReplies: []createTopicReply{{
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
	gen, err := c.AddBroker(3, "10.0.0.3", 9092, nil)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 11 {
		t.Fatalf("generation %d want 11", gen)
	}
	ab, _, metas, _ := follower.membershipSnapshot()
	if ab != 1 || metas != 1 {
		t.Fatalf("follower add_broker=%d metadata=%d want 1,1", ab, metas)
	}
	lab, _, _, _ := leader.membershipSnapshot()
	if lab != 1 {
		t.Fatalf("leader add_broker=%d want 1", lab)
	}
}

func TestRemoveBrokerTyped14NoHintThenOk(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		removeBrokerCodes: []uint16{notController},
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
	gen, err := c.RemoveBroker(3)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 12 {
		t.Fatalf("generation %d want 12", gen)
	}
	_, rb, metas, _ := follower.membershipSnapshot()
	if rb != 1 || metas != 1 {
		t.Fatalf("follower remove_broker=%d metadata=%d want 1,1", rb, metas)
	}
	_, lrb, _, _ := leader.membershipSnapshot()
	if lrb != 1 {
		t.Fatalf("leader remove_broker=%d want 1", lrb)
	}
}

func TestAddBrokerMaxRedirectsZeroRaisesOnFirst14(t *testing.T) {
	follower := &adminBroker{
		addBrokerReplies: []createTopicReply{{
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
	_, err = c.AddBroker(3, "10.0.0.3", 9092, nil)
	if brokerCode(err) != notController {
		t.Fatalf("err=%v want code 14", err)
	}
	ab, _, metas, accepts := follower.membershipSnapshot()
	if ab != 1 || metas != 0 || accepts != 1 {
		t.Fatalf("add_broker=%d metadata=%d accepts=%d want 1,0,1", ab, metas, accepts)
	}
}

func TestDescribeConfigsError14RedirectsViaControllerID(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		describeConfigsReplies: []createTopicReply{{
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
	got, err := c.DescribeConfigs("events")
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != "events" || got.TopicID != 1 || got.PartitionCount != 1 {
		t.Fatalf("got %+v want topic=events id=1 partitions=1", got)
	}
	if len(got.Configs) != 1 || got.Configs[0] != [2]string{"retention.ms", "86400000"} {
		t.Fatalf("configs %+v", got.Configs)
	}
	dc, _, metas, _ := follower.configsSnapshot()
	if dc != 1 || metas != 1 {
		t.Fatalf("follower describe_configs=%d metadata=%d want 1,1", dc, metas)
	}
	ldc, _, _, _ := leader.configsSnapshot()
	if ldc != 1 {
		t.Fatalf("leader describe_configs=%d want 1", ldc)
	}
}

func TestAlterConfigsTyped14NoHintThenOk(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		alterConfigsCodes: []uint16{notController},
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
	if err := c.AlterConfigs("events", [][2]string{{"retention.ms", "86400000"}}); err != nil {
		t.Fatal(err)
	}
	_, ac, metas, _ := follower.configsSnapshot()
	if ac != 1 || metas != 1 {
		t.Fatalf("follower alter_configs=%d metadata=%d want 1,1", ac, metas)
	}
	_, lac, _, _ := leader.configsSnapshot()
	if lac != 1 {
		t.Fatalf("leader alter_configs=%d want 1", lac)
	}
}

func TestDescribeConfigsMaxRedirectsZeroRaisesOnFirst14(t *testing.T) {
	follower := &adminBroker{
		describeConfigsReplies: []createTopicReply{{
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
	_, err = c.DescribeConfigs("events")
	if brokerCode(err) != notController {
		t.Fatalf("err=%v want code 14", err)
	}
	dc, _, metas, accepts := follower.configsSnapshot()
	if dc != 1 || metas != 0 || accepts != 1 {
		t.Fatalf("describe_configs=%d metadata=%d accepts=%d want 1,0,1", dc, metas, accepts)
	}
}

func TestDeleteOffsetsError14RedirectsViaControllerID(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		deleteOffsetsReplies: []createTopicReply{{
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
	got, err := c.DeleteOffsets("g", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("deleted_count %d want 3", got)
	}
	n, metas, _ := follower.deleteOffsetsSnapshot()
	if n != 1 || metas != 1 {
		t.Fatalf("follower delete_offsets=%d metadata=%d want 1,1", n, metas)
	}
	ln, _, _ := leader.deleteOffsetsSnapshot()
	if ln != 1 {
		t.Fatalf("leader delete_offsets=%d want 1", ln)
	}
}

func TestDeleteOffsetsTyped14NoHintThenOk(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		deleteOffsetsCodes: []uint16{notController},
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
	got, err := c.DeleteOffsets("g", []codec.OffsetEntry{{Topic: "events", Partition: 0}})
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("deleted_count %d want 3", got)
	}
	n, metas, _ := follower.deleteOffsetsSnapshot()
	if n != 1 || metas != 1 {
		t.Fatalf("follower delete_offsets=%d metadata=%d want 1,1", n, metas)
	}
	ln, _, _ := leader.deleteOffsetsSnapshot()
	if ln != 1 {
		t.Fatalf("leader delete_offsets=%d want 1", ln)
	}
}

func TestDeleteOffsetsMaxRedirectsZeroRaisesOnFirst14(t *testing.T) {
	follower := &adminBroker{
		deleteOffsetsReplies: []createTopicReply{{
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
	_, err = c.DeleteOffsets("g", nil)
	if brokerCode(err) != notController {
		t.Fatalf("err=%v want code 14", err)
	}
	n, metas, accepts := follower.deleteOffsetsSnapshot()
	if n != 1 || metas != 0 || accepts != 1 {
		t.Fatalf("delete_offsets=%d metadata=%d accepts=%d want 1,0,1", n, metas, accepts)
	}
}

func TestDefaultMaxRetriesZeroRaisesOnCreateTopicTimeout(t *testing.T) {
	srv := &adminBroker{
		createTopicReplies: []createTopicReply{{code: timeoutCode}},
	}
	addr, stop := startAdmin(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.CreateTopic("events", 1)
	if brokerCode(err) != timeoutCode {
		t.Fatalf("err=%v want code 7", err)
	}
	ct, _, _, _, metas, _ := srv.snapshot()
	if ct != 1 || metas != 0 {
		t.Fatalf("create_topic=%d metadata=%d want 1,0", ct, metas)
	}
}

func TestCreateTopicRetriesTimeoutThenOk(t *testing.T) {
	srv := &adminBroker{
		createTopicReplies: []createTopicReply{{code: timeoutCode}, {code: 0}},
	}
	addr, stop := startAdmin(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	if err := c.CreateTopic("events", 1); err != nil {
		t.Fatal(err)
	}
	ct, _, _, _, metas, _ := srv.snapshot()
	if ct != 2 || metas != 0 {
		t.Fatalf("create_topic=%d metadata=%d want 2,0", ct, metas)
	}
}

func TestCreateTopic14RedirectNotCountedAsRetry(t *testing.T) {
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

func TestCreateTopicNotFoundNotRetried(t *testing.T) {
	srv := &adminBroker{
		createTopicReplies: []createTopicReply{{code: notFoundCode}},
	}
	addr, stop := startAdmin(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	err = c.CreateTopic("events", 1)
	if brokerCode(err) != notFoundCode {
		t.Fatalf("err=%v want code 2", err)
	}
	ct, _, _, _, metas, _ := srv.snapshot()
	if ct != 1 || metas != 0 {
		t.Fatalf("create_topic=%d metadata=%d want 1,0", ct, metas)
	}
}

func TestCreateTopicSendsEmptyConfigs(t *testing.T) {
	srv := &adminBroker{}
	addr, stop := startAdmin(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.CreateTopic("events", 1); err != nil {
		t.Fatal(err)
	}
	got := srv.lastCreateConfigs()
	if len(got) != 0 {
		t.Fatalf("configs=%v want empty", got)
	}
}

func TestCreateTopicWithConfigsSendsPairsAndReturnsTopicID(t *testing.T) {
	srv := &adminBroker{
		createTopicReplies: []createTopicReply{{code: 0, topicID: 42}},
	}
	addr, stop := startAdmin(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id, err := c.CreateTopicWithConfigs("events", 1, [][2]string{{"retention.ms", "1000"}})
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Fatalf("topic id %d want 42", id)
	}
	got := srv.lastCreateConfigs()
	if len(got) != 1 || got[0] != [2]string{"retention.ms", "1000"} {
		t.Fatalf("configs=%v want [(retention.ms 1000)]", got)
	}
}

func TestCreateTopicWithConfigsError14Redirects(t *testing.T) {
	leader := &adminBroker{
		createTopicReplies: []createTopicReply{{code: 0, topicID: 7}},
	}
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
	id, err := c.CreateTopicWithConfigs("events", 1, [][2]string{{"retention.ms", "1000"}})
	if err != nil {
		t.Fatal(err)
	}
	if id != 7 {
		t.Fatalf("topic id %d want 7", id)
	}
	ct, _, _, _, metas, _ := follower.snapshot()
	if ct != 1 || metas != 1 {
		t.Fatalf("follower create_topic=%d metadata=%d want 1,1", ct, metas)
	}
	lct, _, _, _, _, _ := leader.snapshot()
	if lct != 1 {
		t.Fatalf("leader create_topic=%d want 1", lct)
	}
	got := leader.lastCreateConfigs()
	if len(got) != 1 || got[0] != [2]string{"retention.ms", "1000"} {
		t.Fatalf("leader configs=%v want [(retention.ms 1000)]", got)
	}
}

func TestCreateTopicExhaustedRetriesRaises(t *testing.T) {
	srv := &adminBroker{
		createTopicReplies: []createTopicReply{
			{code: timeoutCode}, {code: timeoutCode}, {code: timeoutCode},
		},
	}
	addr, stop := startAdmin(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	err = c.CreateTopic("events", 1)
	if brokerCode(err) != timeoutCode {
		t.Fatalf("err=%v want code 7", err)
	}
	ct, _, _, _, metas, _ := srv.snapshot()
	if ct != 3 || metas != 0 {
		t.Fatalf("create_topic=%d metadata=%d want 3,0", ct, metas)
	}
}

func TestCreateAclsRetriesTimeoutThenOk(t *testing.T) {
	srv := &adminBroker{
		createAclsCodes: []uint16{timeoutCode, 0},
	}
	addr, stop := startAdmin(t, srv)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	if err := c.CreateAcls([]codec.AclBinding{{
		Principal: "User:alice", ResourceType: 0, Resource: "events", Operation: 3, Permission: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	_, _, ca, _, metas, _ := srv.snapshot()
	if ca != 2 || metas != 0 {
		t.Fatalf("create_acls=%d metadata=%d want 2,0", ca, metas)
	}
}

func TestOffsetCommitError14RedirectsViaControllerID(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		offsetCommitReplies: []createTopicReply{{
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
	if err := c.OffsetCommit("g", "t", 0, 5); err != nil {
		t.Fatal(err)
	}
	n, metas, _ := follower.offsetCommitSnapshot()
	if n != 1 || metas != 1 {
		t.Fatalf("follower offset_commit=%d metadata=%d want 1,1", n, metas)
	}
	ln, _, _ := leader.offsetCommitSnapshot()
	if ln != 1 {
		t.Fatalf("leader offset_commit=%d want 1", ln)
	}
}

func TestOffsetFetchTyped14NoHintThenOk(t *testing.T) {
	leader := &adminBroker{}
	_, stopL := startAdmin(t, leader)
	defer stopL()
	follower := &adminBroker{
		offsetFetchCodes: []uint16{notController},
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
	offs, err := c.OffsetFetch("g", "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(offs) != 0 {
		t.Fatalf("offsets %v want empty", offs)
	}
	n, metas, _ := follower.offsetFetchSnapshot()
	if n != 1 || metas != 1 {
		t.Fatalf("follower offset_fetch=%d metadata=%d want 1,1", n, metas)
	}
	ln, _, _ := leader.offsetFetchSnapshot()
	if ln != 1 {
		t.Fatalf("leader offset_fetch=%d want 1", ln)
	}
}

func TestOffsetCommitMaxRedirectsZeroRaisesOnFirst14(t *testing.T) {
	follower := &adminBroker{
		offsetCommitReplies: []createTopicReply{{
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
	err = c.OffsetCommit("g", "t", 0, 5)
	if brokerCode(err) != notController {
		t.Fatalf("err=%v want code 14", err)
	}
	n, metas, accepts := follower.offsetCommitSnapshot()
	if n != 1 || metas != 0 || accepts != 1 {
		t.Fatalf("offset_commit=%d metadata=%d accepts=%d want 1,0,1", n, metas, accepts)
	}
}

