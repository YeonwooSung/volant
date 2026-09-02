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

type scriptedBroker struct {
	mu            sync.Mutex
	produceCodes  []uint16
	fetchCodes    []uint16
	meta          codec.MetadataResponse
	produceCount  int
	fetchCount    int
	metadataCount int
	acceptCount   int
	ln            net.Listener
}

func startScripted(t *testing.T, s *scriptedBroker) (addr string, stop func()) {
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

func (s *scriptedBroker) port() int {
	return s.ln.Addr().(*net.TCPAddr).Port
}

func (s *scriptedBroker) snapshot() (produces, fetches, metas, accepts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.produceCount, s.fetchCount, s.metadataCount, s.acceptCount
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
	var payload []byte
	var err error
	switch f.Opcode {
	case codec.OpProduce:
		s.produceCount++
		req, e := codec.DecodeProduceRequest(f.Payload)
		if e != nil {
			return nil, e
		}
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
			count = 1
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
	return frame.Encode(f.Opcode, f.CorrelationID, payload)
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
