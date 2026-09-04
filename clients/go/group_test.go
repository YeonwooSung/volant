package volant_test

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	volant "github.com/volant-mq/volant/clients/go"
	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

type tpKey struct {
	topic     string
	partition uint32
}

type fakeGroupBroker struct {
	mu            sync.Mutex
	memberID      string
	generation    uint32
	assignment    []codec.Assignment
	revoked       []codec.Assignment
	hbCodes       []uint16
	offsets       map[tpKey]uint64
	records       map[tpKey][]codec.FetchRecord
	joins         []codec.JoinGroupRequest
	heartbeats    []codec.HeartbeatRequest
	commits       []codec.OffsetCommitRequest
	fetches       []codec.FetchRequest
	leaves        []codec.LeaveGroupRequest
	offsetFetches []codec.OffsetFetchRequest
	listOffsets   []codec.ListOffsetsRequest
	bounds          map[tpKey]codec.OffsetListing
	omitBounds      map[tpKey]struct{}
	topics          []codec.TopicInfo
	metadatas       int
	describeMembers []codec.GroupMemberInfo
	describeError   uint16
	describeCount   int
}

func newFakeGroupBroker() *fakeGroupBroker {
	return &fakeGroupBroker{
		memberID:   "m-1",
		generation: 1,
		offsets:    make(map[tpKey]uint64),
		records:    make(map[tpKey][]codec.FetchRecord),
		bounds:     make(map[tpKey]codec.OffsetListing),
		omitBounds: make(map[tpKey]struct{}),
	}
}

func (s *fakeGroupBroker) setAssignment(asgn, revoked []codec.Assignment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assignment = append([]codec.Assignment(nil), asgn...)
	s.revoked = append([]codec.Assignment(nil), revoked...)
}

func (s *fakeGroupBroker) pushHeartbeat(code uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hbCodes = append(s.hbCodes, code)
}

func (s *fakeGroupBroker) setTopic(name string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parts := make([]codec.PartitionInfo, n)
	for i := 0; i < n; i++ {
		parts[i] = codec.PartitionInfo{PartitionID: uint32(i)}
	}
	s.topics = append(s.topics, codec.TopicInfo{Name: name, Partitions: parts})
}

func (s *fakeGroupBroker) setDescribeMembers(members []codec.GroupMemberInfo, errCode uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.describeMembers = append([]codec.GroupMemberInfo(nil), members...)
	s.describeError = errCode
}

func (s *fakeGroupBroker) snapshot() (joins []codec.JoinGroupRequest, hbs []codec.HeartbeatRequest, commits []codec.OffsetCommitRequest, fetches []codec.FetchRequest, leaves []codec.LeaveGroupRequest, ofs []codec.OffsetFetchRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]codec.JoinGroupRequest(nil), s.joins...),
		append([]codec.HeartbeatRequest(nil), s.heartbeats...),
		append([]codec.OffsetCommitRequest(nil), s.commits...),
		append([]codec.FetchRequest(nil), s.fetches...),
		append([]codec.LeaveGroupRequest(nil), s.leaves...),
		append([]codec.OffsetFetchRequest(nil), s.offsetFetches...)
}

func (s *fakeGroupBroker) listOffsetSnapshot() []codec.ListOffsetsRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]codec.ListOffsetsRequest(nil), s.listOffsets...)
}

func startFakeGroup(t *testing.T, s *fakeGroupBroker) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
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

func (s *fakeGroupBroker) serve(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
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
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	}
}

func (s *fakeGroupBroker) handle(f *frame.Frame) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var payload []byte
	var err error
	respOp := f.Opcode
	switch f.Opcode {
	case codec.OpJoinGroup:
		req, e := codec.DecodeJoinGroupRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.joins = append(s.joins, req)
		member := s.memberID
		if member == "" {
			member = req.MemberID
			if member == "" {
				member = "m-1"
			}
			s.memberID = member
		}
		if s.generation == 0 {
			s.generation = 1
		} else if len(s.joins) > 1 {
			s.generation++
		}
		payload, err = codec.EncodeJoinGroupResponse(codec.JoinGroupResponse{
			ErrorCode:  0,
			Generation: s.generation,
			MemberID:   member,
			Assignment: append([]codec.Assignment(nil), s.assignment...),
			Revoked:    append([]codec.Assignment(nil), s.revoked...),
		})
	case codec.OpHeartbeat:
		req, e := codec.DecodeHeartbeatRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.heartbeats = append(s.heartbeats, req)
		code := uint16(0)
		if len(s.hbCodes) > 0 {
			code = s.hbCodes[0]
			s.hbCodes = s.hbCodes[1:]
		}
		payload, err = codec.EncodeHeartbeatResponse(codec.HeartbeatResponse{ErrorCode: code})
	case codec.OpLeaveGroup:
		req, e := codec.DecodeLeaveGroupRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.leaves = append(s.leaves, req)
		payload, err = codec.EncodeLeaveGroupResponse(codec.LeaveGroupResponse{ErrorCode: 0})
	case codec.OpOffsetFetch:
		req, e := codec.DecodeOffsetFetchRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.offsetFetches = append(s.offsetFetches, req)
		var entries []codec.OffsetFetchEntry
		if len(req.Entries) == 0 {
			for k, off := range s.offsets {
				entries = append(entries, codec.OffsetFetchEntry{
					Topic: k.topic, Partition: k.partition, Offset: off,
				})
			}
		} else {
			for _, e := range req.Entries {
				off, ok := s.offsets[tpKey{e.Topic, e.Partition}]
				if !ok {
					off = ^uint64(0)
				}
				entries = append(entries, codec.OffsetFetchEntry{
					Topic: e.Topic, Partition: e.Partition, Offset: off,
				})
			}
		}
		payload, err = codec.EncodeOffsetFetchResponse(codec.OffsetFetchResponse{
			ErrorCode: 0, Entries: entries,
		})
	case codec.OpOffsetCommit:
		req, e := codec.DecodeOffsetCommitRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.commits = append(s.commits, req)
		for _, e := range req.Entries {
			s.offsets[tpKey{e.Topic, e.Partition}] = e.Offset
		}
		payload, err = codec.EncodeOffsetCommitResponse(codec.OffsetCommitResponse{ErrorCode: 0})
	case codec.OpMetadata:
		s.metadatas++
		payload, err = codec.EncodeMetadataResponse(codec.MetadataResponse{
			Topics: append([]codec.TopicInfo(nil), s.topics...),
		})
	case codec.OpDescribeGroup:
		req, e := codec.DecodeDescribeGroupRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.describeCount++
		var resp codec.DescribeGroupResponse
		if s.describeError != 0 {
			resp = codec.DescribeGroupResponse{
				ErrorCode: s.describeError,
				GroupID:   req.GroupID,
			}
		} else {
			resp = codec.DescribeGroupResponse{
				ErrorCode:  0,
				GroupID:    req.GroupID,
				Generation: s.generation,
				Members:    append([]codec.GroupMemberInfo(nil), s.describeMembers...),
			}
		}
		payload, err = codec.EncodeDescribeGroupResponse(resp)
		respOp = codec.OpDescribeGroupResponse
	case codec.OpListOffsets:
		req, e := codec.DecodeListOffsetsRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.listOffsets = append(s.listOffsets, req)
		var entries []codec.OffsetListing
		if len(req.Partitions) == 0 {
			for k, e := range s.bounds {
				if k.topic == req.Topic {
					entries = append(entries, e)
				}
			}
		} else {
			for _, p := range req.Partitions {
				if _, omit := s.omitBounds[tpKey{req.Topic, p}]; omit {
					continue
				}
				if e, ok := s.bounds[tpKey{req.Topic, p}]; ok {
					entries = append(entries, e)
				} else {
					entries = append(entries, codec.OffsetListing{Partition: p, Earliest: 0, Latest: 0})
				}
			}
		}
		payload, err = codec.EncodeListOffsetsResponse(codec.ListOffsetsResponse{
			ErrorCode: 0, Topic: req.Topic, Entries: entries,
		})
		respOp = codec.OpListOffsetsResponse
	case codec.OpFetch:
		req, e := codec.DecodeFetchRequest(f.Payload)
		if e != nil {
			return nil, e
		}
		s.fetches = append(s.fetches, req)
		all := s.records[tpKey{req.Topic, req.Partition}]
		var recs []codec.FetchRecord
		for _, r := range all {
			if r.Offset >= req.FromOffset {
				recs = append(recs, r)
				if req.MaxMessages > 0 && uint32(len(recs)) >= req.MaxMessages {
					break
				}
			}
		}
		var hwm uint64
		if n := len(all); n > 0 {
			hwm = all[n-1].Offset + 1
		}
		payload, err = codec.EncodeFetchResponse(codec.FetchResponse{
			Topic: req.Topic, Partition: req.Partition,
			HighWatermark: hwm, ErrorCode: 0, Records: recs,
		})
	default:
		return nil, fmt.Errorf("unexpected opcode %d", f.Opcode)
	}
	if err != nil {
		return nil, err
	}
	return frame.Encode(respOp, f.CorrelationID, payload)
}

func TestJoinGroupConsumerPositionsFromOffsetFetch(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.offsets[tpKey{"t", 0}] = 5
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if g.MemberID() != "m-1" {
		t.Fatalf("member=%q want m-1", g.MemberID())
	}
	if g.Generation() != 1 {
		t.Fatalf("generation=%d want 1", g.Generation())
	}
	if g.GroupID() != "g" {
		t.Fatalf("group=%q want g", g.GroupID())
	}
	asgn := g.Assignment()
	if len(asgn) != 1 || asgn[0].Topic != "t" || asgn[0].Partition != 0 {
		t.Fatalf("assignment %+v", asgn)
	}
	pos := g.Positions()
	if len(pos) != 1 || pos[0].Offset != 5 {
		t.Fatalf("positions %+v want offset 5", pos)
	}
	_, _, _, _, _, ofs := s.snapshot()
	if len(ofs) != 1 || len(ofs[0].Entries) != 1 || ofs[0].Entries[0].Partition != 0 {
		t.Fatalf("offset fetch %+v", ofs)
	}
}

func TestJoinGroupConsumerUnknownOffsetUsesListOffsetsEarliest(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	// no stored offset → fake returns u64::MAX; default bounds earliest=0
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 0, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	pos := g.Positions()
	if len(pos) != 1 || pos[0].Offset != 0 {
		t.Fatalf("positions %+v want offset 0", pos)
	}
	if g.AutoOffsetReset() != "earliest" {
		t.Fatalf("reset=%q want earliest", g.AutoOffsetReset())
	}
	got := s.listOffsetSnapshot()
	if len(got) != 1 || got[0].Topic != "t" || len(got[0].Partitions) != 1 || got[0].Partitions[0] != 0 {
		t.Fatalf("list_offsets %+v", got)
	}
}

func TestPollHeartbeatAndFetchAdvancesPositions(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.offsets[tpKey{"t", 0}] = 0
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{
		{Offset: 0, Value: []byte("a")},
		{Offset: 1, Value: []byte("b")},
	}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	recs, err := g.Poll(50 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("len(recs)=%d want 2", len(recs))
	}
	if recs[0].Topic != "t" || recs[0].Partition != 0 || string(recs[0].Record.Value) != "a" {
		t.Fatalf("rec0 %+v", recs[0])
	}
	if recs[1].Record.Offset != 1 {
		t.Fatalf("rec1 offset=%d", recs[1].Record.Offset)
	}
	pos := g.Positions()
	if len(pos) != 1 || pos[0].Offset != 2 {
		t.Fatalf("positions %+v want next 2", pos)
	}

	_, hbs, _, fetches, _, _ := s.snapshot()
	if len(hbs) != 1 || hbs[0].MemberID != "m-1" || hbs[0].Generation != 1 {
		t.Fatalf("heartbeats %+v", hbs)
	}
	if len(fetches) != 1 || fetches[0].FromOffset != 0 || fetches[0].MaxWaitMs == 0 {
		t.Fatalf("fetches %+v (want from=0 and max_wait>0)", fetches)
	}
	if fetches[0].MaxMessages != 100 || fetches[0].MaxBytes != 4*1024*1024 {
		t.Fatalf("fetch knobs %+v want max_messages=100 max_bytes=4MiB", fetches[0])
	}
	if g.FetchMaxMessages() != 100 || g.FetchMaxBytes() != 4*1024*1024 {
		t.Fatalf("stored knobs messages=%d bytes=%d", g.FetchMaxMessages(), g.FetchMaxBytes())
	}
	_, _, commits, _, _, _ := s.snapshot()
	if len(commits) != 0 {
		t.Fatalf("commits=%d want 0 (auto-commit default off)", len(commits))
	}
}

func TestPollFetchMaxMessagesFromOption(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{{Offset: 0, Value: []byte("a")}}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000,
		volant.WithBackgroundHeartbeat(false), volant.WithFetchMaxMessages(10))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if _, err := g.Poll(0); err != nil {
		t.Fatal(err)
	}
	_, _, _, fetches, _, _ := s.snapshot()
	if len(fetches) != 1 || fetches[0].MaxMessages != 10 || fetches[0].MaxBytes != 4*1024*1024 {
		t.Fatalf("fetches %+v want max_messages=10 max_bytes=4MiB", fetches)
	}
	if g.FetchMaxMessages() != 10 {
		t.Fatalf("FetchMaxMessages=%d want 10", g.FetchMaxMessages())
	}
}

func TestPollFetchMaxBytesFromOption(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{{Offset: 0, Value: []byte("a")}}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000,
		volant.WithBackgroundHeartbeat(false), volant.WithFetchMaxBytes(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if _, err := g.Poll(0); err != nil {
		t.Fatal(err)
	}
	_, _, _, fetches, _, _ := s.snapshot()
	if len(fetches) != 1 || fetches[0].MaxMessages != 100 || fetches[0].MaxBytes != 4096 {
		t.Fatalf("fetches %+v want max_messages=100 max_bytes=4096", fetches)
	}
	if g.FetchMaxBytes() != 4096 {
		t.Fatalf("FetchMaxBytes=%d want 4096", g.FetchMaxBytes())
	}
}

func TestPollFetchKnobsClampNonPositive(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{{Offset: 0, Value: []byte("a")}}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000,
		volant.WithBackgroundHeartbeat(false),
		volant.WithFetchMaxMessages(0),
		volant.WithFetchMaxBytes(-1))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if g.FetchMaxMessages() != 100 || g.FetchMaxBytes() != 4*1024*1024 {
		t.Fatalf("clamped knobs messages=%d bytes=%d", g.FetchMaxMessages(), g.FetchMaxBytes())
	}
	if _, err := g.Poll(0); err != nil {
		t.Fatal(err)
	}
	_, _, _, fetches, _, _ := s.snapshot()
	if len(fetches) != 1 || fetches[0].MaxMessages != 100 || fetches[0].MaxBytes != 4*1024*1024 {
		t.Fatalf("fetches %+v want defaults after clamp", fetches)
	}
}

func TestPollZeroTimeoutIsNonBlocking(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{{Offset: 0, Value: []byte("x")}}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if _, err := g.Poll(0); err != nil {
		t.Fatal(err)
	}
	_, _, _, fetches, _, _ := s.snapshot()
	if len(fetches) != 1 || fetches[0].MaxWaitMs != 0 {
		t.Fatalf("fetches %+v want max_wait=0", fetches)
	}
}

func TestCommitUsesMemberAndGeneration(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.offsets[tpKey{"t", 0}] = 0
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{{Offset: 0, Value: []byte("a")}}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if _, err := g.Poll(0); err != nil {
		t.Fatal(err)
	}
	if err := g.Commit(); err != nil {
		t.Fatal(err)
	}
	_, _, commits, _, _, _ := s.snapshot()
	if len(commits) != 1 {
		t.Fatalf("commits=%d want 1", len(commits))
	}
	cm := commits[0]
	if cm.GroupID != "g" || cm.MemberID != "m-1" || cm.Generation != 1 {
		t.Fatalf("commit meta %+v", cm)
	}
	if len(cm.Entries) != 1 || cm.Entries[0].Topic != "t" || cm.Entries[0].Offset != 1 {
		t.Fatalf("commit entries %+v", cm.Entries)
	}
}

func TestPollRejoinsOnError9AndHonorsRevoked(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{
		{Topic: "t", Partition: 0},
		{Topic: "t", Partition: 1},
	}, nil)
	s.offsets[tpKey{"t", 0}] = 3
	s.offsets[tpKey{"t", 1}] = 7
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{{Offset: 3, Value: []byte("p0")}}
	s.records[tpKey{"t", 1}] = []codec.FetchRecord{{Offset: 7, Value: []byte("p1")}}
	s.pushHeartbeat(volant.RebalanceInProgress)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if len(g.Assignment()) != 2 {
		t.Fatalf("initial assignment %+v", g.Assignment())
	}

	// Next join (triggered by error 9) keeps p0, revokes p1.
	s.setAssignment(
		[]codec.Assignment{{Topic: "t", Partition: 0}},
		[]codec.Assignment{{Topic: "t", Partition: 1}},
	)

	recs, err := g.Poll(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Partition != 0 {
		t.Fatalf("poll after rejoin %+v want only p0", recs)
	}
	revoked := g.LastRevoked()
	if len(revoked) != 1 || revoked[0].Partition != 1 {
		t.Fatalf("revoked %+v want p1", revoked)
	}
	asgn := g.Assignment()
	if len(asgn) != 1 || asgn[0].Partition != 0 {
		t.Fatalf("assignment after rejoin %+v", asgn)
	}
	for _, p := range g.Positions() {
		if p.Partition == 1 {
			t.Fatalf("revoked p1 still in positions %+v", g.Positions())
		}
	}
	if g.Generation() < 2 {
		t.Fatalf("generation=%d want >= 2 after rejoin", g.Generation())
	}

	joins, hbs, _, fetches, _, _ := s.snapshot()
	if len(joins) != 2 {
		t.Fatalf("joins=%d want 2 (initial + rejoin)", len(joins))
	}
	if joins[1].MemberID != "m-1" {
		t.Fatalf("rejoin member_id=%q want m-1", joins[1].MemberID)
	}
	if len(hbs) != 1 {
		t.Fatalf("heartbeats=%d want 1", len(hbs))
	}
	for _, f := range fetches {
		if f.Partition == 1 {
			t.Fatalf("fetched revoked partition: %+v", fetches)
		}
	}
}

func TestCloseLeavesGroup(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := g.Poll(0); err != volant.ErrGroupClosed {
		t.Fatalf("Poll after Close: %v", err)
	}
	if err := g.Commit(); err != volant.ErrGroupClosed {
		t.Fatalf("Commit after Close: %v", err)
	}
	_, _, _, _, leaves, _ := s.snapshot()
	if len(leaves) != 1 || leaves[0].GroupID != "g" || leaves[0].MemberID != "m-1" {
		t.Fatalf("leaves %+v", leaves)
	}
}

func TestLeaveLeavesGroup(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Leave(); err != nil {
		t.Fatal(err)
	}
	if err := g.Leave(); err != nil {
		t.Fatalf("second Leave: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close after Leave: %v", err)
	}
	if _, err := g.Poll(0); err != volant.ErrGroupClosed {
		t.Fatalf("Poll after Leave: %v", err)
	}
	if err := g.Commit(); err != volant.ErrGroupClosed {
		t.Fatalf("Commit after Leave: %v", err)
	}
	_, _, _, _, leaves, _ := s.snapshot()
	if len(leaves) != 1 || leaves[0].GroupID != "g" || leaves[0].MemberID != "m-1" {
		t.Fatalf("leaves %+v", leaves)
	}
}

func TestJoinEarliestUnknownUsesListOffsetsEarliest(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.bounds[tpKey{"t", 0}] = codec.OffsetListing{Partition: 0, Earliest: 7, Latest: 20}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	pos := g.Positions()
	if len(pos) != 1 || pos[0].Offset != 7 {
		t.Fatalf("positions %+v want offset 7", pos)
	}
	got := s.listOffsetSnapshot()
	if len(got) != 1 || got[0].Topic != "t" || len(got[0].Partitions) != 1 || got[0].Partitions[0] != 0 {
		t.Fatalf("list_offsets %+v", got)
	}
	_, _, _, fetches, _, _ := s.snapshot()
	if len(fetches) != 0 {
		t.Fatalf("fetches=%d want 0", len(fetches))
	}
}

func TestJoinEarliestListOffsetsMissingPartition(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.omitBounds[tpKey{"t", 0}] = struct{}{}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "list_offsets missing partition") {
		t.Fatalf("err=%v want list_offsets missing partition", err)
	}
	got := s.listOffsetSnapshot()
	if len(got) != 1 {
		t.Fatalf("list_offsets=%+v want 1", got)
	}
	_, _, _, fetches, _, _ := s.snapshot()
	if len(fetches) != 0 {
		t.Fatalf("fetches=%d want 0", len(fetches))
	}
}

func TestJoinLatestUnknownUsesListOffsets(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.bounds[tpKey{"t", 0}] = codec.OffsetListing{Partition: 0, Earliest: 7, Latest: 20}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000,
		volant.WithBackgroundHeartbeat(false), volant.WithAutoOffsetReset("latest"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	pos := g.Positions()
	if len(pos) != 1 || pos[0].Offset != 20 {
		t.Fatalf("positions %+v want offset 20", pos)
	}
	got := s.listOffsetSnapshot()
	if len(got) != 1 || got[0].Topic != "t" || len(got[0].Partitions) != 1 || got[0].Partitions[0] != 0 {
		t.Fatalf("list_offsets %+v", got)
	}
	_, _, _, fetches, _, _ := s.snapshot()
	if len(fetches) != 0 {
		t.Fatalf("fetches=%d want 0", len(fetches))
	}
}

func TestJoinLatestCommittedSkipsListOffsets(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.offsets[tpKey{"t", 0}] = 5
	s.bounds[tpKey{"t", 0}] = codec.OffsetListing{Partition: 0, Earliest: 7, Latest: 20}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000,
		volant.WithBackgroundHeartbeat(false), volant.WithAutoOffsetReset("latest"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	pos := g.Positions()
	if len(pos) != 1 || pos[0].Offset != 5 {
		t.Fatalf("positions %+v want offset 5", pos)
	}
	if got := s.listOffsetSnapshot(); len(got) != 0 {
		t.Fatalf("list_offsets=%+v want none", got)
	}
}

func TestJoinEarliestCommittedSkipsListOffsets(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.offsets[tpKey{"t", 0}] = 3
	s.bounds[tpKey{"t", 0}] = codec.OffsetListing{Partition: 0, Earliest: 7, Latest: 20}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	pos := g.Positions()
	if len(pos) != 1 || pos[0].Offset != 3 {
		t.Fatalf("positions %+v want offset 3", pos)
	}
	if got := s.listOffsetSnapshot(); len(got) != 0 {
		t.Fatalf("list_offsets=%+v want none", got)
	}
}

func TestJoinNoneUnknownRaisesWithoutFetch(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000,
		volant.WithBackgroundHeartbeat(false), volant.WithAutoOffsetReset("none"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "auto_offset_reset") {
		t.Fatalf("err=%v want auto_offset_reset", err)
	}
	_, _, _, fetches, _, _ := s.snapshot()
	if len(fetches) != 0 {
		t.Fatalf("fetches=%d want 0", len(fetches))
	}
	if got := s.listOffsetSnapshot(); len(got) != 0 {
		t.Fatalf("list_offsets=%+v want none", got)
	}
}

func TestJoinInvalidAutoOffsetReset(t *testing.T) {
	_, err := volant.JoinGroupConsumer(nil, "g", []string{"t"}, 10_000, volant.WithAutoOffsetReset("foo"))
	if err == nil || !strings.Contains(err.Error(), "unknown auto_offset_reset") {
		t.Fatalf("err=%v want unknown auto_offset_reset", err)
	}
}

func TestJoinLatestEmptyAssignmentSkipsListOffsets(t *testing.T) {
	s := newFakeGroupBroker()
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000,
		volant.WithBackgroundHeartbeat(false), volant.WithAutoOffsetReset("latest"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if len(g.Positions()) != 0 {
		t.Fatalf("positions %+v want empty", g.Positions())
	}
	if got := s.listOffsetSnapshot(); len(got) != 0 {
		t.Fatalf("list_offsets=%+v want none", got)
	}
	_, _, _, _, _, ofs := s.snapshot()
	if len(ofs) != 0 {
		t.Fatalf("offset fetch %+v want none", ofs)
	}
}

func TestJoinGroupConsumerNilClient(t *testing.T) {
	_, err := volant.JoinGroupConsumer(nil, "g", []string{"t"}, 10_000)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestJoinGroupConsumerSessionTimeoutMs(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	if g.SessionTimeoutMs() != 10000 {
		t.Fatalf("SessionTimeoutMs()=%d want 10000", g.SessionTimeoutMs())
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	g, err = volant.JoinGroupConsumer(c, "g", []string{"t"}, 0, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if g.SessionTimeoutMs() != 10000 {
		t.Fatalf("zero join SessionTimeoutMs()=%d want 10000", g.SessionTimeoutMs())
	}
}

func TestJoinGroupConsumerStaticSendsInstanceID(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumerStatic(c, "g", []string{"t"}, 10_000, "inst-1", volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if g.GroupInstanceID() != "inst-1" {
		t.Fatalf("instance=%q want inst-1", g.GroupInstanceID())
	}
	joins, _, _, _, _, _ := s.snapshot()
	if len(joins) != 1 {
		t.Fatalf("joins=%d want 1", len(joins))
	}
	if joins[0].GroupInstanceID != "inst-1" {
		t.Fatalf("join instance=%q want inst-1", joins[0].GroupInstanceID)
	}
	if joins[0].MemberID != "" {
		t.Fatalf("first join member_id=%q want empty", joins[0].MemberID)
	}
}

func TestJoinGroupConsumerDefaultIsDynamic(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if g.GroupInstanceID() != "" {
		t.Fatalf("instance=%q want empty", g.GroupInstanceID())
	}
	joins, _, _, _, _, _ := s.snapshot()
	if len(joins) != 1 || joins[0].GroupInstanceID != "" {
		t.Fatalf("joins %+v want empty instance", joins)
	}
}

func TestJoinGroupConsumerStaticRejoinKeepsInstanceID(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.pushHeartbeat(volant.RebalanceInProgress)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumerStatic(c, "g", []string{"t"}, 10_000, "inst-1", volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if _, err := g.Poll(0); err != nil {
		t.Fatal(err)
	}
	joins, _, _, _, _, _ := s.snapshot()
	if len(joins) != 2 {
		t.Fatalf("joins=%d want 2 (initial + rejoin)", len(joins))
	}
	if joins[0].GroupInstanceID != "inst-1" || joins[1].GroupInstanceID != "inst-1" {
		t.Fatalf("instance ids %+v want inst-1 on both", []string{joins[0].GroupInstanceID, joins[1].GroupInstanceID})
	}
	if joins[1].MemberID != "m-1" {
		t.Fatalf("rejoin member_id=%q want m-1", joins[1].MemberID)
	}
}

func TestHeartbeatIntervalClamped(t *testing.T) {
	if got := volant.HeartbeatInterval(0); got != 100*time.Millisecond {
		t.Fatalf("0 → %v want 100ms", got)
	}
	if got := volant.HeartbeatInterval(300); got != 100*time.Millisecond {
		t.Fatalf("300 → %v want 100ms", got)
	}
	if got := volant.HeartbeatInterval(900); got != 300*time.Millisecond {
		t.Fatalf("900 → %v want 300ms", got)
	}
	if got := volant.HeartbeatInterval(10_000); got != 3*time.Second {
		t.Fatalf("10000 → %v want 3s", got)
	}
}

func TestBackgroundHeartbeatWithoutPoll(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 300)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	var hbs []codec.HeartbeatRequest
	for time.Now().Before(deadline) {
		_, hbs, _, _, _, _ = s.snapshot()
		if len(hbs) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(hbs) == 0 {
		t.Fatal("expected background heartbeats without Poll")
	}
	_, _, _, fetches, _, _ := s.snapshot()
	if len(fetches) != 0 {
		t.Fatalf("unexpected fetches %+v", fetches)
	}

	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	var leaves []codec.LeaveGroupRequest
	_, hbs, _, _, leaves, _ = s.snapshot()
	n := len(hbs)
	time.Sleep(350 * time.Millisecond)
	_, hbs2, _, _, leaves2, _ := s.snapshot()
	if len(hbs2) != n {
		t.Fatalf("heartbeats after Close: %d → %d", n, len(hbs2))
	}
	if len(leaves) != 1 || leaves[0].MemberID != "m-1" {
		t.Fatalf("leaves %+v", leaves)
	}
	if len(leaves2) != 1 {
		t.Fatalf("extra leaves after Close: %+v", leaves2)
	}
}

func TestBackgroundHeartbeatRejoinsOnError9(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.pushHeartbeat(volant.RebalanceInProgress)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if g.Generation() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if g.Generation() < 2 {
		t.Fatalf("generation=%d want >= 2 after background rejoin", g.Generation())
	}
	joins, _, _, _, _, _ := s.snapshot()
	if len(joins) < 2 {
		t.Fatalf("joins=%d want >= 2", len(joins))
	}
}

func TestPollDoesNotAutoCommitByDefault(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{{Offset: 0, Value: []byte("a")}}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	recs, err := g.Poll(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("len(recs)=%d want 1", len(recs))
	}
	_, _, commits, _, _, _ := s.snapshot()
	if len(commits) != 0 {
		t.Fatalf("commits=%d want 0", len(commits))
	}
}

func TestAutoCommitIntervalZeroCommitsAfterPoll(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{{Offset: 0, Value: []byte("a")}}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000,
		volant.WithBackgroundHeartbeat(false), volant.WithAutoCommit(0))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	recs, err := g.Poll(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("len(recs)=%d want 1", len(recs))
	}
	_, _, commits, _, _, _ := s.snapshot()
	if len(commits) != 1 {
		t.Fatalf("commits=%d want 1", len(commits))
	}
	cm := commits[0]
	if cm.GroupID != "g" || cm.MemberID != "m-1" || cm.Generation != 1 {
		t.Fatalf("commit meta %+v", cm)
	}
	if len(cm.Entries) != 1 || cm.Entries[0].Offset != 1 {
		t.Fatalf("commit entries %+v", cm.Entries)
	}
}

func TestAutoCommitIntervalFirstPollOnly(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{{Offset: 0, Value: []byte("a")}}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000,
		volant.WithBackgroundHeartbeat(false), volant.WithAutoCommit(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if _, err := g.Poll(0); err != nil {
		t.Fatal(err)
	}
	_, _, commits, _, _, _ := s.snapshot()
	if len(commits) != 1 {
		t.Fatalf("after first poll commits=%d want 1", len(commits))
	}

	s.mu.Lock()
	s.records[tpKey{"t", 0}] = append(s.records[tpKey{"t", 0}], codec.FetchRecord{Offset: 1, Value: []byte("b")})
	s.mu.Unlock()

	if recs, err := g.Poll(0); err != nil {
		t.Fatal(err)
	} else if len(recs) != 1 {
		t.Fatalf("second poll recs=%d want 1", len(recs))
	}
	_, _, commits, _, _, _ = s.snapshot()
	if len(commits) != 1 {
		t.Fatalf("after second poll commits=%d want 1", len(commits))
	}
}

func TestAutoCommitCloseCommitsPendingThenLeaves(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{{Offset: 0, Value: []byte("a")}}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000,
		volant.WithBackgroundHeartbeat(false), volant.WithAutoCommit(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := g.Poll(0); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.records[tpKey{"t", 0}] = append(s.records[tpKey{"t", 0}], codec.FetchRecord{Offset: 1, Value: []byte("b")})
	s.mu.Unlock()
	if _, err := g.Poll(0); err != nil {
		t.Fatal(err)
	}
	_, _, commits, _, leaves, _ := s.snapshot()
	if len(commits) != 1 {
		t.Fatalf("before Close commits=%d want 1", len(commits))
	}
	if len(leaves) != 0 {
		t.Fatalf("leaves before Close: %+v", leaves)
	}

	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, commits, _, leaves, _ = s.snapshot()
	if len(commits) != 2 {
		t.Fatalf("after Close commits=%d want 2", len(commits))
	}
	if len(commits[1].Entries) != 1 || commits[1].Entries[0].Offset != 2 {
		t.Fatalf("close commit entries %+v", commits[1].Entries)
	}
	if commits[1].MemberID != "m-1" || commits[1].Generation != 1 {
		t.Fatalf("close commit meta %+v", commits[1])
	}
	if len(leaves) != 1 || leaves[0].MemberID != "m-1" {
		t.Fatalf("leaves %+v", leaves)
	}
}

func TestHeartbeatCountJoinIsZeroThenPollIsOne(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if g.HeartbeatCount() != 0 {
		t.Fatalf("HeartbeatCount=%d want 0 after join", g.HeartbeatCount())
	}
	_, hbs, _, _, _, _ := s.snapshot()
	if len(hbs) != 0 {
		t.Fatalf("heartbeats=%d want 0 after join (JoinGroup is not counted)", len(hbs))
	}

	if _, err := g.Poll(0); err != nil {
		t.Fatal(err)
	}
	if g.HeartbeatCount() != 1 {
		t.Fatalf("HeartbeatCount=%d want 1 after one Poll", g.HeartbeatCount())
	}
	_, hbs, _, _, _, _ = s.snapshot()
	if len(hbs) != 1 {
		t.Fatalf("heartbeats=%d want 1 after one Poll", len(hbs))
	}
}

func TestHeartbeatCountNil(t *testing.T) {
	var g *volant.GroupConsumer
	if g.HeartbeatCount() != 0 {
		t.Fatalf("nil HeartbeatCount=%d want 0", g.HeartbeatCount())
	}
}

func TestHeartbeatCountBackgroundWithoutPoll(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if g.HeartbeatCount() != 0 {
		t.Fatalf("HeartbeatCount=%d want 0 immediately after join", g.HeartbeatCount())
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if g.HeartbeatCount() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := g.HeartbeatCount(); n < 1 {
		t.Fatalf("HeartbeatCount=%d want >= 1 without Poll", n)
	}
	_, _, _, fetches, _, _ := s.snapshot()
	if len(fetches) != 0 {
		t.Fatalf("unexpected fetches %+v", fetches)
	}
}

func TestBackgroundHeartbeatDisabledIsPollOnly(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 300, volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	time.Sleep(350 * time.Millisecond)
	_, hbs, _, _, _, _ := s.snapshot()
	if len(hbs) != 0 {
		t.Fatalf("heartbeats=%d want 0 with heartbeat disabled", len(hbs))
	}
}

func TestE2EGroupConsumerJoinPollCommitResume(t *testing.T) {
	if os.Getenv("VOLANT_E2E") != "1" {
		t.Skip("set VOLANT_E2E=1 to run live broker e2e")
	}
	addr, cleanup := startBroker(t)
	defer cleanup()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	topic := fmt.Sprintf("go-gc-%d-%d", os.Getpid(), time.Now().UnixNano())
	group := fmt.Sprintf("go-gcg-%d", os.Getpid())
	if _, err := c.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := c.Produce(topic, 0, nil, []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	g, err := volant.JoinGroupConsumer(c, group, []string{topic}, 10_000)
	if err != nil {
		t.Fatalf("JoinGroupConsumer: %v", err)
	}
	var got []string
	for i := 0; i < 8 && len(got) < 3; i++ {
		recs, err := g.Poll(200 * time.Millisecond)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		for _, r := range recs {
			got = append(got, string(r.Record.Value))
		}
	}
	if len(got) != 3 {
		t.Fatalf("first consumer got %v want 3 records", got)
	}
	if err := g.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := c.Produce(topic, 0, nil, []byte("new")); err != nil {
		t.Fatalf("Produce new: %v", err)
	}

	c2, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	g2, err := volant.JoinGroupConsumer(c2, group, []string{topic}, 10_000)
	if err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	defer g2.Close()
	var later []string
	for i := 0; i < 8 && len(later) == 0; i++ {
		recs, err := g2.Poll(200 * time.Millisecond)
		if err != nil {
			t.Fatalf("resume Poll: %v", err)
		}
		for _, r := range recs {
			later = append(later, string(r.Record.Value))
		}
	}
	if len(later) != 1 || later[0] != "new" {
		t.Fatalf("resume got %v want [new]", later)
	}
	if err := c.DeleteTopic(topic); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
}

func TestE2EGroupConsumerSplitAssignment(t *testing.T) {
	if os.Getenv("VOLANT_E2E") != "1" {
		t.Skip("set VOLANT_E2E=1 to run live broker e2e")
	}
	addr, cleanup := startBroker(t)
	defer cleanup()

	admin, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	topic := fmt.Sprintf("go-split-%d-%d", os.Getpid(), time.Now().UnixNano())
	group := fmt.Sprintf("go-splitg-%d", os.Getpid())
	if _, err := admin.CreateTopic(topic, 2); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	c1, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	g1, err := volant.JoinGroupConsumer(c1, group, []string{topic}, 10_000)
	if err != nil {
		t.Fatalf("join g1: %v", err)
	}
	defer g1.Close()
	g2, err := volant.JoinGroupConsumer(c2, group, []string{topic}, 10_000)
	if err != nil {
		t.Fatalf("join g2: %v", err)
	}
	defer g2.Close()

	// g1's assignment is stale until heartbeat/poll sees error 9 and rejoins.
	for i := 0; i < 6; i++ {
		if _, err := g1.Poll(0); err != nil {
			t.Fatalf("g1 poll: %v", err)
		}
		if _, err := g2.Poll(0); err != nil {
			t.Fatalf("g2 poll: %v", err)
		}
		a1 := g1.Assignment()
		a2 := g2.Assignment()
		if len(a1) == 0 || len(a2) == 0 {
			continue
		}
		seen := map[uint32]int{}
		overlap := false
		for _, a := range a1 {
			seen[a.Partition]++
		}
		for _, a := range a2 {
			if seen[a.Partition] > 0 {
				overlap = true
			}
			seen[a.Partition]++
		}
		if !overlap && seen[0] > 0 && seen[1] > 0 {
			if err := admin.DeleteTopic(topic); err != nil {
				t.Fatalf("DeleteTopic: %v", err)
			}
			return
		}
	}
	t.Fatalf("assignments not disjoint+cover: g1=%+v g2=%+v", g1.Assignment(), g2.Assignment())
}

func TestE2EGroupConsumerStaticMembership(t *testing.T) {
	if os.Getenv("VOLANT_E2E") != "1" {
		t.Skip("set VOLANT_E2E=1 to run live broker e2e")
	}
	addr, cleanup := startBroker(t)
	defer cleanup()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	topic := fmt.Sprintf("go-static-%d-%d", os.Getpid(), time.Now().UnixNano())
	group := fmt.Sprintf("go-staticg-%d", os.Getpid())
	if _, err := c.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	g, err := volant.JoinGroupConsumerStatic(c, group, []string{topic}, 10_000, "inst-1")
	if err != nil {
		t.Fatalf("JoinGroupConsumerStatic: %v", err)
	}
	defer g.Close()
	if g.GroupInstanceID() != "inst-1" {
		t.Fatalf("instance=%q want inst-1", g.GroupInstanceID())
	}
	if g.MemberID() != "static:inst-1" {
		t.Fatalf("member=%q want static:inst-1", g.MemberID())
	}
	if err := c.DeleteTopic(topic); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
}
