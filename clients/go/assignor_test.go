package volant_test

import (
	"strings"
	"testing"
	"time"

	volant "github.com/volant-mq/volant/clients/go"
	"github.com/volant-mq/volant/clients/go/codec"
)

func TestRangeAssignUnevenPartitions(t *testing.T) {
	parts := volant.RangeAssign(5, []string{"a", "b"})
	if len(parts[0])+len(parts[1]) != 5 {
		t.Fatalf("cover %d want 5", len(parts[0])+len(parts[1]))
	}
	assertU32s(t, parts[0], 0, 1, 2)
	assertU32s(t, parts[1], 3, 4)
}

func TestRangeAssignEvenSplit(t *testing.T) {
	parts := volant.RangeAssign(4, []string{"m0", "m1"})
	assertU32s(t, parts[0], 0, 1)
	assertU32s(t, parts[1], 2, 3)
}

func TestRangeAssignSingleMemberGetsAll(t *testing.T) {
	parts := volant.RangeAssign(3, []string{"solo"})
	assertU32s(t, parts[0], 0, 1, 2)
}

func TestRangeAssignThreeMembersSevenPartitions(t *testing.T) {
	parts := volant.RangeAssign(7, []string{"c", "a", "b"})
	assertU32s(t, parts[1], 0, 1, 2)
	assertU32s(t, parts[2], 3, 4)
	assertU32s(t, parts[0], 5, 6)
}

func TestRangeAssignEmptyMembersOrZeroPartitions(t *testing.T) {
	if got := volant.RangeAssign(5, nil); len(got) != 0 {
		t.Fatalf("empty members: %+v", got)
	}
	got := volant.RangeAssign(0, []string{"a", "b"})
	if len(got) != 2 || len(got[0]) != 0 || len(got[1]) != 0 {
		t.Fatalf("zero partitions: %+v", got)
	}
}

func TestRangeAssignMultiDisjointCover(t *testing.T) {
	assigns := volant.RangeAssignMulti(
		[]string{"m1", "m2"},
		[][]string{{"t"}, {"t"}},
		map[string]uint32{"t": 4},
	)
	assertAssigns(t, assigns[0], "t", 0, 1)
	assertAssigns(t, assigns[1], "t", 2, 3)
}

func TestRangeAssignMultiSkipsMissingTopic(t *testing.T) {
	assigns := volant.RangeAssignMulti(
		[]string{"solo"},
		[][]string{{"missing", "t"}},
		map[string]uint32{"t": 2},
	)
	assertAssigns(t, assigns[0], "t", 0, 1)
}

func TestRangeAssignMultiEmptyMembers(t *testing.T) {
	got := volant.RangeAssignMulti(nil, nil, map[string]uint32{})
	if len(got) != 0 {
		t.Fatalf("empty members: %+v", got)
	}
}

func TestJoinGroupConsumerRangeFetchesEveryPartition(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.setTopic("t", 3)
	s.records[tpKey{"t", 0}] = []codec.FetchRecord{{Offset: 0, Value: []byte{0}}}
	s.records[tpKey{"t", 1}] = []codec.FetchRecord{{Offset: 0, Value: []byte{1}}}
	s.records[tpKey{"t", 2}] = []codec.FetchRecord{{Offset: 0, Value: []byte{2}}}
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithAssignor("range"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	asgn := g.Assignment()
	if len(asgn) != 3 {
		t.Fatalf("assignment %+v want 3 partitions", asgn)
	}
	for i, a := range asgn {
		if a.Topic != "t" || a.Partition != uint32(i) {
			t.Fatalf("assignment[%d]=%+v", i, a)
		}
	}

	recs, err := g.Poll(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("poll %+v want 3 records", recs)
	}
	_, _, _, fetches, _, _ := s.snapshot()
	seen := map[uint32]int{}
	for _, f := range fetches {
		seen[f.Partition]++
	}
	if seen[0] == 0 || seen[1] == 0 || seen[2] == 0 {
		t.Fatalf("fetches %+v want partitions 0,1,2", fetches)
	}
	s.mu.Lock()
	metas := s.metadatas
	describes := s.describeCount
	s.mu.Unlock()
	if metas != 1 {
		t.Fatalf("metadata calls=%d want 1", metas)
	}
	if describes != 1 {
		t.Fatalf("describe_group calls=%d want 1", describes)
	}
}

func TestJoinGroupConsumerBrokerDoesNotCallMetadata(t *testing.T) {
	s := newFakeGroupBroker()
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.setTopic("t", 3)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	asgn := g.Assignment()
	if len(asgn) != 1 || asgn[0].Partition != 0 {
		t.Fatalf("assignment %+v want only p0", asgn)
	}
	s.mu.Lock()
	metas := s.metadatas
	describes := s.describeCount
	s.mu.Unlock()
	if metas != 0 {
		t.Fatalf("metadata calls=%d want 0", metas)
	}
	if describes != 0 {
		t.Fatalf("describe_group calls=%d want 0", describes)
	}
}

func TestJoinGroupConsumerRangeDescribeSplitsHalf(t *testing.T) {
	cases := []struct {
		member string
		want   []uint32
	}{
		{"m-a", []uint32{0, 1}},
		{"m-b", []uint32{2, 3}},
	}
	for _, tc := range cases {
		t.Run(tc.member, func(t *testing.T) {
			s := newFakeGroupBroker()
			s.memberID = tc.member
			s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
			s.setTopic("t", 4)
			s.setDescribeMembers([]codec.GroupMemberInfo{
				{MemberID: "m-a", Topics: []string{"t"}},
				{MemberID: "m-b", Topics: []string{"t"}},
			}, 0)
			addr, stop := startFakeGroup(t, s)
			defer stop()

			c, err := volant.DialTimeout(addr, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()

			g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithAssignor("range"), volant.WithBackgroundHeartbeat(false))
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()

			assertAssigns(t, g.Assignment(), "t", tc.want...)
			s.mu.Lock()
			describes := s.describeCount
			s.mu.Unlock()
			if describes != 1 {
				t.Fatalf("describe_group calls=%d want 1", describes)
			}
		})
	}
}

func TestJoinGroupConsumerRangeDescribeErrorFallsBackToSolo(t *testing.T) {
	s := newFakeGroupBroker()
	s.memberID = "m-a"
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.setTopic("t", 4)
	s.setDescribeMembers(nil, 2)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithAssignor("range"), volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	assertAssigns(t, g.Assignment(), "t", 0, 1, 2, 3)
}

func TestJoinGroupConsumerRangeDescribeOmitsSelfStillIncludes(t *testing.T) {
	s := newFakeGroupBroker()
	s.memberID = "m-b"
	s.setAssignment([]codec.Assignment{{Topic: "t", Partition: 0}}, nil)
	s.setTopic("t", 4)
	s.setDescribeMembers([]codec.GroupMemberInfo{
		{MemberID: "m-a", Topics: []string{"t"}},
	}, 0)
	addr, stop := startFakeGroup(t, s)
	defer stop()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithAssignor("range"), volant.WithBackgroundHeartbeat(false))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	assertAssigns(t, g.Assignment(), "t", 2, 3)
}

func TestJoinGroupConsumerUnknownAssignor(t *testing.T) {
	_, err := volant.JoinGroupConsumer(nil, "g", []string{"t"}, 10_000, volant.WithAssignor("sticky"))
	if err == nil || !strings.Contains(err.Error(), "unknown assignor") {
		t.Fatalf("err=%v want unknown assignor", err)
	}
}

func assertU32s(t *testing.T, got []uint32, want ...uint32) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d (%v vs %v)", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func assertAssigns(t *testing.T, got []volant.Assignment, topic string, parts ...uint32) {
	t.Helper()
	if len(got) != len(parts) {
		t.Fatalf("len=%d want %d (%+v)", len(got), len(parts), got)
	}
	for i, p := range parts {
		if got[i].Topic != topic || got[i].Partition != p {
			t.Fatalf("got %+v want %s-%d at %d", got, topic, p, i)
		}
	}
}
