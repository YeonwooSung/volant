package volant

import (
	"errors"
	"sort"
	"time"

	"github.com/volant-mq/volant/clients/go/codec"
)

// Wire sentinel: unknown / not-committed offset (docs/PHASE3_SPEC.md).
const offsetUnknown = ^uint64(0)

// Broker group error codes that mean the member should re-JoinGroup.
const (
	// RebalanceInProgress is heartbeat/group error_code 9.
	RebalanceInProgress uint16 = 9
	// UnknownMemberID is error_code 10.
	UnknownMemberID uint16 = 10
	// IllegalGeneration is error_code 11.
	IllegalGeneration uint16 = 11
)

// ErrGroupClosed is returned by Poll/Commit after Close.
var ErrGroupClosed = errors.New("group consumer closed")

type topicPartition struct {
	topic     string
	partition uint32
}

// FetchedRecord is one record from GroupConsumer.Poll with topic/partition.
type FetchedRecord struct {
	Topic     string
	Partition uint32
	Record    Record
}

// Position is the next-read offset for one assigned partition.
type Position struct {
	Topic     string
	Partition uint32
	Offset    uint64
}

// GroupConsumer joins a group, polls assigned partitions, and commits.
//
// Not safe for concurrent use. The Client must stay open for the lifetime
// of the consumer; Close leaves the group and does not close the Client.
type GroupConsumer struct {
	client           *Client
	groupID          string
	topics           []string
	sessionTimeoutMs uint32
	memberID         string
	groupInstanceID  string
	generation       uint32
	assignment       []Assignment
	lastRevoked      []Assignment
	positions        map[topicPartition]uint64
	closed           bool
}

// JoinGroupConsumer joins a consumer group on the given topics.
// sessionTimeoutMs 0 defaults to 10000. Dynamic membership (empty instance id).
func JoinGroupConsumer(c *Client, group string, topics []string, sessionTimeoutMs int) (*GroupConsumer, error) {
	return joinGroupConsumer(c, group, topics, sessionTimeoutMs, "")
}

// JoinGroupConsumerStatic joins with Phase 12 static membership.
// Empty groupInstanceID is dynamic (same as JoinGroupConsumer).
// Re-join after error 9/10/11 resends the same instance id.
func JoinGroupConsumerStatic(c *Client, group string, topics []string, sessionTimeoutMs int, groupInstanceID string) (*GroupConsumer, error) {
	return joinGroupConsumer(c, group, topics, sessionTimeoutMs, groupInstanceID)
}

func joinGroupConsumer(c *Client, group string, topics []string, sessionTimeoutMs int, instanceID string) (*GroupConsumer, error) {
	if c == nil {
		return nil, errors.New("nil client")
	}
	timeout := uint32(sessionTimeoutMs)
	if timeout == 0 {
		timeout = 10_000
	}
	if topics == nil {
		topics = []string{}
	}
	g := &GroupConsumer{
		client:           c,
		groupID:          group,
		topics:           append([]string(nil), topics...),
		sessionTimeoutMs: timeout,
		groupInstanceID:  instanceID,
		positions:        make(map[topicPartition]uint64),
	}
	if err := g.doJoin(); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *GroupConsumer) doJoin() error {
	previous := copyAssignment(g.assignment)
	result, err := g.client.joinGroup(g.groupID, g.memberID, g.topics, int(g.sessionTimeoutMs), g.groupInstanceID)
	if err != nil {
		return err
	}
	g.memberID = result.MemberID
	g.generation = result.Generation
	newAssignment := copyAssignment(result.Assignment)

	oldSet := assignmentSet(previous)
	newSet := assignmentSet(newAssignment)

	var revoked []Assignment
	for tp := range oldSet {
		if _, ok := newSet[tp]; !ok {
			revoked = append(revoked, Assignment{Topic: tp.topic, Partition: tp.partition})
		}
	}
	for _, a := range result.Revoked {
		tp := topicPartition{a.Topic, a.Partition}
		found := false
		for _, r := range revoked {
			if r.Topic == tp.topic && r.Partition == tp.partition {
				found = true
				break
			}
		}
		if !found {
			revoked = append(revoked, Assignment{Topic: tp.topic, Partition: tp.partition})
		}
	}
	sort.Slice(revoked, func(i, j int) bool {
		if revoked[i].Topic != revoked[j].Topic {
			return revoked[i].Topic < revoked[j].Topic
		}
		return revoked[i].Partition < revoked[j].Partition
	})

	var added []topicPartition
	for tp := range newSet {
		if _, ok := oldSet[tp]; !ok {
			added = append(added, tp)
		}
	}

	for _, a := range revoked {
		delete(g.positions, topicPartition{a.Topic, a.Partition})
	}

	g.assignment = newAssignment
	g.lastRevoked = revoked

	if len(added) > 0 || (len(g.positions) == 0 && len(g.assignment) > 0) {
		// First join: positions empty and assignment full → fetch all.
		// Rebalance: only fetch offsets for newly added partitions.
		var toFetch []topicPartition
		if len(previous) == 0 {
			for _, a := range g.assignment {
				toFetch = append(toFetch, topicPartition{a.Topic, a.Partition})
			}
		} else {
			toFetch = added
		}
		if err := g.fetchPositionsFor(toFetch); err != nil {
			return err
		}
	}

	for _, a := range g.assignment {
		tp := topicPartition{a.Topic, a.Partition}
		if _, ok := g.positions[tp]; !ok {
			g.positions[tp] = 0
		}
	}
	return nil
}

func (g *GroupConsumer) fetchPositionsFor(partitions []topicPartition) error {
	if len(partitions) == 0 {
		return nil
	}
	entries := make([]codec.OffsetEntry, 0, len(partitions))
	for _, p := range partitions {
		entries = append(entries, codec.OffsetEntry{Topic: p.topic, Partition: p.partition})
	}
	fetched, err := g.client.fetchOffsets(g.groupID, entries)
	if err != nil {
		return err
	}
	for _, e := range fetched {
		pos := e.Offset
		if pos == offsetUnknown {
			pos = 0
		}
		g.positions[topicPartition{e.Topic, e.Partition}] = pos
	}
	return nil
}

// Poll heartbeats, rejoins on error 9/10/11, and fetches assigned partitions.
//
// timeout is the Fetch max-wait budget for this call (0 = non-blocking).
// One heartbeat plus one fetch pass; remaining time is used as max_wait_ms
// on each partition fetch until it runs out.
func (g *GroupConsumer) Poll(timeout time.Duration) ([]FetchedRecord, error) {
	if g == nil || g.closed {
		return nil, ErrGroupClosed
	}
	if err := g.client.Heartbeat(g.groupID, g.memberID, g.generation); err != nil {
		if needsRebalance(err) {
			if err := g.doJoin(); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	var out []FetchedRecord
	assignment := copyAssignment(g.assignment)
	for _, a := range assignment {
		from := g.positions[topicPartition{a.Topic, a.Partition}]
		var maxWait uint32
		if !deadline.IsZero() {
			if rem := time.Until(deadline); rem > 0 {
				ms := rem / time.Millisecond
				if ms > 0 {
					maxWait = uint32(ms)
				}
			}
		}
		recs, err := g.client.fetchAt(a.Topic, int(a.Partition), int64(from), 100, maxWait)
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			next := r.Offset
			if next < offsetUnknown {
				next++
			}
			g.positions[topicPartition{a.Topic, a.Partition}] = next
			out = append(out, FetchedRecord{
				Topic:     a.Topic,
				Partition: a.Partition,
				Record:    r,
			})
		}
	}
	return out, nil
}

// Commit commits last+1 positions for assigned partitions with
// member_id + generation (not the admin empty-member path).
func (g *GroupConsumer) Commit() error {
	if g == nil || g.closed {
		return ErrGroupClosed
	}
	if len(g.positions) == 0 {
		return nil
	}
	entries := make([]codec.OffsetCommitEntry, 0, len(g.positions))
	for tp, off := range g.positions {
		entries = append(entries, codec.OffsetCommitEntry{
			Topic:     tp.topic,
			Partition: tp.partition,
			Offset:    off,
			Metadata:  "",
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Topic != entries[j].Topic {
			return entries[i].Topic < entries[j].Topic
		}
		return entries[i].Partition < entries[j].Partition
	})
	return g.client.commitOffsets(g.groupID, g.memberID, g.generation, entries)
}

// Close leaves the group. The Client is left open. Idempotent.
func (g *GroupConsumer) Close() error {
	if g == nil || g.closed {
		return nil
	}
	g.closed = true
	if g.memberID == "" || g.client == nil {
		return nil
	}
	return g.client.LeaveGroup(g.groupID, g.memberID)
}

// Assignment is the current (topic, partition) list.
func (g *GroupConsumer) Assignment() []Assignment {
	if g == nil {
		return nil
	}
	return copyAssignment(g.assignment)
}

// LastRevoked is partitions dropped on the most recent join/rebalance.
func (g *GroupConsumer) LastRevoked() []Assignment {
	if g == nil {
		return nil
	}
	return copyAssignment(g.lastRevoked)
}

// MemberID is the broker-assigned member id.
func (g *GroupConsumer) MemberID() string {
	if g == nil {
		return ""
	}
	return g.memberID
}

// Generation is the current group generation.
func (g *GroupConsumer) Generation() uint32 {
	if g == nil {
		return 0
	}
	return g.generation
}

// GroupID is the consumer group id.
func (g *GroupConsumer) GroupID() string {
	if g == nil {
		return ""
	}
	return g.groupID
}

// GroupInstanceID is the Phase 12 static membership id (empty = dynamic).
func (g *GroupConsumer) GroupInstanceID() string {
	if g == nil {
		return ""
	}
	return g.groupInstanceID
}

// Positions returns next-read offsets for assigned partitions.
func (g *GroupConsumer) Positions() []Position {
	if g == nil || len(g.positions) == 0 {
		return nil
	}
	out := make([]Position, 0, len(g.positions))
	for tp, off := range g.positions {
		out = append(out, Position{Topic: tp.topic, Partition: tp.partition, Offset: off})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

func needsRebalance(err error) bool {
	var be *codec.BrokerError
	if !errors.As(err, &be) || be == nil {
		return false
	}
	return be.Code == RebalanceInProgress || be.Code == UnknownMemberID || be.Code == IllegalGeneration
}

func copyAssignment(in []Assignment) []Assignment {
	if len(in) == 0 {
		return nil
	}
	out := make([]Assignment, len(in))
	copy(out, in)
	return out
}

func assignmentSet(in []Assignment) map[topicPartition]struct{} {
	out := make(map[topicPartition]struct{}, len(in))
	for _, a := range in {
		out[topicPartition{a.Topic, a.Partition}] = struct{}{}
	}
	return out
}
