package volant

import (
	"errors"
	"fmt"
	"sort"
	"sync"
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

const (
	assignorBroker = "broker"
	assignorRange  = "range"
	resetEarliest  = "earliest"
	resetLatest    = "latest"
	resetNone      = "none"
)

// GroupConsumerOption configures [JoinGroupConsumer].
type GroupConsumerOption func(*groupConsumerOptions)

type groupConsumerOptions struct {
	backgroundHeartbeat bool
	instanceID          string
	assignor            string
	autoCommit          bool
	autoCommitInterval  time.Duration
	autoOffsetReset     string
	fetchMaxMessages    uint32
	fetchMaxBytes       uint32
}

const (
	pollMaxMessages = 100
	pollMaxBytes    = 4 * 1024 * 1024
)

func clampFetchMaxMessages(n int) uint32 {
	if n <= 0 {
		return pollMaxMessages
	}
	return uint32(n)
}

func clampFetchMaxBytes(n int) uint32 {
	if n <= 0 {
		return pollMaxBytes
	}
	return uint32(n)
}

// WithFetchMaxMessages bounds each assigned Fetch inside Poll
// (default 100). Values <= 0 clamp to 100. Not Kafka max.poll.records.
func WithFetchMaxMessages(n int) GroupConsumerOption {
	return func(o *groupConsumerOptions) {
		o.fetchMaxMessages = clampFetchMaxMessages(n)
	}
}

// WithFetchMaxBytes bounds each assigned Fetch inside Poll
// (default 4MiB). Values <= 0 clamp to 4MiB.
func WithFetchMaxBytes(n int) GroupConsumerOption {
	return func(o *groupConsumerOptions) {
		o.fetchMaxBytes = clampFetchMaxBytes(n)
	}
}

// WithAutoCommit enables offset auto-commit after a successful Poll that
// returned records. interval 0 commits after every such Poll; interval > 0
// commits on the first successful Poll, then when at least interval has
// elapsed since the last auto or explicit Commit. Default is off (explicit
// Commit only). Not Kafka enable.auto.commit (no background commit goroutine).
func WithAutoCommit(interval time.Duration) GroupConsumerOption {
	return func(o *groupConsumerOptions) {
		o.autoCommit = true
		if interval < 0 {
			interval = 0
		}
		o.autoCommitInterval = interval
	}
}

// WithAssignor selects the fetch-set assignor: "broker" (default, honor
// JoinGroup) or "range" (local range over DescribeGroup members; still
// no SyncGroup). Empty is "broker". Unknown values fail JoinGroupConsumer.
func WithAssignor(name string) GroupConsumerOption {
	return func(o *groupConsumerOptions) {
		o.assignor = name
	}
}

func normalizeAssignor(name string) (string, error) {
	switch name {
	case "", assignorBroker:
		return assignorBroker, nil
	case assignorRange:
		return assignorRange, nil
	default:
		return "", fmt.Errorf("unknown assignor %q", name)
	}
}

// WithAutoOffsetReset selects the fetch position when OffsetFetch is
// missing or OFFSET_UNKNOWN: "earliest" (default, ListOffsets earliest),
// "latest" (ListOffsets latest / LEO), or "none" (error).
// Empty is "earliest". Unknown values fail Join.
func WithAutoOffsetReset(name string) GroupConsumerOption {
	return func(o *groupConsumerOptions) {
		o.autoOffsetReset = name
	}
}

func normalizeAutoOffsetReset(name string) (string, error) {
	switch name {
	case "", resetEarliest:
		return resetEarliest, nil
	case resetLatest:
		return resetLatest, nil
	case resetNone:
		return resetNone, nil
	default:
		return "", fmt.Errorf("unknown auto_offset_reset %q", name)
	}
}

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

const (
	hbIntervalMin = 100 * time.Millisecond
	hbIntervalMax = 3000 * time.Millisecond
)

// HeartbeatInterval is sessionTimeoutMs/3, clamped to [100ms, 3000ms].
func HeartbeatInterval(sessionTimeoutMs int) time.Duration {
	if sessionTimeoutMs < 0 {
		sessionTimeoutMs = 0
	}
	d := time.Duration(sessionTimeoutMs) * time.Millisecond / 3
	if d < hbIntervalMin {
		return hbIntervalMin
	}
	if d > hbIntervalMax {
		return hbIntervalMax
	}
	return d
}

// WithBackgroundHeartbeat enables or disables the post-join heartbeat
// goroutine. Default is true. Pass false to keep v0.32 poll-only heartbeats.
func WithBackgroundHeartbeat(enabled bool) GroupConsumerOption {
	return func(o *groupConsumerOptions) {
		o.backgroundHeartbeat = enabled
	}
}

// GroupConsumer joins a group, polls assigned partitions, and commits.
//
// After a successful join a background goroutine heartbeats at
// HeartbeatInterval so a silent consumer does not expire. Poll, Commit,
// rejoin, and that loop share an internal mutex around join state and
// GroupConsumer RPCs, but this is not a fully concurrent consumer: do
// not call Poll/Commit from multiple goroutines, and do not use the
// same Client for other RPCs while the consumer is open.
//
// The Client must stay open for the lifetime of the consumer; Close
// stops the heartbeat goroutine, leaves the group, and does not close
// the Client.
type GroupConsumer struct {
	client              *Client
	groupID             string
	topics              []string
	sessionTimeoutMs    uint32
	memberID            string
	groupInstanceID     string
	generation          uint32
	assignment          []Assignment
	lastRevoked         []Assignment
	positions           map[topicPartition]uint64
	closed              bool
	backgroundHeartbeat bool
	assignor            string
	autoCommit          bool
	autoCommitInterval  time.Duration
	autoOffsetReset     string
	fetchMaxMessages    uint32
	fetchMaxBytes       uint32
	lastAutoCommit      time.Time
	dirty               bool

	mu       sync.Mutex
	stop     chan struct{}
	stopOnce sync.Once
	hbDone   chan struct{}
}

// JoinGroupConsumer joins a consumer group on the given topics.
// sessionTimeoutMs 0 defaults to 10000. Dynamic membership (empty instance
// id). Background heartbeat is on unless WithBackgroundHeartbeat(false)
// is passed.
func JoinGroupConsumer(c *Client, group string, topics []string, sessionTimeoutMs int, opts ...GroupConsumerOption) (*GroupConsumer, error) {
	o := groupConsumerOptions{backgroundHeartbeat: true, assignor: assignorBroker}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if _, err := normalizeAssignor(o.assignor); err != nil {
		return nil, err
	}
	if _, err := normalizeAutoOffsetReset(o.autoOffsetReset); err != nil {
		return nil, err
	}
	return joinGroupConsumer(c, group, topics, sessionTimeoutMs, o)
}

// JoinGroupConsumerStatic joins with Phase 12 static membership.
// Empty groupInstanceID is dynamic (same as JoinGroupConsumer).
// Re-join after error 9/10/11 resends the same instance id.
func JoinGroupConsumerStatic(c *Client, group string, topics []string, sessionTimeoutMs int, groupInstanceID string, opts ...GroupConsumerOption) (*GroupConsumer, error) {
	o := groupConsumerOptions{backgroundHeartbeat: true, instanceID: groupInstanceID, assignor: assignorBroker}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	o.instanceID = groupInstanceID
	return joinGroupConsumer(c, group, topics, sessionTimeoutMs, o)
}

func joinGroupConsumer(c *Client, group string, topics []string, sessionTimeoutMs int, o groupConsumerOptions) (*GroupConsumer, error) {
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
	assignor, err := normalizeAssignor(o.assignor)
	if err != nil {
		return nil, err
	}
	reset, err := normalizeAutoOffsetReset(o.autoOffsetReset)
	if err != nil {
		return nil, err
	}
	fetchMaxMessages := o.fetchMaxMessages
	if fetchMaxMessages == 0 {
		fetchMaxMessages = pollMaxMessages
	}
	fetchMaxBytes := o.fetchMaxBytes
	if fetchMaxBytes == 0 {
		fetchMaxBytes = pollMaxBytes
	}
	g := &GroupConsumer{
		client:              c,
		groupID:             group,
		topics:              append([]string(nil), topics...),
		sessionTimeoutMs:    timeout,
		groupInstanceID:     o.instanceID,
		positions:           make(map[topicPartition]uint64),
		backgroundHeartbeat: o.backgroundHeartbeat,
		assignor:            assignor,
		autoCommit:          o.autoCommit,
		autoCommitInterval:  o.autoCommitInterval,
		autoOffsetReset:     reset,
		fetchMaxMessages:    fetchMaxMessages,
		fetchMaxBytes:       fetchMaxBytes,
		stop:                make(chan struct{}),
		hbDone:              make(chan struct{}),
	}
	if err := g.doJoin(); err != nil {
		close(g.hbDone)
		return nil, err
	}
	if g.backgroundHeartbeat {
		go g.heartbeatLoop()
	} else {
		close(g.hbDone)
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
	if g.assignor == assignorRange {
		local, err := g.localRangeAssignment()
		if err != nil {
			return err
		}
		newAssignment = local
	}

	oldSet := assignmentSet(previous)
	newSet := assignmentSet(newAssignment)

	var revoked []Assignment
	for tp := range oldSet {
		if _, ok := newSet[tp]; !ok {
			revoked = append(revoked, Assignment{Topic: tp.topic, Partition: tp.partition})
		}
	}
	if g.assignor != assignorRange {
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

	var missing []topicPartition
	for _, a := range g.assignment {
		tp := topicPartition{a.Topic, a.Partition}
		if _, ok := g.positions[tp]; !ok {
			missing = append(missing, tp)
		}
	}
	return g.applyReset(missing)
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
	found := make(map[topicPartition]uint64, len(fetched))
	for _, e := range fetched {
		found[topicPartition{e.Topic, e.Partition}] = e.Offset
	}
	var unknown []topicPartition
	for _, p := range partitions {
		off, ok := found[p]
		if !ok || off == offsetUnknown {
			unknown = append(unknown, p)
			continue
		}
		g.positions[p] = off
	}
	return g.applyReset(unknown)
}

func (g *GroupConsumer) applyReset(partitions []topicPartition) error {
	if len(partitions) == 0 {
		return nil
	}
	switch g.autoOffsetReset {
	case resetNone:
		p := partitions[0]
		return fmt.Errorf("no committed offset for %s-%d and auto_offset_reset=%q", p.topic, p.partition, resetNone)
	case resetEarliest, resetLatest:
		byTopic := make(map[string][]uint32)
		for _, p := range partitions {
			byTopic[p.topic] = append(byTopic[p.topic], p.partition)
		}
		for topic, parts := range byTopic {
			listings, err := g.client.ListOffsets(topic, parts)
			if err != nil {
				return err
			}
			got := make(map[uint32]uint64, len(listings))
			for _, e := range listings {
				if g.autoOffsetReset == resetEarliest {
					got[e.Partition] = e.Earliest
				} else {
					got[e.Partition] = e.Latest
				}
			}
			for _, part := range parts {
				off, ok := got[part]
				if !ok {
					return fmt.Errorf("list_offsets missing partition %s-%d", topic, part)
				}
				g.positions[topicPartition{topic, part}] = off
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown auto_offset_reset %q", g.autoOffsetReset)
	}
}

func (g *GroupConsumer) rangeMembersFromDescribe() (ids []string, topics [][]string) {
	desc, err := g.client.DescribeGroup(g.groupID)
	if err != nil {
		return nil, nil
	}
	seen := false
	for _, m := range desc.Members {
		ids = append(ids, m.MemberID)
		topics = append(topics, append([]string(nil), m.Topics...))
		if m.MemberID == g.memberID {
			seen = true
		}
	}
	if !seen {
		ids = append(ids, g.memberID)
		topics = append(topics, append([]string(nil), g.topics...))
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if id == g.memberID {
			return ids, topics
		}
	}
	return nil, nil
}

func (g *GroupConsumer) localRangeAssignment() ([]Assignment, error) {
	meta, err := g.client.Metadata()
	if err != nil {
		return nil, err
	}
	counts := make(map[string]uint32, len(meta.Topics))
	for _, t := range meta.Topics {
		counts[t.Name] = uint32(len(t.Partitions))
	}
	ids, memberTopics := g.rangeMembersFromDescribe()
	if len(ids) == 0 {
		ids = []string{g.memberID}
		memberTopics = [][]string{append([]string(nil), g.topics...)}
	}
	assigned := RangeAssignMulti(ids, memberTopics, counts)
	if len(assigned) == 0 {
		return nil, nil
	}
	idx := -1
	for i, id := range ids {
		if id == g.memberID {
			idx = i
			break
		}
	}
	if idx < 0 || idx >= len(assigned) {
		solo := RangeAssignMulti([]string{g.memberID}, [][]string{append([]string(nil), g.topics...)}, counts)
		if len(solo) == 0 {
			return nil, nil
		}
		return solo[0], nil
	}
	return assigned[idx], nil
}

// Poll heartbeats, rejoins on error 9/10/11, and fetches assigned partitions.
//
// timeout is the Fetch max-wait budget for this call (0 = non-blocking).
// One heartbeat plus one fetch pass; remaining time is used as max_wait_ms
// on each partition fetch until it runs out.
func (g *GroupConsumer) Poll(timeout time.Duration) ([]FetchedRecord, error) {
	if g == nil {
		return nil, ErrGroupClosed
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
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
	maxMessages := g.fetchMaxMessages
	if maxMessages == 0 {
		maxMessages = pollMaxMessages
	}
	maxBytes := g.fetchMaxBytes
	if maxBytes == 0 {
		maxBytes = pollMaxBytes
	}
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
		recs, err := g.client.FetchOpts(a.Topic, int(a.Partition), int64(from), maxMessages, maxBytes, maxWait)
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
	if len(out) > 0 {
		g.dirty = true
		if err := g.maybeAutoCommit(); err != nil {
			return out, err
		}
	}
	return out, nil
}

// Commit commits last+1 positions for assigned partitions with
// member_id + generation (not the admin empty-member path).
func (g *GroupConsumer) Commit() error {
	if g == nil {
		return ErrGroupClosed
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrGroupClosed
	}
	return g.commitLocked()
}

func (g *GroupConsumer) commitLocked() error {
	if len(g.positions) == 0 {
		g.lastAutoCommit = time.Now()
		g.dirty = false
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
	if err := g.client.CommitOffsets(g.groupID, g.memberID, g.generation, entries); err != nil {
		return err
	}
	g.lastAutoCommit = time.Now()
	g.dirty = false
	return nil
}

func (g *GroupConsumer) maybeAutoCommit() error {
	if !g.autoCommit {
		return nil
	}
	now := time.Now()
	if g.autoCommitInterval > 0 && !g.lastAutoCommit.IsZero() {
		if now.Sub(g.lastAutoCommit) < g.autoCommitInterval {
			return nil
		}
	}
	return g.commitLocked()
}

// Close stops the heartbeat goroutine (if any) then leaves the group.
// The Client is left open. Idempotent. Auto-commit on + uncommitted
// positions: best-effort commit once (error ignored), then LeaveGroup.
func (g *GroupConsumer) Close() error {
	if g == nil {
		return nil
	}
	g.stopOnce.Do(func() {
		if g.stop != nil {
			close(g.stop)
		}
	})
	if g.hbDone != nil {
		<-g.hbDone
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil
	}
	if g.autoCommit && g.dirty {
		_ = g.commitLocked()
	}
	g.closed = true
	if g.memberID == "" || g.client == nil {
		return nil
	}
	return g.client.LeaveGroup(g.groupID, g.memberID)
}

func (g *GroupConsumer) heartbeatLoop() {
	defer close(g.hbDone)
	ticker := time.NewTicker(HeartbeatInterval(int(g.sessionTimeoutMs)))
	defer ticker.Stop()
	for {
		select {
		case <-g.stop:
			return
		case <-ticker.C:
			g.heartbeatOnce()
		}
	}
}

func (g *GroupConsumer) heartbeatOnce() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	err := g.client.Heartbeat(g.groupID, g.memberID, g.generation)
	if err == nil {
		return
	}
	if needsRebalance(err) {
		_ = g.doJoin()
	}
}

// Assignment is the current (topic, partition) list.
func (g *GroupConsumer) Assignment() []Assignment {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return copyAssignment(g.assignment)
}

// LastRevoked is partitions dropped on the most recent join/rebalance.
func (g *GroupConsumer) LastRevoked() []Assignment {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return copyAssignment(g.lastRevoked)
}

// MemberID is the broker-assigned member id.
func (g *GroupConsumer) MemberID() string {
	if g == nil {
		return ""
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.memberID
}

// Generation is the current group generation.
func (g *GroupConsumer) Generation() uint32 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
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

// AutoOffsetReset is the join-time reset policy (earliest / latest / none).
func (g *GroupConsumer) AutoOffsetReset() string {
	if g == nil {
		return ""
	}
	return g.autoOffsetReset
}

// FetchMaxMessages is the Poll fetch max_messages (default 100).
func (g *GroupConsumer) FetchMaxMessages() uint32 {
	if g == nil {
		return 0
	}
	return g.fetchMaxMessages
}

// FetchMaxBytes is the Poll fetch max_bytes (default 4MiB).
func (g *GroupConsumer) FetchMaxBytes() uint32 {
	if g == nil {
		return 0
	}
	return g.fetchMaxBytes
}

// Positions returns next-read offsets for assigned partitions.
func (g *GroupConsumer) Positions() []Position {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.positions) == 0 {
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
