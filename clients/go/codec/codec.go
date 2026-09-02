// Package codec encodes and decodes little-endian native payloads.
//
// Matches crates/volant-protocol/src/payload.rs for Produce, Fetch,
// CreateTopic, Metadata, DeleteTopic, OffsetCommit, OffsetFetch,
// JoinGroup, Heartbeat, LeaveGroup, Auth, DescribeGroup, ListGroups, CreatePartitions, ListOffsets, CreateAcls, DeleteAcls, ListAcls, InitProducerId, and Scram.
// Frame headers are big-endian (see package frame); payload integers and
// length prefixes are little-endian.
package codec

import (
	"encoding/binary"
	"fmt"

	"github.com/volant-mq/volant/clients/go/frame"
)

const (
	OpProduce               uint16 = 1
	OpFetch                 uint16 = 2
	OpCreateTopic           uint16 = 3
	OpMetadata              uint16 = 4
	OpDeleteTopic           uint16 = 5
	OpOffsetCommit          uint16 = 6
	OpOffsetFetch           uint16 = 7
	OpJoinGroup             uint16 = 8
	OpHeartbeat             uint16 = 9
	OpLeaveGroup            uint16 = 10
	OpAuth                  uint16 = 30
	OpAuthResponse          uint16 = 31
	OpInitProducerId        uint16 = 32
	OpInitProducerIdResponse uint16 = 33
	OpScramFirst            uint16 = 60
	OpScramFirstResponse    uint16 = 61
	OpScramFinal            uint16 = 62
	OpScramFinalResponse    uint16 = 63
	OpDescribeGroup         uint16 = 34
	OpDescribeGroupResponse uint16 = 35
	OpListGroups                 uint16 = 36
	OpListGroupsResponse         uint16 = 37
	OpDeleteOffsets              uint16 = 38
	OpDeleteOffsetsResponse      uint16 = 39
	OpDescribeConfigs            uint16 = 40
	OpDescribeConfigsResponse    uint16 = 41
	OpAlterConfigs               uint16 = 42
	OpAlterConfigsResponse       uint16 = 43
	OpDeleteRecords              uint16 = 44
	OpDeleteRecordsResponse      uint16 = 45
	OpCreatePartitions           uint16 = 46
	OpCreatePartitionsResponse   uint16 = 47
	OpListOffsets                uint16 = 48
	OpListOffsetsResponse        uint16 = 49
	OpCreateAcls                 uint16 = 54
	OpCreateAclsResponse         uint16 = 55
	OpDeleteAcls                 uint16 = 56
	OpDeleteAclsResponse         uint16 = 57
	OpListAcls                   uint16 = 58
	OpListAclsResponse           uint16 = 59
	OpCreateScramUser            uint16 = 64
	OpCreateScramUserResponse    uint16 = 65
	OpDeleteScramUser            uint16 = 66
	OpDeleteScramUserResponse    uint16 = 67
	OpListScramUsers             uint16 = 68
	OpListScramUsersResponse     uint16 = 69
	OpReassignPartitions         uint16 = 114
	OpReassignPartitionsResponse uint16 = 115
	OpError                 uint16 = 0xFFFF

	nullLen = 0xFFFFFFFF

	// ReassignAllPartitions is the ReassignPartitions.partition sentinel
	// (u32::MAX): apply to every partition of the topic.
	ReassignAllPartitions uint32 = 0xFFFFFFFF
)

// BrokerError is a non-zero broker error_code or Error opcode.
type BrokerError struct {
	Code    uint16
	Message string
	Op      string
}

func (e *BrokerError) Error() string {
	prefix := ""
	if e.Op != "" {
		prefix = e.Op + ": "
	}
	detail := e.Message
	if detail == "" {
		detail = fmt.Sprintf("error_code=%d", e.Code)
	}
	return fmt.Sprintf("%s%s (code=%d)", prefix, detail, e.Code)
}

type writer struct {
	buf []byte
}

func (w *writer) u8(v uint8) { w.buf = append(w.buf, v) }

func (w *writer) u16(v uint16) {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	w.buf = append(w.buf, b[:]...)
}

func (w *writer) u32(v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	w.buf = append(w.buf, b[:]...)
}

func (w *writer) i32(v int32) { w.u32(uint32(v)) }

func (w *writer) u64(v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	w.buf = append(w.buf, b[:]...)
}

func (w *writer) i64(v int64) { w.u64(uint64(v)) }

func (w *writer) raw(b []byte) { w.buf = append(w.buf, b...) }

type reader struct {
	data []byte
	i    int
}

func (r *reader) remaining() int { return len(r.data) - r.i }

func (r *reader) need(n int, msg string) error {
	if r.remaining() < n {
		return &frame.ProtocolError{Msg: msg}
	}
	return nil
}

func (r *reader) u8() (uint8, error) {
	if err := r.need(1, "truncated u8"); err != nil {
		return 0, err
	}
	v := r.data[r.i]
	r.i++
	return v, nil
}

func (r *reader) u16() (uint16, error) {
	if err := r.need(2, "truncated u16"); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint16(r.data[r.i:])
	r.i += 2
	return v, nil
}

func (r *reader) u32() (uint32, error) {
	if err := r.need(4, "truncated u32"); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint32(r.data[r.i:])
	r.i += 4
	return v, nil
}

func (r *reader) i32() (int32, error) {
	v, err := r.u32()
	return int32(v), err
}

func (r *reader) u64() (uint64, error) {
	if err := r.need(8, "truncated u64"); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint64(r.data[r.i:])
	r.i += 8
	return v, nil
}

func (r *reader) i64() (int64, error) {
	v, err := r.u64()
	return int64(v), err
}

func (r *reader) take(n int, msg string) ([]byte, error) {
	if err := r.need(n, msg); err != nil {
		return nil, err
	}
	out := make([]byte, n)
	copy(out, r.data[r.i:r.i+n])
	r.i += n
	return out, nil
}

func putString(w *writer, s string) error {
	raw := []byte(s)
	if len(raw) > 0xFFFF {
		return &frame.ProtocolError{Msg: fmt.Sprintf("string too long: %d bytes", len(raw))}
	}
	w.u16(uint16(len(raw)))
	w.raw(raw)
	return nil
}

func getString(r *reader) (string, error) {
	n, err := r.u16()
	if err != nil {
		return "", err
	}
	raw, err := r.take(int(n), "truncated string body")
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func putAclBinding(w *writer, e AclBinding) error {
	if err := putString(w, e.Principal); err != nil {
		return err
	}
	w.u8(e.ResourceType)
	if err := putString(w, e.Resource); err != nil {
		return err
	}
	w.u8(e.Operation)
	w.u8(e.Permission)
	return nil
}

func getAclBinding(r *reader) (AclBinding, error) {
	principal, err := getString(r)
	if err != nil {
		return AclBinding{}, err
	}
	resourceType, err := r.u8()
	if err != nil {
		return AclBinding{}, err
	}
	resource, err := getString(r)
	if err != nil {
		return AclBinding{}, err
	}
	operation, err := r.u8()
	if err != nil {
		return AclBinding{}, err
	}
	permission, err := r.u8()
	if err != nil {
		return AclBinding{}, err
	}
	return AclBinding{
		Principal:    principal,
		ResourceType: resourceType,
		Resource:     resource,
		Operation:    operation,
		Permission:   permission,
	}, nil
}

func putBytes(w *writer, b []byte) {
	if b == nil {
		b = []byte{}
	}
	w.u32(uint32(len(b)))
	w.raw(b)
}

func getBytes(r *reader) ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if n == nullLen {
		return nil, &frame.ProtocolError{Msg: "unexpected optional null in required bytes"}
	}
	return r.take(int(n), "truncated bytes body")
}

func putOptionalBytes(w *writer, b []byte) {
	if b == nil {
		w.u32(nullLen)
		return
	}
	putBytes(w, b)
}

func getOptionalBytes(r *reader) ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if n == nullLen {
		return nil, nil
	}
	return r.take(int(n), "truncated optional bytes body")
}

// Header is a single produce/fetch record header.
type Header struct {
	Name  string
	Value []byte
}

func putHeaders(w *writer, headers []Header) error {
	w.u32(uint32(len(headers)))
	for _, h := range headers {
		if err := putString(w, h.Name); err != nil {
			return err
		}
		putBytes(w, h.Value)
	}
	return nil
}

func getHeaders(r *reader) ([]Header, error) {
	count, err := r.u32()
	if err != nil {
		return nil, err
	}
	out := make([]Header, 0, count)
	for i := uint32(0); i < count; i++ {
		name, err := getString(r)
		if err != nil {
			return nil, err
		}
		value, err := getBytes(r)
		if err != nil {
			return nil, err
		}
		out = append(out, Header{Name: name, Value: value})
	}
	return out, nil
}

// ProduceMessage is one record in a Produce request.
type ProduceMessage struct {
	Key         []byte // nil = null
	Value       []byte
	TimestampMs int64
	Headers     []Header
}

// ProduceRequest is the Produce opcode body.
type ProduceRequest struct {
	Topic         string
	Partition     int32
	Acks          uint8
	Messages      []ProduceMessage
	ProducerID    uint64
	ProducerEpoch uint16
	BaseSequence  int32
}

// ProduceResponse is the Produce opcode reply.
type ProduceResponse struct {
	Topic      string
	Partition  uint32
	BaseOffset uint64
	Count      uint32
	ErrorCode  uint16
}

// FetchRequest is the Fetch opcode body.
type FetchRequest struct {
	Topic       string
	Partition   uint32
	FromOffset  uint64
	MaxMessages uint32
	MaxBytes    uint32
	MaxWaitMs   uint32
}

// FetchRecord is one record in a Fetch response.
type FetchRecord struct {
	Offset      uint64
	TimestampMs int64
	Key         []byte // nil = null
	Value       []byte
	Headers     []Header
}

// FetchResponse is the Fetch opcode reply.
type FetchResponse struct {
	Topic         string
	Partition     uint32
	HighWatermark uint64
	ErrorCode     uint16
	Records       []FetchRecord
}

// CreateTopicRequest is the CreateTopic opcode body.
type CreateTopicRequest struct {
	Name       string
	Partitions uint32
	Configs    [][2]string
}

// CreateTopicResponse is the CreateTopic opcode reply.
type CreateTopicResponse struct {
	TopicID    uint32
	Name       string
	Partitions uint32
	ErrorCode  uint16
}

// DeleteTopicRequest is the DeleteTopic opcode body.
type DeleteTopicRequest struct {
	Name string
}

// DeleteTopicResponse is the DeleteTopic opcode reply.
type DeleteTopicResponse struct {
	Name      string
	ErrorCode uint16
}

// MetadataRequest is the Metadata opcode body. Empty Topics means all topics.
type MetadataRequest struct {
	Topics []string
}

// BrokerInfo is one broker in a Metadata response.
type BrokerInfo struct {
	NodeID uint32
	Host   string
	Port   uint16
}

// PartitionInfo is one partition in a Metadata topic.
type PartitionInfo struct {
	PartitionID uint32
	Leader      uint32
	HWM         uint64
	Replicas    []uint32
	ISR         []uint32
	LeaderEpoch uint32
}

// TopicInfo is one topic in a Metadata response.
type TopicInfo struct {
	Name       string
	TopicID    uint32
	ErrorCode  uint16
	Partitions []PartitionInfo
}

// MetadataResponse is the Metadata opcode reply.
type MetadataResponse struct {
	Brokers []BrokerInfo
	Topics  []TopicInfo
}

// ErrorResponse is the Error opcode body.
type ErrorResponse struct {
	Code    uint16
	Message string
}

// OffsetCommitEntry is one topic/partition commit in an OffsetCommit request.
type OffsetCommitEntry struct {
	Topic     string
	Partition uint32
	Offset    uint64
	Metadata  string
}

// OffsetCommitRequest is the OffsetCommit opcode body.
type OffsetCommitRequest struct {
	GroupID    string
	MemberID   string
	Generation uint32
	Entries    []OffsetCommitEntry
}

// OffsetCommitResponse is the OffsetCommit opcode reply.
type OffsetCommitResponse struct {
	ErrorCode uint16
}

// OffsetEntry is one topic/partition selector in an OffsetFetch request.
type OffsetEntry struct {
	Topic     string
	Partition uint32
}

// OffsetFetchRequest is the OffsetFetch opcode body. Empty Entries means all.
type OffsetFetchRequest struct {
	GroupID string
	Entries []OffsetEntry
}

// OffsetFetchEntry is one committed offset in an OffsetFetch response.
type OffsetFetchEntry struct {
	Topic     string
	Partition uint32
	Offset    uint64
	Metadata  string
}

// OffsetFetchResponse is the OffsetFetch opcode reply.
type OffsetFetchResponse struct {
	ErrorCode uint16
	Entries   []OffsetFetchEntry
}

// Assignment is one topic/partition pair from JoinGroup.
type Assignment struct {
	Topic     string
	Partition uint32
}

// JoinGroupRequest is the JoinGroup opcode body.
type JoinGroupRequest struct {
	GroupID          string
	MemberID         string
	SessionTimeoutMs uint32
	Topics           []string
	GroupInstanceID  string
}

// JoinGroupResponse is the JoinGroup opcode reply.
type JoinGroupResponse struct {
	ErrorCode  uint16
	Generation uint32
	MemberID   string
	Assignment []Assignment
	Revoked    []Assignment
}

// HeartbeatRequest is the Heartbeat opcode body.
type HeartbeatRequest struct {
	GroupID    string
	MemberID   string
	Generation uint32
}

// HeartbeatResponse is the Heartbeat opcode reply.
type HeartbeatResponse struct {
	ErrorCode uint16
}

// LeaveGroupRequest is the LeaveGroup opcode body.
type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

// LeaveGroupResponse is the LeaveGroup opcode reply.
type LeaveGroupResponse struct {
	ErrorCode uint16
}

// AuthRequest is the Auth opcode (30) body: one put_string token.
type AuthRequest struct {
	Token string
}

// AuthResponse is the Auth reply (opcode 31).
type AuthResponse struct {
	ErrorCode uint16
}

type InitProducerIdRequest struct {
	TransactionalID string
}

// InitProducerIdResponse is the InitProducerId reply (opcode 33).
type InitProducerIdResponse struct {
	ProducerID uint64
	Epoch      uint16
	ErrorCode  uint16
}
// GroupState is the ListGroups state byte (Phase 12).
type GroupState uint8

type ScramFirstRequest struct {
	Username    string
	ClientNonce string
}

type ScramFirstResponse struct {
	ErrorCode     uint16
	CombinedNonce string
	Salt          []byte
	Iterations    uint32
}

type ScramFinalRequest struct {
	Username      string
	CombinedNonce string
	ClientProof   []byte
}

type ScramFinalResponse struct {
	ErrorCode       uint16
	ServerSignature []byte
}

const (
	// GroupStateEmpty is offsets on disk only; no live members.
	GroupStateEmpty GroupState = 0
	// GroupStateStable is at least one live member.
	GroupStateStable GroupState = 1
)

// GroupStateFromU8 maps the wire byte (unknown values decode as Empty).
func GroupStateFromU8(v uint8) GroupState {
	if v == 1 {
		return GroupStateStable
	}
	return GroupStateEmpty
}

// GroupListing is one group in a ListGroups response.
type GroupListing struct {
	GroupID     string
	State       GroupState
	MemberCount uint32
	Generation  uint32
}

// GroupMemberInfo is one member in a DescribeGroup response.
type GroupMemberInfo struct {
	MemberID   string
	Topics     []string
	Assignment []Assignment
}

// DescribeGroupRequest is the DescribeGroup opcode (34) body.
type DescribeGroupRequest struct {
	GroupID string
}

// DescribeGroupResponse is the DescribeGroup reply (opcode 35).
type DescribeGroupResponse struct {
	ErrorCode  uint16
	GroupID    string
	Generation uint32
	Members    []GroupMemberInfo
}

// ListGroupsResponse is the ListGroups reply (opcode 37).
type ListGroupsResponse struct {
	ErrorCode uint16
	Groups    []GroupListing
}

// OffsetListing is one partition earliest/latest pair from ListOffsets
// (Phase 15 / v0.50).
type OffsetListing struct {
	Partition uint32
	Earliest  uint64
	Latest    uint64
}

// ListOffsetsRequest is the ListOffsets opcode (48) body.
// Empty Partitions means all partitions of the topic.
type ListOffsetsRequest struct {
	Topic      string
	Partitions []uint32
}

// ListOffsetsResponse is the ListOffsets reply (opcode 49).
type ListOffsetsResponse struct {
	ErrorCode uint16
	Topic     string
	Entries   []OffsetListing
}

// AclBinding is one ACL entry on the wire (Phase 20 / v0.56).
// ResourceType: 0=Topic, 1=Group, 2=Cluster.
// Operation: 0=All … 7=ClusterAction.
// Permission: 0=Deny, 1=Allow.
type AclBinding struct {
	Principal    string
	ResourceType uint8
	Resource     string
	Operation    uint8
	Permission   uint8
}

// CreateAclsRequest is the CreateAcls opcode (54) body.
type CreateAclsRequest struct {
	Entries []AclBinding
}

// CreateAclsResponse is the CreateAcls reply (opcode 55).
type CreateAclsResponse struct {
	ErrorCode uint16
}

// DeleteAclsRequest is the DeleteAcls opcode (56) body (exact-match).
type DeleteAclsRequest struct {
	Entries []AclBinding
}

// DeleteAclsResponse is the DeleteAcls reply (opcode 57).
type DeleteAclsResponse struct {
	ErrorCode uint16
	Removed   uint32
}

// ListAclsRequest is the ListAcls opcode (58) body.
// Empty Principal/Resource = any. ResourceType 255 = any type.
type ListAclsRequest struct {
	Principal    string
	ResourceType uint8
	Resource     string
}

// ListAclsResponse is the ListAcls reply (opcode 59).
type ListAclsResponse struct {
	ErrorCode uint16
	Entries   []AclBinding
}

// CreatePartitionsRequest is the CreatePartitions opcode (46) body.
// TotalCount is the desired total partition count (must exceed current).
// DeleteRecordsRequest is the DeleteRecords opcode (44) body.
// WaitMajority is the Phase 137 trailer: 0 = broker default, 1 = force
// wait, 2 = force no-wait. Encode always writes it; decode treats a
// missing trailer as 0.
// DescribeConfigsRequest is the DescribeConfigs opcode (40) body.
// DeleteOffsetsRequest is the DeleteOffsets opcode (38) body.
// Empty Entries deletes all offsets for the group.
type CreateScramUserRequest struct {
	Username   string
	Password   string
	Iterations uint32
}

type CreateScramUserResponse struct {
	ErrorCode uint16
}

type DeleteScramUserRequest struct {
	Username string
}

type DeleteScramUserResponse struct {
	ErrorCode uint16
}

type ListScramUsersResponse struct {
	ErrorCode uint16
	Usernames []string
}

type DeleteOffsetsRequest struct {
	GroupID string
	Entries []OffsetEntry
}

// DeleteOffsetsResponse is the DeleteOffsets reply (opcode 39).
type DeleteOffsetsResponse struct {
	ErrorCode    uint16
	DeletedCount uint32
}

type DescribeConfigsRequest struct {
	Topic string
}

// DescribeConfigsResponse is the DescribeConfigs reply (opcode 41).
type DescribeConfigsResponse struct {
	ErrorCode      uint16
	Topic          string
	TopicID        uint32
	PartitionCount uint32
	Configs        [][2]string
}

// AlterConfigsRequest is the AlterConfigs opcode (42) body.
// Empty value clears that key.
type AlterConfigsRequest struct {
	Topic   string
	Configs [][2]string
}

// AlterConfigsResponse is the AlterConfigs reply (opcode 43).
type AlterConfigsResponse struct {
	ErrorCode uint16
	Topic     string
}

type DeleteRecordsRequest struct {
	Topic        string
	Partition    uint32
	BeforeOffset uint64
	WaitMajority uint8
}

// DeleteRecordsResponse is the DeleteRecords reply (opcode 45).
type DeleteRecordsResponse struct {
	ErrorCode    uint16
	Topic        string
	Partition    uint32
	LowWatermark uint64
}

type CreatePartitionsRequest struct {
	Topic      string
	TotalCount uint32
}

// CreatePartitionsResponse is the CreatePartitions reply (opcode 47).
// Partitions is the new total (0 on error).
type CreatePartitionsResponse struct {
	ErrorCode  uint16
	Topic      string
	Partitions uint32
}

// ReassignPartitionsRequest is the ReassignPartitions opcode (114) body.
// Partition == ReassignAllPartitions (u32::MAX) applies to every partition.
// Empty Replicas means auto-place with current membership.
type ReassignPartitionsRequest struct {
	Topic     string
	Partition uint32
	Replicas  []uint32
}

// ReassignPartitionsResponse is the ReassignPartitions reply (opcode 115).
// Generation is the assignment generation after apply (0 on error).
type ReassignPartitionsResponse struct {
	ErrorCode  uint16
	Generation uint32
}


func EncodeProduceRequest(req ProduceRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Topic); err != nil {
		return nil, err
	}
	w.i32(req.Partition)
	w.u8(req.Acks)
	w.u32(uint32(len(req.Messages)))
	for _, m := range req.Messages {
		putOptionalBytes(w, m.Key)
		putBytes(w, m.Value)
		w.i64(m.TimestampMs)
		if err := putHeaders(w, m.Headers); err != nil {
			return nil, err
		}
	}
	// Phase 10 idempotent trailer (always written by current encoders).
	w.u64(req.ProducerID)
	w.u16(req.ProducerEpoch)
	w.i32(req.BaseSequence)
	return w.buf, nil
}

func DecodeProduceRequest(payload []byte) (ProduceRequest, error) {
	r := &reader{data: payload}
	topic, err := getString(r)
	if err != nil {
		return ProduceRequest{}, err
	}
	partition, err := r.i32()
	if err != nil {
		return ProduceRequest{}, err
	}
	acks, err := r.u8()
	if err != nil {
		return ProduceRequest{}, err
	}
	n, err := r.u32()
	if err != nil {
		return ProduceRequest{}, err
	}
	msgs := make([]ProduceMessage, 0, n)
	for i := uint32(0); i < n; i++ {
		key, err := getOptionalBytes(r)
		if err != nil {
			return ProduceRequest{}, err
		}
		value, err := getBytes(r)
		if err != nil {
			return ProduceRequest{}, err
		}
		ts, err := r.i64()
		if err != nil {
			return ProduceRequest{}, err
		}
		headers, err := getHeaders(r)
		if err != nil {
			return ProduceRequest{}, err
		}
		msgs = append(msgs, ProduceMessage{Key: key, Value: value, TimestampMs: ts, Headers: headers})
	}
	var producerID uint64
	var producerEpoch uint16
	baseSeq := int32(-1)
	if r.remaining() >= 8+2+4 {
		producerID, err = r.u64()
		if err != nil {
			return ProduceRequest{}, err
		}
		producerEpoch, err = r.u16()
		if err != nil {
			return ProduceRequest{}, err
		}
		baseSeq, err = r.i32()
		if err != nil {
			return ProduceRequest{}, err
		}
	}
	return ProduceRequest{
		Topic:         topic,
		Partition:     partition,
		Acks:          acks,
		Messages:      msgs,
		ProducerID:    producerID,
		ProducerEpoch: producerEpoch,
		BaseSequence:  baseSeq,
	}, nil
}

func EncodeProduceResponse(resp ProduceResponse) ([]byte, error) {
	w := &writer{}
	if err := putString(w, resp.Topic); err != nil {
		return nil, err
	}
	w.u32(resp.Partition)
	w.u64(resp.BaseOffset)
	w.u32(resp.Count)
	w.u16(resp.ErrorCode)
	return w.buf, nil
}

func DecodeProduceResponse(payload []byte) (ProduceResponse, error) {
	r := &reader{data: payload}
	topic, err := getString(r)
	if err != nil {
		return ProduceResponse{}, err
	}
	part, err := r.u32()
	if err != nil {
		return ProduceResponse{}, err
	}
	off, err := r.u64()
	if err != nil {
		return ProduceResponse{}, err
	}
	count, err := r.u32()
	if err != nil {
		return ProduceResponse{}, err
	}
	code, err := r.u16()
	if err != nil {
		return ProduceResponse{}, err
	}
	return ProduceResponse{
		Topic:      topic,
		Partition:  part,
		BaseOffset: off,
		Count:      count,
		ErrorCode:  code,
	}, nil
}

func EncodeFetchRequest(req FetchRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Topic); err != nil {
		return nil, err
	}
	w.u32(req.Partition)
	w.u64(req.FromOffset)
	w.u32(req.MaxMessages)
	w.u32(req.MaxBytes)
	w.u32(req.MaxWaitMs)
	return w.buf, nil
}

func DecodeFetchRequest(payload []byte) (FetchRequest, error) {
	r := &reader{data: payload}
	topic, err := getString(r)
	if err != nil {
		return FetchRequest{}, err
	}
	part, err := r.u32()
	if err != nil {
		return FetchRequest{}, err
	}
	off, err := r.u64()
	if err != nil {
		return FetchRequest{}, err
	}
	maxMsg, err := r.u32()
	if err != nil {
		return FetchRequest{}, err
	}
	maxBytes, err := r.u32()
	if err != nil {
		return FetchRequest{}, err
	}
	maxWait, err := r.u32()
	if err != nil {
		return FetchRequest{}, err
	}
	return FetchRequest{
		Topic:       topic,
		Partition:   part,
		FromOffset:  off,
		MaxMessages: maxMsg,
		MaxBytes:    maxBytes,
		MaxWaitMs:   maxWait,
	}, nil
}

func EncodeFetchResponse(resp FetchResponse) ([]byte, error) {
	w := &writer{}
	if err := putString(w, resp.Topic); err != nil {
		return nil, err
	}
	w.u32(resp.Partition)
	w.u64(resp.HighWatermark)
	w.u16(resp.ErrorCode)
	w.u32(uint32(len(resp.Records)))
	for _, rec := range resp.Records {
		w.u64(rec.Offset)
		w.i64(rec.TimestampMs)
		putOptionalBytes(w, rec.Key)
		putBytes(w, rec.Value)
		if err := putHeaders(w, rec.Headers); err != nil {
			return nil, err
		}
	}
	return w.buf, nil
}

func DecodeFetchResponse(payload []byte) (FetchResponse, error) {
	r := &reader{data: payload}
	topic, err := getString(r)
	if err != nil {
		return FetchResponse{}, err
	}
	part, err := r.u32()
	if err != nil {
		return FetchResponse{}, err
	}
	hwm, err := r.u64()
	if err != nil {
		return FetchResponse{}, err
	}
	code, err := r.u16()
	if err != nil {
		return FetchResponse{}, err
	}
	n, err := r.u32()
	if err != nil {
		return FetchResponse{}, err
	}
	recs := make([]FetchRecord, 0, n)
	for i := uint32(0); i < n; i++ {
		off, err := r.u64()
		if err != nil {
			return FetchResponse{}, err
		}
		ts, err := r.i64()
		if err != nil {
			return FetchResponse{}, err
		}
		key, err := getOptionalBytes(r)
		if err != nil {
			return FetchResponse{}, err
		}
		value, err := getBytes(r)
		if err != nil {
			return FetchResponse{}, err
		}
		headers, err := getHeaders(r)
		if err != nil {
			return FetchResponse{}, err
		}
		recs = append(recs, FetchRecord{
			Offset:      off,
			TimestampMs: ts,
			Key:         key,
			Value:       value,
			Headers:     headers,
		})
	}
	return FetchResponse{
		Topic:         topic,
		Partition:     part,
		HighWatermark: hwm,
		ErrorCode:     code,
		Records:       recs,
	}, nil
}

func EncodeCreateTopicRequest(req CreateTopicRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Name); err != nil {
		return nil, err
	}
	w.u32(req.Partitions)
	// Phase 13 config trailer (always written by current encoders).
	w.u32(uint32(len(req.Configs)))
	for _, kv := range req.Configs {
		if err := putString(w, kv[0]); err != nil {
			return nil, err
		}
		if err := putString(w, kv[1]); err != nil {
			return nil, err
		}
	}
	return w.buf, nil
}

func DecodeCreateTopicRequest(payload []byte) (CreateTopicRequest, error) {
	r := &reader{data: payload}
	name, err := getString(r)
	if err != nil {
		return CreateTopicRequest{}, err
	}
	parts, err := r.u32()
	if err != nil {
		return CreateTopicRequest{}, err
	}
	var configs [][2]string
	if r.remaining() >= 4 {
		n, err := r.u32()
		if err != nil {
			return CreateTopicRequest{}, err
		}
		configs = make([][2]string, 0, n)
		for i := uint32(0); i < n; i++ {
			k, err := getString(r)
			if err != nil {
				return CreateTopicRequest{}, err
			}
			v, err := getString(r)
			if err != nil {
				return CreateTopicRequest{}, err
			}
			configs = append(configs, [2]string{k, v})
		}
	}
	if configs == nil {
		configs = [][2]string{}
	}
	return CreateTopicRequest{Name: name, Partitions: parts, Configs: configs}, nil
}

func EncodeCreateTopicResponse(resp CreateTopicResponse) ([]byte, error) {
	w := &writer{}
	w.u32(resp.TopicID)
	if err := putString(w, resp.Name); err != nil {
		return nil, err
	}
	w.u32(resp.Partitions)
	w.u16(resp.ErrorCode)
	return w.buf, nil
}

func DecodeCreateTopicResponse(payload []byte) (CreateTopicResponse, error) {
	r := &reader{data: payload}
	id, err := r.u32()
	if err != nil {
		return CreateTopicResponse{}, err
	}
	name, err := getString(r)
	if err != nil {
		return CreateTopicResponse{}, err
	}
	parts, err := r.u32()
	if err != nil {
		return CreateTopicResponse{}, err
	}
	code, err := r.u16()
	if err != nil {
		return CreateTopicResponse{}, err
	}
	return CreateTopicResponse{TopicID: id, Name: name, Partitions: parts, ErrorCode: code}, nil
}

func EncodeDeleteTopicRequest(req DeleteTopicRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Name); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeDeleteTopicRequest(payload []byte) (DeleteTopicRequest, error) {
	name, err := getString(&reader{data: payload})
	if err != nil {
		return DeleteTopicRequest{}, err
	}
	return DeleteTopicRequest{Name: name}, nil
}

func EncodeDeleteTopicResponse(resp DeleteTopicResponse) ([]byte, error) {
	w := &writer{}
	if err := putString(w, resp.Name); err != nil {
		return nil, err
	}
	w.u16(resp.ErrorCode)
	return w.buf, nil
}

func DecodeDeleteTopicResponse(payload []byte) (DeleteTopicResponse, error) {
	r := &reader{data: payload}
	name, err := getString(r)
	if err != nil {
		return DeleteTopicResponse{}, err
	}
	code, err := r.u16()
	if err != nil {
		return DeleteTopicResponse{}, err
	}
	return DeleteTopicResponse{Name: name, ErrorCode: code}, nil
}

func EncodeMetadataRequest(req MetadataRequest) ([]byte, error) {
	w := &writer{}
	w.u32(uint32(len(req.Topics)))
	for _, t := range req.Topics {
		if err := putString(w, t); err != nil {
			return nil, err
		}
	}
	return w.buf, nil
}

func DecodeMetadataRequest(payload []byte) (MetadataRequest, error) {
	r := &reader{data: payload}
	n, err := r.u32()
	if err != nil {
		return MetadataRequest{}, err
	}
	topics := make([]string, 0, n)
	for i := uint32(0); i < n; i++ {
		s, err := getString(r)
		if err != nil {
			return MetadataRequest{}, err
		}
		topics = append(topics, s)
	}
	return MetadataRequest{Topics: topics}, nil
}

func EncodeMetadataResponse(resp MetadataResponse) ([]byte, error) {
	w := &writer{}
	w.u32(uint32(len(resp.Brokers)))
	for _, b := range resp.Brokers {
		w.u32(b.NodeID)
		if err := putString(w, b.Host); err != nil {
			return nil, err
		}
		w.u16(b.Port)
	}
	w.u32(uint32(len(resp.Topics)))
	for _, t := range resp.Topics {
		if err := putString(w, t.Name); err != nil {
			return nil, err
		}
		w.u32(t.TopicID)
		w.u16(t.ErrorCode)
		w.u32(uint32(len(t.Partitions)))
		for _, p := range t.Partitions {
			w.u32(p.PartitionID)
			w.u32(p.Leader)
			w.u64(p.HWM)
			w.u32(uint32(len(p.Replicas)))
			for _, replica := range p.Replicas {
				w.u32(replica)
			}
			w.u32(uint32(len(p.ISR)))
			for _, replica := range p.ISR {
				w.u32(replica)
			}
			w.u32(p.LeaderEpoch)
		}
	}
	return w.buf, nil
}

func DecodeMetadataResponse(payload []byte) (MetadataResponse, error) {
	r := &reader{data: payload}
	nBrokers, err := r.u32()
	if err != nil {
		return MetadataResponse{}, err
	}
	brokers := make([]BrokerInfo, 0, nBrokers)
	for i := uint32(0); i < nBrokers; i++ {
		id, err := r.u32()
		if err != nil {
			return MetadataResponse{}, err
		}
		host, err := getString(r)
		if err != nil {
			return MetadataResponse{}, err
		}
		port, err := r.u16()
		if err != nil {
			return MetadataResponse{}, err
		}
		brokers = append(brokers, BrokerInfo{NodeID: id, Host: host, Port: port})
	}
	nTopics, err := r.u32()
	if err != nil {
		return MetadataResponse{}, err
	}
	topics := make([]TopicInfo, 0, nTopics)
	for i := uint32(0); i < nTopics; i++ {
		name, err := getString(r)
		if err != nil {
			return MetadataResponse{}, err
		}
		tid, err := r.u32()
		if err != nil {
			return MetadataResponse{}, err
		}
		code, err := r.u16()
		if err != nil {
			return MetadataResponse{}, err
		}
		nParts, err := r.u32()
		if err != nil {
			return MetadataResponse{}, err
		}
		parts := make([]PartitionInfo, 0, nParts)
		for j := uint32(0); j < nParts; j++ {
			pid, err := r.u32()
			if err != nil {
				return MetadataResponse{}, err
			}
			leader, err := r.u32()
			if err != nil {
				return MetadataResponse{}, err
			}
			hwm, err := r.u64()
			if err != nil {
				return MetadataResponse{}, err
			}
			nRep, err := r.u32()
			if err != nil {
				return MetadataResponse{}, err
			}
			replicas := make([]uint32, nRep)
			for k := uint32(0); k < nRep; k++ {
				replicas[k], err = r.u32()
				if err != nil {
					return MetadataResponse{}, err
				}
			}
			nISR, err := r.u32()
			if err != nil {
				return MetadataResponse{}, err
			}
			isr := make([]uint32, nISR)
			for k := uint32(0); k < nISR; k++ {
				isr[k], err = r.u32()
				if err != nil {
					return MetadataResponse{}, err
				}
			}
			epoch, err := r.u32()
			if err != nil {
				return MetadataResponse{}, err
			}
			parts = append(parts, PartitionInfo{
				PartitionID: pid,
				Leader:      leader,
				HWM:         hwm,
				Replicas:    replicas,
				ISR:         isr,
				LeaderEpoch: epoch,
			})
		}
		topics = append(topics, TopicInfo{
			Name:       name,
			TopicID:    tid,
			ErrorCode:  code,
			Partitions: parts,
		})
	}
	return MetadataResponse{Brokers: brokers, Topics: topics}, nil
}

func EncodeOffsetCommitRequest(req OffsetCommitRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.GroupID); err != nil {
		return nil, err
	}
	if err := putString(w, req.MemberID); err != nil {
		return nil, err
	}
	w.u32(req.Generation)
	w.u32(uint32(len(req.Entries)))
	for _, e := range req.Entries {
		if err := putString(w, e.Topic); err != nil {
			return nil, err
		}
		w.u32(e.Partition)
		w.u64(e.Offset)
		if err := putString(w, e.Metadata); err != nil {
			return nil, err
		}
	}
	return w.buf, nil
}

func DecodeOffsetCommitRequest(payload []byte) (OffsetCommitRequest, error) {
	r := &reader{data: payload}
	groupID, err := getString(r)
	if err != nil {
		return OffsetCommitRequest{}, err
	}
	memberID, err := getString(r)
	if err != nil {
		return OffsetCommitRequest{}, err
	}
	generation, err := r.u32()
	if err != nil {
		return OffsetCommitRequest{}, err
	}
	n, err := r.u32()
	if err != nil {
		return OffsetCommitRequest{}, err
	}
	entries := make([]OffsetCommitEntry, 0, n)
	for i := uint32(0); i < n; i++ {
		topic, err := getString(r)
		if err != nil {
			return OffsetCommitRequest{}, err
		}
		part, err := r.u32()
		if err != nil {
			return OffsetCommitRequest{}, err
		}
		off, err := r.u64()
		if err != nil {
			return OffsetCommitRequest{}, err
		}
		meta, err := getString(r)
		if err != nil {
			return OffsetCommitRequest{}, err
		}
		entries = append(entries, OffsetCommitEntry{
			Topic: topic, Partition: part, Offset: off, Metadata: meta,
		})
	}
	return OffsetCommitRequest{
		GroupID:    groupID,
		MemberID:   memberID,
		Generation: generation,
		Entries:    entries,
	}, nil
}

func EncodeOffsetCommitResponse(resp OffsetCommitResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	return w.buf, nil
}

func DecodeOffsetCommitResponse(payload []byte) (OffsetCommitResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return OffsetCommitResponse{}, err
	}
	return OffsetCommitResponse{ErrorCode: code}, nil
}

func EncodeOffsetFetchRequest(req OffsetFetchRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.GroupID); err != nil {
		return nil, err
	}
	w.u32(uint32(len(req.Entries)))
	for _, e := range req.Entries {
		if err := putString(w, e.Topic); err != nil {
			return nil, err
		}
		w.u32(e.Partition)
	}
	return w.buf, nil
}

func DecodeOffsetFetchRequest(payload []byte) (OffsetFetchRequest, error) {
	r := &reader{data: payload}
	groupID, err := getString(r)
	if err != nil {
		return OffsetFetchRequest{}, err
	}
	n, err := r.u32()
	if err != nil {
		return OffsetFetchRequest{}, err
	}
	entries := make([]OffsetEntry, 0, n)
	for i := uint32(0); i < n; i++ {
		topic, err := getString(r)
		if err != nil {
			return OffsetFetchRequest{}, err
		}
		part, err := r.u32()
		if err != nil {
			return OffsetFetchRequest{}, err
		}
		entries = append(entries, OffsetEntry{Topic: topic, Partition: part})
	}
	return OffsetFetchRequest{GroupID: groupID, Entries: entries}, nil
}

func EncodeOffsetFetchResponse(resp OffsetFetchResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	w.u32(uint32(len(resp.Entries)))
	for _, e := range resp.Entries {
		if err := putString(w, e.Topic); err != nil {
			return nil, err
		}
		w.u32(e.Partition)
		w.u64(e.Offset)
		if err := putString(w, e.Metadata); err != nil {
			return nil, err
		}
	}
	return w.buf, nil
}

func DecodeOffsetFetchResponse(payload []byte) (OffsetFetchResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return OffsetFetchResponse{}, err
	}
	n, err := r.u32()
	if err != nil {
		return OffsetFetchResponse{}, err
	}
	entries := make([]OffsetFetchEntry, 0, n)
	for i := uint32(0); i < n; i++ {
		topic, err := getString(r)
		if err != nil {
			return OffsetFetchResponse{}, err
		}
		part, err := r.u32()
		if err != nil {
			return OffsetFetchResponse{}, err
		}
		off, err := r.u64()
		if err != nil {
			return OffsetFetchResponse{}, err
		}
		meta, err := getString(r)
		if err != nil {
			return OffsetFetchResponse{}, err
		}
		entries = append(entries, OffsetFetchEntry{
			Topic: topic, Partition: part, Offset: off, Metadata: meta,
		})
	}
	return OffsetFetchResponse{ErrorCode: code, Entries: entries}, nil
}

func putAssignments(w *writer, items []Assignment) error {
	w.u32(uint32(len(items)))
	for _, a := range items {
		if err := putString(w, a.Topic); err != nil {
			return err
		}
		w.u32(a.Partition)
	}
	return nil
}

func getAssignments(r *reader) ([]Assignment, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	out := make([]Assignment, 0, n)
	for i := uint32(0); i < n; i++ {
		topic, err := getString(r)
		if err != nil {
			return nil, err
		}
		part, err := r.u32()
		if err != nil {
			return nil, err
		}
		out = append(out, Assignment{Topic: topic, Partition: part})
	}
	return out, nil
}

func EncodeJoinGroupRequest(req JoinGroupRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.GroupID); err != nil {
		return nil, err
	}
	if err := putString(w, req.MemberID); err != nil {
		return nil, err
	}
	w.u32(req.SessionTimeoutMs)
	w.u32(uint32(len(req.Topics)))
	for _, t := range req.Topics {
		if err := putString(w, t); err != nil {
			return nil, err
		}
	}
	// Phase 12 trailing field (always written by current encoders).
	if err := putString(w, req.GroupInstanceID); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeJoinGroupRequest(payload []byte) (JoinGroupRequest, error) {
	r := &reader{data: payload}
	groupID, err := getString(r)
	if err != nil {
		return JoinGroupRequest{}, err
	}
	memberID, err := getString(r)
	if err != nil {
		return JoinGroupRequest{}, err
	}
	timeout, err := r.u32()
	if err != nil {
		return JoinGroupRequest{}, err
	}
	n, err := r.u32()
	if err != nil {
		return JoinGroupRequest{}, err
	}
	topics := make([]string, 0, n)
	for i := uint32(0); i < n; i++ {
		t, err := getString(r)
		if err != nil {
			return JoinGroupRequest{}, err
		}
		topics = append(topics, t)
	}
	// Phase 12 trailing field; legacy payloads omit it.
	instanceID := ""
	if r.remaining() > 0 {
		instanceID, err = getString(r)
		if err != nil {
			return JoinGroupRequest{}, err
		}
	}
	return JoinGroupRequest{
		GroupID:          groupID,
		MemberID:         memberID,
		SessionTimeoutMs: timeout,
		Topics:           topics,
		GroupInstanceID:  instanceID,
	}, nil
}

func EncodeJoinGroupResponse(resp JoinGroupResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	w.u32(resp.Generation)
	if err := putString(w, resp.MemberID); err != nil {
		return nil, err
	}
	if err := putAssignments(w, resp.Assignment); err != nil {
		return nil, err
	}
	// Phase 17 trailing revoked list (always written by current encoders).
	if err := putAssignments(w, resp.Revoked); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeJoinGroupResponse(payload []byte) (JoinGroupResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	gen, err := r.u32()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	memberID, err := getString(r)
	if err != nil {
		return JoinGroupResponse{}, err
	}
	assignment, err := getAssignments(r)
	if err != nil {
		return JoinGroupResponse{}, err
	}
	// Phase 17 trailing revoked list; legacy payloads omit it.
	var revoked []Assignment
	if r.remaining() >= 4 {
		revoked, err = getAssignments(r)
		if err != nil {
			return JoinGroupResponse{}, err
		}
	}
	if revoked == nil {
		revoked = []Assignment{}
	}
	return JoinGroupResponse{
		ErrorCode:  code,
		Generation: gen,
		MemberID:   memberID,
		Assignment: assignment,
		Revoked:    revoked,
	}, nil
}

func EncodeHeartbeatRequest(req HeartbeatRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.GroupID); err != nil {
		return nil, err
	}
	if err := putString(w, req.MemberID); err != nil {
		return nil, err
	}
	w.u32(req.Generation)
	return w.buf, nil
}

func DecodeHeartbeatRequest(payload []byte) (HeartbeatRequest, error) {
	r := &reader{data: payload}
	groupID, err := getString(r)
	if err != nil {
		return HeartbeatRequest{}, err
	}
	memberID, err := getString(r)
	if err != nil {
		return HeartbeatRequest{}, err
	}
	gen, err := r.u32()
	if err != nil {
		return HeartbeatRequest{}, err
	}
	return HeartbeatRequest{GroupID: groupID, MemberID: memberID, Generation: gen}, nil
}

func EncodeHeartbeatResponse(resp HeartbeatResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	return w.buf, nil
}

func DecodeHeartbeatResponse(payload []byte) (HeartbeatResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return HeartbeatResponse{}, err
	}
	return HeartbeatResponse{ErrorCode: code}, nil
}

func EncodeLeaveGroupRequest(req LeaveGroupRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.GroupID); err != nil {
		return nil, err
	}
	if err := putString(w, req.MemberID); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeLeaveGroupRequest(payload []byte) (LeaveGroupRequest, error) {
	r := &reader{data: payload}
	groupID, err := getString(r)
	if err != nil {
		return LeaveGroupRequest{}, err
	}
	memberID, err := getString(r)
	if err != nil {
		return LeaveGroupRequest{}, err
	}
	return LeaveGroupRequest{GroupID: groupID, MemberID: memberID}, nil
}

func EncodeLeaveGroupResponse(resp LeaveGroupResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	return w.buf, nil
}

func DecodeLeaveGroupResponse(payload []byte) (LeaveGroupResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return LeaveGroupResponse{}, err
	}
	return LeaveGroupResponse{ErrorCode: code}, nil
}

func EncodeAuthRequest(req AuthRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Token); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeAuthRequest(payload []byte) (AuthRequest, error) {
	token, err := getString(&reader{data: payload})
	if err != nil {
		return AuthRequest{}, err
	}
	return AuthRequest{Token: token}, nil
}

func EncodeAuthResponse(resp AuthResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	return w.buf, nil
}

func DecodeAuthResponse(payload []byte) (AuthResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return AuthResponse{}, err
	}
	return AuthResponse{ErrorCode: code}, nil
}

func EncodeDescribeGroupRequest(req DescribeGroupRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.GroupID); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeDescribeGroupRequest(payload []byte) (DescribeGroupRequest, error) {
	groupID, err := getString(&reader{data: payload})
	if err != nil {
		return DescribeGroupRequest{}, err
	}
	return DescribeGroupRequest{GroupID: groupID}, nil
}

func EncodeDescribeGroupResponse(resp DescribeGroupResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	if err := putString(w, resp.GroupID); err != nil {
		return nil, err
	}
	w.u32(resp.Generation)
	w.u32(uint32(len(resp.Members)))
	for _, m := range resp.Members {
		if err := putString(w, m.MemberID); err != nil {
			return nil, err
		}
		w.u32(uint32(len(m.Topics)))
		for _, t := range m.Topics {
			if err := putString(w, t); err != nil {
				return nil, err
			}
		}
		if err := putAssignments(w, m.Assignment); err != nil {
			return nil, err
		}
	}
	return w.buf, nil
}

func DecodeDescribeGroupResponse(payload []byte) (DescribeGroupResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return DescribeGroupResponse{}, err
	}
	groupID, err := getString(r)
	if err != nil {
		return DescribeGroupResponse{}, err
	}
	generation, err := r.u32()
	if err != nil {
		return DescribeGroupResponse{}, err
	}
	n, err := r.u32()
	if err != nil {
		return DescribeGroupResponse{}, err
	}
	members := make([]GroupMemberInfo, 0, n)
	for i := uint32(0); i < n; i++ {
		memberID, err := getString(r)
		if err != nil {
			return DescribeGroupResponse{}, err
		}
		nTopics, err := r.u32()
		if err != nil {
			return DescribeGroupResponse{}, err
		}
		topics := make([]string, 0, nTopics)
		for j := uint32(0); j < nTopics; j++ {
			t, err := getString(r)
			if err != nil {
				return DescribeGroupResponse{}, err
			}
			topics = append(topics, t)
		}
		assignment, err := getAssignments(r)
		if err != nil {
			return DescribeGroupResponse{}, err
		}
		members = append(members, GroupMemberInfo{
			MemberID:   memberID,
			Topics:     topics,
			Assignment: assignment,
		})
	}
	return DescribeGroupResponse{
		ErrorCode:  code,
		GroupID:    groupID,
		Generation: generation,
		Members:    members,
	}, nil
}

func EncodeListGroupsRequest() []byte {
	return []byte{}
}

func EncodeListGroupsResponse(resp ListGroupsResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	w.u32(uint32(len(resp.Groups)))
	for _, g := range resp.Groups {
		if err := putString(w, g.GroupID); err != nil {
			return nil, err
		}
		w.u8(uint8(g.State))
		w.u32(g.MemberCount)
		w.u32(g.Generation)
	}
	return w.buf, nil
}

func DecodeListGroupsResponse(payload []byte) (ListGroupsResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return ListGroupsResponse{}, err
	}
	n, err := r.u32()
	if err != nil {
		return ListGroupsResponse{}, err
	}
	groups := make([]GroupListing, 0, n)
	for i := uint32(0); i < n; i++ {
		groupID, err := getString(r)
		if err != nil {
			return ListGroupsResponse{}, err
		}
		state, err := r.u8()
		if err != nil {
			return ListGroupsResponse{}, err
		}
		memberCount, err := r.u32()
		if err != nil {
			return ListGroupsResponse{}, err
		}
		generation, err := r.u32()
		if err != nil {
			return ListGroupsResponse{}, err
		}
		groups = append(groups, GroupListing{
			GroupID:     groupID,
			State:       GroupStateFromU8(state),
			MemberCount: memberCount,
			Generation:  generation,
		})
	}
	return ListGroupsResponse{ErrorCode: code, Groups: groups}, nil
}

func EncodeListOffsetsRequest(req ListOffsetsRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Topic); err != nil {
		return nil, err
	}
	parts := req.Partitions
	if parts == nil {
		parts = []uint32{}
	}
	w.u32(uint32(len(parts)))
	for _, p := range parts {
		w.u32(p)
	}
	return w.buf, nil
}

func DecodeListOffsetsRequest(payload []byte) (ListOffsetsRequest, error) {
	r := &reader{data: payload}
	topic, err := getString(r)
	if err != nil {
		return ListOffsetsRequest{}, err
	}
	n, err := r.u32()
	if err != nil {
		return ListOffsetsRequest{}, err
	}
	parts := make([]uint32, 0, n)
	for i := uint32(0); i < n; i++ {
		p, err := r.u32()
		if err != nil {
			return ListOffsetsRequest{}, err
		}
		parts = append(parts, p)
	}
	return ListOffsetsRequest{Topic: topic, Partitions: parts}, nil
}

func EncodeListOffsetsResponse(resp ListOffsetsResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	if err := putString(w, resp.Topic); err != nil {
		return nil, err
	}
	w.u32(uint32(len(resp.Entries)))
	for _, e := range resp.Entries {
		w.u32(e.Partition)
		w.u64(e.Earliest)
		w.u64(e.Latest)
	}
	return w.buf, nil
}

func DecodeListOffsetsResponse(payload []byte) (ListOffsetsResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return ListOffsetsResponse{}, err
	}
	topic, err := getString(r)
	if err != nil {
		return ListOffsetsResponse{}, err
	}
	n, err := r.u32()
	if err != nil {
		return ListOffsetsResponse{}, err
	}
	entries := make([]OffsetListing, 0, n)
	for i := uint32(0); i < n; i++ {
		part, err := r.u32()
		if err != nil {
			return ListOffsetsResponse{}, err
		}
		earliest, err := r.u64()
		if err != nil {
			return ListOffsetsResponse{}, err
		}
		latest, err := r.u64()
		if err != nil {
			return ListOffsetsResponse{}, err
		}
		entries = append(entries, OffsetListing{
			Partition: part, Earliest: earliest, Latest: latest,
		})
	}
	return ListOffsetsResponse{ErrorCode: code, Topic: topic, Entries: entries}, nil
}

func putConfigPairs(w *writer, configs [][2]string) error {
	if configs == nil {
		configs = [][2]string{}
	}
	w.u32(uint32(len(configs)))
	for _, kv := range configs {
		if err := putString(w, kv[0]); err != nil {
			return err
		}
		if err := putString(w, kv[1]); err != nil {
			return err
		}
	}
	return nil
}

func getConfigPairs(r *reader) ([][2]string, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	configs := make([][2]string, 0, n)
	for i := uint32(0); i < n; i++ {
		k, err := getString(r)
		if err != nil {
			return nil, err
		}
		v, err := getString(r)
		if err != nil {
			return nil, err
		}
		configs = append(configs, [2]string{k, v})
	}
	return configs, nil
}

func encodeAclEntries(w *writer, entries []AclBinding) error {
	if entries == nil {
		entries = []AclBinding{}
	}
	w.u32(uint32(len(entries)))
	for _, e := range entries {
		if err := putAclBinding(w, e); err != nil {
			return err
		}
	}
	return nil
}

func decodeAclEntries(r *reader) ([]AclBinding, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	entries := make([]AclBinding, 0, n)
	for i := uint32(0); i < n; i++ {
		e, err := getAclBinding(r)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func EncodeCreateAclsRequest(req CreateAclsRequest) ([]byte, error) {
	w := &writer{}
	if err := encodeAclEntries(w, req.Entries); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeCreateAclsRequest(payload []byte) (CreateAclsRequest, error) {
	r := &reader{data: payload}
	entries, err := decodeAclEntries(r)
	if err != nil {
		return CreateAclsRequest{}, err
	}
	return CreateAclsRequest{Entries: entries}, nil
}

func EncodeCreateAclsResponse(resp CreateAclsResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	return w.buf, nil
}

func DecodeCreateAclsResponse(payload []byte) (CreateAclsResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return CreateAclsResponse{}, err
	}
	return CreateAclsResponse{ErrorCode: code}, nil
}

func EncodeDeleteAclsRequest(req DeleteAclsRequest) ([]byte, error) {
	w := &writer{}
	if err := encodeAclEntries(w, req.Entries); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeDeleteAclsRequest(payload []byte) (DeleteAclsRequest, error) {
	r := &reader{data: payload}
	entries, err := decodeAclEntries(r)
	if err != nil {
		return DeleteAclsRequest{}, err
	}
	return DeleteAclsRequest{Entries: entries}, nil
}

func EncodeDeleteAclsResponse(resp DeleteAclsResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	w.u32(resp.Removed)
	return w.buf, nil
}

func DecodeDeleteAclsResponse(payload []byte) (DeleteAclsResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return DeleteAclsResponse{}, err
	}
	removed, err := r.u32()
	if err != nil {
		return DeleteAclsResponse{}, err
	}
	return DeleteAclsResponse{ErrorCode: code, Removed: removed}, nil
}

func EncodeListAclsRequest(req ListAclsRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Principal); err != nil {
		return nil, err
	}
	w.u8(req.ResourceType)
	if err := putString(w, req.Resource); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeListAclsRequest(payload []byte) (ListAclsRequest, error) {
	r := &reader{data: payload}
	principal, err := getString(r)
	if err != nil {
		return ListAclsRequest{}, err
	}
	resourceType, err := r.u8()
	if err != nil {
		return ListAclsRequest{}, err
	}
	resource, err := getString(r)
	if err != nil {
		return ListAclsRequest{}, err
	}
	return ListAclsRequest{
		Principal:    principal,
		ResourceType: resourceType,
		Resource:     resource,
	}, nil
}

func EncodeListAclsResponse(resp ListAclsResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	if err := encodeAclEntries(w, resp.Entries); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeListAclsResponse(payload []byte) (ListAclsResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return ListAclsResponse{}, err
	}
	entries, err := decodeAclEntries(r)
	if err != nil {
		return ListAclsResponse{}, err
	}
	return ListAclsResponse{ErrorCode: code, Entries: entries}, nil
}

func EncodeCreateScramUserRequest(req CreateScramUserRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Username); err != nil {
		return nil, err
	}
	if err := putString(w, req.Password); err != nil {
		return nil, err
	}
	w.u32(req.Iterations)
	return w.buf, nil
}

func DecodeCreateScramUserRequest(payload []byte) (CreateScramUserRequest, error) {
	r := &reader{data: payload}
	user, err := getString(r)
	if err != nil {
		return CreateScramUserRequest{}, err
	}
	pass, err := getString(r)
	if err != nil {
		return CreateScramUserRequest{}, err
	}
	iters, err := r.u32()
	if err != nil {
		return CreateScramUserRequest{}, err
	}
	return CreateScramUserRequest{Username: user, Password: pass, Iterations: iters}, nil
}

func EncodeCreateScramUserResponse(resp CreateScramUserResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	return w.buf, nil
}

func DecodeCreateScramUserResponse(payload []byte) (CreateScramUserResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return CreateScramUserResponse{}, err
	}
	return CreateScramUserResponse{ErrorCode: code}, nil
}

func EncodeDeleteScramUserRequest(req DeleteScramUserRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Username); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeDeleteScramUserRequest(payload []byte) (DeleteScramUserRequest, error) {
	r := &reader{data: payload}
	user, err := getString(r)
	if err != nil {
		return DeleteScramUserRequest{}, err
	}
	return DeleteScramUserRequest{Username: user}, nil
}

func EncodeDeleteScramUserResponse(resp DeleteScramUserResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	return w.buf, nil
}

func DecodeDeleteScramUserResponse(payload []byte) (DeleteScramUserResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return DeleteScramUserResponse{}, err
	}
	return DeleteScramUserResponse{ErrorCode: code}, nil
}

func EncodeListScramUsersRequest() []byte {
	return []byte{}
}

func DecodeListScramUsersRequest(payload []byte) error {
	return nil
}

func EncodeListScramUsersResponse(resp ListScramUsersResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	w.u32(uint32(len(resp.Usernames)))
	for _, name := range resp.Usernames {
		if err := putString(w, name); err != nil {
			return nil, err
		}
	}
	return w.buf, nil
}

func DecodeListScramUsersResponse(payload []byte) (ListScramUsersResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return ListScramUsersResponse{}, err
	}
	n, err := r.u32()
	if err != nil {
		return ListScramUsersResponse{}, err
	}
	names := make([]string, 0, n)
	for i := uint32(0); i < n; i++ {
		name, err := getString(r)
		if err != nil {
			return ListScramUsersResponse{}, err
		}
		names = append(names, name)
	}
	return ListScramUsersResponse{ErrorCode: code, Usernames: names}, nil
}

func EncodeDeleteOffsetsRequest(req DeleteOffsetsRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.GroupID); err != nil {
		return nil, err
	}
	entries := req.Entries
	if entries == nil {
		entries = []OffsetEntry{}
	}
	w.u32(uint32(len(entries)))
	for _, e := range entries {
		if err := putString(w, e.Topic); err != nil {
			return nil, err
		}
		w.u32(e.Partition)
	}
	return w.buf, nil
}


func DecodeDeleteOffsetsRequest(payload []byte) (DeleteOffsetsRequest, error) {
	r := &reader{data: payload}
	groupID, err := getString(r)
	if err != nil {
		return DeleteOffsetsRequest{}, err
	}
	n, err := r.u32()
	if err != nil {
		return DeleteOffsetsRequest{}, err
	}
	entries := make([]OffsetEntry, 0, n)
	for i := uint32(0); i < n; i++ {
		topic, err := getString(r)
		if err != nil {
			return DeleteOffsetsRequest{}, err
		}
		part, err := r.u32()
		if err != nil {
			return DeleteOffsetsRequest{}, err
		}
		entries = append(entries, OffsetEntry{Topic: topic, Partition: part})
	}
	return DeleteOffsetsRequest{GroupID: groupID, Entries: entries}, nil
}


func EncodeDeleteOffsetsResponse(resp DeleteOffsetsResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	w.u32(resp.DeletedCount)
	return w.buf, nil
}


func DecodeDeleteOffsetsResponse(payload []byte) (DeleteOffsetsResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return DeleteOffsetsResponse{}, err
	}
	n, err := r.u32()
	if err != nil {
		return DeleteOffsetsResponse{}, err
	}
	return DeleteOffsetsResponse{ErrorCode: code, DeletedCount: n}, nil
}

func EncodeDescribeConfigsRequest(req DescribeConfigsRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Topic); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeDescribeConfigsRequest(payload []byte) (DescribeConfigsRequest, error) {
	r := &reader{data: payload}
	topic, err := getString(r)
	if err != nil {
		return DescribeConfigsRequest{}, err
	}
	return DescribeConfigsRequest{Topic: topic}, nil
}

func EncodeDescribeConfigsResponse(resp DescribeConfigsResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	if err := putString(w, resp.Topic); err != nil {
		return nil, err
	}
	w.u32(resp.TopicID)
	w.u32(resp.PartitionCount)
	if err := putConfigPairs(w, resp.Configs); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeDescribeConfigsResponse(payload []byte) (DescribeConfigsResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return DescribeConfigsResponse{}, err
	}
	topic, err := getString(r)
	if err != nil {
		return DescribeConfigsResponse{}, err
	}
	topicID, err := r.u32()
	if err != nil {
		return DescribeConfigsResponse{}, err
	}
	parts, err := r.u32()
	if err != nil {
		return DescribeConfigsResponse{}, err
	}
	configs, err := getConfigPairs(r)
	if err != nil {
		return DescribeConfigsResponse{}, err
	}
	return DescribeConfigsResponse{
		ErrorCode:      code,
		Topic:          topic,
		TopicID:        topicID,
		PartitionCount: parts,
		Configs:        configs,
	}, nil
}

func EncodeAlterConfigsRequest(req AlterConfigsRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Topic); err != nil {
		return nil, err
	}
	if err := putConfigPairs(w, req.Configs); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeAlterConfigsRequest(payload []byte) (AlterConfigsRequest, error) {
	r := &reader{data: payload}
	topic, err := getString(r)
	if err != nil {
		return AlterConfigsRequest{}, err
	}
	configs, err := getConfigPairs(r)
	if err != nil {
		return AlterConfigsRequest{}, err
	}
	return AlterConfigsRequest{Topic: topic, Configs: configs}, nil
}

func EncodeAlterConfigsResponse(resp AlterConfigsResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	if err := putString(w, resp.Topic); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeAlterConfigsResponse(payload []byte) (AlterConfigsResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return AlterConfigsResponse{}, err
	}
	topic, err := getString(r)
	if err != nil {
		return AlterConfigsResponse{}, err
	}
	return AlterConfigsResponse{ErrorCode: code, Topic: topic}, nil
}

func EncodeDeleteRecordsRequest(req DeleteRecordsRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Topic); err != nil {
		return nil, err
	}
	w.u32(req.Partition)
	w.u64(req.BeforeOffset)
	// Phase 137: always write the wait_majority trailer.
	w.u8(req.WaitMajority)
	return w.buf, nil
}

func DecodeDeleteRecordsRequest(payload []byte) (DeleteRecordsRequest, error) {
	r := &reader{data: payload}
	topic, err := getString(r)
	if err != nil {
		return DeleteRecordsRequest{}, err
	}
	part, err := r.u32()
	if err != nil {
		return DeleteRecordsRequest{}, err
	}
	before, err := r.u64()
	if err != nil {
		return DeleteRecordsRequest{}, err
	}
	// Phase 137: optional wait_majority trailer (absent → 0).
	var wait uint8
	if r.remaining() >= 1 {
		wait, err = r.u8()
		if err != nil {
			return DeleteRecordsRequest{}, err
		}
	}
	return DeleteRecordsRequest{
		Topic:        topic,
		Partition:    part,
		BeforeOffset: before,
		WaitMajority: wait,
	}, nil
}

func EncodeDeleteRecordsResponse(resp DeleteRecordsResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	if err := putString(w, resp.Topic); err != nil {
		return nil, err
	}
	w.u32(resp.Partition)
	w.u64(resp.LowWatermark)
	return w.buf, nil
}

func DecodeDeleteRecordsResponse(payload []byte) (DeleteRecordsResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return DeleteRecordsResponse{}, err
	}
	topic, err := getString(r)
	if err != nil {
		return DeleteRecordsResponse{}, err
	}
	part, err := r.u32()
	if err != nil {
		return DeleteRecordsResponse{}, err
	}
	low, err := r.u64()
	if err != nil {
		return DeleteRecordsResponse{}, err
	}
	return DeleteRecordsResponse{
		ErrorCode:    code,
		Topic:        topic,
		Partition:    part,
		LowWatermark: low,
	}, nil
}

func EncodeCreatePartitionsRequest(req CreatePartitionsRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Topic); err != nil {
		return nil, err
	}
	w.u32(req.TotalCount)
	return w.buf, nil
}

func DecodeCreatePartitionsRequest(payload []byte) (CreatePartitionsRequest, error) {
	r := &reader{data: payload}
	topic, err := getString(r)
	if err != nil {
		return CreatePartitionsRequest{}, err
	}
	n, err := r.u32()
	if err != nil {
		return CreatePartitionsRequest{}, err
	}
	return CreatePartitionsRequest{Topic: topic, TotalCount: n}, nil
}

func EncodeCreatePartitionsResponse(resp CreatePartitionsResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	if err := putString(w, resp.Topic); err != nil {
		return nil, err
	}
	w.u32(resp.Partitions)
	return w.buf, nil
}

func DecodeCreatePartitionsResponse(payload []byte) (CreatePartitionsResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return CreatePartitionsResponse{}, err
	}
	topic, err := getString(r)
	if err != nil {
		return CreatePartitionsResponse{}, err
	}
	n, err := r.u32()
	if err != nil {
		return CreatePartitionsResponse{}, err
	}
	return CreatePartitionsResponse{ErrorCode: code, Topic: topic, Partitions: n}, nil
}

func EncodeReassignPartitionsRequest(req ReassignPartitionsRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Topic); err != nil {
		return nil, err
	}
	w.u32(req.Partition)
	replicas := req.Replicas
	if replicas == nil {
		replicas = []uint32{}
	}
	w.u32(uint32(len(replicas)))
	for _, id := range replicas {
		w.u32(id)
	}
	return w.buf, nil
}

func DecodeReassignPartitionsRequest(payload []byte) (ReassignPartitionsRequest, error) {
	r := &reader{data: payload}
	topic, err := getString(r)
	if err != nil {
		return ReassignPartitionsRequest{}, err
	}
	part, err := r.u32()
	if err != nil {
		return ReassignPartitionsRequest{}, err
	}
	n, err := r.u32()
	if err != nil {
		return ReassignPartitionsRequest{}, err
	}
	replicas := make([]uint32, 0, n)
	for i := uint32(0); i < n; i++ {
		id, err := r.u32()
		if err != nil {
			return ReassignPartitionsRequest{}, err
		}
		replicas = append(replicas, id)
	}
	return ReassignPartitionsRequest{Topic: topic, Partition: part, Replicas: replicas}, nil
}

func EncodeReassignPartitionsResponse(resp ReassignPartitionsResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	w.u32(resp.Generation)
	return w.buf, nil
}

func DecodeReassignPartitionsResponse(payload []byte) (ReassignPartitionsResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return ReassignPartitionsResponse{}, err
	}
	gen, err := r.u32()
	if err != nil {
		return ReassignPartitionsResponse{}, err
	}
	return ReassignPartitionsResponse{ErrorCode: code, Generation: gen}, nil
}

func EncodeInitProducerIdRequest(req InitProducerIdRequest) ([]byte, error) {
	w := &writer{}
	// Always write the string; empty transactional_id = non-transactional PID.
	// Legacy empty body still decodes as "".
	if err := putString(w, req.TransactionalID); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeInitProducerIdRequest(payload []byte) (InitProducerIdRequest, error) {
	r := &reader{data: payload}
	if r.remaining() == 0 {
		return InitProducerIdRequest{TransactionalID: ""}, nil
	}
	txn, err := getString(r)
	if err != nil {
		return InitProducerIdRequest{}, err
	}
	return InitProducerIdRequest{TransactionalID: txn}, nil
}

func EncodeInitProducerIdResponse(resp InitProducerIdResponse) ([]byte, error) {
	w := &writer{}
	w.u64(resp.ProducerID)
	w.u16(resp.Epoch)
	w.u16(resp.ErrorCode)
	return w.buf, nil
}

func DecodeInitProducerIdResponse(payload []byte) (InitProducerIdResponse, error) {
	r := &reader{data: payload}
	pid, err := r.u64()
	if err != nil {
		return InitProducerIdResponse{}, err
	}
	epoch, err := r.u16()
	if err != nil {
		return InitProducerIdResponse{}, err
	}
	code, err := r.u16()
	if err != nil {
		return InitProducerIdResponse{}, err
	}
	return InitProducerIdResponse{ProducerID: pid, Epoch: epoch, ErrorCode: code}, nil
}

func EncodeScramFirstRequest(req ScramFirstRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Username); err != nil {
		return nil, err
	}
	if err := putString(w, req.ClientNonce); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeScramFirstRequest(payload []byte) (ScramFirstRequest, error) {
	r := &reader{data: payload}
	user, err := getString(r)
	if err != nil {
		return ScramFirstRequest{}, err
	}
	nonce, err := getString(r)
	if err != nil {
		return ScramFirstRequest{}, err
	}
	return ScramFirstRequest{Username: user, ClientNonce: nonce}, nil
}

func EncodeScramFirstResponse(resp ScramFirstResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	if err := putString(w, resp.CombinedNonce); err != nil {
		return nil, err
	}
	putBytes(w, resp.Salt)
	w.u32(resp.Iterations)
	return w.buf, nil
}

func DecodeScramFirstResponse(payload []byte) (ScramFirstResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return ScramFirstResponse{}, err
	}
	combined, err := getString(r)
	if err != nil {
		return ScramFirstResponse{}, err
	}
	salt, err := getBytes(r)
	if err != nil {
		return ScramFirstResponse{}, err
	}
	iters, err := r.u32()
	if err != nil {
		return ScramFirstResponse{}, err
	}
	return ScramFirstResponse{
		ErrorCode:     code,
		CombinedNonce: combined,
		Salt:          salt,
		Iterations:    iters,
	}, nil
}

func EncodeScramFinalRequest(req ScramFinalRequest) ([]byte, error) {
	w := &writer{}
	if err := putString(w, req.Username); err != nil {
		return nil, err
	}
	if err := putString(w, req.CombinedNonce); err != nil {
		return nil, err
	}
	putBytes(w, req.ClientProof)
	return w.buf, nil
}

func DecodeScramFinalRequest(payload []byte) (ScramFinalRequest, error) {
	r := &reader{data: payload}
	user, err := getString(r)
	if err != nil {
		return ScramFinalRequest{}, err
	}
	combined, err := getString(r)
	if err != nil {
		return ScramFinalRequest{}, err
	}
	proof, err := getBytes(r)
	if err != nil {
		return ScramFinalRequest{}, err
	}
	return ScramFinalRequest{
		Username:      user,
		CombinedNonce: combined,
		ClientProof:   proof,
	}, nil
}

func EncodeScramFinalResponse(resp ScramFinalResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.ErrorCode)
	putBytes(w, resp.ServerSignature)
	return w.buf, nil
}

func DecodeScramFinalResponse(payload []byte) (ScramFinalResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return ScramFinalResponse{}, err
	}
	sig, err := getBytes(r)
	if err != nil {
		return ScramFinalResponse{}, err
	}
	return ScramFinalResponse{ErrorCode: code, ServerSignature: sig}, nil
}
func EncodeErrorResponse(resp ErrorResponse) ([]byte, error) {
	w := &writer{}
	w.u16(resp.Code)
	if err := putString(w, resp.Message); err != nil {
		return nil, err
	}
	return w.buf, nil
}

func DecodeErrorResponse(payload []byte) (ErrorResponse, error) {
	r := &reader{data: payload}
	code, err := r.u16()
	if err != nil {
		return ErrorResponse{}, err
	}
	msg, err := getString(r)
	if err != nil {
		return ErrorResponse{}, err
	}
	return ErrorResponse{Code: code, Message: msg}, nil
}

// DecodeResponse dispatches a response payload by opcode.
func DecodeResponse(opcode uint16, payload []byte) (any, error) {
	switch opcode {
	case OpProduce:
		return DecodeProduceResponse(payload)
	case OpFetch:
		return DecodeFetchResponse(payload)
	case OpCreateTopic:
		return DecodeCreateTopicResponse(payload)
	case OpMetadata:
		return DecodeMetadataResponse(payload)
	case OpDeleteTopic:
		return DecodeDeleteTopicResponse(payload)
	case OpOffsetCommit:
		return DecodeOffsetCommitResponse(payload)
	case OpOffsetFetch:
		return DecodeOffsetFetchResponse(payload)
	case OpJoinGroup:
		return DecodeJoinGroupResponse(payload)
	case OpHeartbeat:
		return DecodeHeartbeatResponse(payload)
	case OpLeaveGroup:
		return DecodeLeaveGroupResponse(payload)
	case OpAuthResponse:
		return DecodeAuthResponse(payload)
	case OpInitProducerIdResponse:
		return DecodeInitProducerIdResponse(payload)
	case OpScramFirstResponse:
		return DecodeScramFirstResponse(payload)
	case OpScramFinalResponse:
		return DecodeScramFinalResponse(payload)
	case OpDescribeGroupResponse:
		return DecodeDescribeGroupResponse(payload)
	case OpListGroupsResponse:
		return DecodeListGroupsResponse(payload)
	case OpCreatePartitionsResponse:
		return DecodeCreatePartitionsResponse(payload)
	case OpListOffsetsResponse:
		return DecodeListOffsetsResponse(payload)
	case OpCreateAclsResponse:
		return DecodeCreateAclsResponse(payload)
	case OpDeleteAclsResponse:
		return DecodeDeleteAclsResponse(payload)
	case OpListAclsResponse:
		return DecodeListAclsResponse(payload)
	case OpCreateScramUserResponse:
		return DecodeCreateScramUserResponse(payload)
	case OpDeleteScramUserResponse:
		return DecodeDeleteScramUserResponse(payload)
	case OpListScramUsersResponse:
		return DecodeListScramUsersResponse(payload)
	case OpDeleteOffsetsResponse:
		return DecodeDeleteOffsetsResponse(payload)
	case OpDescribeConfigsResponse:
		return DecodeDescribeConfigsResponse(payload)
	case OpAlterConfigsResponse:
		return DecodeAlterConfigsResponse(payload)
	case OpDeleteRecordsResponse:
		return DecodeDeleteRecordsResponse(payload)
	case OpReassignPartitionsResponse:
		return DecodeReassignPartitionsResponse(payload)
	case OpError:
		return DecodeErrorResponse(payload)
	default:
		return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unknown response opcode %d", opcode)}
	}
}
