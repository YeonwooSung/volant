// Package volant is a synchronous TCP client for the native Volant protocol.
//
// This is not a Kafka client and does not speak the Kafka shim (--kafka-listen).
package volant

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

// Version is the client library version (crate stays 0.2.0).
const Version = "0.2.0"

const defaultTimeout = 10 * time.Second

// Re-exported wire / error types.
type (
	ProtocolError    = frame.ProtocolError
	BrokerError      = codec.BrokerError
	Record           = codec.FetchRecord
	Header           = codec.Header
	Metadata         = codec.MetadataResponse
	BrokerInfo       = codec.BrokerInfo
	TopicInfo        = codec.TopicInfo
	PartitionInfo    = codec.PartitionInfo
	ProduceMessage   = codec.ProduceMessage
	MetadataResponse = codec.MetadataResponse
)

// Offset is one committed (partition, offset) pair from OffsetFetch.
type Offset struct {
	Partition uint32
	Offset    uint64
}

// Client is a sync TCP client for the native Volant protocol (MVP).
type Client struct {
	addr     string
	conn     net.Conn
	timeout  time.Duration
	nextCorr uint32
	buf      []byte
}

// Dial connects to a native Volant listener (host:port) with a 10s timeout.
func Dial(addr string) (*Client, error) {
	return DialTimeout(addr, defaultTimeout)
}

// DialTimeout connects with an explicit dial / RPC timeout.
func DialTimeout(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:     addr,
		conn:     conn,
		timeout:  timeout,
		nextCorr: 1,
	}, nil
}

// Close closes the TCP connection. Subsequent RPCs return an error.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) setDeadline() {
	if c.timeout > 0 && c.conn != nil {
		_ = c.conn.SetDeadline(time.Now().Add(c.timeout))
	}
}

func (c *Client) nextCorrelation() uint32 {
	corr := c.nextCorr
	c.nextCorr = (c.nextCorr + 1) & 0xFFFFFFFF
	if c.nextCorr == 0 {
		c.nextCorr = 1
	}
	return corr
}

func (c *Client) send(opcode uint16, payload []byte) (uint32, error) {
	if c.conn == nil {
		return 0, &frame.ProtocolError{Msg: "client closed"}
	}
	corr := c.nextCorrelation()
	raw, err := frame.Encode(opcode, corr, payload)
	if err != nil {
		return 0, err
	}
	c.setDeadline()
	if _, err := c.conn.Write(raw); err != nil {
		return 0, err
	}
	return corr, nil
}

func (c *Client) recvFrame() (*frame.Frame, error) {
	if c.conn == nil {
		return nil, &frame.ProtocolError{Msg: "client closed"}
	}
	for {
		f, rest, err := frame.TryDecode(c.buf)
		if err != nil {
			return nil, err
		}
		if f != nil {
			c.buf = append([]byte(nil), rest...)
			return f, nil
		}
		var need int
		if len(c.buf) >= frame.HeaderLen {
			payloadLen := binary.BigEndian.Uint32(c.buf[8:12])
			if payloadLen > frame.MaxPayload {
				return nil, &frame.ProtocolError{
					Msg: fmt.Sprintf("payload too large: %d > %d", payloadLen, frame.MaxPayload),
				}
			}
			need = frame.HeaderLen + int(payloadLen) - len(c.buf)
		} else {
			need = frame.HeaderLen - len(c.buf)
		}
		if need < 4096 {
			need = 4096
		}
		tmp := make([]byte, need)
		c.setDeadline()
		n, err := c.conn.Read(tmp)
		if n > 0 {
			c.buf = append(c.buf, tmp[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				return nil, &frame.ProtocolError{Msg: "connection closed while reading frame"}
			}
			return nil, err
		}
	}
}

func (c *Client) roundTrip(opcode uint16, payload []byte) (any, error) {
	corr, err := c.send(opcode, payload)
	if err != nil {
		return nil, err
	}
	f, err := c.recvFrame()
	if err != nil {
		return nil, err
	}
	if f.CorrelationID != corr {
		return nil, &frame.ProtocolError{
			Msg: fmt.Sprintf("correlation mismatch: sent %d, got %d", corr, f.CorrelationID),
		}
	}
	if f.Version != frame.ProtocolVersion {
		return nil, &frame.ProtocolError{
			Msg: fmt.Sprintf("unsupported protocol version: %d", f.Version),
		}
	}
	decoded, err := codec.DecodeResponse(f.Opcode, f.Payload)
	if err != nil {
		return nil, err
	}
	if er, ok := decoded.(codec.ErrorResponse); ok {
		return nil, &codec.BrokerError{Code: er.Code, Message: er.Message}
	}
	return decoded, nil
}

func check(errorCode uint16, op string) error {
	if errorCode != 0 {
		return &codec.BrokerError{Code: errorCode, Op: op}
	}
	return nil
}

// CreateTopic creates a topic with the given partition count.
func (c *Client) CreateTopic(name string, partitions int) error {
	payload, err := codec.EncodeCreateTopicRequest(codec.CreateTopicRequest{
		Name:       name,
		Partitions: uint32(partitions),
		Configs:    [][2]string{},
	})
	if err != nil {
		return err
	}
	decoded, err := c.roundTrip(codec.OpCreateTopic, payload)
	if err != nil {
		return err
	}
	resp, ok := decoded.(codec.CreateTopicResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for create_topic: %T", decoded)}
	}
	return check(resp.ErrorCode, "create_topic")
}

// DeleteTopic deletes a topic by name.
func (c *Client) DeleteTopic(name string) error {
	payload, err := codec.EncodeDeleteTopicRequest(codec.DeleteTopicRequest{Name: name})
	if err != nil {
		return err
	}
	decoded, err := c.roundTrip(codec.OpDeleteTopic, payload)
	if err != nil {
		return err
	}
	resp, ok := decoded.(codec.DeleteTopicResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for delete_topic: %T", decoded)}
	}
	return check(resp.ErrorCode, "delete_topic")
}

// Produce sends one message (null key when key is nil) with acks=1.
// Idempotent produce is not implemented; trailer is (0, 0, -1).
// Returns the broker-assigned base offset.
func (c *Client) Produce(topic string, partition int, key, value []byte) (int64, error) {
	if value == nil {
		value = []byte{}
	}
	payload, err := codec.EncodeProduceRequest(codec.ProduceRequest{
		Topic:     topic,
		Partition: int32(partition),
		Acks:      1,
		Messages: []codec.ProduceMessage{
			{Key: key, Value: value, TimestampMs: -1},
		},
		ProducerID:    0,
		ProducerEpoch: 0,
		BaseSequence:  -1,
	})
	if err != nil {
		return 0, err
	}
	decoded, err := c.roundTrip(codec.OpProduce, payload)
	if err != nil {
		return 0, err
	}
	resp, ok := decoded.(codec.ProduceResponse)
	if !ok {
		return 0, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for produce: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "produce"); err != nil {
		return 0, err
	}
	return int64(resp.BaseOffset), nil
}

// Fetch reads records from topic/partition starting at offset.
// Defaults match the Python client: max_messages=128, max_bytes=4MiB, max_wait_ms=0.
func (c *Client) Fetch(topic string, partition int, offset int64) ([]Record, error) {
	payload, err := codec.EncodeFetchRequest(codec.FetchRequest{
		Topic:       topic,
		Partition:   uint32(partition),
		FromOffset:  uint64(offset),
		MaxMessages: 128,
		MaxBytes:    4 * 1024 * 1024,
		MaxWaitMs:   0,
	})
	if err != nil {
		return nil, err
	}
	decoded, err := c.roundTrip(codec.OpFetch, payload)
	if err != nil {
		return nil, err
	}
	resp, ok := decoded.(codec.FetchResponse)
	if !ok {
		return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for fetch: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "fetch"); err != nil {
		return nil, err
	}
	return resp.Records, nil
}

// Metadata returns cluster brokers and topics (all topics when the list is empty).
func (c *Client) Metadata() (Metadata, error) {
	payload, err := codec.EncodeMetadataRequest(codec.MetadataRequest{Topics: []string{}})
	if err != nil {
		return Metadata{}, err
	}
	decoded, err := c.roundTrip(codec.OpMetadata, payload)
	if err != nil {
		return Metadata{}, err
	}
	resp, ok := decoded.(codec.MetadataResponse)
	if !ok {
		return Metadata{}, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for metadata: %T", decoded)}
	}
	return resp, nil
}

// OffsetCommit commits one group offset (admin path: empty member, generation 0).
func (c *Client) OffsetCommit(group, topic string, partition int, offset int64) error {
	payload, err := codec.EncodeOffsetCommitRequest(codec.OffsetCommitRequest{
		GroupID:    group,
		MemberID:   "",
		Generation: 0,
		Entries: []codec.OffsetCommitEntry{
			{Topic: topic, Partition: uint32(partition), Offset: uint64(offset), Metadata: ""},
		},
	})
	if err != nil {
		return err
	}
	decoded, err := c.roundTrip(codec.OpOffsetCommit, payload)
	if err != nil {
		return err
	}
	resp, ok := decoded.(codec.OffsetCommitResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for offset_commit: %T", decoded)}
	}
	return check(resp.ErrorCode, "offset_commit")
}

// OffsetFetch returns committed offsets for topic as []Offset.
// Empty wire entries mean all offsets for the group; this method filters
// to topic client-side (same as the CLI).
func (c *Client) OffsetFetch(group, topic string) ([]Offset, error) {
	payload, err := codec.EncodeOffsetFetchRequest(codec.OffsetFetchRequest{
		GroupID: group,
		Entries: []codec.OffsetEntry{},
	})
	if err != nil {
		return nil, err
	}
	decoded, err := c.roundTrip(codec.OpOffsetFetch, payload)
	if err != nil {
		return nil, err
	}
	resp, ok := decoded.(codec.OffsetFetchResponse)
	if !ok {
		return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for offset_fetch: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "offset_fetch"); err != nil {
		return nil, err
	}
	out := make([]Offset, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		if e.Topic == topic {
			out = append(out, Offset{Partition: e.Partition, Offset: e.Offset})
		}
	}
	return out, nil
}
