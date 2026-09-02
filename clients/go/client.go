// Package volant is a synchronous TCP client for the native Volant protocol.
//
// This is not a Kafka client and does not speak the Kafka shim (--kafka-listen).
package volant

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
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
	Assignment       = codec.Assignment
)

// Offset is one committed (partition, offset) pair from OffsetFetch.
type Offset struct {
	Partition uint32
	Offset    uint64
}

// JoinGroupResult is the successful JoinGroup reply (Rust client field names).
type JoinGroupResult struct {
	MemberID   string
	Generation uint32
	Assignment []Assignment
	Revoked    []Assignment
}

// Client is a sync TCP client for the native Volant protocol (MVP).
type Client struct {
	addr      string
	conn      net.Conn
	timeout   time.Duration
	nextCorr  uint32
	buf       []byte
	tls       bool
	authToken string
}

// TLSConfig is optional TLS for [DialTLS]. Zero value uses system roots
// and no client certificate. Plaintext remains [Dial] / [DialTimeout].
type TLSConfig struct {
	// CAFile is a PEM CA bundle added to the system trust store (same
	// idea as Rust webpki-roots + optional tls_ca). Empty = system roots
	// only. Required in practice for a private / lab CA unless Insecure.
	CAFile string
	// Insecure skips certificate verification (tests / lab only).
	Insecure bool
	// CertFile is an optional client certificate PEM for mTLS.
	CertFile string
	// KeyFile is the client private key PEM. Must be paired with CertFile.
	KeyFile string
	// ServerName overrides the hostname used for SNI / verify
	// (defaults to the dial host).
	ServerName string
}

// Dial connects to a native Volant listener (host:port) with a 10s timeout.
func Dial(addr string) (*Client, error) {
	return DialTimeout(addr, defaultTimeout)
}

// DialTimeout connects with an explicit dial / RPC timeout.
func DialTimeout(addr string, timeout time.Duration) (*Client, error) {
	return dialPlain(addr, timeout, "")
}

// DialAuth is [Dial] plus a shared-token Auth (opcode 30) after connect.
// An empty token skips Auth (same as [Dial]).
func DialAuth(addr, token string) (*Client, error) {
	return dialPlain(addr, defaultTimeout, token)
}

func dialPlain(addr string, timeout time.Duration, token string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	c := &Client{
		addr:      addr,
		conn:      conn,
		timeout:   timeout,
		nextCorr:  1,
		authToken: token,
	}
	if err := c.maybeAuthenticate(); err != nil {
		return nil, err
	}
	return c, nil
}

// DialTLS connects with TLS after TCP (v0.27). Example:
//
//	c, err := volant.DialTLS("127.0.0.1:9092", volant.TLSConfig{CAFile: "ca.pem"})
func DialTLS(addr string, cfg TLSConfig) (*Client, error) {
	return DialTLSTimeout(addr, cfg, defaultTimeout)
}

// DialTLSTimeout is [DialTLS] with an explicit dial / handshake / RPC timeout.
func DialTLSTimeout(addr string, cfg TLSConfig, timeout time.Duration) (*Client, error) {
	return dialTLS(addr, cfg, timeout, "")
}

// DialTLSAuth is [DialTLS] plus a shared-token Auth after the TLS handshake.
// An empty token skips Auth (same as [DialTLS]).
func DialTLSAuth(addr string, cfg TLSConfig, token string) (*Client, error) {
	return dialTLS(addr, cfg, defaultTimeout, token)
}

func dialTLS(addr string, cfg TLSConfig, timeout time.Duration, token string) (*Client, error) {
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return nil, fmt.Errorf("tls_cert and tls_key must both be set or both unset")
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	if timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(timeout))
	}
	tlsConn, err := wrapTLS(conn, addr, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if timeout > 0 {
		_ = tlsConn.SetDeadline(time.Time{})
	}
	c := &Client{
		addr:      addr,
		conn:      tlsConn,
		timeout:   timeout,
		nextCorr:  1,
		tls:       true,
		authToken: token,
	}
	if err := c.maybeAuthenticate(); err != nil {
		return nil, err
	}
	return c, nil
}

// TLS reports whether the connection is TLS-wrapped.
func (c *Client) TLS() bool {
	return c != nil && c.tls
}

func wrapTLS(conn net.Conn, addr string, cfg TLSConfig) (net.Conn, error) {
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return nil, fmt.Errorf("tls_cert and tls_key must both be set or both unset")
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	tc := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: host,
	}
	if cfg.ServerName != "" {
		tc.ServerName = cfg.ServerName
	}
	if cfg.Insecure {
		tc.InsecureSkipVerify = true
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls_ca: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls_ca: no certificates in %s", cfg.CAFile)
		}
		tc.RootCAs = pool
	}
	if cfg.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("client TLS cert: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	tconn := tls.Client(conn, tc)
	if err := tconn.Handshake(); err != nil {
		return nil, err
	}
	return tconn, nil
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

func (c *Client) maybeAuthenticate() error {
	if c.authToken == "" {
		return nil
	}
	if err := c.authenticate(c.authToken); err != nil {
		_ = c.Close()
		return err
	}
	return nil
}

func (c *Client) authenticate(token string) error {
	payload, err := codec.EncodeAuthRequest(codec.AuthRequest{Token: token})
	if err != nil {
		return err
	}
	decoded, err := c.roundTrip(codec.OpAuth, payload)
	if err != nil {
		return err
	}
	resp, ok := decoded.(codec.AuthResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for auth: %T", decoded)}
	}
	return check(resp.ErrorCode, "auth")
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
	return c.fetchAt(topic, partition, offset, 128, 0)
}

func (c *Client) fetchAt(topic string, partition int, offset int64, maxMessages, maxWaitMs uint32) ([]Record, error) {
	if maxMessages == 0 {
		maxMessages = 128
	}
	payload, err := codec.EncodeFetchRequest(codec.FetchRequest{
		Topic:       topic,
		Partition:   uint32(partition),
		FromOffset:  uint64(offset),
		MaxMessages: maxMessages,
		MaxBytes:    4 * 1024 * 1024,
		MaxWaitMs:   maxWaitMs,
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
	return c.commitOffsets(group, "", 0, []codec.OffsetCommitEntry{
		{Topic: topic, Partition: uint32(partition), Offset: uint64(offset), Metadata: ""},
	})
}

func (c *Client) commitOffsets(group, memberID string, generation uint32, entries []codec.OffsetCommitEntry) error {
	if entries == nil {
		entries = []codec.OffsetCommitEntry{}
	}
	payload, err := codec.EncodeOffsetCommitRequest(codec.OffsetCommitRequest{
		GroupID:    group,
		MemberID:   memberID,
		Generation: generation,
		Entries:    entries,
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
	entries, err := c.fetchOffsets(group, nil)
	if err != nil {
		return nil, err
	}
	out := make([]Offset, 0, len(entries))
	for _, e := range entries {
		if e.Topic == topic {
			out = append(out, Offset{Partition: e.Partition, Offset: e.Offset})
		}
	}
	return out, nil
}

func (c *Client) fetchOffsets(group string, entries []codec.OffsetEntry) ([]codec.OffsetFetchEntry, error) {
	if entries == nil {
		entries = []codec.OffsetEntry{}
	}
	payload, err := codec.EncodeOffsetFetchRequest(codec.OffsetFetchRequest{
		GroupID: group,
		Entries: entries,
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
	return resp.Entries, nil
}

// JoinGroup joins a consumer group. First join sends empty member_id
// (broker assigns one). sessionTimeoutMs 0 defaults to 10000.
func (c *Client) JoinGroup(group string, topics []string, sessionTimeoutMs int) (JoinGroupResult, error) {
	return c.joinGroup(group, "", topics, sessionTimeoutMs, "")
}

func (c *Client) joinGroup(group, memberID string, topics []string, sessionTimeoutMs int, instanceID string) (JoinGroupResult, error) {
	timeout := uint32(sessionTimeoutMs)
	if timeout == 0 {
		timeout = 10_000
	}
	if topics == nil {
		topics = []string{}
	}
	payload, err := codec.EncodeJoinGroupRequest(codec.JoinGroupRequest{
		GroupID:          group,
		MemberID:         memberID,
		SessionTimeoutMs: timeout,
		Topics:           topics,
		GroupInstanceID:  instanceID,
	})
	if err != nil {
		return JoinGroupResult{}, err
	}
	decoded, err := c.roundTrip(codec.OpJoinGroup, payload)
	if err != nil {
		return JoinGroupResult{}, err
	}
	resp, ok := decoded.(codec.JoinGroupResponse)
	if !ok {
		return JoinGroupResult{}, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for join_group: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "join_group"); err != nil {
		return JoinGroupResult{}, err
	}
	return JoinGroupResult{
		MemberID:   resp.MemberID,
		Generation: resp.Generation,
		Assignment: resp.Assignment,
		Revoked:    resp.Revoked,
	}, nil
}

// Heartbeat keeps a group member alive. Non-zero error_code is BrokerError
// (9 = rebalance in progress).
func (c *Client) Heartbeat(group, memberID string, generation uint32) error {
	payload, err := codec.EncodeHeartbeatRequest(codec.HeartbeatRequest{
		GroupID:    group,
		MemberID:   memberID,
		Generation: generation,
	})
	if err != nil {
		return err
	}
	decoded, err := c.roundTrip(codec.OpHeartbeat, payload)
	if err != nil {
		return err
	}
	resp, ok := decoded.(codec.HeartbeatResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for heartbeat: %T", decoded)}
	}
	return check(resp.ErrorCode, "heartbeat")
}

// LeaveGroup leaves a consumer group.
func (c *Client) LeaveGroup(group, memberID string) error {
	payload, err := codec.EncodeLeaveGroupRequest(codec.LeaveGroupRequest{
		GroupID:  group,
		MemberID: memberID,
	})
	if err != nil {
		return err
	}
	decoded, err := c.roundTrip(codec.OpLeaveGroup, payload)
	if err != nil {
		return err
	}
	resp, ok := decoded.(codec.LeaveGroupResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for leave_group: %T", decoded)}
	}
	return check(resp.ErrorCode, "leave_group")
}
