// Package volant is a synchronous TCP client for the native Volant protocol.
//
// This is not a Kafka client and does not speak the Kafka shim (--kafka-listen).
package volant

import (
	"crypto/hmac"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

// Version is the client library version (crate stays 0.2.0).
const Version = "0.2.0"

const defaultTimeout = 10 * time.Second

// notLeaderForPartition is native ErrorCode::NotLeaderForPartition.
const notLeaderForPartition uint16 = 13

// unknownProducerID is native ErrorCode::UnknownProducerId.
const unknownProducerID uint16 = 21

type seqKey struct {
	topic     string
	partition int32
}

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
	GroupListing     = codec.GroupListing
	GroupMemberInfo  = codec.GroupMemberInfo
	GroupState       = codec.GroupState
	OffsetListing    = codec.OffsetListing
)

// DeleteRecordsResult is the successful DeleteRecords reply (Phase 14 / v0.52).
type DeleteRecordsResult struct {
	Topic        string
	Partition    uint32
	LowWatermark uint64
}

const (
	// GroupStateEmpty is offsets on disk only; no live members.
	GroupStateEmpty = codec.GroupStateEmpty
	// GroupStateStable is at least one live member.
	GroupStateStable = codec.GroupStateStable
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

// DescribeGroupResult is the successful DescribeGroup reply (Phase 11 / v0.49).
type DescribeGroupResult struct {
	GroupID    string
	Generation uint32
	Members    []GroupMemberInfo
}

// Client is a sync TCP client for the native Volant protocol (MVP).
type Client struct {
	addr              string
	conn              net.Conn
	timeout           time.Duration
	nextCorr          uint32
	buf               []byte
	tls               bool
	tlsCfg            TLSConfig
	authToken         string
	scramUser         string
	scramPass         string
	maxRedirects      int
	enableIdempotence bool
	producerID        uint64
	producerEpoch     uint16
	producerReady     bool
	nextSeq           map[seqKey]int32
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
	return dialPlain(addr, timeout, "", "", "")
}

// DialAuth is [Dial] plus a shared-token Auth (opcode 30) after connect.
// An empty token skips Auth (same as [Dial]).
func DialAuth(addr, token string) (*Client, error) {
	return dialPlain(addr, defaultTimeout, token, "", "")
}

// DialScram is [Dial] plus SCRAM-SHA-256 after connect (v0.46).
// Username and password must both be non-empty.
func DialScram(addr, user, pass string) (*Client, error) {
	if err := checkScramPair(user, pass); err != nil {
		return nil, err
	}
	return dialPlain(addr, defaultTimeout, "", user, pass)
}

func dialPlain(addr string, timeout time.Duration, token, scramUser, scramPass string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	c := &Client{
		addr:         addr,
		conn:         conn,
		timeout:      timeout,
		nextCorr:     1,
		authToken:    token,
		scramUser:    scramUser,
		scramPass:    scramPass,
		maxRedirects: 1,
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
	return dialTLS(addr, cfg, timeout, "", "", "")
}

// DialTLSAuth is [DialTLS] plus a shared-token Auth after the TLS handshake.
// An empty token skips Auth (same as [DialTLS]).
func DialTLSAuth(addr string, cfg TLSConfig, token string) (*Client, error) {
	return dialTLS(addr, cfg, defaultTimeout, token, "", "")
}

// DialTLSScram is [DialTLS] plus SCRAM-SHA-256 after the handshake (v0.46).
// Username and password must both be non-empty.
func DialTLSScram(addr string, cfg TLSConfig, user, pass string) (*Client, error) {
	if err := checkScramPair(user, pass); err != nil {
		return nil, err
	}
	return dialTLS(addr, cfg, defaultTimeout, "", user, pass)
}

func dialTLS(addr string, cfg TLSConfig, timeout time.Duration, token, scramUser, scramPass string) (*Client, error) {
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
		addr:         addr,
		conn:         tlsConn,
		timeout:      timeout,
		nextCorr:     1,
		tls:          true,
		tlsCfg:       cfg,
		authToken:    token,
		scramUser:    scramUser,
		scramPass:    scramPass,
		maxRedirects: 1,
	}
	if err := c.maybeAuthenticate(); err != nil {
		return nil, err
	}
	return c, nil
}

// SetMaxRedirects sets extra Produce/Fetch attempts after NotLeaderForPartition
// (error 13). Default is 1 (one initial send + one redirect). 0 disables
// redirect and raises on the first 13. Negative values are treated as 0.
func (c *Client) SetMaxRedirects(n int) {
	if n < 0 {
		n = 0
	}
	c.maxRedirects = n
}

// EnableIdempotence turns on InitProducerId + per-partition produce sequences.
// Default is off (trailer (0, 0, -1)). Call before the first Produce.
func (c *Client) EnableIdempotence() {
	c.enableIdempotence = true
	if c.nextSeq == nil {
		c.nextSeq = make(map[seqKey]int32)
	}
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

func checkScramPair(user, pass string) error {
	if user == "" || pass == "" {
		return fmt.Errorf("scram username and password must both be set")
	}
	return nil
}

func (c *Client) maybeAuthenticate() error {
	if c.authToken != "" {
		if err := c.authenticate(c.authToken); err != nil {
			_ = c.Close()
			return err
		}
		return nil
	}
	if c.scramUser != "" && c.scramPass != "" {
		if err := c.authenticateScram(c.scramUser, c.scramPass); err != nil {
			_ = c.Close()
			return err
		}
		return nil
	}
	if c.scramUser != "" || c.scramPass != "" {
		_ = c.Close()
		return fmt.Errorf("scram username and password must both be set")
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

func (c *Client) authenticateScram(username, password string) error {
	clientNonce, err := generateClientNonce()
	if err != nil {
		return err
	}
	payload, err := codec.EncodeScramFirstRequest(codec.ScramFirstRequest{
		Username:    username,
		ClientNonce: clientNonce,
	})
	if err != nil {
		return err
	}
	decoded, err := c.roundTrip(codec.OpScramFirst, payload)
	if err != nil {
		return err
	}
	first, ok := decoded.(codec.ScramFirstResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for scram first: %T", decoded)}
	}
	if err := check(first.ErrorCode, "scram first"); err != nil {
		return err
	}
	proof, expectedSig, err := ClientProofAndServerSig(
		username, password, clientNonce, first.CombinedNonce, first.Salt, first.Iterations,
	)
	if err != nil {
		return err
	}
	payload, err = codec.EncodeScramFinalRequest(codec.ScramFinalRequest{
		Username:      username,
		CombinedNonce: first.CombinedNonce,
		ClientProof:   proof,
	})
	if err != nil {
		return err
	}
	decoded, err = c.roundTrip(codec.OpScramFinal, payload)
	if err != nil {
		return err
	}
	final, ok := decoded.(codec.ScramFinalResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for scram final: %T", decoded)}
	}
	if err := check(final.ErrorCode, "scram final"); err != nil {
		return err
	}
	if !hmac.Equal(final.ServerSignature, expectedSig) {
		return &frame.ProtocolError{Msg: "scram server signature mismatch"}
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

// CreatePartitions grows topic to totalCount partitions (native opcode 46).
// totalCount must exceed the current count. Returns the new total. Non-zero
// error_code is BrokerError. This is not Kafka CreatePartitions (API key 37).
func (c *Client) CreatePartitions(topic string, totalCount uint32) (uint32, error) {
	payload, err := codec.EncodeCreatePartitionsRequest(codec.CreatePartitionsRequest{
		Topic:      topic,
		TotalCount: totalCount,
	})
	if err != nil {
		return 0, err
	}
	decoded, err := c.roundTrip(codec.OpCreatePartitions, payload)
	if err != nil {
		return 0, err
	}
	resp, ok := decoded.(codec.CreatePartitionsResponse)
	if !ok {
		return 0, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for create_partitions: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "create_partitions"); err != nil {
		return 0, err
	}
	return resp.Partitions, nil
}

// ReassignPartitions reassigns replicas for topic (native opcode 114).
// A nil partition updates every partition (wire u32::MAX). Nil or empty
// replicas asks the controller to auto-place with the current membership
// (same as CreateTopic). Returns the assignment generation. Non-zero
// error_code is BrokerError. This is not Kafka AlterPartitionReassignments
// (API key 45).
func (c *Client) ReassignPartitions(topic string, replicas []uint32, partition *uint32) (uint32, error) {
	part := codec.ReassignAllPartitions
	if partition != nil {
		part = *partition
	}
	payload, err := codec.EncodeReassignPartitionsRequest(codec.ReassignPartitionsRequest{
		Topic:     topic,
		Partition: part,
		Replicas:  replicas,
	})
	if err != nil {
		return 0, err
	}
	decoded, err := c.roundTrip(codec.OpReassignPartitions, payload)
	if err != nil {
		return 0, err
	}
	resp, ok := decoded.(codec.ReassignPartitionsResponse)
	if !ok {
		return 0, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for reassign_partitions: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "reassign_partitions"); err != nil {
		return 0, err
	}
	return resp.Generation, nil
}

// Produce sends one message (null key when key is nil) with acks=1.
// Default trailer is (0, 0, -1). After EnableIdempotence the first produce
// sends InitProducerId (empty transactional_id) and later produces attach
// pid/epoch/seq. Returns the broker-assigned base offset.
func (c *Client) Produce(topic string, partition int, key, value []byte) (int64, error) {
	if value == nil {
		value = []byte{}
	}
	reinitBudget := 0
	if c.enableIdempotence {
		reinitBudget = 1
	}
	for {
		payload, err := c.encodeProduce(topic, partition, key, value)
		if err != nil {
			return 0, err
		}
		maxAttempts := 1 + c.maxRedirects
		retriedUnknown := false
		for attempt := 1; ; attempt++ {
			decoded, err := c.roundTrip(codec.OpProduce, payload)
			if err != nil {
				if be, ok := err.(*codec.BrokerError); ok && be.Code == unknownProducerID && reinitBudget > 0 {
					reinitBudget--
					c.resetProducerID()
					retriedUnknown = true
					break
				}
				if be, ok := err.(*codec.BrokerError); ok && be.Code == notLeaderForPartition && attempt < maxAttempts && partition >= 0 {
					ok, rerr := c.redirectToLeader(topic, uint32(partition))
					if rerr != nil {
						return 0, rerr
					}
					if ok {
						continue
					}
				}
				return 0, err
			}
			resp, ok := decoded.(codec.ProduceResponse)
			if !ok {
				return 0, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for produce: %T", decoded)}
			}
			if resp.ErrorCode == unknownProducerID && reinitBudget > 0 {
				reinitBudget--
				c.resetProducerID()
				retriedUnknown = true
				break
			}
			if resp.ErrorCode == notLeaderForPartition && attempt < maxAttempts {
				ok, rerr := c.redirectToLeader(resp.Topic, resp.Partition)
				if rerr != nil {
					return 0, rerr
				}
				if ok {
					continue
				}
			}
			if err := check(resp.ErrorCode, "produce"); err != nil {
				return 0, err
			}
			seqPart := int32(partition)
			if partition < 0 {
				seqPart = int32(resp.Partition)
			}
			c.noteProduceSuccess(topic, seqPart, 1)
			return int64(resp.BaseOffset), nil
		}
		if !retriedUnknown {
			return 0, &frame.ProtocolError{Msg: "produce loop exited"}
		}
	}
}

func (c *Client) encodeProduce(topic string, partition int, key, value []byte) ([]byte, error) {
	pid, epoch, seq, err := c.produceTrailer(topic, int32(partition))
	if err != nil {
		return nil, err
	}
	return codec.EncodeProduceRequest(codec.ProduceRequest{
		Topic:     topic,
		Partition: int32(partition),
		Acks:      1,
		Messages: []codec.ProduceMessage{
			{Key: key, Value: value, TimestampMs: -1},
		},
		ProducerID:    pid,
		ProducerEpoch: epoch,
		BaseSequence:  seq,
	})
}

func (c *Client) produceTrailer(topic string, partition int32) (uint64, uint16, int32, error) {
	if !c.enableIdempotence {
		return 0, 0, -1, nil
	}
	if err := c.ensureProducerID(); err != nil {
		return 0, 0, 0, err
	}
	if c.nextSeq == nil {
		c.nextSeq = make(map[seqKey]int32)
	}
	return c.producerID, c.producerEpoch, c.nextSeq[seqKey{topic: topic, partition: partition}], nil
}

func (c *Client) noteProduceSuccess(topic string, partition int32, count int32) {
	if !c.enableIdempotence {
		return
	}
	if c.nextSeq == nil {
		c.nextSeq = make(map[seqKey]int32)
	}
	key := seqKey{topic: topic, partition: partition}
	c.nextSeq[key] = c.nextSeq[key] + count
}

func (c *Client) resetProducerID() {
	c.producerReady = false
	c.producerID = 0
	c.producerEpoch = 0
	c.nextSeq = make(map[seqKey]int32)
}

func (c *Client) ensureProducerID() error {
	if c.producerReady {
		return nil
	}
	payload, err := codec.EncodeInitProducerIdRequest(codec.InitProducerIdRequest{TransactionalID: ""})
	if err != nil {
		return err
	}
	decoded, err := c.roundTrip(codec.OpInitProducerId, payload)
	if err != nil {
		return err
	}
	resp, ok := decoded.(codec.InitProducerIdResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for init_producer_id: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "init_producer_id"); err != nil {
		return err
	}
	c.producerID = resp.ProducerID
	c.producerEpoch = resp.Epoch
	c.producerReady = true
	return nil
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
	maxAttempts := 1 + c.maxRedirects
	for attempt := 1; ; attempt++ {
		decoded, err := c.roundTrip(codec.OpFetch, payload)
		if err != nil {
			if be, ok := err.(*codec.BrokerError); ok && be.Code == notLeaderForPartition && attempt < maxAttempts {
				ok, rerr := c.redirectToLeader(topic, uint32(partition))
				if rerr != nil {
					return nil, rerr
				}
				if ok {
					continue
				}
			}
			return nil, err
		}
		resp, ok := decoded.(codec.FetchResponse)
		if !ok {
			return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for fetch: %T", decoded)}
		}
		if resp.ErrorCode == notLeaderForPartition && attempt < maxAttempts {
			ok, rerr := c.redirectToLeader(resp.Topic, resp.Partition)
			if rerr != nil {
				return nil, rerr
			}
			if ok {
				continue
			}
		}
		if err := check(resp.ErrorCode, "fetch"); err != nil {
			return nil, err
		}
		return resp.Records, nil
	}
}

// redirectToLeader refreshes Metadata and reconnects to the partition leader.
// ok is false when Metadata has no leader / unknown broker / empty host
// (caller should surface the original error 13).
func (c *Client) redirectToLeader(topic string, partition uint32) (bool, error) {
	meta, err := c.Metadata()
	if err != nil {
		return false, err
	}
	var leaderID uint32
	found := false
	for _, t := range meta.Topics {
		if t.Name != topic {
			continue
		}
		for _, p := range t.Partitions {
			if p.PartitionID == partition {
				leaderID = p.Leader
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		return false, nil
	}
	var broker *codec.BrokerInfo
	for i := range meta.Brokers {
		if meta.Brokers[i].NodeID == leaderID {
			broker = &meta.Brokers[i]
			break
		}
	}
	if broker == nil || broker.Host == "" {
		return false, nil
	}
	addr := net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))
	if addr == c.addr {
		return true, nil
	}
	if err := c.reconnect(addr); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Client) reconnect(addr string) error {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.buf = nil
	conn, err := net.DialTimeout("tcp", addr, c.timeout)
	if err != nil {
		return err
	}
	if c.tls {
		if c.timeout > 0 {
			_ = conn.SetDeadline(time.Now().Add(c.timeout))
		}
		tlsConn, err := wrapTLS(conn, addr, c.tlsCfg)
		if err != nil {
			_ = conn.Close()
			return err
		}
		if c.timeout > 0 {
			_ = tlsConn.SetDeadline(time.Time{})
		}
		conn = tlsConn
	}
	c.conn = conn
	c.addr = addr
	return c.maybeAuthenticate()
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

// ListOffsets returns earliest/latest offsets for topic (native opcode 48).
// Nil or empty partitions means all partitions (wire count 0). Non-zero
// error_code is BrokerError. This is not Kafka ListOffsets (no timestamp
// or isolation); both ends of each log are returned.
func (c *Client) ListOffsets(topic string, partitions []uint32) ([]OffsetListing, error) {
	if partitions == nil {
		partitions = []uint32{}
	}
	payload, err := codec.EncodeListOffsetsRequest(codec.ListOffsetsRequest{
		Topic:      topic,
		Partitions: partitions,
	})
	if err != nil {
		return nil, err
	}
	decoded, err := c.roundTrip(codec.OpListOffsets, payload)
	if err != nil {
		return nil, err
	}
	resp, ok := decoded.(codec.ListOffsetsResponse)
	if !ok {
		return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for list_offsets: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "list_offsets"); err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

// DeleteRecords truncates records before beforeOffset (native opcode 44).
// Sends wait_majority 0 (broker default; Phase 137). Error 13 is not
// redirected (Produce/Fetch only). This is not Kafka DeleteRecords.

// DescribeConfigsResult is the successful DescribeConfigs reply (Phase 13 / v0.53).
type DescribeConfigsResult struct {
	Topic          string
	TopicID        uint32
	PartitionCount uint32
	Configs        [][2]string
}


// DescribeConfigs returns topic configuration (native opcode 40/41).
// Topic configs only (not Kafka DescribeConfigs / BROKER). Empty values
// mean the key is unset. Non-zero error_code is BrokerError with
// Op "describe_configs".

// DeleteOffsets deletes committed offsets for group (native opcode 38).
// Nil or empty entries deletes all offsets for the group (wire count 0).
// Returns the number of offset files removed. Non-zero error_code is
// BrokerError. This is not Kafka OffsetDelete.
func (c *Client) DeleteOffsets(group string, entries []codec.OffsetEntry) (uint32, error) {
	if entries == nil {
		entries = []codec.OffsetEntry{}
	}
	payload, err := codec.EncodeDeleteOffsetsRequest(codec.DeleteOffsetsRequest{
		GroupID: group,
		Entries: entries,
	})
	if err != nil {
		return 0, err
	}
	decoded, err := c.roundTrip(codec.OpDeleteOffsets, payload)
	if err != nil {
		return 0, err
	}
	resp, ok := decoded.(codec.DeleteOffsetsResponse)
	if !ok {
		return 0, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for delete_offsets: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "delete_offsets"); err != nil {
		return 0, err
	}
	return resp.DeletedCount, nil
}

func (c *Client) DescribeConfigs(topic string) (DescribeConfigsResult, error) {
	payload, err := codec.EncodeDescribeConfigsRequest(codec.DescribeConfigsRequest{Topic: topic})
	if err != nil {
		return DescribeConfigsResult{}, err
	}
	decoded, err := c.roundTrip(codec.OpDescribeConfigs, payload)
	if err != nil {
		return DescribeConfigsResult{}, err
	}
	resp, ok := decoded.(codec.DescribeConfigsResponse)
	if !ok {
		return DescribeConfigsResult{}, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for describe_configs: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "describe_configs"); err != nil {
		return DescribeConfigsResult{}, err
	}
	return DescribeConfigsResult{
		Topic:          resp.Topic,
		TopicID:        resp.TopicID,
		PartitionCount: resp.PartitionCount,
		Configs:        resp.Configs,
	}, nil
}


// AlterConfigs updates topic configuration (native opcode 42/43).
// Empty value clears that key (same as Rust). Topic configs only.
// Non-zero error_code is BrokerError with Op "alter_configs".
func (c *Client) AlterConfigs(topic string, configs [][2]string) error {
	if configs == nil {
		configs = [][2]string{}
	}
	payload, err := codec.EncodeAlterConfigsRequest(codec.AlterConfigsRequest{
		Topic:   topic,
		Configs: configs,
	})
	if err != nil {
		return err
	}
	decoded, err := c.roundTrip(codec.OpAlterConfigs, payload)
	if err != nil {
		return err
	}
	resp, ok := decoded.(codec.AlterConfigsResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for alter_configs: %T", decoded)}
	}
	return check(resp.ErrorCode, "alter_configs")
}

func (c *Client) DeleteRecords(topic string, partition uint32, beforeOffset uint64) (DeleteRecordsResult, error) {
	return c.DeleteRecordsWithWaitFlag(topic, partition, beforeOffset, 0)
}

// DeleteRecordsWithWaitFlag is DeleteRecords plus the Phase 137 trailer.
// waitMajority: 0 = broker default, 1 = force wait, 2 = force no-wait.
func (c *Client) DeleteRecordsWithWaitFlag(topic string, partition uint32, beforeOffset uint64, waitMajority uint8) (DeleteRecordsResult, error) {
	payload, err := codec.EncodeDeleteRecordsRequest(codec.DeleteRecordsRequest{
		Topic:        topic,
		Partition:    partition,
		BeforeOffset: beforeOffset,
		WaitMajority: waitMajority,
	})
	if err != nil {
		return DeleteRecordsResult{}, err
	}
	decoded, err := c.roundTrip(codec.OpDeleteRecords, payload)
	if err != nil {
		return DeleteRecordsResult{}, err
	}
	resp, ok := decoded.(codec.DeleteRecordsResponse)
	if !ok {
		return DeleteRecordsResult{}, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for delete_records: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "delete_records"); err != nil {
		return DeleteRecordsResult{}, err
	}
	return DeleteRecordsResult{
		Topic:        resp.Topic,
		Partition:    resp.Partition,
		LowWatermark: resp.LowWatermark,
	}, nil
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

// DescribeGroup describes a live consumer group (native opcode 34/35).
// Error 2 (NotFound, no live members) is a BrokerError.
func (c *Client) DescribeGroup(id string) (DescribeGroupResult, error) {
	payload, err := codec.EncodeDescribeGroupRequest(codec.DescribeGroupRequest{GroupID: id})
	if err != nil {
		return DescribeGroupResult{}, err
	}
	decoded, err := c.roundTrip(codec.OpDescribeGroup, payload)
	if err != nil {
		return DescribeGroupResult{}, err
	}
	resp, ok := decoded.(codec.DescribeGroupResponse)
	if !ok {
		return DescribeGroupResult{}, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for describe_group: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "describe_group"); err != nil {
		return DescribeGroupResult{}, err
	}
	return DescribeGroupResult{
		GroupID:    resp.GroupID,
		Generation: resp.Generation,
		Members:    resp.Members,
	}, nil
}

// ListGroups lists known consumer groups (native opcode 36/37).
func (c *Client) ListGroups() ([]GroupListing, error) {
	decoded, err := c.roundTrip(codec.OpListGroups, codec.EncodeListGroupsRequest())
	if err != nil {
		return nil, err
	}
	resp, ok := decoded.(codec.ListGroupsResponse)
	if !ok {
		return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for list_groups: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "list_groups"); err != nil {
		return nil, err
	}
	return resp.Groups, nil
}

// CreateScramUser creates or replaces a SCRAM user (native opcode 64/65).
// iterations 0 means the broker default (4096). Password is sent in the
// clear (use TLS). This is not the v0.46 handshake (60–63).
func (c *Client) CreateScramUser(username, password string, iterations uint32) error {
	payload, err := codec.EncodeCreateScramUserRequest(codec.CreateScramUserRequest{
		Username:   username,
		Password:   password,
		Iterations: iterations,
	})
	if err != nil {
		return err
	}
	decoded, err := c.roundTrip(codec.OpCreateScramUser, payload)
	if err != nil {
		return err
	}
	resp, ok := decoded.(codec.CreateScramUserResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for create_scram_user: %T", decoded)}
	}
	return check(resp.ErrorCode, "create_scram_user")
}

// DeleteScramUser deletes a SCRAM user (native opcode 66/67).
func (c *Client) DeleteScramUser(username string) error {
	payload, err := codec.EncodeDeleteScramUserRequest(codec.DeleteScramUserRequest{Username: username})
	if err != nil {
		return err
	}
	decoded, err := c.roundTrip(codec.OpDeleteScramUser, payload)
	if err != nil {
		return err
	}
	resp, ok := decoded.(codec.DeleteScramUserResponse)
	if !ok {
		return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for delete_scram_user: %T", decoded)}
	}
	return check(resp.ErrorCode, "delete_scram_user")
}

// ListScramUsers lists SCRAM usernames (native opcode 68/69).
func (c *Client) ListScramUsers() ([]string, error) {
	decoded, err := c.roundTrip(codec.OpListScramUsers, codec.EncodeListScramUsersRequest())
	if err != nil {
		return nil, err
	}
	resp, ok := decoded.(codec.ListScramUsersResponse)
	if !ok {
		return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for list_scram_users: %T", decoded)}
	}
	if err := check(resp.ErrorCode, "list_scram_users"); err != nil {
		return nil, err
	}
	return resp.Usernames, nil
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
