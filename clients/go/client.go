// Package volant is a synchronous TCP client for the native Volant protocol.
//
// This is not a Kafka client and does not speak the Kafka shim (--kafka-listen).
package volant

import (
	"crypto/hmac"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
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

// notController is native ErrorCode::NotController (controller-gated admin).
const notController uint16 = 14

var controllerIDRe = regexp.MustCompile(`controller_id=(\d+)`)

func parseControllerID(msg string) *uint32 {
	m := controllerIDRe.FindStringSubmatch(msg)
	if m == nil {
		return nil
	}
	n, err := strconv.ParseUint(m[1], 10, 32)
	if err != nil {
		return nil
	}
	id := uint32(n)
	return &id
}

// unknownProducerID is native ErrorCode::UnknownProducerId.
const unknownProducerID uint16 = 21

// invalidTxnState is native ErrorCode::InvalidTxnState.
const invalidTxnState uint16 = 22

// Transient produce retry codes (match Rust is_transient_error_code).
const (
	errIO                 uint16 = 6
	errTimeout            uint16 = 7
	errNotEnoughReplicas  uint16 = 15
	errBrokerNotAvailable uint16 = 16
)

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
	AclBinding       = codec.AclBinding
	MembershipBroker = codec.MembershipBroker
	MembershipList   = codec.MembershipList
)

// DeleteRecordsResult is the successful DeleteRecords reply (Phase 14 / v0.52).
type DeleteRecordsResult struct {
	Topic        string
	Partition    uint32
	LowWatermark uint64
}

// FetchResult is a successful Fetch reply (records plus high watermark).
type FetchResult struct {
	Topic         string
	Partition     uint32
	HighWatermark uint64
	Records       []Record
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

// OffsetFetchEntry is one committed (topic, partition, offset, metadata)
// from OffsetFetchAll.
type OffsetFetchEntry struct {
	Topic     string
	Partition uint32
	Offset    uint64
	Metadata  string
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
	maxRetries        int
	retryBackoff      time.Duration
	acks              uint8
	fetchMaxMessages  uint32
	fetchMaxBytes     uint32
	fetchMaxWaitMs    uint32
	enableIdempotence bool
	transactionalID   string
	producerID        uint64
	producerEpoch     uint16
	producerReady     bool
	nextSeq           map[seqKey]int32
	seqAtBegin        map[seqKey]int32
	inTransaction     bool
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
		maxRedirects:     1,
		retryBackoff:     50 * time.Millisecond,
		acks:             1,
		fetchMaxMessages: 128,
		fetchMaxBytes:    4 * 1024 * 1024,
		fetchMaxWaitMs:   0,
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
		maxRedirects:     1,
		retryBackoff:     50 * time.Millisecond,
		acks:             1,
		fetchMaxMessages: 128,
		fetchMaxBytes:    4 * 1024 * 1024,
		fetchMaxWaitMs:   0,
	}
	if err := c.maybeAuthenticate(); err != nil {
		return nil, err
	}
	return c, nil
}

// SetMaxRedirects sets extra Produce/Fetch/DeleteRecords attempts after
// NotLeaderForPartition (error 13). Default is 1 (one initial send + one
// redirect). 0 disables redirect and raises on the first 13. Negative
// values are treated as 0.
func (c *Client) SetMaxRedirects(n int) {
	if n < 0 {
		n = 0
	}
	c.maxRedirects = n
}

// SetMaxRetries sets extra Produce/Fetch/Heartbeat/SCRAM attempts after
// the first on transient broker/transport errors. Default is 0 (no extra
// attempts). Negative values are treated as 0. Error 13 stays on the
// redirect budget; error 21 stays on the one re-Init. Heartbeat
// rebalance codes 9 / 10 / 11 are not retried. SCRAM 17 / 18 and
// server-signature mismatch are not retried.
func (c *Client) SetMaxRetries(n int) {
	if n < 0 {
		n = 0
	}
	c.maxRetries = n
}

// SetRetryBackoff sets the sleep between produce/fetch retries. Default
// is 50ms. Zero is allowed (tests). Negative values are treated as 0.
func (c *Client) SetRetryBackoff(d time.Duration) {
	if d < 0 {
		d = 0
	}
	c.retryBackoff = d
}

// SetAcks sets the default produce acks used by Produce. 1 = leader
// only; 255 = acks=all (ISR). Default is 1. ProduceAcks / ProduceBatch
// stay explicit.
func (c *Client) SetAcks(acks uint8) {
	c.acks = acks
}

// Acks returns the default produce acks (1 = leader, 255 = all).
func (c *Client) Acks() uint8 {
	return c.acks
}

// SetFetchMaxMessages sets the default Fetch max_messages (default 128).
// 0 is kept as-is (wire-legal; no clamp) so tests can send 0.
func (c *Client) SetFetchMaxMessages(n uint32) {
	c.fetchMaxMessages = n
}

// FetchMaxMessages returns the default Fetch max_messages.
func (c *Client) FetchMaxMessages() uint32 {
	return c.fetchMaxMessages
}

// SetFetchMaxBytes sets the default Fetch max_bytes (default 4MiB).
// 0 is kept as-is (wire-legal; no clamp).
func (c *Client) SetFetchMaxBytes(n uint32) {
	c.fetchMaxBytes = n
}

// FetchMaxBytes returns the default Fetch max_bytes.
func (c *Client) FetchMaxBytes() uint32 {
	return c.fetchMaxBytes
}

// SetFetchMaxWaitMs sets the default Fetch max_wait_ms (default 0).
// 0 is kept as-is (wire-legal; no clamp).
func (c *Client) SetFetchMaxWaitMs(n uint32) {
	c.fetchMaxWaitMs = n
}

// FetchMaxWaitMs returns the default Fetch max_wait_ms.
func (c *Client) FetchMaxWaitMs() uint32 {
	return c.fetchMaxWaitMs
}

// EnableIdempotence turns on InitProducerId + per-partition produce sequences.
// Default is off (trailer (0, 0, -1)). Call before the first Produce.
func (c *Client) EnableIdempotence() {
	c.enableIdempotence = true
	if c.nextSeq == nil {
		c.nextSeq = make(map[seqKey]int32)
	}
}

// InitProducerID ensures InitProducerId has run (native opcode 32).
// Returns the stored producer id and epoch. A second call is a no-op
// (already ready). Produce / BeginTxn still init implicitly.
func (c *Client) InitProducerID() (producerID uint64, epoch uint16, err error) {
	if err := c.ensureProducerID(); err != nil {
		return 0, 0, err
	}
	return c.producerID, c.producerEpoch, nil
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
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	retryAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpAuth, payload)
		if err != nil {
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return err
		}
		resp, ok := decoded.(codec.AuthResponse)
		if !ok {
			return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for auth: %T", decoded)}
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		return check(resp.ErrorCode, "auth")
	}
}

func (c *Client) authenticateScram(username, password string) error {
	// First+final is one unit (v0.108): a transient first or final
	// restarts from first with a new client nonce. 17 / 18 and
	// protocol errors (including signature mismatch) are not retried.
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	retryAttempt := 0
	for {
		err := c.scramHandshake(username, password)
		if err == nil {
			return nil
		}
		if isTransientProduceErr(err) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		return err
	}
}

func (c *Client) scramHandshake(username, password string) error {
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

// adminRoundTrip sends a controller-gated admin RPC. Error 14 follows
// maxRedirects (not counted as a transient retry). Transient 6 / 7 /
// 15 / 16 and TCP/IO retry up to maxRetries extra times (default 0).
func (c *Client) adminRoundTrip(opcode uint16, payload []byte, op string) (any, error) {
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxAttempts := 1 + c.maxRedirects
	retryAttempt := 0
	for attempt := 1; ; attempt++ {
		decoded, err := c.roundTrip(opcode, payload)
		if err != nil {
			ok, rerr := c.maybeRedirectController(err, attempt, maxAttempts)
			if rerr != nil {
				return nil, rerr
			}
			if ok {
				continue
			}
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				attempt--
				c.sleepProduceRetry()
				continue
			}
			return nil, err
		}
		code, ok := typedAdminErrorCode(decoded)
		if !ok {
			return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for %s: %T", op, decoded)}
		}
		ok, rerr := c.maybeRedirectControllerCode(code, "", attempt, maxAttempts)
		if rerr != nil {
			return nil, rerr
		}
		if ok {
			continue
		}
		if isTransientBroker(code) && retryAttempt < maxRetries {
			retryAttempt++
			attempt--
			c.sleepProduceRetry()
			continue
		}
		if err := check(code, op); err != nil {
			return nil, err
		}
		return decoded, nil
	}
}

func typedAdminErrorCode(decoded any) (uint16, bool) {
	switch resp := decoded.(type) {
	case codec.CreateTopicResponse:
		return resp.ErrorCode, true
	case codec.DeleteTopicResponse:
		return resp.ErrorCode, true
	case codec.CreatePartitionsResponse:
		return resp.ErrorCode, true
	case codec.ReassignPartitionsResponse:
		return resp.ErrorCode, true
	case codec.CreateAclsResponse:
		return resp.ErrorCode, true
	case codec.DeleteAclsResponse:
		return resp.ErrorCode, true
	case codec.CreateScramUserResponse:
		return resp.ErrorCode, true
	case codec.DeleteScramUserResponse:
		return resp.ErrorCode, true
	case codec.ListScramUsersResponse:
		return resp.ErrorCode, true
	case codec.ListAclsResponse:
		return resp.ErrorCode, true
	case codec.AddBrokerResponse:
		return resp.ErrorCode, true
	case codec.RemoveBrokerResponse:
		return resp.ErrorCode, true
	case codec.DescribeConfigsResponse:
		return resp.ErrorCode, true
	case codec.AlterConfigsResponse:
		return resp.ErrorCode, true
	default:
		return 0, false
	}
}

// CreateTopic creates a topic with the given partition count.
// Error 14 (NotController) follows maxRedirects (same budget as Produce/Fetch 13).
// Transient 6 / 7 / 15 / 16 and TCP/IO follow maxRetries (default 0); 14 is not a retry.
func (c *Client) CreateTopic(name string, partitions int) error {
	_, err := c.CreateTopicWithConfigs(name, partitions, nil)
	return err
}

// CreateTopicID is CreateTopic but returns the broker-assigned topic id
// (same as CreateTopicWithConfigs / Python create_topic / Java createTopic).
// CreateTopic stays error-only. Error 14 and transient retry inherit
// adminRoundTrip.
func (c *Client) CreateTopicID(name string, partitions int) (uint32, error) {
	return c.CreateTopicWithConfigs(name, partitions, nil)
}

// CreateTopicWithConfigs is CreateTopic plus native CreateTopic config pairs
// (Phase 13 trailer; same as Python configs= / Rust create_topic_with_configs).
// Empty value is allowed. Returns the broker-assigned topic id. This is not
// Kafka CreateTopics configs / IncrementalAlterConfigs. Error 14 and
// transient retry inherit adminRoundTrip.
func (c *Client) CreateTopicWithConfigs(name string, partitions int, configs [][2]string) (uint32, error) {
	if configs == nil {
		configs = [][2]string{}
	}
	payload, err := codec.EncodeCreateTopicRequest(codec.CreateTopicRequest{
		Name:       name,
		Partitions: uint32(partitions),
		Configs:    configs,
	})
	if err != nil {
		return 0, err
	}
	decoded, err := c.adminRoundTrip(codec.OpCreateTopic, payload, "create_topic")
	if err != nil {
		return 0, err
	}
	return decoded.(codec.CreateTopicResponse).TopicID, nil
}

// DeleteTopic deletes a topic by name.
// Error 14 follows maxRedirects.
func (c *Client) DeleteTopic(name string) error {
	payload, err := codec.EncodeDeleteTopicRequest(codec.DeleteTopicRequest{Name: name})
	if err != nil {
		return err
	}
	_, err = c.adminRoundTrip(codec.OpDeleteTopic, payload, "delete_topic")
	return err
}

// CreatePartitions grows topic to totalCount partitions (native opcode 46).
// totalCount must exceed the current count. Returns the new total. Non-zero
// error_code is BrokerError. Error 14 follows maxRedirects. This is not Kafka
// CreatePartitions (API key 37).
func (c *Client) CreatePartitions(topic string, totalCount uint32) (uint32, error) {
	payload, err := codec.EncodeCreatePartitionsRequest(codec.CreatePartitionsRequest{
		Topic:      topic,
		TotalCount: totalCount,
	})
	if err != nil {
		return 0, err
	}
	decoded, err := c.adminRoundTrip(codec.OpCreatePartitions, payload, "create_partitions")
	if err != nil {
		return 0, err
	}
	return decoded.(codec.CreatePartitionsResponse).Partitions, nil
}

// ReassignPartitions reassigns replicas for topic (native opcode 114).
// A nil partition updates every partition (wire u32::MAX). Nil or empty
// replicas asks the controller to auto-place with the current membership
// (same as CreateTopic). Returns the assignment generation. Non-zero
// error_code is BrokerError. This is not Kafka AlterPartitionReassignments
// (API key 45).

// AddBroker adds a broker endpoint to the membership overlay (native 102/103).
// rack nil is absent on the wire (flag 0). Returns the overlay generation.
// Overlay is still SoT; this is not Kafka broker catalog. Error 14 follows
// maxRedirects when the broker cannot forward.

// SetTransactionalID sets the native transactional_id used on InitProducerId
// (opcode 32) and required by BeginTransaction. Empty / unset means
// non-transactional (v0.47 empty-id idempotence still works).
func (c *Client) SetTransactionalID(id string) {
	c.transactionalID = id
	if id != "" && c.nextSeq == nil {
		c.nextSeq = make(map[seqKey]int32)
	}
}

// BeginTransaction opens a native transaction (opcode 50). Requires SetTransactionalID.
// Transient broker/transport errors retry up to maxRetries extra times
// (default 0). InvalidTxnState (22) and txn fence / epoch errors are
// not retried.
func (c *Client) BeginTransaction() error {
	if c.transactionalID == "" {
		return fmt.Errorf("transactional_id not configured")
	}
	if err := c.ensureProducerID(); err != nil {
		return err
	}
	payload, err := codec.EncodeBeginTxnRequest(codec.BeginTxnRequest{
		ProducerID:    c.producerID,
		ProducerEpoch: c.producerEpoch,
	})
	if err != nil {
		return err
	}
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	retryAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpBeginTxn, payload)
		if err != nil {
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return err
		}
		resp, ok := decoded.(codec.BeginTxnResponse)
		if !ok {
			return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for begin_txn: %T", decoded)}
		}
		if resp.ErrorCode == invalidTxnState {
			return check(resp.ErrorCode, "begin_txn")
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		if err := check(resp.ErrorCode, "begin_txn"); err != nil {
			return err
		}
		c.seqAtBegin = make(map[seqKey]int32, len(c.nextSeq))
		for k, v := range c.nextSeq {
			c.seqAtBegin[k] = v
		}
		c.inTransaction = true
		return nil
	}
}

// CommitTransaction ends the open transaction with committed=1 (opcode 52).
// offsets may be nil. Returns per-batch TxnProduceResult rows.
func (c *Client) CommitTransaction(offsets []codec.TxnOffsetCommit) ([]codec.TxnProduceResult, error) {
	return c.endTransaction(true, offsets)
}

// AbortTransaction ends the open transaction with committed=0 and rewinds sequences.
func (c *Client) AbortTransaction() error {
	_, err := c.endTransaction(false, nil)
	return err
}

func (c *Client) endTransaction(committed bool, offsets []codec.TxnOffsetCommit) ([]codec.TxnProduceResult, error) {
	if !c.producerReady {
		return nil, fmt.Errorf("producer id not initialized")
	}
	if offsets == nil {
		offsets = []codec.TxnOffsetCommit{}
	}
	payload, err := codec.EncodeEndTxnRequest(codec.EndTxnRequest{
		ProducerID:    c.producerID,
		ProducerEpoch: c.producerEpoch,
		Committed:     committed,
		Offsets:       offsets,
	})
	if err != nil {
		return nil, err
	}
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	retryAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpEndTxn, payload)
		if err != nil {
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return nil, err
		}
		resp, ok := decoded.(codec.EndTxnResponse)
		if !ok {
			return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for end_txn: %T", decoded)}
		}
		if resp.ErrorCode == invalidTxnState {
			return nil, check(resp.ErrorCode, "end_txn")
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		if err := check(resp.ErrorCode, "end_txn"); err != nil {
			return nil, err
		}
		c.inTransaction = false
		if !committed {
			c.nextSeq = make(map[seqKey]int32, len(c.seqAtBegin))
			for k, v := range c.seqAtBegin {
				c.nextSeq[k] = v
			}
		}
		c.seqAtBegin = make(map[seqKey]int32)
		return resp.Results, nil
	}
}

func (c *Client) AddBroker(id uint32, host string, port uint16, rack *string) (uint64, error) {
	payload, err := codec.EncodeAddBrokerRequest(codec.AddBrokerRequest{
		ID: id, Host: host, Port: port, Rack: rack,
	})
	if err != nil {
		return 0, err
	}
	decoded, err := c.adminRoundTrip(codec.OpAddBroker, payload, "add_broker")
	if err != nil {
		return 0, err
	}
	return decoded.(codec.AddBrokerResponse).Generation, nil
}

// RemoveBroker removes a broker from the membership overlay (native 104/105).
// Returns the overlay generation. Error 14 follows maxRedirects when the
// broker cannot forward.
func (c *Client) RemoveBroker(id uint32) (uint64, error) {
	payload, err := codec.EncodeRemoveBrokerRequest(codec.RemoveBrokerRequest{ID: id})
	if err != nil {
		return 0, err
	}
	decoded, err := c.adminRoundTrip(codec.OpRemoveBroker, payload, "remove_broker")
	if err != nil {
		return 0, err
	}
	return decoded.(codec.RemoveBrokerResponse).Generation, nil
}

// ListMembers lists configured + live membership (native opcode 106/107).
// Overlay is still SoT. Transient broker/transport errors retry up to
// maxRetries extra times (default 0). Error 14 (NotController) uses
// maxRedirects via redirectToController (independent of retry).
// maxRedirects=0 does not redirect. Error 2 / 9 / 10 / 11 / 13 / 17 /
// 18 / 21 / 22 and protocol are not retried or redirected.
func (c *Client) ListMembers() (MembershipList, error) {
	maxAttempts := 1 + c.maxRedirects
	redirectAttempt := 0
	for {
		got, err := c.listMembersRpc()
		if err != nil {
			ok, rerr := c.maybeRedirectController(err, redirectAttempt+1, maxAttempts)
			if rerr != nil {
				return MembershipList{}, rerr
			}
			if ok {
				redirectAttempt++
				continue
			}
			return MembershipList{}, err
		}
		return got, nil
	}
}

// listMembersRpc is ListMembers without the v0.121 error-14 wrap.
// Used by redirectToController so hunt and ListMembers are not
// mutually recursive. Transient retry is still v0.95.
func (c *Client) listMembersRpc() (MembershipList, error) {
	payload := codec.EncodeListMembersRequest()
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	retryAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpListMembers, payload)
		if err != nil {
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return MembershipList{}, err
		}
		resp, ok := decoded.(codec.ListMembersResponse)
		if !ok {
			return MembershipList{}, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for list_members: %T", decoded)}
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		if err := check(resp.ErrorCode, "list_members"); err != nil {
			return MembershipList{}, err
		}
		return MembershipList{
			Generation: resp.Generation,
			Brokers:    resp.Brokers,
			Live:       resp.Live,
		}, nil
	}
}

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
	decoded, err := c.adminRoundTrip(codec.OpReassignPartitions, payload, "reassign_partitions")
	if err != nil {
		return 0, err
	}
	return decoded.(codec.ReassignPartitionsResponse).Generation, nil
}

// Produce sends one message (null key when key is nil) with the client
// default acks (1 unless SetAcks). Default trailer is (0, 0, -1). After
// EnableIdempotence the first produce sends InitProducerId (empty
// transactional_id) and later produces attach pid/epoch/seq. Returns
// the broker-assigned base offset.
func (c *Client) Produce(topic string, partition int, key, value []byte) (int64, error) {
	return c.ProduceAcks(topic, partition, key, value, c.acks)
}

// ProduceAcks is Produce with an explicit acks byte. 1 = leader only;
// 255 = acks=all (ISR). Same as the Rust client / Python acks=.
func (c *Client) ProduceAcks(topic string, partition int, key, value []byte, acks uint8) (int64, error) {
	if value == nil {
		value = []byte{}
	}
	return c.ProduceBatch(topic, partition, []codec.ProduceMessage{
		{Key: key, Value: value, TimestampMs: -1},
	}, acks)
}

// ProduceHeaders is Produce with native record headers on the single
// message and the client default acks. Produce / ProduceAcks still send
// empty headers. Reuses ProduceBatch retry / error 13 / error 21.
func (c *Client) ProduceHeaders(topic string, partition int, key, value []byte, headers []codec.Header) (int64, error) {
	if value == nil {
		value = []byte{}
	}
	return c.ProduceBatch(topic, partition, []codec.ProduceMessage{
		{Key: key, Value: value, TimestampMs: -1, Headers: headers},
	}, c.acks)
}

// ProduceTimestamp is Produce with a caller-supplied native timestamp on
// the single message, the client default acks, and empty headers.
// Produce / ProduceAcks / ProduceHeaders still send TimestampMs: -1
// (broker now). Reuses ProduceBatch retry / error 13 / error 21.
func (c *Client) ProduceTimestamp(topic string, partition int, key, value []byte, timestampMs int64) (int64, error) {
	if value == nil {
		value = []byte{}
	}
	return c.ProduceBatch(topic, partition, []codec.ProduceMessage{
		{Key: key, Value: value, TimestampMs: timestampMs},
	}, c.acks)
}

// ProduceHeadersAcks is ProduceHeaders with an explicit acks byte.
// 1 = leader only; 255 = acks=all (ISR). ProduceHeaders still uses
// the client default acks. Reuses ProduceBatch retry / error 13 /
// error 21.
func (c *Client) ProduceHeadersAcks(topic string, partition int, key, value []byte, headers []codec.Header, acks uint8) (int64, error) {
	if value == nil {
		value = []byte{}
	}
	return c.ProduceBatch(topic, partition, []codec.ProduceMessage{
		{Key: key, Value: value, TimestampMs: -1, Headers: headers},
	}, acks)
}

// ProduceTimestampHeaders is Produce with a caller-supplied native
// timestamp and native record headers on the single message, using
// the client default acks. ProduceTimestamp still sends empty
// headers. ProduceHeaders / ProduceHeadersAcks still send
// TimestampMs: -1. Reuses ProduceBatch retry / error 13 / error 21.
func (c *Client) ProduceTimestampHeaders(topic string, partition int, key, value []byte, timestampMs int64, headers []codec.Header) (int64, error) {
	if value == nil {
		value = []byte{}
	}
	return c.ProduceBatch(topic, partition, []codec.ProduceMessage{
		{Key: key, Value: value, TimestampMs: timestampMs, Headers: headers},
	}, c.acks)
}

// ProduceTimestampAcks is Produce with a caller-supplied native timestamp
// on the single message, empty headers, and an explicit acks byte.
// 1 = leader only; 255 = acks=all (ISR). ProduceTimestamp still uses
// the client default acks. ProduceAcks still sends TimestampMs: -1.
// Reuses ProduceBatch retry / error 13 / error 21.
func (c *Client) ProduceTimestampAcks(topic string, partition int, key, value []byte, timestampMs int64, acks uint8) (int64, error) {
	if value == nil {
		value = []byte{}
	}
	return c.ProduceBatch(topic, partition, []codec.ProduceMessage{
		{Key: key, Value: value, TimestampMs: timestampMs},
	}, acks)
}

// ProduceTimestampHeadersAcks is Produce with a caller-supplied native
// timestamp, native record headers, and an explicit acks byte on the
// single message. 1 = leader only; 255 = acks=all (ISR).
// ProduceTimestampHeaders still uses the client default acks.
// ProduceHeadersAcks still sends TimestampMs: -1. Reuses ProduceBatch
// retry / error 13 / error 21.
func (c *Client) ProduceTimestampHeadersAcks(topic string, partition int, key, value []byte, timestampMs int64, headers []codec.Header, acks uint8) (int64, error) {
	if value == nil {
		value = []byte{}
	}
	return c.ProduceBatch(topic, partition, []codec.ProduceMessage{
		{Key: key, Value: value, TimestampMs: timestampMs, Headers: headers},
	}, acks)
}

// ProduceBatch sends msgs in one Produce RPC. acks: 1 = leader, 255 = all.
func (c *Client) ProduceBatch(topic string, partition int, msgs []codec.ProduceMessage, acks uint8) (int64, error) {
	if len(msgs) == 0 {
		return 0, fmt.Errorf("produce batch is empty")
	}
	reinitBudget := 0
	if c.enableIdempotence {
		reinitBudget = 1
	}
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	retryAttempt := 0
	for {
		payload, err := c.encodeProduce(topic, partition, msgs, acks)
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
				if isTransientProduceErr(err) && retryAttempt < maxRetries {
					retryAttempt++
					attempt--
					c.sleepProduceRetry()
					continue
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
			if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
				retryAttempt++
				attempt--
				c.sleepProduceRetry()
				continue
			}
			if err := check(resp.ErrorCode, "produce"); err != nil {
				return 0, err
			}
			seqPart := int32(partition)
			if partition < 0 {
				seqPart = int32(resp.Partition)
			}
			c.noteProduceSuccess(topic, seqPart, int32(len(msgs)))
			return int64(resp.BaseOffset), nil
		}
		if !retriedUnknown {
			return 0, &frame.ProtocolError{Msg: "produce loop exited"}
		}
	}
}

func isTransientBroker(code uint16) bool {
	switch code {
	case errIO, errTimeout, errNotEnoughReplicas, errBrokerNotAvailable:
		return true
	default:
		return false
	}
}

func isTransientTransport(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*codec.BrokerError); ok {
		return false
	}
	if _, ok := err.(*frame.ProtocolError); ok {
		return false
	}
	var ne net.Error
	return errors.As(err, &ne)
}

func isTransientProduceErr(err error) bool {
	if be, ok := err.(*codec.BrokerError); ok {
		return isTransientBroker(be.Code)
	}
	return isTransientTransport(err)
}

func (c *Client) sleepProduceRetry() {
	if c.retryBackoff > 0 {
		time.Sleep(c.retryBackoff)
	}
}

func (c *Client) encodeProduce(topic string, partition int, msgs []codec.ProduceMessage, acks uint8) ([]byte, error) {
	pid, epoch, seq, err := c.produceTrailer(topic, int32(partition))
	if err != nil {
		return nil, err
	}
	return codec.EncodeProduceRequest(codec.ProduceRequest{
		Topic:         topic,
		Partition:     int32(partition),
		Acks:          acks,
		Messages:      msgs,
		ProducerID:    pid,
		ProducerEpoch: epoch,
		BaseSequence:  seq,
	})
}

func (c *Client) produceTrailer(topic string, partition int32) (uint64, uint16, int32, error) {
	if !c.enableIdempotence && c.transactionalID == "" {
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
	if !c.enableIdempotence && c.transactionalID == "" {
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
	payload, err := codec.EncodeInitProducerIdRequest(codec.InitProducerIdRequest{TransactionalID: c.transactionalID})
	if err != nil {
		return err
	}
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	retryAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpInitProducerId, payload)
		if err != nil {
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return err
		}
		resp, ok := decoded.(codec.InitProducerIdResponse)
		if !ok {
			return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for init_producer_id: %T", decoded)}
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		if err := check(resp.ErrorCode, "init_producer_id"); err != nil {
			return err
		}
		c.producerID = resp.ProducerID
		c.producerEpoch = resp.Epoch
		c.producerReady = true
		return nil
	}
}

// Fetch reads records from topic/partition starting at offset.
// Uses the client default knobs (128 / 4MiB / 0 unless SetFetchMax*).
// FetchOpts still takes explicit knobs.
func (c *Client) Fetch(topic string, partition int, offset int64) ([]Record, error) {
	res, err := c.FetchResult(topic, partition, offset)
	if err != nil {
		return nil, err
	}
	return res.Records, nil
}

// FetchResult is Fetch plus the already-decoded high watermark.
func (c *Client) FetchResult(topic string, partition int, offset int64) (FetchResult, error) {
	return c.FetchOptsResult(topic, partition, offset, c.fetchMaxMessages, c.fetchMaxBytes, c.fetchMaxWaitMs)
}

// FetchOpts is Fetch with explicit max_messages, max_bytes, and max_wait_ms.
// Transient broker/transport errors retry up to maxRetries extra times
// (default 0). Error 13 uses maxRedirects only.
func (c *Client) FetchOpts(topic string, partition int, offset int64, maxMessages, maxBytes, maxWaitMs uint32) ([]Record, error) {
	res, err := c.FetchOptsResult(topic, partition, offset, maxMessages, maxBytes, maxWaitMs)
	if err != nil {
		return nil, err
	}
	return res.Records, nil
}

// FetchOptsResult is FetchOpts plus the already-decoded high watermark.
func (c *Client) FetchOptsResult(topic string, partition int, offset int64, maxMessages, maxBytes, maxWaitMs uint32) (FetchResult, error) {
	payload, err := codec.EncodeFetchRequest(codec.FetchRequest{
		Topic:       topic,
		Partition:   uint32(partition),
		FromOffset:  uint64(offset),
		MaxMessages: maxMessages,
		MaxBytes:    maxBytes,
		MaxWaitMs:   maxWaitMs,
	})
	if err != nil {
		return FetchResult{}, err
	}
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	retryAttempt := 0
	for {
		maxAttempts := 1 + c.maxRedirects
		retried := false
		for attempt := 1; ; attempt++ {
			decoded, err := c.roundTrip(codec.OpFetch, payload)
			if err != nil {
				if be, ok := err.(*codec.BrokerError); ok && be.Code == notLeaderForPartition && attempt < maxAttempts {
					ok, rerr := c.redirectToLeader(topic, uint32(partition))
					if rerr != nil {
						return FetchResult{}, rerr
					}
					if ok {
						continue
					}
				}
				if isTransientProduceErr(err) && retryAttempt < maxRetries {
					retryAttempt++
					c.sleepProduceRetry()
					retried = true
					break
				}
				return FetchResult{}, err
			}
			resp, ok := decoded.(codec.FetchResponse)
			if !ok {
				return FetchResult{}, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for fetch: %T", decoded)}
			}
			if resp.ErrorCode == notLeaderForPartition && attempt < maxAttempts {
				ok, rerr := c.redirectToLeader(resp.Topic, resp.Partition)
				if rerr != nil {
					return FetchResult{}, rerr
				}
				if ok {
					continue
				}
			}
			if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				retried = true
				break
			}
			if err := check(resp.ErrorCode, "fetch"); err != nil {
				return FetchResult{}, err
			}
			return FetchResult{
				Topic:         resp.Topic,
				Partition:     resp.Partition,
				HighWatermark: resp.HighWatermark,
				Records:       resp.Records,
			}, nil
		}
		if !retried {
			return FetchResult{}, &frame.ProtocolError{Msg: "fetch loop exited"}
		}
	}
}

func (c *Client) fetchAt(topic string, partition int, offset int64, maxMessages, maxWaitMs uint32) ([]Record, error) {
	if maxMessages == 0 {
		maxMessages = 128
	}
	return c.FetchOpts(topic, partition, offset, maxMessages, 4*1024*1024, maxWaitMs)
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

func (c *Client) maybeRedirectController(err error, attempt, maxAttempts int) (bool, error) {
	be, ok := err.(*codec.BrokerError)
	if !ok {
		return false, nil
	}
	return c.maybeRedirectControllerCode(be.Code, be.Message, attempt, maxAttempts)
}

func (c *Client) maybeRedirectControllerCode(code uint16, msg string, attempt, maxAttempts int) (bool, error) {
	if code != notController || attempt >= maxAttempts {
		return false, nil
	}
	return c.redirectToController(parseControllerID(msg))
}

// redirectToController refreshes Metadata and reconnects to the controller.
// If controllerID is set (parsed from controller_id=N in a 14 Error message,
// or Metadata's v0.77 trailer when non-zero), look that node up in Metadata
// brokers, then listMembersRpc if Metadata has no matching id. Otherwise pick
// the first advertised broker whose host:port is not this connection.
// ok is false on no other broker / lookup miss / empty host / reconnect fail
// (caller should surface the original error 14).
func (c *Client) redirectToController(controllerID *uint32) (bool, error) {
	meta, err := c.Metadata()
	if err != nil {
		return false, err
	}
	if controllerID == nil && meta.ControllerID != 0 {
		id := meta.ControllerID
		controllerID = &id
	}
	var host string
	var port uint16
	if controllerID != nil {
		found := false
		for i := range meta.Brokers {
			if meta.Brokers[i].NodeID == *controllerID {
				host = meta.Brokers[i].Host
				port = meta.Brokers[i].Port
				found = true
				break
			}
		}
		if !found {
			members, lerr := c.listMembersRpc()
			if lerr != nil {
				return false, nil
			}
			for i := range members.Brokers {
				if members.Brokers[i].ID == *controllerID {
					host = members.Brokers[i].Host
					port = members.Brokers[i].Port
					found = true
					break
				}
			}
		}
		if !found || host == "" {
			return false, nil
		}
	} else {
		picked := false
		for i := range meta.Brokers {
			if meta.Brokers[i].Host == "" {
				continue
			}
			addr := net.JoinHostPort(meta.Brokers[i].Host, strconv.Itoa(int(meta.Brokers[i].Port)))
			if addr != c.addr {
				host = meta.Brokers[i].Host
				port = meta.Brokers[i].Port
				picked = true
				break
			}
		}
		if !picked {
			return false, nil
		}
	}
	addr := net.JoinHostPort(host, strconv.Itoa(int(port)))
	if addr == c.addr {
		return true, nil
	}
	if err := c.reconnect(addr); err != nil {
		return false, nil
	}
	return true, nil
}

// Reconnect closes the current socket, dials addr, and re-runs Auth
// (token wins) or SCRAM when configured. Producer id / txn / sequences
// are not reset.
func (c *Client) Reconnect(addr string) error {
	return c.reconnect(addr)
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

// Metadata returns cluster brokers and topics (all topics).
// Same as MetadataTopics(nil). Transient broker/transport errors
// retry up to maxRetries extra times (default 0). Native Metadata
// has no top-level error_code; failures arrive as Error opcode /
// transport. Error 2 / 9 / 10 / 11 / 13 / 14 are not retried.
func (c *Client) Metadata() (Metadata, error) {
	return c.MetadataTopics(nil)
}

// MetadataTopics returns cluster brokers and the named topics.
// Nil or empty topics means all topics (same as Metadata).
// Same decode, retry, and error handling as Metadata. This is the
// native Metadata topics list, not Kafka allow_auto_topic_creation
// / topic ids.
func (c *Client) MetadataTopics(topics []string) (Metadata, error) {
	if topics == nil {
		topics = []string{}
	}
	payload, err := codec.EncodeMetadataRequest(codec.MetadataRequest{Topics: topics})
	if err != nil {
		return Metadata{}, err
	}
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	retryAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpMetadata, payload)
		if err != nil {
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return Metadata{}, err
		}
		resp, ok := decoded.(codec.MetadataResponse)
		if !ok {
			return Metadata{}, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for metadata: %T", decoded)}
		}
		return resp, nil
	}
}

// OffsetCommit commits one group offset (admin path: empty member, generation 0).
// Error 14 follows maxRedirects. Transient 6 / 7 / 15 / 16 follow maxRetries.
func (c *Client) OffsetCommit(group, topic string, partition int, offset int64) error {
	return c.OffsetCommitMeta(group, topic, partition, offset, "")
}

// OffsetCommitMeta is OffsetCommit with per-entry metadata (admin path).
// Empty metadata matches OffsetCommit. Error 14 follows maxRedirects.
// Transient 6 / 7 / 15 / 16 follow maxRetries.
func (c *Client) OffsetCommitMeta(group, topic string, partition int, offset int64, metadata string) error {
	return c.CommitOffsets(group, "", 0, []codec.OffsetCommitEntry{
		{Topic: topic, Partition: uint32(partition), Offset: uint64(offset), Metadata: metadata},
	})
}

// OffsetCommitMember is OffsetCommit with caller member_id + generation
// (Java 6-arg parity). Empty memberID + generation 0 is the admin path.
// Error 14 / transient retry inherit from CommitOffsets.
func (c *Client) OffsetCommitMember(group, topic string, partition int, offset int64, memberID string, generation uint32) error {
	return c.OffsetCommitMemberMeta(group, topic, partition, offset, memberID, generation, "")
}

// OffsetCommitMemberMeta is OffsetCommitMember with per-entry metadata
// (Java 7-arg parity). Empty metadata matches OffsetCommitMember.
func (c *Client) OffsetCommitMemberMeta(group, topic string, partition int, offset int64, memberID string, generation uint32, metadata string) error {
	return c.CommitOffsets(group, memberID, generation, []codec.OffsetCommitEntry{
		{Topic: topic, Partition: uint32(partition), Offset: uint64(offset), Metadata: metadata},
	})
}

// CommitOffsets sends one OffsetCommit RPC with N entries (native opcode 6).
// generation 0 skips the broker generation check. Error 14 follows
// maxRedirects. Transient 6 / 7 / 15 / 16 follow maxRetries.
func (c *Client) CommitOffsets(group, memberID string, generation uint32, entries []codec.OffsetCommitEntry) error {
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
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxAttempts := 1 + c.maxRedirects
	retryAttempt := 0
	redirectAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpOffsetCommit, payload)
		if err != nil {
			ok, rerr := c.maybeRedirectController(err, redirectAttempt+1, maxAttempts)
			if rerr != nil {
				return rerr
			}
			if ok {
				redirectAttempt++
				continue
			}
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return err
		}
		resp, ok := decoded.(codec.OffsetCommitResponse)
		if !ok {
			return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for offset_commit: %T", decoded)}
		}
		ok, rerr := c.maybeRedirectControllerCode(resp.ErrorCode, "", redirectAttempt+1, maxAttempts)
		if rerr != nil {
			return rerr
		}
		if ok {
			redirectAttempt++
			continue
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		return check(resp.ErrorCode, "offset_commit")
	}
}

// ListOffsets returns earliest/latest offsets for topic (native opcode 48).
// Nil or empty partitions means all partitions (wire count 0). Non-zero
// error_code is BrokerError. Transient broker/transport errors retry up
// to maxRetries extra times (default 0). Error 13 follows Produce/Fetch
// redirect (maxRedirects); 13 is not a transient retry. This is not
// Kafka ListOffsets (no timestamp or isolation); both ends of each log
// are returned.
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
	redirectPart := uint32(0)
	if len(partitions) > 0 {
		redirectPart = partitions[0]
	}
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxAttempts := 1 + c.maxRedirects
	retryAttempt := 0
	redirectAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpListOffsets, payload)
		if err != nil {
			if be, ok := err.(*codec.BrokerError); ok && be.Code == notLeaderForPartition && redirectAttempt+1 < maxAttempts {
				ok, rerr := c.redirectToLeader(topic, redirectPart)
				if rerr != nil {
					return nil, rerr
				}
				if ok {
					redirectAttempt++
					continue
				}
			}
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return nil, err
		}
		resp, ok := decoded.(codec.ListOffsetsResponse)
		if !ok {
			return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for list_offsets: %T", decoded)}
		}
		if resp.ErrorCode == notLeaderForPartition && redirectAttempt+1 < maxAttempts {
			leadTopic := resp.Topic
			if leadTopic == "" {
				leadTopic = topic
			}
			ok, rerr := c.redirectToLeader(leadTopic, redirectPart)
			if rerr != nil {
				return nil, rerr
			}
			if ok {
				redirectAttempt++
				continue
			}
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		if err := check(resp.ErrorCode, "list_offsets"); err != nil {
			return nil, err
		}
		return resp.Entries, nil
	}
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
// Op "describe_configs". Error 14 follows maxRedirects.

// DeleteOffsets deletes committed offsets for group (native opcode 38).
// Nil or empty entries deletes all offsets for the group (wire count 0).
// Returns the number of offset files removed. Non-zero error_code is
// BrokerError. Error 14 follows maxRedirects. Transient broker/transport
// errors retry up to maxRetries extra times (default 0). This is not
// Kafka OffsetDelete.
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
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxAttempts := 1 + c.maxRedirects
	retryAttempt := 0
	redirectAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpDeleteOffsets, payload)
		if err != nil {
			ok, rerr := c.maybeRedirectController(err, redirectAttempt+1, maxAttempts)
			if rerr != nil {
				return 0, rerr
			}
			if ok {
				redirectAttempt++
				continue
			}
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return 0, err
		}
		resp, ok := decoded.(codec.DeleteOffsetsResponse)
		if !ok {
			return 0, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for delete_offsets: %T", decoded)}
		}
		ok, rerr := c.maybeRedirectControllerCode(resp.ErrorCode, "", redirectAttempt+1, maxAttempts)
		if rerr != nil {
			return 0, rerr
		}
		if ok {
			redirectAttempt++
			continue
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		if err := check(resp.ErrorCode, "delete_offsets"); err != nil {
			return 0, err
		}
		return resp.DeletedCount, nil
	}
}

func (c *Client) DescribeConfigs(topic string) (DescribeConfigsResult, error) {
	payload, err := codec.EncodeDescribeConfigsRequest(codec.DescribeConfigsRequest{Topic: topic})
	if err != nil {
		return DescribeConfigsResult{}, err
	}
	decoded, err := c.adminRoundTrip(codec.OpDescribeConfigs, payload, "describe_configs")
	if err != nil {
		return DescribeConfigsResult{}, err
	}
	resp := decoded.(codec.DescribeConfigsResponse)
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
// Error 14 follows maxRedirects.
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
	_, err = c.adminRoundTrip(codec.OpAlterConfigs, payload, "alter_configs")
	return err
}

func (c *Client) DeleteRecords(topic string, partition uint32, beforeOffset uint64) (DeleteRecordsResult, error) {
	return c.DeleteRecordsWithWaitFlag(topic, partition, beforeOffset, 0)
}

// DeleteRecordsWithWaitFlag is DeleteRecords plus the Phase 137 trailer.
// waitMajority: 0 = broker default, 1 = force wait, 2 = force no-wait.
// Error 13 follows Produce/Fetch redirect (maxRedirects); 13 is not a
// transient retry. Transient 6 / 7 / 15 / 16 follow maxRetries (default 0).
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
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxAttempts := 1 + c.maxRedirects
	retryAttempt := 0
	redirectAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpDeleteRecords, payload)
		if err != nil {
			if be, ok := err.(*codec.BrokerError); ok && be.Code == notLeaderForPartition && redirectAttempt+1 < maxAttempts {
				ok, rerr := c.redirectToLeader(topic, partition)
				if rerr != nil {
					return DeleteRecordsResult{}, rerr
				}
				if ok {
					redirectAttempt++
					continue
				}
			}
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return DeleteRecordsResult{}, err
		}
		resp, ok := decoded.(codec.DeleteRecordsResponse)
		if !ok {
			return DeleteRecordsResult{}, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for delete_records: %T", decoded)}
		}
		if resp.ErrorCode == notLeaderForPartition && redirectAttempt+1 < maxAttempts {
			ok, rerr := c.redirectToLeader(resp.Topic, resp.Partition)
			if rerr != nil {
				return DeleteRecordsResult{}, rerr
			}
			if ok {
				redirectAttempt++
				continue
			}
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
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
}

// OffsetFetch returns committed offsets for topic as []Offset.
// Empty wire entries mean all offsets for the group; this method filters
// to topic client-side (same as the CLI). Error 14 follows maxRedirects.
// Transient 6 / 7 / 15 / 16 follow maxRetries.
func (c *Client) OffsetFetch(group, topic string) ([]Offset, error) {
	entries, err := c.FetchOffsets(group, nil)
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

// OffsetFetchAll returns all committed offsets for group as []OffsetFetchEntry.
// Empty wire entries mean all offsets (same as OffsetFetch); this method does
// not filter by topic. Error 14 follows maxRedirects. Transient 6 / 7 / 15 / 16
// follow maxRetries.
func (c *Client) OffsetFetchAll(group string) ([]OffsetFetchEntry, error) {
	entries, err := c.FetchOffsets(group, nil)
	if err != nil {
		return nil, err
	}
	out := make([]OffsetFetchEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, OffsetFetchEntry{
			Topic:     e.Topic,
			Partition: e.Partition,
			Offset:    e.Offset,
			Metadata:  e.Metadata,
		})
	}
	return out, nil
}

// FetchOffsets returns committed offsets for group. Nil or empty entries
// request all group offsets (same as OffsetFetch / OffsetFetchAll).
// Non-empty entries are sent on the wire (Rust fetch_offsets parity).
// Error 14 follows maxRedirects. Transient 6 / 7 / 15 / 16 follow maxRetries.
func (c *Client) FetchOffsets(group string, entries []codec.OffsetEntry) ([]codec.OffsetFetchEntry, error) {
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
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxAttempts := 1 + c.maxRedirects
	retryAttempt := 0
	redirectAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpOffsetFetch, payload)
		if err != nil {
			ok, rerr := c.maybeRedirectController(err, redirectAttempt+1, maxAttempts)
			if rerr != nil {
				return nil, rerr
			}
			if ok {
				redirectAttempt++
				continue
			}
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return nil, err
		}
		resp, ok := decoded.(codec.OffsetFetchResponse)
		if !ok {
			return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for offset_fetch: %T", decoded)}
		}
		ok, rerr := c.maybeRedirectControllerCode(resp.ErrorCode, "", redirectAttempt+1, maxAttempts)
		if rerr != nil {
			return nil, rerr
		}
		if ok {
			redirectAttempt++
			continue
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		if err := check(resp.ErrorCode, "offset_fetch"); err != nil {
			return nil, err
		}
		return resp.Entries, nil
	}
}

// JoinGroup joins a consumer group. First join sends empty member_id
// (broker assigns one). sessionTimeoutMs 0 defaults to 10000.
// Sends empty group_instance_id (dynamic membership).
func (c *Client) JoinGroup(group string, topics []string, sessionTimeoutMs int) (JoinGroupResult, error) {
	return c.joinGroup(group, "", topics, sessionTimeoutMs, "")
}

// JoinGroupWithInstance joins with Phase 12 static membership.
// Empty instanceID is dynamic membership (same as JoinGroup).
func (c *Client) JoinGroupWithInstance(group string, topics []string, sessionTimeoutMs int, instanceID string) (JoinGroupResult, error) {
	return c.joinGroup(group, "", topics, sessionTimeoutMs, instanceID)
}

// JoinGroupMember joins (or rejoins) with an explicit member_id.
// Empty memberID is a first join (same as JoinGroup).
func (c *Client) JoinGroupMember(group, memberID string, topics []string, sessionTimeoutMs int) (JoinGroupResult, error) {
	return c.joinGroup(group, memberID, topics, sessionTimeoutMs, "")
}

// JoinGroupMemberInstance joins with member_id and Phase 12 instance id.
// Empty memberID is a first join; empty instanceID is dynamic membership.
func (c *Client) JoinGroupMemberInstance(group, memberID string, topics []string, sessionTimeoutMs int, instanceID string) (JoinGroupResult, error) {
	return c.joinGroup(group, memberID, topics, sessionTimeoutMs, instanceID)
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
// (9 = rebalance in progress). Transient broker/transport errors retry
// up to maxRetries extra times (default 0). Error 14 follows
// maxRedirects. Rebalance codes 9 / 10 / 11 are not retried.
func (c *Client) Heartbeat(group, memberID string, generation uint32) error {
	payload, err := codec.EncodeHeartbeatRequest(codec.HeartbeatRequest{
		GroupID:    group,
		MemberID:   memberID,
		Generation: generation,
	})
	if err != nil {
		return err
	}
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxAttempts := 1 + c.maxRedirects
	retryAttempt := 0
	redirectAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpHeartbeat, payload)
		if err != nil {
			ok, rerr := c.maybeRedirectController(err, redirectAttempt+1, maxAttempts)
			if rerr != nil {
				return rerr
			}
			if ok {
				redirectAttempt++
				continue
			}
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return err
		}
		resp, ok := decoded.(codec.HeartbeatResponse)
		if !ok {
			return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for heartbeat: %T", decoded)}
		}
		ok, rerr := c.maybeRedirectControllerCode(resp.ErrorCode, "", redirectAttempt+1, maxAttempts)
		if rerr != nil {
			return rerr
		}
		if ok {
			redirectAttempt++
			continue
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		return check(resp.ErrorCode, "heartbeat")
	}
}

// DescribeGroup describes a live consumer group (native opcode 34/35).
// Error 2 (NotFound, no live members) is a BrokerError. Transient
// broker/transport errors retry up to maxRetries extra times (default
// 0). Error 14 follows maxRedirects. Error 2 / 9 / 10 / 11 / 13 are
// not retried.
func (c *Client) DescribeGroup(id string) (DescribeGroupResult, error) {
	payload, err := codec.EncodeDescribeGroupRequest(codec.DescribeGroupRequest{GroupID: id})
	if err != nil {
		return DescribeGroupResult{}, err
	}
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxAttempts := 1 + c.maxRedirects
	retryAttempt := 0
	redirectAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpDescribeGroup, payload)
		if err != nil {
			ok, rerr := c.maybeRedirectController(err, redirectAttempt+1, maxAttempts)
			if rerr != nil {
				return DescribeGroupResult{}, rerr
			}
			if ok {
				redirectAttempt++
				continue
			}
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return DescribeGroupResult{}, err
		}
		resp, ok := decoded.(codec.DescribeGroupResponse)
		if !ok {
			return DescribeGroupResult{}, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for describe_group: %T", decoded)}
		}
		ok, rerr := c.maybeRedirectControllerCode(resp.ErrorCode, "", redirectAttempt+1, maxAttempts)
		if rerr != nil {
			return DescribeGroupResult{}, rerr
		}
		if ok {
			redirectAttempt++
			continue
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
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
}

// ListGroups lists known consumer groups (native opcode 36/37).
// Transient broker/transport errors retry up to maxRetries extra times
// (default 0). Error 14 follows maxRedirects. Error 2 / 9 / 10 / 11 /
// 13 are not retried.
func (c *Client) ListGroups() ([]GroupListing, error) {
	payload := codec.EncodeListGroupsRequest()
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxAttempts := 1 + c.maxRedirects
	retryAttempt := 0
	redirectAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpListGroups, payload)
		if err != nil {
			ok, rerr := c.maybeRedirectController(err, redirectAttempt+1, maxAttempts)
			if rerr != nil {
				return nil, rerr
			}
			if ok {
				redirectAttempt++
				continue
			}
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return nil, err
		}
		resp, ok := decoded.(codec.ListGroupsResponse)
		if !ok {
			return nil, &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for list_groups: %T", decoded)}
		}
		ok, rerr := c.maybeRedirectControllerCode(resp.ErrorCode, "", redirectAttempt+1, maxAttempts)
		if rerr != nil {
			return nil, rerr
		}
		if ok {
			redirectAttempt++
			continue
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		if err := check(resp.ErrorCode, "list_groups"); err != nil {
			return nil, err
		}
		return resp.Groups, nil
	}
}

// CreateScramUser creates or replaces a SCRAM user (native opcode 64/65).
// iterations 0 means the broker default (4096). Password is sent in the
// clear (use TLS). This is not the v0.46 handshake (60–63). Error 14
// follows maxRedirects.
func (c *Client) CreateScramUser(username, password string, iterations uint32) error {
	payload, err := codec.EncodeCreateScramUserRequest(codec.CreateScramUserRequest{
		Username:   username,
		Password:   password,
		Iterations: iterations,
	})
	if err != nil {
		return err
	}
	_, err = c.adminRoundTrip(codec.OpCreateScramUser, payload, "create_scram_user")
	return err
}

// DeleteScramUser deletes a SCRAM user (native opcode 66/67). Error 14
// follows maxRedirects.
func (c *Client) DeleteScramUser(username string) error {
	payload, err := codec.EncodeDeleteScramUserRequest(codec.DeleteScramUserRequest{Username: username})
	if err != nil {
		return err
	}
	_, err = c.adminRoundTrip(codec.OpDeleteScramUser, payload, "delete_scram_user")
	return err
}

// ListScramUsers lists SCRAM usernames (native opcode 68/69). Error 14
// follows maxRedirects.
func (c *Client) ListScramUsers() ([]string, error) {
	decoded, err := c.adminRoundTrip(codec.OpListScramUsers, codec.EncodeListScramUsersRequest(), "list_scram_users")
	if err != nil {
		return nil, err
	}
	return decoded.(codec.ListScramUsersResponse).Usernames, nil
}

// CreateAcls creates ACL bindings (native opcode 54/55).
// This is not Kafka CreateAcls (API key 30). Error 14 follows maxRedirects.
func (c *Client) CreateAcls(entries []codec.AclBinding) error {
	payload, err := codec.EncodeCreateAclsRequest(codec.CreateAclsRequest{Entries: entries})
	if err != nil {
		return err
	}
	_, err = c.adminRoundTrip(codec.OpCreateAcls, payload, "create_acls")
	return err
}

// DeleteAcls deletes exact-matching ACL bindings (native opcode 56/57).
// Returns the number of entries removed. No filter-delete. Error 14 follows
// maxRedirects.
func (c *Client) DeleteAcls(entries []codec.AclBinding) (uint32, error) {
	payload, err := codec.EncodeDeleteAclsRequest(codec.DeleteAclsRequest{Entries: entries})
	if err != nil {
		return 0, err
	}
	decoded, err := c.adminRoundTrip(codec.OpDeleteAcls, payload, "delete_acls")
	if err != nil {
		return 0, err
	}
	return decoded.(codec.DeleteAclsResponse).Removed, nil
}

// ListAcls lists ACL bindings with optional filters (native opcode 58/59).
// Empty principal/resource = any. resourceType 255 = any type. Error 14
// follows maxRedirects.
func (c *Client) ListAcls(principal string, resourceType uint8, resource string) ([]codec.AclBinding, error) {
	payload, err := codec.EncodeListAclsRequest(codec.ListAclsRequest{
		Principal:    principal,
		ResourceType: resourceType,
		Resource:     resource,
	})
	if err != nil {
		return nil, err
	}
	decoded, err := c.adminRoundTrip(codec.OpListAcls, payload, "list_acls")
	if err != nil {
		return nil, err
	}
	return decoded.(codec.ListAclsResponse).Entries, nil
}

// LeaveGroup leaves a consumer group. Transient broker/transport errors
// retry up to maxRetries extra times (default 0). Error 10
// (UnknownMemberId) is success (already left). Error 14 follows
// maxRedirects. Rebalance 9 / IllegalGeneration 11 / 13 / not-found 2
// are not retried.
func (c *Client) LeaveGroup(group, memberID string) error {
	payload, err := codec.EncodeLeaveGroupRequest(codec.LeaveGroupRequest{
		GroupID:  group,
		MemberID: memberID,
	})
	if err != nil {
		return err
	}
	maxRetries := c.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	maxAttempts := 1 + c.maxRedirects
	retryAttempt := 0
	redirectAttempt := 0
	for {
		decoded, err := c.roundTrip(codec.OpLeaveGroup, payload)
		if err != nil {
			if be, ok := err.(*codec.BrokerError); ok && be.Code == 10 {
				return nil
			}
			ok, rerr := c.maybeRedirectController(err, redirectAttempt+1, maxAttempts)
			if rerr != nil {
				return rerr
			}
			if ok {
				redirectAttempt++
				continue
			}
			if isTransientProduceErr(err) && retryAttempt < maxRetries {
				retryAttempt++
				c.sleepProduceRetry()
				continue
			}
			return err
		}
		resp, ok := decoded.(codec.LeaveGroupResponse)
		if !ok {
			return &frame.ProtocolError{Msg: fmt.Sprintf("unexpected response for leave_group: %T", decoded)}
		}
		if resp.ErrorCode == 10 {
			return nil
		}
		ok, rerr := c.maybeRedirectControllerCode(resp.ErrorCode, "", redirectAttempt+1, maxAttempts)
		if rerr != nil {
			return rerr
		}
		if ok {
			redirectAttempt++
			continue
		}
		if isTransientBroker(resp.ErrorCode) && retryAttempt < maxRetries {
			retryAttempt++
			c.sleepProduceRetry()
			continue
		}
		return check(resp.ErrorCode, "leave_group")
	}
}
