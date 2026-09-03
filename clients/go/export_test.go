package volant

import (
	"net"
	"time"
)

// DialAuthRetries is test-only: DialAuth with retry knobs applied before Auth.
func DialAuthRetries(addr, token string, maxRetries int, backoff time.Duration) (*Client, error) {
	timeout := 5 * time.Second
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	if backoff < 0 {
		backoff = 0
	}
	c := &Client{
		addr:         addr,
		conn:         conn,
		timeout:      timeout,
		nextCorr:     1,
		authToken:    token,
		maxRedirects:     1,
		maxRetries:       maxRetries,
		retryBackoff:     backoff,
		acks:              1,
		deleteRecordsWait: 0,
		fetchMaxMessages:  128,
		fetchMaxBytes:     4 * 1024 * 1024,
		fetchMaxWaitMs:    0,
	}
	if err := c.maybeAuthenticate(); err != nil {
		return nil, err
	}
	return c, nil
}

// DialScramRetry is a test-only DialScram that applies maxRetries /
// retry backoff before the handshake (SetMaxRetries is post-Dial).
func DialScramRetry(addr, user, pass string, maxRetries int, backoff time.Duration) (*Client, error) {
	if err := checkScramPair(user, pass); err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", addr, defaultTimeout)
	if err != nil {
		return nil, err
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	if backoff < 0 {
		backoff = 0
	}
	c := &Client{
		addr:         addr,
		conn:         conn,
		timeout:      defaultTimeout,
		nextCorr:     1,
		scramUser:    user,
		scramPass:    pass,
		maxRedirects:     1,
		maxRetries:       maxRetries,
		retryBackoff:     backoff,
		acks:              1,
		deleteRecordsWait: 0,
		fetchMaxMessages:  128,
		fetchMaxBytes:     4 * 1024 * 1024,
		fetchMaxWaitMs:    0,
	}
	if err := c.maybeAuthenticate(); err != nil {
		return nil, err
	}
	return c, nil
}
