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
		maxRedirects: 1,
		maxRetries:   maxRetries,
		retryBackoff: backoff,
	}
	if err := c.maybeAuthenticate(); err != nil {
		return nil, err
	}
	return c, nil
}
