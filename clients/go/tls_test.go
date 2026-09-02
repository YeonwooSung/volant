package volant_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	volant "github.com/volant-mq/volant/clients/go"
	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

type tlsMaterial struct {
	dir        string
	caCRT      string
	serverCRT  string
	serverKey  string
	clientCRT  string
	clientKey  string
	serverTLS  tls.Certificate
	clientTLS  tls.Certificate
	caPool     *x509.CertPool
}

func generateTLSMaterial(t *testing.T) tlsMaterial {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "volant-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTmpl, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "volant-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	writePEM := func(name, typ string, der []byte) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	marshalKey := func(name string, key *ecdsa.PrivateKey) string {
		t.Helper()
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return writePEM(name, "PRIVATE KEY", der)
	}

	m := tlsMaterial{
		dir:       dir,
		caCRT:     writePEM("ca.crt", "CERTIFICATE", caDER),
		serverCRT: writePEM("server.crt", "CERTIFICATE", serverDER),
		serverKey: marshalKey("server.key", serverKey),
		clientCRT: writePEM("client.crt", "CERTIFICATE", clientDER),
		clientKey: marshalKey("client.key", clientKey),
		caPool:    x509.NewCertPool(),
	}
	m.caPool.AddCert(caCert)
	m.serverTLS, err = tls.LoadX509KeyPair(m.serverCRT, m.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	m.clientTLS, err = tls.LoadX509KeyPair(m.clientCRT, m.clientKey)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func serveTLSMetadata(t *testing.T, cfg *tls.Config) (addr string, stop func()) {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 0, 4096)
		tmp := make([]byte, 4096)
		for {
			f, _, err := frame.TryDecode(buf)
			if err != nil {
				return
			}
			if f != nil {
				payload, err := codec.EncodeMetadataResponse(codec.MetadataResponse{
					Brokers: []codec.BrokerInfo{{NodeID: 1, Host: "127.0.0.1", Port: 1}},
				})
				if err != nil {
					return
				}
				raw, err := frame.Encode(codec.OpMetadata, f.CorrelationID, payload)
				if err != nil {
					return
				}
				_, _ = conn.Write(raw)
				return
			}
			n, err := conn.Read(tmp)
			if n > 0 {
				buf = append(buf, tmp[:n]...)
			}
			if err != nil {
				return
			}
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

func TestDialPlainDefaultUnchanged(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 0, 4096)
		tmp := make([]byte, 4096)
		for {
			f, _, err := frame.TryDecode(buf)
			if err != nil {
				return
			}
			if f != nil {
				payload, _ := codec.EncodeMetadataResponse(codec.MetadataResponse{
					Brokers: []codec.BrokerInfo{{NodeID: 1, Host: "127.0.0.1", Port: 1}},
				})
				raw, _ := frame.Encode(codec.OpMetadata, f.CorrelationID, payload)
				_, _ = conn.Write(raw)
				return
			}
			n, err := conn.Read(tmp)
			if n > 0 {
				buf = append(buf, tmp[:n]...)
			}
			if err != nil {
				return
			}
		}
	}()

	c, err := volant.DialTimeout(ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.TLS() {
		t.Fatal("plaintext Dial should not report TLS")
	}
	meta, err := c.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Brokers) != 1 {
		t.Fatalf("brokers=%d want 1", len(meta.Brokers))
	}
	<-done
}

func TestDialTLSWithCA(t *testing.T) {
	m := generateTLSMaterial(t)
	addr, stop := serveTLSMetadata(t, &tls.Config{
		Certificates: []tls.Certificate{m.serverTLS},
		MinVersion:   tls.VersionTLS12,
	})
	defer stop()

	c, err := volant.DialTLSTimeout(addr, volant.TLSConfig{CAFile: m.caCRT}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if !c.TLS() {
		t.Fatal("expected TLS connection")
	}
	meta, err := c.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Brokers) != 1 {
		t.Fatalf("brokers=%d want 1", len(meta.Brokers))
	}
}

func TestDialTLSInsecure(t *testing.T) {
	m := generateTLSMaterial(t)
	addr, stop := serveTLSMetadata(t, &tls.Config{
		Certificates: []tls.Certificate{m.serverTLS},
		MinVersion:   tls.VersionTLS12,
	})
	defer stop()

	c, err := volant.DialTLSTimeout(addr, volant.TLSConfig{Insecure: true}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
}

func TestDialTLSRejectsUntrusted(t *testing.T) {
	m := generateTLSMaterial(t)
	addr, stop := serveTLSMetadata(t, &tls.Config{
		Certificates: []tls.Certificate{m.serverTLS},
		MinVersion:   tls.VersionTLS12,
	})
	defer stop()

	_, err := volant.DialTLSTimeout(addr, volant.TLSConfig{}, 5*time.Second)
	if err == nil {
		t.Fatal("expected handshake failure without CA")
	}
}

func TestDialTLSmTLS(t *testing.T) {
	m := generateTLSMaterial(t)
	addr, stop := serveTLSMetadata(t, &tls.Config{
		Certificates: []tls.Certificate{m.serverTLS},
		ClientCAs:    m.caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	})
	defer stop()

	c, err := volant.DialTLSTimeout(addr, volant.TLSConfig{
		CAFile:   m.caCRT,
		CertFile: m.clientCRT,
		KeyFile:  m.clientKey,
	}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
}

func TestDialTLSmTLSWithoutClientCertFails(t *testing.T) {
	m := generateTLSMaterial(t)
	addr, stop := serveTLSMetadata(t, &tls.Config{
		Certificates: []tls.Certificate{m.serverTLS},
		ClientCAs:    m.caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	})
	defer stop()

	c, err := volant.DialTLSTimeout(addr, volant.TLSConfig{CAFile: m.caCRT}, 5*time.Second)
	if err == nil {
		_, rpcErr := c.Metadata()
		_ = c.Close()
		if rpcErr == nil {
			t.Fatal("expected handshake or RPC failure without client cert")
		}
	}
}

func TestTLSConfigCertKeyMustBePaired(t *testing.T) {
	_, err := volant.DialTLSTimeout("127.0.0.1:1", volant.TLSConfig{CertFile: "client.pem"}, time.Second)
	if err == nil {
		t.Fatal("expected paired cert/key error")
	}
	_, err = volant.DialTLSTimeout("127.0.0.1:1", volant.TLSConfig{KeyFile: "client.key"}, time.Second)
	if err == nil {
		t.Fatal("expected paired cert/key error")
	}
}

func TestE2ETlsAgainstServer(t *testing.T) {
	if os.Getenv("VOLANT_E2E") != "1" {
		t.Skip("set VOLANT_E2E=1 to run live broker TLS e2e")
	}
	bin := findServerBin()
	if bin == "" {
		t.Skip("volant-server not found; build with `cargo build -p volant-server --features tls`")
	}
	m := generateTLSMaterial(t)
	dir := t.TempDir()
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cmd := exec.Command(bin,
		"--listen", addr,
		"--data-dir", dir,
		"--tls-cert", m.serverCRT,
		"--tls-key", m.serverKey,
	)
	cmd.Dir = repoRoot()
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		t.Fatalf("start volant-server: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() {
			_, _ = cmd.Process.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	}()
	if err := waitPort("127.0.0.1", port, 8*time.Second); err != nil {
		t.Skipf("volant-server did not listen with --tls-*; build with --features tls: %v", err)
	}
	c, err := volant.DialTLSTimeout(addr, volant.TLSConfig{CAFile: m.caCRT}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	meta, err := c.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Brokers) == 0 {
		t.Fatal("expected at least one broker")
	}
}
