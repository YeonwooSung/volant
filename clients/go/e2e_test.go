package volant_test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	volant "github.com/volant-mq/volant/clients/go"
)

func TestE2ECreateProduceFetchMetadata(t *testing.T) {
	if os.Getenv("VOLANT_E2E") != "1" {
		t.Skip("set VOLANT_E2E=1 to run live broker e2e")
	}
	addr, cleanup := startBroker(t)
	defer cleanup()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	topic := fmt.Sprintf("go-e2e-%d-%d", os.Getpid(), time.Now().UnixNano())
	if err := c.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	off, err := c.Produce(topic, 0, nil, []byte("hello"))
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if off != 0 {
		t.Fatalf("base offset=%d want 0", off)
	}

	recs, err := c.Fetch(topic, 0, 0)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("len(recs)=%d want 1", len(recs))
	}
	if recs[0].Offset != 0 || recs[0].Key != nil || string(recs[0].Value) != "hello" {
		t.Fatalf("record %+v", recs[0])
	}

	meta, err := c.Metadata()
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	found := false
	for _, tp := range meta.Topics {
		if tp.Name == topic {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("topic %q missing from metadata", topic)
	}
	if len(meta.Brokers) == 0 {
		t.Fatal("expected at least one broker")
	}

	if err := c.DeleteTopic(topic); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	meta2, err := c.Metadata()
	if err != nil {
		t.Fatalf("Metadata after delete: %v", err)
	}
	for _, tp := range meta2.Topics {
		if tp.Name == topic {
			t.Fatalf("topic %q still present after delete", topic)
		}
	}
}

func TestE2EOffsetCommitFetch(t *testing.T) {
	if os.Getenv("VOLANT_E2E") != "1" {
		t.Skip("set VOLANT_E2E=1 to run live broker e2e")
	}
	addr, cleanup := startBroker(t)
	defer cleanup()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	topic := fmt.Sprintf("go-off-%d-%d", os.Getpid(), time.Now().UnixNano())
	group := fmt.Sprintf("go-g-%d", os.Getpid())
	if err := c.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := c.Produce(topic, 0, nil, []byte("hello")); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if err := c.OffsetCommit(group, topic, 0, 5); err != nil {
		t.Fatalf("OffsetCommit: %v", err)
	}
	offs, err := c.OffsetFetch(group, topic)
	if err != nil {
		t.Fatalf("OffsetFetch: %v", err)
	}
	if len(offs) != 1 || offs[0].Partition != 0 || offs[0].Offset != 5 {
		t.Fatalf("offsets %+v want [{0 5}]", offs)
	}
	if err := c.DeleteTopic(topic); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
}

func TestE2EJoinHeartbeatLeave(t *testing.T) {
	if os.Getenv("VOLANT_E2E") != "1" {
		t.Skip("set VOLANT_E2E=1 to run live broker e2e")
	}
	addr, cleanup := startBroker(t)
	defer cleanup()

	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	topic := fmt.Sprintf("go-grp-%d-%d", os.Getpid(), time.Now().UnixNano())
	group := fmt.Sprintf("go-cg-%d", os.Getpid())
	if err := c.CreateTopic(topic, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	j, err := c.JoinGroup(group, []string{topic}, 10000)
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}
	if j.MemberID == "" {
		t.Fatal("expected broker-assigned member id")
	}
	if j.Generation < 1 {
		t.Fatalf("generation=%d want >= 1", j.Generation)
	}
	if len(j.Assignment) != 1 || j.Assignment[0].Topic != topic || j.Assignment[0].Partition != 0 {
		t.Fatalf("assignment %+v want [{%s 0}]", j.Assignment, topic)
	}
	if err := c.Heartbeat(group, j.MemberID, j.Generation); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if err := c.LeaveGroup(group, j.MemberID); err != nil {
		t.Fatalf("LeaveGroup: %v", err)
	}
	if err := c.DeleteTopic(topic); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
}

func startBroker(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	if existing := os.Getenv("VOLANT_BROKER"); existing != "" {
		host, portS, err := net.SplitHostPort(existing)
		if err != nil {
			t.Fatalf("VOLANT_BROKER: %v", err)
		}
		port, err := strconv.Atoi(portS)
		if err != nil {
			t.Fatalf("VOLANT_BROKER port: %v", err)
		}
		if err := waitPort(host, port, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		return existing, func() {}
	}

	bin := ensureServerBin(t)
	dir := t.TempDir()
	port := freePort(t)
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	cmd := exec.Command(bin, "--listen", addr, "--data-dir", dir)
	cmd.Dir = repoRoot()
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		t.Fatalf("start volant-server: %v", err)
	}
	if err := waitPort("127.0.0.1", port, 15*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatal(err)
	}
	return addr, func() {
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
	}
}

func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func findServerBin() string {
	if env := os.Getenv("VOLANT_SERVER"); env != "" {
		if st, err := os.Stat(env); err == nil && !st.IsDir() {
			return env
		}
	}
	root := repoRoot()
	for _, rel := range []string{
		filepath.Join("target", "debug", "volant-server"),
		filepath.Join("target", "release", "volant-server"),
	} {
		p := filepath.Join(root, rel)
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return ""
}

func ensureServerBin(t *testing.T) string {
	t.Helper()
	if found := findServerBin(); found != "" {
		return found
	}
	if _, err := exec.LookPath("cargo"); err != nil {
		t.Skip("volant-server not found; build with `cargo build -p volant-server` or set VOLANT_SERVER / VOLANT_BROKER")
	}
	cmd := exec.Command("cargo", "build", "-p", "volant-server")
	cmd.Dir = repoRoot()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("cargo build -p volant-server failed: %v\n%s", err, out)
	}
	if found := findServerBin(); found != "" {
		return found
	}
	t.Skip("volant-server not found after cargo build; set VOLANT_SERVER / VOLANT_BROKER")
	return ""
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func waitPort(host string, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	var last error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		last = err
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("broker did not listen on %s: %v", addr, last)
}
