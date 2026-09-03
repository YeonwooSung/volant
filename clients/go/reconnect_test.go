package volant_test

import (
	"testing"

	volant "github.com/volant-mq/volant/clients/go"
	"github.com/volant-mq/volant/clients/go/codec"
)

func TestReconnectSecondListenerMetadata(t *testing.T) {
	addr1, got1, stop1 := serveAuth(t, 0, true)
	defer stop1()
	addr2, got2, stop2 := serveAuth(t, 0, true)
	defer stop2()

	c, err := volant.Dial(addr1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if err := c.Reconnect(addr2); err != nil {
		t.Fatal(err)
	}
	meta, err := c.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Brokers) != 1 {
		t.Fatalf("brokers %d", len(meta.Brokers))
	}
	if got1.firstOpcode != codec.OpMetadata {
		t.Fatalf("first opcode %d want metadata", got1.firstOpcode)
	}
	if got2.firstOpcode != codec.OpMetadata {
		t.Fatalf("second opcode %d want metadata", got2.firstOpcode)
	}
	if got1.err != nil {
		t.Fatal(got1.err)
	}
	if got2.err != nil {
		t.Fatal(got2.err)
	}
}

func TestReconnectResendsAuth(t *testing.T) {
	addr1, got1, stop1 := serveAuth(t, 0, false)
	defer stop1()
	addr2, got2, stop2 := serveAuth(t, 0, true)
	defer stop2()

	c, err := volant.DialAuth(addr1, "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Reconnect(addr2); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if got1.authCount != 1 || got1.token != "s3cret" {
		t.Fatalf("first auth count=%d token=%q", got1.authCount, got1.token)
	}
	if got2.authCount != 1 || got2.token != "s3cret" {
		t.Fatalf("second auth count=%d token=%q", got2.authCount, got2.token)
	}
	if got1.firstOpcode != codec.OpAuth || got2.firstOpcode != codec.OpAuth {
		t.Fatalf("opcodes first=%d second=%d", got1.firstOpcode, got2.firstOpcode)
	}
}

func TestReconnectRerunsScram(t *testing.T) {
	addr, got, stop := serveScram(t, scramPass, false, 2)
	defer stop()

	c, err := volant.DialScram(addr, scramUser, scramPass)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if err := c.Reconnect(addr); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Metadata(); err != nil {
		t.Fatal(err)
	}
	if len(got.firstUsernames) != 2 || len(got.finalUsernames) != 2 {
		t.Fatalf("first=%v final=%v opcodes=%v", got.firstUsernames, got.finalUsernames, got.opcodes)
	}
	if got.firstUsernames[0] != scramUser || got.firstUsernames[1] != scramUser {
		t.Fatalf("first users %v", got.firstUsernames)
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
}
