# Volant Go client (native protocol MVP)

Sync TCP client for the **native** Volant wire protocol. This is **not**
`segmentio/kafka-go` / `franz-go` and does **not** speak the Kafka shim
(`--kafka-listen`).

Module: `github.com/volant-mq/volant/clients/go` (import package `volant`).
Crate / client version **0.2.0**.

## Usage

```go
import volant "github.com/volant-mq/volant/clients/go"

c, err := volant.Dial("127.0.0.1:9092")
if err != nil {
    log.Fatal(err)
}
defer c.Close()

if err := c.CreateTopic("t", 1); err != nil {
    log.Fatal(err)
}
off, err := c.Produce("t", 0, nil, []byte("hello"))
if err != nil {
    log.Fatal(err)
}
recs, err := c.Fetch("t", 0, 0)
if err != nil {
    log.Fatal(err)
}
for _, rec := range recs {
    fmt.Println(rec.Offset, rec.Key, rec.Value)
}
if err := c.OffsetCommit("g", "t", 0, 5); err != nil {
    log.Fatal(err)
}
offs, err := c.OffsetFetch("g", "t")
_ = offs
j, err := c.JoinGroup("g", []string{"t"}, 10000)
if err != nil {
    log.Fatal(err)
}
err = c.Heartbeat("g", j.MemberID, j.Generation)
err = c.LeaveGroup("g", j.MemberID)
g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000)
batch, err := g.Poll(500 * time.Millisecond)
err = g.Commit()
err = g.Close()
_ = batch
meta, err := c.Metadata()
_ = off
_ = meta

// Optional TLS (v0.27). Dial stays plaintext.
c, err = volant.DialTLS("127.0.0.1:9092", volant.TLSConfig{CAFile: "ca.pem"})
// Lab / tests only:
c, err = volant.DialTLS("127.0.0.1:9092", volant.TLSConfig{Insecure: true})
// Optional mTLS (client cert + key PEMs, both required):
c, err = volant.DialTLS("127.0.0.1:9092", volant.TLSConfig{
    CAFile:   "ca.pem",
    CertFile: "client.pem",
    KeyFile:  "client.key",
})
```

`Produce(..., nil, value)` sends a null key. `Fetch` returns `[]Record`
(`Offset`, `Key`, `Value`). `Metadata()` returns brokers + topics.
`OffsetCommit` is an admin commit (empty member, generation 0).
`OffsetFetch` returns `[]Offset` (`Partition`, `Offset`) for the topic.
`JoinGroup` sends empty `member_id` on first join; the result has
`MemberID`, `Generation`, and `Assignment`.
`JoinGroupConsumer` is the high-level loop (join, OffsetFetch
positions or 0, poll = heartbeat + fetch assigned, commit with
member+generation, rejoin on error 9, honor revoked). `Close` leaves
the group and does not close the `Client`.

Correlation ids increment per request. Decode verifies magic `V` (0x56),
protocol version 1, and IEEE CRC32 of the **payload only**. Broker
`error_code != 0` is a `BrokerError`.

## Tests

Frame / codec tests need no broker:

```bash
cd clients/go
go test ./...
```

Live create → produce → fetch (spawns `volant-server` on a free port):

```bash
# from repo root
cargo build -p volant-server
VOLANT_E2E=1 go test ./clients/go -count=1
```

- `VOLANT_E2E=1` — enable the e2e test (skipped otherwise).
- `VOLANT_BROKER=127.0.0.1:9092` — use an already-running native listener.
- `VOLANT_SERVER=/path/to/volant-server` — override the binary.

Repo helper: `scripts/go_client_smoke.sh` (skips if `go` is missing).
Not a required default-CI job.

TLS knobs match the Rust client as closely as `crypto/tls` allows:
`DialTLS` / `DialTLSTimeout` wrap after TCP connect; `TLSConfig.CAFile`
is a PEM added to the system trust store; `Insecure` skips verify
(tests / lab only); `CertFile` + `KeyFile` are optional mTLS PEMs and
must be paired. Handshake failures close the TCP socket.

## Honesty

`JoinGroupConsumer` starts a background heartbeat goroutine after
join (interval `sessionTimeoutMs/3`, clamped 100–3000 ms; v0.37).
Pass `WithBackgroundHeartbeat(false)` for the v0.32 poll-only loop.
Not a fully concurrent API: do not share the `Client` while the
consumer is open.

Not implemented: `kafka-go`, custom assignor, static membership,
SCRAM / shared-token auth, async I/O, idempotent produce, leader
redirect. Thin `OffsetCommit` is still the admin path (empty member,
generation 0); `GroupConsumer.Commit` sends member+generation.
Sync only; one TCP connection; acks=1 by default. TLS
does not change broker TLS (Phase 8/19) and does not add Kafka API keys.

See [docs/V19_SPEC.md](../../docs/V19_SPEC.md),
[docs/V24_SPEC.md](../../docs/V24_SPEC.md),
[docs/V27_SPEC.md](../../docs/V27_SPEC.md),
[docs/V28_SPEC.md](../../docs/V28_SPEC.md), and
[docs/V32_SPEC.md](../../docs/V32_SPEC.md), and
[docs/V37_SPEC.md](../../docs/V37_SPEC.md).
