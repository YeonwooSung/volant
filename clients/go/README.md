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
meta, err := c.Metadata()
_ = off
_ = meta
```

`Produce(..., nil, value)` sends a null key. `Fetch` returns `[]Record`
(`Offset`, `Key`, `Value`). `Metadata()` returns brokers + topics.

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

## Honesty

Not implemented: Java client, `kafka-go`, consumer groups, TLS / SCRAM /
shared-token auth, async I/O, idempotent produce, leader redirect. Sync
only; one TCP connection; acks=1 by default.

See [docs/V19_SPEC.md](../../docs/V19_SPEC.md).
