# Volant Go client (native protocol MVP)

Sync TCP client for the **native** Volant wire protocol. This is **not**
`segmentio/kafka-go` / `franz-go` and does **not** speak the Kafka shim
(`--kafka-listen`).

Module: `github.com/volant-mq/volant/clients/go` (import package `volant`).
Crate / client version **0.2.0**.

## Usage

```go
import (
    volant "github.com/volant-mq/volant/clients/go"
    "github.com/volant-mq/volant/clients/go/codec"
)

c, err := volant.Dial("127.0.0.1:9092")
if err != nil {
    log.Fatal(err)
}
defer c.Close()

if err := c.CreateTopic("t", 1); err != nil {
    log.Fatal(err)
}
n, err := c.CreatePartitions("t", 2)
_ = n
gen, err := c.ReassignPartitions("t", []uint32{1, 2}, nil) // all partitions
_ = gen
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
bounds, err := c.ListOffsets("t", nil) // all partitions; or []uint32{0}
_ = bounds
cfg, err := c.DescribeConfigs("t")
_ = cfg
err = c.AlterConfigs("t", [][2]string{{"retention.ms", "86400000"}})
cut, err := c.DeleteRecords("t", 0, 100) // wait_majority=0
// cut, err = c.DeleteRecordsWithWaitFlag("t", 0, 100, 1) // force majority wait
_ = cut
j, err := c.JoinGroup("g", []string{"t"}, 10000)
if err != nil {
    log.Fatal(err)
}
err = c.Heartbeat("g", j.MemberID, j.Generation)
err = c.LeaveGroup("g", j.MemberID)
g, err := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000)
// Phase 12 static membership (empty instance id = dynamic):
g, err = volant.JoinGroupConsumerStatic(c, "g", []string{"t"}, 10_000, "inst-1")
batch, err := g.Poll(500 * time.Millisecond)
err = g.Commit()
err = g.Close()
// Opt-in auto-commit (v0.48). Default off. interval 0 = after every Poll.
g, err = volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithAutoCommit(5*time.Second))
// Opt-in auto_offset_reset (v0.62). Default earliest (position 0).
g, err = volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithAutoOffsetReset("latest"))
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
// Optional shared-token Auth (v0.42). Empty token skips Auth.
c, err = volant.DialAuth("127.0.0.1:9092", "s3cret")
c, err = volant.DialTLSAuth("127.0.0.1:9092", volant.TLSConfig{CAFile: "ca.pem"}, "s3cret")
// Optional idempotent produce (v0.47). Default off (trailer (0, 0, -1)).
c.EnableIdempotence()
// Optional native transactions (v0.57). Opcodes 50–53; not Kafka txns.
c.SetTransactionalID("txn-1")
_ = c.BeginTransaction()
_ = c.Produce("t", 0, nil, []byte("hello"))
_, _ = c.CommitTransaction(nil) // or []codec.TxnOffsetCommit
_ = c.AbortTransaction()
// Optional TransactionalProducer helper (v0.63). Queues offsets until Commit.
p, err := volant.NewTransactionalProducer(c) // c must have transactional_id
_ = p.Begin()
_, _ = p.Produce("t", 0, nil, []byte("x"))
p.AddOffsets("g", []volant.TxnOffset{{Topic: "t", Partition: 0, Offset: 1}})
results, err := p.Commit() // or p.Abort()
_ = p.IsOpen()
_ = results
// Optional SCRAM-SHA-256 (v0.46). Dial / DialAuth / DialTLS stay.
c, err = volant.DialScram("127.0.0.1:9092", "alice", "s3cret")
c, err = volant.DialTLSScram("127.0.0.1:9092", volant.TLSConfig{CAFile: "ca.pem"}, "alice", "s3cret")
// SCRAM admin (v0.55). Opcodes 64–69; not the handshake. Password in clear.
err = c.CreateScramUser("alice", "s3cret", 0) // 0 = broker default 4096
names, err := c.ListScramUsers()
err = c.DeleteScramUser("alice")
_ = names
// ACL admin (v0.56). Opcodes 54–59; exact-match delete. Not Kafka CreateAcls.
e := codec.AclBinding{Principal: "User:alice", ResourceType: 0, Resource: "events", Operation: 3, Permission: 1}
err = c.CreateAcls([]codec.AclBinding{e})
listed, err := c.ListAcls("", 255, "")
removed, err := c.DeleteAcls([]codec.AclBinding{e})
_ = listed
_ = removed
```

`Produce(..., nil, value)` sends a null key. `Fetch` returns `[]Record`
(`Offset`, `Key`, `Value`). `Metadata()` returns brokers + topics.
`OffsetCommit` is an admin commit (empty member, generation 0).
`OffsetFetch` returns `[]Offset` (`Partition`, `Offset`) for the topic.
`CreatePartitions` grows the topic to `totalCount` partitions and
returns the new total (native opcode 46, not Kafka CreatePartitions).
`ReassignPartitions` reassigns replicas and returns the assignment
generation (native opcode 114, not Kafka AlterPartitionReassignments).
Nil `partition` is all partitions (`u32::MAX`); nil / empty `replicas`
is auto-place.
`ListOffsets` returns `[]OffsetListing` (`Partition`, `Earliest`,
`Latest`); nil / empty partitions means all (native opcode 48, not
Kafka timestamp ListOffsets).
`DeleteRecords` / `DeleteRecordsWithWaitFlag` return
`DeleteRecordsResult` (`Topic`, `Partition`, `LowWatermark`); native
opcode 44, not Kafka DeleteRecords (API key 21). `waitMajority` 0 =
broker default, 1 = force wait, 2 = force no-wait. Error 13 follows
Produce/Fetch redirect.
`JoinGroup` sends empty `member_id` on first join; the result has
`MemberID`, `Generation`, and `Assignment`.
`JoinGroupConsumer` is the high-level loop (join, OffsetFetch
positions or 0, poll = heartbeat + fetch assigned, commit with
member+generation, rejoin on error 9, honor revoked).
`JoinGroupConsumerStatic` sends Phase 12 `group_instance_id` (empty =
dynamic) and resends it on rejoin. `Close` leaves the group and does
not close the `Client`.
`RangeAssign` / `RangeAssignMulti` match the broker range algorithm.
`WithAssignor("range")` replaces the fetch set with a **solo** local
range (this member only — JoinGroup does not return the live member
list). Default assignor is broker.

Produce, Fetch, and DeleteRecords follow `NotLeaderForPartition`
(error 13) by default: Metadata, reconnect to the partition leader,
retry once (`SetMaxRedirects(1)` is the Dial default).
`SetMaxRedirects(0)` raises on the first 13. Still one TCP connection
at a time. Other admin RPCs do not redirect.

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

Shared-token Auth (v0.42): `DialAuth` / `DialTLSAuth` send native
opcode 30 after connect when the token is non-empty. A rejected token
returns `BrokerError` with code 17 and closes the socket. `Dial` /
`DialTLS` are unchanged.

Idempotent produce (v0.47) is opt-in via `EnableIdempotence()`. The
first Produce sends native InitProducerId (opcode 32) with an empty
transactional_id; later produces attach pid/epoch/seq. Default off
keeps trailer `(0, 0, -1)`. Redirect keeps the same pid. UnknownProducerId
(21) re-Inits once and resets sequences. Not Kafka idempotent produce
v2.
Native transactions (v0.57) are opt-in via `SetTransactionalID`.
`BeginTransaction` / `CommitTransaction` / `AbortTransaction` send
opcodes 50–53. Init uses that id. Abort rewinds sequences. Not Kafka
transactions (API keys 22/24/25/26/28).
`TransactionalProducer` (v0.63) is a thin helper: `Begin` / `Produce` /
`AddOffsets` (local queue) / `Commit` / `Abort`. Produce is
write-through; LSO/commit is broker-side. `NewTransactionalProducer`
fails if `transactional_id` is unset.
SCRAM-SHA-256 (v0.46): `DialScram` / `DialTLSScram` send opcodes 60
then 62 after connect. Empty user or password is an error before
dial. A rejected proof or server-signature mismatch fails Dial.
Leader redirect re-runs the same auth path.
Create/Delete/ListScramUsers (v0.55) are admin RPCs (opcodes 64–69),
not the handshake. `CreateScramUser` sends the password in the clear
(use TLS). Not Kafka AlterUserScramCredentials.
Create/Delete/ListAcls (v0.56) are admin RPCs (opcodes 54–59).
`CreateAcls([]codec.AclBinding)` / `DeleteAcls(...)` (returns
removed) / `ListAcls(principal, resourceType, resource)`. Empty
principal/resource and `resourceType=255` list any. Delete is
exact-match only. Not Kafka CreateAcls / DeleteAcls / DescribeAcls
(API keys 30/31/29).

## Honesty

`JoinGroupConsumer` starts a background heartbeat goroutine after
join (interval `sessionTimeoutMs/3`, clamped 100–3000 ms; v0.37).
Pass `WithBackgroundHeartbeat(false)` for the v0.32 poll-only loop.
Not a fully concurrent API: do not share the `Client` while the
consumer is open.

Opt-in auto-commit (`WithAutoCommit(interval)`, default **off**;
v0.48) commits assigned positions after a successful `Poll` that
returned records. Interval `0` commits every such Poll; `> 0`
commits on the first successful Poll, then on the interval. Explicit
`Commit()` still works and resets the clock. `Close` best-effort
commits dirty positions then leaves. This is **not** Kafka
`enable.auto.commit` (no background commit goroutine).

`WithAutoOffsetReset` (v0.62) is a tiny Kafka subset: `earliest`
(default, position 0, no ListOffsets), `latest` (native ListOffsets
LEO), `none` (error if OffsetFetch is missing / `OFFSET_UNKNOWN`).
Invalid strings fail Join before JoinGroup. Not Kafka
`auto.offset.reset` (no timestamp). Rust GroupConsumer still starts
at 0 / OffsetFetch only.

Not implemented: `kafka-go`, Kafka cooperative-sticky / SyncGroup,
seeing other group members on the wire, SCRAM, async I/O, Kafka
transactions (API keys 22/24/25/26/28). Native BeginTxn/EndTxn
(opcodes 50–53) is opt-in via `SetTransactionalID`. Idempotent produce
is opt-in (`EnableIdempotence()`); default off. Local
`WithAssignor("range")` cannot split
seeing other group members on the wire, SCRAM-SHA-512, Kafka SASL,
async I/O, idempotent
produce. Local `WithAssignor("range")` cannot split
across live members. Thin `Client.JoinGroup` still sends empty
`group_instance_id`; use `JoinGroupConsumerStatic` for static
membership. Thin `OffsetCommit` is still the admin path (empty member,
generation 0); `GroupConsumer.Commit` sends member+generation.
Sync only; one TCP connection; acks=1 by default. TLS
does not change broker TLS (Phase 8/19) and does not add Kafka API keys.
Leader redirect is Produce/Fetch only (default one extra attempt).

See [docs/V19_SPEC.md](../../docs/V19_SPEC.md),
[docs/V24_SPEC.md](../../docs/V24_SPEC.md),
[docs/V27_SPEC.md](../../docs/V27_SPEC.md),
[docs/V28_SPEC.md](../../docs/V28_SPEC.md),
[docs/V32_SPEC.md](../../docs/V32_SPEC.md),
[docs/V36_SPEC.md](../../docs/V36_SPEC.md),
[docs/V37_SPEC.md](../../docs/V37_SPEC.md),
[docs/V41_SPEC.md](../../docs/V41_SPEC.md),
[docs/V42_SPEC.md](../../docs/V42_SPEC.md),
[docs/V43_SPEC.md](../../docs/V43_SPEC.md), and
[docs/V47_SPEC.md](../../docs/V47_SPEC.md),
[docs/V48_SPEC.md](../../docs/V48_SPEC.md),
[docs/V49_SPEC.md](../../docs/V49_SPEC.md),
[docs/V50_SPEC.md](../../docs/V50_SPEC.md),
[docs/V51_SPEC.md](../../docs/V51_SPEC.md),
[docs/V52_SPEC.md](../../docs/V52_SPEC.md),
[docs/V53_SPEC.md](../../docs/V53_SPEC.md),
[docs/V54_SPEC.md](../../docs/V54_SPEC.md),
[docs/V46_SPEC.md](../../docs/V46_SPEC.md).
[docs/V50_SPEC.md](../../docs/V50_SPEC.md).,
[docs/V46_SPEC.md](../../docs/V46_SPEC.md),
[docs/V55_SPEC.md](../../docs/V55_SPEC.md),
[docs/V56_SPEC.md](../../docs/V56_SPEC.md),
[docs/V58_SPEC.md](../../docs/V58_SPEC.md),
[docs/V59_SPEC.md](../../docs/V59_SPEC.md).
[docs/V57_SPEC.md](../../docs/V57_SPEC.md),
[docs/V63_SPEC.md](../../docs/V63_SPEC.md).
[docs/V62_SPEC.md](../../docs/V62_SPEC.md).
