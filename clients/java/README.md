# Volant Java client (native protocol MVP)

Sync TCP client for the **native** Volant wire protocol. This is **not**
`kafka-clients` / `librdkafka` and does **not** speak the Kafka shim
(`--kafka-listen`).

Package: `io.volant` (Maven coordinates `io.volant:volant-client:0.2.0`).
Crate / client version **0.2.0**.

## Usage

```java
import static java.nio.charset.StandardCharsets.UTF_8;

import io.volant.Client;
import io.volant.GroupConsumer;
import io.volant.Metadata;
import io.volant.Offset;
import io.volant.OffsetListing;
import io.volant.Record;
import java.util.List;

try (Client c = Client.connect("127.0.0.1", 9092)) {
  c.createTopic("t", 1);
  int parts = c.createPartitions("t", 2);
  long off = c.produce("t", 0, null, "hello".getBytes(UTF_8));
  List<Record> recs = c.fetch("t", 0, 0);
  for (Record rec : recs) {
    System.out.println(rec.offset + " " + rec.key + " " + new String(rec.value, UTF_8));
  }
  c.offsetCommit("g", "t", 0, 5);
  List<Offset> offs = c.offsetFetch("g", "t");
  List<OffsetListing> bounds = c.listOffsets("t"); // all; or listOffsets("t", 0)
  JoinGroupResult j = c.joinGroup("g", List.of("t"), 10000);
  c.heartbeat("g", j.memberId, j.generation);
  c.leaveGroup("g", j.memberId);
  GroupConsumer g = GroupConsumer.join(c, "g", List.of("t"), 10_000);
  // Phase 12 static membership (empty instance id = dynamic):
  GroupConsumer s = GroupConsumer.joinStatic(c, "g", List.of("t"), 10_000, "inst-1");
  List<Record> polled = g.poll(500);
  g.commit();
  g.close();
  // Opt-in auto-commit (v0.48). Default off. interval 0 = after every poll.
  GroupConsumer a = GroupConsumer.joinWithAutoCommit(c, "g", List.of("t"), 10_000, 5000);
  Metadata meta = c.metadata();
}

// Optional TLS (v0.27). connect() stays plaintext.
try (Client c = Client.connectTls("127.0.0.1", 9092, TlsOptions.ca("ca.pem"))) {
  Metadata meta = c.metadata();
}
// Lab / tests only:
Client.connectTls("127.0.0.1", 9092, TlsOptions.insecure());
// Optional mTLS (client cert + key PEMs, both required):
Client.connectTls(
    "127.0.0.1",
    9092,
    TlsOptions.ca("ca.pem").clientCert("client.pem", "client.key"));
// Optional shared-token Auth (v0.42). null / empty skips Auth.
Client.connect("127.0.0.1", 9092, "s3cret");
Client.connectTls("127.0.0.1", 9092, TlsOptions.ca("ca.pem"), "s3cret");
// Optional idempotent produce (v0.47). Default off (trailer (0, 0, -1)).
try (Client c = Client.connect("127.0.0.1", 9092)) {
  c.setEnableIdempotence(true);
}
// Optional SCRAM-SHA-256 (v0.46). Existing overloads stay.
Client.connectScram("127.0.0.1", 9092, "alice", "s3cret");
Client.connectTlsScram("127.0.0.1", 9092, TlsOptions.ca("ca.pem"), "alice", "s3cret");
```

`produce(..., null, value)` sends a null key. `fetch` returns `List<Record>`
(`offset`, `key`, `value`). `metadata()` returns brokers + topics.
`offsetCommit` is an admin commit (empty member, generation 0).
`offsetFetch` returns `List<Offset>` (`partition`, `offset`) for the topic.
`createPartitions` grows the topic to `totalCount` partitions
and returns the new total (native opcode 46, not Kafka CreatePartitions).
`listOffsets` returns `List<OffsetListing>` (`partition`, `earliest`,
`latest`); no / empty partitions means all (native opcode 48, not
Kafka timestamp ListOffsets).
`joinGroup` sends empty `memberId` on first join.
`GroupConsumer` joins, polls assigned partitions, heartbeats, commits with
member+generation, and rejoins on heartbeat error 9.
`joinStatic` sends Phase 12 `group_instance_id` (empty = dynamic) and
resends it on rejoin.
`RangeAssignor.rangeAssign` / `rangeAssignMulti` match the broker range
algorithm. `GroupConsumer.join(..., "range")` replaces the fetch set
with a **solo** local range (this member only — JoinGroup does not
return the live member list). Default assignor is broker.

Produce and Fetch follow `NotLeaderForPartition` (error 13) by default:
Metadata, reconnect to the partition leader, retry once
(`setMaxRedirects(1)` is the connect default). `setMaxRedirects(0)`
raises on the first 13. Still one TCP connection at a time.

Correlation ids increment per request. Decode verifies magic `V` (0x56),
protocol version 1, and IEEE CRC32 of the **payload only**. Broker
`error_code != 0` is a `BrokerException`.

## Tests

Frame / codec tests need no broker:

```bash
cd clients/java
mvn -q test
```

Live create → produce → fetch (spawns `volant-server` on a free port):

```bash
# from repo root
cargo build -p volant-server
VOLANT_E2E=1 mvn -q -f clients/java/pom.xml test
```

- `VOLANT_E2E=1` — enable the e2e test (skipped otherwise).
- `VOLANT_BROKER=127.0.0.1:9092` — use an already-running native listener.
- `VOLANT_SERVER=/path/to/volant-server` — override the binary.

Repo helper: `scripts/java_client_smoke.sh` (skips if `mvn` is missing).
Not a required default-CI job.

TLS knobs match the Rust client as closely as JDK `SSLSocket` allows:
`connectTls` wraps after TCP connect; `TlsOptions.ca` trusts a PEM CA
(replaces the JVM default store); `TlsOptions.insecure` skips verify
(tests / lab only); `clientCert` is optional mTLS (PEM cert + PKCS#8
or PKCS#1 RSA key, both required). Handshake failures close the TCP
socket.

Shared-token Auth (v0.42): `connect(..., authToken)` /
`connectTls(..., authToken)` send native opcode 30 after connect when
the token is non-empty. A rejected token throws `BrokerException` with
code 17 and closes the socket. Existing overloads are unchanged.

Idempotent produce (v0.47) is opt-in via `setEnableIdempotence(true)`.
The first produce sends native InitProducerId (opcode 32) with an
empty transactional_id; later produces attach pid/epoch/seq. Default
off keeps trailer `(0, 0, -1)`. Redirect keeps the same pid. If the
broker returns UnknownProducerId (21), the client re-Inits once and
resets sequences. Not Kafka idempotent produce v2; no transactions.
SCRAM-SHA-256 (v0.46): `connectScram` / `connectTlsScram` send opcodes
60 then 62 after connect. Null or empty user or password throws
`IllegalArgumentException` before connect. A rejected proof or
server-signature mismatch fails the constructor. Leader redirect
re-runs the same auth path.

## Honesty

`GroupConsumer` starts a background heartbeat executor after join
(interval `sessionTimeoutMs / 3`, clamped 100–3000 ms; v0.37).
Pass `heartbeat=false` for the v0.33 poll-only loop. Not a fully
concurrent API: do not share the `Client` while the consumer is open.

Opt-in auto-commit (`joinWithAutoCommit(..., intervalMs)`, default
**off**; v0.48) commits assigned positions after a successful `poll`
that returned records. Interval `0` commits every such poll; `> 0`
commits on the first successful poll, then on the interval. Explicit
`commit()` still works and resets the clock. `close` best-effort
commits dirty positions then leaves. This is **not** Kafka
`enable.auto.commit` (no background commit thread). Named method so
it does not collide with `join(..., boolean heartbeat)` or
`join(..., String assignor)`.

Not implemented: `kafka-clients`, Kafka cooperative-sticky / SyncGroup,
seeing other group members on the wire, SCRAM, async I/O, transactions
(BeginTxn/EndTxn). Idempotent produce is opt-in
(`setEnableIdempotence(true)`); default off. Local `assignor="range"`
cannot split across
seeing other group members on the wire, SCRAM-SHA-512, Kafka SASL,
async I/O, idempotent
produce. Local `assignor="range"` cannot split across
live members. Sync only; one TCP connection; acks=1 by default. Thin
`joinGroup` still sends empty `group_instance_id`; use
`GroupConsumer.joinStatic` for static membership. Convenience
`offsetCommit` is
admin-only (`generation=0`); `GroupConsumer.commit` sends the joined
member+generation. TLS does not change broker TLS (Phase 8/19) and
does not add Kafka API keys. Client private keys other than PKCS#8 /
RSA PKCS#1 PEM are not loaded. Leader redirect is Produce/Fetch only
(default one extra attempt).

See [docs/V23_SPEC.md](../../docs/V23_SPEC.md),
[docs/V27_SPEC.md](../../docs/V27_SPEC.md),
[docs/V28_SPEC.md](../../docs/V28_SPEC.md),
[docs/V33_SPEC.md](../../docs/V33_SPEC.md),
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
[docs/V46_SPEC.md](../../docs/V46_SPEC.md).
