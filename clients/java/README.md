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
import io.volant.Record;
import java.util.List;

try (Client c = Client.connect("127.0.0.1", 9092)) {
  c.createTopic("t", 1);
  long off = c.produce("t", 0, null, "hello".getBytes(UTF_8));
  List<Record> recs = c.fetch("t", 0, 0);
  for (Record rec : recs) {
    System.out.println(rec.offset + " " + rec.key + " " + new String(rec.value, UTF_8));
  }
  c.offsetCommit("g", "t", 0, 5);
  List<Offset> offs = c.offsetFetch("g", "t");
  JoinGroupResult j = c.joinGroup("g", List.of("t"), 10000);
  c.heartbeat("g", j.memberId, j.generation);
  c.leaveGroup("g", j.memberId);
  GroupConsumer g = GroupConsumer.join(c, "g", List.of("t"), 10_000);
  List<Record> polled = g.poll(500);
  g.commit();
  g.close();
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
```

`produce(..., null, value)` sends a null key. `fetch` returns `List<Record>`
(`offset`, `key`, `value`). `metadata()` returns brokers + topics.
`offsetCommit` is an admin commit (empty member, generation 0).
`offsetFetch` returns `List<Offset>` (`partition`, `offset`) for the topic.
`joinGroup` sends empty `memberId` on first join.
`GroupConsumer` joins, polls assigned partitions, heartbeats, commits with
member+generation, and rejoins on heartbeat error 9.
`RangeAssignor.rangeAssign` / `rangeAssignMulti` match the broker range
algorithm. `GroupConsumer.join(..., "range")` replaces the fetch set
with a **solo** local range (this member only — JoinGroup does not
return the live member list). Default assignor is broker.

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

## Honesty

Not implemented: `kafka-clients`, Kafka cooperative-sticky / SyncGroup,
seeing other group members on the wire, static membership, SCRAM /
shared-token auth, async I/O, idempotent produce, leader redirect.
Local `assignor="range"` cannot split across live members. Sync only;
one TCP connection; acks=1 by default. Convenience `offsetCommit` is
admin-only (`generation=0`); `GroupConsumer.commit` sends the joined
member+generation. TLS does not change broker TLS (Phase 8/19) and
does not add Kafka API keys. Client private keys other than PKCS#8 /
RSA PKCS#1 PEM are not loaded.

See [docs/V23_SPEC.md](../../docs/V23_SPEC.md),
[docs/V27_SPEC.md](../../docs/V27_SPEC.md),
[docs/V28_SPEC.md](../../docs/V28_SPEC.md),
[docs/V33_SPEC.md](../../docs/V33_SPEC.md), and
[docs/V41_SPEC.md](../../docs/V41_SPEC.md).
