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
import io.volant.Metadata;
import io.volant.Record;
import java.util.List;

try (Client c = Client.connect("127.0.0.1", 9092)) {
  c.createTopic("t", 1);
  long off = c.produce("t", 0, null, "hello".getBytes(UTF_8));
  List<Record> recs = c.fetch("t", 0, 0);
  for (Record rec : recs) {
    System.out.println(rec.offset + " " + rec.key + " " + new String(rec.value, UTF_8));
  }
  Metadata meta = c.metadata();
}
```

`produce(..., null, value)` sends a null key. `fetch` returns `List<Record>`
(`offset`, `key`, `value`). `metadata()` returns brokers + topics.

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

## Honesty

Not implemented: `kafka-clients`, consumer groups, TLS / SCRAM /
shared-token auth, async I/O, idempotent produce, leader redirect. Sync
only; one TCP connection; acks=1 by default.

See [docs/V23_SPEC.md](../../docs/V23_SPEC.md).
