# v0.108 — SCRAM handshake retry on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V46_SPEC.md](./V46_SPEC.md) /
[V61_SPEC.md](./V61_SPEC.md): produce / fetch / heartbeat / offsets /
admin / InitProducerId share `max_retries`, but the SCRAM-SHA-256
handshake (`_authenticate_scram` / `authenticateScram`, opcodes
first+final) is still a single shot. Connect / reconnect call it when
`scram_username`+`scram_password` are set and `auth_token` is not.

Reuse those same knobs and the same transient set. No new constructor
args. Wrap the **entire handshake as one unit**. If first **or** final
fails transiently, retry from **first with a new client nonce**. Do
**not** retry only the final step with the old nonce (SCRAM is not
resumable that way). Do **not** wrap token Auth `_authenticate`
(sibling v0.106) or DeleteRecords (sibling v0.110).

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change the
broker, `volant-protocol`, or Rust `volant-client`.

## Goals

1. Extra handshake attempts after the first on **transient** errors
   only. Budget is independent of `max_redirects`.
2. Same transient set as produce / fetch / heartbeat / offset-admin /
   Leave / Describe / Metadata / BeginTxn / EndTxn / InitProducerId /
   Rust `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable — on first **or** final (typed or
     `BrokerError` / `Error` opcode)
   - Transport: TCP / socket / IO (Python `OSError`; Go
     `isTransientTransport`; Java `isTransientTransport` /
     `RuntimeException` wrapping IO — match produce)
3. **Not retried:**
   - Error **17** / **18** (auth failed / bad credentials)
   - Error **13** / **14**
   - Error **9** / **10** / **11**
   - Error **2** (NotFound)
   - Error **21** / **22**
   - Protocol errors including **server signature mismatch**
   - Token Auth path (`_authenticate`)
4. Default `max_retries=0` so existing SCRAM tests stay valid (no
   extra first/final RPCs).
5. Sleep between retry attempts using the existing produce/fetch
   backoff helper (`_sleep_produce_retry` / `sleepProduceRetry` /
   `Thread.sleep` via the same field).
6. No new public methods. Wrap only:
   - `_authenticate_scram`
   - `authenticateScram`
   - Java `authenticateScram`

## Transient errors

Match produce / fetch / heartbeat / offset-admin / Leave / Describe /
Metadata / BeginTxn / EndTxn / InitProducerId and
`crates/volant-client` `is_transient_error_code`.

**Broker** codes (`crates/volant-protocol` `ErrorCode`):

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** socket / IO errors from the TCP layer (not
`ProtocolError` / constructor errors). Java retries
`RuntimeException` wrapping `IOException` the same way produce does.

**Not retried:**

- Error **17** (`AuthenticationFailed`) / **18** (bad credentials).
- Error **13** (`NotLeaderForPartition`) / **14** (`NotController`) —
  this slice is retry, not redirect.
- Error **9** (`RebalanceInProgress`), **10** (`UnknownMemberId`),
  **11** (`IllegalGeneration`).
- Error **2** (`NotFound`).
- Error **21** (`UnknownProducerId`) / **22** (`InvalidTxnState`).
- Protocol / constructor errors, including server signature mismatch.
- Token Auth (`_authenticate` / `authenticate`) — sibling v0.106.

## Non-goals

| Deferred | Why |
|----------|-----|
| Token Auth retry | Sibling v0.106 |
| DeleteRecords wrap | Sibling v0.110 |
| Rust `authenticate_scram` | Sibling v0.109 |
| Kafka SASL / SCRAM-SHA-512 | Native SHA-256 only |
| Reconnect on transport fail | Match produce (same socket) |
| New Dial / connect overloads | No new public methods |
| Phase 155 / homemade Raft | Frozen |
| Broker / protocol / Rust client | Frozen |

## API

No new public methods. Existing constructors / Dial / connect stay.
`auth_token` still wins over SCRAM (v0.42). Handshake now shares
produce/fetch knobs:

```python
Client("127.0.0.1:9092", scram_username="alice", scram_password="s3cret",
       max_retries=0, retry_backoff_ms=50)
c.max_retries = 3  # applies on reconnect / later handshake
```

```go
c, err := volant.DialScram("127.0.0.1:9092", "alice", "s3cret")
c.SetMaxRetries(3) // post-Dial; first handshake used the default 0
c.SetRetryBackoff(50 * time.Millisecond)
```

```java
Client c = Client.connectScram("127.0.0.1", 9092, "alice", "s3cret");
c.setMaxRetries(3); // post-connect; first handshake used the default 0
c.setRetryBackoffMs(50);
```

Default is **0 extra attempts**. Go `DialScram` / `DialTLSScram` and
Java `connectScram` / `connectTlsScram` stay as they are.

## Semantics

Same budget as produce/fetch (independent of redirect):

- Default `max_retries=0`: first SCRAM-first transient 7 raises; one
  first RPC, zero final.
- `max_retries=2`, backoff 0: first SCRAM-first Timeout then a full
  successful handshake → connect ok; two first RPCs, **new nonce**
  each attempt.
- First ok, final Timeout, then full success (`max_retries=2`,
  backoff 0) → connect ok; handshake **restarted** (at least two
  first RPCs, new nonce).
- First **17** (even with `max_retries=2`) raises immediately; one
  first, zero final.
- Exhausted retries: always 7 on first with `max_retries=2` → raise
  7 after `1 + max_retries` first RPCs.
- Transport fail then ok with `max_retries >= 1` → success (same
  socket; match produce).

Honesty: a transient final does **not** resend final with the old
nonce. Token still wins if both are set. Go / Java `SetMaxRetries` /
`setMaxRetries` is post-Dial, so the first connect handshake sees
the default 0 unless a test helper applies the knobs first.
Reconnect inherits the current value.

## Tests

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

Extend `_ScramServer` / Go / Java SCRAM stubs so they can queue
first-error / final-error codes and keep the connection open after a
transient reply. Count first+final RPCs.

| Case | Expect |
|------|--------|
| Default `max_retries=0`, first SCRAM-first 7 | raise 7; 1 first, 0 final |
| `max_retries=2`, backoff 0, first 7 then full success | connect ok; 2 first RPCs, new nonce each |
| First ok, final 7, then full success (`max_retries=2`) | connect ok; handshake restarted (≥2 first) |
| First 17 even with `max_retries=2` | raise 17 immediately; 1 first, 0 final |
| Existing pinned vector / token-wins / bad signature | still pass |

## Honesty leftovers

- **Not Kafka** `retries` / SASL / SCRAM-SHA-512.
- **Default 0** (Rust default is also 0).
- **No Kafka API keys / opcodes / Phase 155.**
- Token Auth is not wrapped (sibling v0.106).
- Rust `authenticate_scram` is unchanged (sibling v0.109).
- DeleteRecords is unchanged (sibling v0.110).
- Go `Dial` / Java `connect` still default `maxRetries=0` at first
  handshake (`SetMaxRetries` is post-Dial). Python constructor can
  pass `max_retries`. Reconnect inherits the current value.
- Transport retry stays on the same socket (match produce).
- 17 / 18 and signature mismatch are not retried.

## Merge notes

Siblings **v0.106** (token Auth) and **v0.110** (DeleteRecords) also
edit `Client`. When merging:

- **Keep the `_authenticate_scram` / `authenticateScram` wrap**
  (whole handshake, new nonce). Do not retry only final.
- Do not wrap token Auth `_authenticate`.
- Do not wrap DeleteRecords.
- Do not change the broker, Kafka shim, or Rust client in this merge.

Expect conflicts on the three Client files. The hunk is local to
`_authenticate_scram` / `authenticateScram`.

## Related

- [V46_SPEC.md](./V46_SPEC.md) — SCRAM-SHA-256 leftover this extends
- [V61_SPEC.md](./V61_SPEC.md) — produce retry leftover this reuses
- [V42_SPEC.md](./V42_SPEC.md) — token Auth still wins over SCRAM
