# v0.55 — Create/Delete/ListScramUsers on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “language clients do not expose
Create/Delete/ListScramUsers.” Rust `volant-client` already has
`create_scram_user` / `delete_scram_user` / `list_scram_users`. This
slice ports native opcodes **64–69** (Phase 22) to the Python, Go, and
Java clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker SCRAM store. These are **admin RPCs**, not the
v0.46 handshake (opcodes **60–63**).

## Goals

1. **Codec** encode/decode for CreateScramUser (64/65), DeleteScramUser
   (66/67), and ListScramUsers (68/69) in Python, Go, and Java. Match
   `crates/volant-protocol/src/payload.rs`.
2. **Public RPC** on each language client, matching the Rust shape.
3. Non-zero `error_code` raises like every other RPC (`BrokerError` /
   `BrokerException`) with `op="create_scram_user"` /
   `"delete_scram_user"` / `"list_scram_users"`.
4. Unit tests without a broker: codec round-trip plus a fake TCP
   server (create ok; delete error 2; list names; error 23). Existing
   v0.46 handshake tests still pass.

## Non-goals

| Deferred | Why |
|----------|-----|
| Handshake / `connectScram` / `DialScram` changes | Already shipped (v0.46); opcodes 60–63 stay |
| Kafka AlterUserScramCredentials | Native 64–69 only |
| SCRAM-SHA-512 | Broker store is SHA-256 |
| New native opcodes | Reuse 64–69 |
| Broker / protocol / Rust client changes | Already shipped (Phase 22) |
| Kafka API keys | Frozen at 38 |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`. Payload integers are
little-endian. Strings are `u16` length + UTF-8 (`put_string`).

### Request opcode 64 `CreateScramUser`

```
username: string
password: string
iterations: u32    # 0 = broker default (4096)
```

### Response opcode 65 `CreateScramUser`

```
error_code: u16    # 0=ok; 3=invalid; 23=unauthorized
```

### Request opcode 66 `DeleteScramUser`

```
username: string
```

### Response opcode 67 `DeleteScramUser`

```
error_code: u16    # 0=ok; 2=not found; 23=unauthorized
```

### Request opcode 68 `ListScramUsers`

Empty payload.

### Response opcode 69 `ListScramUsers`

```
error_code: u16    # 0=ok; 23=unauthorized
username_count: u32
  for each: username string
```

Password is sent in the clear on CreateScramUser (same as Rust; use
TLS). This is **not** Kafka AlterUserScramCredentials.

## API

```python
c.create_scram_user(username: str, password: str, iterations: int = 0) -> None
c.delete_scram_user(username: str) -> None
c.list_scram_users() -> list[str]
```

```go
c.CreateScramUser(username, password string, iterations uint32) error
c.DeleteScramUser(username string) error
c.ListScramUsers() ([]string, error)
```

```java
c.createScramUser(String username, String password)           // iterations=0
c.createScramUser(String username, String password, int iterations)
c.deleteScramUser(String username)
c.listScramUsers()  // List<String>
```

Non-zero `error_code` raises `BrokerError(..., op="create_scram_user")`
(Python), `BrokerError{Code, Op: "create_scram_user"}` (Go), or
`BrokerException(code, "", "create_scram_user")` (Java) — same `op`
strings for delete / list.

Leave `connectScram` / `DialScram` / `scram_username` alone.

## Tests

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode create `alice` / `s3cret` / 4096 | request + response 0 |
| Encode/decode delete `alice` | request + response 0 |
| List request | empty bytes |
| List response two names | `alice`, `bob` |
| Fake server create ok | no raise; wire fields match |
| Fake server delete `error_code=2` | raises with `op="delete_scram_user"` |
| Fake server list | returns names |
| Fake server `error_code=23` | raises with `op="list_scram_users"` |
| v0.46 handshake tests | still pass |

## Merge notes

Sibling slices **v0.51–v0.54** also edit the same codec / Client /
README files. When merging:

- **Keep all opcodes.** Do not drop 60–63 (handshake) or 64–69
  (admin). `decode_response` / `DecodeResponse` / `decodeResponse` is
  a switch — union every case.
- Admin path is **additive**. Do not reuse 64–69 for a new API.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- Admin path only. Does not change the v0.46 handshake (60–63).
- Password is sent in the clear on CreateScramUser (same as Rust; use
  TLS).
- Not Kafka AlterUserScramCredentials. Native 64–69 only.
- Still SHA-256 store (broker). No SCRAM-SHA-512 on these clients.
- Shared-token Auth (v0.42) still wins over handshake SCRAM when a
  token is set.
- No Kafka API keys / new opcodes / broker changes / Phase 155.

See [V46_SPEC.md](./V46_SPEC.md) (handshake 60–63) and
[PHASE22_SPEC.md](./PHASE22_SPEC.md) (broker store).
