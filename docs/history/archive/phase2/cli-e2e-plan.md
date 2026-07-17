# Phase 2 — CLI + E2E Plan (iteration 1)

## Goal

Ship a working `volant` CLI and localhost TCP e2e coverage for create → produce → fetch,
plus honest ROADMAP/README updates. Protocol/client/server net stack is incomplete in this
worktree, so this agent implements the missing pieces required by `docs/PHASE2_SPEC.md`.

## CLI commands

| Command | Behaviour |
|---------|-----------|
| `volant topic create NAME --partitions N --broker HOST:PORT` | `Client::create_topic` |
| `volant topic list --broker HOST:PORT` | `Client::metadata` → print topic names |
| `volant topic delete NAME --broker HOST:PORT` | `Client::delete_topic` |
| `volant produce TOPIC --value STR [--key STR] [--partition N] --broker HOST:PORT` | `Client::produce` |
| `volant consume TOPIC --partition N [--from OFFSET] [--max N] --broker HOST:PORT` | `Client::fetch` |
| `volant version` | Print package version |

Defaults: `--broker 127.0.0.1:9092`, create partitions=1, consume from=0, max=100.

## E2E test strategy

File: `crates/volant-client/tests/e2e_tcp.rs`

1. `TcpListener::bind("127.0.0.1:0")` — ephemeral port
2. Build `Broker` with `tempfile`/unique `data_dir` under `std::env::temp_dir()`
3. Spawn `volant_broker::net::serve_listener(listener, Arc<Broker>)`
4. `Client::connect` to local address
5. Assert:
   - create topic (multi-partition)
   - produce value, fetch same partition, payload match
   - same key → same partition (key stickiness via murmur2)
6. Tear down via drop / abort join handle

## ROADMAP / README updates

- Phase 2 milestones: mark TCP server, APIs, partitioning, client SDK, CLI as done when green
- Stretch items (auth, idempotent produce PID) remain open
- Next phase pointer → Phase 3 consumer groups
- README quick start: server + CLI create/produce/consume examples

## Dependencies on client/server APIs

| Layer | Required API | Status at plan time | Action |
|-------|--------------|---------------------|--------|
| protocol | real `Request`/`Response` + encode/decode/pack | placeholders | implement per PHASE2_SPEC |
| broker | `delete_topic`, `metadata`, `select_partition`, `partition_count` | missing | add |
| broker net | `run_server` / `serve_listener` | missing | add `volant_broker::net` |
| client | `Client::{connect,create_topic,delete_topic,metadata,produce,fetch}` | missing | add |
| server | listen loop | placeholder sleep | call `run_server` |
| CLI | clap wiring | stubs | wire to client |

## Implementation order

1. Protocol payloads (LE) + opcodes (DeleteTopic=5)
2. Broker admin/partition helpers + murmur2
3. Net accept/dispatch loop
4. Client sequential request/response over TCP
5. Server binary + CLI
6. E2E + docs + `cargo test --workspace`

## Non-goals

Auth/TLS, consumer groups, Kafka wire compatibility, exploit-style stress tooling.
