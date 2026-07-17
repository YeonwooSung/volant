# Phase 3 — CLI + E2E Plan (iteration 1)

## Goal

Ship working CLI group commands, optional `consume --group`, multi-consumer e2e
coverage, and honest ROADMAP/README updates for Phase 3. The group stack
(protocol payloads, coordinator, client `GroupConsumer`) is still placeholder
in this worktree, so this agent implements missing pieces per
`docs/PHASE3_SPEC.md`.

## CLI commands

| Command | Behaviour |
|---------|-----------|
| `volant group fetch-offsets --group G [--topic T --partition P] --broker HOST:PORT` | `OffsetFetch`; print topic/partition/offset/metadata |
| `volant group commit --group G --topic T --partition P --offset N --broker HOST:PORT` | Admin `OffsetCommit` (generation=0, empty member_id) |
| `volant consume TOPIC --group G [--max N] --broker HOST:PORT` | Join group, poll once (or until max msgs), commit, leave |
| `volant consume TOPIC --partition P [--from O] [--max N]` | Unchanged Phase 2 path (no group) |

Defaults: `--broker 127.0.0.1:9092`, consume max=100, group session_timeout=10000.

## E2E test strategy

File: `crates/volant-client/tests/e2e_group.rs`

1. Boot broker on `127.0.0.1:0` with unique temp `data_dir`
2. Create multi-partition topic (e.g. 4 partitions)
3. Produce messages to all partitions
4. Two `GroupConsumer`s join same group → disjoint assignments covering all partitions
5. Poll + commit; leave; new consumer joins → resumes from committed offsets
6. Protocol / assignor unit tests live in protocol + broker crates

## ROADMAP / README updates

- Phase 3 milestones: mark group membership, OffsetCommit/Fetch, range assignor, CLI done when green
- Lag metrics remain open (stretch)
- Sticky/cooperative assignor deferred (Phase 3.1)
- Next phase pointer → Phase 4 stream processing
- README: consumer groups section + CLI examples

## Dependencies (status at plan time)

| Layer | Required API | Status | Action |
|-------|--------------|--------|--------|
| protocol | opcodes 6–10 + LE payloads | placeholders for 6–7 only | implement per PHASE3_SPEC |
| broker | `GroupCoordinator`, `offset_store`, range assignor | missing | add modules + wire in net |
| client | `GroupConsumer` + offset RPCs | missing | add |
| CLI | group subcommands + consume --group | Phase 2 only | wire |
| e2e | multi-consumer | missing | `e2e_group.rs` |

## Implementation order

1. Protocol request/response payloads + error codes 9–12
2. Assignor + offset store + GroupCoordinator
3. Net dispatch for group opcodes
4. Client group/offset APIs + GroupConsumer
5. CLI + e2e + docs
6. `cargo test --workspace` / build cli+server

## Non-goals

Cooperative sticky assignor, static membership, cross-node coordinator,
transactional offsets, Kafka wire compatibility.
