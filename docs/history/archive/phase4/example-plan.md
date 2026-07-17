# Phase 4 — Word-Count Example Plan (iteration 1)

## Goal

Ship a working **word-count** stream example binary and offline unit test that
demonstrate Phase 4 operators end-to-end:

```
source topic `lines` → flat_map (tokenize) → keyed count → sink topic `counts`
```

Binding: [`docs/PHASE4_SPEC.md`](../PHASE4_SPEC.md).

## Ownership

| Artifact | Location |
|----------|----------|
| Example binary | `crates/volant-examples` → `[[bin]] name = "word-count"` |
| Minimal operators | `crates/volant-stream` (`ops/`, extend `Operator` + `Pipeline`) |
| Offline test | `crates/volant-stream` unit tests + examples crate smoke |
| Docs | this plan, `docs/phase4/example-review.md` |

## Dependencies (status at plan time)

| Layer | Required | Status | Action |
|-------|----------|--------|--------|
| `Operator` / `Pipeline` | trait + chain | scaffold only | extend `punctuate`; keep process loop |
| Stateless ops | `flat_map` (required), map/filter/foreach | missing | implement in `ops/` |
| Stateful ops | keyed `reduce` / count | missing | implement `Reduce` + `count()` helper |
| Source / sink | GroupConsumer poll + produce | client ready | wire in example binary |
| Topology API | StreamBuilder | missing | optional; example may use Pipeline + client loop |
| `volant-examples` | workspace member | missing | create crate + bin |

## Topology & record conventions

Per PHASE4_SPEC:

1. **Input** (`lines`): value = UTF-8 text line; key optional/ignored
2. **After tokenize (`flat_map`)**: key = word bytes, value = `b"1"`
3. **After count (`reduce`)**: key = word, value = decimal count as UTF-8 bytes
4. **Sink** (`counts`): produce keyed messages; acks=1
5. **At-least-once**: commit group offsets **after** successful sink produce

## CLI flags

```
word-count --broker HOST:PORT --group GROUP --source TOPIC --sink TOPIC
```

| Flag | Default |
|------|---------|
| `--broker` | `127.0.0.1:9092` |
| `--group` | `word-count` |
| `--source` | `lines` |
| `--sink` | `counts` |

Also support `--help` via clap.

## Offline test (no broker)

Build a `Pipeline`:

```rust
Pipeline::new()
    .then(flat_map(tokenize_line))
    .then(count())  // or reduce summing u64
```

Feed records with values `"hello world"`, `"hello"`, assert sink records show
`hello→2`, `world→1` (order of first emission may vary; check final map state
or last-emitted per key).

## Live run (manual / docs)

```bash
# terminal 1
cargo run -p volant-server -- --data-dir /tmp/v --listen 127.0.0.1:9092
cargo run -p volant-cli -- topic create lines --partitions 1 --broker 127.0.0.1:9092
cargo run -p volant-cli -- topic create counts --partitions 1 --broker 127.0.0.1:9092

# terminal 2
cargo run -p volant-examples --bin word-count -- \
  --broker 127.0.0.1:9092 --group wc-app --source lines --sink counts

# terminal 3
cargo run -p volant-cli -- produce lines --value "hello world" --broker 127.0.0.1:9092
cargo run -p volant-cli -- consume counts --partition 0 --from 0 --broker 127.0.0.1:9092
```

## Implementation order

1. Extend `Operator` with default `punctuate`
2. Implement `ops::{map, filter, flat_map, foreach, reduce}` + re-exports
3. Word-count helpers (`tokenize_line`, `count` reduce) for reuse in test + binary
4. Create `volant-examples` workspace member with `word-count` bin
5. Offline unit test in `volant-stream`
6. `cargo build -p volant-examples` + `cargo test -p volant-stream`
7. Review doc + fix loop

## Non-goals

- Exactly-once / transactions
- Full StreamBuilder if Pipeline + client loop suffices
- RocksDB / windowing (not required for word-count)
- CLI `volant stream word-count` subcommand
