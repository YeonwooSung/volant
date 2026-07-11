# Phase 4 — Lightweight Stream Processing (binding)

## Goals

Kafka Streams–like operators **in-process**, no separate cluster:

- Stateless: `map`, `filter`, `flat_map`, `foreach`
- Stateful: `reduce` (keyed), tumbling windows
- Source / sink to Volant topics via `volant-client`
- At-least-once: commit consumer offsets **after** successful sink produce
- Word-count example end-to-end on live broker

## Non-goals

- Exactly-once / transactions (stretch — document only)
- WASM plugins
- RocksDB (use in-memory `HashMap` state; optional file snapshot later)
- Distributed stream tasks / rebalance of stream workers (single process OK)

## Crate layout

```
crates/volant-stream/
  src/
    lib.rs
    operator.rs      # Operator trait (existing)
    pipeline.rs      # Pipeline (existing, extend)
    ops/
      mod.rs
      map.rs
      filter.rs
      flat_map.rs
      foreach.rs
      reduce.rs
    window.rs        # tumbling window aggregator
    state.rs         # KeyValueStore trait + MemoryStore
    source.rs        # TopicSource (GroupConsumer or plain fetch)
    sink.rs          # TopicSink (produce)
    topology.rs      # StreamBuilder / Topology
    runtime.rs       # StreamApp::run loop

crates/volant-examples/   # or examples/word_count binary
  word_count/
```

Prefer workspace member `crates/volant-examples` with bin `word-count`, **or**
`crates/volant-stream/examples/word_count.rs`. Example binary is required.

## Core types

```rust
pub trait Operator: Send {
    fn process(&mut self, record: Record) -> Result<Vec<Record>>;
    fn name(&self) -> &str { "operator" }
    /// Flush window/state timers; default no-op.
    fn punctuate(&mut self, now_ms: i64) -> Result<Vec<Record>> { Ok(vec![]) }
}

// Convenience constructors returning impl Operator / Box<dyn Operator>
pub fn map<F>(f: F) -> impl Operator where F: FnMut(Record) -> Result<Record> + Send + 'static;
pub fn filter<F>(f: F) -> impl Operator where F: FnMut(&Record) -> bool + Send + 'static;
pub fn flat_map<F>(f: F) -> impl Operator where F: FnMut(Record) -> Result<Vec<Record>> + Send + 'static;
pub fn foreach<F>(f: F) -> impl Operator where F: FnMut(&Record) + Send + 'static;

// Keyed reduce: key = record.key (empty key => "")
// value codec: user provides serialize/deserialize of aggregate via Bytes in record.value
pub struct Reduce<S, F> { ... } // or function reduce(init, add)

// Tumbling window: size_ms, emit record per key at window end with aggregated value
pub struct TumblingWindow<A> { ... }
```

### Record conventions for word-count

- Input: value = text line (UTF-8)
- After flat_map words: key = word, value = b"1" or empty
- After reduce/count: key = word, value = decimal count as UTF-8 bytes

## Topology API

```rust
let app = StreamBuilder::new("word-count")
    .source_topic("lines", SourceConfig { group_id: "wc-app", ... })
    .map(...)
    .filter(...)
    .flat_map(...)
    .reduce(...) // optional
    .sink_topic("counts")
    .build()?;

app.run(client).await?; // until ctrl-c or error
```

Fluent builder may box operators into a `Pipeline` plus source/sink handles.

### Runtime loop (at-least-once)

1. Poll `GroupConsumer` (or multi-partition fetch) for input records
2. `pipeline.process(records)`
3. Produce outputs to sink topic (acks=1)
4. `consumer.commit()` offsets
5. On crash between 3 and 4: possible duplicates (at-least-once) — document this

Also support **in-process** mode for tests: `Pipeline::process` without network.

## State store

```rust
pub trait KeyValueStore: Send {
    fn get(&self, key: &[u8]) -> Option<Bytes>;
    fn put(&mut self, key: Bytes, value: Bytes);
    fn delete(&mut self, key: &[u8]);
    fn iter(&self) -> ...; // optional
}
pub struct MemoryStore { map: HashMap<Bytes, Bytes> }
```

## Windowing (minimum viable)

- Tumbling windows only, event-time = `record.timestamp_ms` (or processing time if 0)
- On `punctuate(now)` or when event advances past window end, emit aggregates
- Runtime calls `punctuate` each poll with `now_ms`

Hopping windows = stretch; skip if time-boxed.

## Tests required

1. Unit: map/filter/flat_map/foreach pure transforms
2. Unit: reduce counts keys
3. Unit: tumbling window emits at boundary
4. Pipeline composition word-count offline (no broker)
5. Integration/e2e (optional if heavy): source→sink on live broker with word-count

## Example: word_count

```bash
# terminal 1
cargo run -p volant-server -- --data-dir /tmp/v --listen 127.0.0.1:9092
# create topics lines, counts
# terminal 2
cargo run -p volant-examples --bin word-count -- --broker 127.0.0.1:9092
# produce lines via CLI; observe counts topic
```

## Docs

- Update ROADMAP Phase 4 complete / Phase 5 next
- README programming model section
- `docs/PHASE4_SPEC.md` is binding

## CLI (optional stretch)

`volant stream word-count` not required if example binary exists.
