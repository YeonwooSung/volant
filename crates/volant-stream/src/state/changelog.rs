//! Broker changelog helpers for EOS durable state (v0.9).
//!
//! Staged store mutations are produced into a regular Volant topic inside the
//! same write-through transaction as sink records and group offsets. Local
//! redb is a cache; the log is the recoverability story.
//!
//! # Record format (version 1)
//!
//! - **key** — store key bytes
//! - **value** — store value bytes; **empty** = delete (tombstone)
//! - **header** `volant-changelog` = `1` (ASCII)
//!
//! Replay is best-effort last-write-wins per key from earliest (single
//! partition). Not a multi-partition restore or standby task.

use bytes::Bytes;
use volant_client::{Client, TransactionalProducer};
use volant_core::{Error, Message, Offset, Result};

use super::{DurableStore, KeyValueStore};

/// Default changelog topic when enabled without an explicit name.
///
/// Prefer `{topology_or_store}__changelog` when more than one app shares a
/// cluster. Internal default is this reserved name.
pub const DEFAULT_CHANGELOG_TOPIC: &str = "__volant_changelog";

/// Header name identifying a Volant changelog record.
pub const CHANGELOG_HEADER: &str = "volant-changelog";

/// Changelog format version (header value).
pub const CHANGELOG_VERSION: &[u8] = b"1";

/// Build a version-1 changelog produce message.
///
/// `value = None` is encoded as an empty payload (tombstone / delete).
pub fn changelog_message(key: Bytes, value: Option<Bytes>) -> Message {
    Message {
        key: Some(key),
        value: value.unwrap_or_default(),
        timestamp_ms: None,
        headers: vec![(
            CHANGELOG_HEADER.to_string(),
            Bytes::from_static(CHANGELOG_VERSION),
        )],
    }
}

/// Decode a fetched changelog record into `(key, value)` (`None` value = delete).
///
/// Records without a key are skipped (`None`). Empty value is a tombstone.
pub fn decode_changelog_record(key: Option<Bytes>, value: Bytes) -> Option<(Bytes, Option<Bytes>)> {
    let key = key?;
    if value.is_empty() {
        Some((key, None))
    } else {
        Some((key, Some(value)))
    }
}

/// Create `topic` with one partition if it is missing (cluster default RF).
pub async fn ensure_changelog_topic(client: &Client, topic: &str) -> Result<()> {
    let meta = client.metadata().await?;
    if meta.topics.iter().any(|t| t.name == topic) {
        return Ok(());
    }
    match client.create_topic(topic, 1).await {
        Ok(_) => Ok(()),
        Err(Error::InvalidArgument(msg)) if msg.contains("already exists") => Ok(()),
        Err(e) => Err(e),
    }
}

/// Produce staged deltas inside an open transaction (partition 0).
///
/// No-op when `deltas` is empty.
pub async fn produce_changelog_in_txn(
    txn: &TransactionalProducer,
    topic: &str,
    deltas: &[(Bytes, Option<Bytes>)],
) -> Result<()> {
    if deltas.is_empty() {
        return Ok(());
    }
    let messages: Vec<Message> = deltas
        .iter()
        .map(|(k, v)| changelog_message(k.clone(), v.clone()))
        .collect();
    txn.produce(topic, Some(0), messages).await?;
    Ok(())
}

/// Fetch committed changelog records from earliest (native committed-only view).
pub async fn fetch_changelog_records(
    client: &Client,
    topic: &str,
) -> Result<Vec<(Bytes, Option<Bytes>)>> {
    let mut out = Vec::new();
    let mut from = Offset::ZERO;
    loop {
        let fetched = client.fetch(topic, 0, from, 256, 0).await?;
        if fetched.records.is_empty() {
            break;
        }
        for r in &fetched.records {
            if let Some(delta) = decode_changelog_record(r.key.clone(), r.value.clone()) {
                out.push(delta);
            }
        }
        let last = fetched
            .records
            .last()
            .map(|r| r.offset)
            .unwrap_or(from.raw());
        let next = last.saturating_add(1);
        if next <= from.raw() {
            break;
        }
        from = Offset::new(next);
    }
    Ok(out)
}

/// Replay a changelog topic onto `store` (best-effort last-write-wins per key).
///
/// Applies committed records only (native fetch hides open/aborted ranges).
/// Does not create the topic — call [`ensure_changelog_topic`] first if needed.
pub async fn replay_changelog(
    store: &mut impl KeyValueStore,
    client: &Client,
    topic: &str,
) -> Result<()> {
    for (key, value) in fetch_changelog_records(client, topic).await? {
        store.apply_changelog(key, value);
    }
    Ok(())
}

impl DurableStore {
    /// Open (or create) a store at `path` and replay `topic` from earliest.
    ///
    /// Auto-creates the changelog topic (1 partition) if missing. Replay is
    /// last-write-wins per key — not a multi-partition or standby restore.
    pub async fn open_with_changelog(
        path: impl AsRef<std::path::Path>,
        client: &Client,
        topic: &str,
    ) -> Result<Self> {
        ensure_changelog_topic(client, topic).await?;
        let mut store = Self::open(path).map_err(|e| Error::Storage(e.to_string()))?;
        replay_changelog(&mut store, client, topic).await?;
        Ok(store)
    }
}
