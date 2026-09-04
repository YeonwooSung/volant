//! Idempotent producer state and transaction lifecycle helpers.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use volant_core::{Error, Message, MessageBatch, PartitionId, Result, TopicName};
use volant_protocol::ErrorCode;

use crate::broker_config::{
    self, KEY_FETCH_SESSION_IDLE_MS, KEY_FETCH_SESSION_MAX, KEY_OPEN_TXN_TIMEOUT_MS,
    KEY_PREPARED_TXN_TIMEOUT_MS, KEY_SWEEP_INTERVAL_MS, KEY_TRANSACTION_MAX_TIMEOUT_MS,
    KEY_TXN_COORDINATOR_TTL_MS,
};
use crate::kafka::codec::{txn_control_message, ControlMarkerType};
use crate::producer_state::{partition_key, ProducerStateFile, StoredBatch, StoredProducer};

use super::*;
use super::{
    topics_from_open, unix_now_ms, AbortedTxnMarker, IdempotentBatchState, OpenTxn, PreparedTxn,
    PreparedTxnsFile, ProducerEpochState, StoredAddedPartition, StoredPreparedPending,
    StoredPreparedTxn, StoredPreparedWritten, StoredTxnRange, TxnMarkersFile, TxnWrittenRange,
};

impl Broker {
    /// Allocate a contiguous producer-id block from `next_producer_id`.
    ///
    /// `fetch_add`s `len` and persists `next_id` the same way
    /// [`Self::init_producer_id`] does (`data_dir/__producer_state`).
    /// Not KRaft broker-epoch fencing. Returns
    /// `(producer_id_start, producer_id_len)`.
    pub fn allocate_producer_ids(&self, len: u32) -> (u64, u32) {
        let start = self
            .next_producer_id
            .fetch_add(len as u64, Ordering::Relaxed);
        let _ = self.persist_producer_state();
        (start, len)
    }

    /// Allocate a producer id + epoch for idempotent produce (Phase 10/11).
    ///
    /// State is persisted under `data_dir/__producer_state` (Phase 11).
    pub fn init_producer_id(&self) -> (u64, u16) {
        let r = self.init_producer_id_with_opts("", false, false, 0);
        (r.producer_id, r.epoch)
    }

    /// Allocate (or fence) a producer id, optionally transactional (Phase 18).
    ///
    /// Non-empty `transactional_id` fences any prior owner of that id by bumping
    /// epoch and clearing open transactions / sequences. Does not enable 2PC
    /// (use [`Self::init_producer_id_with_opts`] for InitProducerId v6).
    /// Uses broker-default open-txn timeout (Phase 93).
    pub fn init_producer_id_with_txn(&self, transactional_id: &str) -> (u64, u16) {
        let r = self.init_producer_id_with_opts(transactional_id, false, false, 0);
        (r.producer_id, r.epoch)
    }

    /// Current prepared-txn timeout in milliseconds (Phase 92).
    ///
    /// `0` means auto-abort is disabled.
    pub fn prepared_txn_timeout_ms(&self) -> u64 {
        self.prepared_txn_timeout_ms.load(Ordering::Relaxed)
    }

    /// Override prepared-txn timeout (Phase 92). `0` disables auto-abort.
    pub fn set_prepared_txn_timeout_ms(&self, timeout_ms: u64) {
        self.prepared_txn_timeout_ms
            .store(timeout_ms, Ordering::Relaxed);
    }

    /// Current broker-default open-txn timeout in milliseconds (Phase 93).
    ///
    /// `0` means open auto-abort is disabled for producers without a positive
    /// client `transaction_timeout_ms`.
    pub fn open_txn_timeout_ms(&self) -> u64 {
        self.open_txn_timeout_ms.load(Ordering::Relaxed)
    }

    /// Override broker-default open-txn timeout (Phase 93). `0` disables when
    /// used as the effective timeout.
    pub fn set_open_txn_timeout_ms(&self, timeout_ms: u64) {
        self.open_txn_timeout_ms
            .store(timeout_ms, Ordering::Relaxed);
    }

    /// Current broker max transaction timeout in milliseconds (Phase 96).
    ///
    /// `0` means no max (clamp + InitProducerId over-max reject disabled).
    pub fn transaction_max_timeout_ms(&self) -> u64 {
        self.transaction_max_timeout_ms.load(Ordering::Relaxed)
    }

    /// Override broker max transaction timeout (Phase 96). `0` disables the max.
    pub fn set_transaction_max_timeout_ms(&self, timeout_ms: u64) {
        self.transaction_max_timeout_ms
            .store(timeout_ms, Ordering::Relaxed);
    }

    /// Background sweep interval in milliseconds (Phase 97/101/106).
    ///
    /// `0` pauses the background sweeper (lazy expire remains). The task is
    /// always spawned from [`crate::net::start_background_tasks`] so a later
    /// `0 → >0` transition takes effect without process restart. Shutdown via
    /// [`crate::BackgroundTasks::shutdown`] stops the loop cleanly.
    pub fn sweep_interval_ms(&self) -> u64 {
        self.sweep_interval_ms.load(Ordering::Relaxed)
    }

    /// Override background sweep interval (Phase 97/101/106). `0` pauses
    /// background work; `>0` enables/resumes on the next poll cycle without
    /// restart (until [`crate::BackgroundTasks::shutdown`]).
    pub fn set_sweep_interval_ms(&self, interval_ms: u64) {
        self.sweep_interval_ms.store(interval_ms, Ordering::Relaxed);
    }

    /// Init-owner registry TTL in ms (Phase 127/128). `0` disables GC.
    pub fn txn_coordinator_ttl_ms(&self) -> u64 {
        self.txn_coordinator_ttl_ms.load(Ordering::Relaxed)
    }

    /// Override Init-owner registry TTL (Phase 128 BROKER config / tests).
    pub fn set_txn_coordinator_ttl_ms(&self, ttl_ms: u64) {
        self.txn_coordinator_ttl_ms.store(ttl_ms, Ordering::Relaxed);
    }

    /// Current broker-level config entries for Kafka DescribeConfigs BROKER
    /// (Phase 99–102). Values are live knobs (product → env → sparse durable →
    /// setters/alter).
    pub fn describe_broker_configs(&self) -> Vec<(String, String)> {
        vec![
            (
                KEY_TRANSACTION_MAX_TIMEOUT_MS.into(),
                self.transaction_max_timeout_ms().to_string(),
            ),
            (
                KEY_OPEN_TXN_TIMEOUT_MS.into(),
                self.open_txn_timeout_ms().to_string(),
            ),
            (
                KEY_PREPARED_TXN_TIMEOUT_MS.into(),
                self.prepared_txn_timeout_ms().to_string(),
            ),
            (
                KEY_FETCH_SESSION_IDLE_MS.into(),
                self.fetch_session_idle_ms().to_string(),
            ),
            (
                KEY_FETCH_SESSION_MAX.into(),
                self.fetch_session_max().to_string(),
            ),
            (
                KEY_SWEEP_INTERVAL_MS.into(),
                self.sweep_interval_ms().to_string(),
            ),
            (
                KEY_TXN_COORDINATOR_TTL_MS.into(),
                self.txn_coordinator_ttl_ms().to_string(),
            ),
        ]
    }

    /// Apply broker-level config updates (Phase 99 Alter / IncrementalAlter).
    ///
    /// Empty value restores the **product** default for that key live (not env).
    /// Unknown keys → [`Error::InvalidArgument`].
    ///
    /// Phase 100–102: on success, merges a **sparse** durable overlay under
    /// `{data_dir}/__broker_config/state.json` — only keys present in `entries`
    /// are written (SET) or removed (DELETE/empty). Keys never altered are not
    /// frozen, so env still applies for them on restart. Direct `set_*` setters
    /// remain process-local only.
    ///
    /// Phase 113: in cluster mode only the **controller** may alter; others get
    /// [`Error::InvalidArgument`] `"not controller"`. On controller success,
    /// returns `Some(generation)` for inter-broker fan-out; single-node returns
    /// `None`.
    pub fn alter_broker_configs(&self, entries: &[(String, String)]) -> Result<Option<u64>> {
        if self.cluster.is_some() && !self.is_controller() {
            return Err(Error::InvalidArgument("not controller".into()));
        }
        self.apply_and_persist_broker_configs(entries)?;
        if self.cluster.is_some() {
            let gen = self.config_generation.fetch_add(1, Ordering::SeqCst) + 1;
            self.applied_config_generation.store(gen, Ordering::SeqCst);
            self.persist_cluster_admin_gens();
            Ok(Some(gen))
        } else {
            Ok(None)
        }
    }

    /// Apply + sparse-persist BROKER knobs without controller / generation gates.
    pub(super) fn apply_and_persist_broker_configs(
        &self,
        entries: &[(String, String)],
    ) -> Result<()> {
        broker_config::validate_entries(entries)?;
        for (k, v) in entries {
            let val = broker_config::resolve_value(k, v)?;
            self.apply_broker_config_value(k, val)?;
        }
        self.persist_broker_config_sparse(entries)
    }

    /// Apply a single known broker config key (no persist).
    pub(super) fn apply_broker_config_value(&self, key: &str, val: u64) -> Result<()> {
        match key {
            KEY_TRANSACTION_MAX_TIMEOUT_MS => self.set_transaction_max_timeout_ms(val),
            KEY_OPEN_TXN_TIMEOUT_MS => self.set_open_txn_timeout_ms(val),
            KEY_PREPARED_TXN_TIMEOUT_MS => self.set_prepared_txn_timeout_ms(val),
            KEY_FETCH_SESSION_IDLE_MS => self.set_fetch_session_idle_ms(val),
            KEY_FETCH_SESSION_MAX => {
                // Cap absurd values to usize::MAX on 32-bit; normal paths fit.
                let max = usize::try_from(val).unwrap_or(usize::MAX);
                self.set_fetch_session_max(max);
            }
            KEY_SWEEP_INTERVAL_MS => self.set_sweep_interval_ms(val),
            KEY_TXN_COORDINATOR_TTL_MS => self.set_txn_coordinator_ttl_ms(val),
            _ => {
                return Err(Error::InvalidArgument(format!(
                    "unknown broker config key: {key}"
                )));
            }
        }
        Ok(())
    }

    /// Load sparse durable BROKER knobs from `{data_dir}/__broker_config/state.json`
    /// (Phase 100–102). Applied **after** product default + env at construction;
    /// only keys present in the file override.
    pub(super) fn load_durable_broker_config(&self) -> Result<()> {
        let store = broker_config::BrokerConfigStore::open(&self.storage.data_dir)?;
        let Some(file) = store.load()? else {
            return Ok(());
        };
        // Apply known keys only; ignore unknown for forward compatibility.
        for key in broker_config::BROKER_CONFIG_KEYS {
            if let Some(val) = file.configs.get(*key) {
                self.apply_broker_config_value(key, *val)?;
            }
        }
        Ok(())
    }

    /// Merge sparse durable overlay for the altered entries (Phase 102).
    ///
    /// SET writes/updates only those keys; DELETE/empty removes them. Empty
    /// overlay removes the file so env can re-apply on next restart.
    pub(super) fn persist_broker_config_sparse(&self, entries: &[(String, String)]) -> Result<()> {
        let store = broker_config::BrokerConfigStore::open(&self.storage.data_dir)?;
        store.merge_alter(entries)
    }

    /// Live open (non-prepared) transaction count (Phase 97 gauge).
    pub fn open_txn_count(&self) -> usize {
        self.open_txns.lock().len()
    }

    /// Live prepared transaction count (Phase 97 gauge).
    pub fn prepared_txn_count(&self) -> usize {
        self.prepared_txns.lock().len()
    }

    /// Total open txns auto-aborted by timeout (lazy + background; Phase 97).
    pub fn open_txns_expired_total(&self) -> u64 {
        self.open_txns_expired_total.load(Ordering::Relaxed)
    }

    /// Total prepared txns auto-aborted by timeout (lazy + background; Phase 97).
    pub fn prepared_txns_expired_total(&self) -> u64 {
        self.prepared_txns_expired_total.load(Ordering::Relaxed)
    }

    /// Soft abort markers fully dropped because their range was entirely below
    /// log start after DeleteRecords / retention / load GC (Phase 104).
    ///
    /// Phase 111 straddling clips do **not** increment this counter.
    pub fn aborted_markers_gc_total(&self) -> u64 {
        self.aborted_markers_gc_total.load(Ordering::Relaxed)
    }

    /// Count of soft abort markers currently held for a partition (Phase 104 tests).
    pub fn aborted_marker_count(&self, topic: &str, partition: u32) -> usize {
        let aborted = self.aborted_txns.lock();
        aborted
            .get(&(topic.to_owned(), partition))
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Soft abort marker ranges for a partition (Phase 111 tests).
    ///
    /// Returns `(producer_id, first_offset, end_offset)` sorted by first_offset.
    pub fn aborted_marker_ranges(&self, topic: &str, partition: u32) -> Vec<(u64, u64, u64)> {
        let aborted = self.aborted_txns.lock();
        let Some(list) = aborted.get(&(topic.to_owned(), partition)) else {
            return Vec::new();
        };
        let mut out: Vec<(u64, u64, u64)> = list
            .iter()
            .map(|m| (m.producer_id, m.first_offset, m.end_offset))
            .collect();
        out.sort_by_key(|e| e.1);
        out
    }

    /// Run one open/prepared timeout expiry + idle fetch-session eviction
    /// (Phase 97).
    ///
    /// Used by the background sweeper and tests. Lazy API paths still call
    /// [`Self::expire_timed_out_txns`] independently. Returns
    /// `(open_aborted, prepared_aborted, sessions_idle_evicted)`.
    ///
    /// Phase 127: also runs txn-coordinator registry TTL GC (count not returned
    /// here; see [`Self::expire_txn_coordinator_registry`] / metrics).
    pub fn sweep_timeouts(&self) -> (usize, usize, usize) {
        let (open_n, prep_n) = self.expire_timed_out_txns();
        let idle_n = self.fetch_sessions.evict_idle_now();
        let _ = self.expire_txn_coordinator_registry();
        (open_n, prep_n, idle_n)
    }

    /// Phase 127/128: drop stale Init-owner registry entries older than
    /// [`Self::txn_coordinator_ttl_ms`] (live knob: env → durable → Alter).
    ///
    /// Returns number of map entries removed (id + pid counted separately).
    /// `0` TTL disables GC.
    pub fn expire_txn_coordinator_registry(&self) -> usize {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ttl = self.txn_coordinator_ttl_ms();
        if ttl == 0 {
            return 0;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.txn_coordinator_registry.expire_stale(ttl, now)
    }

    /// Phase 127: cumulative registry GC removals.
    pub fn txn_coordinator_registry_gc_total(&self) -> u64 {
        self.txn_coordinator_registry.gc_total()
    }

    /// InitProducerId with Phase 90 2PC options (Enable2Pc / KeepPreparedTxn)
    /// and Phase 93 open-txn timeout.
    ///
    /// When a prepared txn exists for `transactional_id`:
    /// - `keep_prepared=true`: preserve it, return OngoingTxn* = prepared pid/epoch,
    ///   and **do not** fence (same producer identity).
    /// - `keep_prepared=false`: force-abort prepared, then fence as usual.
    ///
    /// `enable_2pc=true` marks the producer so the first EndTxn prepares rather
    /// than one-shot finalizing.
    ///
    /// `transaction_timeout_ms` (Phase 93): when **> 0**, stored as the
    /// producer open-txn timeout; when **≤ 0**, the producer uses the broker
    /// default (`open_txn_timeout_ms` / `VOLANT_OPEN_TXN_TIMEOUT_MS`).
    ///
    /// Phase 96: when broker max > 0 and client timeout **exceeds** max,
    /// returns `error_code = 50` (`INVALID_TRANSACTION_TIMEOUT`) without
    /// mutating producer state (Kafka-honest reject).
    ///
    /// Phase 92/93: timed-out prepared and open txns are auto-aborted before
    /// KeepPrepared / fence handling.
    pub fn init_producer_id_with_opts(
        &self,
        transactional_id: &str,
        enable_2pc: bool,
        keep_prepared: bool,
        transaction_timeout_ms: i32,
    ) -> InitProducerIdResult {
        self.expire_timed_out_txns();
        let client_timeout = if transaction_timeout_ms > 0 {
            transaction_timeout_ms as u64
        } else {
            0
        };
        // Phase 96: Kafka-honest reject when client timeout exceeds broker max.
        let max = self.transaction_max_timeout_ms.load(Ordering::Relaxed);
        if max > 0 && client_timeout > max {
            return InitProducerIdResult {
                error_code: 50, // INVALID_TRANSACTION_TIMEOUT
                producer_id: 0,
                epoch: 0,
                ongoing_txn_producer_id: -1,
                ongoing_txn_producer_epoch: -1,
            };
        }
        if transactional_id.is_empty() {
            let id = self.next_producer_id.fetch_add(1, Ordering::Relaxed);
            let epoch = 0u16;
            self.producer_state.write().insert(
                id,
                ProducerEpochState {
                    epoch,
                    transactional: false,
                    transactional_id: String::new(),
                    enable_2pc: false,
                    transaction_timeout_ms: client_timeout,
                    partitions: HashMap::new(),
                },
            );
            let _ = self.persist_producer_state();
            return InitProducerIdResult {
                error_code: 0,
                producer_id: id,
                epoch,
                ongoing_txn_producer_id: -1,
                ongoing_txn_producer_epoch: -1,
            };
        }

        // Prepared path: KeepPreparedTxn reuses identity without fencing.
        if keep_prepared {
            let prepared = self.prepared_txns.lock();
            if let Some(prep) = prepared.get(transactional_id) {
                let pid = prep.producer_id;
                let epoch = prep.producer_epoch;
                let ongoing_pid = prep.producer_id as i64;
                let ongoing_epoch = prep.producer_epoch as i16;
                drop(prepared);
                // Ensure producer state reflects enable_2pc + identity + timeout.
                {
                    let mut state = self.producer_state.write();
                    if let Some(prod) = state.get_mut(&pid) {
                        prod.transactional = true;
                        prod.transactional_id = transactional_id.to_owned();
                        prod.epoch = epoch;
                        if enable_2pc {
                            prod.enable_2pc = true;
                        }
                        prod.transaction_timeout_ms = client_timeout;
                    } else {
                        state.insert(
                            pid,
                            ProducerEpochState {
                                epoch,
                                transactional: true,
                                transactional_id: transactional_id.to_owned(),
                                enable_2pc,
                                transaction_timeout_ms: client_timeout,
                                partitions: HashMap::new(),
                            },
                        );
                    }
                }
                self.transactional_ids
                    .write()
                    .insert(transactional_id.to_owned(), pid);
                // Open non-prepared txn for this pid is still fenced/aborted.
                let fenced = self.open_txns.lock().remove(&pid);
                if let Some(txn) = fenced {
                    self.record_aborted_from_txn(pid, &txn);
                    self.append_txn_control_markers(pid, epoch, ControlMarkerType::Abort, &txn);
                }
                // Phase 94: fence / KeepPrepared clears abortable (new client epoch path).
                self.clear_txn_abortable(pid);
                self.note_txn_coordinator(transactional_id, pid, self.node_id);
                let _ = self.persist_producer_state();
                // KeepPrepared leaves prepare_* as the last coordinator-log state.
                return InitProducerIdResult {
                    error_code: 0,
                    producer_id: pid,
                    epoch,
                    ongoing_txn_producer_id: ongoing_pid,
                    ongoing_txn_producer_epoch: ongoing_epoch,
                };
            }
        } else {
            // Drop prepared (force abort) before normal fence/allocate.
            // Release the prepared_txns lock before force_abort (it re-locks to persist).
            let dropped = self.prepared_txns.lock().remove(transactional_id);
            if let Some(prep) = dropped {
                let pid = prep.producer_id;
                self.force_abort_prepared(prep);
                // Intentional force-abort is not a timeout abortable signal.
                self.clear_txn_abortable(pid);
            }
        }

        let mut txn_ids = self.transactional_ids.write();
        if let Some(&existing) = txn_ids.get(transactional_id) {
            let mut state = self.producer_state.write();
            if let Some(prod) = state.get_mut(&existing) {
                let old_epoch = prod.epoch;
                prod.epoch = prod.epoch.wrapping_add(1);
                if prod.epoch == 0 {
                    prod.epoch = 1;
                }
                prod.partitions.clear();
                prod.transactional = true;
                prod.transactional_id = transactional_id.to_owned();
                if enable_2pc {
                    prod.enable_2pc = true;
                }
                prod.transaction_timeout_ms = client_timeout;
                // Keep enable_2pc sticky if already set, unless caller is not
                // using v6 — still allow sticky true from prior Init.
                let epoch = prod.epoch;
                drop(state);
                // Fence: open write-through ranges become aborted (Phase 86).
                let fenced = self.open_txns.lock().remove(&existing);
                if let Some(txn) = fenced {
                    self.record_aborted_from_txn(existing, &txn);
                    self.append_txn_control_markers(
                        existing,
                        old_epoch,
                        ControlMarkerType::Abort,
                        &txn,
                    );
                    self.append_transaction_state(
                        transactional_id,
                        TXN_STATE_COMPLETE_ABORT,
                        existing,
                        old_epoch,
                        txn.opened_at_ms,
                    );
                }
                // Phase 94: epoch fence clears abortable for the new identity.
                self.clear_txn_abortable(existing);
                // Phase 120: this broker remains/becomes Init owner for the txn id.
                self.note_txn_coordinator(transactional_id, existing, self.node_id);
                let _ = self.persist_producer_state();
                self.append_transaction_state(
                    transactional_id,
                    TXN_STATE_EMPTY,
                    existing,
                    epoch,
                    0,
                );
                return InitProducerIdResult {
                    error_code: 0,
                    producer_id: existing,
                    epoch,
                    ongoing_txn_producer_id: -1,
                    ongoing_txn_producer_epoch: -1,
                };
            }
        }
        // Allocate new PID for this transactional id.
        let id = self.next_producer_id.fetch_add(1, Ordering::Relaxed);
        let epoch = 0u16;
        self.producer_state.write().insert(
            id,
            ProducerEpochState {
                epoch,
                transactional: true,
                transactional_id: transactional_id.to_owned(),
                enable_2pc,
                transaction_timeout_ms: client_timeout,
                partitions: HashMap::new(),
            },
        );
        txn_ids.insert(transactional_id.to_owned(), id);
        drop(txn_ids);
        // Phase 120: this broker is the Init owner / txn coordinator.
        self.note_txn_coordinator(transactional_id, id, self.node_id);
        let _ = self.persist_producer_state();
        self.append_transaction_state(transactional_id, TXN_STATE_EMPTY, id, epoch, 0);
        InitProducerIdResult {
            error_code: 0,
            producer_id: id,
            epoch,
            ongoing_txn_producer_id: -1,
            ongoing_txn_producer_epoch: -1,
        }
    }

    /// Begin a transaction for a transactional producer (Phase 18).
    ///
    /// Returns protocol error code (`0` = ok). Rejects when a prepared txn
    /// exists for this producer (Phase 90). Sets `opened_at_ms` (Phase 93).
    /// Phase 94: producers in the abortable set must EndTxn first.
    pub fn begin_txn(&self, producer_id: u64, producer_epoch: u16) -> u16 {
        self.expire_timed_out_txns();
        let state = self.producer_state.read();
        let Some(prod) = state.get(&producer_id) else {
            return ErrorCode::UnknownProducerId as u16;
        };
        if prod.epoch != producer_epoch {
            return ErrorCode::InvalidProducerEpoch as u16;
        }
        if !prod.transactional {
            return ErrorCode::InvalidTxnState as u16;
        }
        let txn_id = prod.transactional_id.clone();
        drop(state);
        if self.is_txn_abortable(producer_id) {
            return ErrorCode::TransactionAbortable as u16;
        }
        if !txn_id.is_empty() && self.prepared_txns.lock().contains_key(&txn_id) {
            return ErrorCode::InvalidTxnState as u16;
        }
        let opened_at_ms = unix_now_ms();
        {
            let mut open = self.open_txns.lock();
            if open.contains_key(&producer_id) {
                return ErrorCode::InvalidTxnState as u16;
            }
            open.insert(
                producer_id,
                OpenTxn {
                    opened_at_ms,
                    producer_epoch,
                    ..OpenTxn::default()
                },
            );
        }
        if !txn_id.is_empty() {
            self.append_transaction_state(
                &txn_id,
                TXN_STATE_ONGOING,
                producer_id,
                producer_epoch,
                opened_at_ms,
            );
        }
        0
    }

    /// Ensure a transaction is open (Phase 31 / Kafka AddPartitionsToTxn).
    ///
    /// If one is already open for this PID+epoch, returns success. Otherwise
    /// begins a new transaction (Kafka has no separate BeginTxn API).
    /// Phase 93: times out aged open txns first.
    /// Phase 94: if the open was just timed out (abortable set), returns
    /// [`ErrorCode::TransactionAbortable`] instead of silently opening a new
    /// txn — client must EndTxn first (AddOffsets/AddPartitions emit 123).
    pub fn ensure_txn_open(&self, producer_id: u64, producer_epoch: u16) -> u16 {
        // begin_txn also expires; call once here for the prepared/open check path.
        self.expire_timed_out_txns();
        let txn_id = {
            let state = self.producer_state.read();
            let Some(prod) = state.get(&producer_id) else {
                return ErrorCode::UnknownProducerId as u16;
            };
            if prod.epoch != producer_epoch {
                return ErrorCode::InvalidProducerEpoch as u16;
            }
            if !prod.transactional {
                return ErrorCode::InvalidTxnState as u16;
            }
            prod.transactional_id.clone()
        };
        if self.is_txn_abortable(producer_id) {
            return ErrorCode::TransactionAbortable as u16;
        }
        if !txn_id.is_empty() && self.prepared_txns.lock().contains_key(&txn_id) {
            return ErrorCode::InvalidTxnState as u16;
        }
        if self.has_open_txn(producer_id) {
            return 0;
        }
        self.begin_txn(producer_id, producer_epoch)
    }

    /// Record partitions successfully added via AddPartitionsToTxn (Phase 105).
    ///
    /// Membership is tracked even when no produce follows, so EndTxn and
    /// crash≡abort can append Kafka control batches for those partitions.
    /// Soft abort markers are **not** created for empty (no write-through)
    /// partitions. Idempotent: re-adding the same (topic, partition) is a no-op.
    ///
    /// Returns protocol error code (`0` = ok). Caller must already have opened
    /// the txn via [`Self::ensure_txn_open`] / [`Self::begin_txn`].
    pub fn record_txn_added_partitions(
        &self,
        producer_id: u64,
        partitions: &[(String, u32)],
    ) -> u16 {
        if partitions.is_empty() {
            return 0;
        }
        {
            let mut open = self.open_txns.lock();
            let Some(txn) = open.get_mut(&producer_id) else {
                return ErrorCode::InvalidTxnState as u16;
            };
            for (topic, part) in partitions {
                let key = (topic.clone(), *part);
                if !txn.added.iter().any(|(t, p)| t == topic && p == part) {
                    txn.added.push(key);
                }
            }
        }
        self.persist_txn_markers();
        0
    }

    /// Whether this producer currently has an open (non-prepared) transaction.
    pub fn has_open_txn(&self, producer_id: u64) -> bool {
        self.open_txns.lock().contains_key(&producer_id)
    }

    /// Topic → partitions for Kafka `TransactionLogValue` (v0.232).
    ///
    /// Collects `OpenTxn.added` + `written` (and pending keys) via
    /// [`topics_from_open`], then the matching [`PreparedTxn::open`] if the
    /// producer is prepared. Empty set is **null** (same as unknown). Topics
    /// and partition ids are sorted.
    pub fn txn_log_partitions(&self, producer_id: u64) -> Option<Vec<(String, Vec<i32>)>> {
        let from_open = |txn: &OpenTxn| {
            let topics = topics_from_open(txn);
            if topics.is_empty() {
                None
            } else {
                Some(topics)
            }
        };
        {
            let open = self.open_txns.lock();
            if let Some(txn) = open.get(&producer_id) {
                return from_open(txn);
            }
        }
        let prepared = self.prepared_txns.lock();
        prepared
            .values()
            .find(|p| p.producer_id == producer_id)
            .and_then(|p| from_open(&p.open))
    }

    /// List open + prepared transactions for ListTransactions (Phase 65/90).
    ///
    /// State is `"Ongoing"`, `"PrepareCommit"`, or `"PrepareAbort"`.
    /// Phase 92/93: timed-out prepared and open entries are auto-aborted first.
    pub fn list_open_transactions(&self) -> Vec<(String, u64, String)> {
        self.expire_timed_out_txns();
        let open = self.open_txns.lock();
        let prepared = self.prepared_txns.lock();
        let prods = self.producer_state.read();
        let mut out = Vec::with_capacity(open.len() + prepared.len());
        for &pid in open.keys() {
            let Some(prod) = prods.get(&pid) else {
                continue;
            };
            if prod.transactional_id.is_empty() {
                continue;
            }
            out.push((prod.transactional_id.clone(), pid, "Ongoing".to_string()));
        }
        for prep in prepared.values() {
            let state = if prep.commit {
                "PrepareCommit"
            } else {
                "PrepareAbort"
            };
            out.push((
                prep.transactional_id.clone(),
                prep.producer_id,
                state.to_string(),
            ));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Describe one transactional id for DescribeTransactions (Phase 66/90/92/93).
    ///
    /// Returns `None` when the transactional id is unknown. When known:
    /// - `"PrepareCommit"` / `"PrepareAbort"` if prepared (Phase 90)
    /// - `"Ongoing"` if an open txn exists
    /// - else `"Empty"`
    /// - topics/partitions from write-through ranges + pending keys
    /// - prepared: timeout = configured prepared timeout; start = `prepared_at_ms`
    /// - open: timeout = effective open timeout; start = `opened_at_ms` (Phase 93)
    /// - empty: timeout/start remain `0`
    ///
    /// Phase 92/93: timed-out prepared and open entries are auto-aborted first.
    pub fn describe_transaction(
        &self,
        transactional_id: &str,
    ) -> Option<(
        String,                  // state
        i32,                     // timeout_ms
        i64,                     // start_time_ms
        u64,                     // producer_id
        u16,                     // producer_epoch
        Vec<(String, Vec<i32>)>, // topics → partitions
    )> {
        self.expire_timed_out_txns();
        let txn_ids = self.transactional_ids.read();
        let Some(&pid) = txn_ids.get(transactional_id) else {
            return None;
        };
        drop(txn_ids);
        let prods = self.producer_state.read();
        let Some(prod) = prods.get(&pid) else {
            return None;
        };
        let epoch = prod.epoch;
        let open_timeout = self.effective_open_txn_timeout_ms(prod);
        drop(prods);

        // Prepared takes precedence over open (they should be mutually exclusive).
        {
            let prepared = self.prepared_txns.lock();
            if let Some(prep) = prepared.get(transactional_id) {
                let state = if prep.commit {
                    "PrepareCommit"
                } else {
                    "PrepareAbort"
                };
                let topics = topics_from_open(&prep.open);
                // Phase 96: report effective (clamped) prepared timeout.
                let timeout_ms = self.effective_prepared_txn_timeout_ms() as i32;
                return Some((
                    state.to_string(),
                    timeout_ms,
                    prep.prepared_at_ms,
                    prep.producer_id,
                    prep.producer_epoch,
                    topics,
                ));
            }
        }

        let open = self.open_txns.lock();
        if let Some(txn) = open.get(&pid) {
            let topics = topics_from_open(txn);
            Some((
                "Ongoing".to_string(),
                open_timeout as i32,
                txn.opened_at_ms,
                pid,
                epoch,
                topics,
            ))
        } else {
            Some(("Empty".to_string(), 0, 0, pid, epoch, Vec::new()))
        }
    }

    /// Active producers for a partition (DescribeProducers, Phase 66).
    ///
    /// Includes producers with committed sequences on the partition and those
    /// with open-txn write-through activity. Fields:
    /// `(producer_id, epoch, last_sequence, last_timestamp=-1, coordinator_epoch=0, txn_start_offset)`.
    /// `txn_start_offset` is the first open write-through offset when present, else `-1`.
    pub fn describe_producers_for_partition(
        &self,
        topic: &str,
        partition: u32,
    ) -> Vec<(u64, i32, i32, i64, i32, i64)> {
        self.expire_timed_out_txns();
        let key = (topic.to_owned(), partition);
        let prods = self.producer_state.read();
        let open = self.open_txns.lock();
        let prepared = self.prepared_txns.lock();
        let mut out = Vec::new();
        for (&pid, prod) in prods.iter() {
            let mut last_seq = -1i32;
            let mut in_scope = false;
            let mut txn_start = -1i64;
            if let Some(st) = prod.partitions.get(&key) {
                last_seq = st
                    .base_sequence
                    .saturating_add(st.count as i32)
                    .saturating_sub(1);
                in_scope = true;
            }
            if let Some(txn) = open.get(&pid) {
                if let Some(st) = txn.pending.get(&key) {
                    last_seq = st
                        .base_sequence
                        .saturating_add(st.count as i32)
                        .saturating_sub(1);
                    in_scope = true;
                }
                if let Some(first) = txn
                    .written
                    .iter()
                    .filter(|b| b.topic == topic && b.partition == partition)
                    .map(|b| b.first_offset)
                    .min()
                {
                    in_scope = true;
                    txn_start = first as i64;
                }
            }
            // Phase 90: prepared ranges also count as in-txn.
            if let Some(prep) = prepared.values().find(|p| p.producer_id == pid) {
                if let Some(st) = prep.open.pending.get(&key) {
                    last_seq = st
                        .base_sequence
                        .saturating_add(st.count as i32)
                        .saturating_sub(1);
                    in_scope = true;
                }
                if let Some(first) = prep
                    .open
                    .written
                    .iter()
                    .filter(|b| b.topic == topic && b.partition == partition)
                    .map(|b| b.first_offset)
                    .min()
                {
                    in_scope = true;
                    if txn_start < 0 || (first as i64) < txn_start {
                        txn_start = first as i64;
                    }
                }
            }
            if in_scope {
                out.push((pid, i32::from(prod.epoch), last_seq, -1, 0, txn_start));
            }
        }
        out.sort_by_key(|p| p.0);
        out
    }

    /// Whether a topic/partition exists (DescribeProducers).
    pub fn partition_exists(&self, topic: &str, partition: u32) -> bool {
        let name = TopicName::new(topic);
        self.topics
            .read()
            .get(&name)
            .map(|t| t.partitions.contains_key(&PartitionId(partition)))
            .unwrap_or(false)
    }

    /// Resolve topic name from numeric Volant topic id (Metadata TopicId lookup).
    pub fn topic_name_by_id(&self, topic_id: u32) -> Option<String> {
        let map = self.topics.read();
        map.values()
            .find(|t| t.id.0 == topic_id)
            .map(|t| t.name.as_str().to_owned())
    }

    /// Buffer consumer offsets to apply on commit (Phase 31 TxnOffsetCommit).
    ///
    /// Entries: `(group_id, topic, partition, offset, metadata)`.
    /// Phase 94: no open + abortable set → [`ErrorCode::TransactionAbortable`].
    pub fn buffer_txn_offsets(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        offsets: &[(String, String, u32, u64, String)],
    ) -> u16 {
        self.expire_timed_out_txns();
        let txn_id = {
            let state = self.producer_state.read();
            let Some(prod) = state.get(&producer_id) else {
                return ErrorCode::UnknownProducerId as u16;
            };
            if prod.epoch != producer_epoch {
                return ErrorCode::InvalidProducerEpoch as u16;
            }
            if !prod.transactional {
                return ErrorCode::InvalidTxnState as u16;
            }
            prod.transactional_id.clone()
        };
        if !txn_id.is_empty() && self.prepared_txns.lock().contains_key(&txn_id) {
            return ErrorCode::InvalidTxnState as u16;
        }
        let mut open = self.open_txns.lock();
        let Some(txn) = open.get_mut(&producer_id) else {
            return if self.is_txn_abortable(producer_id) {
                ErrorCode::TransactionAbortable as u16
            } else {
                ErrorCode::InvalidTxnState as u16
            };
        };
        txn.deferred_offsets.extend(offsets.iter().cloned());
        0
    }

    /// Whether the producer id is transactional (Phase 18).
    pub fn is_transactional_producer(&self, producer_id: u64) -> bool {
        self.producer_state
            .read()
            .get(&producer_id)
            .map(|p| p.transactional)
            .unwrap_or(false)
    }

    /// Write-through produce inside an open transaction (Phase 18/86).
    ///
    /// Appends to the partition log immediately and records a range that holds
    /// LSO back until EndTxn. On success returns [`IdempotentCheck::Accept`] or
    /// `Duplicate` with the real log base offset.
    pub fn buffer_txn_produce(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        topic: &str,
        partition: u32,
        base_sequence: i32,
        messages: Vec<Message>,
    ) -> IdempotentCheck {
        self.expire_timed_out_txns();
        let message_count = messages.len() as u32;
        if message_count == 0 {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidArg as u16,
            };
        }
        let txn_id = {
            let state = self.producer_state.read();
            let Some(prod) = state.get(&producer_id) else {
                return IdempotentCheck::Reject {
                    error_code: ErrorCode::UnknownProducerId as u16,
                };
            };
            if prod.epoch != producer_epoch {
                return IdempotentCheck::Reject {
                    error_code: ErrorCode::InvalidProducerEpoch as u16,
                };
            }
            if !prod.transactional {
                return IdempotentCheck::Reject {
                    error_code: ErrorCode::InvalidTxnState as u16,
                };
            }
            prod.transactional_id.clone()
        };
        if !txn_id.is_empty() && self.prepared_txns.lock().contains_key(&txn_id) {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidTxnState as u16,
            };
        }
        // Sequence check under the open-txn lock, then append outside it.
        let key = (topic.to_owned(), partition);
        {
            let open = self.open_txns.lock();
            let Some(txn) = open.get(&producer_id) else {
                // Phase 94: timeout auto-abort → TRANSACTION_ABORTABLE; else InvalidTxnState.
                let code = if self.is_txn_abortable(producer_id) {
                    ErrorCode::TransactionAbortable as u16
                } else {
                    ErrorCode::InvalidTxnState as u16
                };
                return IdempotentCheck::Reject { error_code: code };
            };
            let last = txn.pending.get(&key).cloned().or_else(|| {
                self.producer_state
                    .read()
                    .get(&producer_id)
                    .and_then(|p| p.partitions.get(&key).cloned())
            });
            match last {
                None => {}
                Some(last) => {
                    if base_sequence == last.base_sequence && message_count == last.count {
                        return IdempotentCheck::Duplicate {
                            base_offset: last.base_offset,
                            count: last.count,
                        };
                    }
                    let expected = last.base_sequence.saturating_add(last.count as i32);
                    if base_sequence != expected {
                        return IdempotentCheck::Reject {
                            error_code: ErrorCode::OutOfOrderSequence as u16,
                        };
                    }
                }
            }
        }

        // Write-through: append now so HWM advances and LSO can diverge.
        let topic_name = TopicName::new(topic);
        let mut mb = MessageBatch::default();
        mb.messages = messages;
        let (records, error_code) =
            match self.produce_with_acks(&topic_name, PartitionId(partition), mb, 1, None) {
                Ok(v) => v,
                Err(_) => {
                    return IdempotentCheck::Reject {
                        error_code: ErrorCode::Unknown as u16,
                    };
                }
            };
        if error_code != 0 {
            return IdempotentCheck::Reject { error_code };
        }
        let base_offset = records.first().map(|r| r.offset.raw()).unwrap_or(0);
        let end_offset = base_offset.saturating_add(message_count as u64);
        let _ = self.flush(&topic_name, PartitionId(partition));

        let mut open = self.open_txns.lock();
        let Some(txn) = open.get_mut(&producer_id) else {
            // Raced with EndTxn/fence/timeout after append — treat as aborted range.
            drop(open);
            self.push_aborted_marker(
                topic,
                partition,
                AbortedTxnMarker {
                    producer_id,
                    first_offset: base_offset,
                    end_offset,
                },
            );
            self.persist_txn_markers();
            let code = if self.is_txn_abortable(producer_id) {
                ErrorCode::TransactionAbortable as u16
            } else {
                ErrorCode::InvalidTxnState as u16
            };
            return IdempotentCheck::Reject { error_code: code };
        };
        txn.written.push(TxnWrittenRange {
            topic: topic.to_owned(),
            partition,
            first_offset: base_offset,
            end_offset,
            base_sequence,
            count: message_count,
        });
        txn.pending.insert(
            key,
            IdempotentBatchState {
                base_sequence,
                count: message_count,
                base_offset,
            },
        );
        drop(open);
        self.persist_txn_markers();
        IdempotentCheck::Accept { base_offset }
    }

    /// Commit or abort an open transaction (Phase 18/86/89/90).
    ///
    /// On commit, written ranges become stable (sequences finalized) and deferred
    /// offsets are applied. On abort, soft markers cover written ranges so
    /// READ_COMMITTED / native fetch hide them; data remains on the log for
    /// READ_UNCOMMITTED.
    ///
    /// Phase 89: dual-write Kafka-style control markers (COMMIT/ABORT) onto each
    /// partition that had write-through ranges (on **finalize** only).
    ///
    /// Phase 90: when the producer has `enable_2pc`, the first EndTxn moves the
    /// open txn to **Prepared** (no markers yet). A second EndTxn with the same
    /// decision finalizes. Prepared txns also complete via this path.
    ///
    /// Phase 92/93: timed-out prepared and open txns are auto-aborted before
    /// finalize/prepare.
    ///
    /// Phase 94: when no open/prepared remains and the producer is in the
    /// abortable set (timeout auto-abort), returns
    /// [`ErrorCode::TransactionAbortable`] and clears the flag so a subsequent
    /// begin/ensure can open a new txn.
    ///
    /// `offsets` entries are `(group_id, topic, partition, offset, metadata)`.
    ///
    /// Returns `(error_code, commit_results, cluster_fanout)`. Callers in cluster
    /// mode must run [`Txn2pcFanout`] via inter-broker RPC after a `0` error
    /// (Phase 114). On prepare fan-out failure, call [`Self::rollback_local_prepare`].
    pub fn end_txn(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        committed: bool,
        offsets: &[(String, String, u32, u64, String)],
    ) -> Result<(u16, Vec<TxnCommitResult>, Txn2pcFanout)> {
        self.expire_timed_out_txns();
        let (enable_2pc, transactional_id) = {
            let state = self.producer_state.read();
            let Some(prod) = state.get(&producer_id) else {
                return Ok((
                    ErrorCode::UnknownProducerId as u16,
                    Vec::new(),
                    Txn2pcFanout::None,
                ));
            };
            if prod.epoch != producer_epoch {
                return Ok((
                    ErrorCode::InvalidProducerEpoch as u16,
                    Vec::new(),
                    Txn2pcFanout::None,
                ));
            }
            (prod.enable_2pc, prod.transactional_id.clone())
        };

        // Phase 90: finalize an existing prepared txn (second EndTxn).
        if !transactional_id.is_empty() {
            let mut prepared = self.prepared_txns.lock();
            if let Some(prep) = prepared.get(&transactional_id) {
                if prep.producer_id != producer_id {
                    return Ok((
                        ErrorCode::InvalidTxnState as u16,
                        Vec::new(),
                        Txn2pcFanout::None,
                    ));
                }
                if prep.producer_epoch != producer_epoch {
                    return Ok((
                        ErrorCode::InvalidProducerEpoch as u16,
                        Vec::new(),
                        Txn2pcFanout::None,
                    ));
                }
                if prep.commit != committed {
                    return Ok((
                        ErrorCode::InvalidTxnState as u16,
                        Vec::new(),
                        Txn2pcFanout::None,
                    ));
                }
                let prep = prepared.remove(&transactional_id).expect("just checked");
                drop(prepared);
                // Completing a live prepare clears any stale abortable mark.
                self.clear_txn_abortable(producer_id);
                let start_ms = prep.prepared_at_ms;
                let results =
                    self.finalize_txn(producer_id, producer_epoch, committed, prep.open, offsets)?;
                self.persist_prepared_txns();
                self.clear_cluster_prepared_index(&transactional_id);
                let state = if committed {
                    TXN_STATE_COMPLETE_COMMIT
                } else {
                    TXN_STATE_COMPLETE_ABORT
                };
                self.append_transaction_state(
                    &transactional_id,
                    state,
                    producer_id,
                    producer_epoch,
                    start_ms,
                );
                let fanout = if self.cluster.is_some() {
                    Txn2pcFanout::Complete {
                        transactional_id,
                        producer_id,
                        producer_epoch,
                        commit: committed,
                    }
                } else {
                    Txn2pcFanout::None
                };
                return Ok((0, results, fanout));
            }
        }

        let txn = {
            let mut open = self.open_txns.lock();
            match open.remove(&producer_id) {
                Some(t) => t,
                None => {
                    // Phase 94: timeout already aborted → TRANSACTION_ABORTABLE.
                    if self.take_txn_abortable(producer_id) {
                        return Ok((
                            ErrorCode::TransactionAbortable as u16,
                            Vec::new(),
                            Txn2pcFanout::None,
                        ));
                    }
                    return Ok((
                        ErrorCode::InvalidTxnState as u16,
                        Vec::new(),
                        Txn2pcFanout::None,
                    ));
                }
            }
        };
        // Successful open finalize also clears abortable (defensive).
        self.clear_txn_abortable(producer_id);

        // Phase 90: first EndTxn on a 2PC producer → prepare (durable).
        if enable_2pc && !transactional_id.is_empty() {
            let prepared_at_ms = unix_now_ms();
            let prep = PreparedTxn {
                transactional_id: transactional_id.clone(),
                producer_id,
                producer_epoch,
                commit: committed,
                prepared_at_ms,
                open: txn,
            };
            self.prepared_txns
                .lock()
                .insert(transactional_id.clone(), prep);
            // Open ranges leave open markers; prepared holds LSO via prepared map.
            self.persist_txn_markers();
            self.persist_prepared_txns();
            self.upsert_cluster_prepared_index(
                &transactional_id,
                producer_id,
                producer_epoch,
                committed,
            );
            let state = if committed {
                TXN_STATE_PREPARE_COMMIT
            } else {
                TXN_STATE_PREPARE_ABORT
            };
            self.append_transaction_state(
                &transactional_id,
                state,
                producer_id,
                producer_epoch,
                prepared_at_ms,
            );
            let fanout = if self.cluster.is_some() {
                Txn2pcFanout::Prepare {
                    transactional_id,
                    producer_id,
                    producer_epoch,
                    commit: committed,
                }
            } else {
                Txn2pcFanout::None
            };
            return Ok((0, Vec::new(), fanout));
        }

        let start_ms = txn.opened_at_ms;
        let results = self.finalize_txn(producer_id, producer_epoch, committed, txn, offsets)?;
        if !transactional_id.is_empty() {
            let state = if committed {
                TXN_STATE_COMPLETE_COMMIT
            } else {
                TXN_STATE_COMPLETE_ABORT
            };
            self.append_transaction_state(
                &transactional_id,
                state,
                producer_id,
                producer_epoch,
                start_ms,
            );
        }
        // Non-2PC one-shot: still fan out complete so peers that held open
        // ranges (from open fan-out) finalize consistently in cluster mode.
        let fanout = if self.cluster.is_some() && !transactional_id.is_empty() {
            Txn2pcFanout::Complete {
                transactional_id,
                producer_id,
                producer_epoch,
                commit: committed,
            }
        } else {
            Txn2pcFanout::None
        };
        Ok((0, results, fanout))
    }

    /// Finalize commit/abort for an open or prepared txn body.
    pub(super) fn finalize_txn(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        committed: bool,
        txn: OpenTxn,
        offsets: &[(String, String, u32, u64, String)],
    ) -> Result<Vec<TxnCommitResult>> {
        if !committed {
            self.record_aborted_from_txn(producer_id, &txn);
            self.append_txn_control_markers(
                producer_id,
                producer_epoch,
                ControlMarkerType::Abort,
                &txn,
            );
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(txn.written.len());
        for batch in &txn.written {
            self.record_idempotent_produce(
                producer_id,
                producer_epoch,
                &batch.topic,
                batch.partition,
                batch.base_sequence,
                batch.count,
                batch.first_offset,
            );
            results.push(TxnCommitResult {
                topic: batch.topic.clone(),
                partition: batch.partition,
                base_offset: batch.first_offset,
                count: batch.count,
            });
        }

        self.append_txn_control_markers(
            producer_id,
            producer_epoch,
            ControlMarkerType::Commit,
            &txn,
        );

        let mut all_offsets = txn.deferred_offsets;
        for o in offsets {
            all_offsets.push(o.clone());
        }
        for (group_id, topic, partition, offset, metadata) in &all_offsets {
            let _ = self.groups().commit_offsets(
                group_id,
                "",
                0,
                &[(topic.clone(), *partition, *offset, metadata.clone())],
            );
        }

        self.persist_txn_markers();
        Ok(results)
    }

    /// Last stable offset for a partition (Phase 86/90/92/93).
    ///
    /// Equal to HWM when no open/prepared write-through ranges exist; otherwise
    /// the minimum first offset among open **and prepared** transactional writes.
    ///
    /// Phase 92/93: expires timed-out prepared and open txns first so Fetch
    /// isolation advances without a separate txn API call.
    pub fn last_stable_offset(&self, topic: &str, partition: u32) -> u64 {
        self.expire_timed_out_txns();
        let hwm = self
            .high_watermark(&TopicName::new(topic), PartitionId(partition))
            .unwrap_or(0);
        let mut lso = hwm;
        {
            let open = self.open_txns.lock();
            for txn in open.values() {
                for r in &txn.written {
                    if r.topic == topic && r.partition == partition {
                        lso = lso.min(r.first_offset);
                    }
                }
            }
        }
        {
            let prepared = self.prepared_txns.lock();
            for prep in prepared.values() {
                for r in &prep.open.written {
                    if r.topic == topic && r.partition == partition {
                        lso = lso.min(r.first_offset);
                    }
                }
            }
        }
        lso
    }

    /// Aborted transactions overlapping `[fetch_offset, upper_bound)` for Fetch.
    ///
    /// Returns `(producer_id, first_offset)` pairs (Kafka aborted_transactions wire).
    pub fn aborted_transactions_for_fetch(
        &self,
        topic: &str,
        partition: u32,
        fetch_offset: u64,
        upper_bound: u64,
    ) -> Vec<(u64, u64)> {
        let aborted = self.aborted_txns.lock();
        let Some(list) = aborted.get(&(topic.to_owned(), partition)) else {
            return Vec::new();
        };
        let mut out: Vec<(u64, u64)> = list
            .iter()
            .filter(|m| m.first_offset < upper_bound && m.end_offset > fetch_offset)
            .map(|m| (m.producer_id, m.first_offset))
            .collect();
        out.sort_by_key(|e| e.1);
        out.dedup();
        out
    }

    /// Whether `offset` falls in an aborted transactional range on the partition.
    pub fn is_aborted_offset(&self, topic: &str, partition: u32, offset: u64) -> bool {
        let aborted = self.aborted_txns.lock();
        let Some(list) = aborted.get(&(topic.to_owned(), partition)) else {
            return false;
        };
        list.iter()
            .any(|m| offset >= m.first_offset && offset < m.end_offset)
    }

    /// Whether `offset` is still unstable (open or prepared write-through txn).
    pub fn is_unstable_offset(&self, topic: &str, partition: u32, offset: u64) -> bool {
        self.expire_timed_out_txns();
        {
            let open = self.open_txns.lock();
            for txn in open.values() {
                for r in &txn.written {
                    if r.topic == topic
                        && r.partition == partition
                        && offset >= r.first_offset
                        && offset < r.end_offset
                    {
                        return true;
                    }
                }
            }
        }
        {
            let prepared = self.prepared_txns.lock();
            for prep in prepared.values() {
                for r in &prep.open.written {
                    if r.topic == topic
                        && r.partition == partition
                        && offset >= r.first_offset
                        && offset < r.end_offset
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Force-abort a prepared txn (InitProducerId KeepPreparedTxn=false / Phase 92 timeout).
    ///
    /// Fence abort writes a single `complete_abort` on `__transaction_state`
    /// (not prepare_abort then complete_abort).
    pub(super) fn force_abort_prepared(&self, prep: PreparedTxn) {
        self.record_aborted_from_txn(prep.producer_id, &prep.open);
        self.append_txn_control_markers(
            prep.producer_id,
            prep.producer_epoch,
            ControlMarkerType::Abort,
            &prep.open,
        );
        self.persist_prepared_txns();
        self.append_transaction_state(
            &prep.transactional_id,
            TXN_STATE_COMPLETE_ABORT,
            prep.producer_id,
            prep.producer_epoch,
            prep.prepared_at_ms,
        );
    }

    /// Lazy expiry of timed-out open **and** prepared txns (Phase 92/93).
    ///
    /// Called at the start of txn/LSO paths and by the Phase 97 background
    /// sweeper. Returns `(open_aborted, prepared_aborted)`.
    pub fn expire_timed_out_txns(&self) -> (usize, usize) {
        let open_n = self.expire_timed_out_open_txns();
        let prep_n = self.expire_timed_out_prepared_txns();
        (open_n, prep_n)
    }

    /// Clamp a positive timeout to the broker max (Phase 96).
    ///
    /// `0` (disabled) is never raised or lowered. When max is `0`, no clamp.
    pub(super) fn clamp_txn_timeout_ms(&self, timeout_ms: u64) -> u64 {
        if timeout_ms == 0 {
            return 0;
        }
        let max = self.transaction_max_timeout_ms.load(Ordering::Relaxed);
        if max > 0 && timeout_ms > max {
            max
        } else {
            timeout_ms
        }
    }

    /// Effective open-txn timeout for a producer (Phase 93 + 96 clamp).
    ///
    /// Positive client timeout wins; otherwise broker default. Then clamped to
    /// [`Self::transaction_max_timeout_ms`] when max > 0. `0` = disabled.
    pub(super) fn effective_open_txn_timeout_ms(&self, prod: &ProducerEpochState) -> u64 {
        let raw = if prod.transaction_timeout_ms > 0 {
            prod.transaction_timeout_ms
        } else {
            self.open_txn_timeout_ms.load(Ordering::Relaxed)
        };
        self.clamp_txn_timeout_ms(raw)
    }

    /// Effective prepared-txn timeout (Phase 92 + 96 clamp).
    ///
    /// Configured prepared timeout, clamped to broker max when max > 0.
    /// `0` = disabled.
    pub(super) fn effective_prepared_txn_timeout_ms(&self) -> u64 {
        self.clamp_txn_timeout_ms(self.prepared_txn_timeout_ms.load(Ordering::Relaxed))
    }

    /// Auto-abort open (non-prepared) transactions older than their effective
    /// timeout (Phase 93 + 96 clamp).
    ///
    /// Returns the number of open txns aborted. Same effect as EndTxn(abort):
    /// soft markers + ABORT control batches; deferred offsets dropped.
    pub fn expire_timed_out_open_txns(&self) -> usize {
        let now = unix_now_ms();
        let expired: Vec<(u64, u16, OpenTxn)> = {
            let mut open = self.open_txns.lock();
            if open.is_empty() {
                return 0;
            }
            let prods = self.producer_state.read();
            let broker_default = self.open_txn_timeout_ms.load(Ordering::Relaxed);
            let mut keys: Vec<u64> = Vec::new();
            for (&pid, txn) in open.iter() {
                let raw = prods
                    .get(&pid)
                    .map(|p| {
                        if p.transaction_timeout_ms > 0 {
                            p.transaction_timeout_ms
                        } else {
                            broker_default
                        }
                    })
                    .unwrap_or(broker_default);
                let timeout = self.clamp_txn_timeout_ms(raw);
                if timeout == 0 {
                    continue;
                }
                let opened = if txn.opened_at_ms > 0 {
                    txn.opened_at_ms
                } else {
                    // Defensive: treat missing clock as "now" (do not mass-abort).
                    now
                };
                if now.saturating_sub(opened) >= timeout as i64 {
                    keys.push(pid);
                }
            }
            keys.into_iter()
                .filter_map(|pid| {
                    let txn = open.remove(&pid)?;
                    let epoch = prods.get(&pid).map(|p| p.epoch).unwrap_or(0);
                    Some((pid, epoch, txn))
                })
                .collect()
        };
        let n = expired.len();
        for (pid, epoch, txn) in expired {
            self.record_aborted_from_txn(pid, &txn);
            self.append_txn_control_markers(pid, epoch, ControlMarkerType::Abort, &txn);
            // Phase 94: client must observe TRANSACTION_ABORTABLE until EndTxn.
            self.mark_txn_abortable(pid);
            let tid = self
                .producer_state
                .read()
                .get(&pid)
                .map(|p| p.transactional_id.clone())
                .unwrap_or_default();
            if !tid.is_empty() {
                self.append_transaction_state(
                    &tid,
                    TXN_STATE_COMPLETE_ABORT,
                    pid,
                    epoch,
                    txn.opened_at_ms,
                );
            }
        }
        if n > 0 {
            self.persist_txn_markers();
            self.open_txns_expired_total
                .fetch_add(n as u64, Ordering::Relaxed);
        }
        n
    }

    /// Auto-abort prepared transactions older than the effective timeout
    /// (Phase 92 + 96 clamp).
    ///
    /// Returns the number of prepared txns aborted. No-op when effective
    /// timeout is `0` (disabled) or the prepared map is empty. Same finalize
    /// path as KeepPreparedTxn=false force-abort. Phase 94 marks abortable
    /// producers.
    pub fn expire_timed_out_prepared_txns(&self) -> usize {
        let timeout_ms = self.effective_prepared_txn_timeout_ms();
        if timeout_ms == 0 {
            return 0;
        }
        let now = unix_now_ms();
        let expired: Vec<PreparedTxn> = {
            let mut map = self.prepared_txns.lock();
            if map.is_empty() {
                return 0;
            }
            let keys: Vec<String> = map
                .iter()
                .filter(|(_, prep)| now.saturating_sub(prep.prepared_at_ms) >= timeout_ms as i64)
                .map(|(k, _)| k.clone())
                .collect();
            keys.into_iter().filter_map(|k| map.remove(&k)).collect()
        };
        let n = expired.len();
        for prep in expired {
            // Soft markers + control batches; persist once at the end via last call.
            self.record_aborted_from_txn(prep.producer_id, &prep.open);
            self.append_txn_control_markers(
                prep.producer_id,
                prep.producer_epoch,
                ControlMarkerType::Abort,
                &prep.open,
            );
            self.mark_txn_abortable(prep.producer_id);
            self.append_transaction_state(
                &prep.transactional_id,
                TXN_STATE_COMPLETE_ABORT,
                prep.producer_id,
                prep.producer_epoch,
                prep.prepared_at_ms,
            );
        }
        if n > 0 {
            self.persist_prepared_txns();
            self.prepared_txns_expired_total
                .fetch_add(n as u64, Ordering::Relaxed);
        }
        n
    }

    /// Whether this producer is in the Phase 94 abortable set (timeout auto-abort).
    pub fn is_txn_abortable(&self, producer_id: u64) -> bool {
        self.abortable_producers.lock().contains(&producer_id)
    }

    /// Mark producer as needing client abort acknowledgment (Phase 94).
    pub(super) fn mark_txn_abortable(&self, producer_id: u64) {
        self.abortable_producers.lock().insert(producer_id);
    }

    /// Clear abortable mark without returning whether it was set (Phase 94).
    pub(super) fn clear_txn_abortable(&self, producer_id: u64) {
        self.abortable_producers.lock().remove(&producer_id);
    }

    /// Clear and return whether the producer was abortable (Phase 94 EndTxn path).
    pub(super) fn take_txn_abortable(&self, producer_id: u64) -> bool {
        self.abortable_producers.lock().remove(&producer_id)
    }

    /// Backdate a prepared txn's `prepared_at_ms` for tests (Phase 92).
    ///
    /// `age_ms` is subtracted from the current wall clock. Returns `false` when
    /// the transactional id is not prepared.
    pub fn backdate_prepared_txn(&self, transactional_id: &str, age_ms: i64) -> bool {
        let mut map = self.prepared_txns.lock();
        let Some(prep) = map.get_mut(transactional_id) else {
            return false;
        };
        prep.prepared_at_ms = unix_now_ms().saturating_sub(age_ms.max(0));
        // Persist so restart-based tests see the aged timestamp.
        drop(map);
        self.persist_prepared_txns();
        true
    }

    /// Backdate an open txn's `opened_at_ms` for tests (Phase 93).
    ///
    /// `age_ms` is subtracted from the current wall clock. Returns `false` when
    /// the producer has no open txn.
    pub fn backdate_open_txn(&self, producer_id: u64, age_ms: i64) -> bool {
        let mut open = self.open_txns.lock();
        let Some(txn) = open.get_mut(&producer_id) else {
            return false;
        };
        txn.opened_at_ms = unix_now_ms().saturating_sub(age_ms.max(0));
        true
    }

    pub(super) fn prepared_txns_path(&self) -> PathBuf {
        self.storage
            .data_dir
            .join("__txn_prepared")
            .join("state.json")
    }

    /// Load durable prepared transactions (Phase 90/92). Prepared **survives** crash.
    pub(super) fn load_prepared_txns(&self) {
        let path = self.prepared_txns_path();
        let Ok(bytes) = fs::read(&path) else {
            return;
        };
        let Ok(file) = serde_json::from_slice::<PreparedTxnsFile>(&bytes) else {
            return;
        };
        let load_now = unix_now_ms();
        let mut map = self.prepared_txns.lock();
        for s in file.prepared {
            let mut pending = HashMap::new();
            for p in s.pending {
                pending.insert(
                    (p.topic, p.partition),
                    IdempotentBatchState {
                        base_sequence: p.base_sequence,
                        count: p.count,
                        base_offset: p.base_offset,
                    },
                );
            }
            let written = s
                .written
                .into_iter()
                .map(|w| TxnWrittenRange {
                    topic: w.topic,
                    partition: w.partition,
                    first_offset: w.first_offset,
                    end_offset: w.end_offset,
                    base_sequence: w.base_sequence,
                    count: w.count,
                })
                .collect();
            // Pre-Phase-92 snapshots lack prepared_at_ms (0) → start clock at load.
            let prepared_at_ms = if s.prepared_at_ms > 0 {
                s.prepared_at_ms
            } else {
                load_now
            };
            map.insert(
                s.transactional_id.clone(),
                PreparedTxn {
                    transactional_id: s.transactional_id,
                    producer_id: s.producer_id,
                    producer_epoch: s.producer_epoch,
                    commit: s.commit,
                    prepared_at_ms,
                    open: OpenTxn {
                        opened_at_ms: 0, // not used once prepared
                        producer_epoch: s.producer_epoch,
                        added: s.added,
                        written,
                        pending,
                        deferred_offsets: s.deferred_offsets,
                    },
                },
            );
        }
    }

    pub(super) fn persist_prepared_txns(&self) {
        let path = self.prepared_txns_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let mut file = PreparedTxnsFile::default();
        {
            let prepared = self.prepared_txns.lock();
            for prep in prepared.values() {
                let written = prep
                    .open
                    .written
                    .iter()
                    .map(|w| StoredPreparedWritten {
                        topic: w.topic.clone(),
                        partition: w.partition,
                        first_offset: w.first_offset,
                        end_offset: w.end_offset,
                        base_sequence: w.base_sequence,
                        count: w.count,
                    })
                    .collect();
                let pending = prep
                    .open
                    .pending
                    .iter()
                    .map(|((topic, part), st)| StoredPreparedPending {
                        topic: topic.clone(),
                        partition: *part,
                        base_sequence: st.base_sequence,
                        count: st.count,
                        base_offset: st.base_offset,
                    })
                    .collect();
                file.prepared.push(StoredPreparedTxn {
                    transactional_id: prep.transactional_id.clone(),
                    producer_id: prep.producer_id,
                    producer_epoch: prep.producer_epoch,
                    commit: prep.commit,
                    prepared_at_ms: prep.prepared_at_ms,
                    added: prep.open.added.clone(),
                    written,
                    pending,
                    deferred_offsets: prep.open.deferred_offsets.clone(),
                });
            }
        }
        let Ok(bytes) = serde_json::to_vec_pretty(&file) else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        if fs::write(&tmp, &bytes).is_ok() {
            let _ = fs::rename(tmp, path);
        }
    }

    pub(super) fn record_aborted_from_txn(&self, producer_id: u64, txn: &OpenTxn) {
        // Collapse per-partition first/end for the producer (Kafka lists first offset).
        let mut per_part: HashMap<(String, u32), (u64, u64)> = HashMap::new();
        for r in &txn.written {
            let e = per_part
                .entry((r.topic.clone(), r.partition))
                .or_insert((r.first_offset, r.end_offset));
            e.0 = e.0.min(r.first_offset);
            e.1 = e.1.max(r.end_offset);
        }
        for ((topic, part), (first, end)) in per_part {
            self.push_aborted_marker(
                &topic,
                part,
                AbortedTxnMarker {
                    producer_id,
                    first_offset: first,
                    end_offset: end,
                },
            );
        }
        self.persist_txn_markers();
    }

    /// Append one Kafka-style control marker per partition that participated in
    /// the txn (Phase 89 dual-write with soft markers; Phase 105 includes empty
    /// AddPartitions membership with no write-through data).
    pub(super) fn append_txn_control_markers(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        marker_type: ControlMarkerType,
        txn: &OpenTxn,
    ) {
        // One marker per (topic, partition), not per batch.
        let mut seen = HashMap::<(String, u32), ()>::new();
        for r in &txn.written {
            let key = (r.topic.clone(), r.partition);
            if seen.contains_key(&key) {
                continue;
            }
            seen.insert(key.clone(), ());
            let msg = txn_control_message(marker_type, producer_id, producer_epoch);
            let topic = TopicName::new(r.topic.clone());
            let _ = self.produce_one(&topic, PartitionId(r.partition), msg);
            let _ = self.flush(&topic, PartitionId(r.partition));
        }
        // Phase 105: control-only for AddPartitions membership without data.
        for (topic_name, partition) in &txn.added {
            let key = (topic_name.clone(), *partition);
            if seen.contains_key(&key) {
                continue;
            }
            seen.insert(key, ());
            let msg = txn_control_message(marker_type, producer_id, producer_epoch);
            let topic = TopicName::new(topic_name.clone());
            let _ = self.produce_one(&topic, PartitionId(*partition), msg);
            let _ = self.flush(&topic, PartitionId(*partition));
        }
    }

    pub(super) fn push_aborted_marker(
        &self,
        topic: &str,
        partition: u32,
        marker: AbortedTxnMarker,
    ) {
        let mut aborted = self.aborted_txns.lock();
        aborted
            .entry((topic.to_owned(), partition))
            .or_default()
            .push(marker);
    }

    /// GC / clip aborted soft markers against `log_start` (Phase 104 + 111).
    ///
    /// Markers cover `[first_offset, end_offset)`:
    /// - `end_offset <= log_start` → **drop** (Phase 104; no live overlap)
    /// - `first_offset < log_start < end_offset` → **clip** `first_offset =
    ///   log_start` (Phase 111; obsolete prefix no longer on the log)
    /// - `first_offset >= log_start` → unchanged
    ///
    /// Returns the number of markers **mutated** (dropped + clipped). The GC
    /// counter advances for **drops only** (Phase 104 semantics preserved).
    pub(super) fn gc_aborted_markers_below(
        &self,
        topic: &str,
        partition: u32,
        log_start: u64,
    ) -> usize {
        let key = (topic.to_owned(), partition);
        let mut aborted = self.aborted_txns.lock();
        let Some(list) = aborted.get_mut(&key) else {
            return 0;
        };
        let before = list.len();
        list.retain(|m| m.end_offset > log_start);
        let dropped = before - list.len();
        let mut clipped = 0usize;
        for m in list.iter_mut() {
            if m.first_offset < log_start {
                m.first_offset = log_start;
                clipped += 1;
            }
        }
        if list.is_empty() {
            aborted.remove(&key);
        }
        if dropped > 0 {
            self.aborted_markers_gc_total
                .fetch_add(dropped as u64, Ordering::Relaxed);
        }
        dropped + clipped
    }

    /// GC / clip markers for one partition and persist `__txn_markers` when any
    /// drop or clip occurred (Phase 104 + 111).
    pub(super) fn gc_and_persist_aborted_markers(
        &self,
        topic: &str,
        partition: u32,
        log_start: u64,
    ) {
        if self.gc_aborted_markers_below(topic, partition, log_start) > 0 {
            self.persist_txn_markers();
        }
    }

    /// GC / clip markers against each partition's current log start
    /// (Phase 104 + 111).
    ///
    /// Used after retention and on load (self-heal). Persists once if anything
    /// was dropped or clipped. Return value is total mutations (drops + clips).
    pub(super) fn gc_stale_aborted_markers_all(&self) -> usize {
        // Snapshot log starts under the topics read lock, then GC without it
        // (aborted_txns is a separate lock; avoid holding both).
        let starts: Vec<(String, u32, u64)> = {
            let topics = self.topics.read();
            let mut out = Vec::new();
            for t in topics.values() {
                for (pid, part) in &t.partitions {
                    out.push((
                        t.name.as_str().to_owned(),
                        pid.0,
                        part.log.log_start_offset().raw(),
                    ));
                }
            }
            out
        };
        let mut total = 0usize;
        for (topic, part, start) in starts {
            total += self.gc_aborted_markers_below(&topic, part, start);
        }
        if total > 0 {
            self.persist_txn_markers();
        }
        total
    }

    pub(super) fn txn_markers_path(&self) -> PathBuf {
        self.storage
            .data_dir
            .join("__txn_markers")
            .join("state.json")
    }

    /// Load soft markers; promote any stored open ranges to aborted (crash ≡ abort).
    ///
    /// Phase 98: when promoting open → aborted, also append ABORT control
    /// RecordBatches (same dual-write as EndTxn abort). Idempotent across
    /// restarts: only the open list is promoted (and then cleared on persist),
    /// so a second load sees empty `open` and does not re-append.
    ///
    /// Phase 105: empty AddPartitions membership (`open_added`) also gets
    /// ABORT control batches (no soft markers — nothing to filter).
    ///
    /// Phase 104/111: after load, drop markers fully below each partition's
    /// current log start and clip straddlers (self-heal after crash / older files).
    pub(super) fn load_txn_markers(&self) {
        let path = self.txn_markers_path();
        let Ok(bytes) = fs::read(&path) else {
            // Still run GC in case memory was seeded elsewhere (no-op normally).
            let _ = self.gc_stale_aborted_markers_all();
            return;
        };
        let Ok(file) = serde_json::from_slice::<TxnMarkersFile>(&bytes) else {
            return;
        };
        {
            let mut aborted = self.aborted_txns.lock();
            for m in &file.aborted {
                aborted
                    .entry((m.topic.clone(), m.partition))
                    .or_default()
                    .push(AbortedTxnMarker {
                        producer_id: m.producer_id,
                        first_offset: m.first_offset,
                        end_offset: m.end_offset,
                    });
            }
            // Crash recovery: open ranges → aborted soft markers.
            // open_added (Phase 105 empty membership) is intentionally omitted:
            // control-only; no soft range to promote.
            for m in &file.open {
                aborted
                    .entry((m.topic.clone(), m.partition))
                    .or_default()
                    .push(AbortedTxnMarker {
                        producer_id: m.producer_id,
                        first_offset: m.first_offset,
                        end_offset: m.end_offset,
                    });
            }
        }
        // Phase 98/105: dual-write ABORT control for crash-promoted opens
        // (written ranges + empty AddPartitions membership).
        if !file.open.is_empty() || !file.open_added.is_empty() {
            self.append_crash_abort_control_markers(&file.open, &file.open_added);
        }
        // Phase 104/111: drop markers entirely below current log start; clip
        // straddlers so first_offset is not below live log.
        let mutated = self.gc_stale_aborted_markers_all();
        // Persist cleaned state (no open ranges after recovery; GC/clip applied).
        // gc_stale already persists when mutated > 0; always persist once after
        // load so open→aborted promotion is durable even with zero GC.
        if mutated == 0 {
            self.persist_txn_markers();
        }
    }

    /// Append ABORT control markers for open ranges / empty membership promoted
    /// on crash recovery (Phase 98 + Phase 105). One marker per
    /// (producer_id, topic, partition).
    ///
    /// Epoch resolution order:
    /// 1. `producer_epoch` stored on the open marker (Phase 98 snapshots)
    /// 2. Live producer state epoch (best-effort for pre-98 files)
    /// 3. Skip control batch (soft abort still applied for written ranges)
    pub(super) fn append_crash_abort_control_markers(
        &self,
        open: &[StoredTxnRange],
        open_added: &[StoredAddedPartition],
    ) {
        // Group written ranges + empty membership by producer_id; track epoch.
        let mut by_pid: HashMap<u64, (Option<u16>, OpenTxn)> = HashMap::new();
        for m in open {
            let entry = by_pid.entry(m.producer_id).or_insert_with(|| {
                (
                    m.producer_epoch,
                    OpenTxn {
                        producer_epoch: m.producer_epoch.unwrap_or(0),
                        ..OpenTxn::default()
                    },
                )
            });
            if entry.0.is_none() {
                entry.0 = m.producer_epoch;
            }
            entry.1.written.push(TxnWrittenRange {
                topic: m.topic.clone(),
                partition: m.partition,
                first_offset: m.first_offset,
                end_offset: m.end_offset,
                base_sequence: 0,
                count: 0,
            });
        }
        for m in open_added {
            let entry = by_pid.entry(m.producer_id).or_insert_with(|| {
                (
                    m.producer_epoch,
                    OpenTxn {
                        producer_epoch: m.producer_epoch.unwrap_or(0),
                        ..OpenTxn::default()
                    },
                )
            });
            if entry.0.is_none() {
                entry.0 = m.producer_epoch;
            }
            let key = (m.topic.clone(), m.partition);
            if !entry
                .1
                .added
                .iter()
                .any(|(t, p)| t == &key.0 && *p == key.1)
            {
                entry.1.added.push(key);
            }
        }
        let mut crash_aborts: Vec<(u64, u16)> = Vec::new();
        for (pid, (stored_epoch, txn)) in by_pid {
            let epoch = match stored_epoch {
                Some(e) => e,
                None => {
                    // Pre-Phase-98 snapshot: best-effort from producer state.
                    let state = self.producer_state.read();
                    match state.get(&pid).map(|p| p.epoch) {
                        Some(e) => e,
                        None => {
                            // Cannot encode a honest control batch without epoch.
                            continue;
                        }
                    }
                }
            };
            self.append_txn_control_markers(pid, epoch, ControlMarkerType::Abort, &txn);
            crash_aborts.push((pid, epoch));
        }
        // v0.226: crash≡abort of open write-through also last-write-wins
        // complete_abort on the opt-in coordinator log (timeout already does).
        self.append_open_complete_aborts(crash_aborts);
    }

    /// Append `complete_abort` for open crash≡abort / leftover ongoing pids.
    ///
    /// No-op when the topic flag is off or the producer has no transactional id.
    pub(super) fn append_open_complete_aborts(&self, pids: impl IntoIterator<Item = (u64, u16)>) {
        if !self.transaction_state_topic {
            return;
        }
        let writes: Vec<(String, u64, u16)> = {
            let prods = self.producer_state.read();
            pids.into_iter()
                .filter_map(|(pid, epoch)| {
                    let prod = prods.get(&pid)?;
                    if prod.transactional_id.is_empty() {
                        return None;
                    }
                    Some((prod.transactional_id.clone(), pid, epoch))
                })
                .collect()
        };
        for (tid, pid, epoch) in writes {
            self.append_transaction_state(&tid, TXN_STATE_COMPLETE_ABORT, pid, epoch, 0);
        }
    }

    pub(super) fn persist_txn_markers(&self) {
        let path = self.txn_markers_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let mut file = TxnMarkersFile::default();
        {
            let open = self.open_txns.lock();
            for (&pid, txn) in open.iter() {
                for r in &txn.written {
                    file.open.push(StoredTxnRange {
                        producer_id: pid,
                        producer_epoch: Some(txn.producer_epoch),
                        topic: r.topic.clone(),
                        partition: r.partition,
                        first_offset: r.first_offset,
                        end_offset: r.end_offset,
                    });
                }
                // Phase 105: empty membership only — skip partitions that already
                // have write-through ranges (those are covered by `open`).
                for (topic, part) in &txn.added {
                    let has_written = txn
                        .written
                        .iter()
                        .any(|r| r.topic == *topic && r.partition == *part);
                    if has_written {
                        continue;
                    }
                    file.open_added.push(StoredAddedPartition {
                        producer_id: pid,
                        producer_epoch: Some(txn.producer_epoch),
                        topic: topic.clone(),
                        partition: *part,
                    });
                }
            }
        }
        {
            let aborted = self.aborted_txns.lock();
            for ((topic, part), list) in aborted.iter() {
                for m in list {
                    file.aborted.push(StoredTxnRange {
                        producer_id: m.producer_id,
                        producer_epoch: None,
                        topic: topic.clone(),
                        partition: *part,
                        first_offset: m.first_offset,
                        end_offset: m.end_offset,
                    });
                }
            }
        }
        let Ok(bytes) = serde_json::to_vec_pretty(&file) else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        if fs::write(&tmp, &bytes).is_ok() {
            let _ = fs::rename(tmp, path);
        }
    }

    /// Check idempotent produce sequence before appending.
    ///
    /// Non-idempotent produces (`producer_id == 0` or `base_sequence < 0`) always
    /// return [`IdempotentCheck::Accept`] without consulting producer state.
    ///
    /// Transactional producers without an open txn are rejected (`InvalidTxnState`).
    /// Callers should route open-txn produces through [`Self::buffer_txn_produce`].
    pub fn check_idempotent_produce(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        topic: &str,
        partition: u32,
        base_sequence: i32,
        message_count: u32,
    ) -> IdempotentCheck {
        if producer_id == 0 || base_sequence < 0 {
            return IdempotentCheck::Accept { base_offset: 0 };
        }
        if message_count == 0 {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidArg as u16,
            };
        }

        let state = self.producer_state.read();
        let Some(prod) = state.get(&producer_id) else {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::UnknownProducerId as u16,
            };
        };
        if prod.epoch != producer_epoch {
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidProducerEpoch as u16,
            };
        }
        if prod.transactional {
            // Transactional PIDs must produce only inside BeginTxn…EndTxn.
            return IdempotentCheck::Reject {
                error_code: ErrorCode::InvalidTxnState as u16,
            };
        }

        let key = (topic.to_owned(), partition);
        match prod.partitions.get(&key) {
            None => IdempotentCheck::Accept { base_offset: 0 },
            Some(last) => {
                if base_sequence == last.base_sequence && message_count == last.count {
                    IdempotentCheck::Duplicate {
                        base_offset: last.base_offset,
                        count: last.count,
                    }
                } else {
                    let expected = last.base_sequence.saturating_add(last.count as i32);
                    if base_sequence == expected {
                        IdempotentCheck::Accept { base_offset: 0 }
                    } else {
                        IdempotentCheck::Reject {
                            error_code: ErrorCode::OutOfOrderSequence as u16,
                        }
                    }
                }
            }
        }
    }

    /// Record a successful idempotent produce batch.
    pub fn record_idempotent_produce(
        &self,
        producer_id: u64,
        producer_epoch: u16,
        topic: &str,
        partition: u32,
        base_sequence: i32,
        count: u32,
        base_offset: u64,
    ) {
        if producer_id == 0 || base_sequence < 0 {
            return;
        }
        {
            let mut state = self.producer_state.write();
            let Some(prod) = state.get_mut(&producer_id) else {
                return;
            };
            if prod.epoch != producer_epoch {
                return;
            }
            prod.partitions.insert(
                (topic.to_owned(), partition),
                IdempotentBatchState {
                    base_sequence,
                    count,
                    base_offset,
                },
            );
        }
        let _ = self.persist_producer_state();
    }

    /// Persist current producer map to disk.
    pub(super) fn persist_producer_state(&self) -> Result<()> {
        let next_id = self.next_producer_id.load(Ordering::Relaxed);
        let state = self.producer_state.read();
        let mut file = ProducerStateFile {
            next_id,
            producers: HashMap::new(),
        };
        for (pid, prod) in state.iter() {
            let mut partitions = HashMap::new();
            for ((topic, part), batch) in &prod.partitions {
                partitions.insert(
                    partition_key(topic, *part),
                    StoredBatch {
                        base_sequence: batch.base_sequence,
                        count: batch.count,
                        base_offset: batch.base_offset,
                    },
                );
            }
            file.producers.insert(
                pid.to_string(),
                StoredProducer {
                    epoch: prod.epoch,
                    transactional_id: prod.transactional_id.clone(),
                    enable_2pc: prod.enable_2pc,
                    transaction_timeout_ms: prod.transaction_timeout_ms,
                    partitions,
                },
            );
        }
        self.producer_store.save(&file)
    }
}
