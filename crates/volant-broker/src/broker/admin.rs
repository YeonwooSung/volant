//! Cluster admin, 2PC participant handlers, fetch-session and auth accessors.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tracing::warn;
use volant_core::{Error, Result};
use volant_protocol::ErrorCode;

use super::*;
use super::{
    unix_now_ms, ClusterPreparedEntry, ClusterPreparedFile, OpenTxn, PreparedTxn,
    ProducerEpochState,
};

impl Broker {
    /// Overlay generation (`0` if no `membership.json` has been written).
    pub fn membership_generation(&self) -> u64 {
        self.cluster
            .as_ref()
            .map(|c| c.membership_generation.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Path to `{data_dir}/cluster/membership.json`.
    pub fn membership_overlay_path(&self) -> std::path::PathBuf {
        crate::cluster::membership_overlay_path(&self.storage.data_dir)
    }

    /// Configured brokers + live ids + overlay generation.
    pub fn list_membership(&self) -> MembershipSnapshot {
        match &self.cluster {
            None => MembershipSnapshot {
                generation: 0,
                brokers: vec![crate::cluster::BrokerEndpoint {
                    id: self.node_id,
                    host: self.advertised_host.read().clone(),
                    port: self.advertised_port.load(Ordering::Relaxed) as u16,
                    rack: None,
                }],
                live: vec![self.node_id],
            },
            Some(c) => MembershipSnapshot {
                generation: c.membership_generation.load(Ordering::Relaxed),
                brokers: c.config.read().brokers.clone(),
                live: c.membership.read().live_brokers(),
            },
        }
    }

    /// Add a broker endpoint. Persist overlay (generation+1). Not marked live
    /// until heartbeat. Rejects duplicate id.
    pub fn add_broker(
        &self,
        id: u32,
        host: String,
        port: u16,
        rack: Option<String>,
    ) -> Result<u64> {
        let cluster = self
            .cluster
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("add-broker requires cluster mode".into()))?;
        if host.trim().is_empty() || port == 0 {
            return Err(Error::InvalidArgument(
                "add-broker requires non-empty host and non-zero port".into(),
            ));
        }
        let mut cfg = cluster.config.write();
        if cfg.broker(id).is_some() {
            return Err(Error::InvalidArgument(format!("duplicate broker id {id}")));
        }
        let mut brokers = cfg.brokers.clone();
        brokers.push(crate::cluster::BrokerEndpoint {
            id,
            host,
            port,
            rack,
        });
        brokers.sort_by_key(|b| b.id);
        let generation = cluster
            .membership_generation
            .load(Ordering::Relaxed)
            .saturating_add(1);
        let overlay = crate::cluster::MembershipOverlay {
            generation,
            brokers: brokers.clone(),
        };
        crate::cluster::save_membership_overlay(&cluster.data_dir, &overlay)?;
        cfg.brokers = brokers;
        cluster
            .membership_generation
            .store(generation, Ordering::Relaxed);
        // Endpoint is configured immediately; live only after heartbeat.
        Ok(generation)
    }

    /// Remove a broker by id. Rejects self and the last remaining broker.
    pub fn remove_broker(&self, id: u32) -> Result<u64> {
        let cluster = self
            .cluster
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("remove-broker requires cluster mode".into()))?;
        let mut cfg = cluster.config.write();
        if cfg.brokers.len() <= 1 {
            return Err(Error::InvalidArgument(
                "cannot remove the last remaining broker".into(),
            ));
        }
        if id == self.node_id {
            return Err(Error::InvalidArgument(
                "cannot remove self from membership".into(),
            ));
        }
        if cfg.broker(id).is_none() {
            return Err(Error::InvalidArgument(format!(
                "broker id {id} is not in membership"
            )));
        }
        let brokers: Vec<_> = cfg.brokers.iter().filter(|b| b.id != id).cloned().collect();
        let generation = cluster
            .membership_generation
            .load(Ordering::Relaxed)
            .saturating_add(1);
        let overlay = crate::cluster::MembershipOverlay {
            generation,
            brokers: brokers.clone(),
        };
        crate::cluster::save_membership_overlay(&cluster.data_dir, &overlay)?;
        cfg.brokers = brokers;
        cluster
            .membership_generation
            .store(generation, Ordering::Relaxed);
        drop(cfg);
        cluster.membership.write().remove_id(id);
        Ok(generation)
    }

    /// Apply a peer `MembershipPut` overlay. Ignores stale generation
    /// (`incoming <= local`). New ids are not marked live.
    ///
    /// Returns the applied generation (local gen when ignored).
    pub fn apply_membership_put(
        &self,
        generation: u64,
        brokers: Vec<crate::cluster::BrokerEndpoint>,
    ) -> Result<u64> {
        let cluster = self
            .cluster
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("membership put requires cluster mode".into()))?;
        let local = cluster.membership_generation.load(Ordering::Relaxed);
        if generation <= local {
            return Ok(local);
        }
        let overlay = crate::cluster::MembershipOverlay {
            generation,
            brokers,
        };
        crate::cluster::validate_membership_overlay(&overlay)?;
        crate::cluster::save_membership_overlay(&cluster.data_dir, &overlay)?;
        let ids: Vec<u32> = overlay.brokers.iter().map(|b| b.id).collect();
        {
            let mut cfg = cluster.config.write();
            cfg.brokers = overlay.brokers;
        }
        cluster
            .membership_generation
            .store(generation, Ordering::Relaxed);
        cluster.membership.write().apply_configured_ids(&ids);
        Ok(generation)
    }

    /// Peers for membership overlay fan-out: configured brokers except self.
    pub fn membership_fanout_peers(&self) -> Vec<(u32, String)> {
        let Some(c) = &self.cluster else {
            return Vec::new();
        };
        let cfg = c.config.read();
        let mut out = Vec::new();
        for b in &cfg.brokers {
            if b.id == self.node_id {
                continue;
            }
            out.push((b.id, format!("{}:{}", b.host, b.port)));
        }
        out
    }

    /// Peers for BROKER config fan-out: live brokers except self (Phase 113 PR3).
    pub fn cluster_broker_config_fanout_peers(&self) -> Vec<(u32, String)> {
        let Some(c) = &self.cluster else {
            return Vec::new();
        };
        let live = c.membership.read().live_brokers();
        let mut out = Vec::new();
        for id in live {
            if id == self.node_id {
                continue;
            }
            if let Some(addr) = self.broker_addr(id) {
                out.push((id, addr));
            }
        }
        out
    }

    /// Increment BROKER config push error counter (Phase 113).
    pub fn note_cluster_config_push_error(&self) {
        self.cluster_config_push_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // --- Phase 114 multi-broker 2PC ---

    /// Multi-broker 2PC fan-out error counter (Phase 114).
    pub fn txn_2pc_fanout_errors_total(&self) -> u64 {
        self.txn_2pc_fanout_errors_total.load(Ordering::Relaxed)
    }

    /// Increment multi-broker 2PC fan-out error counter (Phase 114).
    pub fn note_txn_2pc_fanout_error(&self) {
        self.txn_2pc_fanout_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Controller cluster prepared index size (Phase 114 gauge).
    pub fn cluster_prepared_txn_count(&self) -> usize {
        self.cluster_prepared_index.lock().len()
    }

    /// Live peers for multi-broker 2PC fan-out (all live except self).
    pub fn txn_2pc_fanout_peers(&self) -> Vec<(u32, String)> {
        self.cluster_broker_config_fanout_peers()
    }

    /// Whether multi-broker 2PC fan-out applies (cluster mode).
    pub fn txn_2pc_cluster_enabled(&self) -> bool {
        self.cluster.is_some()
    }

    /// Build an open fan-out payload for a producer that just began/ensured open.
    pub fn txn_2pc_open_fanout(&self, producer_id: u64) -> Txn2pcFanout {
        if self.cluster.is_none() {
            return Txn2pcFanout::None;
        }
        let state = self.producer_state.read();
        let Some(prod) = state.get(&producer_id) else {
            return Txn2pcFanout::None;
        };
        if !prod.transactional || prod.transactional_id.is_empty() {
            return Txn2pcFanout::None;
        }
        // Self is the Init/open coordinator for this fan-out.
        self.note_txn_coordinator(&prod.transactional_id, producer_id, self.node_id);
        Txn2pcFanout::Open {
            transactional_id: prod.transactional_id.clone(),
            producer_id,
            producer_epoch: prod.epoch,
            enable_2pc: prod.enable_2pc,
            coordinator_node_id: self.node_id,
            install_open: true,
        }
    }

    /// Phase 120/124: register txn coordinator (Init owner) for forward resolution.
    ///
    /// Persists under `{data_dir}/__txn_coordinator` when the registry is durable.
    pub fn note_txn_coordinator(
        &self,
        transactional_id: &str,
        producer_id: u64,
        coordinator_node_id: u32,
    ) {
        self.txn_coordinator_registry
            .note(transactional_id, producer_id, coordinator_node_id);
    }

    /// Phase 124: durable txn coordinator registry (Init-owner map).
    pub fn txn_coordinator_registry(&self) -> &TxnCoordinatorRegistry {
        &self.txn_coordinator_registry
    }

    /// Phase 124: entries restored from disk at last open.
    pub fn txn_coordinator_registry_restored(&self) -> u64 {
        self.txn_coordinator_registry.restored()
    }

    /// Phase 124: durable registry persist failures.
    pub fn txn_coordinator_registry_persist_errors_total(&self) -> u64 {
        self.txn_coordinator_registry.persist_errors_total()
    }

    /// Phase 120: resolve txn coordinator node id for EndTxn forward.
    ///
    /// Lookup order: transactional_id map → cluster prepared index
    /// `coordinator_node_id` → producer_id map (durable registry, Phase 124).
    pub fn resolve_txn_coordinator(
        &self,
        transactional_id: &str,
        producer_id: Option<u64>,
    ) -> Option<u32> {
        if !transactional_id.is_empty() {
            if let Some(id) = self
                .txn_coordinator_registry
                .resolve_by_id(transactional_id)
            {
                return Some(id);
            }
            if let Some(entry) = self.cluster_prepared_index.lock().get(transactional_id) {
                if entry.coordinator_node_id != 0 {
                    return Some(entry.coordinator_node_id);
                }
            }
        }
        if let Some(pid) = producer_id {
            if let Some(id) = self.txn_coordinator_registry.resolve_by_pid(pid) {
                return Some(id);
            }
        }
        None
    }

    /// Phase 121: resolve FindCoordinator endpoint for a group or transactional key.
    ///
    /// Lookup order:
    /// 1. Single-node / no cluster → this broker's advertised address.
    /// 2. Transaction key with known Init owner (Phase 120 registry) → that owner.
    /// 3. Sticky murmur2 over sorted **configured** broker ids; skip dead members
    ///    by walking the static ring to the next live broker.
    ///
    /// `key_type`: `0` = group, `1` = transaction (same as Kafka wire).
    pub fn resolve_find_coordinator(&self, key: &str, key_type: i8) -> (u32, String, u16) {
        let host = self.advertised_host.read().clone();
        let port = self.advertised_port.load(Ordering::Relaxed) as u16;
        let Some(cluster) = &self.cluster else {
            return (self.node_id, host, port);
        };

        // Known transactional_id → Init-owner registry overrides sticky hash.
        if key_type == 1 && !key.is_empty() {
            if let Some(owner) = self.resolve_txn_coordinator(key, None) {
                if let Some(ep) = self.coordinator_endpoint(owner) {
                    return ep;
                }
            }
        }

        let ring = cluster.config.read().broker_ids();
        let live = cluster.membership.read().live_brokers();
        let chosen = sticky_coordinator_id(key.as_bytes(), &ring, &live).unwrap_or(self.node_id);
        self.coordinator_endpoint(chosen)
            .unwrap_or((self.node_id, host, port))
    }

    /// Host/port for a coordinator node id (self uses advertised).
    pub(super) fn coordinator_endpoint(&self, node_id: u32) -> Option<(u32, String, u16)> {
        if node_id == self.node_id {
            let host = self.advertised_host.read().clone();
            let port = self.advertised_port.load(Ordering::Relaxed) as u16;
            return Some((node_id, host, port));
        }
        let cluster = self.cluster.as_ref()?;
        let b = cluster.config.read().broker(node_id)?.clone();
        Some((b.id, b.host, b.port))
    }

    /// Phase 120: Init registration fan-out (producer + coordinator, no open).
    pub fn txn_2pc_init_register_fanout(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: u16,
        enable_2pc: bool,
    ) -> Txn2pcFanout {
        if self.cluster.is_none() || transactional_id.is_empty() {
            return Txn2pcFanout::None;
        }
        self.note_txn_coordinator(transactional_id, producer_id, self.node_id);
        Txn2pcFanout::Open {
            transactional_id: transactional_id.to_owned(),
            producer_id,
            producer_epoch,
            enable_2pc,
            coordinator_node_id: self.node_id,
            install_open: false,
        }
    }

    /// Successful transparent txn forwards (Phase 120).
    pub fn txn_forward_total(&self) -> u64 {
        self.txn_forward_total.load(Ordering::Relaxed)
    }

    /// Failed transparent txn forward attempts (Phase 120).
    pub fn txn_forward_errors_total(&self) -> u64 {
        self.txn_forward_errors_total.load(Ordering::Relaxed)
    }

    /// Record a successful multi-broker txn forward (Phase 120).
    pub fn record_txn_forward_ok(&self) {
        self.txn_forward_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed multi-broker txn forward (Phase 120).
    pub fn record_txn_forward_error(&self) {
        self.txn_forward_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Apply inter-broker `TxnParticipantOpen` (Phase 114 + Phase 120).
    ///
    /// Installs producer state and optionally empty open txn so remote partition
    /// leaders can accept write-through produce. Idempotent for matching pid/epoch.
    /// Registers txn coordinator for EndTxn forward (Phase 120).
    pub fn handle_txn_participant_open(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: u16,
        enable_2pc: bool,
        coordinator_node_id: u32,
        install_open: bool,
    ) -> u16 {
        {
            let mut state = self.producer_state.write();
            if let Some(prod) = state.get_mut(&producer_id) {
                if prod.epoch > producer_epoch {
                    return ErrorCode::InvalidProducerEpoch as u16;
                }
                // Accept equal or newer epoch from coordinator fan-out.
                prod.epoch = producer_epoch;
                prod.transactional = true;
                prod.transactional_id = transactional_id.to_owned();
                if enable_2pc {
                    prod.enable_2pc = true;
                }
            } else {
                state.insert(
                    producer_id,
                    ProducerEpochState {
                        epoch: producer_epoch,
                        transactional: true,
                        transactional_id: transactional_id.to_owned(),
                        enable_2pc,
                        transaction_timeout_ms: 0,
                        partitions: HashMap::new(),
                    },
                );
            }
        }
        if !transactional_id.is_empty() {
            self.transactional_ids
                .write()
                .insert(transactional_id.to_owned(), producer_id);
        }
        // Phase 120: learn Init owner for transparent EndTxn forward.
        let coord = if coordinator_node_id != 0 {
            coordinator_node_id
        } else {
            // Legacy peers: treat the sender as unknown; keep prior mapping if any.
            0
        };
        if coord != 0 {
            self.note_txn_coordinator(transactional_id, producer_id, coord);
        }
        // Ensure open txn exists (empty) for write-through on this leader.
        if install_open {
            let already_prepared = !transactional_id.is_empty()
                && self.prepared_txns.lock().contains_key(transactional_id);
            {
                let mut open = self.open_txns.lock();
                if let Some(txn) = open.get_mut(&producer_id) {
                    // Already open — keep existing written ranges; refresh epoch.
                    txn.producer_epoch = producer_epoch;
                } else if !already_prepared {
                    open.insert(
                        producer_id,
                        OpenTxn {
                            opened_at_ms: unix_now_ms(),
                            producer_epoch,
                            ..OpenTxn::default()
                        },
                    );
                }
            }
        }
        let _ = self.persist_producer_state();
        0
    }

    /// Apply inter-broker `TxnParticipantPrepare` (Phase 114).
    ///
    /// Moves local open ranges for this pid into prepared (or no-ops if none).
    /// Controller also upserts the cluster prepared index.
    pub fn handle_txn_participant_prepare(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: u16,
        commit: bool,
    ) -> u16 {
        if transactional_id.is_empty() {
            return ErrorCode::InvalidArg as u16;
        }
        // Already prepared with matching decision → idempotent OK.
        {
            let prepared = self.prepared_txns.lock();
            if let Some(prep) = prepared.get(transactional_id) {
                if prep.producer_id != producer_id {
                    return ErrorCode::InvalidTxnState as u16;
                }
                if prep.producer_epoch != producer_epoch {
                    return ErrorCode::InvalidProducerEpoch as u16;
                }
                if prep.commit != commit {
                    return ErrorCode::InvalidTxnState as u16;
                }
                // Still ensure cluster index if we are controller.
                drop(prepared);
                self.upsert_cluster_prepared_index(
                    transactional_id,
                    producer_id,
                    producer_epoch,
                    commit,
                );
                return 0;
            }
        }

        let txn = {
            let mut open = self.open_txns.lock();
            open.remove(&producer_id)
        };
        if let Some(txn) = txn {
            if txn.producer_epoch != 0 && txn.producer_epoch != producer_epoch {
                // Epoch mismatch on open body — put back and reject.
                self.open_txns.lock().insert(producer_id, txn);
                return ErrorCode::InvalidProducerEpoch as u16;
            }
            // Validate producer epoch if known.
            {
                let state = self.producer_state.read();
                if let Some(prod) = state.get(&producer_id) {
                    if prod.epoch != producer_epoch {
                        // Put open back.
                        self.open_txns.lock().insert(producer_id, txn);
                        return ErrorCode::InvalidProducerEpoch as u16;
                    }
                }
            }
            let prep = PreparedTxn {
                transactional_id: transactional_id.to_owned(),
                producer_id,
                producer_epoch,
                commit,
                prepared_at_ms: unix_now_ms(),
                open: txn,
            };
            self.prepared_txns
                .lock()
                .insert(transactional_id.to_owned(), prep);
            self.persist_txn_markers();
            self.persist_prepared_txns();
        } else {
            // No local open ranges — still OK (empty participant). Ensure
            // producer is known for complete/fence later when needed.
            let state = self.producer_state.read();
            if let Some(prod) = state.get(&producer_id) {
                if prod.epoch != producer_epoch {
                    return ErrorCode::InvalidProducerEpoch as u16;
                }
            }
        }
        self.upsert_cluster_prepared_index(transactional_id, producer_id, producer_epoch, commit);
        0
    }

    /// Apply inter-broker `TxnParticipantComplete` (Phase 114).
    ///
    /// Finalizes local prepared (or open fallback) for this txn and clears the
    /// controller cluster index entry when present.
    ///
    /// **Fence note:** `commit=false` force-aborts prepared even when the
    /// prepared decision was PrepareCommit (InitProducerId KeepPreparedTxn=false
    /// cluster fan-out). Client EndTxn decision mismatch is rejected **locally**
    /// before fan-out, so peers only see matching completes or fence aborts.
    pub fn handle_txn_participant_complete(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: u16,
        commit: bool,
    ) -> u16 {
        if !transactional_id.is_empty() {
            let prep = {
                let mut prepared = self.prepared_txns.lock();
                prepared.remove(transactional_id)
            };
            if let Some(prep) = prep {
                if prep.producer_id != producer_id {
                    // Put back — wrong identity.
                    self.prepared_txns
                        .lock()
                        .insert(transactional_id.to_owned(), prep);
                    return ErrorCode::InvalidTxnState as u16;
                }
                if prep.producer_epoch != producer_epoch {
                    self.prepared_txns
                        .lock()
                        .insert(transactional_id.to_owned(), prep);
                    return ErrorCode::InvalidProducerEpoch as u16;
                }
                if prep.commit != commit {
                    if commit {
                        // Commit complete against PrepareAbort — reject.
                        self.prepared_txns
                            .lock()
                            .insert(transactional_id.to_owned(), prep);
                        return ErrorCode::InvalidTxnState as u16;
                    }
                    // commit=false with PrepareCommit → force-abort (fence).
                    self.force_abort_prepared(prep);
                    self.clear_cluster_prepared_index(transactional_id);
                    return 0;
                }
                let _ = self.finalize_txn(producer_id, producer_epoch, commit, prep.open, &[]);
                self.persist_prepared_txns();
                self.clear_cluster_prepared_index(transactional_id);
                return 0;
            }
        }
        // Fallback: open (non-prepared) ranges — fence abort path may hit peers
        // that never prepared.
        let txn = {
            let mut open = self.open_txns.lock();
            open.remove(&producer_id)
        };
        if let Some(txn) = txn {
            let _ = self.finalize_txn(producer_id, producer_epoch, commit, txn, &[]);
        }
        self.clear_cluster_prepared_index(transactional_id);
        0
    }

    pub(super) fn cluster_prepared_index_path(&self) -> PathBuf {
        self.storage
            .data_dir
            .join("__txn_prepared")
            .join("cluster.json")
    }

    pub(super) fn load_cluster_prepared_index(&self) {
        // Only meaningful on controller, but load if file exists (restart race).
        let path = self.cluster_prepared_index_path();
        let Ok(bytes) = fs::read(&path) else {
            return;
        };
        let Ok(file) = serde_json::from_slice::<ClusterPreparedFile>(&bytes) else {
            return;
        };
        let mut map = self.cluster_prepared_index.lock();
        for e in file.prepared {
            map.insert(e.transactional_id.clone(), e);
        }
    }

    pub(super) fn persist_cluster_prepared_index(&self) {
        // Controllers own the durable index; non-controllers may hold a soft copy.
        let path = self.cluster_prepared_index_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let mut file = ClusterPreparedFile::default();
        {
            let map = self.cluster_prepared_index.lock();
            file.prepared = map.values().cloned().collect();
        }
        let Ok(bytes) = serde_json::to_vec_pretty(&file) else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        if fs::write(&tmp, &bytes).is_ok() {
            let _ = fs::rename(tmp, path);
        }
    }

    pub(super) fn upsert_cluster_prepared_index(
        &self,
        transactional_id: &str,
        producer_id: u64,
        producer_epoch: u16,
        commit: bool,
    ) {
        // Persist on controller only (SoT). Peers skip durable cluster index.
        if self.cluster.is_some() && !self.is_controller() {
            return;
        }
        let entry = ClusterPreparedEntry {
            transactional_id: transactional_id.to_owned(),
            producer_id,
            producer_epoch,
            commit,
            prepared_at_ms: unix_now_ms(),
            coordinator_node_id: self.node_id,
        };
        self.cluster_prepared_index
            .lock()
            .insert(transactional_id.to_owned(), entry);
        self.persist_cluster_prepared_index();
    }

    pub(super) fn clear_cluster_prepared_index(&self, transactional_id: &str) {
        if transactional_id.is_empty() {
            return;
        }
        let removed = self.cluster_prepared_index.lock().remove(transactional_id);
        if removed.is_some() {
            self.persist_cluster_prepared_index();
        } else if self.cluster.is_some() && self.is_controller() {
            // Still rewrite so a stale file cannot resurrect the entry after
            // peers completed while controller had no local entry.
            self.persist_cluster_prepared_index();
        }
    }

    /// Roll back a just-local prepare if cluster fan-out failed (Phase 114).
    ///
    /// Moves prepared back to open when possible so the client can retry EndTxn.
    pub fn rollback_local_prepare(&self, transactional_id: &str) {
        let prep = {
            let mut prepared = self.prepared_txns.lock();
            prepared.remove(transactional_id)
        };
        if let Some(prep) = prep {
            let pid = prep.producer_id;
            let mut open = self.open_txns.lock();
            open.insert(pid, prep.open);
            drop(open);
            self.persist_prepared_txns();
            self.persist_txn_markers();
            self.clear_cluster_prepared_index(transactional_id);
        }
    }

    /// Apply inter-broker `ClusterBrokerConfig` (Phase 113 PR3).
    ///
    /// Ignores stale/equal generations (`generation <= applied`). On accept:
    /// apply knobs + sparse durable merge, then record `applied_config_generation`.
    /// Returns `(error_code, applied_generation)`.
    pub fn handle_cluster_broker_config(
        &self,
        generation: u64,
        entries: &[(String, String)],
    ) -> (u16, u64) {
        let applied = self.applied_config_generation.load(Ordering::SeqCst);
        if generation <= applied {
            return (0, applied);
        }
        if let Err(e) = self.apply_and_persist_broker_configs(entries) {
            warn!(
                generation,
                error = %e,
                "cluster broker config apply failed"
            );
            return (ErrorCode::InvalidArg as u16, applied);
        }
        self.applied_config_generation
            .store(generation, Ordering::SeqCst);
        // Mirror SoT gen so a later promote can re-push at the correct generation.
        let cur = self.config_generation.load(Ordering::SeqCst);
        if generation > cur {
            self.config_generation.store(generation, Ordering::SeqCst);
        }
        self.persist_cluster_admin_gens();
        (0, generation)
    }

    /// Peers for ACL snapshot fan-out: live brokers except self (Phase 113 PR4).
    pub fn cluster_acl_fanout_peers(&self) -> Vec<(u32, String)> {
        // Same membership set as BROKER config fan-out.
        self.cluster_broker_config_fanout_peers()
    }

    /// Increment ACL snapshot push error counter (Phase 113).
    pub fn note_cluster_acl_push_error(&self) {
        self.cluster_acl_push_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Apply inter-broker `ClusterAclSnapshot` (Phase 113 PR4).
    ///
    /// Ignores stale/equal generations. On accept: install snapshot + persist
    /// local `__acls`, then record `applied_acl_generation`.
    pub fn handle_cluster_acl_snapshot(&self, generation: u64, snapshot: &[u8]) -> (u16, u64) {
        let applied = self.applied_acl_generation.load(Ordering::SeqCst);
        if generation <= applied {
            return (0, applied);
        }
        let snap = match crate::acl::AclState::decode_snapshot_bytes(snapshot) {
            Ok(s) => s,
            Err(e) => {
                warn!(generation, error = %e, "cluster acl snapshot decode failed");
                return (ErrorCode::InvalidArg as u16, applied);
            }
        };
        if let Err(e) = self.acls.install_snapshot(&snap) {
            warn!(generation, error = %e, "cluster acl snapshot install failed");
            return (ErrorCode::Storage as u16, applied);
        }
        self.applied_acl_generation
            .store(generation, Ordering::SeqCst);
        let cur = self.acl_generation.load(Ordering::SeqCst);
        if generation > cur {
            self.acl_generation.store(generation, Ordering::SeqCst);
        }
        self.persist_cluster_admin_gens();
        (0, generation)
    }

    /// Create ACL entries with cluster controller gate (Phase 113 PR4).
    ///
    /// Returns `Some(generation)` for fan-out when running in cluster mode.
    pub fn create_acls_admin(&self, entries: Vec<crate::acl::AclEntry>) -> Result<Option<u64>> {
        if self.cluster.is_some() && !self.is_controller() {
            return Err(Error::InvalidArgument("not controller".into()));
        }
        self.acls.create(entries)?;
        Ok(self.bump_acl_generation_if_cluster())
    }

    /// Delete ACL entries with cluster controller gate (Phase 113 PR4).
    ///
    /// Returns `(removed_count, optional generation for fan-out)`.
    pub fn delete_acls_admin(
        &self,
        entries: &[crate::acl::AclEntry],
    ) -> Result<(usize, Option<u64>)> {
        if self.cluster.is_some() && !self.is_controller() {
            return Err(Error::InvalidArgument("not controller".into()));
        }
        let n = self.acls.delete(entries)?;
        // Only bump generation when something changed (or always for consistency
        // of "mutate happened"? Always bump so empty delete still is controller-
        // only with no-op fan-out of same snapshot — skip bump when n==0).
        let gen = if n > 0 {
            self.bump_acl_generation_if_cluster()
        } else {
            None
        };
        Ok((n, gen))
    }

    pub(super) fn bump_acl_generation_if_cluster(&self) -> Option<u64> {
        if self.cluster.is_none() {
            return None;
        }
        let gen = self.acl_generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.applied_acl_generation.store(gen, Ordering::SeqCst);
        self.persist_cluster_admin_gens();
        Some(gen)
    }

    /// JSON snapshot bytes for inter-broker ACL push (Phase 113).
    pub fn acl_snapshot_wire_bytes(&self) -> Result<bytes::Bytes> {
        let v = self.acls.encode_snapshot_bytes()?;
        Ok(bytes::Bytes::from(v))
    }

    /// Current fetch-session idle TTL in milliseconds (Phase 95). `0` disables.
    pub fn fetch_session_idle_ms(&self) -> u64 {
        self.fetch_sessions.idle_timeout_ms()
    }

    /// Override fetch-session idle TTL (Phase 95). `0` disables idle eviction.
    pub fn set_fetch_session_idle_ms(&self, ms: u64) {
        self.fetch_sessions.set_idle_timeout_ms(ms);
    }

    /// Current max concurrent fetch sessions (Phase 95). `0` = unlimited.
    pub fn fetch_session_max(&self) -> usize {
        self.fetch_sessions.max_sessions()
    }

    /// Override max concurrent fetch sessions (Phase 95). `0` = unlimited.
    pub fn set_fetch_session_max(&self, max: usize) {
        self.fetch_sessions.set_max_sessions(max);
    }

    /// Shared metrics registry.
    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// Configure shared-token auth. `None` disables the auth gate.
    pub fn set_auth_token(&self, token: Option<String>) {
        *self.auth_token.write() = token;
    }

    /// Current auth token if configured.
    pub fn auth_token(&self) -> Option<String> {
        self.auth_token.read().clone()
    }

    /// Phase 20 ACL state.
    pub fn acls(&self) -> &crate::acl::AclState {
        &self.acls
    }

    /// Configure ACL enforcement (server startup).
    pub fn configure_acls(
        &self,
        enable: bool,
        file: Option<&std::path::Path>,
        super_users: Vec<String>,
        auth_principal: String,
    ) -> Result<()> {
        self.acls
            .configure(enable, file, super_users, auth_principal)
    }

    /// Principal name applied after successful shared-token Auth.
    pub fn auth_principal_name(&self) -> String {
        self.acls.auth_principal()
    }

    /// SCRAM-SHA-256 user store (Phase 22).
    pub fn scram(&self) -> &crate::scram::ScramStore {
        &self.scram
    }

    /// Whether connections must authenticate (token, SCRAM users, or caller mTLS).
    ///
    /// Callers with mTLS should OR this with their mTLS-enabled flag.
    pub fn auth_required(&self) -> bool {
        self.auth_token().is_some() || self.scram.has_users()
    }

    /// Upsert a SCRAM user at startup (`--scram-user user:pass`).
    pub fn upsert_scram_user(&self, username: &str, password: &str) -> Result<()> {
        self.scram.upsert_user(username, password, 0)
    }

    /// Configure metrics HTTP shared token (Phase 21). `None` = open scrape.
    pub fn set_metrics_token(&self, token: Option<String>) {
        *self.metrics_token.write() = token;
    }

    /// Current metrics token if configured.
    pub fn metrics_token(&self) -> Option<String> {
        self.metrics_token.read().clone()
    }

    /// Configure inter-broker TLS. `None` keeps inter-broker plaintext.
    pub fn set_inter_broker_tls(&self, config: Option<InterBrokerTls>) {
        *self.inter_broker_tls.write() = config;
    }

    /// Current inter-broker TLS settings, if enabled.
    pub fn inter_broker_tls(&self) -> Option<InterBrokerTls> {
        self.inter_broker_tls.read().clone()
    }
}
