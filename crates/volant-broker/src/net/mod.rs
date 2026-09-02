//! Framed TCP server for the Volant broker.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, info_span, warn, Instrument};
use volant_core::{Error, Result};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{pack_response, Response};

use crate::broker::Broker;
use crate::replica::run_follower_loops;

/// Default max wait when joining background tasks after stop is signaled.
const BG_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounded wait when aborting in-flight accept-loop connection tasks (Phase 109).
const CONN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Handle for broker background tasks (group expiry, retention, sweeper, cluster).
///
/// Returned by [`start_background_tasks`]. Call [`BackgroundTasks::shutdown`] to
/// signal stop and join; dropping signals stop and aborts remaining tasks.
///
/// A no-op handle (from a second [`start_background_tasks`] call on the same
/// broker) has an empty task set; its shutdown/abort/Drop are safe no-ops and
/// do **not** stop the already-running first-flight tasks.
#[must_use = "call BackgroundTasks::shutdown to stop and join background tasks"]
pub struct BackgroundTasks {
    stop_tx: watch::Sender<bool>,
    handles: Vec<JoinHandle<()>>,
}

impl BackgroundTasks {
    /// Signal stop and await all background tasks (bounded by a short timeout).
    ///
    /// Loops observe the stop flag via `tokio::select!`, so joins normally
    /// complete well under the timeout. On timeout, remaining tasks are aborted.
    ///
    /// No-op when this handle owns no tasks (duplicate [`start_background_tasks`]).
    pub async fn shutdown(mut self) {
        let _ = self.stop_tx.send(true);
        let handles = std::mem::take(&mut self.handles);
        if handles.is_empty() {
            return;
        }
        let aborts: Vec<_> = handles.iter().map(|h| h.abort_handle()).collect();
        let join_all = async {
            for h in handles {
                let _ = h.await;
            }
        };
        if tokio::time::timeout(BG_SHUTDOWN_TIMEOUT, join_all)
            .await
            .is_err()
        {
            warn!(
                timeout_ms = BG_SHUTDOWN_TIMEOUT.as_millis() as u64,
                "background task shutdown timed out; aborting remaining tasks"
            );
            for a in aborts {
                a.abort();
            }
        }
    }

    /// Signal stop and abort all tasks without awaiting (tests / best-effort drop).
    ///
    /// No-op when this handle owns no tasks (duplicate [`start_background_tasks`]).
    pub fn abort(mut self) {
        let _ = self.stop_tx.send(true);
        for h in std::mem::take(&mut self.handles) {
            h.abort();
        }
    }
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
        for h in self.handles.drain(..) {
            h.abort();
        }
    }
}

/// Bind and serve until the accept loop fails fatally or a shutdown signal arrives.
pub async fn run_server(addr: SocketAddr, broker: Arc<Broker>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    broker.set_advertised(local.ip().to_string(), local.port());
    info!(%local, "volant broker listening");
    serve_listener(listener, broker).await
}

/// Accept loop over an already-bound listener (useful for port-0 e2e tests).
///
/// Starts background tasks (single-flight) and joins them when the accept loop
/// exits or a process shutdown signal (`ctrl_c` / SIGTERM on Unix) is received.
/// In-flight connection tasks are aborted with a bounded drain timeout (Phase 109).
pub async fn serve_listener(listener: TcpListener, broker: Arc<Broker>) -> Result<()> {
    serve_listener_until(listener, broker, shutdown_signal()).await
}

/// Like [`serve_listener`], but stops when `shutdown` completes (Phase 109).
///
/// Useful for tests and coordinated multi-listener shutdown without relying on
/// process signals.
pub async fn serve_listener_until<F>(
    listener: TcpListener,
    broker: Arc<Broker>,
    shutdown: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()>,
{
    if let Ok(local) = listener.local_addr() {
        broker.set_advertised(local.ip().to_string(), local.port());
        info!(%local, "volant broker accept loop started");
    }

    let bg = start_background_tasks(Arc::clone(&broker));
    let result = accept_loop(listener, Arc::clone(&broker), shutdown).await;
    bg.shutdown().await;
    result
}

async fn accept_loop<F>(listener: TcpListener, broker: Arc<Broker>, shutdown: F) -> Result<()>
where
    F: std::future::Future<Output = ()>,
{
    tokio::pin!(shutdown);
    let mut conns: Vec<JoinHandle<()>> = Vec::new();
    loop {
        conns.retain(|h| !h.is_finished());
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received; draining native accept loop");
                break;
            }
            acc = listener.accept() => {
                match acc {
                    Ok((stream, peer)) => {
                        broker.metrics().record_connection();
                        debug!(%peer, "accepted connection");
                        let b = Arc::clone(&broker);
                        conns.push(tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, b).await {
                                debug!(%peer, error = %e, "connection closed");
                            }
                        }));
                    }
                    Err(e) => {
                        error!(error = %e, "accept failed");
                        drain_connection_tasks(conns).await;
                        return Err(Error::Io(e));
                    }
                }
            }
        }
    }
    drain_connection_tasks(conns).await;
    Ok(())
}

/// Abort in-flight connection tasks and await them (bounded).
async fn drain_connection_tasks(handles: Vec<JoinHandle<()>>) {
    if handles.is_empty() {
        return;
    }
    for h in &handles {
        h.abort();
    }
    let join_all = async {
        for h in handles {
            let _ = h.await;
        }
    };
    if tokio::time::timeout(CONN_DRAIN_TIMEOUT, join_all)
        .await
        .is_err()
    {
        warn!(
            timeout_ms = CONN_DRAIN_TIMEOUT.as_millis() as u64,
            "connection drain timed out"
        );
    }
}

/// Process-level stop: Ctrl-C, and on Unix also SIGTERM.
///
/// Public so Kafka/metrics accept loops and the TLS server path can share the
/// same signal set (Phase 109).
pub async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(_) => {
                    let _ = ctrl_c.await;
                    return;
                }
            };
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}

mod dispatch;
mod fanout;
mod metrics_http;
mod rpc;

pub use dispatch::{dispatch_request, dispatch_request_as, dispatch_with_auth};
pub use fanout::{
    catch_up_peer_admin_state, catch_up_peer_truncate_journal, complete_assignment_mutation,
    drain_delete_records_outbox, fanout_assignment_consensus, fanout_cluster_acl_snapshot,
    fanout_cluster_broker_config, fanout_delete_records, fanout_delete_records_replicas_only,
    fanout_isr_update_reports, fanout_membership_put, fanout_metadata_raft_append,
    fanout_session_mirror_ops, fanout_truncate_journal_note,
    fanout_truncate_journal_note_provisional, fanout_truncate_journal_push,
    fanout_txn_participant_complete, fanout_txn_participant_open, fanout_txn_participant_prepare,
    maybe_fanout_assignment_consensus, maybe_forward_kafka_end_txn, maybe_forward_kafka_fetch,
    maybe_forward_kafka_txn, peek_add_offsets_to_txn_ids, peek_end_txn_ids,
    peek_txn_offset_commit_ids, run_txn_2pc_fanout, schedule_catch_up_peer_admin_state,
    schedule_catch_up_peer_truncate_journal, schedule_isr_update_reports,
    schedule_session_mirror_fanout, snapshot_if_must_wait, DeleteRecordsFanoutResult,
};
pub use metrics_http::{render_metrics, run_metrics_server, run_metrics_server_until};
pub use rpc::{
    delete_records_fanout_budget, inter_broker_rpc, inter_broker_rpc_timeout,
    DEFAULT_DELETE_RECORDS_FANOUT_BUDGET_MS, DEFAULT_INTER_BROKER_RPC_TIMEOUT_MS,
    MAX_INTER_BROKER_TIMEOUT_MS, MIN_INTER_BROKER_TIMEOUT_MS,
};

use fanout::heartbeat_mesh;

/// Start group expiry, retention, txn/session sweep, cluster heartbeat, and
/// follower replication tasks.
///
/// # Single-flight (Phase 109)
///
/// Only the **first** call per [`Broker`] spawns tasks. Subsequent calls return
/// a no-op [`BackgroundTasks`] whose [`BackgroundTasks::shutdown`] /
/// [`BackgroundTasks::abort`] / `Drop` do nothing to the already-running set.
/// Holders of the first handle remain responsible for shutdown.
///
/// Returns a [`BackgroundTasks`] handle. Call [`BackgroundTasks::shutdown`] to
/// signal stop and join (Phase 106). Phase 101 always-spawn + 0-pause for the
/// sweeper is preserved.
pub fn start_background_tasks(broker: Arc<Broker>) -> BackgroundTasks {
    if !broker.claim_background_tasks() {
        // No-op handle: empty task set. stop channel already true so Drop is quiet.
        let (stop_tx, _) = watch::channel(true);
        return BackgroundTasks {
            stop_tx,
            handles: Vec::new(),
        };
    }

    let (stop_tx, _) = watch::channel(false);
    let mut handles = Vec::new();

    // Periodic session expiry for consumer groups.
    {
        let b = Arc::clone(&broker);
        let mut stop_rx = stop_tx.subscribe();
        handles.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => break,
                    _ = interval.tick() => {
                        b.groups()
                            .expire_sessions(|topic| b.partition_count_opt(topic));
                    }
                }
            }
        }));
    }

    // Periodic retention (Phase 13).
    {
        let b = Arc::clone(&broker);
        let mut stop_rx = stop_tx.subscribe();
        handles.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => break,
                    _ = interval.tick() => {
                        if let Err(e) = b.apply_retention_all() {
                            debug!(error = %e, "apply_retention_all failed");
                        }
                    }
                }
            }
        }));
    }

    // Phase 97 + 101: open/prepared txn timeout + idle fetch-session sweep.
    // Always spawn so 0→>0 (AlterConfigs / setter) enables without restart.
    // Interval 0 pauses work (200ms poll); >0 sleeps then sweep_timeouts.
    {
        let b = Arc::clone(&broker);
        let mut stop_rx = stop_tx.subscribe();
        handles.push(tokio::spawn(async move {
            loop {
                let ms = b.sweep_interval_ms();
                if ms == 0 {
                    // Paused: poll occasionally so a later enable is observed
                    // without spinning (Phase 101: works from boot with 0 too).
                    tokio::select! {
                        _ = stop_rx.changed() => break,
                        _ = tokio::time::sleep(Duration::from_millis(200)) => continue,
                    }
                }
                tokio::select! {
                    _ = stop_rx.changed() => break,
                    _ = tokio::time::sleep(Duration::from_millis(ms)) => {
                        // Re-check after sleep in case interval was set to 0 mid-wait.
                        if b.sweep_interval_ms() == 0 {
                            continue;
                        }
                        let (open_n, prep_n, idle_n) = b.sweep_timeouts();
                        if open_n > 0 || prep_n > 0 || idle_n > 0 {
                            debug!(
                                open_aborted = open_n,
                                prepared_aborted = prep_n,
                                sessions_idle_evicted = idle_n,
                                "background timeout sweep"
                            );
                        }
                        // v0.30: periodic best-effort MirrorPut of foreign mirrors
                        // so peers can self-converge without a client Fetch.
                        if b.fetch_sessions().queue_foreign_mirror_puts() > 0 {
                            schedule_session_mirror_fanout(&b);
                        }
                    }
                }
            }
        }));
    }

    if broker.cluster_config().is_some() {
        // Membership tick + controller expiry.
        {
            let b = Arc::clone(&broker);
            let mut stop_rx = stop_tx.subscribe();
            handles.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(500));
                loop {
                    tokio::select! {
                        _ = stop_rx.changed() => break,
                        _ = interval.tick() => {
                            b.tick_cluster();
                            // Phase 142: death-path ISR shrink may enqueue reports.
                            if b.has_pending_isr_reports() {
                                schedule_isr_update_reports(&b);
                            }
                        }
                    }
                }
            }));
        }

        // Phase 134: peer-to-peer heartbeat mesh (all configured peers).
        {
            let b = Arc::clone(&broker);
            let mut stop_rx = stop_tx.subscribe();
            handles.push(tokio::spawn(async move {
                let session = b
                    .cluster_config()
                    .map(|c| c.session_timeout_ms)
                    .unwrap_or(3000);
                let period = Duration::from_millis(u64::from(session / 3).max(100));
                let mut interval = tokio::time::interval(period);
                loop {
                    tokio::select! {
                        _ = stop_rx.changed() => break,
                        _ = interval.tick() => {
                            if let Err(e) = heartbeat_mesh(&b).await {
                                debug!(error = %e, "heartbeat mesh tick failed");
                            }
                        }
                    }
                }
            }));
        }

        // Phase 116 + 123: reconcile outbox from local log_start (leadership
        // handoff), then drain durable entries for live peers.
        {
            let b = Arc::clone(&broker);
            let mut stop_rx = stop_tx.subscribe();
            handles.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(500));
                loop {
                    tokio::select! {
                        _ = stop_rx.changed() => break,
                        _ = interval.tick() => {
                            // Phase 123: rebuild pending truncates when we lead
                            // with an advanced log_start (covers leadership change).
                            let _ = b.reconcile_delete_records_outbox();
                            if b.delete_records_outbox_depth() == 0 {
                                continue;
                            }
                            drain_delete_records_outbox(&b).await;
                        }
                    }
                }
            }));
        }

        // Follower ReplicaFetch loops.
        handles.push(run_follower_loops(Arc::clone(&broker), stop_tx.subscribe()));

        // v0.11: opt-in openraft metadata election. Dropping the task (or
        // aborting serve_listener) drops the raft node so the node is isolated.
        if broker.openraft_metadata_enabled() {
            let b = Arc::clone(&broker);
            let mut stop_rx = stop_tx.subscribe();
            handles.push(tokio::spawn(async move {
                let _guard = crate::cluster::OpenraftGuard(Arc::clone(&b));
                if let Err(e) = b.boot_openraft_metadata().await {
                    warn!(error = %e, "openraft metadata boot failed");
                    return;
                }
                loop {
                    tokio::select! {
                        _ = stop_rx.changed() => break,
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {
                            b.refresh_openraft_metrics();
                        }
                    }
                }
            }));
        }
    }

    BackgroundTasks { stop_tx, handles }
}

async fn handle_connection(mut stream: TcpStream, broker: Arc<Broker>) -> Result<()> {
    let mut buf = BytesMut::with_capacity(8 * 1024);
    // When auth is disabled, treat the connection as already authenticated.
    // Re-evaluated per request so bootstrap CreateScramUser can flip the gate.
    let mut authenticated = !broker.auth_required();
    let mut principal: Option<String> = None;
    let mut scram_challenge: Option<crate::scram::ScramChallenge> = None;

    loop {
        loop {
            match decode_frame(&mut buf)? {
                Some(frame) => {
                    let corr = frame.header.correlation_id;
                    let opcode = frame.header.opcode;
                    let span = info_span!(
                        "rpc",
                        opcode,
                        correlation_id = corr,
                        authenticated,
                        principal = principal.as_deref().unwrap_or("")
                    );
                    let response = async {
                        dispatch_with_auth(
                            &broker,
                            frame,
                            &mut authenticated,
                            &mut principal,
                            &mut scram_challenge,
                        )
                        .await
                    }
                    .instrument(span)
                    .await;
                    write_response(&mut stream, corr, response).await?;
                }
                None => break,
            }
        }

        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
    }
}

async fn write_response(stream: &mut TcpStream, corr: u32, response: Response) -> Result<()> {
    let frame = pack_response(corr, &response)?;
    let mut out = BytesMut::new();
    encode_frame(&frame, &mut out)?;
    stream.write_all(&out).await?;
    Ok(())
}
