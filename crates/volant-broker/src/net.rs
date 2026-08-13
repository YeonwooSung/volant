//! Framed TCP server for the Volant broker.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, info_span, warn, Instrument};
use volant_core::{Error, Message, MessageBatch, Offset, PartitionId, Result, TopicName};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_request, pack_response, Assignment, BrokerInfo, ErrorCode, FetchRecord,
    Frame, OffsetFetchEntry, PartitionInfo, Request, Response, TopicInfo,
};

use crate::broker::{Broker, Txn2pcFanout};
use crate::metrics::Metrics;
use crate::replica::run_follower_loops;
use crate::truncate_journal::TruncateJournal;

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
        let mut sigterm = match tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ) {
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

/// Serve Prometheus metrics over plain HTTP `GET /metrics`.
///
/// Binds `addr` and serves until the accept loop fails or a process shutdown
/// signal arrives (Phase 109). Intended to run as a background task alongside
/// the broker accept loop.
pub async fn run_metrics_server(addr: SocketAddr, broker: Arc<Broker>) -> Result<()> {
    run_metrics_server_until(addr, broker, shutdown_signal()).await
}

/// Like [`run_metrics_server`], but stops when `shutdown` completes (Phase 109).
pub async fn run_metrics_server_until<F>(
    addr: SocketAddr,
    broker: Arc<Broker>,
    shutdown: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()>,
{
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    info!(%local, "volant metrics listening");
    metrics_accept_loop(listener, broker, shutdown).await
}

async fn metrics_accept_loop<F>(
    listener: TcpListener,
    broker: Arc<Broker>,
    shutdown: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()>,
{
    tokio::pin!(shutdown);
    let mut conns: Vec<JoinHandle<()>> = Vec::new();
    loop {
        conns.retain(|h| !h.is_finished());
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received; draining metrics accept loop");
                break;
            }
            acc = listener.accept() => {
                match acc {
                    Ok((stream, peer)) => {
                        let b = Arc::clone(&broker);
                        conns.push(tokio::spawn(async move {
                            let mut stream = stream;
                            if let Err(e) = serve_metrics_connection(&mut stream, &b).await {
                                debug!(%peer, error = %e, "metrics connection closed");
                            }
                        }));
                    }
                    Err(e) => {
                        error!(error = %e, "metrics accept failed");
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

async fn serve_metrics_connection(stream: &mut TcpStream, broker: &Broker) -> Result<()> {
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    // Minimal HTTP/1.1: any request path is treated as GET /metrics for MVP.
    // Reject non-GET for slightly better hygiene.
    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or("");

    let response = if !first_line.starts_with("GET ") {
        http_response(
            "405 Method Not Allowed",
            "text/plain; charset=utf-8",
            "method not allowed\n",
            &[],
        )
    } else if let Some(expected) = broker.metrics_token() {
        // Phase 21: require Authorization: Bearer|Token <token>.
        if metrics_token_ok(&req, &expected) {
            let body = broker_metrics_text(broker);
            http_response(
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                &body,
                &[],
            )
        } else {
            http_response(
                "401 Unauthorized",
                "text/plain; charset=utf-8",
                "unauthorized\n",
                &[("WWW-Authenticate", "Bearer")],
            )
        }
    } else {
        let body = broker_metrics_text(broker);
        http_response(
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            &body,
            &[],
        )
    };

    stream.write_all(response.as_bytes()).await?;
    let _ = stream.shutdown().await;
    Ok(())
}

fn http_response(status: &str, content_type: &str, body: &str, extra: &[(&str, &str)]) -> String {
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (k, v) in extra {
        out.push_str(k);
        out.push_str(": ");
        out.push_str(v);
        out.push_str("\r\n");
    }
    out.push_str("\r\n");
    out.push_str(body);
    out
}

/// Validate metrics Authorization header against the configured token.
fn metrics_token_ok(request: &str, expected: &str) -> bool {
    for line in request.lines() {
        let line = line.trim();
        let lower = line.to_ascii_lowercase();
        if !lower.starts_with("authorization:") {
            continue;
        }
        let value = line.split_once(':').map(|(_, v)| v.trim()).unwrap_or("");
        let value_lower = value.to_ascii_lowercase();
        if let Some(rest) = value_lower
            .strip_prefix("bearer ")
            .or_else(|| value_lower.strip_prefix("token "))
        {
            // Compare using original casing for the token portion.
            let token = value[value.len() - rest.len()..].trim();
            return token == expected;
        }
    }
    false
}

/// Prometheus text for `GET /metrics` (also used by Phase 141 ops tests).
pub fn render_metrics(broker: &Broker) -> String {
    broker_metrics_text(broker)
}

fn broker_metrics_text(broker: &Broker) -> String {
    let metrics: Arc<Metrics> = broker.metrics();
    let lag = broker.consumer_lag_snapshots();
    let mut text = metrics.render_prometheus(
        broker.topic_count(),
        broker.partition_count_total(),
        broker.messages_coalesced(),
        env!("CARGO_PKG_VERSION"),
        &lag,
    );
    // Phase 95/97/115: fetch session gauges/counters.
    let sessions = broker.fetch_sessions();
    text.push_str("# HELP volant_fetch_sessions_active Live fetch sessions (this broker)\n");
    text.push_str("# TYPE volant_fetch_sessions_active gauge\n");
    text.push_str(&format!(
        "volant_fetch_sessions_active {}\n",
        sessions.active_count()
    ));
    text.push_str(
        "# HELP volant_fetch_sessions_evicted_total Idle TTL + LRU fetch session evictions\n",
    );
    text.push_str("# TYPE volant_fetch_sessions_evicted_total counter\n");
    text.push_str(&format!(
        "volant_fetch_sessions_evicted_total {}\n",
        sessions.evicted_total()
    ));
    // Phase 97: idle-only session counter + open/prepared txn gauges/counters.
    text.push_str(
        "# HELP volant_fetch_sessions_idle_evicted_total Idle TTL fetch session evictions\n",
    );
    text.push_str("# TYPE volant_fetch_sessions_idle_evicted_total counter\n");
    text.push_str(&format!(
        "volant_fetch_sessions_idle_evicted_total {}\n",
        sessions.idle_evicted_total()
    ));
    // Phase 115: durable restore + persist errors.
    text.push_str(
        "# HELP volant_fetch_sessions_restored Sessions restored from disk at last broker open\n",
    );
    text.push_str("# TYPE volant_fetch_sessions_restored gauge\n");
    text.push_str(&format!(
        "volant_fetch_sessions_restored {}\n",
        sessions.restored()
    ));
    text.push_str(
        "# HELP volant_fetch_sessions_persist_errors_total Durable session snapshot write failures\n",
    );
    text.push_str("# TYPE volant_fetch_sessions_persist_errors_total counter\n");
    text.push_str(&format!(
        "volant_fetch_sessions_persist_errors_total {}\n",
        sessions.persist_errors_total()
    ));
    // Phase 119: multi-broker session forward.
    text.push_str(
        "# HELP volant_fetch_session_forward_total Successful Kafka Fetch session forwards to owner\n",
    );
    text.push_str("# TYPE volant_fetch_session_forward_total counter\n");
    text.push_str(&format!(
        "volant_fetch_session_forward_total {}\n",
        sessions.forward_total()
    ));
    text.push_str(
        "# HELP volant_fetch_session_forward_errors_total Failed Kafka Fetch session forward attempts\n",
    );
    text.push_str("# TYPE volant_fetch_session_forward_errors_total counter\n");
    text.push_str(&format!(
        "volant_fetch_session_forward_errors_total {}\n",
        sessions.forward_errors_total()
    ));
    // Phase 138: session mirror + promote.
    text.push_str(
        "# HELP volant_fetch_session_mirror_puts_total Mirror put installs applied on this broker\n",
    );
    text.push_str("# TYPE volant_fetch_session_mirror_puts_total counter\n");
    text.push_str(&format!(
        "volant_fetch_session_mirror_puts_total {}\n",
        sessions.mirror_puts_applied_total()
    ));
    text.push_str(
        "# HELP volant_fetch_session_mirror_deletes_total Mirror deletes applied on this broker\n",
    );
    text.push_str("# TYPE volant_fetch_session_mirror_deletes_total counter\n");
    text.push_str(&format!(
        "volant_fetch_session_mirror_deletes_total {}\n",
        sessions.mirror_deletes_applied_total()
    ));
    text.push_str(
        "# HELP volant_fetch_session_promote_total Mirror→primary promotions after owner miss\n",
    );
    text.push_str("# TYPE volant_fetch_session_promote_total counter\n");
    text.push_str(&format!(
        "volant_fetch_session_promote_total {}\n",
        sessions.promote_total()
    ));
    text.push_str(
        "# HELP volant_fetch_sessions_mirrored Foreign session mirrors currently held\n",
    );
    text.push_str("# TYPE volant_fetch_sessions_mirrored gauge\n");
    text.push_str(&format!(
        "volant_fetch_sessions_mirrored {}\n",
        sessions.mirrored_count()
    ));
    // Phase 139: mirror polish (coalesce / fence / durable restore).
    text.push_str(
        "# HELP volant_fetch_session_mirror_puts_coalesced_total Pending Put ops dropped by coalesce\n",
    );
    text.push_str("# TYPE volant_fetch_session_mirror_puts_coalesced_total counter\n");
    text.push_str(&format!(
        "volant_fetch_session_mirror_puts_coalesced_total {}\n",
        sessions.mirror_puts_coalesced_total()
    ));
    text.push_str(
        "# HELP volant_fetch_session_mirror_stale_put_rejects_total Stale mirror puts rejected by fencing\n",
    );
    text.push_str("# TYPE volant_fetch_session_mirror_stale_put_rejects_total counter\n");
    text.push_str(&format!(
        "volant_fetch_session_mirror_stale_put_rejects_total {}\n",
        sessions.mirror_stale_put_rejects_total()
    ));
    text.push_str(
        "# HELP volant_fetch_session_promote_supersede_total Promotions where newer mirror superseded primary\n",
    );
    text.push_str("# TYPE volant_fetch_session_promote_supersede_total counter\n");
    text.push_str(&format!(
        "volant_fetch_session_promote_supersede_total {}\n",
        sessions.promote_supersede_total()
    ));
    text.push_str(
        "# HELP volant_fetch_session_mirror_restored Foreign mirrors restored from durable snapshot\n",
    );
    text.push_str("# TYPE volant_fetch_session_mirror_restored counter\n");
    text.push_str(&format!(
        "volant_fetch_session_mirror_restored {}\n",
        sessions.mirror_restored()
    ));
    // Phase 143: promote claim fence (lowest-id dual-promote).
    text.push_str(
        "# HELP volant_fetch_session_promote_claim_reject_total Dual-promote claim-lose rejects on put/promote\n",
    );
    text.push_str("# TYPE volant_fetch_session_promote_claim_reject_total counter\n");
    text.push_str(&format!(
        "volant_fetch_session_promote_claim_reject_total {}\n",
        sessions.promote_claim_reject_total()
    ));
    // Phase 147: serve foreign mirror without promote on owner miss.
    text.push_str(
        "# HELP volant_fetch_session_serve_from_mirror_total Owner-miss serves from foreign mirror without promote\n",
    );
    text.push_str("# TYPE volant_fetch_session_serve_from_mirror_total counter\n");
    text.push_str(&format!(
        "volant_fetch_session_serve_from_mirror_total {}\n",
        sessions.serve_from_mirror_total()
    ));
    // Phase 120/122: multi-broker EndTxn / AddOffsets / TxnOffsetCommit forward.
    text.push_str(
        "# HELP volant_txn_forward_total Successful Kafka txn API forwards to coordinator (EndTxn/AddOffsets/TxnOffsetCommit)\n",
    );
    text.push_str("# TYPE volant_txn_forward_total counter\n");
    text.push_str(&format!(
        "volant_txn_forward_total {}\n",
        broker.txn_forward_total()
    ));
    text.push_str(
        "# HELP volant_txn_forward_errors_total Failed Kafka txn API forward attempts\n",
    );
    text.push_str("# TYPE volant_txn_forward_errors_total counter\n");
    text.push_str(&format!(
        "volant_txn_forward_errors_total {}\n",
        broker.txn_forward_errors_total()
    ));
    // Phase 124: durable Init-owner registry restore / persist.
    text.push_str(
        "# HELP volant_txn_coordinator_registry_restored Txn coordinator registry map entries restored at last open\n",
    );
    text.push_str("# TYPE volant_txn_coordinator_registry_restored gauge\n");
    text.push_str(&format!(
        "volant_txn_coordinator_registry_restored {}\n",
        broker.txn_coordinator_registry_restored()
    ));
    text.push_str(
        "# HELP volant_txn_coordinator_registry_persist_errors_total Durable txn coordinator registry snapshot write failures\n",
    );
    text.push_str("# TYPE volant_txn_coordinator_registry_persist_errors_total counter\n");
    text.push_str(&format!(
        "volant_txn_coordinator_registry_persist_errors_total {}\n",
        broker.txn_coordinator_registry_persist_errors_total()
    ));
    // Phase 127: registry TTL GC.
    text.push_str(
        "# HELP volant_txn_coordinator_registry_gc_total Txn coordinator registry entries removed by TTL GC\n",
    );
    text.push_str("# TYPE volant_txn_coordinator_registry_gc_total counter\n");
    text.push_str(&format!(
        "volant_txn_coordinator_registry_gc_total {}\n",
        broker.txn_coordinator_registry_gc_total()
    ));
    text.push_str("# HELP volant_open_txns Live open (non-prepared) transactions\n");
    text.push_str("# TYPE volant_open_txns gauge\n");
    text.push_str(&format!("volant_open_txns {}\n", broker.open_txn_count()));
    text.push_str("# HELP volant_prepared_txns Live prepared (2PC) transactions\n");
    text.push_str("# TYPE volant_prepared_txns gauge\n");
    text.push_str(&format!(
        "volant_prepared_txns {}\n",
        broker.prepared_txn_count()
    ));
    text.push_str(
        "# HELP volant_open_txns_expired_total Open txns auto-aborted by timeout\n",
    );
    text.push_str("# TYPE volant_open_txns_expired_total counter\n");
    text.push_str(&format!(
        "volant_open_txns_expired_total {}\n",
        broker.open_txns_expired_total()
    ));
    text.push_str(
        "# HELP volant_prepared_txns_expired_total Prepared txns auto-aborted by timeout\n",
    );
    text.push_str("# TYPE volant_prepared_txns_expired_total counter\n");
    text.push_str(&format!(
        "volant_prepared_txns_expired_total {}\n",
        broker.prepared_txns_expired_total()
    ));
    // Phase 104: soft abort markers fully dropped after log prefix delete /
    // retention / load. Phase 111 straddling clips do not increment this.
    text.push_str(
        "# HELP volant_aborted_markers_gc_total Soft abort markers fully GC'd (below log start)\n",
    );
    text.push_str("# TYPE volant_aborted_markers_gc_total counter\n");
    text.push_str(&format!(
        "volant_aborted_markers_gc_total {}\n",
        broker.aborted_markers_gc_total()
    ));
    // Phase 113: DeleteRecords inter-broker fan-out failures (best-effort).
    text.push_str(
        "# HELP volant_delete_records_fanout_errors_total DeleteRecords replica fan-out failures\n",
    );
    text.push_str("# TYPE volant_delete_records_fanout_errors_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_fanout_errors_total {}\n",
        broker.delete_records_fanout_errors_total()
    ));
    // Phase 135: optional client-visible wait on truncate-journal majority.
    text.push_str(
        "# HELP volant_delete_records_majority_wait_success_total DeleteRecords wait-mode journal majority successes\n",
    );
    text.push_str("# TYPE volant_delete_records_majority_wait_success_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_majority_wait_success_total {}\n",
        broker.delete_records_majority_wait_success_total()
    ));
    text.push_str(
        "# HELP volant_delete_records_majority_wait_fail_total DeleteRecords wait-mode journal majority failures\n",
    );
    text.push_str("# TYPE volant_delete_records_majority_wait_fail_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_majority_wait_fail_total {}\n",
        broker.delete_records_majority_wait_fail_total()
    ));
    // Phase 116: durable DeleteRecords outbox for offline / failed peers.
    text.push_str(
        "# HELP volant_delete_records_outbox_depth Pending DeleteRecords truncates for peers\n",
    );
    text.push_str("# TYPE volant_delete_records_outbox_depth gauge\n");
    text.push_str(&format!(
        "volant_delete_records_outbox_depth {}\n",
        broker.delete_records_outbox_depth()
    ));
    text.push_str(
        "# HELP volant_delete_records_outbox_enqueued_total DeleteRecords outbox enqueues\n",
    );
    text.push_str("# TYPE volant_delete_records_outbox_enqueued_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_outbox_enqueued_total {}\n",
        broker.delete_records_outbox_enqueued_total()
    ));
    text.push_str(
        "# HELP volant_delete_records_outbox_retry_success_total Successful outbox drains\n",
    );
    text.push_str("# TYPE volant_delete_records_outbox_retry_success_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_outbox_retry_success_total {}\n",
        broker.delete_records_outbox_retry_success_total()
    ));
    text.push_str(
        "# HELP volant_delete_records_outbox_retry_errors_total Failed outbox drain RPCs\n",
    );
    text.push_str("# TYPE volant_delete_records_outbox_retry_errors_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_outbox_retry_errors_total {}\n",
        broker.delete_records_outbox_retry_errors_total()
    ));
    text.push_str(
        "# HELP volant_delete_records_outbox_drops_total Outbox enqueues dropped at capacity\n",
    );
    text.push_str("# TYPE volant_delete_records_outbox_drops_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_outbox_drops_total {}\n",
        broker.delete_records_outbox_drops_total()
    ));
    // Phase 123: leadership handoff reconcile from local log_start.
    text.push_str(
        "# HELP volant_delete_records_outbox_reconcile_total DeleteRecords outbox leadership reconciles\n",
    );
    text.push_str("# TYPE volant_delete_records_outbox_reconcile_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_outbox_reconcile_total {}\n",
        broker.delete_records_outbox_reconcile_total()
    ));
    text.push_str(
        "# HELP volant_cluster_config_push_errors_total BROKER config fan-out failures\n",
    );
    text.push_str("# TYPE volant_cluster_config_push_errors_total counter\n");
    text.push_str(&format!(
        "volant_cluster_config_push_errors_total {}\n",
        broker.cluster_config_push_errors_total()
    ));
    text.push_str("# HELP volant_config_generation Controller BROKER config generation\n");
    text.push_str("# TYPE volant_config_generation gauge\n");
    text.push_str(&format!(
        "volant_config_generation {}\n",
        broker.config_generation()
    ));
    text.push_str(
        "# HELP volant_applied_config_generation Last applied BROKER config generation\n",
    );
    text.push_str("# TYPE volant_applied_config_generation gauge\n");
    text.push_str(&format!(
        "volant_applied_config_generation {}\n",
        broker.applied_config_generation()
    ));
    text.push_str(
        "# HELP volant_cluster_acl_push_errors_total ACL snapshot fan-out failures\n",
    );
    text.push_str("# TYPE volant_cluster_acl_push_errors_total counter\n");
    text.push_str(&format!(
        "volant_cluster_acl_push_errors_total {}\n",
        broker.cluster_acl_push_errors_total()
    ));
    // Phase 114: multi-broker 2PC fan-out failures + controller prepared index.
    text.push_str(
        "# HELP volant_txn_2pc_fanout_errors_total Multi-broker 2PC prepare/complete/open fan-out failures\n",
    );
    text.push_str("# TYPE volant_txn_2pc_fanout_errors_total counter\n");
    text.push_str(&format!(
        "volant_txn_2pc_fanout_errors_total {}\n",
        broker.txn_2pc_fanout_errors_total()
    ));
    text.push_str(
        "# HELP volant_cluster_prepared_txns Controller cluster prepared index size\n",
    );
    text.push_str("# TYPE volant_cluster_prepared_txns gauge\n");
    text.push_str(&format!(
        "volant_cluster_prepared_txns {}\n",
        broker.cluster_prepared_txn_count()
    ));
    text.push_str("# HELP volant_acl_generation Controller ACL generation\n");
    text.push_str("# TYPE volant_acl_generation gauge\n");
    text.push_str(&format!(
        "volant_acl_generation {}\n",
        broker.acl_generation()
    ));
    text.push_str(
        "# HELP volant_applied_acl_generation Last applied ACL generation\n",
    );
    text.push_str("# TYPE volant_applied_acl_generation gauge\n");
    text.push_str(&format!(
        "volant_applied_acl_generation {}\n",
        broker.applied_acl_generation()
    ));
    // Phase 117: admin catch-up counters.
    text.push_str(
        "# HELP volant_cluster_admin_catchup_success_total Successful ACL/config catch-up RPCs\n",
    );
    text.push_str("# TYPE volant_cluster_admin_catchup_success_total counter\n");
    text.push_str(&format!(
        "volant_cluster_admin_catchup_success_total {}\n",
        broker.cluster_admin_catchup_success_total()
    ));
    text.push_str(
        "# HELP volant_cluster_admin_catchup_errors_total Failed ACL/config catch-up RPCs\n",
    );
    text.push_str("# TYPE volant_cluster_admin_catchup_errors_total counter\n");
    text.push_str(&format!(
        "volant_cluster_admin_catchup_errors_total {}\n",
        broker.cluster_admin_catchup_errors_total()
    ));
    // Phase 136: admin catch-up schedule skips (single-flight / min-interval).
    text.push_str(
        "# HELP volant_admin_catchup_skipped_total Admin catch-up schedules skipped (in-flight or throttle)\n",
    );
    text.push_str("# TYPE volant_admin_catchup_skipped_total counter\n");
    text.push_str(&format!(
        "volant_admin_catchup_skipped_total {}\n",
        broker.admin_catchup_skipped_total()
    ));
    // Phase 118: ISR expand / shrink.
    text.push_str(
        "# HELP volant_isr_expand_total ISR membership expansions (rejoin / catch-up)\n",
    );
    text.push_str("# TYPE volant_isr_expand_total counter\n");
    text.push_str(&format!(
        "volant_isr_expand_total {}\n",
        broker.isr_expand_total()
    ));
    text.push_str(
        "# HELP volant_isr_shrink_total ISR membership removals (death or lag)\n",
    );
    text.push_str("# TYPE volant_isr_shrink_total counter\n");
    text.push_str(&format!(
        "volant_isr_shrink_total {}\n",
        broker.isr_shrink_total()
    ));
    // Phase 125: time-based ISR shrink.
    text.push_str(
        "# HELP volant_isr_time_shrink_total ISR removals due to time-based lag\n",
    );
    text.push_str("# TYPE volant_isr_time_shrink_total counter\n");
    text.push_str(&format!(
        "volant_isr_time_shrink_total {}\n",
        broker.isr_time_shrink_total()
    ));
    // Phase 126: preferred read replica redirects.
    text.push_str(
        "# HELP volant_preferred_replica_redirect_total Fetch PreferredReadReplica redirects\n",
    );
    text.push_str("# TYPE volant_preferred_replica_redirect_total counter\n");
    text.push_str(&format!(
        "volant_preferred_replica_redirect_total {}\n",
        broker.preferred_replica_redirect_total()
    ));
    // Phase 140: preferred candidate suppressed (e.g. READ_COMMITTED).
    text.push_str(
        "# HELP volant_preferred_replica_suppressed_total Fetch preferred candidates suppressed\n",
    );
    text.push_str("# TYPE volant_preferred_replica_suppressed_total counter\n");
    text.push_str(&format!(
        "volant_preferred_replica_suppressed_total {}\n",
        broker.preferred_replica_suppressed_total()
    ));
    // Phase 144: preferred suppressed due to established fetch session.
    text.push_str(
        "# HELP volant_preferred_replica_session_suppressed_total Fetch preferred suppressed for established session\n",
    );
    text.push_str("# TYPE volant_preferred_replica_session_suppressed_total counter\n");
    text.push_str(&format!(
        "volant_preferred_replica_session_suppressed_total {}\n",
        broker.preferred_replica_session_suppressed_total()
    ));
    // Phase 129: truncate journal.
    text.push_str(
        "# HELP volant_truncate_journal_generation Controller/local truncate journal generation\n",
    );
    text.push_str("# TYPE volant_truncate_journal_generation gauge\n");
    text.push_str(&format!(
        "volant_truncate_journal_generation {}\n",
        broker.truncate_journal_generation()
    ));
    text.push_str(
        "# HELP volant_truncate_journal_entries Truncate journal watermark count\n",
    );
    text.push_str("# TYPE volant_truncate_journal_entries gauge\n");
    text.push_str(&format!(
        "volant_truncate_journal_entries {}\n",
        broker.truncate_journal().entry_count()
    ));
    text.push_str(
        "# HELP volant_truncate_journal_consensus_success_total Majority journal commits\n",
    );
    text.push_str("# TYPE volant_truncate_journal_consensus_success_total counter\n");
    text.push_str(&format!(
        "volant_truncate_journal_consensus_success_total {}\n",
        broker.truncate_journal_consensus_success_total()
    ));
    text.push_str(
        "# HELP volant_truncate_journal_consensus_fail_total Journal proposals without majority\n",
    );
    text.push_str("# TYPE volant_truncate_journal_consensus_fail_total counter\n");
    text.push_str(&format!(
        "volant_truncate_journal_consensus_fail_total {}\n",
        broker.truncate_journal_consensus_fail_total()
    ));
    // Phase 141: N=2 majority ops / health gauges (configured vs live).
    text.push_str(
        "# HELP volant_cluster_configured_brokers Configured static membership size (1 if single-node)\n",
    );
    text.push_str("# TYPE volant_cluster_configured_brokers gauge\n");
    text.push_str(&format!(
        "volant_cluster_configured_brokers {}\n",
        broker.configured_broker_count()
    ));
    text.push_str(
        "# HELP volant_cluster_live_brokers Live membership size from local view (1 if single-node)\n",
    );
    text.push_str("# TYPE volant_cluster_live_brokers gauge\n");
    text.push_str(&format!(
        "volant_cluster_live_brokers {}\n",
        broker.live_broker_count()
    ));
    text.push_str(
        "# HELP volant_cluster_majority_quorum Journal majority floor(N/2)+1 for configured N\n",
    );
    text.push_str("# TYPE volant_cluster_majority_quorum gauge\n");
    text.push_str(&format!(
        "volant_cluster_majority_quorum {}\n",
        broker.majority_quorum_size()
    ));
    text.push_str(
        "# HELP volant_cluster_majority_impossible 1 when live < majority(configured); journal majority cannot succeed\n",
    );
    text.push_str("# TYPE volant_cluster_majority_impossible gauge\n");
    text.push_str(&format!(
        "volant_cluster_majority_impossible {}\n",
        if broker.majority_impossible() { 1 } else { 0 }
    ));
    // Phase 131: truncate journal rejoin catch-up.
    text.push_str(
        "# HELP volant_journal_catchup_success_total Successful truncate-journal catch-up pushes\n",
    );
    text.push_str("# TYPE volant_journal_catchup_success_total counter\n");
    text.push_str(&format!(
        "volant_journal_catchup_success_total {}\n",
        broker.journal_catchup_success_total()
    ));
    text.push_str(
        "# HELP volant_journal_catchup_errors_total Failed truncate-journal catch-up pushes\n",
    );
    text.push_str("# TYPE volant_journal_catchup_errors_total counter\n");
    text.push_str(&format!(
        "volant_journal_catchup_errors_total {}\n",
        broker.journal_catchup_errors_total()
    ));
    // Phase 132: schedule skips (single-flight / min-interval).
    text.push_str(
        "# HELP volant_journal_catchup_skipped_total Catch-up schedules skipped (in-flight or throttle)\n",
    );
    text.push_str("# TYPE volant_journal_catchup_skipped_total counter\n");
    text.push_str(&format!(
        "volant_journal_catchup_skipped_total {}\n",
        broker.journal_catchup_skipped_total()
    ));
    text
}

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
        handles.push(run_follower_loops(broker, stop_tx.subscribe()));
    }

    BackgroundTasks { stop_tx, handles }
}

/// Phase 134: peer-to-peer heartbeat mesh (MVP).
///
/// Each tick:
/// - if self is the current controller, self-touch membership locally
/// - send [`Request::HeartbeatBroker`] to every other configured broker that
///   has an address (sequential, per-peer `inter_broker_rpc` timeout)
/// - on successful response: always [`Broker::note_peer_live`]
/// - **only** if the peer contacted is the current controller: apply the
///   controller alive-set + optional ClusterState pull (Phase 110/117 path)
///
/// Non-controller responses must never drive [`Broker::apply_controller_alive_set`]
/// — partial alive lists could shrink ISR incorrectly.
async fn heartbeat_mesh(broker: &Broker) -> Result<()> {
    let controller = broker.controller_id();
    // Controller still self-touches membership locally (existing path).
    if controller == broker.node_id() {
        let _ = broker.handle_heartbeat_broker(broker.node_id(), controller, broker.generation());
    }

    let Some(cfg) = broker.cluster_config() else {
        return Ok(());
    };
    let self_id = broker.node_id();
    let req = Request::HeartbeatBroker {
        broker_id: self_id,
        controller_id_known: controller,
        generation: broker.generation(),
        // Phase 117: report applied admin gens so controller can catch up lag.
        applied_config_generation: broker.applied_config_generation(),
        applied_acl_generation: broker.applied_acl_generation(),
        // Phase 131: report applied truncate-journal gen for rejoin catch-up.
        applied_journal_generation: broker.truncate_journal_applied_generation(),
    };

    for peer_id in cfg.broker_ids() {
        if peer_id == self_id {
            continue;
        }
        let Some(addr) = broker.broker_addr(peer_id) else {
            continue;
        };
        if let Err(e) = heartbeat_to_peer(broker, peer_id, &addr, &req).await {
            debug!(peer_id, error = %e, "peer heartbeat failed");
        }
    }
    Ok(())
}

/// Heartbeat one peer. Alive-set / ClusterState apply only when `peer_id` is
/// the current controller.
async fn heartbeat_to_peer(
    broker: &Broker,
    peer_id: u32,
    addr: &str,
    req: &Request,
) -> Result<()> {
    let resp = inter_broker_rpc(broker, addr, req).await?;
    match resp {
        Response::HeartbeatBroker {
            controller_id,
            generation,
            alive_brokers,
            ..
        } => {
            // Always mark the peer we reached as live (mesh liveness).
            broker.note_peer_live(peer_id);

            // Critical correctness (Phase 134): only trust alive-set / SoT
            // pull from the *current* controller. Non-controller peers may
            // return a partial local membership view.
            if peer_id == broker.controller_id() {
                // Phase 110: diff controller alive-set → on_broker_death for gaps
                // (local ISR shrink) before refreshing live peers.
                broker.apply_controller_alive_set(&alive_brokers)?;
                // Ensure the peer we reached (reported controller) stays live even
                // if a stale response omitted it from alive_brokers.
                broker.note_peer_live(controller_id);
                // Pull ClusterState if generation advanced.
                if generation > broker.generation() {
                    let cs_req = Request::ClusterState {
                        known_generation: broker.generation(),
                    };
                    if let Ok(cs_resp) = inter_broker_rpc(broker, addr, &cs_req).await {
                        if let Response::ClusterState {
                            generation: g,
                            controller_id: c,
                            topics,
                            ..
                        } = cs_resp
                        {
                            broker.apply_cluster_state(g, c, &topics)?;
                        }
                    }
                }
            }
            Ok(())
        }
        other => Err(Error::Protocol(format!(
            "unexpected heartbeat response from peer {peer_id}: {other:?}"
        ))),
    }
}

/// Phase 131: re-push full truncate journal snapshot to one lagging peer.
///
/// Any node with a newer journal may push (multi-controller). Uses opcode 88
/// `TruncateJournalPush`. Increments journal catch-up success/error metrics.
/// No-op when peer does not lag or local journal is empty.
///
/// Prefer [`schedule_catch_up_peer_truncate_journal`] from the HeartbeatBroker
/// path (Phase 132) so membership is not stalled by the RPC. This direct API
/// remains for tests and explicit callers.
pub async fn catch_up_peer_truncate_journal(
    broker: &Broker,
    peer_id: u32,
    peer_addr: &str,
    peer_applied_journal: u64,
) {
    if !broker.peer_journal_gen_lags(peer_applied_journal) {
        return;
    }
    let generation = broker.truncate_journal_generation();
    let snapshot = broker.truncate_journal().snapshot_bytes();
    let req = Request::TruncateJournalPush {
        generation,
        snapshot,
    };
    match inter_broker_rpc(broker, peer_addr, &req).await {
        Ok(Response::TruncateJournalPush { error_code: 0 }) => {
            broker.truncate_journal().note_journal_catchup_success();
            debug!(
                peer_id,
                %peer_addr,
                generation,
                peer_applied_journal,
                "truncate journal catch-up push ok"
            );
        }
        Ok(Response::TruncateJournalPush { error_code }) => {
            warn!(
                peer_id,
                %peer_addr,
                error_code,
                generation,
                peer_applied_journal,
                "truncate journal catch-up peer error"
            );
            broker.truncate_journal().note_journal_catchup_error();
        }
        Ok(other) => {
            warn!(
                peer_id,
                %peer_addr,
                ?other,
                generation,
                "truncate journal catch-up unexpected response"
            );
            broker.truncate_journal().note_journal_catchup_error();
        }
        Err(e) => {
            warn!(
                peer_id,
                %peer_addr,
                error = %e,
                generation,
                "truncate journal catch-up rpc failed"
            );
            broker.truncate_journal().note_journal_catchup_error();
        }
    }
}

/// Phase 132: schedule a non-blocking journal catch-up for a lagging peer.
///
/// Claims per-peer single-flight + min-interval throttle via
/// [`Broker::try_begin_journal_catchup`], then spawns a task that runs
/// [`catch_up_peer_truncate_journal`] and releases the claim. Returns
/// immediately so HeartbeatBroker membership is not stalled by the push RPC.
///
/// No-op when the peer does not lag or the schedule is throttled / already
/// in-flight (skipped metric increments on throttle).
pub fn schedule_catch_up_peer_truncate_journal(
    broker: Arc<Broker>,
    peer_id: u32,
    peer_addr: String,
    peer_applied_journal: u64,
) {
    if !broker.peer_journal_gen_lags(peer_applied_journal) {
        return;
    }
    if !broker.try_begin_journal_catchup(peer_id) {
        debug!(
            peer_id,
            peer_applied_journal,
            "truncate journal catch-up schedule skipped (in-flight or throttle)"
        );
        return;
    }
    tokio::spawn(async move {
        // Bound overall work; inter_broker_rpc already has its own timeout.
        // An extra outer timeout ensures finish_journal_catchup always runs.
        let timeout = inter_broker_rpc_timeout() + Duration::from_secs(1);
        let result = tokio::time::timeout(
            timeout,
            catch_up_peer_truncate_journal(
                &broker,
                peer_id,
                &peer_addr,
                peer_applied_journal,
            ),
        )
        .await;
        if result.is_err() {
            warn!(
                peer_id,
                %peer_addr,
                peer_applied_journal,
                "truncate journal catch-up timed out"
            );
            broker.truncate_journal().note_journal_catchup_error();
        }
        broker.finish_journal_catchup(peer_id);
    });
}

/// Phase 117: re-push controller ACL + BROKER config SoT to one lagging peer.
///
/// Uses Phase 113 opcodes. Increments catch-up success/error metrics. No-op when
/// this node is not the controller or when neither domain lags.
///
/// Prefer [`schedule_catch_up_peer_admin_state`] from the HeartbeatBroker path
/// (Phase 136) so membership is not stalled by up to two RPCs. This direct API
/// remains for tests and explicit callers.
pub async fn catch_up_peer_admin_state(
    broker: &Broker,
    peer_id: u32,
    peer_addr: &str,
    peer_applied_config: u64,
    peer_applied_acl: u64,
) {
    if !broker.is_controller() {
        return;
    }
    let (need_config, need_acl) =
        broker.peer_admin_gens_lag(peer_applied_config, peer_applied_acl);
    if !need_config && !need_acl {
        return;
    }

    if need_config {
        let generation = broker.config_generation();
        let entries = broker.describe_broker_configs();
        let req = Request::ClusterBrokerConfig {
            generation,
            entries,
        };
        match inter_broker_rpc(broker, peer_addr, &req).await {
            Ok(Response::ClusterBrokerConfig {
                error_code: 0,
                ..
            }) => {
                broker.note_cluster_admin_catchup_success();
            }
            Ok(Response::ClusterBrokerConfig {
                error_code,
                applied_generation,
            }) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    error_code,
                    applied_generation,
                    generation,
                    "admin config catch-up peer error"
                );
                broker.note_cluster_admin_catchup_error();
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    ?other,
                    generation,
                    "admin config catch-up unexpected response"
                );
                broker.note_cluster_admin_catchup_error();
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    error = %e,
                    generation,
                    "admin config catch-up rpc failed"
                );
                broker.note_cluster_admin_catchup_error();
            }
        }
    }

    if need_acl {
        let generation = broker.acl_generation();
        let snapshot = match broker.acl_snapshot_wire_bytes() {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    peer_id,
                    generation,
                    error = %e,
                    "admin acl catch-up encode failed"
                );
                broker.note_cluster_admin_catchup_error();
                return;
            }
        };
        let req = Request::ClusterAclSnapshot {
            generation,
            snapshot,
        };
        match inter_broker_rpc(broker, peer_addr, &req).await {
            Ok(Response::ClusterAclSnapshot {
                error_code: 0,
                ..
            }) => {
                broker.note_cluster_admin_catchup_success();
            }
            Ok(Response::ClusterAclSnapshot {
                error_code,
                applied_generation,
            }) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    error_code,
                    applied_generation,
                    generation,
                    "admin acl catch-up peer error"
                );
                broker.note_cluster_admin_catchup_error();
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    ?other,
                    generation,
                    "admin acl catch-up unexpected response"
                );
                broker.note_cluster_admin_catchup_error();
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    error = %e,
                    generation,
                    "admin acl catch-up rpc failed"
                );
                broker.note_cluster_admin_catchup_error();
            }
        }
    }
}

/// Phase 136: schedule a non-blocking admin (ACL/config) catch-up for a lagging peer.
///
/// Claims per-peer single-flight + min-interval throttle via
/// [`Broker::try_begin_admin_catchup`], then spawns a task that runs
/// [`catch_up_peer_admin_state`] and releases the claim. Returns immediately so
/// HeartbeatBroker membership is not stalled by config/ACL re-push RPCs.
///
/// No-op when this node is not the controller, the peer does not lag, or the
/// schedule is throttled / already in-flight (skipped metric increments on
/// throttle).
pub fn schedule_catch_up_peer_admin_state(
    broker: Arc<Broker>,
    peer_id: u32,
    peer_addr: String,
    peer_applied_config: u64,
    peer_applied_acl: u64,
) {
    if !broker.is_controller() {
        return;
    }
    let (need_config, need_acl) =
        broker.peer_admin_gens_lag(peer_applied_config, peer_applied_acl);
    if !need_config && !need_acl {
        return;
    }
    if !broker.try_begin_admin_catchup(peer_id) {
        debug!(
            peer_id,
            peer_applied_config,
            peer_applied_acl,
            "admin catch-up schedule skipped (in-flight or throttle)"
        );
        return;
    }
    tokio::spawn(async move {
        // Admin catch-up may run up to 2 RPCs (config + ACL); outer bound is
        // 2× inter_broker timeout + 1s so finish_admin_catchup always runs.
        let timeout = inter_broker_rpc_timeout() * 2 + Duration::from_secs(1);
        let result = tokio::time::timeout(
            timeout,
            catch_up_peer_admin_state(
                &broker,
                peer_id,
                &peer_addr,
                peer_applied_config,
                peer_applied_acl,
            ),
        )
        .await;
        if result.is_err() {
            warn!(
                peer_id,
                %peer_addr,
                peer_applied_config,
                peer_applied_acl,
                "admin catch-up timed out"
            );
            broker.note_cluster_admin_catchup_error();
        }
        broker.finish_admin_catchup(peer_id);
    });
}

/// Best-effort ACL snapshot fan-out to live peers (Phase 113 PR4).
///
/// Called after a successful **controller** ACL mutate. Loads the current
/// durable snapshot from the controller and pushes it with `generation`.
/// Failures increment [`Broker::cluster_acl_push_errors_total`].
pub async fn fanout_cluster_acl_snapshot(broker: &Broker, generation: u64) {
    let peers = broker.cluster_acl_fanout_peers();
    if peers.is_empty() {
        return;
    }
    let snapshot = match broker.acl_snapshot_wire_bytes() {
        Ok(b) => b,
        Err(e) => {
            warn!(generation, error = %e, "acl snapshot encode for fan-out failed");
            broker.note_cluster_acl_push_error();
            return;
        }
    };
    let req = Request::ClusterAclSnapshot {
        generation,
        snapshot,
    };
    for (peer_id, addr) in peers {
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::ClusterAclSnapshot {
                error_code: 0,
                ..
            }) => {}
            Ok(Response::ClusterAclSnapshot {
                error_code,
                applied_generation,
            }) => {
                warn!(
                    peer_id,
                    %addr,
                    error_code,
                    applied_generation,
                    generation,
                    "cluster acl fan-out peer error"
                );
                broker.note_cluster_acl_push_error();
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %addr,
                    ?other,
                    generation,
                    "cluster acl fan-out unexpected response"
                );
                broker.note_cluster_acl_push_error();
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %addr,
                    error = %e,
                    generation,
                    "cluster acl fan-out rpc failed"
                );
                broker.note_cluster_acl_push_error();
            }
        }
    }
}

/// Best-effort BROKER config fan-out to live peers (Phase 113 PR3).
///
/// Called after a successful **controller** [`Broker::alter_broker_configs`].
/// Failures increment [`Broker::cluster_config_push_errors_total`] and never
/// fail the client path. No-op when there are no peers.
pub async fn fanout_cluster_broker_config(
    broker: &Broker,
    generation: u64,
    entries: &[(String, String)],
) {
    let peers = broker.cluster_broker_config_fanout_peers();
    if peers.is_empty() {
        return;
    }
    let req = Request::ClusterBrokerConfig {
        generation,
        entries: entries.to_vec(),
    };
    for (peer_id, addr) in peers {
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::ClusterBrokerConfig {
                error_code: 0,
                ..
            }) => {}
            Ok(Response::ClusterBrokerConfig {
                error_code,
                applied_generation,
            }) => {
                warn!(
                    peer_id,
                    %addr,
                    error_code,
                    applied_generation,
                    generation,
                    "cluster broker config fan-out peer error"
                );
                broker.note_cluster_config_push_error();
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %addr,
                    ?other,
                    generation,
                    "cluster broker config fan-out unexpected response"
                );
                broker.note_cluster_config_push_error();
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %addr,
                    error = %e,
                    generation,
                    "cluster broker config fan-out rpc failed"
                );
                broker.note_cluster_config_push_error();
            }
        }
    }
}

/// Phase 129/130/135: multi-controller majority note + best-effort full-snapshot push.
///
/// 1. Durable **local** note (counts as 1 ack).
/// 2. **Parallel** `TruncateJournalNote` to all other live peers (`JoinSet`).
/// 3. If acks ≥ majority(configured N) → consensus success metric.
/// 4. Best-effort **parallel** `TruncateJournalPush` to **all** live peers
///    (full journal snapshot) so multi-key catch-up works even when a peer
///    acked the single-key note.
///
/// Returns `true` when there is **no cluster** or acks ≥ majority(configured N).
/// Not full Raft log/leader election. Client visibility of majority is gated by
/// [`Broker::delete_records_wait_majority`] (Phase 135; default off).
pub async fn fanout_truncate_journal_note(
    broker: &Broker,
    topic: &str,
    partition: u32,
    before_offset: u64,
    leader_epoch: i32,
) -> bool {
    if broker.cluster_config().is_none() {
        return true;
    }
    let n = broker.cluster_member_count();
    let need = TruncateJournal::majority(n);

    // 1) Local durable note (proposer).
    let local_gen =
        broker.local_note_truncate_journal(topic, partition, before_offset, leader_epoch);
    let mut acks = 1usize;

    // 2) Parallel note to every other live peer (multi-controller).
    let peers: Vec<(u32, String)> = broker
        .live_brokers()
        .into_iter()
        .filter(|id| *id != broker.node_id())
        .filter_map(|id| broker.broker_addr(id).map(|a| (id, a)))
        .collect();
    let peer_ids: Vec<u32> = peers.iter().map(|(id, _)| *id).collect();

    let auth = broker.auth_token();
    let tls = broker.inter_broker_tls();
    let mut set = tokio::task::JoinSet::new();
    for (peer_id, addr) in peers {
        let req = Request::TruncateJournalNote {
            topic: topic.to_owned(),
            partition,
            before_offset,
            leader_epoch,
        };
        let auth = auth.clone();
        let tls = tls.clone();
        set.spawn(async move {
            let res = inter_broker_rpc_owned(&addr, &req, auth, tls).await;
            (peer_id, res)
        });
    }

    let mut max_gen = local_gen;
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((_peer_id, Ok(Response::TruncateJournalNote {
                error_code: 0,
                generation,
            }))) => {
                acks += 1;
                if generation > max_gen {
                    max_gen = generation;
                }
            }
            Ok((peer_id, Ok(Response::TruncateJournalNote { error_code, .. }))) => {
                warn!(
                    peer_id,
                    error_code, topic, partition, "truncate journal note peer error"
                );
            }
            Ok((peer_id, Ok(other))) => {
                warn!(peer_id, ?other, topic, partition, "truncate journal note unexpected");
            }
            Ok((peer_id, Err(e))) => {
                warn!(
                    peer_id,
                    error = %e,
                    topic,
                    partition,
                    "truncate journal note rpc failed"
                );
            }
            Err(e) => {
                warn!(error = %e, topic, partition, "truncate journal note join error");
            }
        }
    }

    let majority_ok = acks >= need;
    if majority_ok {
        broker.truncate_journal().note_consensus_success();
        debug!(
            acks,
            need,
            n,
            topic,
            partition,
            before_offset,
            "truncate journal majority consensus ok"
        );
    } else {
        broker.truncate_journal().note_consensus_fail();
        warn!(
            acks,
            need,
            n,
            topic,
            partition,
            before_offset,
            "truncate journal majority consensus failed (best-effort state retained)"
        );
    }

    // Always full-snapshot push to live peers so multi-key journal catch-up works.
    let push_peers: Vec<(u32, String)> = peer_ids
        .into_iter()
        .filter_map(|id| broker.broker_addr(id).map(|a| (id, a)))
        .collect();
    fanout_truncate_journal_push_to(broker, max_gen.max(local_gen), push_peers).await;
    majority_ok
}

/// Phase 129/130: best-effort **parallel** push of full truncate journal snapshot
/// to all live peers (excluding self).
pub async fn fanout_truncate_journal_push(broker: &Broker, generation: u64) {
    if broker.cluster_config().is_none() {
        return;
    }
    let peers: Vec<(u32, String)> = broker
        .live_brokers()
        .into_iter()
        .filter(|id| *id != broker.node_id())
        .filter_map(|id| broker.broker_addr(id).map(|a| (id, a)))
        .collect();
    fanout_truncate_journal_push_to(broker, generation, peers).await;
}

/// Best-effort parallel push of the full truncate journal snapshot to an
/// explicit peer list.
async fn fanout_truncate_journal_push_to(
    broker: &Broker,
    generation: u64,
    peers: Vec<(u32, String)>,
) {
    if peers.is_empty() {
        return;
    }
    let snapshot = broker.truncate_journal().snapshot_bytes();
    let gen = generation.max(broker.truncate_journal_generation());
    let auth = broker.auth_token();
    let tls = broker.inter_broker_tls();
    let mut set = tokio::task::JoinSet::new();
    for (peer_id, addr) in peers {
        let req = Request::TruncateJournalPush {
            generation: gen,
            snapshot: snapshot.clone(),
        };
        let auth = auth.clone();
        let tls = tls.clone();
        set.spawn(async move {
            let res = inter_broker_rpc_owned(&addr, &req, auth, tls).await;
            (peer_id, res)
        });
    }
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((_, Ok(Response::TruncateJournalPush { error_code: 0 }))) => {}
            Ok((peer_id, Ok(Response::TruncateJournalPush { error_code }))) => {
                warn!(peer_id, error_code, "truncate journal push peer error");
            }
            Ok((peer_id, Ok(other))) => {
                warn!(peer_id, ?other, "truncate journal push unexpected response");
            }
            Ok((peer_id, Err(e))) => {
                warn!(peer_id, error = %e, "truncate journal push rpc failed");
            }
            Err(e) => {
                warn!(error = %e, "truncate journal push join error");
            }
        }
    }
}

/// Result of [`fanout_delete_records`] (Phase 135).
///
/// `majority_ok` reflects **truncate-journal majority** only (not replica log
/// truncate / outbox). Single-node and note-skipped (not leader) paths report
/// `true` so wait-mode does not false-fail the client for those cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteRecordsFanoutResult {
    /// Whether journal majority was reached (or no cluster / note skipped).
    pub majority_ok: bool,
}

/// Best-effort DeleteRecords fan-out to other replicas (Phase 113/116 + 129/130/135).
///
/// After a successful **leader** local truncate: multi-controller journal note
/// (majority + full-snapshot push), then **parallel** `ReplicaDeleteRecords` to
/// peers. Peers are **pre-enqueued** on the durable outbox before the JoinSet
/// so a budget abort or join failure cannot lose retry state; successful /
/// fenced peers are `drop_entry`'d.
///
/// Returns [`DeleteRecordsFanoutResult`]; default client path still ignores
/// `majority_ok` unless [`Broker::delete_records_wait_majority`] is on.
///
/// `truncate_to` is the **achieved** log start (whole-segment-clamped low
/// watermark), not the client-requested offset.
///
/// Journal majority is evaluated **before** the remaining budget is applied to
/// replica fan-out, so a slow peer log-truncate cannot flip `majority_ok`.
/// Overall deadline: [`delete_records_fanout_budget`] (default **20s**, or at
/// least `3 *` [`inter_broker_rpc_timeout`] `+ 2s` when env unset). Each peer
/// RPC is still bounded by [`inter_broker_rpc_timeout`] (default **5s**).
pub async fn fanout_delete_records(
    broker: &Broker,
    topic: &str,
    partition: u32,
    truncate_to: u64,
) -> DeleteRecordsFanoutResult {
    let budget = delete_records_fanout_budget();
    let start = std::time::Instant::now();

    // Phase 129/130/135: journal note first so majority_ok is known even if
    // subsequent ReplicaDeleteRecords hits the remaining budget.
    // Only stamp while we still lead — never send leader_epoch=-1 (ingress
    // rejects negative epochs for non-zero watermarks). Leadership loss after
    // local truncate skips the note; the new leader reconcile uses log_start.
    // Prefer majority_ok=true when note is skipped (not leader) — client already
    // got local success or NotLeader before fan-out in the normal path.
    let majority_ok = if broker.cluster_config().is_some() {
        match broker.led_partition_epoch(topic, partition) {
            Some(epoch) => {
                let note_budget = budget.saturating_sub(start.elapsed());
                if note_budget.is_zero() {
                    warn!(
                        topic,
                        partition,
                        truncate_to,
                        "delete records fan-out budget exhausted before journal note"
                    );
                    false
                } else {
                    match tokio::time::timeout(
                        note_budget,
                        fanout_truncate_journal_note(
                            broker,
                            topic,
                            partition,
                            truncate_to,
                            epoch,
                        ),
                    )
                    .await
                    {
                        Ok(ok) => ok,
                        Err(_) => {
                            warn!(
                                topic,
                                partition,
                                truncate_to,
                                budget_ms = budget.as_millis() as u64,
                                "delete records journal note exceeded fan-out budget"
                            );
                            false
                        }
                    }
                }
            }
            None => {
                debug!(
                    topic,
                    partition,
                    truncate_to,
                    "skip truncate journal note: not partition leader (or unknown TP)"
                );
                true
            }
        }
    } else {
        true
    };

    let remaining = budget.saturating_sub(start.elapsed());
    if remaining.is_zero() {
        warn!(
            topic,
            partition,
            truncate_to,
            budget_ms = budget.as_millis() as u64,
            "delete records fan-out budget exhausted before replica truncate; unfinished peers remain on outbox for drain/reconcile"
        );
        return DeleteRecordsFanoutResult { majority_ok };
    }

    match tokio::time::timeout(
        remaining,
        fanout_delete_records_replica_inner(broker, topic, partition, truncate_to),
    )
    .await
    {
        Ok(()) => {}
        Err(_) => {
            warn!(
                topic,
                partition,
                truncate_to,
                budget_ms = budget.as_millis() as u64,
                "delete records replica fan-out overall budget exceeded; unfinished peers remain on outbox for drain/reconcile"
            );
        }
    }
    DeleteRecordsFanoutResult { majority_ok }
}

async fn fanout_delete_records_replica_inner(
    broker: &Broker,
    topic: &str,
    partition: u32,
    truncate_to: u64,
) {
    let peers = broker.delete_records_fanout_peers(topic, partition);
    if peers.is_empty() {
        return;
    }

    // Pre-enqueue all fan-out peers before spawning RPCs so budget abort /
    // JoinError cannot lose outbox coverage (re-enqueue on later failure is
    // idempotent).
    for (replica_id, _addr, leader_epoch) in &peers {
        broker.enqueue_delete_records_outbox(
            *replica_id,
            topic,
            partition,
            truncate_to,
            *leader_epoch,
        );
    }

    let auth = broker.auth_token();
    let tls = broker.inter_broker_tls();
    let mut set = tokio::task::JoinSet::new();
    for (replica_id, addr, leader_epoch) in peers {
        let req = Request::ReplicaDeleteRecords {
            topic: topic.to_owned(),
            partition,
            before_offset: truncate_to,
            leader_epoch,
        };
        let auth = auth.clone();
        let tls = tls.clone();
        set.spawn(async move {
            let res = inter_broker_rpc_owned(&addr, &req, auth, tls).await;
            (replica_id, addr, leader_epoch, res)
        });
    }
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((replica_id, _addr, _epoch, Ok(Response::ReplicaDeleteRecords {
                error_code: 0,
                ..
            }))) => {
                broker.delete_records_outbox().drop_entry(
                    replica_id,
                    topic,
                    partition,
                );
            }
            Ok((replica_id, addr, leader_epoch, Ok(Response::ReplicaDeleteRecords {
                error_code,
                low_watermark,
            }))) => {
                warn!(
                    replica_id,
                    %addr,
                    error_code,
                    low_watermark,
                    topic,
                    partition,
                    "delete records fan-out peer error"
                );
                broker.note_delete_records_fanout_error();
                if error_code == ErrorCode::InvalidProducerEpoch as u16 {
                    broker.delete_records_outbox().drop_entry(
                        replica_id,
                        topic,
                        partition,
                    );
                } else {
                    // Already pre-enqueued; re-enqueue is idempotent / refreshes.
                    broker.enqueue_delete_records_outbox(
                        replica_id,
                        topic,
                        partition,
                        truncate_to,
                        leader_epoch,
                    );
                }
            }
            Ok((replica_id, addr, leader_epoch, Ok(other))) => {
                warn!(
                    replica_id,
                    %addr,
                    ?other,
                    topic,
                    partition,
                    "delete records fan-out unexpected response"
                );
                broker.note_delete_records_fanout_error();
                broker.enqueue_delete_records_outbox(
                    replica_id,
                    topic,
                    partition,
                    truncate_to,
                    leader_epoch,
                );
            }
            Ok((replica_id, addr, leader_epoch, Err(e))) => {
                warn!(
                    replica_id,
                    %addr,
                    error = %e,
                    topic,
                    partition,
                    "delete records fan-out rpc failed"
                );
                broker.note_delete_records_fanout_error();
                broker.enqueue_delete_records_outbox(
                    replica_id,
                    topic,
                    partition,
                    truncate_to,
                    leader_epoch,
                );
            }
            // Pre-enqueued: JoinError has no replica_id; entry stays for drain.
            Err(e) => {
                warn!(error = %e, topic, partition, "delete records fan-out join error");
                broker.note_delete_records_fanout_error();
            }
        }
    }
}

/// Drain durable DeleteRecords outbox for currently live peers (Phase 116 + 123).
///
/// **Parallel** at-least-once retry of `ReplicaDeleteRecords`. Success removes
/// the entry; transport / peer errors leave it and increment retry-error metrics.
/// When this node still leads the partition, the RPC uses the **current**
/// local leader epoch (Phase 123) so an epoch bump does not self-fence.
/// No-op when the outbox is empty or the broker is single-node with no pending.
pub async fn drain_delete_records_outbox(broker: &Broker) {
    let pending = broker.delete_records_outbox_pending_live();
    if pending.is_empty() {
        return;
    }
    let auth = broker.auth_token();
    let tls = broker.inter_broker_tls();
    let mut set = tokio::task::JoinSet::new();
    for entry in pending {
        let Some(addr) = broker.broker_addr(entry.replica_id) else {
            continue;
        };
        // Phase 123: prefer current epoch while we still lead this TP.
        let leader_epoch = broker
            .led_partition_epoch(&entry.topic, entry.partition)
            .unwrap_or(entry.leader_epoch);
        let req = Request::ReplicaDeleteRecords {
            topic: entry.topic.clone(),
            partition: entry.partition,
            before_offset: entry.before_offset,
            leader_epoch,
        };
        let auth = auth.clone();
        let tls = tls.clone();
        let replica_id = entry.replica_id;
        let topic = entry.topic.clone();
        let partition = entry.partition;
        let before_offset = entry.before_offset;
        set.spawn(async move {
            let res = inter_broker_rpc_owned(&addr, &req, auth, tls).await;
            (replica_id, topic, partition, before_offset, leader_epoch, res)
        });
    }
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((replica_id, topic, partition, before_offset, _epoch, Ok(Response::ReplicaDeleteRecords {
                error_code: 0,
                ..
            }))) => {
                broker.delete_records_outbox().note_retry_success(
                    replica_id,
                    &topic,
                    partition,
                    before_offset,
                );
            }
            Ok((replica_id, topic, partition, _before, _epoch, Ok(Response::ReplicaDeleteRecords {
                error_code,
                low_watermark,
            }))) => {
                if error_code == ErrorCode::InvalidProducerEpoch as u16 {
                    // Stale epoch — drop; Phase 123 new-leader reconcile re-creates.
                    broker.delete_records_outbox().drop_entry(
                        replica_id,
                        &topic,
                        partition,
                    );
                    warn!(
                        replica_id,
                        topic = %topic,
                        partition,
                        error_code,
                        low_watermark,
                        "delete records outbox drain fenced; dropping entry"
                    );
                } else {
                    warn!(
                        replica_id,
                        error_code,
                        low_watermark,
                        topic = %topic,
                        partition,
                        "delete records outbox drain peer error"
                    );
                    broker.delete_records_outbox().note_retry_error();
                }
            }
            Ok((replica_id, topic, partition, _before, _epoch, Ok(other))) => {
                warn!(
                    replica_id,
                    ?other,
                    topic = %topic,
                    partition,
                    "delete records outbox drain unexpected response"
                );
                broker.delete_records_outbox().note_retry_error();
            }
            Ok((replica_id, topic, partition, _before, _epoch, Err(e))) => {
                debug!(
                    replica_id,
                    error = %e,
                    topic = %topic,
                    partition,
                    "delete records outbox drain rpc failed"
                );
                broker.delete_records_outbox().note_retry_error();
            }
            Err(e) => {
                warn!(error = %e, "delete records outbox drain join error");
                broker.delete_records_outbox().note_retry_error();
            }
        }
    }
}

/// Run multi-broker 2PC fan-out indicated by [`Txn2pcFanout`] (Phase 114).
///
/// - **Open**: best-effort (metric++ on failure; does not fail the client).
/// - **Prepare**: strict for live peers; returns `false` if any peer fails
///   (caller should [`Broker::rollback_local_prepare`]).
/// - **Complete**: strict for live peers; returns `false` on failure (client
///   already local-finalized — metric++ and log; re-issue may be needed).
///
/// Returns `true` when all required peer RPCs succeeded (or there were no peers).
pub async fn run_txn_2pc_fanout(broker: &Broker, fanout: &Txn2pcFanout) -> bool {
    match fanout {
        Txn2pcFanout::None => true,
        Txn2pcFanout::Open {
            transactional_id,
            producer_id,
            producer_epoch,
            enable_2pc,
            coordinator_node_id,
            install_open,
        } => {
            fanout_txn_participant_open(
                broker,
                transactional_id,
                *producer_id,
                *producer_epoch,
                *enable_2pc,
                *coordinator_node_id,
                *install_open,
            )
            .await;
            true
        }
        Txn2pcFanout::Prepare {
            transactional_id,
            producer_id,
            producer_epoch,
            commit,
        } => {
            fanout_txn_participant_prepare(
                broker,
                transactional_id,
                *producer_id,
                *producer_epoch,
                *commit,
            )
            .await
        }
        Txn2pcFanout::Complete {
            transactional_id,
            producer_id,
            producer_epoch,
            commit,
        } => {
            fanout_txn_participant_complete(
                broker,
                transactional_id,
                *producer_id,
                *producer_epoch,
                *commit,
            )
            .await
        }
    }
}

/// Best-effort open fan-out (Phase 114 + Phase 120 coordinator trailer).
pub async fn fanout_txn_participant_open(
    broker: &Broker,
    transactional_id: &str,
    producer_id: u64,
    producer_epoch: u16,
    enable_2pc: bool,
    coordinator_node_id: u32,
    install_open: bool,
) {
    let peers = broker.txn_2pc_fanout_peers();
    if peers.is_empty() {
        return;
    }
    let req = Request::TxnParticipantOpen {
        transactional_id: transactional_id.to_owned(),
        producer_id,
        producer_epoch,
        enable_2pc,
        coordinator_node_id,
        install_open,
    };
    for (peer_id, addr) in peers {
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::TxnParticipantOpen { error_code: 0 }) => {}
            Ok(Response::TxnParticipantOpen { error_code }) => {
                warn!(
                    peer_id,
                    %addr,
                    error_code,
                    producer_id,
                    "txn open fan-out peer error"
                );
                broker.note_txn_2pc_fanout_error();
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %addr,
                    ?other,
                    producer_id,
                    "txn open fan-out unexpected response"
                );
                broker.note_txn_2pc_fanout_error();
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %addr,
                    error = %e,
                    producer_id,
                    "txn open fan-out rpc failed"
                );
                broker.note_txn_2pc_fanout_error();
            }
        }
    }
}

/// Strict prepare fan-out (Phase 114). Returns `true` if all live peers OK.
pub async fn fanout_txn_participant_prepare(
    broker: &Broker,
    transactional_id: &str,
    producer_id: u64,
    producer_epoch: u16,
    commit: bool,
) -> bool {
    let peers = broker.txn_2pc_fanout_peers();
    if peers.is_empty() {
        return true;
    }
    let req = Request::TxnParticipantPrepare {
        transactional_id: transactional_id.to_owned(),
        producer_id,
        producer_epoch,
        commit,
    };
    let mut ok = true;
    for (peer_id, addr) in peers {
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::TxnParticipantPrepare { error_code: 0 }) => {}
            Ok(Response::TxnParticipantPrepare { error_code }) => {
                warn!(
                    peer_id,
                    %addr,
                    error_code,
                    producer_id,
                    transactional_id,
                    "txn prepare fan-out peer error"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %addr,
                    ?other,
                    producer_id,
                    "txn prepare fan-out unexpected response"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %addr,
                    error = %e,
                    producer_id,
                    "txn prepare fan-out rpc failed"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
        }
    }
    ok
}

/// Strict complete fan-out (Phase 114). Returns `true` if all live peers OK.
pub async fn fanout_txn_participant_complete(
    broker: &Broker,
    transactional_id: &str,
    producer_id: u64,
    producer_epoch: u16,
    commit: bool,
) -> bool {
    let peers = broker.txn_2pc_fanout_peers();
    if peers.is_empty() {
        return true;
    }
    let req = Request::TxnParticipantComplete {
        transactional_id: transactional_id.to_owned(),
        producer_id,
        producer_epoch,
        commit,
    };
    let mut ok = true;
    for (peer_id, addr) in peers {
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::TxnParticipantComplete { error_code: 0 }) => {}
            Ok(Response::TxnParticipantComplete { error_code }) => {
                warn!(
                    peer_id,
                    %addr,
                    error_code,
                    producer_id,
                    transactional_id,
                    "txn complete fan-out peer error"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %addr,
                    ?other,
                    producer_id,
                    "txn complete fan-out unexpected response"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %addr,
                    error = %e,
                    producer_id,
                    "txn complete fan-out rpc failed"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
        }
    }
    ok
}

/// Phase 120: peek EndTxn request body for transactional_id + producer_id.
///
/// `version` is the Kafka EndTxn API version (0–5). Returns `None` on truncated body.
pub fn peek_end_txn_ids(version: i16, body: &[u8]) -> Option<(String, u64)> {
    use bytes::Buf;
    use crate::kafka::wire;
    let flex = version >= 3;
    let mut src = body;
    let txn_id = wire::read_string(&mut src, flex).ok()?;
    if src.remaining() < 8 + 2 + 1 {
        return None;
    }
    let producer_id = src.get_i64() as u64;
    Some((txn_id, producer_id))
}

/// Phase 122: peek AddOffsetsToTxn body for transactional_id + producer_id.
///
/// Wire: txn_id, producer_id, producer_epoch, group_id (classic 0–2 / flex 3–4).
pub fn peek_add_offsets_to_txn_ids(version: i16, body: &[u8]) -> Option<(String, u64)> {
    use bytes::Buf;
    use crate::kafka::wire;
    let flex = version >= 3;
    let mut src = body;
    let txn_id = wire::read_string(&mut src, flex).ok()?;
    if src.remaining() < 8 + 2 {
        return None;
    }
    let producer_id = src.get_i64() as u64;
    Some((txn_id, producer_id))
}

/// Phase 122: peek TxnOffsetCommit body for transactional_id + producer_id.
///
/// Wire: txn_id, group_id, producer_id, producer_epoch, … (classic 0–2 / flex 3–6).
pub fn peek_txn_offset_commit_ids(version: i16, body: &[u8]) -> Option<(String, u64)> {
    use bytes::Buf;
    use crate::kafka::wire;
    let flex = version >= 3;
    let mut src = body;
    let txn_id = wire::read_string(&mut src, flex).ok()?;
    let _group_id = wire::read_string(&mut src, flex).ok()?;
    if src.remaining() < 8 + 2 {
        return None;
    }
    let producer_id = src.get_i64() as u64;
    Some((txn_id, producer_id))
}

/// Phase 120/122: minimal Kafka txn API error response bodies (no response header).
fn put_end_txn_error_response(out: &mut bytes::BytesMut, version: i16, err: i16) {
    use bytes::BufMut;
    use crate::kafka::codec::put_empty_tag_buffer;
    let flex = version >= 3;
    out.put_i32(0); // throttle
    out.put_i16(err);
    if version >= 5 {
        out.put_i64(-1);
        out.put_i16(-1);
    }
    if flex {
        put_empty_tag_buffer(out);
    }
}

fn put_add_offsets_error_response(out: &mut bytes::BytesMut, version: i16, err: i16) {
    use bytes::BufMut;
    use crate::kafka::codec::put_empty_tag_buffer;
    let flex = version >= 3;
    out.put_i32(0); // throttle
    out.put_i16(err);
    if flex {
        put_empty_tag_buffer(out);
    }
}

fn put_txn_offset_commit_empty_response(out: &mut bytes::BytesMut, version: i16) {
    use bytes::BufMut;
    use crate::kafka::codec::{put_compact_array_len, put_empty_tag_buffer};
    let flex = version >= 3;
    out.put_i32(0); // throttle
    if flex {
        put_compact_array_len(out, 0);
        put_empty_tag_buffer(out);
    } else {
        out.put_i32(0); // empty topics
    }
}

/// Build an honest client-facing body when txn forward fails (peer/RPC).
fn put_txn_forward_error_body(
    out: &mut bytes::BytesMut,
    api_key: i16,
    api_version: i16,
) {
    // UnknownProducerId (59) for simple error-code APIs; TxnOffsetCommit has no
    // top-level error — empty topics (no silent local buffer).
    match api_key {
        25 => put_add_offsets_error_response(out, api_version, 59),
        28 => put_txn_offset_commit_empty_response(out, api_version),
        // 26 EndTxn (default)
        _ => put_end_txn_error_response(out, api_version, 59),
    }
}

/// Phase 120/122: if a Kafka txn API should be served by the Init-owner
/// coordinator, forward the body and return the coordinator response.
/// `None` = handle locally (no cluster, registry miss, or self is coordinator).
///
/// Supported `api_key`: 25 AddOffsetsToTxn, 26 EndTxn, 28 TxnOffsetCommit.
/// Never re-forwards on the coordinator (caller is the Kafka client path only).
pub async fn maybe_forward_kafka_txn(
    broker: &Broker,
    api_key: i16,
    api_version: i16,
    principal: &str,
    body: &[u8],
) -> Option<Bytes> {
    use bytes::BytesMut;

    if broker.cluster_config().is_none() {
        return None;
    }
    let (txn_id, producer_id) = match api_key {
        25 => peek_add_offsets_to_txn_ids(api_version, body)?,
        26 => peek_end_txn_ids(api_version, body)?,
        28 => peek_txn_offset_commit_ids(api_version, body)?,
        _ => return None,
    };
    let Some(coord) = broker.resolve_txn_coordinator(&txn_id, Some(producer_id)) else {
        return None;
    };
    if coord == broker.node_id() {
        return None;
    }
    let Some(addr) = broker.broker_addr(coord) else {
        broker.record_txn_forward_error();
        let mut out = BytesMut::new();
        put_txn_forward_error_body(&mut out, api_key, api_version);
        return Some(out.freeze());
    };

    let req = Request::KafkaTxnForward {
        api_key,
        api_version,
        principal: principal.to_owned(),
        body: Bytes::copy_from_slice(body),
    };
    match inter_broker_rpc(broker, &addr, &req).await {
        Ok(Response::KafkaTxnForward {
            error_code: 0,
            body,
        }) => {
            broker.record_txn_forward_ok();
            Some(body)
        }
        Ok(Response::KafkaTxnForward { error_code, .. }) => {
            tracing::debug!(
                coord,
                error_code,
                api_key,
                %txn_id,
                producer_id,
                "kafka txn forward peer error"
            );
            broker.record_txn_forward_error();
            let mut out = BytesMut::new();
            put_txn_forward_error_body(&mut out, api_key, api_version);
            Some(out.freeze())
        }
        Ok(other) => {
            tracing::debug!(
                coord,
                ?other,
                api_key,
                %txn_id,
                "kafka txn forward unexpected response"
            );
            broker.record_txn_forward_error();
            let mut out = BytesMut::new();
            put_txn_forward_error_body(&mut out, api_key, api_version);
            Some(out.freeze())
        }
        Err(e) => {
            tracing::debug!(
                coord,
                error = %e,
                api_key,
                %txn_id,
                "kafka txn forward rpc failed"
            );
            broker.record_txn_forward_error();
            let mut out = BytesMut::new();
            put_txn_forward_error_body(&mut out, api_key, api_version);
            Some(out.freeze())
        }
    }
}

/// Phase 120: EndTxn-only wrapper around [`maybe_forward_kafka_txn`].
pub async fn maybe_forward_kafka_end_txn(
    broker: &Broker,
    api_version: i16,
    principal: &str,
    end_txn_body: &[u8],
) -> Option<Bytes> {
    maybe_forward_kafka_txn(broker, 26, api_version, principal, end_txn_body).await
}

/// Phase 119 + 138 + 147: if this Fetch should be served by a peer session owner,
/// forward the Kafka body and return the owner's response body.
/// `None` = handle locally (primary hit, serve-from-mirror, or promote-from-mirror).
///
/// Never re-forwards on the owner (caller is the Kafka client path only).
pub async fn maybe_forward_kafka_fetch(
    broker: &Broker,
    api_version: i16,
    principal: &str,
    fetch_body: &[u8],
) -> Option<Bytes> {
    use crate::kafka::fetch_session::{decode_session_owner, INITIAL_EPOCH};
    use crate::kafka::produce_fetch::{peek_fetch_session, put_fetch_empty_response};
    use bytes::BytesMut;

    if broker.cluster_config().is_none() || api_version < 7 {
        return None;
    }
    let (session_id, session_epoch) = peek_fetch_session(api_version, fetch_body)?;
    // Create path stays local.
    if session_id == 0 || session_epoch == INITIAL_EPOCH {
        return None;
    }
    // Local primary hit → encode_fetch. Mirror-only still attempts Phase 119
    // forward while owner is reachable; on owner miss, try_owner_miss_local_serve
    // prefers serve-from-mirror without promote (Phase 147).
    if broker.fetch_sessions().contains(session_id) {
        return None;
    }
    let owner = decode_session_owner(session_id)?;
    if owner == broker.node_id() {
        // Encoded as us but primary missing: serve mirror or promote (Phase 147/138).
        if broker
            .fetch_sessions()
            .try_owner_miss_local_serve(session_id)
        {
            return None;
        }
        return None;
    }
    let Some(addr) = broker.broker_addr(owner) else {
        // Owner addr unknown — serve mirror or promote.
        if broker
            .fetch_sessions()
            .try_owner_miss_local_serve(session_id)
        {
            return None;
        }
        broker.fetch_sessions().record_forward_error();
        let mut out = BytesMut::new();
        put_fetch_empty_response(&mut out, api_version, 70, session_id);
        return Some(out.freeze());
    };

    let req = Request::KafkaFetchForward {
        api_version,
        principal: principal.to_owned(),
        body: Bytes::copy_from_slice(fetch_body),
    };
    match inter_broker_rpc(broker, &addr, &req).await {
        Ok(Response::KafkaFetchForward {
            error_code: 0,
            body,
        }) => {
            broker.fetch_sessions().record_forward_ok();
            Some(body)
        }
        Ok(Response::KafkaFetchForward { error_code, .. }) => {
            tracing::debug!(
                owner,
                error_code,
                session_id,
                "kafka fetch forward peer error"
            );
            broker.fetch_sessions().record_forward_error();
            if broker
                .fetch_sessions()
                .try_owner_miss_local_serve(session_id)
            {
                return None;
            }
            let mut out = BytesMut::new();
            put_fetch_empty_response(&mut out, api_version, 70, session_id);
            Some(out.freeze())
        }
        Ok(other) => {
            tracing::debug!(owner, ?other, session_id, "kafka fetch forward unexpected");
            broker.fetch_sessions().record_forward_error();
            if broker
                .fetch_sessions()
                .try_owner_miss_local_serve(session_id)
            {
                return None;
            }
            let mut out = BytesMut::new();
            put_fetch_empty_response(&mut out, api_version, 70, session_id);
            Some(out.freeze())
        }
        Err(e) => {
            tracing::debug!(owner, error = %e, session_id, "kafka fetch forward rpc failed");
            broker.fetch_sessions().record_forward_error();
            // Phase 147: prefer serve-from-mirror; Phase 138 promote when knobs say so.
            if broker
                .fetch_sessions()
                .try_owner_miss_local_serve(session_id)
            {
                return None;
            }
            let mut out = BytesMut::new();
            put_fetch_empty_response(&mut out, api_version, 70, session_id);
            Some(out.freeze())
        }
    }
}

/// Phase 138: best-effort fan-out of pending session mirror put/delete ops.
///
/// Does not fail the client path; fire-and-forget with per-RPC timeout.
pub async fn fanout_session_mirror_ops(broker: &Broker) {
    use crate::kafka::fetch_session::SessionMirrorOp;
    use bytes::Bytes;

    let ops = broker.fetch_sessions().drain_mirror_ops();
    if ops.is_empty() || broker.cluster_config().is_none() {
        return;
    }
    let self_id = broker.node_id();
    let peers: Vec<(u32, String)> = broker
        .live_brokers()
        .into_iter()
        .filter(|&id| id != self_id)
        .filter_map(|id| broker.broker_addr(id).map(|a| (id, a)))
        .collect();
    if peers.is_empty() {
        return;
    }

    for op in ops {
        match op {
            SessionMirrorOp::Put(session_id) => {
                let Some(snap) = broker.fetch_sessions().export_session_bytes(session_id) else {
                    continue;
                };
                let req = Request::FetchSessionMirrorPut {
                    session_id,
                    snapshot: Bytes::from(snap),
                };
                for (peer_id, addr) in &peers {
                    match inter_broker_rpc(broker, addr, &req).await {
                        Ok(Response::FetchSessionMirrorPut { error_code: 0 }) => {}
                        Ok(Response::FetchSessionMirrorPut { error_code }) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                error_code,
                                "session mirror put peer error"
                            );
                        }
                        Ok(other) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                ?other,
                                "session mirror put unexpected"
                            );
                        }
                        Err(e) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                error = %e,
                                "session mirror put rpc failed"
                            );
                        }
                    }
                }
            }
            SessionMirrorOp::Delete(session_id) => {
                let req = Request::FetchSessionMirrorDelete { session_id };
                for (peer_id, addr) in &peers {
                    match inter_broker_rpc(broker, addr, &req).await {
                        Ok(Response::FetchSessionMirrorDelete { error_code: 0 }) => {}
                        Ok(Response::FetchSessionMirrorDelete { error_code }) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                error_code,
                                "session mirror delete peer error"
                            );
                        }
                        Ok(other) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                ?other,
                                "session mirror delete unexpected"
                            );
                        }
                        Err(e) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                error = %e,
                                "session mirror delete rpc failed"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Phase 142: best-effort drain of pending IsrUpdate reports to the controller.
///
/// Fire-and-forget; does not block client/replica paths. On success aligns the
/// leader's local assignment generation to the controller response.
pub fn schedule_isr_update_reports(broker: &Arc<Broker>) {
    if !broker.has_pending_isr_reports() {
        return;
    }
    let b = Arc::clone(broker);
    tokio::spawn(async move {
        fanout_isr_update_reports(&b).await;
    });
}

/// Send queued IsrUpdate RPCs to the current controller (Phase 142).
pub async fn fanout_isr_update_reports(broker: &Broker) {
    let reports = broker.drain_pending_isr_reports();
    if reports.is_empty() {
        return;
    }
    let controller_id = broker.controller_id();
    if controller_id == broker.node_id() {
        // Became controller since enqueue; apply locally.
        for r in reports {
            let (err, gen) = broker.apply_leader_isr_update(
                &r.topic,
                r.partition,
                r.leader_id,
                r.leader_epoch,
                &r.isr,
                r.generation_hint,
            );
            if err == 0 {
                broker.align_assignment_generation(gen);
            }
        }
        return;
    }
    let Some(addr) = broker.broker_addr(controller_id) else {
        tracing::debug!(
            controller_id,
            "isr update: no controller addr; reports dropped"
        );
        return;
    };
    for r in reports {
        let req = Request::IsrUpdate {
            topic: r.topic.clone(),
            partition: r.partition,
            leader_id: r.leader_id,
            leader_epoch: r.leader_epoch,
            isr: r.isr.clone(),
            generation_hint: r.generation_hint,
        };
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::IsrUpdate {
                error_code: 0,
                generation,
            }) => {
                broker.align_assignment_generation(generation);
            }
            Ok(Response::IsrUpdate {
                error_code,
                generation: _,
            }) => {
                tracing::debug!(
                    topic = %r.topic,
                    partition = r.partition,
                    error_code,
                    "isr update rejected by controller"
                );
            }
            Ok(other) => {
                tracing::debug!(
                    topic = %r.topic,
                    partition = r.partition,
                    ?other,
                    "isr update unexpected response"
                );
            }
            Err(e) => {
                tracing::debug!(
                    topic = %r.topic,
                    partition = r.partition,
                    error = %e,
                    "isr update rpc failed"
                );
            }
        }
    }
}

/// Schedule [`fanout_session_mirror_ops`] after local Kafka Fetch session mutations.
///
/// Phase 139: **Deletes** flush immediately. **Puts** are single-flight debounced
/// by `mirror_put_min_interval_ms` (default 50; `0` = immediate after coalesce).
/// Does not block the client Fetch response path.
pub fn schedule_session_mirror_fanout(broker: &Arc<Broker>) {
    if broker.cluster_config().is_none() {
        return;
    }
    let sessions = broker.fetch_sessions();
    if !sessions.has_pending_mirror_ops() {
        return;
    }

    // Delete pending → flush now (no debounce wait).
    if sessions.has_pending_mirror_delete() {
        let b = Arc::clone(broker);
        tokio::spawn(async move {
            fanout_session_mirror_ops(b.as_ref()).await;
        });
        return;
    }

    let interval_ms = sessions.mirror_put_min_interval_ms();
    if interval_ms == 0 {
        let b = Arc::clone(broker);
        tokio::spawn(async move {
            fanout_session_mirror_ops(b.as_ref()).await;
        });
        return;
    }

    // Puts only: single-flight debounce. Further schedules are no-ops until flush.
    if !sessions.try_arm_mirror_put_debounce() {
        return;
    }
    let b = Arc::clone(broker);
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
        b.fetch_sessions().clear_mirror_put_debounce_armed();
        fanout_session_mirror_ops(b.as_ref()).await;
    });
}

/// Default per inter-broker RPC timeout: **5 seconds**.
///
/// Override with `VOLANT_INTER_BROKER_RPC_TIMEOUT_MS` (milliseconds). Values
/// are clamped to `[1ms, 10min]`; `0` is not “disable” — it becomes 1ms so a
/// deadline always exists.
pub const DEFAULT_INTER_BROKER_RPC_TIMEOUT_MS: u64 = 5_000;

/// Minimum clamp for env-derived timeouts (ms). `0` and empty-invalid still become this.
pub const MIN_INTER_BROKER_TIMEOUT_MS: u64 = 1;
/// Maximum clamp for per-RPC and fan-out budget env values (10 minutes).
pub const MAX_INTER_BROKER_TIMEOUT_MS: u64 = 600_000;

/// Default overall budget for DeleteRecords peer fan-out (journal note + push
/// + ReplicaDeleteRecords): **20 seconds** (≥ 3 × default 5s RPC + margin).
///
/// Override with `VOLANT_DELETE_RECORDS_FANOUT_BUDGET_MS` (milliseconds).
/// Values are clamped to `[1ms, 10min]`; `0` is not “disable”. When the env is
/// **unset**, the effective budget is
/// `max(DEFAULT, 3 * inter_broker_rpc_timeout_ms + 2000)` (also clamped to
/// the max) so a raised per-RPC timeout still leaves room for all three phases.
pub const DEFAULT_DELETE_RECORDS_FANOUT_BUDGET_MS: u64 = 20_000;

fn env_duration_ms(var: &str, default_ms: u64) -> Duration {
    let ms = std::env::var(var)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default_ms)
        .clamp(MIN_INTER_BROKER_TIMEOUT_MS, MAX_INTER_BROKER_TIMEOUT_MS);
    Duration::from_millis(ms)
}

/// Effective per-RPC timeout (default 5s; env `VOLANT_INTER_BROKER_RPC_TIMEOUT_MS`).
///
/// Clamped to [`MIN_INTER_BROKER_TIMEOUT_MS`]..=[`MAX_INTER_BROKER_TIMEOUT_MS`].
pub fn inter_broker_rpc_timeout() -> Duration {
    env_duration_ms(
        "VOLANT_INTER_BROKER_RPC_TIMEOUT_MS",
        DEFAULT_INTER_BROKER_RPC_TIMEOUT_MS,
    )
}

/// Effective DeleteRecords fan-out overall budget.
///
/// - Env `VOLANT_DELETE_RECORDS_FANOUT_BUDGET_MS` set → that value, clamped to
///   `[1ms, 10min]` (`0` is not disable).
/// - Else → `max(DEFAULT_DELETE_RECORDS_FANOUT_BUDGET_MS,
///   3 * inter_broker_rpc_timeout_ms + 2000)`, also clamped to the max.
pub fn delete_records_fanout_budget() -> Duration {
    if let Ok(s) = std::env::var("VOLANT_DELETE_RECORDS_FANOUT_BUDGET_MS") {
        if let Ok(ms) = s.parse::<u64>() {
            return Duration::from_millis(
                ms.clamp(MIN_INTER_BROKER_TIMEOUT_MS, MAX_INTER_BROKER_TIMEOUT_MS),
            );
        }
    }
    let rpc_ms = inter_broker_rpc_timeout().as_millis() as u64;
    let floor = (3u64.saturating_mul(rpc_ms)).saturating_add(2_000);
    let ms = DEFAULT_DELETE_RECORDS_FANOUT_BUDGET_MS
        .max(floor)
        .clamp(MIN_INTER_BROKER_TIMEOUT_MS, MAX_INTER_BROKER_TIMEOUT_MS);
    Duration::from_millis(ms)
}

/// Inter-broker RPC over a short-lived connection (plain TCP or optional TLS).
///
/// When the local broker has an auth token configured, sends Auth first.
/// When [`Broker::inter_broker_tls`] is set (and the `tls` feature is enabled),
/// the connection is upgraded to TLS before the RPC.
///
/// Bounded by [`inter_broker_rpc_timeout`] (default **5s**) so a black-holed
/// peer cannot stall client paths that await fan-out.
pub async fn inter_broker_rpc(broker: &Broker, addr: &str, req: &Request) -> Result<Response> {
    inter_broker_rpc_owned(
        addr,
        req,
        broker.auth_token(),
        broker.inter_broker_tls(),
    )
    .await
}

/// Owned-credentials RPC (Send-safe for parallel `JoinSet` fan-out).
async fn inter_broker_rpc_owned(
    addr: &str,
    req: &Request,
    auth_token: Option<String>,
    inter_broker_tls: Option<crate::broker::InterBrokerTls>,
) -> Result<Response> {
    let timeout = inter_broker_rpc_timeout();
    match tokio::time::timeout(
        timeout,
        inter_broker_rpc_inner(addr, req, auth_token, inter_broker_tls),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => Err(Error::Protocol(format!(
            "inter-broker rpc to {addr} timed out after {}ms",
            timeout.as_millis()
        ))),
    }
}

async fn inter_broker_rpc_inner(
    addr: &str,
    req: &Request,
    auth_token: Option<String>,
    inter_broker_tls: Option<crate::broker::InterBrokerTls>,
) -> Result<Response> {
    let tcp = TcpStream::connect(addr).await?;

    #[cfg(feature = "tls")]
    if let Some(tls_cfg) = inter_broker_tls {
        let mut stream = connect_inter_broker_tls(tcp, addr, &tls_cfg).await?;
        return inter_broker_rpc_on(&mut stream, req, auth_token).await;
    }

    #[cfg(not(feature = "tls"))]
    if inter_broker_tls.is_some() {
        return Err(Error::InvalidArgument(
            "inter-broker TLS configured but volant-broker was built without `--features tls`"
                .into(),
        ));
    }

    let mut stream = tcp;
    inter_broker_rpc_on(&mut stream, req, auth_token).await
}

async fn inter_broker_rpc_on<S>(
    stream: &mut S,
    req: &Request,
    auth_token: Option<String>,
) -> Result<Response>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if let Some(token) = auth_token {
        let auth = Request::Auth { token };
        let frame = pack_request(0, &auth)?;
        let mut out = BytesMut::new();
        encode_frame(&frame, &mut out)?;
        stream.write_all(&out).await?;
        let auth_resp = read_one_response(stream).await?;
        match auth_resp {
            Response::Auth { error_code } if error_code == 0 => {}
            Response::Auth { error_code } => {
                return Err(Error::Protocol(format!(
                    "inter-broker auth failed: error_code={error_code}"
                )));
            }
            Response::Error { code, message } => {
                return Err(Error::Protocol(format!(
                    "inter-broker auth error {code}: {message}"
                )));
            }
            other => {
                return Err(Error::Protocol(format!(
                    "unexpected inter-broker auth response: {other:?}"
                )));
            }
        }
    }

    let frame = pack_request(1, req)?;
    let mut out = BytesMut::new();
    encode_frame(&frame, &mut out)?;
    stream.write_all(&out).await?;
    read_one_response(stream).await
}

async fn read_one_response<S>(stream: &mut S) -> Result<Response>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(Error::Protocol("peer closed during rpc".into()));
        }
        if let Some(frame) = decode_frame(&mut buf)? {
            return volant_protocol::decode_response(frame.header.opcode, &frame.payload);
        }
    }
}

/// Build a TLS client stream for inter-broker RPC.
#[cfg(feature = "tls")]
async fn connect_inter_broker_tls(
    tcp: TcpStream,
    addr: &str,
    cfg: &crate::broker::InterBrokerTls,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use rustls::ClientConfig as RustlsClientConfig;
    use rustls::RootCertStore;
    use rustls::pki_types::ServerName;
    use tokio_rustls::TlsConnector;

    let _ = rustls::crypto::ring::default_provider().install_default();

    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);

    let builder = if cfg.insecure {
        RustlsClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
    } else {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(ca_path) = &cfg.ca_path {
            let file = File::open(ca_path).map_err(|e| {
                Error::InvalidArgument(format!("open inter-broker tls_ca {}: {e}", ca_path.display()))
            })?;
            let mut reader = BufReader::new(file);
            let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| Error::InvalidArgument(format!("parse inter-broker tls_ca PEM: {e}")))?;
            for cert in certs {
                roots
                    .add(cert)
                    .map_err(|e| Error::InvalidArgument(format!("add inter-broker tls_ca cert: {e}")))?;
            }
        }
        RustlsClientConfig::builder().with_root_certificates(roots)
    };

    // Phase 19: optional client cert for mTLS to peers.
    let rustls_config = match (&cfg.client_cert, &cfg.client_key) {
        (Some(cert_path), Some(key_path)) => {
            let certs = load_pem_certs(cert_path)?;
            let key = load_pem_key(key_path)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| Error::InvalidArgument(format!("inter-broker client cert: {e}")))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(Error::InvalidArgument(
                "inter-broker TLS client_cert and client_key must both be set or both unset"
                    .into(),
            ));
        }
    };

    let connector = TlsConnector::from(Arc::new(rustls_config));
    let server_name = ServerName::try_from(host.to_owned()).map_err(|e| {
        Error::InvalidArgument(format!("invalid inter-broker TLS server name '{host}': {e}"))
    })?;
    connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))
}

#[cfg(feature = "tls")]
fn load_pem_certs(path: &std::path::Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    use std::fs::File;
    use std::io::BufReader;
    let file = File::open(path).map_err(|e| {
        Error::InvalidArgument(format!("open cert {}: {e}", path.display()))
    })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::InvalidArgument(format!("parse cert PEM {}: {e}", path.display())))
}

#[cfg(feature = "tls")]
fn load_pem_key(path: &std::path::Path) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    use std::fs::File;
    use std::io::BufReader;
    let file = File::open(path).map_err(|e| {
        Error::InvalidArgument(format!("open key {}: {e}", path.display()))
    })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| Error::InvalidArgument(format!("parse key PEM {}: {e}", path.display())))?
        .ok_or_else(|| Error::InvalidArgument(format!("no private key in {}", path.display())))
}

#[cfg(feature = "tls")]
#[derive(Debug)]
struct NoCertVerifier;

#[cfg(feature = "tls")]
impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
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

/// Dispatch one framed request with connection auth / SCRAM state (plaintext + TLS).
pub async fn dispatch_with_auth(
    broker: &Arc<Broker>,
    frame: Frame,
    authenticated: &mut bool,
    principal: &mut Option<String>,
    scram_challenge: &mut Option<crate::scram::ScramChallenge>,
) -> Response {
    let req = match decode_request(frame.header.opcode, &frame.payload) {
        Ok(r) => r,
        Err(e) => {
            broker.metrics().record_error(ErrorCode::Protocol as u16);
            return Response::Error {
                code: ErrorCode::Protocol as u16,
                message: e.to_string(),
            };
        }
    };

    // Shared-token Auth (Phase 7).
    if let Request::Auth { token } = &req {
        let response = match broker.auth_token() {
            None => {
                // Auth disabled: accept any token as a no-op success.
                *authenticated = true;
                *principal = Some(broker.auth_principal_name());
                Response::Auth { error_code: 0 }
            }
            Some(expected) if expected == *token => {
                *authenticated = true;
                *principal = Some(broker.auth_principal_name());
                Response::Auth { error_code: 0 }
            }
            Some(_) => {
                *authenticated = false;
                *principal = None;
                broker
                    .metrics()
                    .record_error(ErrorCode::AuthenticationFailed as u16);
                Response::Auth {
                    error_code: ErrorCode::AuthenticationFailed as u16,
                }
            }
        };
        return response;
    }

    // SCRAM-SHA-256 (Phase 22) — allowed before authentication.
    if matches!(
        &req,
        Request::ScramFirst { .. } | Request::ScramFinal { .. }
    ) {
        return handle_scram(broker, req, authenticated, principal, scram_challenge);
    }

    // Bootstrap CreateScramUser when the store is empty (no auth yet).
    if matches!(&req, Request::CreateScramUser { .. }) && !broker.scram().has_users() {
        return dispatch_request_as(broker, req, principal.as_deref()).await;
    }

    let auth_required = broker.auth_required();
    if auth_required && !*authenticated {
        broker
            .metrics()
            .record_error(ErrorCode::AuthenticationRequired as u16);
        return Response::Error {
            code: ErrorCode::AuthenticationRequired as u16,
            message: "authentication required; send Auth or ScramFirst/ScramFinal first".into(),
        };
    }

    dispatch_request_as(broker, req, principal.as_deref()).await
}

fn handle_scram(
    broker: &Broker,
    req: Request,
    authenticated: &mut bool,
    principal: &mut Option<String>,
    scram_challenge: &mut Option<crate::scram::ScramChallenge>,
) -> Response {
    match req {
        Request::ScramFirst {
            username,
            client_nonce,
        } => match broker.scram().begin(&username, &client_nonce) {
            Ok((chal, salt, iterations, combined_nonce)) => {
                *scram_challenge = Some(chal);
                Response::ScramFirst {
                    error_code: 0,
                    combined_nonce,
                    salt: bytes::Bytes::from(salt),
                    iterations,
                }
            }
            Err(_) => {
                *scram_challenge = None;
                Response::ScramFirst {
                    error_code: ErrorCode::InvalidArg as u16,
                    combined_nonce: String::new(),
                    salt: bytes::Bytes::new(),
                    iterations: 0,
                }
            }
        },
        Request::ScramFinal {
            username,
            combined_nonce,
            client_proof,
        } => {
            let Some(chal) = scram_challenge.take() else {
                broker
                    .metrics()
                    .record_error(ErrorCode::AuthenticationFailed as u16);
                return Response::ScramFinal {
                    error_code: ErrorCode::AuthenticationFailed as u16,
                    server_signature: bytes::Bytes::new(),
                };
            };
            match broker
                .scram()
                .finish(&chal, &username, &combined_nonce, &client_proof)
            {
                Ok(server_sig) => {
                    *authenticated = true;
                    *principal = Some(username);
                    Response::ScramFinal {
                        error_code: 0,
                        server_signature: bytes::Bytes::from(server_sig),
                    }
                }
                Err(_) => {
                    *authenticated = false;
                    *principal = None;
                    broker
                        .metrics()
                        .record_error(ErrorCode::AuthenticationFailed as u16);
                    Response::ScramFinal {
                        error_code: ErrorCode::AuthenticationFailed as u16,
                        server_signature: bytes::Bytes::new(),
                    }
                }
            }
        }
        _ => Response::Error {
            code: ErrorCode::Protocol as u16,
            message: "internal scram dispatch error".into(),
        },
    }
}

/// Handle a decoded request (shared by plaintext and TLS accept paths).
pub async fn dispatch_request(broker: &Arc<Broker>, req: Request) -> Response {
    dispatch_request_as(broker, req, None).await
}

/// Dispatch with an optional connection principal for ACL checks (Phase 20).
pub async fn dispatch_request_as(
    broker: &Arc<Broker>,
    req: Request,
    principal: Option<&str>,
) -> Response {
    if let Some(denied) = authorize_request(broker, &req, principal) {
        broker
            .metrics()
            .record_error(ErrorCode::AuthorizationFailed as u16);
        return denied;
    }
    match handle_request(broker, req).await {
        Ok(resp) => {
            record_response_metrics(broker, &resp);
            resp
        }
        Err(e) => {
            let resp = map_error(e);
            record_response_metrics(broker, &resp);
            resp
        }
    }
}

fn deny(msg: impl Into<String>) -> Response {
    Response::Error {
        code: ErrorCode::AuthorizationFailed as u16,
        message: msg.into(),
    }
}

/// Return an AuthorizationFailed response if the principal may not run `req`.
fn authorize_request(
    broker: &Broker,
    req: &Request,
    principal: Option<&str>,
) -> Option<Response> {
    use crate::acl::{AclOperation, ResourceType, CLUSTER_RESOURCE};

    // Inter-broker traffic and auth handshakes are not ACL-gated.
    // TruncateJournal* is *not* in this list: the journal survives leadership
    // and is Cluster-Alter gated when ACLs are enabled (inter-broker auth
    // principal still allowed).
    match req {
        Request::ReplicaFetch { .. }
        | Request::HeartbeatBroker { .. }
        | Request::ClusterState { .. }
        | Request::ReplicaDeleteRecords { .. }
        | Request::ClusterBrokerConfig { .. }
        | Request::ClusterAclSnapshot { .. }
        | Request::TxnParticipantOpen { .. }
        | Request::TxnParticipantPrepare { .. }
        | Request::TxnParticipantComplete { .. }
        | Request::KafkaFetchForward { .. }
        | Request::KafkaTxnForward { .. }
        | Request::FetchSessionMirrorPut { .. }
        | Request::FetchSessionMirrorDelete { .. }
        | Request::IsrUpdate { .. }
        | Request::Auth { .. }
        | Request::ScramFirst { .. }
        | Request::ScramFinal { .. } => return None,
        _ => {}
    }

    if !broker.acls().is_enabled() {
        return None;
    }

    let acls = broker.acls();
    let check = |rt: ResourceType, resource: &str, op: AclOperation| -> bool {
        acls.authorize(principal, rt, resource, op)
    };

    let ok = match req {
        Request::Produce { topic, .. } => check(ResourceType::Topic, topic, AclOperation::Write),
        Request::Fetch { topic, .. } | Request::ListOffsets { topic, .. } => {
            check(ResourceType::Topic, topic, AclOperation::Read)
        }
        Request::CreateTopic { name, .. } => {
            check(ResourceType::Topic, name, AclOperation::Create)
        }
        Request::DeleteTopic { name } | Request::DeleteRecords { topic: name, .. } => {
            check(ResourceType::Topic, name, AclOperation::Delete)
        }
        Request::Metadata { topics } => {
            if topics.is_empty() {
                check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Describe)
            } else {
                topics
                    .iter()
                    .all(|t| check(ResourceType::Topic, t, AclOperation::Describe))
            }
        }
        Request::DescribeConfigs { topic } => {
            check(ResourceType::Topic, topic, AclOperation::Describe)
        }
        Request::AlterConfigs { topic, .. } | Request::CreatePartitions { topic, .. } => {
            check(ResourceType::Topic, topic, AclOperation::Alter)
        }
        Request::OffsetCommit { group_id, .. }
        | Request::OffsetFetch { group_id, .. }
        | Request::JoinGroup { group_id, .. }
        | Request::Heartbeat { group_id, .. }
        | Request::LeaveGroup { group_id, .. } => {
            check(ResourceType::Group, group_id, AclOperation::Read)
        }
        Request::DescribeGroup { group_id } => {
            check(ResourceType::Group, group_id, AclOperation::Describe)
        }
        Request::DeleteOffsets { group_id, .. } => {
            check(ResourceType::Group, group_id, AclOperation::Delete)
        }
        Request::ListGroups => {
            check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Describe)
        }
        Request::InitProducerId { .. }
        | Request::BeginTxn { .. }
        | Request::EndTxn { .. } => {
            check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Write)
        }
        Request::CreateAcls { .. } | Request::DeleteAcls { .. } => {
            check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Alter)
        }
        Request::ListAcls { .. } => {
            check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Describe)
        }
        Request::CreateScramUser { .. } | Request::DeleteScramUser { .. } => {
            check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Alter)
        }
        Request::ListScramUsers => {
            check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Describe)
        }
        // Journal RPCs survive leadership: require Cluster Alter, or allow the
        // configured inter-broker auth principal (token Auth) so fan-out works.
        Request::TruncateJournalNote { .. } | Request::TruncateJournalPush { .. } => {
            let ib = broker.auth_principal_name();
            principal.map(|p| p == ib.as_str()).unwrap_or(false)
                || check(ResourceType::Cluster, CLUSTER_RESOURCE, AclOperation::Alter)
        }
        Request::ReplicaFetch { .. }
        | Request::HeartbeatBroker { .. }
        | Request::ClusterState { .. }
        | Request::ReplicaDeleteRecords { .. }
        | Request::ClusterBrokerConfig { .. }
        | Request::ClusterAclSnapshot { .. }
        | Request::TxnParticipantOpen { .. }
        | Request::TxnParticipantPrepare { .. }
        | Request::TxnParticipantComplete { .. }
        | Request::KafkaFetchForward { .. }
        | Request::KafkaTxnForward { .. }
        | Request::FetchSessionMirrorPut { .. }
        | Request::FetchSessionMirrorDelete { .. }
        | Request::IsrUpdate { .. }
        | Request::Auth { .. }
        | Request::ScramFirst { .. }
        | Request::ScramFinal { .. } => true,
    };

    if ok {
        None
    } else {
        Some(deny(format!(
            "principal '{}' not authorized",
            principal.unwrap_or("")
        )))
    }
}

fn record_response_metrics(broker: &Broker, resp: &Response) {
    let m = broker.metrics();
    match resp {
        Response::Produce {
            count,
            error_code,
            ..
        } => {
            let ok = *error_code == 0;
            // Approximate bytes not available here; count messages only.
            m.record_produce(ok, u64::from(*count), 0);
            if !ok {
                m.record_error(*error_code);
            }
        }
        Response::Fetch {
            records,
            error_code,
            ..
        } => {
            let ok = *error_code == 0;
            let messages = records.len() as u64;
            let bytes: u64 = records.iter().map(|r| r.value.len() as u64).sum();
            m.record_fetch(ok, messages, bytes);
            if !ok {
                m.record_error(*error_code);
            }
        }
        Response::Error { code, .. } => {
            m.record_error(*code);
        }
        Response::CreateTopic { error_code, .. }
        | Response::DeleteTopic { error_code, .. }
        | Response::OffsetCommit { error_code }
        | Response::OffsetFetch { error_code, .. }
        | Response::JoinGroup { error_code, .. }
        | Response::Heartbeat { error_code }
        | Response::LeaveGroup { error_code }
        | Response::ReplicaFetch { error_code, .. }
        | Response::HeartbeatBroker { error_code, .. }
        | Response::ClusterState { error_code, .. }
        | Response::Auth { error_code }
        | Response::InitProducerId { error_code, .. }
        | Response::DescribeGroup { error_code, .. }
        | Response::ListGroups { error_code, .. }
        | Response::DeleteOffsets { error_code, .. }
        | Response::DescribeConfigs { error_code, .. }
        | Response::AlterConfigs { error_code, .. }
        | Response::DeleteRecords { error_code, .. }
        | Response::CreatePartitions { error_code, .. }
        | Response::ListOffsets { error_code, .. }
        | Response::BeginTxn { error_code }
        | Response::EndTxn { error_code, .. }
        | Response::CreateAcls { error_code }
        | Response::DeleteAcls { error_code, .. }
        | Response::ListAcls { error_code, .. }
        | Response::ScramFirst { error_code, .. }
        | Response::ScramFinal { error_code, .. }
        | Response::CreateScramUser { error_code }
        | Response::DeleteScramUser { error_code }
        | Response::ListScramUsers { error_code, .. }
        | Response::ReplicaDeleteRecords { error_code, .. }
        | Response::ClusterBrokerConfig { error_code, .. }
        | Response::ClusterAclSnapshot { error_code, .. }
        | Response::TxnParticipantOpen { error_code }
        | Response::TxnParticipantPrepare { error_code }
        | Response::TxnParticipantComplete { error_code }
        | Response::KafkaFetchForward { error_code, .. }
        | Response::KafkaTxnForward { error_code, .. }
        | Response::TruncateJournalNote { error_code, .. }
        | Response::TruncateJournalPush { error_code }
        | Response::FetchSessionMirrorPut { error_code }
        | Response::FetchSessionMirrorDelete { error_code }
        | Response::IsrUpdate { error_code, .. } => {
            if *error_code != 0 {
                m.record_error(*error_code);
            }
        }
        Response::Metadata { .. } => {}
    }
}

async fn handle_request(broker: &Arc<Broker>, req: Request) -> Result<Response> {
    match req {
        Request::Auth { .. } => {
            // Handled in dispatch_with_auth; should not reach here.
            Ok(Response::Auth { error_code: 0 })
        }
        Request::CreateTopic {
            name,
            partitions,
            configs,
        } => {
            if broker.cluster_config().is_some() && !broker.is_controller() {
                return Ok(Response::Error {
                    code: ErrorCode::NotController as u16,
                    message: format!(
                        "not controller; controller_id={}",
                        broker.controller_id()
                    ),
                });
            }
            let topic = TopicName::new(name.clone());
            match broker.create_topic_with_configs(topic, partitions, &configs) {
                Ok(id) => Ok(Response::CreateTopic {
                    topic_id: id.0,
                    name,
                    partitions,
                    error_code: 0,
                }),
                Err(e) => {
                    // Surface NotController-style messages.
                    if e.to_string().contains("not controller") {
                        Ok(Response::Error {
                            code: ErrorCode::NotController as u16,
                            message: e.to_string(),
                        })
                    } else {
                        Err(e)
                    }
                }
            }
        }
        Request::DeleteTopic { name } => {
            let topic = TopicName::new(name.clone());
            broker.delete_topic(&topic)?;
            Ok(Response::DeleteTopic {
                name,
                error_code: 0,
            })
        }
        Request::Metadata { topics } => {
            let filter: Option<Vec<TopicName>> = if topics.is_empty() {
                None
            } else {
                Some(topics.into_iter().map(TopicName::new).collect())
            };
            let snap = broker.metadata(filter.as_deref());
            Ok(Response::Metadata {
                brokers: snap
                    .brokers
                    .into_iter()
                    .map(|(node_id, host, port)| BrokerInfo {
                        node_id,
                        host,
                        port,
                    })
                    .collect(),
                topics: snap
                    .topics
                    .into_iter()
                    .map(|t| TopicInfo {
                        name: t.name.0,
                        topic_id: t.topic_id.0,
                        error_code: 0,
                        partitions: t
                            .partitions
                            .into_iter()
                            .map(|p| PartitionInfo {
                                partition_id: p.partition_id.0,
                                leader: p.leader,
                                hwm: p.hwm,
                                replicas: p.replicas,
                                isr: p.isr,
                                leader_epoch: p.leader_epoch,
                            })
                            .collect(),
                    })
                    .collect(),
            })
        }
        Request::Produce {
            topic,
            partition,
            acks,
            messages,
            producer_id,
            producer_epoch,
            base_sequence,
        } => {
            let span = info_span!("produce", topic = %topic, partition, msg_count = messages.len());
            async {
                let topic_name = TopicName::new(topic.clone());
                if messages.is_empty() {
                    return Err(Error::InvalidArgument("empty produce batch".into()));
                }

                let approx_bytes: u64 = messages.iter().map(|m| m.value.len() as u64).sum();
                let msg_count = messages.len() as u32;

                let pid = if partition < 0 {
                    let key = messages[0].key.as_deref();
                    broker.select_partition(&topic_name, key)?
                } else {
                    PartitionId(partition as u32)
                };

                // Leadership check early for clearer response.
                if broker.cluster_config().is_some()
                    && broker.topics_has_partition(&topic_name, pid)
                    && !broker.is_partition_leader(&topic_name, pid)
                {
                    return Ok(Response::Produce {
                        topic,
                        partition: pid.0,
                        base_offset: 0,
                        count: 0,
                        error_code: ErrorCode::NotLeaderForPartition as u16,
                    });
                }

                // Phase 18/86: transactional produce write-through; LSO holds until EndTxn.
                if producer_id != 0
                    && base_sequence >= 0
                    && broker.is_transactional_producer(producer_id)
                {
                    let mut msgs = Vec::with_capacity(messages.len());
                    for m in messages {
                        let timestamp_ms = if m.timestamp_ms < 0 {
                            None
                        } else {
                            Some(m.timestamp_ms)
                        };
                        msgs.push(Message {
                            key: m.key,
                            value: m.value,
                            timestamp_ms,
                            headers: m.headers,
                        });
                    }
                    match broker.buffer_txn_produce(
                        producer_id,
                        producer_epoch,
                        &topic,
                        pid.0,
                        base_sequence,
                        msgs,
                    ) {
                        crate::broker::IdempotentCheck::Reject { error_code } => {
                            return Ok(Response::Produce {
                                topic,
                                partition: pid.0,
                                base_offset: 0,
                                count: 0,
                                error_code,
                            });
                        }
                        crate::broker::IdempotentCheck::Duplicate {
                            base_offset,
                            count,
                        } => {
                            return Ok(Response::Produce {
                                topic,
                                partition: pid.0,
                                base_offset,
                                count,
                                error_code: 0,
                            });
                        }
                        crate::broker::IdempotentCheck::Accept { base_offset } => {
                            return Ok(Response::Produce {
                                topic,
                                partition: pid.0,
                                base_offset,
                                count: msg_count,
                                error_code: 0,
                            });
                        }
                    }
                }

                // Idempotent de-dupe / sequence gate (Phase 10).
                match broker.check_idempotent_produce(
                    producer_id,
                    producer_epoch,
                    &topic,
                    pid.0,
                    base_sequence,
                    msg_count,
                ) {
                    crate::broker::IdempotentCheck::Reject { error_code } => {
                        return Ok(Response::Produce {
                            topic,
                            partition: pid.0,
                            base_offset: 0,
                            count: 0,
                            error_code,
                        });
                    }
                    crate::broker::IdempotentCheck::Duplicate {
                        base_offset,
                        count,
                    } => {
                        return Ok(Response::Produce {
                            topic,
                            partition: pid.0,
                            base_offset,
                            count,
                            error_code: 0,
                        });
                    }
                    crate::broker::IdempotentCheck::Accept { .. } => {}
                }

                let mut batch = MessageBatch::default();
                for m in messages {
                    let timestamp_ms = if m.timestamp_ms < 0 {
                        None
                    } else {
                        Some(m.timestamp_ms)
                    };
                    batch.messages.push(Message {
                        key: m.key,
                        value: m.value,
                        timestamp_ms,
                        headers: m.headers,
                    });
                }

                // Append; for acks=all enforce min_isr and wait for HWM asynchronously.
                let (records, error_code) =
                    broker.produce_with_acks(&topic_name, pid, batch, acks, None)?;

                if error_code == ErrorCode::NotLeaderForPartition as u16
                    || error_code == ErrorCode::NotEnoughReplicas as u16
                {
                    return Ok(Response::Produce {
                        topic,
                        partition: pid.0,
                        base_offset: 0,
                        count: 0,
                        error_code,
                    });
                }

                let base_offset = records.first().map(|r| r.offset.raw()).unwrap_or(0);
                let count = records.len() as u32;

                let mut final_error = error_code;
                if acks == 255 && broker.cluster_config().is_some() && count > 0 {
                    let target = base_offset + u64::from(count);
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                    loop {
                        let hwm = broker.committed_hwm(&topic_name, pid).unwrap_or(0);
                        if hwm >= target {
                            break;
                        }
                        if tokio::time::Instant::now() >= deadline {
                            final_error = ErrorCode::Timeout as u16;
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }

                if acks != 0 {
                    broker.flush(&topic_name, pid)?;
                }

                if final_error == 0 {
                    broker.metrics().add_produce_bytes(approx_bytes);
                    broker.record_idempotent_produce(
                        producer_id,
                        producer_epoch,
                        &topic,
                        pid.0,
                        base_sequence,
                        count,
                        base_offset,
                    );
                }

                Ok(Response::Produce {
                    topic,
                    partition: pid.0,
                    base_offset,
                    count,
                    error_code: final_error,
                })
            }
            .instrument(span)
            .await
        }

        Request::Fetch {
            topic,
            partition,
            from_offset,
            max_messages,
            max_bytes: _,
            max_wait_ms,
        } => {
            let span = info_span!("fetch", topic = %topic, partition, from_offset);
            async {
                let topic_name = TopicName::new(topic.clone());
                let pid = PartitionId(partition);
                let from = Offset::new(from_offset);
                let max = max_messages as usize;

                // In multi-node, prefer leader for client fetch (followers may have data
                // but HWM is authoritative on leader). Still allow fetch on any replica
                // capped at local committed_hwm.
                let mut records = broker.fetch(&topic_name, pid, from, max)?;
                if records.is_empty() && max_wait_ms > 0 {
                    let deadline =
                        tokio::time::Instant::now() + Duration::from_millis(u64::from(max_wait_ms));
                    while records.is_empty() && tokio::time::Instant::now() < deadline {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        records = broker.fetch(&topic_name, pid, from, max)?;
                    }
                }

                let hwm = broker.high_watermark(&topic_name, pid).unwrap_or(0);
                let wire_records = records
                    .into_iter()
                    .map(|r| FetchRecord {
                        offset: r.offset.raw(),
                        timestamp_ms: r.timestamp_ms,
                        key: r.key,
                        value: r.value,
                        headers: r.headers,
                    })
                    .collect();

                Ok(Response::Fetch {
                    topic,
                    partition,
                    high_watermark: hwm,
                    error_code: 0,
                    records: wire_records,
                })
            }
            .instrument(span)
            .await
        }
        Request::JoinGroup {
            group_id,
            member_id,
            session_timeout_ms,
            topics,
            group_instance_id,
        } => {
            let result = broker.groups().join(
                &group_id,
                &member_id,
                session_timeout_ms,
                topics,
                &group_instance_id,
                |t| broker.partition_count_opt(t),
            )?;
            Ok(Response::JoinGroup {
                error_code: result.error_code,
                generation: result.generation,
                member_id: result.member_id,
                assignment: result
                    .assignment
                    .into_iter()
                    .map(|(topic, partition)| Assignment { topic, partition })
                    .collect(),
                revoked: result
                    .revoked
                    .into_iter()
                    .map(|(topic, partition)| Assignment { topic, partition })
                    .collect(),
            })
        }
        Request::Heartbeat {
            group_id,
            member_id,
            generation,
        } => {
            let result = broker
                .groups()
                .heartbeat(&group_id, &member_id, generation);
            Ok(Response::Heartbeat {
                error_code: result.error_code,
            })
        }
        Request::LeaveGroup {
            group_id,
            member_id,
        } => {
            let result = broker.groups().leave(&group_id, &member_id, |t| {
                broker.partition_count_opt(t)
            });
            Ok(Response::LeaveGroup {
                error_code: result.error_code,
            })
        }
        Request::OffsetCommit {
            group_id,
            member_id,
            generation,
            entries,
        } => {
            let wire: Vec<(String, u32, u64, String)> = entries
                .into_iter()
                .map(|e| (e.topic, e.partition, e.offset, e.metadata))
                .collect();
            let result = broker
                .groups()
                .commit_offsets(&group_id, &member_id, generation, &wire)?;
            Ok(Response::OffsetCommit {
                error_code: result.error_code,
            })
        }
        Request::OffsetFetch { group_id, entries } => {
            let wire: Vec<(String, u32)> = entries
                .into_iter()
                .map(|e| (e.topic, e.partition))
                .collect();
            let result = broker.groups().fetch_offsets(&group_id, &wire)?;
            Ok(Response::OffsetFetch {
                error_code: result.error_code,
                entries: result
                    .entries
                    .into_iter()
                    .map(|e| OffsetFetchEntry {
                        topic: e.topic,
                        partition: e.partition,
                        offset: e.offset,
                        metadata: e.metadata,
                    })
                    .collect(),
            })
        }
        Request::ReplicaFetch {
            topic,
            partition,
            from_offset,
            max_bytes,
            replica_id,
        } => {
            let (error_code, high_watermark, leader_epoch, records) = broker
                .handle_replica_fetch(&topic, partition, from_offset, max_bytes, replica_id)?;
            // Phase 142: best-effort leader→controller ISR report after local reconcile.
            if broker.has_pending_isr_reports() {
                schedule_isr_update_reports(broker);
            }
            Ok(Response::ReplicaFetch {
                error_code,
                topic,
                partition,
                high_watermark,
                leader_epoch,
                records,
            })
        }
        Request::HeartbeatBroker {
            broker_id,
            controller_id_known,
            generation,
            applied_config_generation,
            applied_acl_generation,
            applied_journal_generation,
        } => {
            let (error_code, controller_id, generation, alive_brokers) =
                broker.handle_heartbeat_broker(broker_id, controller_id_known, generation);
            // Phase 117 + 136: if we are controller and peer lags on ACL/config
            // gens, re-push SoT state (covers offline miss + rejoin).
            // Phase 136: schedule async (single-flight + min-interval) so the
            // HeartbeatBroker response is not blocked on config/ACL RPCs.
            if error_code == 0
                && broker.is_controller()
                && broker_id != broker.node_id()
            {
                let (need_cfg, need_acl) =
                    broker.peer_admin_gens_lag(applied_config_generation, applied_acl_generation);
                if need_cfg || need_acl {
                    if let Some(addr) = broker.broker_addr(broker_id) {
                        schedule_catch_up_peer_admin_state(
                            Arc::clone(broker),
                            broker_id,
                            addr,
                            applied_config_generation,
                            applied_acl_generation,
                        );
                    }
                }
            }
            // Phase 131 + 132: any node with a newer truncate journal re-pushes
            // to a lagging peer (multi-controller; not controller-gated).
            // Phase 132: schedule async (single-flight + min-interval) so the
            // HeartbeatBroker response is not blocked on TruncateJournalPush.
            if error_code == 0
                && broker_id != broker.node_id()
                && broker.peer_journal_gen_lags(applied_journal_generation)
            {
                if let Some(addr) = broker.broker_addr(broker_id) {
                    schedule_catch_up_peer_truncate_journal(
                        Arc::clone(broker),
                        broker_id,
                        addr,
                        applied_journal_generation,
                    );
                }
            }
            Ok(Response::HeartbeatBroker {
                error_code,
                controller_id,
                generation,
                alive_brokers,
            })
        }
        Request::ClusterState { known_generation: _ } => {
            let (error_code, generation, controller_id, topics) =
                broker.cluster_state_snapshot();
            Ok(Response::ClusterState {
                error_code,
                generation,
                controller_id,
                topics,
            })
        }
        // Phase 113 PR1: decode + dispatch stubs (real fan-out in PR2–PR4).
        Request::ReplicaDeleteRecords {
            topic,
            partition,
            before_offset,
            leader_epoch,
        } => {
            let (error_code, low_watermark) = broker.handle_replica_delete_records(
                &topic,
                partition,
                before_offset,
                leader_epoch,
            );
            Ok(Response::ReplicaDeleteRecords {
                error_code,
                low_watermark,
            })
        }
        Request::ClusterBrokerConfig {
            generation,
            entries,
        } => {
            let (error_code, applied_generation) =
                broker.handle_cluster_broker_config(generation, &entries);
            Ok(Response::ClusterBrokerConfig {
                error_code,
                applied_generation,
            })
        }
        Request::ClusterAclSnapshot {
            generation,
            snapshot,
        } => {
            let (error_code, applied_generation) =
                broker.handle_cluster_acl_snapshot(generation, &snapshot);
            Ok(Response::ClusterAclSnapshot {
                error_code,
                applied_generation,
            })
        }
        Request::TruncateJournalNote {
            topic,
            partition,
            before_offset,
            leader_epoch,
        } => {
            let (error_code, generation) = broker.handle_truncate_journal_note(
                &topic,
                partition,
                before_offset,
                leader_epoch,
            );
            Ok(Response::TruncateJournalNote {
                error_code,
                generation,
            })
        }
        Request::TruncateJournalPush {
            generation,
            snapshot,
        } => {
            let error_code = broker.handle_truncate_journal_push(generation, &snapshot);
            Ok(Response::TruncateJournalPush { error_code })
        }
        Request::InitProducerId { transactional_id } => {
            let (producer_id, epoch) =
                broker.init_producer_id_with_txn(&transactional_id);
            // Phase 120: register Init owner on peers (no open).
            if !transactional_id.is_empty() {
                let fanout = broker.txn_2pc_init_register_fanout(
                    &transactional_id,
                    producer_id,
                    epoch,
                    false,
                );
                let _ = run_txn_2pc_fanout(broker, &fanout).await;
            }
            Ok(Response::InitProducerId {
                producer_id,
                epoch,
                error_code: 0,
            })
        }
        Request::BeginTxn {
            producer_id,
            producer_epoch,
        } => {
            let error_code = broker.begin_txn(producer_id, producer_epoch);
            if error_code == 0 {
                let fanout = broker.txn_2pc_open_fanout(producer_id);
                let _ = run_txn_2pc_fanout(broker, &fanout).await;
            }
            Ok(Response::BeginTxn { error_code })
        }
        Request::EndTxn {
            producer_id,
            producer_epoch,
            committed,
            offsets,
        } => {
            let offset_tuples: Vec<_> = offsets
                .into_iter()
                .map(|o| {
                    (
                        o.group_id,
                        o.topic,
                        o.partition,
                        o.offset,
                        o.metadata,
                    )
                })
                .collect();
            let (mut error_code, results, fanout) =
                broker.end_txn(producer_id, producer_epoch, committed, &offset_tuples)?;
            if error_code == 0 {
                match &fanout {
                    Txn2pcFanout::Prepare {
                        transactional_id, ..
                    } => {
                        if !run_txn_2pc_fanout(broker, &fanout).await {
                            broker.rollback_local_prepare(transactional_id);
                            error_code = ErrorCode::Unknown as u16;
                        }
                    }
                    Txn2pcFanout::None => {}
                    _ => {
                        let _ = run_txn_2pc_fanout(broker, &fanout).await;
                    }
                }
            }
            Ok(Response::EndTxn {
                error_code,
                results: results
                    .into_iter()
                    .map(|r| volant_protocol::TxnProduceResult {
                        topic: r.topic,
                        partition: r.partition,
                        base_offset: r.base_offset,
                        count: r.count,
                    })
                    .collect(),
            })
        }
        Request::TxnParticipantOpen {
            transactional_id,
            producer_id,
            producer_epoch,
            enable_2pc,
            coordinator_node_id,
            install_open,
        } => {
            let error_code = broker.handle_txn_participant_open(
                &transactional_id,
                producer_id,
                producer_epoch,
                enable_2pc,
                coordinator_node_id,
                install_open,
            );
            Ok(Response::TxnParticipantOpen { error_code })
        }
        Request::TxnParticipantPrepare {
            transactional_id,
            producer_id,
            producer_epoch,
            commit,
        } => {
            let error_code = broker.handle_txn_participant_prepare(
                &transactional_id,
                producer_id,
                producer_epoch,
                commit,
            );
            Ok(Response::TxnParticipantPrepare { error_code })
        }
        Request::TxnParticipantComplete {
            transactional_id,
            producer_id,
            producer_epoch,
            commit,
        } => {
            let error_code = broker.handle_txn_participant_complete(
                &transactional_id,
                producer_id,
                producer_epoch,
                commit,
            );
            Ok(Response::TxnParticipantComplete { error_code })
        }
        Request::KafkaFetchForward {
            api_version,
            principal,
            body,
        } => {
            // Phase 119: owner-side local encode only (never re-forward).
            let mut src = body;
            let mut out = BytesMut::new();
            crate::kafka::produce_fetch::encode_fetch(
                broker,
                &mut src,
                &mut out,
                api_version,
                &principal,
            );
            // Phase 138: mirror session mutations from owner-side encode.
            schedule_session_mirror_fanout(broker);
            Ok(Response::KafkaFetchForward {
                error_code: 0,
                body: out.freeze(),
            })
        }
        Request::FetchSessionMirrorPut {
            session_id: _,
            snapshot,
        } => {
            // Phase 138: install foreign mirror snapshot (best-effort SoT copy).
            let error_code = match broker.fetch_sessions().apply_mirror_put(&snapshot) {
                Ok(()) => 0,
                Err(e) => {
                    tracing::debug!(error = %e, "fetch session mirror put apply failed");
                    ErrorCode::InvalidArg as u16
                }
            };
            Ok(Response::FetchSessionMirrorPut { error_code })
        }
        Request::FetchSessionMirrorDelete { session_id } => {
            broker.fetch_sessions().apply_mirror_delete(session_id);
            Ok(Response::FetchSessionMirrorDelete { error_code: 0 })
        }
        Request::IsrUpdate {
            topic,
            partition,
            leader_id,
            leader_epoch,
            isr,
            generation_hint,
        } => {
            let (error_code, generation) = broker.apply_leader_isr_update(
                &topic,
                partition,
                leader_id,
                leader_epoch,
                &isr,
                generation_hint,
            );
            Ok(Response::IsrUpdate {
                error_code,
                generation,
            })
        }
        Request::KafkaTxnForward {
            api_key,
            api_version,
            principal,
            body,
        } => {
            // Phase 120/122: coordinator-side local encode only (never re-forward).
            // api_key: 25 AddOffsetsToTxn, 26 EndTxn (+ 2PC fan-out), 28 TxnOffsetCommit.
            let mut src = body;
            let mut out = BytesMut::new();
            match api_key {
                25 => {
                    crate::kafka::txn::encode_add_offsets_to_txn(
                        broker,
                        &mut src,
                        &mut out,
                        api_version,
                        &principal,
                    );
                }
                26 => {
                    if let Some(fanout) = crate::kafka::txn::encode_end_txn(
                        broker,
                        &mut src,
                        &mut out,
                        api_version,
                        &principal,
                    ) {
                        use crate::broker::Txn2pcFanout;
                        match &fanout {
                            Txn2pcFanout::Prepare {
                                transactional_id, ..
                            } => {
                                if !run_txn_2pc_fanout(broker, &fanout).await {
                                    broker.rollback_local_prepare(transactional_id);
                                    out.clear();
                                    put_end_txn_error_response(&mut out, api_version, -1); // Unknown
                                }
                            }
                            Txn2pcFanout::None => {}
                            _ => {
                                let _ = run_txn_2pc_fanout(broker, &fanout).await;
                            }
                        }
                    }
                }
                28 => {
                    crate::kafka::txn::encode_txn_offset_commit(
                        broker,
                        &mut src,
                        &mut out,
                        api_version,
                        &principal,
                    );
                }
                _ => {
                    return Ok(Response::KafkaTxnForward {
                        error_code: ErrorCode::InvalidArg as u16,
                        body: Bytes::new(),
                    });
                }
            }
            Ok(Response::KafkaTxnForward {
                error_code: 0,
                body: out.freeze(),
            })
        }
        Request::DescribeGroup { group_id } => {
            match broker.groups().describe_group(&group_id) {
                Some(desc) => {
                    let members = desc
                        .members
                        .into_iter()
                        .map(|m| volant_protocol::GroupMemberInfo {
                            member_id: m.member_id,
                            topics: m.topics,
                            assignment: m
                                .assignment
                                .into_iter()
                                .map(|(topic, partition)| volant_protocol::Assignment {
                                    topic,
                                    partition,
                                })
                                .collect(),
                        })
                        .collect();
                    Ok(Response::DescribeGroup {
                        error_code: 0,
                        group_id: desc.group_id,
                        generation: desc.generation,
                        members,
                    })
                }
                None => Ok(Response::DescribeGroup {
                    error_code: ErrorCode::NotFound as u16,
                    group_id,
                    generation: 0,
                    members: vec![],
                }),
            }
        }
        Request::ListGroups => {
            let groups = broker
                .groups()
                .list_groups()
                .into_iter()
                .map(|g| volant_protocol::GroupListing {
                    group_id: g.group_id,
                    state: if g.stable {
                        volant_protocol::GroupState::Stable
                    } else {
                        volant_protocol::GroupState::Empty
                    },
                    member_count: g.member_count,
                    generation: g.generation,
                })
                .collect();
            Ok(Response::ListGroups {
                error_code: 0,
                groups,
            })
        }
        Request::DeleteOffsets { group_id, entries } => {
            let pairs: Vec<(String, u32)> = entries
                .into_iter()
                .map(|e| (e.topic, e.partition))
                .collect();
            let deleted_count = broker.groups().delete_offsets(&group_id, &pairs)?;
            Ok(Response::DeleteOffsets {
                error_code: 0,
                deleted_count,
            })
        }
        Request::DescribeConfigs { topic } => match broker.describe_configs(&topic) {
            Ok((topic_id, partition_count, cfg)) => Ok(Response::DescribeConfigs {
                error_code: 0,
                topic,
                topic_id,
                partition_count,
                configs: cfg.to_entries(),
            }),
            Err(Error::NotFound(_)) => Ok(Response::DescribeConfigs {
                error_code: ErrorCode::NotFound as u16,
                topic,
                topic_id: 0,
                partition_count: 0,
                configs: vec![],
            }),
            Err(e) => Err(e),
        },
        Request::AlterConfigs { topic, configs } => match broker.alter_configs(&topic, &configs) {
            Ok(_) => Ok(Response::AlterConfigs {
                error_code: 0,
                topic,
            }),
            Err(Error::NotFound(_)) => Ok(Response::AlterConfigs {
                error_code: ErrorCode::NotFound as u16,
                topic,
            }),
            Err(e) => Err(e),
        },
        Request::DeleteRecords {
            topic,
            partition,
            before_offset,
            wait_majority,
        } => match broker.delete_records(&topic, partition, before_offset) {
            Ok((low_watermark, error_code)) => {
                // Phase 113/129/130/135/137: fan-out after local success.
                // Budget is enforced inside fanout_delete_records (default 20s
                // overall; 5s per inter-broker RPC).
                // Fan-out journal note + ReplicaDeleteRecords + outbox at the
                // **achieved** low_watermark (whole-segment clamp), not the
                // client-requested before_offset — peers/journal must not be
                // told a watermark the leader has not reached.
                //
                // Default (wait off): always return local error_code (0 on
                // success) even if journal majority fails. Wait on: surface
                // NotEnoughReplicas (15) when majority fails; low_watermark
                // remains the achieved local low (no rollback).
                // Phase 137: per-request wait_majority trailer (0=broker, 1/2 force).
                let mut err = error_code;
                if error_code == 0 {
                    let fan =
                        fanout_delete_records(broker, &topic, partition, low_watermark).await;
                    if broker.effective_delete_records_wait_majority(wait_majority) {
                        if fan.majority_ok {
                            broker.note_delete_records_majority_wait_success();
                        } else {
                            err = ErrorCode::NotEnoughReplicas as u16;
                            broker.note_delete_records_majority_wait_fail();
                        }
                    }
                }
                Ok(Response::DeleteRecords {
                    error_code: err,
                    topic,
                    partition,
                    low_watermark,
                })
            }
            Err(Error::NotFound(_)) => Ok(Response::DeleteRecords {
                error_code: ErrorCode::NotFound as u16,
                topic,
                partition,
                low_watermark: 0,
            }),
            Err(e) => Err(e),
        },
        Request::CreatePartitions {
            topic,
            total_count,
        } => match broker.create_partitions(&topic, total_count) {
            Ok(partitions) => Ok(Response::CreatePartitions {
                error_code: 0,
                topic,
                partitions,
            }),
            Err(Error::NotFound(_)) => Ok(Response::CreatePartitions {
                error_code: ErrorCode::NotFound as u16,
                topic,
                partitions: 0,
            }),
            Err(Error::InvalidArgument(m)) if m.starts_with("not controller") => {
                Ok(Response::CreatePartitions {
                    error_code: ErrorCode::NotController as u16,
                    topic,
                    partitions: 0,
                })
            }
            Err(Error::InvalidArgument(_)) => Ok(Response::CreatePartitions {
                error_code: ErrorCode::InvalidArg as u16,
                topic,
                partitions: 0,
            }),
            Err(e) => Err(e),
        },
        Request::ListOffsets { topic, partitions } => {
            match broker.list_offsets(&topic, &partitions) {
                Ok(entries) => Ok(Response::ListOffsets {
                    error_code: 0,
                    topic,
                    entries: entries
                        .into_iter()
                        .map(|(partition, earliest, latest)| {
                            volant_protocol::OffsetListing {
                                partition,
                                earliest,
                                latest,
                            }
                        })
                        .collect(),
                }),
                Err(Error::NotFound(_)) => Ok(Response::ListOffsets {
                    error_code: ErrorCode::NotFound as u16,
                    topic,
                    entries: vec![],
                }),
                Err(e) => Err(e),
            }
        }
        Request::CreateAcls { entries } => {
            match wire_to_acl_entries(&entries) {
                Ok(parsed) => match broker.create_acls_admin(parsed) {
                    Ok(gen) => {
                        if let Some(g) = gen {
                            fanout_cluster_acl_snapshot(broker, g).await;
                        }
                        Ok(Response::CreateAcls { error_code: 0 })
                    }
                    Err(Error::InvalidArgument(m)) if m.starts_with("not controller") => {
                        Ok(Response::CreateAcls {
                            error_code: ErrorCode::NotController as u16,
                        })
                    }
                    Err(_) => Ok(Response::CreateAcls {
                        error_code: ErrorCode::Storage as u16,
                    }),
                },
                Err(_) => Ok(Response::CreateAcls {
                    error_code: ErrorCode::InvalidArg as u16,
                }),
            }
        }
        Request::DeleteAcls { entries } => {
            match wire_to_acl_entries(&entries) {
                Ok(parsed) => match broker.delete_acls_admin(&parsed) {
                    Ok((removed, gen)) => {
                        if let Some(g) = gen {
                            fanout_cluster_acl_snapshot(broker, g).await;
                        }
                        Ok(Response::DeleteAcls {
                            error_code: 0,
                            removed: removed as u32,
                        })
                    }
                    Err(Error::InvalidArgument(m)) if m.starts_with("not controller") => {
                        Ok(Response::DeleteAcls {
                            error_code: ErrorCode::NotController as u16,
                            removed: 0,
                        })
                    }
                    Err(_) => Ok(Response::DeleteAcls {
                        error_code: ErrorCode::Storage as u16,
                        removed: 0,
                    }),
                },
                Err(_msg) => Ok(Response::DeleteAcls {
                    error_code: ErrorCode::InvalidArg as u16,
                    removed: 0,
                }),
            }
        }
        Request::ListAcls {
            principal,
            resource_type,
            resource,
        } => {
            let rt = if resource_type == 255 {
                None
            } else {
                crate::acl::ResourceType::from_u8(resource_type)
            };
            if resource_type != 255 && rt.is_none() {
                return Ok(Response::ListAcls {
                    error_code: ErrorCode::InvalidArg as u16,
                    entries: vec![],
                });
            }
            let p = if principal.is_empty() {
                None
            } else {
                Some(principal.as_str())
            };
            let r = if resource.is_empty() {
                None
            } else {
                Some(resource.as_str())
            };
            let entries = broker
                .acls()
                .list(p, rt, r)
                .into_iter()
                .map(acl_entry_to_wire)
                .collect();
            Ok(Response::ListAcls {
                error_code: 0,
                entries,
            })
        }
        Request::ScramFirst { .. } | Request::ScramFinal { .. } => {
            // Handled in dispatch_with_auth before this path.
            Ok(Response::Error {
                code: ErrorCode::Protocol as u16,
                message: "scram must be handled on the connection auth path".into(),
            })
        }
        Request::CreateScramUser {
            username,
            password,
            iterations,
        } => match broker.scram().upsert_user(&username, &password, iterations) {
            Ok(()) => Ok(Response::CreateScramUser { error_code: 0 }),
            Err(Error::InvalidArgument(_)) => Ok(Response::CreateScramUser {
                error_code: ErrorCode::InvalidArg as u16,
            }),
            Err(_) => Ok(Response::CreateScramUser {
                error_code: ErrorCode::Storage as u16,
            }),
        },
        Request::DeleteScramUser { username } => match broker.scram().delete_user(&username) {
            Ok(true) => Ok(Response::DeleteScramUser { error_code: 0 }),
            Ok(false) => Ok(Response::DeleteScramUser {
                error_code: ErrorCode::NotFound as u16,
            }),
            Err(_) => Ok(Response::DeleteScramUser {
                error_code: ErrorCode::Storage as u16,
            }),
        },
        Request::ListScramUsers => Ok(Response::ListScramUsers {
            error_code: 0,
            usernames: broker.scram().list_usernames(),
        }),
    }
}

fn wire_to_acl_entries(
    entries: &[volant_protocol::AclBinding],
) -> std::result::Result<Vec<crate::acl::AclEntry>, String> {
    let mut out = Vec::with_capacity(entries.len());
    for e in entries {
        let resource_type = crate::acl::ResourceType::from_u8(e.resource_type)
            .ok_or_else(|| format!("invalid resource_type {}", e.resource_type))?;
        let operation = crate::acl::AclOperation::from_u8(e.operation)
            .ok_or_else(|| format!("invalid operation {}", e.operation))?;
        let permission = crate::acl::AclPermission::from_u8(e.permission)
            .ok_or_else(|| format!("invalid permission {}", e.permission))?;
        if e.principal.is_empty() {
            return Err("empty principal".into());
        }
        if e.resource.is_empty() {
            return Err("empty resource".into());
        }
        out.push(crate::acl::AclEntry {
            principal: e.principal.clone(),
            resource_type,
            resource: e.resource.clone(),
            operation,
            permission,
        });
    }
    Ok(out)
}

fn acl_entry_to_wire(e: crate::acl::AclEntry) -> volant_protocol::AclBinding {
    volant_protocol::AclBinding {
        principal: e.principal,
        resource_type: e.resource_type.as_u8(),
        resource: e.resource,
        operation: e.operation.as_u8(),
        permission: e.permission.as_u8(),
    }
}

fn map_error(e: Error) -> Response {
    let (code, message) = match &e {
        Error::NotFound(m) => (ErrorCode::NotFound as u16, m.clone()),
        Error::InvalidArgument(m) => (ErrorCode::InvalidArg as u16, m.clone()),
        Error::Storage(m) => (ErrorCode::Storage as u16, m.clone()),
        Error::Protocol(m) => (ErrorCode::Protocol as u16, m.clone()),
        Error::Io(err) => (ErrorCode::Io as u16, err.to_string()),
        Error::NotImplemented(m) => (ErrorCode::Unsupported as u16, (*m).to_string()),
    };
    Response::Error { code, message }
}
