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
use volant_core::{Error, Message, MessageBatch, Offset, PartitionId, Result, TopicName};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_request, pack_response, Assignment, BrokerInfo, ErrorCode, FetchRecord,
    Frame, OffsetFetchEntry, PartitionInfo, Request, Response, TopicInfo,
};

use crate::broker::{Broker, Txn2pcFanout};
use crate::metrics::Metrics;
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
    // Phase 95: process-local fetch session gauges/counters (cheap stretch).
    let sessions = broker.fetch_sessions();
    text.push_str("# HELP volant_fetch_sessions_active Live process-local fetch sessions\n");
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
                        }
                    }
                }
            }));
        }

        // Heartbeat to controller (non-controller brokers).
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
                            if let Err(e) = heartbeat_to_controller(&b).await {
                                debug!(error = %e, "controller heartbeat failed");
                            }
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

async fn heartbeat_to_controller(broker: &Broker) -> Result<()> {
    let controller = broker.controller_id();
    // Even the controller heartbeats to itself locally.
    if controller == broker.node_id() {
        let _ = broker.handle_heartbeat_broker(broker.node_id(), controller, broker.generation());
        return Ok(());
    }
    let Some(addr) = broker.broker_addr(controller) else {
        return Ok(());
    };
    let req = Request::HeartbeatBroker {
        broker_id: broker.node_id(),
        controller_id_known: controller,
        generation: broker.generation(),
    };
    let resp = inter_broker_rpc(broker, &addr, &req).await?;
    match resp {
        Response::HeartbeatBroker {
            controller_id,
            generation,
            alive_brokers,
            ..
        } => {
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
                if let Ok(cs_resp) = inter_broker_rpc(broker, &addr, &cs_req).await {
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
            Ok(())
        }
        other => Err(Error::Protocol(format!(
            "unexpected heartbeat response: {other:?}"
        ))),
    }
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

/// Best-effort DeleteRecords fan-out to other replicas (Phase 113 PR2).
///
/// One synchronous attempt per peer after a successful **leader** local truncate.
/// Failures increment [`Broker::delete_records_fanout_errors_total`] and never
/// fail the client path. No-op in single-node mode.
pub async fn fanout_delete_records(
    broker: &Broker,
    topic: &str,
    partition: u32,
    before_offset: u64,
) {
    let peers = broker.delete_records_fanout_peers(topic, partition);
    if peers.is_empty() {
        return;
    }
    for (replica_id, addr, leader_epoch) in peers {
        let req = Request::ReplicaDeleteRecords {
            topic: topic.to_owned(),
            partition,
            before_offset,
            leader_epoch,
        };
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::ReplicaDeleteRecords {
                error_code: 0,
                ..
            }) => {}
            Ok(Response::ReplicaDeleteRecords {
                error_code,
                low_watermark,
            }) => {
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
            }
            Ok(other) => {
                warn!(
                    replica_id,
                    %addr,
                    ?other,
                    topic,
                    partition,
                    "delete records fan-out unexpected response"
                );
                broker.note_delete_records_fanout_error();
            }
            Err(e) => {
                warn!(
                    replica_id,
                    %addr,
                    error = %e,
                    topic,
                    partition,
                    "delete records fan-out rpc failed"
                );
                broker.note_delete_records_fanout_error();
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
        } => {
            fanout_txn_participant_open(
                broker,
                transactional_id,
                *producer_id,
                *producer_epoch,
                *enable_2pc,
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

/// Best-effort open fan-out (Phase 114).
pub async fn fanout_txn_participant_open(
    broker: &Broker,
    transactional_id: &str,
    producer_id: u64,
    producer_epoch: u16,
    enable_2pc: bool,
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

/// Inter-broker RPC over a short-lived connection (plain TCP or optional TLS).
///
/// When the local broker has an auth token configured, sends Auth first.
/// When [`Broker::inter_broker_tls`] is set (and the `tls` feature is enabled),
/// the connection is upgraded to TLS before the RPC.
pub async fn inter_broker_rpc(broker: &Broker, addr: &str, req: &Request) -> Result<Response> {
    let tcp = TcpStream::connect(addr).await?;

    #[cfg(feature = "tls")]
    if let Some(tls_cfg) = broker.inter_broker_tls() {
        let mut stream = connect_inter_broker_tls(tcp, addr, &tls_cfg).await?;
        return inter_broker_rpc_on(broker, &mut stream, req).await;
    }

    #[cfg(not(feature = "tls"))]
    if broker.inter_broker_tls().is_some() {
        return Err(Error::InvalidArgument(
            "inter-broker TLS configured but volant-broker was built without `--features tls`"
                .into(),
        ));
    }

    let mut stream = tcp;
    inter_broker_rpc_on(broker, &mut stream, req).await
}

async fn inter_broker_rpc_on<S>(broker: &Broker, stream: &mut S, req: &Request) -> Result<Response>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if let Some(token) = broker.auth_token() {
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
    broker: &Broker,
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
pub async fn dispatch_request(broker: &Broker, req: Request) -> Response {
    dispatch_request_as(broker, req, None).await
}

/// Dispatch with an optional connection principal for ACL checks (Phase 20).
pub async fn dispatch_request_as(
    broker: &Broker,
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
        Request::ReplicaFetch { .. }
        | Request::HeartbeatBroker { .. }
        | Request::ClusterState { .. }
        | Request::ReplicaDeleteRecords { .. }
        | Request::ClusterBrokerConfig { .. }
        | Request::ClusterAclSnapshot { .. }
        | Request::TxnParticipantOpen { .. }
        | Request::TxnParticipantPrepare { .. }
        | Request::TxnParticipantComplete { .. }
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
        | Response::TxnParticipantComplete { error_code } => {
            if *error_code != 0 {
                m.record_error(*error_code);
            }
        }
        Response::Metadata { .. } => {}
    }
}

async fn handle_request(broker: &Broker, req: Request) -> Result<Response> {
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
        } => {
            let (error_code, controller_id, generation, alive_brokers) =
                broker.handle_heartbeat_broker(broker_id, controller_id_known, generation);
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
        Request::InitProducerId { transactional_id } => {
            let (producer_id, epoch) =
                broker.init_producer_id_with_txn(&transactional_id);
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
        } => {
            let error_code = broker.handle_txn_participant_open(
                &transactional_id,
                producer_id,
                producer_epoch,
                enable_2pc,
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
        } => match broker.delete_records(&topic, partition, before_offset) {
            Ok((low_watermark, error_code)) => {
                // Phase 113: best-effort fan-out after local leader success.
                if error_code == 0 {
                    fanout_delete_records(broker, &topic, partition, before_offset).await;
                }
                Ok(Response::DeleteRecords {
                    error_code,
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
