//! Prometheus HTTP `/metrics` server and text renderer.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use volant_core::{Error, Result};

use crate::broker::Broker;
use crate::metrics::Metrics;

use super::{drain_connection_tasks, shutdown_signal};

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
    text.push_str("# HELP volant_fetch_sessions_mirrored Foreign session mirrors currently held\n");
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
    // Phase 146: incremental/delta MirrorPut wire.
    text.push_str(
        "# HELP volant_fetch_session_mirror_delta_puts_total Delta MirrorPut payloads sent or applied\n",
    );
    text.push_str("# TYPE volant_fetch_session_mirror_delta_puts_total counter\n");
    text.push_str(&format!(
        "volant_fetch_session_mirror_delta_puts_total {}\n",
        sessions.mirror_delta_puts_total()
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
    text.push_str("# HELP volant_txn_forward_errors_total Failed Kafka txn API forward attempts\n");
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
    text.push_str("# HELP volant_open_txns_expired_total Open txns auto-aborted by timeout\n");
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
    // Phase 135/148: optional client-visible wait on truncate-journal majority.
    // Wait mode (Phase 148) defers local truncate until majority; fail leaves
    // log_start unchanged. Counters tick only when effective wait is on.
    text.push_str(
        "# HELP volant_delete_records_majority_wait_success_total DeleteRecords wait-mode journal majority successes\n",
    );
    text.push_str("# TYPE volant_delete_records_majority_wait_success_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_majority_wait_success_total {}\n",
        broker.delete_records_majority_wait_success_total()
    ));
    text.push_str(
        "# HELP volant_delete_records_majority_wait_fail_total DeleteRecords wait-mode journal majority failures (Phase 148: no local truncate)\n",
    );
    text.push_str("# TYPE volant_delete_records_majority_wait_fail_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_majority_wait_fail_total {}\n",
        broker.delete_records_majority_wait_fail_total()
    ));
    // Phase 148: majority-first ordering (wait mode only; same events as wait_*).
    text.push_str(
        "# HELP volant_delete_records_majority_first_success_total DeleteRecords wait-mode majority-first successes (journal then local truncate)\n",
    );
    text.push_str("# TYPE volant_delete_records_majority_first_success_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_majority_first_success_total {}\n",
        broker.delete_records_majority_first_success_total()
    ));
    text.push_str(
        "# HELP volant_delete_records_majority_first_fail_total DeleteRecords wait-mode majority-first failures (log_start unchanged)\n",
    );
    text.push_str("# TYPE volant_delete_records_majority_first_fail_total counter\n");
    text.push_str(&format!(
        "volant_delete_records_majority_first_fail_total {}\n",
        broker.delete_records_majority_first_fail_total()
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
    text.push_str("# HELP volant_cluster_acl_push_errors_total ACL snapshot fan-out failures\n");
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
    text.push_str("# HELP volant_cluster_prepared_txns Controller cluster prepared index size\n");
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
    text.push_str("# HELP volant_applied_acl_generation Last applied ACL generation\n");
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
    text.push_str("# HELP volant_isr_expand_total ISR membership expansions (rejoin / catch-up)\n");
    text.push_str("# TYPE volant_isr_expand_total counter\n");
    text.push_str(&format!(
        "volant_isr_expand_total {}\n",
        broker.isr_expand_total()
    ));
    text.push_str("# HELP volant_isr_shrink_total ISR membership removals (death or lag)\n");
    text.push_str("# TYPE volant_isr_shrink_total counter\n");
    text.push_str(&format!(
        "volant_isr_shrink_total {}\n",
        broker.isr_shrink_total()
    ));
    // Phase 125: time-based ISR shrink.
    text.push_str("# HELP volant_isr_time_shrink_total ISR removals due to time-based lag\n");
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
    // Phase 145: rack-aware replica assignment on create.
    text.push_str(
        "# HELP volant_rack_aware_assignment_total Topic create/create-partitions using rack-diversity placement\n",
    );
    text.push_str("# TYPE volant_rack_aware_assignment_total counter\n");
    text.push_str(&format!(
        "volant_rack_aware_assignment_total {}\n",
        broker.rack_aware_assignment_total()
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
    text.push_str("# HELP volant_truncate_journal_entries Truncate journal watermark count\n");
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
    // Phase 150: assignment generation majority consensus.
    text.push_str("# HELP volant_assignment_consensus_success_total Majority assignment commits\n");
    text.push_str("# TYPE volant_assignment_consensus_success_total counter\n");
    text.push_str(&format!(
        "volant_assignment_consensus_success_total {}\n",
        broker.assignment_consensus_success_total()
    ));
    text.push_str(
        "# HELP volant_assignment_consensus_fail_total Assignment proposals without majority\n",
    );
    text.push_str("# TYPE volant_assignment_consensus_fail_total counter\n");
    text.push_str(&format!(
        "volant_assignment_consensus_fail_total {}\n",
        broker.assignment_consensus_fail_total()
    ));
    text.push_str(
        "# HELP volant_assignment_committed_generation Last majority-committed assignment generation\n",
    );
    text.push_str("# TYPE volant_assignment_committed_generation gauge\n");
    text.push_str(&format!(
        "volant_assignment_committed_generation {}\n",
        broker.assignment_committed_generation()
    ));
    // Phase 152: Metadata serves committed assignment when consensus enabled.
    text.push_str(
        "# HELP volant_assignment_metadata_committed_only Metadata uses committed assignment snapshot (0/1)\n",
    );
    text.push_str("# TYPE volant_assignment_metadata_committed_only gauge\n");
    text.push_str(&format!(
        "volant_assignment_metadata_committed_only {}\n",
        if broker.assignment_metadata_committed_only() {
            1
        } else {
            0
        }
    ));
    text.push_str(
        "# HELP volant_assignment_generation_lag Live assignment generation minus committed (max 0)\n",
    );
    text.push_str("# TYPE volant_assignment_generation_lag gauge\n");
    text.push_str(&format!(
        "volant_assignment_generation_lag {}\n",
        broker.assignment_generation_lag()
    ));
    // Phase 154: KRaft-style metadata Raft log.
    text.push_str("# HELP volant_metadata_raft_term Metadata Raft current term\n");
    text.push_str("# TYPE volant_metadata_raft_term gauge\n");
    text.push_str(&format!(
        "volant_metadata_raft_term {}\n",
        broker.metadata_raft_term()
    ));
    text.push_str("# HELP volant_metadata_raft_commit_index Metadata Raft commit index\n");
    text.push_str("# TYPE volant_metadata_raft_commit_index gauge\n");
    text.push_str(&format!(
        "volant_metadata_raft_commit_index {}\n",
        broker.metadata_raft_commit_index()
    ));
    text.push_str("# HELP volant_metadata_raft_last_applied Metadata Raft last applied index\n");
    text.push_str("# TYPE volant_metadata_raft_last_applied gauge\n");
    text.push_str(&format!(
        "volant_metadata_raft_last_applied {}\n",
        broker.metadata_raft_last_applied()
    ));
    text.push_str(
        "# HELP volant_metadata_raft_append_success_total Majority metadata Raft appends\n",
    );
    text.push_str("# TYPE volant_metadata_raft_append_success_total counter\n");
    text.push_str(&format!(
        "volant_metadata_raft_append_success_total {}\n",
        broker.metadata_raft_append_success_total()
    ));
    text.push_str(
        "# HELP volant_metadata_raft_append_fail_total Metadata Raft appends without majority\n",
    );
    text.push_str("# TYPE volant_metadata_raft_append_fail_total counter\n");
    text.push_str(&format!(
        "volant_metadata_raft_append_fail_total {}\n",
        broker.metadata_raft_append_fail_total()
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
