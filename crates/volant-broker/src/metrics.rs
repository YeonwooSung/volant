//! Prometheus-style metrics for the Volant broker.
//!
//! Lightweight atomic counters with a text exposition renderer (format 0.0.4).
//! No external Prometheus client crate is required.

use std::sync::atomic::{AtomicU64, Ordering};

/// Broker metrics registry (shared via [`std::sync::Arc`]).
#[derive(Debug, Default)]
pub struct Metrics {
    produce_requests_ok: AtomicU64,
    produce_requests_error: AtomicU64,
    produce_messages: AtomicU64,
    produce_bytes: AtomicU64,
    fetch_requests_ok: AtomicU64,
    fetch_requests_error: AtomicU64,
    fetch_messages: AtomicU64,
    fetch_bytes: AtomicU64,
    rpc_errors: AtomicU64,
    connections_accepted: AtomicU64,
    /// Error responses broken down by coarse code bucket (0..=18 + unknown).
    rpc_errors_by_code: [AtomicU64; 32],
}

impl Metrics {
    /// Create a zeroed metrics registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an accepted TCP connection.
    pub fn record_connection(&self) {
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a produce RPC outcome.
    pub fn record_produce(&self, ok: bool, messages: u64, bytes: u64) {
        if ok {
            self.produce_requests_ok.fetch_add(1, Ordering::Relaxed);
            self.produce_messages.fetch_add(messages, Ordering::Relaxed);
            self.produce_bytes.fetch_add(bytes, Ordering::Relaxed);
        } else {
            self.produce_requests_error.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Add produce value-bytes without incrementing request counters.
    pub fn add_produce_bytes(&self, bytes: u64) {
        if bytes > 0 {
            self.produce_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record a fetch RPC outcome.
    pub fn record_fetch(&self, ok: bool, messages: u64, bytes: u64) {
        if ok {
            self.fetch_requests_ok.fetch_add(1, Ordering::Relaxed);
            self.fetch_messages.fetch_add(messages, Ordering::Relaxed);
            self.fetch_bytes.fetch_add(bytes, Ordering::Relaxed);
        } else {
            self.fetch_requests_error.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record an error response by protocol error code.
    pub fn record_error(&self, code: u16) {
        self.rpc_errors.fetch_add(1, Ordering::Relaxed);
        let idx = if (code as usize) < self.rpc_errors_by_code.len() {
            code as usize
        } else {
            31 // unknown bucket
        };
        self.rpc_errors_by_code[idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Render Prometheus text exposition format 0.0.4.
    ///
    /// `topics` / `partitions` are live gauges computed by the caller.
    /// `messages_coalesced` is the broker's existing coalesce counter.
    pub fn render_prometheus(
        &self,
        topics: u64,
        partitions: u64,
        messages_coalesced: u64,
        version: &str,
    ) -> String {
        let mut out = String::with_capacity(2048);
        let p = |s: &mut String, line: &str| {
            s.push_str(line);
            s.push('\n');
        };

        p(&mut out, "# HELP volant_produce_requests_total Produce RPCs");
        p(&mut out, "# TYPE volant_produce_requests_total counter");
        out.push_str(&format!(
            "volant_produce_requests_total{{result=\"ok\"}} {}\n",
            self.produce_requests_ok.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "volant_produce_requests_total{{result=\"error\"}} {}\n",
            self.produce_requests_error.load(Ordering::Relaxed)
        ));

        p(
            &mut out,
            "# HELP volant_produce_messages_total Messages appended via produce",
        );
        p(&mut out, "# TYPE volant_produce_messages_total counter");
        out.push_str(&format!(
            "volant_produce_messages_total {}\n",
            self.produce_messages.load(Ordering::Relaxed)
        ));

        p(
            &mut out,
            "# HELP volant_produce_bytes_total Approximate value bytes produced",
        );
        p(&mut out, "# TYPE volant_produce_bytes_total counter");
        out.push_str(&format!(
            "volant_produce_bytes_total {}\n",
            self.produce_bytes.load(Ordering::Relaxed)
        ));

        p(&mut out, "# HELP volant_fetch_requests_total Fetch RPCs");
        p(&mut out, "# TYPE volant_fetch_requests_total counter");
        out.push_str(&format!(
            "volant_fetch_requests_total{{result=\"ok\"}} {}\n",
            self.fetch_requests_ok.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "volant_fetch_requests_total{{result=\"error\"}} {}\n",
            self.fetch_requests_error.load(Ordering::Relaxed)
        ));

        p(
            &mut out,
            "# HELP volant_fetch_messages_total Messages returned via fetch",
        );
        p(&mut out, "# TYPE volant_fetch_messages_total counter");
        out.push_str(&format!(
            "volant_fetch_messages_total {}\n",
            self.fetch_messages.load(Ordering::Relaxed)
        ));

        p(
            &mut out,
            "# HELP volant_fetch_bytes_total Approximate value bytes fetched",
        );
        p(&mut out, "# TYPE volant_fetch_bytes_total counter");
        out.push_str(&format!(
            "volant_fetch_bytes_total {}\n",
            self.fetch_bytes.load(Ordering::Relaxed)
        ));

        p(
            &mut out,
            "# HELP volant_rpc_errors_total Error responses by error_code",
        );
        p(&mut out, "# TYPE volant_rpc_errors_total counter");
        out.push_str(&format!(
            "volant_rpc_errors_total {}\n",
            self.rpc_errors.load(Ordering::Relaxed)
        ));
        for (code, counter) in self.rpc_errors_by_code.iter().enumerate() {
            let v = counter.load(Ordering::Relaxed);
            if v > 0 {
                out.push_str(&format!(
                    "volant_rpc_errors_total{{code=\"{code}\"}} {v}\n"
                ));
            }
        }

        p(
            &mut out,
            "# HELP volant_connections_accepted_total TCP connections accepted",
        );
        p(
            &mut out,
            "# TYPE volant_connections_accepted_total counter",
        );
        out.push_str(&format!(
            "volant_connections_accepted_total {}\n",
            self.connections_accepted.load(Ordering::Relaxed)
        ));

        p(
            &mut out,
            "# HELP volant_messages_coalesced_total Messages in multi-message produce batches",
        );
        p(
            &mut out,
            "# TYPE volant_messages_coalesced_total counter",
        );
        out.push_str(&format!(
            "volant_messages_coalesced_total {messages_coalesced}\n"
        ));

        p(&mut out, "# HELP volant_topics Topic count");
        p(&mut out, "# TYPE volant_topics gauge");
        out.push_str(&format!("volant_topics {topics}\n"));

        p(&mut out, "# HELP volant_partitions Partition count");
        p(&mut out, "# TYPE volant_partitions gauge");
        out.push_str(&format!("volant_partitions {partitions}\n"));

        let ver = sanitize_label(version);
        p(&mut out, "# HELP volant_build_info Build information");
        p(&mut out, "# TYPE volant_build_info gauge");
        out.push_str(&format!(
            "volant_build_info{{version=\"{ver}\"}} 1\n"
        ));

        out
    }
}

fn sanitize_label(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_volant_prefix() {
        let m = Metrics::new();
        m.record_produce(true, 3, 100);
        m.record_fetch(true, 2, 50);
        m.record_connection();
        m.record_error(17);
        let text = m.render_prometheus(1, 4, 3, "0.1.0");
        assert!(text.contains("volant_produce_requests_total"));
        assert!(text.contains("volant_fetch_messages_total"));
        assert!(text.contains("volant_connections_accepted_total"));
        assert!(text.contains("volant_build_info"));
        assert!(text.contains("volant_rpc_errors_total{code=\"17\"}"));
        assert!(text.contains("volant_topics 1"));
        assert!(text.contains("volant_partitions 4"));
    }
}
